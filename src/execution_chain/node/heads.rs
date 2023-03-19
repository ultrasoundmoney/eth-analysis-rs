use async_tungstenite::{tokio as tungstenite, tungstenite::Message};
use chrono::{DateTime, Utc};
use futures::{channel::mpsc, SinkExt, Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use super::{blocks::ExecutionNodeBlock, decoders::*, ExecutionNode, EXECUTION_URL};
use crate::execution_chain::{node::BlockNumber, BlockRange};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Head {
    pub hash: String,
    #[serde(deserialize_with = "from_i32_hex_str")]
    pub number: BlockNumber,
    pub parent_hash: String,
    #[serde(deserialize_with = "from_unix_timestamp_hex_str")]
    pub timestamp: DateTime<Utc>,
}

impl From<NewHeadMessage> for Head {
    fn from(message: NewHeadMessage) -> Self {
        message.params.result
    }
}

impl From<ExecutionNodeBlock> for Head {
    fn from(block: ExecutionNodeBlock) -> Self {
        Self {
            hash: block.hash,
            number: block.number,
            parent_hash: block.parent_hash,
            timestamp: block.timestamp,
        }
    }
}

#[derive(Deserialize)]
pub struct NewHeadParams {
    result: Head,
}

#[derive(Deserialize)]
pub struct NewHeadMessage {
    params: NewHeadParams,
}

enum HeadsMessage {
    Subscribe,
    #[allow(dead_code)]
    Unsubscribe(String),
}

impl From<HeadsMessage> for Message {
    fn from(message: HeadsMessage) -> Self {
        match message {
            HeadsMessage::Subscribe => {
                let msg = json!({
                    "id": 0,
                    "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": ["newHeads"]
                });
                let message_text = serde_json::to_string(&msg).unwrap();
                Message::text(message_text)
            }
            HeadsMessage::Unsubscribe(id) => {
                let msg = json!({
                    "id": 0,
                    "jsonrpc": "2.0",
                    "method": "eth_unsubscribe",
                    "params": [id]
                });
                let message_text = serde_json::to_string(&msg).unwrap();
                Message::text(message_text)
            }
        }
    }
}

pub fn stream_new_heads() -> impl Stream<Item = Head> {
    let (mut new_heads_tx, new_heads_rx) = mpsc::unbounded();

    tokio::spawn(async move {
        let url = (*EXECUTION_URL).to_string();
        let mut ws = tungstenite::connect_async(&url).await.unwrap().0;

        ws.send(HeadsMessage::Subscribe.into()).await.unwrap();

        loop {
            if (ws.next().await).is_some() {
                tracing::debug!("got subscription confirmation message");
                break;
            }
        }

        while let Some(message_result) = ws.next().await {
            let message = message_result.unwrap();

            // We get ping messages too. Do nothing with those.
            if message.is_ping() {
                continue;
            }

            let message_text = message.into_text().unwrap();
            let new_head_message: NewHeadMessage = serde_json::from_str(&message_text).unwrap();
            let new_head = new_head_message.into();
            new_heads_tx.send(new_head).await.unwrap();
        }
    });

    new_heads_rx
}

fn stream_new_head_block_numbers() -> impl Stream<Item = BlockNumber> {
    stream_new_heads().map(|head| head.number)
}

fn stream_historic_block_numbers(block_range: BlockRange) -> impl Stream<Item = BlockNumber> {
    let (mut tx, rx) = futures::channel::mpsc::channel(10);

    tokio::spawn(async move {
        let mut execution_node = ExecutionNode::connect().await;
        for block_number in block_range {
            let block = execution_node
                .get_block_by_number(&block_number)
                .await
                .unwrap();
            tx.send(block.number).await.unwrap();
        }
    });

    rx
}

pub async fn stream_heads_from(gte_slot: BlockNumber) -> impl Stream<Item = BlockNumber> {
    debug!(from = gte_slot, "streaming heads");

    let mut execution_node = ExecutionNode::connect().await;
    let last_block_on_start = execution_node.get_latest_block().await;
    debug!(
        block_number = last_block_on_start.number,
        "last block on chain",
    );

    // We stream heads as requested until caught up with the chain and then pass heads as they come
    // in from our node. The only way to be sure how high we should stream, is to wait for the
    // first head from the node to come in. We don't want to wait. So ask for the latest head, take
    // this as the max, and immediately start listening for new heads. Running the small risk the
    // chain has advanced between these two calls.
    let heads_stream = stream_new_head_block_numbers();

    let block_range = BlockRange::new(gte_slot, last_block_on_start.number);

    let historic_heads_stream = stream_historic_block_numbers(block_range);

    historic_heads_stream.chain(heads_stream)
}
