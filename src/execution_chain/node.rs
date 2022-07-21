mod blocks;
mod decoders;
mod heads;
mod supply_deltas;

use super::SupplyDelta;
use core::panic;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use self::blocks::*;
use self::heads::*;
use self::supply_deltas::SupplyDeltaMessage;
use async_tungstenite::{
    tokio::{connect_async, TokioAdapter},
    tungstenite::Message,
    WebSocketStream,
};
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::prelude::*;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RpcMessage {
    RpcMessageError { id: u16, error: RpcError },
    RpcMessageResult { id: u16, result: serde_json::Value },
}

fn make_supply_delta_subscribe_message(greater_than_or_equal_to: &u32) -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_subscribe",
        "params": ["issuance", greater_than_or_equal_to]
    });

    serde_json::to_string(&msg).unwrap()
}

#[allow(dead_code)]
fn make_eth_unsubscribe_message(id: &str) -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_unsubscribe",
        "params": [id]

    });

    serde_json::to_string(&msg).unwrap()
}

fn make_new_heads_subscribe_message() -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_subscribe",
        "params": ["newHeads"]
    });

    serde_json::to_string(&msg).unwrap()
}

struct IdPool {
    next_id: u16,
    in_use_ids: HashSet<u16>,
}

impl IdPool {
    fn new(size: usize) -> Self {
        Self {
            next_id: 0,
            in_use_ids: HashSet::with_capacity(size),
        }
    }

    fn get_next_id(&mut self) -> u16 {
        if self.in_use_ids.len() == self.in_use_ids.capacity() {
            panic!("execution node id pool exhausted")
        }

        while self.in_use_ids.contains(&self.next_id) {
            self.next_id = self.next_id + 1;
        }

        self.in_use_ids.insert(self.next_id);

        self.next_id
    }

    fn free_id(&mut self, id: &u16) {
        self.in_use_ids.remove(id);
    }
}

pub fn stream_supply_deltas(greater_than_or_equal_to: u32) -> mpsc::UnboundedReceiver<SupplyDelta> {
    let (mut supply_deltas_tx, supply_deltas_rx) = mpsc::unbounded();

    tokio::spawn(async move {
        let url = format!("{}", &crate::config::get_execution_url());
        let mut ws = connect_async(&url).await.unwrap().0;

        let deltas_subscribe_message =
            make_supply_delta_subscribe_message(&greater_than_or_equal_to);

        ws.send(Message::text(deltas_subscribe_message))
            .await
            .unwrap();

        loop {
            if let Some(_) = ws.next().await {
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
            let supply_delta_message =
                serde_json::from_str::<SupplyDeltaMessage>(&message_text).unwrap();
            let supply_delta = SupplyDelta::from(supply_delta_message);
            supply_deltas_tx.send(supply_delta).await.unwrap();
        }
    });

    supply_deltas_rx
}

pub fn stream_supply_delta_chunks(
    from: u32,
    chunk_size: usize,
) -> mpsc::UnboundedReceiver<Vec<SupplyDelta>> {
    let (mut supply_delta_chunks_tx, supply_delta_chunks_rx) = mpsc::unbounded();

    let mut supply_deltas_rx = stream_supply_deltas(from);

    let mut supply_delta_buffer = Vec::with_capacity(chunk_size);

    tokio::spawn(async move {
        while let Some(supply_delta) = supply_deltas_rx.next().await {
            supply_delta_buffer.push(supply_delta);

            if supply_delta_buffer.len() >= chunk_size {
                supply_delta_chunks_tx
                    .send(supply_delta_buffer)
                    .await
                    .unwrap();
                supply_delta_buffer = vec![];
            }
        }
    });

    supply_delta_chunks_rx
}

pub fn stream_new_heads() -> mpsc::UnboundedReceiver<Head> {
    let (mut new_heads_tx, new_heads_rx) = mpsc::unbounded();

    tokio::spawn(async move {
        let url = format!("{}", &crate::config::get_execution_url());
        let mut ws = connect_async(&url).await.unwrap().0;

        let new_heads_subscribe_message = make_new_heads_subscribe_message();

        ws.send(Message::text(new_heads_subscribe_message))
            .await
            .unwrap();

        loop {
            if let Some(_) = ws.next().await {
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

            let new_head_message =
                serde_json::from_str::<NewHeadMessage>(&message.into_text().unwrap()).unwrap();
            let new_head = Head::from(new_head_message);
            new_heads_tx.send(new_head).await.unwrap();
        }
    });

    new_heads_rx
}

#[derive(Error, Debug)]
pub enum SupplyDeltaByBlockNumberError {
    #[error("did not receive a supply delta within the given timeout")]
    Timeout,
    #[error("connection closed before receiving a supply delta")]
    Closed,
}

pub async fn get_supply_delta_by_block_number(
    block_number: u32,
) -> Result<SupplyDelta, SupplyDeltaByBlockNumberError> {
    crate::performance::LifetimeMeasure::log_lifetime("get_supply_delta_by_block_number");

    let url = format!("{}", &crate::config::get_execution_url());
    let mut ws = connect_async(&url).await.unwrap().0;

    let deltas_subscribe_message = make_supply_delta_subscribe_message(&(block_number));

    ws.send(Message::text(deltas_subscribe_message))
        .await
        .unwrap();

    ws.next().await;
    tracing::debug!("got subscription confirmation message");

    // We work with a pipeline that also pipes through ping messages. We ignore those but at the
    // same time expect an answer quickly and don't want to hang waiting, so we use a timeout.
    let supply_delta_future = tokio::spawn(async move {
        while let Some(message_result) = ws.next().await {
            let message = message_result.unwrap();

            // We get ping messages too. Do nothing with those.
            if message.is_ping() {
                continue;
            }

            let message_text = message.into_text().unwrap();
            let supply_delta_message =
                serde_json::from_str::<SupplyDeltaMessage>(&message_text).unwrap();
            let supply_delta = SupplyDelta::from(supply_delta_message);
            return Ok(supply_delta);
        }

        Err(SupplyDeltaByBlockNumberError::Closed)
    });

    match timeout(Duration::from_secs(4), supply_delta_future).await {
        Err(_) => Err(SupplyDeltaByBlockNumberError::Timeout),
        // Unwrap the JoinError.
        Ok(res) => res.unwrap(),
    }
}

type NodeMessageRx = SplitStream<WebSocketStream<TokioAdapter<TcpStream>>>;
type MessageHandlersShared =
    Arc<Mutex<HashMap<u16, oneshot::Sender<Result<serde_json::Value, RpcError>>>>>;
type IdPoolShared = Arc<Mutex<IdPool>>;

async fn handle_messages(
    mut ws_rx: NodeMessageRx,
    message_handlers: MessageHandlersShared,
    id_pool: IdPoolShared,
) -> Result<(), Box<dyn Error>> {
    while let Some(message_result) = ws_rx.next().await {
        let message = message_result?;

        // We get ping messages too. Do nothing with those.
        if message.is_ping() {
            continue;
        }

        let message_text = message.into_text()?;
        let rpc_message = serde_json::from_str::<RpcMessage>(&message_text).unwrap();

        let id = match rpc_message {
            RpcMessage::RpcMessageResult { id, .. } => id,
            RpcMessage::RpcMessageError { id, .. } => id,
        };

        match message_handlers.lock().unwrap().remove(&id) {
            None => {
                tracing::error!(
                    "got a message but missing a registered handler for id: {id}, message: {:?}",
                    rpc_message
                );
            }
            Some(tx) => match rpc_message {
                RpcMessage::RpcMessageResult { result, .. } => {
                    tx.send(Ok(result)).unwrap();
                }
                RpcMessage::RpcMessageError { error, .. } => {
                    tx.send(Err(error)).unwrap();
                }
            },
        };

        id_pool.lock().unwrap().free_id(&id);
    }

    Ok(())
}

pub struct ExecutionNode {
    id_pool: Arc<Mutex<IdPool>>,
    message_handlers: MessageHandlersShared,
    message_sink: SplitSink<WebSocketStream<TokioAdapter<TcpStream>>, Message>,
}

impl ExecutionNode {
    pub async fn connect() -> Self {
        let id_pool_am = Arc::new(Mutex::new(IdPool::new(u16::MAX.into())));

        let message_handlers_am = Arc::new(Mutex::new(HashMap::with_capacity(u16::MAX.into())));

        let url = format!("{}", &crate::config::get_execution_url());
        let (ws_tx, ws_rx) = connect_async(&url).await.unwrap().0.split();

        // We'd like to read websocket messages concurrently so we read in a thread.
        // The websocket uses pipelining, so IDs are used to match request and response.
        // We'd like the request to wait for a response (from the thread).
        // Currently we use a HashMap + callback channel system, this means requests hang
        // when the websocket thread panics. Try rewriting to an implementation where the
        // sending end gets moved to the thread so that it may be dropped when the thread panics.
        // As a workaround we panic main when this thread panics.
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        let id_pool_ref = id_pool_am.clone();
        let message_handlers_ref = message_handlers_am.clone();
        tokio::spawn(async move {
            handle_messages(ws_rx, message_handlers_ref, id_pool_ref)
                .await
                .unwrap()
        });

        ExecutionNode {
            id_pool: id_pool_am,
            message_handlers: message_handlers_am,
            message_sink: ws_tx,
        }
    }

    pub async fn get_latest_block(&mut self) -> ExecutionNodeBlock {
        let value = self
            .call(
                "eth_getBlockByNumber",
                &json!((String::from("latest"), false)),
            )
            .await
            .unwrap();

        serde_json::from_value::<ExecutionNodeBlock>(value).unwrap()
    }

    #[allow(dead_code)]
    pub async fn get_block_by_number(&mut self, number: &u32) -> ExecutionNodeBlock {
        let hex_number = format!("0x{:x}", number);
        let value = self
            .call("eth_getBlockByNumber", &json!((hex_number, false)))
            .await
            .unwrap();

        serde_json::from_value::<ExecutionNodeBlock>(value).unwrap()
    }

    async fn call(&mut self, method: &str, params: &Value) -> Result<serde_json::Value, RpcError> {
        let id = self.id_pool.lock().unwrap().get_next_id();

        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });

        let message = serde_json::to_string(&json).unwrap();

        let (tx, rx) = oneshot::channel();

        self.message_handlers.lock().unwrap().insert(id, tx);
        self.message_sink
            .send(Message::Text(message))
            .await
            .unwrap();

        rx.await.unwrap()
    }

    #[allow(dead_code)]
    pub async fn close(mut self) {
        self.message_sink.close().await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_get_latest_block() {
        let mut node = ExecutionNode::connect().await;
        let _block = node.get_latest_block().await;
        node.close().await;
    }

    #[tokio::test]
    async fn test_get_block() {
        let mut node = ExecutionNode::connect().await;
        let block = node.get_block_by_number(&0).await;
        assert_eq!(0, block.number);
        node.close().await;
    }

    #[ignore]
    #[tokio::test]
    async fn test_stream_supply_deltas() {
        let stream = stream_supply_deltas(15_000_000);
        let ten_deltas_future = stream.take(10).collect::<Vec<_>>();
        match timeout(Duration::from_secs(4), ten_deltas_future).await {
            Err(_) => {
                panic!("test_stream_supply_deltas failed to receive ten deltas in three seconds")
            }
            Ok(deltas) => {
                assert_eq!(deltas.len(), 10);
            }
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_get_supply_delta_by_block_number() {
        let supply_delta = get_supply_delta_by_block_number(15_000_000).await.unwrap();
        assert_eq!(supply_delta.block_number, 15_000_000);
    }
}
