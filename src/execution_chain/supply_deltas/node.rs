use std::time::Duration;

use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::{channel::mpsc::UnboundedReceiver, SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::time::timeout;

use crate::{eth_units::Wei, execution_chain::SupplyDelta};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyDeltaF {
    block: u32,
    burn: Wei,
    destruct: Option<Wei>,
    hash: String,
    issuance: Option<Wei>,
    parent_hash: String,
    subsidy: Wei,
    uncles: Wei,
}

impl From<SupplyDeltaMessage> for SupplyDelta {
    fn from(message: SupplyDeltaMessage) -> Self {
        let f = message.params.result;

        Self {
            block_hash: f.hash,
            block_number: f.block,
            fee_burn: f.burn,
            fixed_reward: f.subsidy,
            parent_hash: f.parent_hash,
            self_destruct: f.destruct.unwrap_or(0),
            supply_delta: f.issuance.unwrap_or(0),
            uncles_reward: f.uncles,
        }
    }
}

#[derive(Deserialize)]
pub struct SupplyDeltaParams {
    // subscription: String,
    result: SupplyDeltaF,
}

#[derive(Deserialize)]
pub struct SupplyDeltaMessage {
    params: SupplyDeltaParams,
}

fn make_supply_delta_subscribe_message(greater_than_or_equal_to: &u32) -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_subscribe",
        "params": ["issuance", greater_than_or_equal_to]
    });

    serde_json::to_string(&msg).unwrap()
}

pub fn stream_supply_deltas(greater_than_or_equal_to: u32) -> UnboundedReceiver<SupplyDelta> {
    let (mut supply_deltas_tx, supply_deltas_rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        let url = format!("{}", &crate::config::get_execution_url());
        let mut ws = connect_async(&url).await.unwrap().0;

        let deltas_subscribe_message =
            make_supply_delta_subscribe_message(&greater_than_or_equal_to);

        ws.send(Message::text(deltas_subscribe_message))
            .await
            .unwrap();

        ws.next().await;
        tracing::debug!("got subscription confirmation message");

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
) -> UnboundedReceiver<Vec<SupplyDelta>> {
    let (mut supply_delta_chunks_tx, supply_delta_chunks_rx) = futures::channel::mpsc::unbounded();

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

#[cfg(test)]
mod tests {
    use core::panic;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

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
