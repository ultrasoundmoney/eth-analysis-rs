use std::time::Duration;

use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::{channel::mpsc::UnboundedReceiver, SinkExt, Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgConnection;
use thiserror::Error;
use tokio::time::timeout;

use super::sync::get_last_synced_supply_delta_number;
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

pub fn stream_supply_deltas_from(greater_than_or_equal_to: u32) -> impl Stream<Item = SupplyDelta> {
    tracing::debug!("streaming supply deltas gte {greater_than_or_equal_to}");
    let (mut tx, rx) = futures::channel::mpsc::unbounded();

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
            tx.send(supply_delta).await.unwrap();
        }
    });

    rx
}

pub async fn stream_supply_deltas_from_last<'a>(
    executor: &mut PgConnection,
) -> impl Stream<Item = SupplyDelta> {
    let last_synced_supply_delta_number = get_last_synced_supply_delta_number(executor)
        .await
        .unwrap_or(super::snapshot::SUPPLY_SNAPSHOT_15082718.block_number);
    super::stream_supply_deltas_from(last_synced_supply_delta_number + 1)
}

pub fn stream_supply_delta_chunks(
    from: u32,
    chunk_size: usize,
) -> UnboundedReceiver<Vec<SupplyDelta>> {
    let (mut tx, rx) = futures::channel::mpsc::unbounded();

    let mut supply_deltas_rx = stream_supply_deltas_from(from);

    let mut supply_delta_buffer = Vec::with_capacity(chunk_size);

    tokio::spawn(async move {
        while let Some(supply_delta) = supply_deltas_rx.next().await {
            supply_delta_buffer.push(supply_delta);

            if supply_delta_buffer.len() >= chunk_size {
                tx.send(supply_delta_buffer).await.unwrap();
                supply_delta_buffer = vec![];
            }
        }
    });

    rx
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

    use serial_test::serial;
    use sqlx::Connection;
    use sqlx::PgConnection;
    use tokio::time::timeout;

    use super::super::add_delta;
    use super::*;
    use crate::config;

    #[ignore]
    #[tokio::test]
    async fn test_stream_supply_deltas() {
        let stream = stream_supply_deltas_from(15_000_000);
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

    #[tokio::test]
    #[serial]
    async fn test_get_latest_synced_supply_delta_number() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        add_delta(&mut transaction, &supply_delta_test).await;

        let latest_synced_supply_delta_number =
            get_last_synced_supply_delta_number(&mut transaction).await;

        assert_eq!(latest_synced_supply_delta_number, Some(0));
    }

    #[tokio::test]
    #[serial]
    async fn test_get_latest_synced_supply_delta_number_empty() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let latest_synced_supply_delta_number =
            get_last_synced_supply_delta_number(&mut transaction).await;

        assert_eq!(latest_synced_supply_delta_number, None);
    }
}
