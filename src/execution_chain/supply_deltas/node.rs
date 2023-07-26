use std::time::{Duration, Instant};

use async_tungstenite::{tokio::connect_async, tungstenite::Message};
use futures::{channel::mpsc::UnboundedReceiver, SinkExt, Stream, StreamExt};
use lazy_static::lazy_static;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgConnection;
use thiserror::Error;
use tokio::time::timeout;

use crate::{
    env,
    execution_chain::{BlockNumber, SupplyDelta},
    units::Wei,
};

use super::sync;

lazy_static! {
    // TODO: set to special GETH_DELTA_FORK_URL
    static ref EXECUTION_URL: String = env::get_env_var_unsafe("GETH_URL");
}

// We started running supply delta analyzation with Geth months ago (V1). Since we started running this
// code, superficial things have changed in the fork (V2), we're not sure the superficial changes are
// backwards compatible with the version we've been running for months. Therefore, we have
// one implementation compatible with the old fork (V1), that will help us ingest the historic deltas
// once, and one implementation that runs against the latest fork (V2), that will hopefully some day
// soon land in master.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyDeltaFV1 {
    block: BlockNumber,
    burn: Wei,
    destruct: Option<Wei>,
    hash: String,
    issuance: Option<Wei>,
    subsidy: Wei,
    uncles: Wei,
}

#[derive(Deserialize)]
pub struct SupplyDeltaParamsV1 {
    result: SupplyDeltaFV1,
}

#[derive(Deserialize)]
pub struct SupplyDeltaMessageV1 {
    params: SupplyDeltaParamsV1,
}

impl From<SupplyDeltaMessageV1> for SupplyDelta {
    fn from(message: SupplyDeltaMessageV1) -> Self {
        let f = message.params.result;

        Self {
            block_hash: f.hash,
            block_number: f.block,
            fee_burn: f.burn,
            fixed_reward: f.subsidy,
            parent_hash: "0x0".to_string(),
            self_destruct: f.destruct.unwrap_or(0),
            supply_delta: f.issuance.unwrap_or(0),
            uncles_reward: f.uncles,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyDeltaF {
    block: BlockNumber,
    burn: Wei,
    destruct: Option<Wei>,
    fixed_reward: Wei,
    hash: String,
    parent_hash: String,
    supply_delta: Option<Wei>,
    uncles_reward: Wei,
}

impl From<SupplyDeltaMessage> for SupplyDelta {
    fn from(message: SupplyDeltaMessage) -> Self {
        let f = message.params.result;

        Self {
            block_hash: f.hash,
            block_number: f.block,
            fee_burn: f.burn,
            fixed_reward: f.fixed_reward,
            parent_hash: f.parent_hash,
            self_destruct: f.destruct.unwrap_or(0),
            supply_delta: f.supply_delta.unwrap_or(0),
            uncles_reward: f.uncles_reward,
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

fn make_supply_delta_subscribe_message(greater_than_or_equal_to: &BlockNumber) -> String {
    let msg = json!({
        "id": 0,
        "jsonrpc": "2.0",
        "method": "eth_subscribe",
        "params": ["supplyDelta", greater_than_or_equal_to]
    });

    serde_json::to_string(&msg).unwrap()
}

#[derive(Deserialize)]
struct SubscriptionError {
    code: i32,
    message: String,
}

// success: {"jsonrpc":"2.0","id":0,"result":"0x166ea326fc28c4a9f4dcf0b5929881c8"}
// failure: {"jsonrpc":"2.0","id":0,"error":{"code":-32601,"message":"no \"issuance\" subscription in eth namespace"}}
#[derive(Deserialize)]
#[serde(untagged)]
enum SubscriptionConfirmation {
    Confirmation { result: String },
    Error { error: SubscriptionError },
}

pub fn stream_supply_deltas_from(
    greater_than_or_equal_to: BlockNumber,
) -> impl Stream<Item = SupplyDelta> {
    tracing::debug!("streaming supply deltas gte {greater_than_or_equal_to}");
    let (mut tx, rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        let url = (*EXECUTION_URL).to_string();
        let mut ws = connect_async(&url).await.unwrap().0;

        let deltas_subscribe_message =
            make_supply_delta_subscribe_message(&greater_than_or_equal_to);

        ws.send(Message::text(deltas_subscribe_message))
            .await
            .unwrap();

        // We recieve a confirmation message from the websocket server when we subscribe.
        let subscription_confirmation = {
            let message = ws.next().await.unwrap().unwrap();
            let message_text = message.into_text().unwrap();
            serde_json::from_str::<SubscriptionConfirmation>(&message_text).unwrap()
        };

        match subscription_confirmation {
            SubscriptionConfirmation::Confirmation { result } => {
                tracing::debug!("subscribed to supply deltas: {}", result);
                // When the subscription is successful continue with the rest of the stream.
            }
            SubscriptionConfirmation::Error { error } => {
                tracing::error!(
                    "failed to subscribe to supply deltas: {} {}",
                    error.code,
                    error.message
                );
                return;
            }
        }

        while let Some(message_result) = ws.next().await {
            let message = message_result.unwrap();

            // We get ping messages too. Do nothing with those.
            if message.is_ping() {
                continue;
            }

            let message_text = message.into_text().unwrap();
            let supply_delta: SupplyDelta = {
                serde_json::from_str::<SupplyDeltaMessage>(&message_text)
                    .map(|message| message.into())
                    .unwrap()
            };
            tx.send(supply_delta).await.unwrap();
        }
    });

    rx
}

pub async fn stream_supply_deltas_from_last<'a>(
    executor: &mut PgConnection,
) -> impl Stream<Item = SupplyDelta> {
    let last_synced_supply_delta_number = sync::get_last_synced_supply_delta_number(executor)
        .await
        .unwrap_or(super::snapshot::SUPPLY_SNAPSHOT_15082718.block_number);
    stream_supply_deltas_from(last_synced_supply_delta_number + 1)
}

pub fn stream_supply_delta_chunks(
    from: BlockNumber,
    chunk_size: usize,
) -> UnboundedReceiver<Vec<SupplyDelta>> {
    let (mut tx, rx) = futures::channel::mpsc::unbounded();

    let mut supply_deltas_rx = stream_supply_deltas_from(from);

    let mut supply_delta_buffer = Vec::with_capacity(chunk_size);

    tokio::spawn(async move {
        let last_delta_received_at = Instant::now();
        let three_seconds = Duration::from_secs(3);
        while let Some(supply_delta) = supply_deltas_rx.next().await {
            supply_delta_buffer.push(supply_delta);

            let duration_since_last_delta = Instant::now().duration_since(last_delta_received_at);

            if supply_delta_buffer.len() >= chunk_size || duration_since_last_delta >= three_seconds
            {
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
    block_number: BlockNumber,
) -> Result<SupplyDelta, SupplyDeltaByBlockNumberError> {
    let url = (*EXECUTION_URL).to_string();
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
            let supply_delta: SupplyDelta =
                serde_json::from_str::<SupplyDeltaMessage>(&message_text)
                    .map(|message| message.into())
                    .unwrap();
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

    use sqlx::Acquire;
    use tokio::time::timeout;

    use crate::{db, execution_chain::supply_deltas::add_delta};

    use super::*;

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
    async fn test_get_latest_synced_supply_delta_number() {
        let mut connection = db::tests::get_test_db_connection().await;
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

        add_delta(&mut *transaction, &supply_delta_test).await;

        let latest_synced_supply_delta_number =
            sync::get_last_synced_supply_delta_number(&mut *transaction).await;

        assert_eq!(latest_synced_supply_delta_number, Some(0));
    }

    #[tokio::test]
    async fn test_get_latest_synced_supply_delta_number_empty() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let latest_synced_supply_delta_number =
            sync::get_last_synced_supply_delta_number(&mut *transaction).await;

        assert_eq!(latest_synced_supply_delta_number, None);
    }
}
