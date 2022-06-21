use crate::{config, eth_units::GWEI_PER_ETH_F64};
use async_tungstenite::{
    tokio::{connect_async, TokioAdapter},
    tungstenite::Message,
    WebSocketStream,
};
use futures::{channel::mpsc::UnboundedSender, prelude::*};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpStream;

// #[derive(Deserialize)]
// #[serde(untagged)]
// enum RpcMessageIssuance {
//     SupplyDeltaMessage {
//         params: SupplyDeltaParams,
//     },
//     SubscriptionConfirmationMessage {
//         id: u32,
//         jsonrpc: String,
//         result: String,
//     },
//     UnsubscriptionConfirmationMessage {
//         id: u32,
//         jsonrpc: String,
//         result: bool,
//     },
// }

// #[derive(Deserialize)]
// struct RpcMessage {
//     jsonrpc: String,
//     id: u32,
// }

fn make_issuance_subscribe_message(number: &u32) -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_subscribe",
        "params": ["issuance", number]
    });

    serde_json::to_string(&msg).unwrap()
}

// fn make_issuance_unsubscribe_message(id: &str) -> String {
//     let msg = json!({
//         "id": 0,
//         "method": "eth_unsubscribe",
//         "params": [id]

//     });

//     serde_json::to_string(&msg).unwrap()
// }

#[derive(Debug, Serialize)]
pub struct SupplyDelta {
    block_number: u32,
    fee_burn: f64,
    fixed_reward: f64,
    hash: String,
    self_destruct: f64,
    supply_delta: f64,
    uncles_reward: f64,
}

#[derive(Debug, Deserialize)]
struct SupplyDeltaF {
    block: u32,
    hash: String,
    issuance: Option<f64>,
    subsidy: f64,
    uncles: f64,
    burn: f64,
    destruct: Option<f64>,
}

impl From<SupplyDeltaMessage> for SupplyDelta {
    fn from(message: SupplyDeltaMessage) -> Self {
        let f = message.params.result;

        Self {
            block_number: f.block,
            fee_burn: f.burn / GWEI_PER_ETH_F64,
            fixed_reward: f.subsidy / GWEI_PER_ETH_F64,
            hash: f.hash,
            self_destruct: f.destruct.unwrap_or(0.0) / GWEI_PER_ETH_F64,
            supply_delta: f.issuance.unwrap_or(0.0) / GWEI_PER_ETH_F64,
            uncles_reward: f.uncles / GWEI_PER_ETH_F64,
        }
    }
}

#[derive(Deserialize)]
struct SupplyDeltaParams {
    // subscription: String,
    result: SupplyDeltaF,
}

#[derive(Deserialize)]
struct SupplyDeltaMessage {
    params: SupplyDeltaParams,
}

pub const SUPPLY_DELTA_BUFFER_SIZE: usize = 10_000;

type NodeStream = WebSocketStream<TokioAdapter<TcpStream>>;

pub struct ExecutionNode {
    ws: NodeStream,
}

impl ExecutionNode {
    pub async fn connect() -> Self {
        let ws = connect_async(format!("{}", &config::get_execution_url()))
            .await
            .unwrap()
            .0;

        ExecutionNode { ws }
    }

    pub async fn stream_supply_deltas_from(
        mut self,
        mut supply_deltas_tx: UnboundedSender<Vec<SupplyDelta>>,
        from: &u32,
    ) {
        let deltas_subscribe_message = make_issuance_subscribe_message(from);

        self.ws
            .send(Message::text(deltas_subscribe_message))
            .await
            .unwrap();

        loop {
            if let Some(_) = self.ws.next().await {
                tracing::debug!("got subscription confirmation message");
                break;
            }
        }

        let mut supply_delta_buffer = Vec::with_capacity(SUPPLY_DELTA_BUFFER_SIZE);

        while let Some(message) = self.ws.next().await {
            let message_text = message.unwrap().into_text().unwrap();
            let supply_delta_message =
                serde_json::from_str::<SupplyDeltaMessage>(&message_text).unwrap();
            let supply_delta = SupplyDelta::from(supply_delta_message);

            supply_delta_buffer.push(supply_delta);

            if supply_delta_buffer.len() >= SUPPLY_DELTA_BUFFER_SIZE {
                supply_deltas_tx.send(supply_delta_buffer).await.unwrap();
                supply_delta_buffer = vec![];
            }
        }
    }
}
