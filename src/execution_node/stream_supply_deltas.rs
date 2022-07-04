use serde::{Deserialize, Serialize};

use crate::eth_units::GWEI_PER_ETH_F64;

#[derive(Debug, Serialize)]
pub struct SupplyDelta {
    pub block_number: u32,
    fee_burn: f64,
    fixed_reward: f64,
    hash: String,
    parent_hash: String,
    self_destruct: f64,
    supply_delta: f64,
    uncles_reward: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyDeltaF {
    block: u32,
    hash: String,
    parent_hash: String,
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
pub struct SupplyDeltaParams {
    // subscription: String,
    result: SupplyDeltaF,
}

#[derive(Deserialize)]
pub struct SupplyDeltaMessage {
    params: SupplyDeltaParams,
}
