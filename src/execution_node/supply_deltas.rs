use serde::{Deserialize, Serialize};

use crate::eth_units::WEI_PER_GWEI_F64;

#[derive(Debug, Serialize)]
pub struct SupplyDelta {
    pub block_hash: String,
    pub block_number: u32,
    pub fee_burn: i64,
    pub fixed_reward: i64,
    pub parent_hash: String,
    pub self_destruct: i64,
    pub supply_delta: i64,
    pub uncles_reward: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyDeltaF {
    block: u32,
    burn: f64,
    destruct: f64,
    hash: String,
    issuance: f64,
    parent_hash: String,
    subsidy: f64,
    uncles: f64,
}

impl From<SupplyDeltaMessage> for SupplyDelta {
    fn from(message: SupplyDeltaMessage) -> Self {
        let f = message.params.result;

        Self {
            block_hash: f.hash,
            block_number: f.block,
            fee_burn: (f.burn / WEI_PER_GWEI_F64).round() as i64,
            fixed_reward: (f.subsidy / WEI_PER_GWEI_F64).round() as i64,
            parent_hash: f.parent_hash,
            self_destruct: (f.destruct / WEI_PER_GWEI_F64).round() as i64,
            supply_delta: (f.issuance / WEI_PER_GWEI_F64).round() as i64,
            uncles_reward: (f.uncles / WEI_PER_GWEI_F64).round() as i64,
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
