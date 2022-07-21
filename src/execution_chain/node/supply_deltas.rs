use serde::Deserialize;

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
