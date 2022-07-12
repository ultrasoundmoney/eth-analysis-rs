use serde::Deserialize;

use crate::{eth_units::Wei, execution_chain::SupplyDelta};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyDeltaF {
    block: u32,
    burn: Wei,
    destruct: Wei,
    hash: String,
    issuance: Wei,
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
            self_destruct: f.destruct,
            supply_delta: f.issuance,
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
