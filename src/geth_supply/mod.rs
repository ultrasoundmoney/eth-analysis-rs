use serde::{Deserialize, Serialize};

use crate::execution_chain::BlockNumber;

pub mod historic;
pub mod live;

pub use historic::backfill_historic_deltas;
pub use live::start_live_api;

#[derive(Debug, Serialize, Deserialize)]
pub struct GethSupplyDelta {
    pub issuance: Option<Issuance>,
    pub burn: Option<Burn>,
    #[serde(rename = "blockNumber")]
    pub block_number: BlockNumber,
    pub hash: String,
    #[serde(rename = "parentHash")]
    pub parent_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Issuance {
    pub withdrawals: Option<String>, // Hex string
    pub reward: Option<String>,      // Hex string
    #[serde(rename = "genesisAlloc")]
    pub genesis_allocation: Option<String>, // Hex string
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Burn {
    #[serde(rename = "1559")]
    pub eip1559: Option<String>, // Hex string
    pub blob: Option<String>, // Hex string
    pub misc: Option<String>, // Hex string
}
