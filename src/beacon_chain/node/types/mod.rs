mod balances;
mod blocks;
mod deposits;
mod headers;

use std::fmt::Display;

use serde::Deserialize;

use crate::beacon_chain::Slot;
use crate::{json_codecs::i32_from_string, units::GweiNewtype};

pub use balances::*;
pub use blocks::*;
pub use deposits::*;
pub use headers::*;

// Type Aliases
pub type StateRoot = String;
pub type BlockRoot = String;

#[allow(dead_code)]
#[derive(Debug)]
pub enum BlockId {
    BlockRoot(String),
    Finalized,
    Genesis,
    Head,
    Slot(Slot),
}

impl From<Slot> for BlockId {
    fn from(slot: Slot) -> Self {
        BlockId::Slot(slot)
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockRoot(block_root) => write!(f, "BlockId(BlockRoot({block_root}))"),
            Self::Finalized => write!(f, "BlockId(Finalized)"),
            Self::Genesis => write!(f, "BlockId(Genesis)"),
            Self::Head => write!(f, "BlockId(Head)"),
            Self::Slot(slot) => write!(f, "BlockId(Slot{slot})"),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Withdrawal {
    #[serde(deserialize_with = "i32_from_string")]
    pub index: i32,
    #[allow(dead_code)]
    pub address: String,
    pub amount: GweiNewtype,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionWithdrawal {
    pub amount: GweiNewtype,
    pub source_address: String,
    pub validator_pubkey: String,
}

#[derive(Deserialize)]
pub struct FinalityCheckpoint {
    #[allow(dead_code)]
    #[serde(deserialize_with = "i32_from_string")]
    epoch: i32,
    #[allow(dead_code)]
    root: String,
}

#[derive(Deserialize)]
pub(super) struct FinalityCheckpoints {
    pub finalized: FinalityCheckpoint,
}

#[derive(Deserialize)]
pub(super) struct CheckpointEnvelope {
    pub data: FinalityCheckpoints,
}

#[derive(Deserialize)]
pub(super) struct StateRootData {
    pub root: StateRoot,
}

#[derive(Deserialize)]
pub(super) struct StateRootEnvelope {
    pub data: StateRootData,
}
