// src/beacon_chain/node/types/headers.rs
use super::blocks::BeaconBlock;
use super::{BlockRoot, StateRoot}; // From types/mod.rs
use crate::beacon_chain::{slot_from_string, Slot};
use serde::Deserialize; // From types/blocks.rs

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BeaconHeader {
    #[serde(deserialize_with = "slot_from_string")]
    pub slot: Slot,
    pub parent_root: BlockRoot,
    pub state_root: StateRoot,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BeaconHeaderEnvelope {
    pub message: BeaconHeader,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BeaconHeaderSignedEnvelope {
    /// Root (hash) of the block this header is about.
    pub root: BlockRoot,
    pub header: BeaconHeaderEnvelope,
}

impl BeaconHeaderSignedEnvelope {
    pub fn slot(&self) -> Slot {
        self.header.message.slot
    }

    pub fn parent_root(&self) -> BlockRoot {
        self.header.message.parent_root.clone()
    }

    pub fn state_root(&self) -> StateRoot {
        self.header.message.state_root.clone()
    }
}

impl From<&BeaconBlock> for BeaconHeaderSignedEnvelope {
    fn from(block: &BeaconBlock) -> Self {
        BeaconHeaderSignedEnvelope {
            // TODO: this default root means this conversion is only suitable for tests.
            // Use a proper block root when available.
            root: "0xdefault".to_string(),
            header: BeaconHeaderEnvelope {
                message: BeaconHeader {
                    slot: block.slot,
                    parent_root: block.parent_root.clone(),
                    state_root: block.state_root.clone(),
                },
            },
        }
    }
}

#[derive(Deserialize)]
pub struct HeaderEnvelope {
    pub data: BeaconHeaderSignedEnvelope,
}
