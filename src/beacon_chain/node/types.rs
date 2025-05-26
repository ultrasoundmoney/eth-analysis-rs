// src/beacon_chain/node/types.rs
// This file will contain data structures related to the beacon node API.

use std::fmt::Display;

use serde::Deserialize;

use crate::{execution_chain::BlockHash, json_codecs::i32_from_string, units::GweiNewtype};

// Assuming Slot needs to be imported from the parent (beacon_chain) or grandparent module
// Adjust this path as necessary based on your project structure.
// If Slot is in beacon_chain::units, it might be:
use crate::beacon_chain::{slot_from_string, Slot};
// Or if it's directly in beacon_chain:
// use crate::beacon_chain::Slot;

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
            Self::BlockRoot(block_root) => write!(f, "BlockId(BlockRoot({}))", block_root),
            Self::Finalized => write!(f, "BlockId(Finalized)"),
            Self::Genesis => write!(f, "BlockId(Genesis)"),
            Self::Head => write!(f, "BlockId(Head)"),
            Self::Slot(slot) => write!(f, "BlockId(Slot{})", slot),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DepositData {
    pub amount: GweiNewtype,
}

#[derive(Debug, Deserialize)]
pub struct Deposit {
    pub data: DepositData,
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
pub struct ExecutionDeposit {
    pub pubkey: String,
    pub withdrawal_credentials: String,
    pub amount: GweiNewtype,
    pub signature: String,
    #[serde(deserialize_with = "i32_from_string")]
    pub index: i32,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionWithdrawal {
    pub amount: GweiNewtype,
    pub source_address: String,
    pub validator_pubkey: String,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionPayload {
    pub block_hash: BlockHash,
    pub withdrawals: Option<Vec<Withdrawal>>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionRequests {
    pub deposits: Vec<ExecutionDeposit>,
    pub withdrawals: Vec<ExecutionWithdrawal>,
}

#[derive(Debug, Deserialize)]
pub struct BeaconBlockBody {
    pub deposits: Vec<Deposit>,
    pub execution_payload: Option<ExecutionPayload>,
    pub execution_requests: Option<ExecutionRequests>,
}

#[derive(Debug, Deserialize)]
pub struct BeaconBlock {
    pub body: BeaconBlockBody,
    pub parent_root: String,
    #[serde(deserialize_with = "slot_from_string")]
    pub slot: Slot,
    pub state_root: String,
}

impl BeaconBlock {
    pub fn block_hash(&self) -> Option<&BlockHash> {
        self.body
            .execution_payload
            .as_ref()
            .map(|payload| &payload.block_hash)
    }

    pub fn deposits(&self) -> Vec<&DepositData> {
        self.body
            .deposits
            .iter()
            .map(|deposit| &deposit.data)
            .collect()
    }

    pub fn execution_request_deposits(&self) -> Vec<&ExecutionDeposit> {
        self.body
            .execution_requests
            .as_ref()
            .map_or(vec![], |req| req.deposits.iter().collect())
    }

    pub fn execution_request_withdrawals(&self) -> Vec<&ExecutionWithdrawal> {
        self.body
            .execution_requests
            .as_ref()
            .map_or(vec![], |req| req.withdrawals.iter().collect())
    }

    pub fn withdrawals(&self) -> Option<&Vec<Withdrawal>> {
        self.body
            .execution_payload
            .as_ref()
            .and_then(|execution_payload| execution_payload.withdrawals.as_ref())
    }

    /// Calculates the sum of all deposits in the block.
    /// This includes both consensus layer (ETH1-style) deposits and
    /// execution request (EIP-6110) deposits.
    pub fn total_deposits_amount(&self) -> GweiNewtype {
        let consensus_deposits_sum = self
            .deposits()
            .iter()
            .fold(GweiNewtype(0), |sum, deposit| sum + deposit.amount);

        let execution_request_deposits_sum = self
            .execution_request_deposits()
            .iter()
            .fold(GweiNewtype(0), |sum, deposit| sum + deposit.amount);

        consensus_deposits_sum + execution_request_deposits_sum
    }
}

// Deserialization specific envelope structs (kept private to node module)
#[derive(Deserialize)]
pub struct BeaconBlockSignedEnvelope {
    message: BeaconBlock,
}

#[derive(Deserialize)]
pub struct BeaconBlockVersionedEnvelope {
    data: BeaconBlockSignedEnvelope,
}

impl From<BeaconBlockVersionedEnvelope> for BeaconBlock {
    fn from(envelope: BeaconBlockVersionedEnvelope) -> Self {
        envelope.data.message
    }
}

#[derive(Debug, Deserialize)]
pub struct ValidatorBalance {
    pub balance: GweiNewtype,
}

#[derive(Deserialize)]
pub(super) struct ValidatorBalancesEnvelope {
    pub data: Vec<ValidatorBalance>,
}

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
pub(super) struct HeaderEnvelope {
    pub data: BeaconHeaderSignedEnvelope,
}

#[derive(Debug, Deserialize)]
pub struct Validator {
    pub effective_balance: GweiNewtype,
}

#[derive(Debug, Deserialize)]
pub struct ValidatorEnvelope {
    pub status: String,
    pub validator: Validator,
}

impl ValidatorEnvelope {
    pub fn is_active(&self) -> bool {
        self.status == "active_ongoing"
            || self.status == "active_exiting"
            || self.status == "active_slashed"
    }

    pub fn effective_balance(&self) -> GweiNewtype {
        self.validator.effective_balance
    }
}

#[derive(Deserialize)]
pub(super) struct ValidatorsEnvelope {
    pub data: Vec<ValidatorEnvelope>,
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
pub struct PendingDeposit {
    pub amount: GweiNewtype,
}

#[derive(Deserialize)]
pub(super) struct PendingDepositsEnvelope {
    pub data: Vec<PendingDeposit>,
}

#[derive(Deserialize)]
pub(super) struct StateRootData {
    pub root: StateRoot,
}

#[derive(Deserialize)]
pub(super) struct StateRootEnvelope {
    pub data: StateRootData,
}

#[cfg(test)]
mod tests {
    use crate::beacon_chain::BeaconBlockBuilder;

    use super::*;

    #[test]
    fn get_deposit_sum_from_block_no_deposits_test() {
        let block = BeaconBlockBuilder::default().build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(0));
    }

    #[test]
    fn get_deposit_sum_from_block_only_consensus_deposits_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(30));
    }

    #[test]
    fn get_deposit_sum_from_block_only_execution_request_deposits_test() {
        let block = BeaconBlockBuilder::default()
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(20));
    }

    #[test]
    fn get_deposit_sum_from_block_both_deposit_types_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)]) // 30
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)]) // 20
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(50));
    }

    #[test]
    fn get_deposit_sum_from_block_empty_consensus_deposits_with_execution_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![])
            .execution_request_deposits(vec![GweiNewtype(5), GweiNewtype(15)])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(20));
    }

    #[test]
    fn get_deposit_sum_from_block_empty_execution_deposits_with_consensus_test() {
        let block = BeaconBlockBuilder::default()
            .deposits(vec![GweiNewtype(10), GweiNewtype(20)])
            .execution_request_deposits(vec![])
            .build();
        assert_eq!(block.total_deposits_amount(), GweiNewtype(30));
    }
}
