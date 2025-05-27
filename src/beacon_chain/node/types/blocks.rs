// src/beacon_chain/node/types/blocks.rs
use super::deposits::{Deposit, DepositData, ExecutionDeposit};
use super::{BlockRoot, ExecutionWithdrawal, StateRoot, Withdrawal}; // From types/mod.rs
use crate::beacon_chain::{slot_from_string, Slot};
use crate::execution_chain::BlockHash;
use crate::units::GweiNewtype;
use serde::Deserialize; // From types/deposits.rs

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
    pub parent_root: BlockRoot,
    #[serde(deserialize_with = "slot_from_string")]
    pub slot: Slot,
    pub state_root: StateRoot,
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
