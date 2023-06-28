use crate::beacon_chain::GENESIS_PARENT_ROOT;

use super::*;

pub struct BeaconBlockBuilder {
    block_hash: Option<BlockHash>,
    deposits: Vec<GweiNewtype>,
    parent_root: BlockRoot,
    slot: Slot,
    state_root: StateRoot,
    withdrawals: Option<Vec<Withdrawal>>,
}

impl Default for BeaconBlockBuilder {
    fn default() -> Self {
        Self {
            deposits: vec![],
            parent_root: GENESIS_PARENT_ROOT.to_string(),
            slot: Slot(0),
            state_root: StateRoot::default(),
            block_hash: None,
            withdrawals: None,
        }
    }
}

impl BeaconBlockBuilder {
    pub fn block_hash(mut self, block_hash: &str) -> Self {
        self.block_hash = Some(block_hash.to_string());
        self
    }

    pub fn withdrawals(mut self, withdrawals: Vec<Withdrawal>) -> Self {
        self.withdrawals = Some(withdrawals);
        self
    }

    pub fn slot(mut self, slot: Slot) -> Self {
        self.slot = slot;
        self
    }

    pub fn build(self) -> BeaconBlock {
        let deposits = self
            .deposits
            .into_iter()
            .map(|deposit| Deposit {
                data: DepositData { amount: deposit },
            })
            .collect();

        let execution_payload = self.block_hash.map(|block_hash| ExecutionPayload {
            block_hash,
            withdrawals: self.withdrawals,
        });

        BeaconBlock {
            body: BeaconBlockBody {
                deposits,
                execution_payload,
            },
            parent_root: self.parent_root,
            slot: self.slot,
            state_root: self.state_root,
        }
    }
}

impl From<&BeaconHeaderSignedEnvelope> for BeaconBlockBuilder {
    fn from(header: &BeaconHeaderSignedEnvelope) -> Self {
        Self {
            block_hash: None,
            deposits: vec![],
            parent_root: header.parent_root(),
            slot: header.slot(),
            state_root: header.state_root(),
            withdrawals: None,
        }
    }
}

pub struct BeaconHeaderSignedEnvelopeBuilder {
    block_root: BlockRoot,
    parent_root: BlockRoot,
    slot: Slot,
    state_root: StateRoot,
}

impl BeaconHeaderSignedEnvelopeBuilder {
    pub fn new(test_id: &str) -> Self {
        let state_root = format!("0x{test_id}_state_root");
        let block_root = format!("0x{test_id}_block_root");

        Self {
            block_root,
            state_root,
            slot: Slot(0),
            parent_root: GENESIS_PARENT_ROOT.to_owned(),
        }
    }

    pub fn slot(mut self, slot: &Slot) -> Self {
        self.slot = *slot;
        self
    }

    pub fn parent_header(mut self, parent_header: &BeaconHeaderSignedEnvelope) -> Self {
        self.slot = parent_header.slot() + 1;
        self.parent_root = parent_header.root.to_owned();
        self
    }

    pub fn build(self) -> BeaconHeaderSignedEnvelope {
        BeaconHeaderSignedEnvelope {
            root: self.block_root,
            header: BeaconHeaderEnvelope {
                message: BeaconHeader {
                    slot: self.slot,
                    parent_root: self.parent_root,
                    state_root: self.state_root,
                },
            },
        }
    }
}
