//! Functions that know how to communicate with  a BeaconChain node to get various pieces of data.
//! Currently, many calls taking a state_root as input do not acknowledge that a state_root may
//! disappear at any time. They should be updated to do so.
use std::fmt::Display;

use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::{
    execution_chain::BlockHash, json_codecs::i32_from_string, performance::TimedExt,
    units::GweiNewtype,
};

use super::{slot_from_string, Slot, BEACON_URL};

#[derive(Debug)]
enum BlockId {
    BlockRoot(String),
    #[allow(dead_code)]
    Finalized,
    #[allow(dead_code)]
    Genesis,
    Head,
    Slot(Slot),
}

impl From<&Slot> for BlockId {
    fn from(slot: &Slot) -> Self {
        BlockId::Slot(*slot)
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
pub struct ExecutionPayload {
    pub block_hash: BlockHash,
    pub withdrawals: Option<Vec<Withdrawal>>,
}

#[derive(Debug, Deserialize)]
pub struct BeaconBlockBody {
    pub deposits: Vec<Deposit>,
    pub execution_payload: Option<ExecutionPayload>,
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

    pub fn withdrawals(&self) -> Option<&Vec<Withdrawal>> {
        self.body
            .execution_payload
            .as_ref()
            .and_then(|execution_payload| execution_payload.withdrawals.as_ref())
    }
}

/// A signed envelope.
#[derive(Deserialize)]
struct BeaconBlockSignedEnvelope {
    message: BeaconBlock,
}

/// A versioned envelope.
#[derive(Deserialize)]
struct BeaconBlockVersionedEnvelope {
    data: BeaconBlockSignedEnvelope,
}

fn make_blocks_url(block_id: &BlockId) -> String {
    let block_id_text = match block_id {
        BlockId::BlockRoot(str) => str.to_owned(),
        BlockId::Finalized => "finalized".to_string(),
        BlockId::Genesis => "genesis".to_string(),
        BlockId::Head => "head".to_string(),
        BlockId::Slot(slot) => slot.to_string(),
    };

    format!("{}/eth/v2/beacon/blocks/{}", *BEACON_URL, block_id_text)
}

pub type StateRoot = String;

#[derive(Debug, Deserialize)]
struct StateRootSecondEnvelope {
    root: StateRoot,
}

#[derive(Debug, Deserialize)]
struct StateRootFirstEnvelope {
    data: StateRootSecondEnvelope,
}

fn make_state_root_url(slot: &Slot) -> String {
    format!("{}/eth/v1/beacon/states/{}/root", *BEACON_URL, slot)
}

#[derive(Debug, Deserialize)]
pub struct ValidatorBalance {
    pub balance: GweiNewtype,
}

#[derive(Debug, Deserialize)]
struct ValidatorBalancesEnvelope {
    data: Vec<ValidatorBalance>,
}

fn make_validator_balances_by_state_url(state_root: &str) -> String {
    format!(
        "{}/eth/v1/beacon/states/{}/validator_balances",
        *BEACON_URL, state_root
    )
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct BeaconHeader {
    #[serde(deserialize_with = "slot_from_string")]
    pub slot: Slot,
    pub parent_root: BlockRoot,
    pub state_root: StateRoot,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct BeaconHeaderEnvelope {
    pub message: BeaconHeader,
}

/// Keccak hash of a beacon block.
pub type BlockRoot = String;

#[derive(Debug, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Deserialize)]
struct HeaderEnvelope {
    data: BeaconHeaderSignedEnvelope,
}

fn make_header_by_block_id_url(block_id: &BlockId) -> String {
    let block_id_text = match block_id {
        BlockId::BlockRoot(str) => str.to_owned(),
        BlockId::Finalized => "finalized".to_string(),
        BlockId::Genesis => "genesis".to_string(),
        BlockId::Head => "head".to_string(),
        BlockId::Slot(slot) => slot.to_string(),
    };

    format!("{}/eth/v1/beacon/headers/{}", *BEACON_URL, block_id_text)
}

fn make_validators_by_state_url(state_root: &str) -> String {
    format!(
        "{}/eth/v1/beacon/states/{}/validators",
        *BEACON_URL, state_root
    )
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

#[derive(Debug, Deserialize)]
struct ValidatorsEnvelope {
    data: Vec<ValidatorEnvelope>,
}

fn make_finality_checkpoint_url() -> String {
    format!(
        "{}/eth/v1/beacon/states/head/finality_checkpoints",
        *BEACON_URL,
    )
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
struct FinalityCheckpoints {
    finalized: FinalityCheckpoint,
}

#[derive(Deserialize)]
struct CheckpointEnvelope {
    data: FinalityCheckpoints,
}

pub struct BeaconNode {
    client: reqwest::Client,
}

impl Default for BeaconNode {
    fn default() -> Self {
        BeaconNode::new()
    }
}

impl BeaconNode {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn get_block(&self, block_id: &BlockId) -> Result<Option<BeaconBlock>> {
        let url = make_blocks_url(block_id);

        let res = self.client.get(&url).send().await?;

        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => {
                let block = res
                    .json::<BeaconBlockVersionedEnvelope>()
                    .await
                    .map(|envelope| envelope.data.message)?;
                Ok(Some(block))
            }
            status => Err(anyhow!(
                "failed to fetch block by block_id. block_id = {} status = {} url = {}",
                block_id,
                status,
                res.url()
            )),
        }
    }

    #[allow(dead_code)]
    pub async fn get_block_by_slot(&self, slot: &Slot) -> Result<Option<BeaconBlock>> {
        self.get_block(&slot.into()).await
    }

    pub async fn get_block_by_block_root(&self, block_root: &str) -> Result<Option<BeaconBlock>> {
        self.get_block(&BlockId::BlockRoot(block_root.to_string()))
            .timed("get_block_by_block_root")
            .await
    }

    #[allow(dead_code)]
    pub async fn get_last_finalized_block(&self) -> Result<BeaconBlock> {
        let block = self
            .get_block(&BlockId::Finalized)
            .await?
            .expect("expect a finalized block to always be available");

        Ok(block)
    }

    #[allow(dead_code)]
    pub async fn get_last_block(&self) -> Result<BeaconBlock> {
        let block = self
            .get_block(&BlockId::Head)
            .await?
            .expect("expect head block to always be available");

        Ok(block)
    }

    pub async fn get_state_root_by_slot(&self, slot: &Slot) -> Result<Option<String>> {
        let url = make_state_root_url(slot);

        let res = self.client.get(&url).send().await?;

        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => {
                let state_root = res
                    .json::<StateRootFirstEnvelope>()
                    .await
                    .map(|envelope| envelope.data.root)?;
                Ok(Some(state_root))
            }
            status => Err(anyhow!(
                "failed to fetch state_root by slot. slot = {} status = {} url = {}",
                slot,
                status,
                res.url()
            )),
        }
    }

    pub async fn get_validator_balances(
        &self,
        state_root: &str,
    ) -> Result<Option<Vec<ValidatorBalance>>> {
        let url = make_validator_balances_by_state_url(state_root);

        let res = self
            .client
            .get(&url)
            .send()
            .timed("get_validator_balances")
            .await?;

        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => {
                let envelope = res.json::<ValidatorBalancesEnvelope>().await?;
                Ok(Some(envelope.data))
            }
            status => Err(anyhow!(
                "failed to fetch validator balances by state_root. state_root = {} status = {} url = {}",
                state_root,
                status,
                res.url()
            )),
        }
    }

    async fn get_header(&self, block_id: &BlockId) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        let url = make_header_by_block_id_url(block_id);

        let res = self.client.get(&url).send().await?;

        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => {
                let envelope = res.json::<HeaderEnvelope>().await?;
                Ok(Some(envelope.data))
            }
            status => Err(anyhow!(
                "failed to fetch header by block id. status = {} url = {}",
                status,
                res.url()
            )),
        }
    }

    pub async fn get_header_by_slot(
        &self,
        slot: &Slot,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        let slot_timestamp = slot.date_time();
        if slot_timestamp > Utc::now() {
            return Err(anyhow!(
                "tried to fetch slot: {}, with expected timestamp: {}, but can't fetch slots from the future",
                &slot,
                slot_timestamp
            ));
        }

        let block_id: BlockId = slot.into();
        self.get_header(&block_id).await
    }

    #[allow(dead_code)]
    pub async fn get_header_by_block_root(
        &self,
        block_root: &str,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        self.get_header(&BlockId::BlockRoot(block_root.to_string()))
            .await
    }

    /// Convenience fn that really gets the header by slot, but checks for us the state_root is as expected.
    #[allow(dead_code)]
    pub async fn get_header_by_state_root(
        &self,
        state_root: &str,
        slot: &Slot,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        let block_id: BlockId = slot.into();
        let header = self.get_header(&block_id).await?;

        match header {
            None => Ok(None),
            Some(header) => {
                if header.header.message.state_root == state_root {
                    Ok(Some(header))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn get_last_header(&self) -> Result<BeaconHeaderSignedEnvelope> {
        self.get_header(&BlockId::Head)
            .await
            .map(|header| header.expect("expect beacon chain head to always point to a block"))
    }

    #[allow(dead_code)]
    pub async fn get_last_finality_checkpoint(&self) -> reqwest::Result<FinalityCheckpoint> {
        let url = make_finality_checkpoint_url();
        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<CheckpointEnvelope>()
            .await
            .map(|envelope| envelope.data.finalized)
    }

    pub async fn get_validators_by_state(
        &self,
        state_root: &str,
    ) -> reqwest::Result<Vec<ValidatorEnvelope>> {
        let url = make_validators_by_state_url(state_root);
        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<ValidatorsEnvelope>()
            .await
            .map(|envelope| envelope.data)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{fs::File, io::BufReader};

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

    #[test]
    fn decode_beacon_block_versioned_envelope() {
        let file =
            File::open("src/beacon_chain/data_samples/beacon_block_versioned_envelope_1229.json")
                .unwrap();
        let reader = BufReader::new(file);

        serde_json::from_reader::<BufReader<File>, BeaconBlockVersionedEnvelope>(reader).unwrap();
    }

    #[test]
    fn decode_validator_balances() {
        let file =
            File::open("src/beacon_chain/data_samples/validator_balances_1229.json").unwrap();
        let reader = BufReader::new(file);

        serde_json::from_reader::<BufReader<File>, ValidatorBalancesEnvelope>(reader).unwrap();
    }

    #[test]
    fn decode_header_envelope() {
        let file = File::open("src/beacon_chain/data_samples/header_1229.json").unwrap();
        let reader = BufReader::new(file);

        serde_json::from_reader::<BufReader<File>, HeaderEnvelope>(reader).unwrap();
    }

    #[test]
    fn decode_validators() {
        let file = File::open("src/beacon_chain/data_samples/validators_1229.json").unwrap();
        let reader = BufReader::new(file);

        serde_json::from_reader::<BufReader<File>, ValidatorsEnvelope>(reader).unwrap();
    }

    const SLOT_1229: Slot = Slot(1229);
    const BLOCK_ROOT_1229: &str =
        "0x35376f52006e12b7e9247b457277fb34f6bd32d83a651e24c2669467607e0778";
    const STATE_ROOT_1229: &str =
        "0x36cb7e3d4585fb90a4ed17a0139de34a08b8354d1a7a054dbe3e4d8a0b93e625";

    #[tokio::test]
    async fn last_finalized_block_test() {
        let beacon_node = BeaconNode::new();
        beacon_node.get_last_finalized_block().await.unwrap();
    }

    #[tokio::test]
    async fn block_by_root_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_block_by_block_root(BLOCK_ROOT_1229)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn header_by_slot_test() {
        let beacon_node = BeaconNode::new();
        beacon_node.get_header_by_slot(&SLOT_1229).await.unwrap();
    }

    #[tokio::test]
    async fn get_none_header_test() {
        let beacon_node = BeaconNode::new();
        let header = beacon_node
            .get_header_by_block_root(
                "0x602d010f6e616e56026e514d6099730499ad9f635dc6f4581bd6d3ac744fbd8d",
            )
            .await
            .unwrap();
        assert_eq!(header, None);
    }

    #[tokio::test]
    async fn state_root_by_slot_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_state_root_by_slot(&SLOT_1229)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn validator_balances_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_validator_balances(STATE_ROOT_1229)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn validators_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_validators_by_state(STATE_ROOT_1229)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_last_finality_checkpoint_test() {
        let beacon_node = BeaconNode::new();
        beacon_node.get_last_finality_checkpoint().await.unwrap();
    }

    #[tokio::test]
    async fn get_header_by_slot_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_header_by_slot(&Slot(4_000_000))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_header_by_hash_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_header_by_block_root(
                "0x09df4a49850ec0c878ba2443f60f5fa6b473abcb14d222915fc44b17885ed8a4",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_last_head_test() {
        let beacon_node = BeaconNode::new();
        beacon_node.get_last_header().await.unwrap();
    }

    #[tokio::test]
    async fn get_header_by_state_root() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_header_by_state_root(
                "0x2e3df8cebeb66206d2b26e6a36a63105f78c22c3ab4b7abaa11b4056b4519588",
                &Slot(5000000),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_deposits() {
        let beacon_node = BeaconNode::new();
        let block = beacon_node
            .get_block_by_slot(&Slot(1229))
            .await
            .unwrap()
            .unwrap();
        let deposits = block.deposits();
        assert_eq!(deposits.len(), 16);
    }

    #[tokio::test]
    async fn get_withdrawals() {
        let beacon_node = BeaconNode::new();
        let block = beacon_node
            .get_block_by_slot(&Slot(6212480))
            .await
            .unwrap()
            .unwrap();
        let withdrawals = block.withdrawals().unwrap();
        assert_eq!(withdrawals.len(), 16);
    }
}
