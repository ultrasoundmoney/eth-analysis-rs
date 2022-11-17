//! Functions that know how to communicate with  a BeaconChain node to get various pieces of data.
//! Currently, many calls taking a state_root as input do not acknowledge that a state_root may
//! disappear at any time. They should be updated to do so.
use std::fmt::Display;

use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::{eth_units::GweiNewtype, json_codecs::from_u32_string, performance::TimedExt};

use super::{beacon_time, Slot, BEACON_URL};

#[derive(Debug)]
enum BlockId {
    BlockRoot(String),
    #[allow(dead_code)]
    Finalized,
    #[allow(dead_code)]
    Genesis,
    Head,
    Slot(u32),
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockRoot(block_root) => write!(f, "BlockId(BlockRoot) {}", block_root),
            Self::Finalized => write!(f, "BlockId(Finalized)"),
            Self::Genesis => write!(f, "BlockId(Genesis)"),
            Self::Head => write!(f, "BlockId(Head)"),
            Self::Slot(slot) => write!(f, "BlockId(slot) {}", slot),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Deposit {
    pub amount: GweiNewtype,
}

#[derive(Debug, Deserialize)]
pub struct DepositEnvelope {
    pub data: Deposit,
}

// #[derive(Debug, Deserialize)]
// pub struct ExecutionPayload {
//     #[serde(deserialize_with = "from_u32_string")]
//     block_number: BlockNumber,
//     block_hash: String,
// }

#[derive(Debug, Deserialize)]
pub struct Body {
    pub deposits: Vec<DepositEnvelope>,
    // pub execution_payload: Option<ExecutionPayload>,
}

#[derive(Debug, Deserialize)]
pub struct BeaconBlock {
    pub body: Body,
    pub parent_root: String,
    #[serde(deserialize_with = "from_u32_string")]
    pub slot: Slot,
    pub state_root: String,
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

fn make_state_root_url(slot: &u32) -> String {
    format!(
        "{}/eth/v1/beacon/states/{}/root",
        *BEACON_URL,
        slot.to_string()
    )
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

#[derive(Debug, Deserialize, PartialEq)]
pub struct BeaconHeader {
    #[serde(deserialize_with = "from_u32_string")]
    pub slot: Slot,
    pub parent_root: String,
    pub state_root: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct BeaconHeaderEnvelope {
    pub message: BeaconHeader,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct BeaconHeaderSignedEnvelope {
    /// block_root
    pub root: String,
    pub header: BeaconHeaderEnvelope,
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
    pub validator: Validator,
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
    #[serde(deserialize_with = "from_u32_string")]
    epoch: u32,
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
                    .map(|envelope| envelope.data.message.into())?;
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
        self.get_block(&BlockId::Slot(*slot)).await
    }

    pub async fn get_block_by_block_root(&self, block_root: &str) -> Result<Option<BeaconBlock>> {
        self.get_block(&BlockId::BlockRoot(block_root.to_string()))
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

    pub async fn get_state_root_by_slot(&self, slot: &u32) -> Result<Option<String>> {
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
            .timed("get validator balances")
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
        slot: &u32,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        let slot_timestamp = beacon_time::get_date_time_from_slot(slot);
        if slot_timestamp > Utc::now() {
            return Err(anyhow!(
                "tried to fetch slot: {}, with expected timestamp: {}, but can't fetch slots from the future",
                &slot,
                slot_timestamp
            ));
        }

        self.get_header(&BlockId::Slot(*slot)).await
    }

    pub async fn get_header_by_hash(
        &self,
        block_root: &str,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        self.get_header(&BlockId::BlockRoot(block_root.to_string()))
            .await
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
    ) -> reqwest::Result<Vec<Validator>> {
        let url = make_validators_by_state_url(state_root);
        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<ValidatorsEnvelope>()
            .await
            .map(|validators_envelope| {
                validators_envelope
                    .data
                    .into_iter()
                    .map(|validator_envelope| validator_envelope.validator)
                    .collect()
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::BufReader};

    use super::*;

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
        let file = File::open("src/beacon_chain/data_samples/validator_balaces_1229.json").unwrap();
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

    const SLOT_1229: u32 = 1229;
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
            .get_header_by_hash(
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
        beacon_node.get_header_by_slot(&4_000_000).await.unwrap();
    }

    #[tokio::test]
    async fn get_header_by_hash_test() {
        let beacon_node = BeaconNode::new();
        beacon_node
            .get_header_by_hash(
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
}
