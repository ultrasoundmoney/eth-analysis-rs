use chrono::Utc;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::config;
use crate::eth_units::{from_u32_string, GweiAmount};
use crate::performance::TimedExt;

use super::{beacon_time, Slot};

#[derive(Debug)]
enum BlockId {
    BlockRoot(String),
    Finalized,
    #[allow(dead_code)]
    Genesis,
    #[allow(dead_code)]
    Head,
    #[allow(dead_code)]
    Slot(u32),
}

#[derive(Debug, Deserialize)]
pub struct Deposit {
    pub amount: GweiAmount,
}

#[derive(Debug, Deserialize)]
pub struct DepositEnvelope {
    pub data: Deposit,
}

#[derive(Debug, Deserialize)]
pub struct Body {
    pub deposits: Vec<DepositEnvelope>,
}

#[derive(Debug, Deserialize)]
pub struct BeaconBlock {
    pub body: Body,
    pub parent_root: String,
    #[serde(deserialize_with = "from_u32_string")]
    pub slot: u32,
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

    format!(
        "{}/eth/v2/beacon/blocks/{}",
        config::get_beacon_url(),
        block_id_text
    )
}

#[derive(Debug, Deserialize)]
struct StateRoot {
    root: String,
}

#[derive(Debug, Deserialize)]
struct StateRootEnvelope {
    data: StateRoot,
}

fn make_state_root_url(slot: &u32) -> String {
    format!(
        "{}/eth/v1/beacon/states/{}/root",
        config::get_beacon_url(),
        slot.to_string()
    )
}

#[derive(Debug, Deserialize)]
pub struct ValidatorBalance {
    pub balance: GweiAmount,
}

#[derive(Debug, Deserialize)]
struct ValidatorBalancesEnvelope {
    data: Vec<ValidatorBalance>,
}

fn make_validator_balances_by_state_url(state_root: &str) -> String {
    format!(
        "{}/eth/v1/beacon/states/{}/validator_balances",
        config::get_beacon_url(),
        state_root
    )
}

#[derive(Debug, Deserialize)]
pub struct BeaconHeader {
    #[serde(deserialize_with = "from_u32_string")]
    pub slot: u32,
    pub parent_root: String,
    pub state_root: String,
}

#[derive(Debug, Deserialize)]
pub struct BeaconHeaderEnvelope {
    pub message: BeaconHeader,
}

#[derive(Debug, Deserialize)]
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

    format!(
        "{}/eth/v1/beacon/headers/{}",
        config::get_beacon_url(),
        block_id_text
    )
}

fn make_validators_by_state_url(state_root: &str) -> String {
    format!(
        "{}/eth/v1/beacon/states/{}/validators",
        config::get_beacon_url(),
        state_root
    )
}

#[derive(Debug, Deserialize)]
pub struct Validator {
    pub effective_balance: GweiAmount,
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
        config::get_beacon_url(),
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

    async fn get_block(&self, block_id: &BlockId) -> reqwest::Result<BeaconBlock> {
        let url = make_blocks_url(block_id);

        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<BeaconBlockVersionedEnvelope>()
            .await
            .map(|envelope| envelope.data.message.into())
    }

    #[allow(dead_code)]
    pub async fn get_block_by_slot(&self, slot: &Slot) -> reqwest::Result<BeaconBlock> {
        self.get_block(&BlockId::Slot(*slot)).await
    }

    pub async fn get_block_by_block_root(&self, block_root: &str) -> reqwest::Result<BeaconBlock> {
        self.get_block(&BlockId::BlockRoot(block_root.to_string()))
            .await
    }

    pub async fn get_last_finalized_block(&self) -> reqwest::Result<BeaconBlock> {
        self.get_block(&BlockId::Finalized).await
    }

    #[allow(dead_code)]
    pub async fn get_last_block(&self) -> reqwest::Result<BeaconBlock> {
        self.get_block(&BlockId::Head).await
    }

    pub async fn get_state_root_by_slot(&self, slot: &u32) -> reqwest::Result<String> {
        let url = make_state_root_url(slot);

        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<StateRootEnvelope>()
            .await
            .map(|envelope| envelope.data.root)
    }

    pub async fn get_validator_balances(
        &self,
        state_root: &str,
    ) -> reqwest::Result<Vec<ValidatorBalance>> {
        let url = make_validator_balances_by_state_url(state_root);

        self.client
            .get(&url)
            .send()
            .timed("get validator balances")
            .await?
            .error_for_status()?
            .json::<ValidatorBalancesEnvelope>()
            .await
            .map(|envelope| envelope.data)
    }

    async fn get_header(
        &self,
        block_id: &BlockId,
    ) -> reqwest::Result<Option<BeaconHeaderSignedEnvelope>> {
        let url = make_header_by_block_id_url(block_id);

        let res = self.client.get(&url).send().await?.error_for_status();

        match res {
            Ok(res) => res
                .json::<HeaderEnvelope>()
                .await
                .map(|envelope| Some(envelope.data)),
            Err(res) => match res.status() {
                None => Err(res),
                Some(status) if status == StatusCode::NOT_FOUND => Ok(None),
                Some(_) => Err(res),
            },
        }
    }

    pub async fn get_header_by_slot(
        &self,
        slot: &u32,
    ) -> reqwest::Result<Option<BeaconHeaderSignedEnvelope>> {
        let slot_timestamp = beacon_time::get_date_time_from_slot(slot);
        if slot_timestamp > Utc::now() {
            // If we would return this case at None, the caller would be unable to distinguish
            // between slots without blocks, and slots that are in the future.
            panic!(
                "tried to get header by slot for slot: {}, but slot is in the future!",
                &slot
            )
        }

        self.get_header(&BlockId::Slot(*slot)).await
    }

    pub async fn get_header_by_hash(
        &self,
        block_root: &str,
    ) -> reqwest::Result<Option<BeaconHeaderSignedEnvelope>> {
        self.get_header(&BlockId::BlockRoot(block_root.to_string()))
            .await
    }

    pub async fn get_last_header(&self) -> reqwest::Result<BeaconHeaderSignedEnvelope> {
        self.get_header(&BlockId::Head)
            .await
            .map(|envelope| envelope.expect("a head to always exist on-chain"))
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
        let header = beacon_node.get_last_header().await.unwrap();
    }
}
