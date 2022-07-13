use reqwest::StatusCode;
use serde::Deserialize;

use crate::config;
use crate::decoders::from_u32_string;
use crate::eth_units::GweiAmount;
use crate::performance::LifetimeMeasure;

enum BlockId {
    #[allow(dead_code)]
    Head,
    Finalized,
    #[allow(dead_code)]
    Genesis,
    BlockRoot(String),
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
    let block_id_str = match block_id {
        BlockId::Head => String::from("head"),
        BlockId::Finalized => String::from("finalized"),
        BlockId::Genesis => String::from("genesis"),
        BlockId::BlockRoot(str) => str.to_string(),
    };

    format!(
        "{}/eth/v2/beacon/blocks/{}",
        config::get_beacon_url(),
        block_id_str
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

fn make_header_by_slot_url(slot: &u32) -> String {
    format!(
        "{}/eth/v1/beacon/headers/{}",
        config::get_beacon_url(),
        slot.to_string()
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

    pub async fn get_block_by_root(&self, block_root: &str) -> reqwest::Result<BeaconBlock> {
        let url = make_blocks_url(&BlockId::BlockRoot(block_root.to_string()));

        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<BeaconBlockVersionedEnvelope>()
            .await
            .map(|envelope| envelope.data.message.into())
    }

    pub async fn get_last_finalized_block(&self) -> reqwest::Result<BeaconBlock> {
        let url = make_blocks_url(&BlockId::Finalized);

        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<BeaconBlockVersionedEnvelope>()
            .await
            .map(|envelope| envelope.data.message.into())
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
            .await?
            .error_for_status()?
            .json::<ValidatorBalancesEnvelope>()
            .await
            .map(|envelope| envelope.data)
    }

    /// This function returns nothing both when a slot does not exist at all, and a slot does exist but
    /// is blockless. In other words, don't use this function alone to tell if a given slot has a
    /// block, check the slot exists first.
    pub async fn get_header_by_slot(
        &self,
        slot: &u32,
    ) -> reqwest::Result<Option<BeaconHeaderSignedEnvelope>> {
        let _m1 = LifetimeMeasure::log_lifetime("get header by slot");
        let url = make_header_by_slot_url(slot);

        let res = self.client.get(&url).send().await?.error_for_status();

        // No header at this slot, either this slot doesn't exist or there is no header at this slot. We assume the slot exists but is headerless.
        match res {
            Ok(res) => res
                .json::<HeaderEnvelope>()
                .await
                .map(|envelope| Some(envelope.data)),
            Err(res) => match res.status() {
                None => Err(res),
                Some(status) => match status {
                    StatusCode::NOT_FOUND => Ok(None),
                    _ => Err(res),
                },
            },
        }
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
            .get_block_by_root(BLOCK_ROOT_1229)
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
}
