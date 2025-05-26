//! Functions that know how to communicate with  a BeaconChain node to get various pieces of data.
//! Currently, many calls taking a state_root as input do not acknowledge that a state_root may
//! disappear at any time. They should be updated to do so.
pub mod test_utils;
pub mod types;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use mockall::automock;
use reqwest::StatusCode;
use tracing::instrument;

use crate::{
    env::ENV_CONFIG, execution_chain::BlockHash, performance::TimedExt, units::GweiNewtype,
};

use super::Slot;

pub use types::*;

fn make_blocks_url(block_id: &BlockId) -> String {
    let block_id_text = match block_id {
        BlockId::BlockRoot(str) => str.to_owned(),
        BlockId::Finalized => "finalized".to_string(),
        BlockId::Genesis => "genesis".to_string(),
        BlockId::Head => "head".to_string(),
        BlockId::Slot(slot) => slot.to_string(),
    };
    let beacon_url = ENV_CONFIG
        .beacon_url
        .as_ref()
        .expect("BEACON_URL is required in env to fetch blocks");

    format!("{beacon_url}/eth/v2/beacon/blocks/{}", block_id_text)
}

fn make_header_by_block_id_url(block_id: &BlockId) -> String {
    let block_id_text = match block_id {
        BlockId::BlockRoot(str) => str.to_owned(),
        BlockId::Finalized => "finalized".to_string(),
        BlockId::Genesis => "genesis".to_string(),
        BlockId::Head => "head".to_string(),
        BlockId::Slot(slot) => slot.to_string(),
    };
    let beacon_url = ENV_CONFIG
        .beacon_url
        .as_ref()
        .expect("BEACON_URL is required in env to fetch headers");
    format!("{beacon_url}/eth/v1/beacon/headers/{}", block_id_text)
}

#[derive(Clone, Debug)]
pub struct BeaconNodeHttp {
    client: reqwest::Client,
    url: String,
}

impl Default for BeaconNodeHttp {
    fn default() -> Self {
        BeaconNodeHttp::new("localhost:5052")
    }
}

#[automock]
#[async_trait]
pub trait BeaconNode {
    async fn get_block_by_block_root(&self, block_root: &str) -> Result<Option<BeaconBlock>>;
    async fn get_block_by_slot(&self, slot: Slot) -> Result<Option<BeaconBlock>>;
    async fn get_header(&self, block_id: &BlockId) -> Result<Option<BeaconHeaderSignedEnvelope>>;
    async fn get_header_by_block_root(
        &self,
        block_root: &str,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>>;
    async fn get_header_by_slot(&self, slot: Slot) -> Result<Option<BeaconHeaderSignedEnvelope>>;
    async fn get_header_by_state_root(
        &self,
        state_root: &str,
        slot: Slot,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>>;
    async fn get_last_block(&self) -> Result<BeaconBlock>;
    async fn get_last_finality_checkpoint(&self) -> Result<FinalityCheckpoint>;
    async fn get_last_finalized_block(&self) -> Result<BeaconBlock>;
    async fn get_last_header(&self) -> Result<BeaconHeaderSignedEnvelope>;
    async fn get_state_root_by_slot(&self, slot: Slot) -> Result<Option<StateRoot>>;
    async fn get_validator_balances(
        &self,
        state_root: &str,
    ) -> Result<Option<Vec<ValidatorBalance>>>;
    async fn get_validators_by_state(&self, state_root: &str) -> Result<Vec<ValidatorEnvelope>>;
    async fn get_validator_balances_by_slot(
        &self,
        slot: Slot,
    ) -> Result<Option<Vec<ValidatorBalance>>>;
    async fn get_pending_deposits_sum(&self, state_root: &str) -> Result<Option<GweiNewtype>>;
}

impl BeaconNodeHttp {
    pub fn new(url: &str) -> Self {
        BeaconNodeHttp {
            client: reqwest::Client::new(),
            url: url.to_string(),
        }
    }

    pub fn new_from_env() -> Self {
        let beacon_url = ENV_CONFIG
            .beacon_url
            .as_ref()
            .expect("BEACON_URL is required in env to fetch blocks");
        BeaconNodeHttp::new(beacon_url)
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
                    .map(|envelope| envelope.into())?;
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
}

#[async_trait]
impl BeaconNode for BeaconNodeHttp {
    #[allow(dead_code)]
    async fn get_block_by_slot(&self, slot: Slot) -> Result<Option<BeaconBlock>> {
        self.get_block(&slot.into()).await
    }

    #[instrument(skip(self))]
    async fn get_block_by_block_root(&self, block_root: &str) -> Result<Option<BeaconBlock>> {
        self.get_block(&BlockId::BlockRoot(block_root.to_string()))
            .timed("get_block_by_block_root")
            .await
    }

    #[allow(dead_code)]
    async fn get_last_finalized_block(&self) -> Result<BeaconBlock> {
        let block = self
            .get_block(&BlockId::Finalized)
            .await?
            .expect("expect a finalized block to always be available");

        Ok(block)
    }

    #[allow(dead_code)]
    async fn get_last_block(&self) -> Result<BeaconBlock> {
        let block = self
            .get_block(&BlockId::Head)
            .await?
            .expect("expect head block to always be available");

        Ok(block)
    }

    async fn get_state_root_by_slot(&self, slot: Slot) -> Result<Option<StateRoot>> {
        let url = format!("{}/eth/v1/beacon/states/{}/root", self.url, slot.0);
        let res = self.client.get(&url).send().await?;
        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => Ok(Some(res.json::<StateRootEnvelope>().await?.data.root)),
            status => Err(anyhow!(
                "failed to fetch state root by slot. slot = {} status = {} url = {}",
                slot.0,
                status,
                res.url()
            )),
        }
    }

    #[instrument(skip(self))]
    async fn get_validator_balances(
        &self,
        state_root: &str,
    ) -> Result<Option<Vec<ValidatorBalance>>> {
        let url = format!(
            "{}/eth/v1/beacon/states/{}/validator_balances",
            self.url, state_root
        );
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

    async fn get_header_by_slot(&self, slot: Slot) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        let slot_timestamp = slot.date_time();
        if slot_timestamp > Utc::now() {
            return Err(anyhow!(
                "tried to fetch slot: {}, with expected timestamp: {}, but can't fetch slots from the future",
                slot,
                slot_timestamp
            ));
        }

        let block_id: BlockId = slot.into();
        self.get_header(&block_id).await
    }

    #[allow(dead_code)]
    async fn get_header_by_block_root(
        &self,
        block_root: &str,
    ) -> Result<Option<BeaconHeaderSignedEnvelope>> {
        self.get_header(&BlockId::BlockRoot(block_root.to_string()))
            .await
    }

    /// Convenience fn that really gets the header by slot, but checks for us the state_root is as expected.
    #[allow(dead_code)]
    async fn get_header_by_state_root(
        &self,
        state_root: &str,
        slot: Slot,
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

    async fn get_last_header(&self) -> Result<BeaconHeaderSignedEnvelope> {
        self.get_header(&BlockId::Head)
            .await
            .map(|header| header.expect("expect beacon chain head to always point to a block"))
    }

    #[allow(dead_code)]
    async fn get_last_finality_checkpoint(&self) -> Result<FinalityCheckpoint> {
        let url = format!(
            "{}/eth/v1/beacon/states/head/finality_checkpoints",
            self.url
        );
        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<CheckpointEnvelope>()
            .await
            .map(|envelope| envelope.data.finalized)
            .map_err(Into::into)
    }

    async fn get_validators_by_state(&self, state_root: &str) -> Result<Vec<ValidatorEnvelope>> {
        let url = format!(
            "{}/eth/v1/beacon/states/{}/validators",
            self.url, state_root
        );
        self.client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<ValidatorsEnvelope>()
            .await
            .map(|envelope| envelope.data)
            .map_err(Into::into)
    }

    async fn get_validator_balances_by_slot(
        &self,
        slot: Slot,
    ) -> Result<Option<Vec<ValidatorBalance>>> {
        let beacon_url = ENV_CONFIG
            .beacon_url
            .as_ref()
            .expect("BEACON_URL is required in env to fetch validator balances");
        let url = format!(
            "{}/eth/v1/beacon/states/{}/validator_balances",
            beacon_url, slot.0
        );
        let res = self
            .client
            .get(&url)
            .send()
            .timed("get_validator_balances_by_slot")
            .await?;
        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => {
                let envelope = res.json::<ValidatorBalancesEnvelope>().await?;
                Ok(Some(envelope.data))
            }
            status => Err(anyhow!(
                "failed to fetch validator balances by slot. slot = {} status = {} url = {}",
                slot.0,
                status,
                res.url()
            )),
        }
    }

    #[instrument(skip(self))]
    async fn get_pending_deposits_sum(&self, state_root: &str) -> Result<Option<GweiNewtype>> {
        let Some(beacon_url) = ENV_CONFIG.beacon_url.as_ref() else {
            return Err(anyhow::anyhow!(
                "BEACON_URL is not configured, cannot fetch pending deposits"
            ));
        };

        let url = format!(
            "{}/eth/v1/beacon/states/{}/pending_deposits",
            beacon_url, state_root
        );

        let res = self.client.get(&url).send().await?;

        match res.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => {
                let envelope = res.json::<PendingDepositsEnvelope>().await?;
                let sum = envelope
                    .data
                    .iter()
                    .fold(GweiNewtype(0), |acc, d| acc + d.amount);
                Ok(Some(sum))
            }
            status => Err(anyhow::anyhow!(
                "failed to fetch pending deposits. state_root = {} status = {} url = {}",
                state_root,
                status,
                res.url()
            )),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::fs;
    use std::{fs::File, io::BufReader};

    use super::*;
    use crate::units::GweiNewtype;

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

    #[test]
    fn test_decode_problematic_json() {
        // Test decoding for the block
        let block_data_path = "src/beacon_chain/data_samples/block_11678488.json";
        let block_json_str = fs::read_to_string(block_data_path)
            .unwrap_or_else(|_| panic!("failed to read block data from {}", block_data_path));

        serde_json::from_str::<BeaconBlockVersionedEnvelope>(&block_json_str)
            .unwrap_or_else(|_| panic!("failed to decode block JSON from {}", block_data_path));

        // Test decoding for the header
        let header_data_path = "src/beacon_chain/data_samples/header-11678488.json";
        let header_json_str = fs::read_to_string(header_data_path)
            .unwrap_or_else(|_| panic!("failed to read header data from {}", header_data_path));

        serde_json::from_str::<HeaderEnvelope>(&header_json_str)
            .unwrap_or_else(|_| panic!("failed to decode header JSON from {}", header_data_path));
    }

    const SLOT_1229: Slot = Slot(1229);
    const BLOCK_ROOT_1229: &str =
        "0x35376f52006e12b7e9247b457277fb34f6bd32d83a651e24c2669467607e0778";
    const STATE_ROOT_1229: &str =
        "0x36cb7e3d4585fb90a4ed17a0139de34a08b8354d1a7a054dbe3e4d8a0b93e625";

    #[tokio::test]
    async fn last_finalized_block_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node.get_last_finalized_block().await.unwrap();
    }

    #[tokio::test]
    async fn block_by_root_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node
            .get_block_by_block_root(BLOCK_ROOT_1229)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn header_by_slot_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node.get_header_by_slot(SLOT_1229).await.unwrap();
    }

    #[tokio::test]
    async fn get_none_header_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
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
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node.get_state_root_by_slot(SLOT_1229).await.unwrap();
    }

    #[ignore = "failing in CI, probably temporary, try re-enabling"]
    #[tokio::test]
    async fn validator_balances_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node
            .get_validator_balances(STATE_ROOT_1229)
            .await
            .unwrap()
            .unwrap();
    }

    #[ignore = "failing in CI, probably temporary, try re-enabling"]
    #[tokio::test]
    async fn validators_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node
            .get_validators_by_state(STATE_ROOT_1229)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_last_finality_checkpoint_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node.get_last_finality_checkpoint().await.unwrap();
    }

    #[tokio::test]
    async fn get_header_by_slot_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node
            .get_header_by_slot(Slot(4_000_000))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_header_by_hash_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node
            .get_header_by_block_root(
                "0x09df4a49850ec0c878ba2443f60f5fa6b473abcb14d222915fc44b17885ed8a4",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_last_head_test() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node.get_last_header().await.unwrap();
    }

    #[tokio::test]
    async fn get_header_by_state_root() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        beacon_node
            .get_header_by_state_root(
                "0x2e3df8cebeb66206d2b26e6a36a63105f78c22c3ab4b7abaa11b4056b4519588",
                Slot(5000000),
            )
            .await
            .unwrap();
    }

    #[ignore = "failing because we lack a beacon node with history"]
    #[tokio::test]
    async fn get_deposits() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        let block = beacon_node
            .get_block_by_slot(Slot(1229))
            .await
            .unwrap()
            .unwrap();
        let deposits = block.deposits();
        assert_eq!(deposits.len(), 16);
    }

    #[ignore = "failing because we lack a beacon node with history"]
    #[tokio::test]
    async fn get_withdrawals() {
        let beacon_node = BeaconNodeHttp::new_from_env();
        let block = beacon_node
            .get_block_by_slot(Slot(6212480))
            .await
            .unwrap()
            .unwrap();
        let withdrawals = block.withdrawals().unwrap();
        assert_eq!(withdrawals.len(), 16);
    }

    #[test]
    fn decode_execution_withdrawal() {
        let json_str = r#"
        [
            {
              "source_address": "0x0f4e76669fe1edb20380df975f1f6f1a36f95d06",
              "validator_pubkey": "0x901bb45707558e5e96859818a6e8eb3a67116ff2d56f2a46dd4e6f335e237b49481c4c5761d678f2638bdc26123cea29",
              "amount": "0"
            }
        ]
        "#;

        let withdrawals_vec: Vec<ExecutionWithdrawal> =
            serde_json::from_str(json_str).expect("failed to deserialize executionwithdrawal JSON");

        assert_eq!(withdrawals_vec.len(), 1);
        let withdrawal = &withdrawals_vec[0];

        assert_eq!(
            withdrawal.source_address,
            "0x0f4e76669fe1edb20380df975f1f6f1a36f95d06"
        );
        assert_eq!(
            withdrawal.validator_pubkey,
            "0x901bb45707558e5e96859818a6e8eb3a67116ff2d56f2a46dd4e6f335e237b49481c4c5761d678f2638bdc26123cea29"
        );
        // Assuming GweiNewtype can be constructed as GweiNewtype(0) and implements PartialEq
        assert_eq!(withdrawal.amount, GweiNewtype(0));
    }

    #[test]
    fn decode_pending_deposits() {
        let file =
            File::open("src/beacon_chain/data_samples/pending_deposits_11649024.json").unwrap();
        let reader = BufReader::new(file);

        let envelope: PendingDepositsEnvelope =
            serde_json::from_reader(reader).expect("failed to decode pending deposits JSON");
        assert!(!envelope.data.is_empty());
        // sanity-check first amount
        assert_eq!(
            envelope.data.first().unwrap().amount,
            GweiNewtype(32_000_000_000)
        );
    }
}
