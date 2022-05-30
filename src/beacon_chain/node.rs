use crate::{config, eth_units::GweiAmount};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Deserializer};

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

fn from_slot<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(s.parse::<u32>().unwrap())
}

#[derive(Debug, Deserialize)]
pub struct BeaconBlock {
    pub body: Body,
    pub parent_root: String,
    #[serde(deserialize_with = "from_slot")]
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

pub async fn get_block_by_root(client: &Client, block_root: &str) -> reqwest::Result<BeaconBlock> {
    client
        .get(make_blocks_url(&BlockId::BlockRoot(block_root.to_string())))
        .send()
        .await?
        .error_for_status()?
        .json::<BeaconBlockVersionedEnvelope>()
        .await
        .map(|envelope| envelope.data.message.into())
}

pub async fn get_last_finalized_block(client: &Client) -> reqwest::Result<BeaconBlock> {
    client
        .get(make_blocks_url(&BlockId::Finalized))
        .send()
        .await?
        .error_for_status()?
        .json::<BeaconBlockVersionedEnvelope>()
        .await
        .map(|envelope| envelope.data.message.into())
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

pub async fn get_state_root_by_slot(
    client: &reqwest::Client,
    slot: &u32,
) -> reqwest::Result<String> {
    client
        .get(make_state_root_url(slot))
        .send()
        .await?
        .error_for_status()?
        .json::<StateRootEnvelope>()
        .await
        .map(|envelope| envelope.data.root)
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

pub async fn get_validator_balances(
    client: &Client,
    state_root: &str,
) -> reqwest::Result<Vec<ValidatorBalance>> {
    client
        .get(make_validator_balances_by_state_url(state_root))
        .send()
        .await?
        .error_for_status()?
        .json::<ValidatorBalancesEnvelope>()
        .await
        .map(|envelope| envelope.data)
}

#[derive(Debug, Deserialize)]
pub struct BeaconHeader {
    pub slot: String,
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

/// This function returns nothing both when a slot does not exist at all, and a slot does exist but
/// is blockless. In other words, don't use this function alone to tell if a given slot has a
/// block, check the slot exists first.
pub async fn get_header_by_slot(
    client: &Client,
    slot: &u32,
) -> reqwest::Result<Option<BeaconHeaderSignedEnvelope>> {
    // No header at this slot, either this slot doesn't exist or there is no header at this slot. We assume the slot exists but is headerless.
    let res = client
        .get(make_header_by_slot_url(slot))
        .send()
        .await?
        .error_for_status();

    match res {
        Ok(res) => res
            .json::<HeaderEnvelope>()
            .await
            .map(|envelope| Some(envelope.data)),
        Err(res) => match res.status() {
            None => Err(res),
            Some(status) => {
                if status == StatusCode::NOT_FOUND {
                    Ok(None)
                } else {
                    Err(res)
                }
            }
        },
    }
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

pub async fn get_validators_by_state(
    client: &Client,
    state_root: &str,
) -> reqwest::Result<Vec<Validator>> {
    client
        .get(make_validators_by_state_url(state_root))
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

        let b: BeaconBlockVersionedEnvelope = serde_json::from_reader(reader).unwrap();
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
    async fn test_last_finalized_block() {
        let client = reqwest::Client::new();
        get_last_finalized_block(&client).await.unwrap();
    }

    #[tokio::test]
    async fn test_block_by_root() {
        let client = reqwest::Client::new();
        get_block_by_root(&client, BLOCK_ROOT_1229).await.unwrap();
    }

    #[tokio::test]
    async fn test_header_by_slot() {
        let client = reqwest::Client::new();
        get_header_by_slot(&client, &SLOT_1229).await.unwrap();
    }

    #[tokio::test]
    async fn test_state_root_by_slot() {
        let client = reqwest::Client::new();
        get_state_root_by_slot(&client, &SLOT_1229).await.unwrap();
    }

    #[tokio::test]
    async fn test_validator_balances() {
        let client = reqwest::Client::new();
        get_validator_balances(&client, STATE_ROOT_1229)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_validators() {
        let client = reqwest::Client::new();
        get_validators_by_state(&client, STATE_ROOT_1229)
            .await
            .unwrap();
    }
}
