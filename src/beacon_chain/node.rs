use super::GweiAmount;
use crate::config;
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
    let s: &str = Deserialize::deserialize(deserializer)?;
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

impl From<NodeValidatorBalance> for ValidatorBalance {
    fn from(node_validator_balance: NodeValidatorBalance) -> Self {
        ValidatorBalance {
            balance: GweiAmount(node_validator_balance.balance.parse::<u64>().unwrap()),
        }
    }
}

struct NodeValidatorBalance {
    balance: String,
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

#[cfg(test)]
mod tests {
    use crate::beacon_chain::data_samples;

    use super::*;

    #[test]
    fn decode_beacon_block_versioned_envelope() {
        serde_json::from_str::<BeaconBlockVersionedEnvelope>(
            data_samples::BEACON_BLOCK_VERSIONED_ENVELOPE_1229,
        )
        .unwrap();
    }

    #[test]
    fn decode_validator_balances() {
        serde_json::from_str::<ValidatorBalancesEnvelope>(data_samples::VALIDATOR_BALANCES_1229)
            .unwrap();
    }

    #[test]
    fn decode_header_envelope() {
        serde_json::from_str::<HeaderEnvelope>(data_samples::HEADER_1229).unwrap();
    }
}
