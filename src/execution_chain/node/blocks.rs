use super::decoders::{
    from_u128_hex_str, from_u32_hex_str, from_u64_hex_str, from_unix_timestamp_hex_str,
};
use crate::execution_chain::blocks::BlockNumber;
use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNodeBlock {
    pub hash: String,
    #[serde(deserialize_with = "from_u32_hex_str")]
    pub number: BlockNumber,
    pub parent_hash: String,
    #[serde(deserialize_with = "from_unix_timestamp_hex_str")]
    pub timestamp: DateTime<Utc>,
    #[serde(deserialize_with = "from_u64_hex_str")]
    pub difficulty: u64,
    #[serde(deserialize_with = "from_u128_hex_str")]
    pub total_difficulty: u128,
}
