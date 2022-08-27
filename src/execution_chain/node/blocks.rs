use super::decoders::{
    from_u128_hex_str, from_u32_hex_str, from_u64_hex_str, from_unix_timestamp_hex_str,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

// Execution chain blocks come in about once every 13s from genesis. With u32 our program
// would overflow when the block number passes 4_294_967_295. u32::MAX * 13 seconds = ~1769 years.
pub type BlockNumber = u32;

// Eyeballed these, they shouldn't grow much more as the merge is imminent.
pub type Difficulty = u64;
pub type TotalDifficulty = u128;

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
    pub difficulty: Difficulty,
    #[serde(deserialize_with = "from_u128_hex_str")]
    pub total_difficulty: TotalDifficulty,
    // Started at 8M, currently at 30M, seems to fit in 2^31 for the foreseeable future.
    #[serde(deserialize_with = "from_u32_hex_str")]
    pub gas_used: u32,
    // Highest gas price seen, ~4000 Gwei, if we want 10x- 100x future proof, we need to handle
    // 4000 * 100 * 1e9 (Gwei), which wouldn't fit in i32, but is <1% of i64.
    #[serde(deserialize_with = "from_u64_hex_str")]
    pub base_fee_per_gas: u64,
}
