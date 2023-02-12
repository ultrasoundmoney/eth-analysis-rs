use super::decoders::{
    from_i32_hex_str, from_u128_hex_str, from_u64_hex_str, from_unix_timestamp_hex_str,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

// Execution chain blocks come in about once every 13s from genesis. With u32 our program
// would overflow when the block number passes 2_147_483_648. i32::MAX * 13 seconds = ~885 years.
pub type BlockNumber = i32;

// Eyeballed this one.
pub type Difficulty = u64;
// Final total difficulty on Ethereum is 76 bits. This should never increase anymore.
pub type TotalDifficulty = u128;

/// Hash for a block on the execution layer.
pub type BlockHash = String;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNodeBlock {
    // Highest gas price seen, ~4000 Gwei, if we want 1000x to future proof, we need to handle
    // 4000 * 1000 * 1e9 (Gwei) = 4e15, which needs 52 bits. Still fits within FLOAT8 too (2^53).
    #[serde(deserialize_with = "from_u64_hex_str")]
    pub base_fee_per_gas: u64,
    #[serde(deserialize_with = "from_u64_hex_str")]
    pub difficulty: Difficulty,
    // Started at 8M, currently at 30M, seems to fit in 2^31 for the foreseeable future.
    #[serde(deserialize_with = "from_i32_hex_str")]
    pub gas_used: i32,
    pub hash: BlockHash,
    #[serde(deserialize_with = "from_i32_hex_str")]
    pub number: BlockNumber,
    pub parent_hash: String,
    #[serde(deserialize_with = "from_unix_timestamp_hex_str")]
    pub timestamp: DateTime<Utc>,
    #[serde(deserialize_with = "from_u128_hex_str")]
    pub total_difficulty: TotalDifficulty,
}

#[cfg(test)]
pub mod tests {
    use crate::{time_frames::GrowingTimeFrame::*, units::WeiNewtype};

    use super::*;

    pub struct ExecutionNodeBlockBuilder {
        hash: String,
        number: BlockNumber,
        parent_hash: String,
        timestamp: DateTime<Utc>,
        gas_used: i32,
        base_fee_per_gas: u64,
    }

    impl ExecutionNodeBlockBuilder {
        pub fn new(test_id: &str) -> Self {
            let hash = format!("0x{test_id}_block_hash");

            Self {
                timestamp: SinceMerge.start_timestamp(),
                number: 0,
                hash,
                parent_hash: "0x0".to_string(),
                gas_used: 0,
                base_fee_per_gas: 0,
            }
        }

        pub fn from_parent(parent: &ExecutionNodeBlock) -> Self {
            let parent_hash = parent.hash.clone();
            let number = parent.number + 1;
            let hash = format!("{parent_hash}_{number}");
            Self::new("from_parent")
                .with_hash(&hash)
                .with_parent(parent)
        }

        pub fn with_hash(mut self, hash: &str) -> Self {
            self.hash = hash.to_string();
            self
        }

        pub fn with_number(mut self, number: BlockNumber) -> Self {
            self.number = number;
            self
        }

        pub fn with_burn(mut self, burn: WeiNewtype) -> Self {
            self.gas_used = 10;
            self.base_fee_per_gas = (burn.0 / self.gas_used as i128) as u64;
            self
        }

        pub fn with_parent(mut self, parent: &ExecutionNodeBlock) -> Self {
            self.parent_hash = parent.hash.to_string();
            self.number = parent.number + 1;
            self.timestamp = parent.timestamp + chrono::Duration::seconds(12);
            self
        }

        pub fn with_timestamp(mut self, timestamp: &DateTime<Utc>) -> Self {
            self.timestamp = *timestamp;
            self
        }

        pub fn build(&self) -> ExecutionNodeBlock {
            ExecutionNodeBlock {
                base_fee_per_gas: self.base_fee_per_gas,
                difficulty: 0,
                gas_used: self.gas_used,
                hash: self.hash.clone(),
                number: self.number,
                parent_hash: self.parent_hash.clone(),
                timestamp: self.timestamp,
                total_difficulty: 1,
            }
        }
    }
}
