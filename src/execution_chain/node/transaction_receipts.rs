use serde::Deserialize;

use super::decoders::from_i32_hex_str;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TransactionReceipt {
    #[serde(deserialize_with = "from_i32_hex_str")]
    pub block_number: i32,
    // Started at 8M, currently at 30M, seems to fit in 2^31 for the foreseeable future.
    #[serde(deserialize_with = "from_i32_hex_str")]
    pub gas_used: i32,
    pub to: Option<String>,
    pub transaction_hash: String,
}
