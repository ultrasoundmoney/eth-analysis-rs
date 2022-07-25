use crate::execution_chain::blocks::BlockNumber;

use super::decoders::from_u32_hex_str;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNodeBlock {
    pub hash: String,
    #[serde(deserialize_with = "from_u32_hex_str")]
    pub number: BlockNumber,
    pub parent_hash: String,
}
