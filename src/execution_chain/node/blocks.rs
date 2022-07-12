use super::decoders::from_u32_hex_str;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNodeBlock {
    pub hash: String,
    #[serde(deserialize_with = "from_u32_hex_str")]
    pub number: u32,
    pub parent_hash: String,
}
