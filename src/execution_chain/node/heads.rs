use super::decoders::*;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::execution_chain::blocks::BlockNumber;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Head {
    pub hash: String,
    #[serde(deserialize_with = "from_u32_hex_str")]
    pub number: BlockNumber,
    pub parent_hash: String,
    #[serde(deserialize_with = "from_unix_timestamp_hex_str")]
    pub timestamp: DateTime<Utc>,
}

impl From<NewHeadMessage> for Head {
    fn from(message: NewHeadMessage) -> Self {
        message.params.result
    }
}

#[derive(Deserialize)]
pub struct NewHeadParams {
    result: Head,
}

#[derive(Deserialize)]
pub struct NewHeadMessage {
    params: NewHeadParams,
}
