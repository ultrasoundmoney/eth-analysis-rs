use super::{blocks::ExecutionNodeBlock, decoders::*};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::execution_chain::node::BlockNumber;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Head {
    pub hash: String,
    #[serde(deserialize_with = "from_i32_hex_str")]
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

impl From<ExecutionNodeBlock> for Head {
    fn from(block: ExecutionNodeBlock) -> Self {
        Self {
            hash: block.hash,
            number: block.number,
            parent_hash: block.parent_hash,
            timestamp: block.timestamp,
        }
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
