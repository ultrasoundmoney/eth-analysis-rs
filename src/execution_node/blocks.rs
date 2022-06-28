use serde::Deserialize;

use super::RpcMessage;

#[derive(Deserialize)]
pub struct BlockF {
    number: String,
}

#[derive(Debug)]
pub struct Block {
    pub number: u32,
}

impl From<RpcMessage<BlockF>> for Block {
    fn from(message: RpcMessage<BlockF>) -> Self {
        Self {
            number: u32::from_str_radix(&message.result.number[2..], 16).unwrap(),
        }
    }
}
