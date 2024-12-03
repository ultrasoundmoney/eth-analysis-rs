use std::ops::Range;

use async_trait::async_trait;
use mockall::{automock, predicate::*};
use serde::Deserialize;

use crate::units::WeiNewtype;

use super::MevBlock;

// Earliest slot our relay has data for.
pub const EARLIEST_AVAILABLE_SLOT: i32 = 5616303;

// These are accepted blocks only.
#[derive(Deserialize)]
pub struct MaybeMevBlock {
    slot: i32,
    block_number: i32,
    block_hash: String,
    #[serde(rename = "value")]
    bid: Option<WeiNewtype>,
}

impl TryFrom<MaybeMevBlock> for MevBlock {
    type Error = String;

    fn try_from(raw: MaybeMevBlock) -> Result<Self, Self::Error> {
        match raw.bid {
            Some(bid) => Ok(MevBlock {
                slot: raw.slot,
                block_number: raw.block_number,
                block_hash: raw.block_hash,
                bid,
            }),
            None => Err(format!("No bid for block {}", raw.block_number)),
        }
    }
}

#[automock]
#[async_trait]
pub trait RelayApi {
    async fn fetch_mev_block(&self, slot: i32) -> Option<MevBlock>;
    async fn fetch_mev_blocks(&self, slots: Range<i32>) -> Vec<MevBlock>;
}

pub struct RelayApiHttp {
    server_url: String,
    client: reqwest::Client,
}

impl RelayApiHttp {
    pub fn new() -> Self {
        Self {
            server_url: "https://relay-analytics.ultrasound.money".into(),
            client: reqwest::Client::new(),
        }
    }

    pub fn new_with_url(server_url: &str) -> Self {
        Self {
            server_url: server_url.into(),
            client: reqwest::Client::new(),
        }
    }
}

impl Default for RelayApiHttp {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RelayApi for RelayApiHttp {
    async fn fetch_mev_block(&self, slot: i32) -> Option<MevBlock> {
        let url = format!(
            "{}/relay/v1/data/bidtraces/proposer_payload_delivered?slot={}",
            self.server_url, slot
        );
        self.client
            .get(url)
            .send()
            .await
            .unwrap()
            .json::<Vec<MaybeMevBlock>>()
            .await
            .ok()
            .and_then(|blocks| blocks.into_iter().next())
            .and_then(|block| block.try_into().ok())
    }

    async fn fetch_mev_blocks(&self, slots: Range<i32>) -> Vec<MevBlock> {
        let mut blocks = Vec::new();

        for slot in slots {
            if let Some(block) = self.fetch_mev_block(slot).await {
                blocks.push(block);
            }

            // add small delay to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        blocks
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn fetch_mev_blocks_test() {
        let mut server = mockito::Server::new();
        server
            .mock(
                "GET",
                "/relay/v1/data/bidtraces/proposer_payload_delivered?slot=0",
            )
            .with_status(200)
            .with_body(
                json!([{
                    "slot": 0,
                    "block_number": 9191911,
                    "block_hash": "abc",
                    "value": "100"
                }])
                .to_string(),
            )
            .create();

        let relay_api = RelayApiHttp::new_with_url(&server.url());

        let blocks = relay_api.fetch_mev_blocks(0..1).await;
        assert_eq!(blocks.len(), 1);

        let block = &blocks[0];
        assert_eq!(block.slot, 0);
        assert_eq!(block.block_number, 9191911);
        assert_eq!(block.block_hash, "abc");
        assert_eq!(block.bid.0, 100);
    }
}
