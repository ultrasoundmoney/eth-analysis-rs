use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::PhoenixMonitor;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockFee {
    pub mined_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupedAnalysis1 {
    pub latest_block_fees: Vec<BlockFee>,
}

impl GroupedAnalysis1 {
    async fn get_current() -> reqwest::Result<GroupedAnalysis1> {
        reqwest::get("https://ultrasound.money/api/fees/grouped-analysis-1")
            .await?
            .error_for_status()?
            .json::<GroupedAnalysis1>()
            .await
    }
}

pub struct GroupedAnalysis1Monitor {}

impl GroupedAnalysis1Monitor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_current_timestamp(&self) -> Result<DateTime<Utc>> {
        let grouped_analysis = GroupedAnalysis1::get_current().await?;
        let mut timestamps = grouped_analysis
            .latest_block_fees
            .into_iter()
            .map(|block_fee| block_fee.mined_at)
            .collect::<Vec<_>>();
        timestamps.sort();
        let newest = timestamps
            .last()
            .context("need at least one block fee to get a timestamp")?;
        Ok(*newest)
    }
}

#[async_trait]
impl PhoenixMonitor for GroupedAnalysis1Monitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        self.get_current_timestamp().await
    }
}
