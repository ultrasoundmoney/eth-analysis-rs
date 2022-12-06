use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::PhoenixMonitor;

#[derive(Debug, Deserialize)]
struct EthPriceStats {
    pub timestamp: DateTime<Utc>,
}

impl EthPriceStats {
    async fn get_current() -> reqwest::Result<EthPriceStats> {
        reqwest::get("https://ultrasound.money/api/v2/fees/eth-price-stats")
            .await?
            .error_for_status()?
            .json::<EthPriceStats>()
            .await
    }
}

pub struct EthPriceStatsMonitor {}

impl EthPriceStatsMonitor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_current_timestamp(&self) -> Result<DateTime<Utc>> {
        EthPriceStats::get_current()
            .await
            .map(|eth_price_stats| eth_price_stats.timestamp)
            .map_err(|e| e.into())
    }
}

#[async_trait]
impl PhoenixMonitor for EthPriceStatsMonitor {
    async fn refresh(&mut self) -> Result<DateTime<Utc>> {
        self.get_current_timestamp().await
    }
}
