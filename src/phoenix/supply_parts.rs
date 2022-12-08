use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::beacon_chain::Slot;

use super::PhoenixMonitor;

#[derive(Debug, Deserialize)]
struct SupplyParts {
    pub slot: Slot,
}

impl SupplyParts {
    async fn get_current() -> reqwest::Result<SupplyParts> {
        reqwest::get("https://ultrasound.money/api/v2/fees/supply-parts")
            .await?
            .error_for_status()?
            .json::<SupplyParts>()
            .await
    }
}

pub struct SupplyPartsMonitor {}

impl SupplyPartsMonitor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_current_timestamp(&self) -> Result<DateTime<Utc>> {
        SupplyParts::get_current()
            .await
            .map(|supply_parts| supply_parts.slot.date_time())
            .map_err(|e| e.into())
    }
}

#[async_trait]
impl PhoenixMonitor for SupplyPartsMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        self.get_current_timestamp().await
    }
}
