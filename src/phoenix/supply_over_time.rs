use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::beacon_chain::{beacon_time, Slot};

use super::PhoenixMonitor;

#[derive(Debug, Deserialize)]
struct SupplyOverTime {
    pub slot: Slot,
}

impl SupplyOverTime {
    async fn get_current() -> reqwest::Result<SupplyOverTime> {
        reqwest::get("https://ultrasound.money/api/v2/fees/supply-over-time")
            .await?
            .error_for_status()?
            .json::<SupplyOverTime>()
            .await
    }
}

pub struct SupplyOverTimeMonitor {}

impl SupplyOverTimeMonitor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_current_timestamp(&self) -> Result<DateTime<Utc>> {
        SupplyOverTime::get_current()
            .await
            .map(|supply_parts| beacon_time::date_time_from_slot(&supply_parts.slot))
            .map_err(|e| e.into())
    }
}

#[async_trait]
impl PhoenixMonitor for SupplyOverTimeMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        self.get_current_timestamp().await
    }
}
