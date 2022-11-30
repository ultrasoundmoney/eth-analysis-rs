//! This module updates the supply-dashboard endpoint. This endpoint is a combination of
//! current eth supply, eth supply over time, fee burn, and average eth price.

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::PgPool;
use tracing::debug;

use crate::{
    beacon_chain::{beacon_time, Slot},
    caching::{self, CacheKey},
    eth_supply::{self, SupplyOverTime, SupplyParts},
    key_value_store,
    performance::TimedExt,
};

#[derive(Serialize)]
struct SupplyDashboard {
    eth_supply_parts: SupplyParts,
    fees_burned: Option<()>,
    slot: Slot,
    supply_over_time: SupplyOverTime,
    timestamp: DateTime<Utc>,
}

/// Updates each individual cache related to the supply dashboard. This is how we used to do
/// things, after the frontend switched over to the new supply-dashboard endpoint this should be
/// dropped.
async fn update_individual_caches(
    db_pool: &PgPool,
    supply_parts: &SupplyParts,
    supply_over_time: &SupplyOverTime,
) -> Result<()> {
    try_join!(
        eth_supply::update_supply_parts_cache(db_pool, supply_parts),
        eth_supply::update_supply_over_time_cache(db_pool, supply_over_time)
    )?;

    Ok(())
}

pub async fn update_cache(db_pool: &PgPool, slot: &Slot) -> Result<()> {
    let supply_parts = eth_supply::get_supply_parts(&mut *db_pool.acquire().await?, slot).await?;

    match supply_parts {
        None => {
            debug!(
                slot,
                "eth supply parts unavailable for slot, skipping supply dashboard update"
            );
        }
        Some(supply_parts) => {
            let supply_over_time =
                eth_supply::get_supply_over_time(db_pool, *slot, supply_parts.block_number())
                    .timed("get-supply-over-time")
                    .await?;

            update_individual_caches(db_pool, &supply_parts, &supply_over_time).await?;

            let supply_dashboard = SupplyDashboard {
                eth_supply_parts: supply_parts,
                fees_burned: None,
                slot: *slot,
                supply_over_time,
                timestamp: beacon_time::get_date_time_from_slot(slot),
            };

            key_value_store::set_value_str(
                db_pool,
                &CacheKey::SupplyDashboard.to_db_key(),
                // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
                // as string however.
                &serde_json::to_string(&supply_dashboard).unwrap(),
            )
            .await;

            caching::publish_cache_update(db_pool, CacheKey::SupplyDashboard).await?;
        }
    };

    Ok(())
}