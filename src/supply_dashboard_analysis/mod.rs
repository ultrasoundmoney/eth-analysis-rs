//! This module updates the supply-dashboard endpoint. This endpoint is a combination of
//! current eth supply, eth supply over time, fee burn, and average eth price.

use anyhow::Result;
use futures::join;
use sqlx::PgPool;
use tracing::warn;

use crate::{
    caching::{self, CacheKey},
    eth_supply::{self, SupplyPartsStore},
    performance::TimedExt,
};

// #[derive(Serialize)]
// struct SupplyDashboardAnalysis {
//     supply_parts: SupplyParts,
//     fees_burned: Option<()>,
//     slot: Slot,
//     supply_over_time: SupplyOverTime,
//     timestamp: DateTime<Utc>,
// }

pub async fn update_cache(db_pool: &PgPool) -> Result<()> {
    // Our limit is whatever the youngest of the table we depend on has stored, currently that is
    // the last stored supply slot.
    let limit_slot = {
        let limit_slot = eth_supply::get_last_stored_supply_slot(db_pool).await?;
        match limit_slot {
            Some(limit_slot) => limit_slot,
            None => {
                warn!("no last stored supply slot available, skipping supply dashboard update");
                return Ok(());
            }
        }
    };

    let supply_parts_store = SupplyPartsStore::new(db_pool);

    let supply_parts = {
        let supply_parts = supply_parts_store.get(limit_slot).await?;
        match supply_parts {
            Some(supply_parts) => supply_parts,
            None => {
                warn!(%limit_slot, "no supply parts available for slot, skipping supply dashboard update");
                return Ok(());
            }
        }
    };

    let supply_over_time =
        eth_supply::get_supply_over_time(db_pool, limit_slot, supply_parts.block_number())
            .timed("get-supply-over-time")
            .await?;

    // let supply_dashboard_analysis = SupplyDashboardAnalysis {
    //     supply_parts: supply_parts.clone(),
    //     fees_burned: None,
    //     slot: limit_slot,
    //     supply_over_time: supply_over_time.clone(),
    //     timestamp: beacon_time::get_date_time_from_slot(&limit_slot),
    // };

    // key_value_store::set_value_str(
    //     db_pool,
    //     &CacheKey::SupplyDashboardAnalysis.to_db_key(),
    //     // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
    //     // as string however.
    //     &serde_json::to_string(&supply_dashboard_analysis).unwrap(),
    // )
    // .await;

    // caching::publish_cache_update(db_pool, CacheKey::SupplyDashboardAnalysis)
    //     .await?;

    join!(
        caching::update_and_publish(db_pool, &CacheKey::SupplyParts, supply_parts),
        caching::update_and_publish(db_pool, &CacheKey::SupplyOverTime, supply_over_time),
    );

    Ok(())
}
