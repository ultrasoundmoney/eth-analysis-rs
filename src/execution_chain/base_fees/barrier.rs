use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tracing::debug;

use crate::{
    beacon_chain::IssuanceStore,
    caching::{self, CacheKey},
    execution_chain::{BlockNumber, ExecutionNodeBlock},
    performance::TimedExt,
    units::GweiNewtype,
};

/// Is the base fee per gas price in Gwei/gas at which burn is outpacing issuance. This is only true
/// when blocks are half full, at 15M gas, which is true on average due to EIP-1559.
#[derive(Debug, Serialize)]
pub struct Barrier {
    pub base_fee_barrier: f64,
    pub blob_fee_barrier: f64
}

#[derive(Debug, Serialize)]
pub struct BarrierStats {
    barrier: f64,
    blob_barrier: f64,
    block_number: BlockNumber,
    timestamp: DateTime<Utc>,
}

const APPROXIMATE_GAS_USED_PER_BLOCK: u32 = 15_000_000u32;
const APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK: i32 = 50400;
const TARGET_BLOB_GAS_PER_BLOCK: i32 = 393216;

pub fn estimate_blob_barrier_from_weekly_issuance(issuance: GweiNewtype) -> f64 {
    issuance.0 as f64
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / TARGET_BLOB_GAS_PER_BLOCK as f64
}

pub fn estimate_barrier_from_weekly_issuance(issuance: GweiNewtype) -> f64 {
    issuance.0 as f64
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / APPROXIMATE_GAS_USED_PER_BLOCK as f64
}

pub async fn get_barrier(issuance_store: &impl IssuanceStore) -> Barrier {
    debug!("estimating base fee per gas barrier");

    let issuance = issuance_store
        .weekly_issuance()
        .timed("get_last_week_issuance")
        .await;

    let base_fee_barrier = estimate_barrier_from_weekly_issuance(issuance);
    debug!("base fee per gas (ultra sound) barrier: {base_fee_barrier}");
    let blob_fee_barrier = estimate_blob_barrier_from_weekly_issuance(issuance);
    debug!("blob fee per gas (ultra sound) barrier: {blob_fee_barrier}");

    Barrier {base_fee_barrier, blob_fee_barrier}
}

// Because other modules need the ultra sound barrier as an input, we calculate it first, then
// update the cache.
pub async fn on_new_barrier(db_pool: &PgPool, barrier: &Barrier, block: &ExecutionNodeBlock) {
    let barrier_stats = BarrierStats {
        barrier: barrier.base_fee_barrier,
        blob_barrier: barrier.blob_fee_barrier,
        block_number: block.number,
        timestamp: block.timestamp,
    };
    caching::update_and_publish(db_pool, &CacheKey::BaseFeePerGasBarrier, barrier_stats).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_from_issuance_test() {
        let issuance = GweiNewtype(11241590000000);
        let barrier = estimate_barrier_from_weekly_issuance(issuance);
        assert_eq!(barrier, 14.869828042328042);
    }
}
