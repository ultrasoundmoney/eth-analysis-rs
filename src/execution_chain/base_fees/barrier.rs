use sqlx::PgPool;
use tracing::debug;

use crate::{
    beacon_chain::{self, IssuanceStore},
    caching::{self, CacheKey},
    performance::TimedExt,
    units::GweiNewtype,
};

/// Is the base fee per gas price at which burn is outpacing issuance. This is only true when
/// blocks are half full, at 15M gas, which is true on average due to EIP-1559.
pub type Barrier = f64;

const APPROXIMATE_GAS_USED_PER_BLOCK: u32 = 15_000_000u32;
const APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK: i32 = 50400;

pub fn estimate_barrier_from_issuance(issuance: GweiNewtype) -> f64 {
    issuance.0 as f64
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / APPROXIMATE_GAS_USED_PER_BLOCK as f64
}

pub async fn get_barrier(issuance_store: impl IssuanceStore) -> Barrier {
    debug!("estimating base fee per gas barrier");

    let issuance = beacon_chain::get_last_week_issuance(issuance_store)
        .timed("get_last_week_issuance")
        .await;

    let barrier = estimate_barrier_from_issuance(issuance);
    debug!("base fee per gas (ultra sound) barrier: {barrier}");

    barrier
}

// Because other modules need the ultra sound barrier as an input, we calculate it first, then
// update the cache.
pub async fn on_new_barrier(db_pool: &PgPool, base_fee_per_gas_barrier: Barrier) {
    caching::update_and_publish(
        db_pool,
        &CacheKey::BaseFeePerGasBarrier,
        base_fee_per_gas_barrier,
    )
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_from_issuance_test() {
        let issuance = GweiNewtype(11241590000000);
        let barrier = estimate_barrier_from_issuance(issuance);
        assert_eq!(barrier, 14.869828042328042);
    }
}
