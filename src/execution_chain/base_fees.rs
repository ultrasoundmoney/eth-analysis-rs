mod over_time;
mod stats;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::{try_join, FutureExt};
use serde::Serialize;
use sqlx::PgPool;
use tracing::{debug, debug_span, event, Instrument, Level};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    time_frames::LimitedTimeFrame,
    units::GweiNewtype,
};

use super::node::ExecutionNodeBlock;

#[derive(Debug, Serialize)]
struct BaseFeePerGas {
    timestamp: DateTime<Utc>,
    wei: u64,
}

async fn update_last_base_fee(db_pool: &PgPool, block: &ExecutionNodeBlock) {
    event!(Level::DEBUG, "updating current base fee");

    let base_fee_per_gas = BaseFeePerGas {
        timestamp: block.timestamp,
        wei: block.base_fee_per_gas,
    };

    caching::update_and_publish(db_pool, &CacheKey::BaseFeePerGas, base_fee_per_gas)
        .await
        .unwrap();
}

const APPROXIMATE_GAS_USED_PER_BLOCK: u32 = 15_000_000u32;
const APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK: i32 = 50400;

fn get_barrier(issuance_gwei: f64) -> f64 {
    issuance_gwei
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / APPROXIMATE_GAS_USED_PER_BLOCK as f64
}

const BASE_REWARD_FACTOR: u8 = 64;

#[allow(dead_code)]
fn get_issuance_time_frame(
    limited_time_frame: LimitedTimeFrame,
    GweiNewtype(effective_balance_sum): GweiNewtype,
) -> f64 {
    let effective_balance_sum = effective_balance_sum as f64;
    let max_issuance_per_epoch = (((BASE_REWARD_FACTOR as f64) * effective_balance_sum)
        / (effective_balance_sum.sqrt().floor()))
    .trunc();

    max_issuance_per_epoch * limited_time_frame.get_epoch_count()
}

pub async fn on_new_block(db_pool: &PgPool, block: &ExecutionNodeBlock) -> Result<()> {
    let issuance =
        beacon_chain::get_last_week_issuance(&mut db_pool.acquire().await.unwrap()).await;

    debug!("issuance: {issuance}");

    let barrier = get_barrier(issuance.0 as f64);

    debug!("barrier: {barrier}");

    try_join!(
        update_last_base_fee(db_pool, block).map(|_| anyhow::Ok(())),
        stats::update_base_fee_stats(db_pool, barrier, block)
            .instrument(debug_span!("update_base_fee_stats")),
        over_time::update_base_fee_over_time(db_pool, barrier, &block.number)
            .instrument(debug_span!("update_base_fee_over_time"))
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time_frames::LimitedTimeFrame::*;

    const SLOT_4658998_EFFECTIVE_BALANCE_SUM: u64 = 13607320000000000;

    #[test]
    fn get_issuance_test() {
        let issuance = get_issuance_time_frame(
            Hour1,
            GweiNewtype(SLOT_4658998_EFFECTIVE_BALANCE_SUM.try_into().unwrap()),
        );
        assert_eq!(issuance, 69990251296.875);
    }

    #[test]
    fn get_barrier_test() {
        let issuance = 1.124159e+13;
        let barrier = get_barrier(issuance);
        assert_eq!(barrier, 14.869828042328042);
    }
}
