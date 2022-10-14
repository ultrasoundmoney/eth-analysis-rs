mod over_time;
mod stats;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::PgPool;
use tracing::{debug, debug_span, event, Instrument, Level};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    eth_units::GweiNewtype,
    key_value_store,
    time_frames::TimeFrame,
};

use super::node::ExecutionNodeBlock;

#[derive(Debug, Serialize)]
struct BaseFeePerGas {
    timestamp: DateTime<Utc>,
    wei: u64,
}

// Find a way to wrap the result of this fn in Ok whilst using try_join!
async fn update_last_base_fee(executor: &PgPool, block: &ExecutionNodeBlock) -> anyhow::Result<()> {
    event!(Level::DEBUG, "updating current base fee");

    let base_fee_per_gas = BaseFeePerGas {
        timestamp: block.timestamp,
        wei: block.base_fee_per_gas,
    };

    key_value_store::set_value(
        executor,
        &CacheKey::BaseFeePerGas.to_db_key(),
        &serde_json::to_value(base_fee_per_gas).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeePerGas).await;

    Ok(())
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
    time_frame: TimeFrame,
    GweiNewtype(effective_balance_sum): GweiNewtype,
) -> f64 {
    let effective_balance_sum = effective_balance_sum as f64;
    let max_issuance_per_epoch = (((BASE_REWARD_FACTOR as f64) * effective_balance_sum)
        / (effective_balance_sum.sqrt().floor()))
    .trunc();
    let issuance = max_issuance_per_epoch * time_frame.get_epoch_count();
    return issuance;
}

pub async fn on_new_block(db_pool: &PgPool, block: &ExecutionNodeBlock) -> Result<()> {
    let issuance =
        beacon_chain::get_last_week_issuance(&mut db_pool.acquire().await.unwrap()).await;

    debug!("issuance: {issuance}");

    let barrier = get_barrier(issuance.0 as f64);

    debug!("barrier: {barrier}");

    try_join!(
        update_last_base_fee(db_pool, block).instrument(debug_span!("update_last_base_fee")),
        stats::update_base_fee_stats(db_pool, barrier, block)
            .instrument(debug_span!("update_base_fee_stats")),
        over_time::update_base_fee_over_time(db_pool, barrier, block.number)
            .instrument(debug_span!("update_base_fee_over_time"))
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::time_frames::LimitedTimeFrame;

    use super::*;

    const SLOT_4658998_EFFECTIVE_BALANCE_SUM: u64 = 13607320000000000;

    #[test]
    fn get_issuance_test() {
        let issuance = get_issuance_time_frame(
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
            GweiNewtype(SLOT_4658998_EFFECTIVE_BALANCE_SUM),
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
