mod barrier;
mod last;
mod over_time;
pub mod routes;
mod stats;

use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::PgPool;

use crate::{
    beacon_chain::IssuanceStore, performance::TimedExt, time_frames::LimitedTimeFrame,
    units::GweiNewtype,
};

use super::node::ExecutionNodeBlock;

#[derive(Debug, Serialize)]
struct BaseFeePerGas {
    timestamp: DateTime<Utc>,
    wei: u64,
}

const BASE_REWARD_FACTOR: u8 = 64;

#[allow(dead_code)]
fn issuance_from_time_frame(
    limited_time_frame: LimitedTimeFrame,
    GweiNewtype(effective_balance_sum): GweiNewtype,
) -> f64 {
    let effective_balance_sum = effective_balance_sum as f64;
    let max_issuance_per_epoch = (((BASE_REWARD_FACTOR as f64) * effective_balance_sum)
        / (effective_balance_sum.sqrt().floor()))
    .trunc();

    max_issuance_per_epoch * limited_time_frame.epoch_count()
}

pub async fn on_new_block(
    db_pool: &PgPool,
    issuance_store: impl IssuanceStore,
    block: &ExecutionNodeBlock,
) {
    let ultra_sound_barrier = barrier::get_barrier(issuance_store).await;

    join!(
        barrier::on_new_barrier(db_pool, ultra_sound_barrier),
        last::update_last_base_fee(db_pool, block).timed("update_last_base_fee"),
        stats::update_base_fee_stats(db_pool, ultra_sound_barrier, block)
            .timed("update_base_fee_stats"),
        over_time::update_base_fee_over_time(db_pool, ultra_sound_barrier, &block.number)
            .timed("update_base_fee_over_time"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time_frames::LimitedTimeFrame::*;

    const SLOT_4658998_EFFECTIVE_BALANCE_SUM: u64 = 13607320000000000;

    #[test]
    fn get_issuance_test() {
        let issuance = issuance_from_time_frame(
            Hour1,
            GweiNewtype(SLOT_4658998_EFFECTIVE_BALANCE_SUM.try_into().unwrap()),
        );
        assert_eq!(issuance, 69990251296.875);
    }
}
