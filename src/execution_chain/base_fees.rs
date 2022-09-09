use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    eth_units::GweiAmount,
    key_value_store,
    time_frames::TimeFrame,
};

use super::node::{BlockNumber, ExecutionNodeBlock};

#[derive(Debug, Serialize)]
struct BaseFeePerGas {
    timestamp: DateTime<Utc>,
    wei: u64,
}

async fn update_last_base_fee(executor: &PgPool, block: &ExecutionNodeBlock) {
    tracing::debug!("updating current base fee");

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
}

type WeiU64 = u64;

#[derive(Debug, PartialEq, Serialize)]
struct BaseFeeAtTime {
    block_number: BlockNumber,
    wei: WeiU64,
}

#[derive(Serialize)]
struct BaseFeeOverTime {
    block_number: BlockNumber,
    d1: Vec<BaseFeeAtTime>,
    barrier: f64,
}

async fn get_base_fee_over_time<'a>(executor: impl PgExecutor<'a>) -> Vec<BaseFeeAtTime> {
    sqlx::query(
        "
            SELECT
                number,
                base_fee_per_gas
            FROM
                blocks_next
            WHERE
                timestamp >= NOW() - '1 hour'::INTERVAL
            ORDER BY number DESC
        ",
    )
    .map(|row: PgRow| {
        let wei = row.get::<i64, _>("base_fee_per_gas") as u64;
        let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
        BaseFeeAtTime { wei, block_number }
    })
    .fetch_all(executor)
    .await
    .unwrap()
}

const BASE_REWARD_FACTOR: u8 = 64;
const APPROXIMATE_GAS_USED_PER_BLOCK: u32 = 15_000_000u32;

#[allow(dead_code)]
fn get_issuance_time_frame(
    time_frame: TimeFrame,
    GweiAmount(effective_balance_sum): GweiAmount,
) -> f64 {
    let effective_balance_sum = effective_balance_sum as f64;
    let max_issuance_per_epoch = (((BASE_REWARD_FACTOR as f64) * effective_balance_sum)
        / (effective_balance_sum.sqrt().floor()))
    .trunc();
    let issuance = max_issuance_per_epoch * time_frame.get_epoch_count();
    return issuance;
}

const APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK: i32 = 50400;

fn get_barrier(issuance_gwei: f64) -> f64 {
    issuance_gwei
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / APPROXIMATE_GAS_USED_PER_BLOCK as f64
}

async fn update_base_fee_over_time(executor: &PgPool, block: &ExecutionNodeBlock) {
    tracing::debug!("updating base fee over time");

    let base_fees = get_base_fee_over_time(executor).await;

    let issuance =
        beacon_chain::get_last_week_issuance(&mut executor.acquire().await.unwrap()).await;

    tracing::debug!("issuance: {issuance}");

    let barrier = get_barrier(issuance.0 as f64);

    tracing::debug!("barrier: {barrier}");

    let base_fees_over_time = BaseFeeOverTime {
        barrier,
        block_number: block.number,
        d1: base_fees,
    };

    key_value_store::set_value(
        executor,
        CacheKey::BaseFeeOverTime.to_db_key(),
        &serde_json::to_value(base_fees_over_time).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeeOverTime).await;
}

pub async fn on_new_block<'a>(db_pool: &PgPool, block: &ExecutionNodeBlock) {
    join!(
        update_last_base_fee(db_pool, block),
        update_base_fee_over_time(db_pool, block)
    );
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use sqlx::Acquire;

    use crate::{db_testing, execution_chain::block_store::BlockStore};

    use super::*;

    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 1,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            total_difficulty: 10,
        }
    }

    #[tokio::test]
    async fn get_base_fee_over_time_d1_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut *transaction);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;

        let base_fees_d1 = get_base_fee_over_time(&mut transaction).await;

        assert_eq!(
            base_fees_d1,
            vec![BaseFeeAtTime {
                wei: 1,
                block_number: 0
            }]
        );
    }

    const SLOT_4658998_EFFECTIVE_BALANCE_SUM: u64 = 13607320000000000;

    #[test]
    fn get_issuance_test() {
        let issuance = get_issuance_time_frame(
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
            GweiAmount(SLOT_4658998_EFFECTIVE_BALANCE_SUM),
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
