use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use tracing::{debug_span, event, warn, Instrument, Level};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    eth_units::{GweiNewtype, WeiF64},
    key_value_store,
    time_frames::{LimitedTimeFrame, TimeFrame},
};

use super::node::{BlockNumber, ExecutionNodeBlock};

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

type WeiU64 = u64;

#[derive(Debug, PartialEq, Serialize)]
struct BaseFeeAtTime {
    block_number: BlockNumber,
    timestamp: DateTime<Utc>,
    wei: WeiU64,
}

#[derive(Serialize)]
struct BaseFeeOverTime {
    barrier: WeiF64,
    block_number: BlockNumber,
    all: Option<Vec<BaseFeeAtTime>>,
    d1: Vec<BaseFeeAtTime>,
    d30: Vec<BaseFeeAtTime>,
    d7: Vec<BaseFeeAtTime>,
    h1: Vec<BaseFeeAtTime>,
    m5: Vec<BaseFeeAtTime>,
}

async fn get_base_fee_over_time(
    executor: impl PgExecutor<'_>,
    time_frame: TimeFrame,
) -> sqlx::Result<Vec<BaseFeeAtTime>> {
    match time_frame {
        TimeFrame::All => {
            warn!("getting base fee over time for time frame 'all' is slow, and may be incorrect depending on blocks_next backfill status");
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas) AS base_fee_per_gas,
                        DATE_TRUNC('day', timestamp) AS timestamp,
                        MAX(number) AS number
                    FROM
                        blocks_next
                    GROUP BY 2
                    ORDER BY 2 ASC
                ",
            )
            .map(|row: PgRow| {
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<i64, _>("base_fee_per_gas") as u64;
                BaseFeeAtTime {
                    wei,
                    block_number,
                    timestamp,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Minute5)
        | TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Hour1) => {
            sqlx::query(
                "
                    SELECT
                        base_fee_per_gas,
                        number,
                        timestamp
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1
                    ORDER BY number ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<i64, _>("base_fee_per_gas") as u64;
                BaseFeeAtTime {
                    wei,
                    block_number,
                    timestamp,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Day1) => {
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas) AS base_fee_per_gas,
                        DATE_TRUNC('minute', timestamp) AS timestamp,
                        MAX(number) AS number
                    FROM
                        blocks_next
                    GROUP BY 2
                    ORDER BY 2 ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<i64, _>("base_fee_per_gas") as u64;
                BaseFeeAtTime {
                    wei,
                    block_number,
                    timestamp,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Day7)
        | TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Day30) => {
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas) AS base_fee_per_gas,
                        DATE_TRUNC('hour', timestamp) AS timestamp,
                        MAX(number) AS number
                    FROM
                        blocks_next
                    GROUP BY 2
                    ORDER BY 2 ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<i64, _>("base_fee_per_gas") as u64;
                BaseFeeAtTime {
                    wei,
                    block_number,
                    timestamp,
                }
            })
            .fetch_all(executor)
            .await
        }
    }
}

const BASE_REWARD_FACTOR: u8 = 64;
const APPROXIMATE_GAS_USED_PER_BLOCK: u32 = 15_000_000u32;

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

const APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK: i32 = 50400;

fn get_barrier(issuance_gwei: f64) -> f64 {
    issuance_gwei
        / APPROXIMATE_NUMBER_OF_BLOCKS_PER_WEEK as f64
        / APPROXIMATE_GAS_USED_PER_BLOCK as f64
}

async fn get_base_fee_per_gas_average(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> sqlx::Result<WeiF64> {
    match time_frame {
        TimeFrame::All => {
            warn!("getting average fee for time frame 'all' is slow, and may be incorrect depending on blocks_next backfill status");
            sqlx::query(
                "
                    SELECT
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS average
                    FROM
                        blocks_next
                ",
            )
            .map(|row: PgRow| row.get::<f64, _>("average"))
            .fetch_one(executor)
            .await
        },
        TimeFrame::LimitedTimeFrame(limited_time_frame) => {
            sqlx::query(
                "
                    SELECT
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS average
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1
                ",
            )
            .bind(limited_time_frame.get_postgres_interval())
            .map(|row: PgRow| row.get::<f64, _>("average"))
            .fetch_one(executor)
            .await
        }
    }
}

#[derive(Debug, PartialEq)]
struct BaseFeePerGasMinMax {
    min: WeiF64,
    max: WeiF64,
}

async fn get_base_fee_per_gas_min_max(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> sqlx::Result<BaseFeePerGasMinMax> {
    match time_frame {
        TimeFrame::All => {
            event!(Level::WARN, "getting the min and max base fee per gas for all blocks is low, and may be incorrect depending on blocks_next backfill status");
            sqlx::query(
                "
                    SELECT
                        MIN(base_fee_per_gas),
                        MAX(base_fee_per_gas)
                    FROM
                        blocks_next
                    WHERE
                    
                ",
            )
            .map(|row: PgRow| {
                let min = row.get::<i64, _>("min") as f64;
                let max = row.get::<i64, _>("max") as f64;
                BaseFeePerGasMinMax { min, max }
            })
            .fetch_one(executor)
            .await
        }
        TimeFrame::LimitedTimeFrame(limited_time_frame) => {
            sqlx::query(
                "
                    SELECT
                        MIN(base_fee_per_gas),
                        MAX(base_fee_per_gas)
                    FROM
                        blocks_next
                        WHERE
                            timestamp >= NOW() - $1
                ",
            )
            .bind(limited_time_frame.get_postgres_interval())
            .map(|row: PgRow| {
                let min = row.get::<i64, _>("min") as f64;
                let max = row.get::<i64, _>("max") as f64;
                BaseFeePerGasMinMax { min, max }
            })
            .fetch_one(executor)
            .await
        }
    }
}

async fn get_base_fee_per_gas_stats_time_frame(
    executor: &PgPool,
    time_frame: &TimeFrame,
) -> sqlx::Result<BaseFeePerGasStatsTimeFrame> {
    let BaseFeePerGasMinMax { min, max } =
        get_base_fee_per_gas_min_max(executor, time_frame).await?;

    let average = get_base_fee_per_gas_average(executor, time_frame).await?;

    Ok(BaseFeePerGasStatsTimeFrame { min, max, average })
}

async fn update_base_fee_stats(
    executor: &PgPool,
    block: &ExecutionNodeBlock,
) -> anyhow::Result<()> {
    event!(Level::DEBUG, "updating base fee over time");

    let issuance =
        beacon_chain::get_last_week_issuance(&mut executor.acquire().await.unwrap()).await;

    event!(Level::DEBUG, "issuance: {issuance}");

    let barrier = get_barrier(issuance.0 as f64);

    event!(Level::DEBUG, "barrier: {barrier}");

    let base_fee_over_time = BaseFeeOverTime {
        barrier,
        block_number: block.number,
        m5: get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await?,
        h1: get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await?,
        d1: get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Day1),
        )
        .await?,
        d7: get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Day1),
        )
        .await?,
        d30: get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Day1),
        )
        .await?,
        all: None,
    };

    key_value_store::set_value(
        executor,
        CacheKey::BaseFeeOverTime.to_db_key(),
        &serde_json::to_value(base_fee_over_time).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeeOverTime).await;

    let m5 = get_base_fee_per_gas_stats_time_frame(
        executor,
        &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
    )
    .await?;

    let h1 = get_base_fee_per_gas_stats_time_frame(
        executor,
        &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
    )
    .await?;

    let d1 = get_base_fee_per_gas_stats_time_frame(
        executor,
        &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
    )
    .await?;

    let d7 = get_base_fee_per_gas_stats_time_frame(
        executor,
        &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
    )
    .await?;

    let d30 = get_base_fee_per_gas_stats_time_frame(
        executor,
        &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
    )
    .await?;

    let base_fee_per_gas_stats = BaseFeePerGasStats {
        all: None,
        average: h1.average,
        barrier,
        block_number: block.number,
        d1,
        d30,
        d7,
        m5,
        max: h1.max,
        min: h1.min,
        h1,
        timestamp: block.timestamp,
    };

    key_value_store::set_value(
        executor,
        CacheKey::BaseFeePerGasStats.to_db_key(),
        &serde_json::to_value(base_fee_per_gas_stats).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeePerGasStats).await;

    Ok(())
}

#[derive(Debug, Serialize)]
struct BaseFeePerGasStatsTimeFrame {
    average: WeiF64,
    max: WeiF64,
    min: WeiF64,
}

#[derive(Serialize)]
struct BaseFeePerGasStats {
    #[deprecated = "use h1 instead"]
    average: WeiF64,
    #[deprecated = "use h1 instead"]
    max: WeiF64,
    #[deprecated = "use h1 instead"]
    min: WeiF64,
    all: Option<BaseFeePerGasStatsTimeFrame>,
    barrier: WeiF64,
    block_number: u32,
    d1: BaseFeePerGasStatsTimeFrame,
    d30: BaseFeePerGasStatsTimeFrame,
    d7: BaseFeePerGasStatsTimeFrame,
    h1: BaseFeePerGasStatsTimeFrame,
    m5: BaseFeePerGasStatsTimeFrame,
    timestamp: DateTime<Utc>,
}

pub async fn on_new_block(db_pool: &PgPool, block: &ExecutionNodeBlock) -> anyhow::Result<()> {
    try_join!(
        update_last_base_fee(db_pool, block).instrument(debug_span!("update_last_base_fee")),
        update_base_fee_stats(db_pool, block).instrument(debug_span!("update_base_fee_stats"))
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, SubsecRound};
    use sqlx::Acquire;

    use crate::{
        db_testing, execution_chain::block_store::BlockStore, time_frames::LimitedTimeFrame,
    };

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
    async fn get_base_fee_over_time_h1_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut transaction);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;

        let base_fees_d1 = get_base_fee_over_time(
            &mut transaction,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(
            base_fees_d1,
            vec![BaseFeeAtTime {
                block_number: 0,
                timestamp: test_block.timestamp,
                wei: 1,
            }]
        );
    }

    #[tokio::test]
    async fn get_base_fee_over_time_asc_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut transaction);
        let test_block_1 = make_test_block();
        let test_block_2 = ExecutionNodeBlock {
            hash: "0xtest2".to_string(),
            parent_hash: "0xtest".to_string(),
            number: 1,
            timestamp: Utc::now().trunc_subsecs(0) + Duration::days(1),
            ..make_test_block()
        };

        block_store.store_block(&test_block_1, 0.0).await;
        block_store.store_block(&test_block_2, 0.0).await;

        let base_fees_h1 = get_base_fee_over_time(
            &mut transaction,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(
            base_fees_h1,
            vec![
                BaseFeeAtTime {
                    wei: 1,
                    block_number: 0,
                    timestamp: test_block_1.timestamp
                },
                BaseFeeAtTime {
                    wei: 1,
                    block_number: 1,
                    timestamp: test_block_2.timestamp
                }
            ]
        );
    }

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

    #[tokio::test]
    async fn get_average_fee_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut transaction);
        let test_block_1 = ExecutionNodeBlock {
            gas_used: 10,
            base_fee_per_gas: 10,
            ..make_test_block()
        };
        let test_block_2 = ExecutionNodeBlock {
            gas_used: 20,
            base_fee_per_gas: 20,
            hash: "0xtest2".to_string(),
            parent_hash: "0xtest".to_string(),
            number: 1,
            ..make_test_block()
        };

        block_store.store_block(&test_block_1, 0.0).await;
        block_store.store_block(&test_block_2, 0.0).await;

        let average_base_fee_per_gas = get_base_fee_per_gas_average(
            &mut transaction,
            &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(average_base_fee_per_gas, 16.666666666666668);
    }

    #[tokio::test]
    async fn get_average_fee_within_range_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut transaction);
        let test_block_in_range = ExecutionNodeBlock {
            gas_used: 10,
            base_fee_per_gas: 10,
            hash: "0xtest1".to_string(),
            ..make_test_block()
        };
        let test_block_outside_range = ExecutionNodeBlock {
            gas_used: 20,
            base_fee_per_gas: 20,
            timestamp: Utc::now().trunc_subsecs(0) - Duration::minutes(6),
            hash: "0xtest2".to_string(),
            parent_hash: "0xtest".to_string(),
            number: 1,
            ..make_test_block()
        };

        block_store.store_block(&test_block_in_range, 0.0).await;
        block_store
            .store_block(&test_block_outside_range, 0.0)
            .await;

        let average_base_fee_per_gas = get_base_fee_per_gas_average(
            &mut transaction,
            &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Minute5),
        )
        .await
        .unwrap();

        assert_eq!(average_base_fee_per_gas, 10.0);
    }

    #[tokio::test]
    async fn get_base_fee_per_gas_min_max_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut transaction);
        let test_block_1 = ExecutionNodeBlock {
            gas_used: 10,
            base_fee_per_gas: 10,
            hash: "0xtest1".to_string(),
            ..make_test_block()
        };
        let test_block_2 = ExecutionNodeBlock {
            gas_used: 20,
            base_fee_per_gas: 20,
            hash: "0xtest2".to_string(),
            parent_hash: "0xtest".to_string(),
            number: 1,
            ..make_test_block()
        };

        block_store.store_block(&test_block_1, 0.0).await;
        block_store.store_block(&test_block_2, 0.0).await;

        let base_fee_per_gas_min_max = get_base_fee_per_gas_min_max(
            &mut transaction,
            &TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(
            base_fee_per_gas_min_max,
            BaseFeePerGasMinMax {
                min: 10.0,
                max: 20.0
            }
        );
    }
}
