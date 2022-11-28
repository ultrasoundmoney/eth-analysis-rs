use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use tracing::{debug, warn};

use crate::{
    caching::{self, CacheKey},
    eth_units::WeiF64,
    execution_chain::{BlockNumber, ExecutionNodeBlock},
    key_value_store,
    time_frames::{LimitedTimeFrame, TimeFrame},
};

async fn get_base_fee_per_gas_average(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> sqlx::Result<WeiF64> {
    match time_frame {
        TimeFrame::SinceMerge => {
            sqlx::query(
                "
                    SELECT
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS average
                    FROM
                        blocks
                    WHERE
                        mined_at >= '2022-09-15T06:42:42Z'::TIMESTAMPTZ
                ",
            )
            .map(|row: PgRow| row.get::<f64, _>("average"))
            .fetch_one(executor)
            .await
        },
        TimeFrame::SinceBurn => {
            warn!("getting average fee for time frame 'since_burn' is slow, and may be incorrect depending on blocks_next backfill status");
            sqlx::query(
                "
                    SELECT
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS average
                    FROM
                        blocks
                ",
            )
            .map(|row: PgRow| row.get::<f64, _>("average"))
            .fetch_one(executor)
            .await
        },
        TimeFrame::Limited(limited_time_frame) => {
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
        TimeFrame::SinceMerge => {
            sqlx::query(
                "
                    SELECT
                        MIN(base_fee_per_gas),
                        MAX(base_fee_per_gas)
                    FROM
                        blocks
                    WHERE
                        mined_at >= '2022-09-15T06:42:42Z'::TIMESTAMPTZ
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
        TimeFrame::SinceBurn => {
            sqlx::query(
                "
                    SELECT
                        MIN(base_fee_per_gas),
                        MAX(base_fee_per_gas)
                    FROM
                        blocks
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
        TimeFrame::Limited(limited_time_frame) => {
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

#[derive(Clone, Debug, Serialize)]
struct BaseFeePerGasStatsTimeFrame {
    average: WeiF64,
    max: WeiF64,
    min: WeiF64,
}

#[derive(Serialize)]
struct BaseFeePerGasStats {
    all: Option<BaseFeePerGasStatsTimeFrame>,
    barrier: WeiF64,
    block_number: BlockNumber,
    d1: BaseFeePerGasStatsTimeFrame,
    d30: BaseFeePerGasStatsTimeFrame,
    d7: BaseFeePerGasStatsTimeFrame,
    h1: BaseFeePerGasStatsTimeFrame,
    m5: BaseFeePerGasStatsTimeFrame,
    since_burn: BaseFeePerGasStatsTimeFrame,
    since_merge: Option<BaseFeePerGasStatsTimeFrame>,
    timestamp: DateTime<Utc>,
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

pub async fn update_base_fee_stats(
    executor: &PgPool,
    barrier: f64,
    block: &ExecutionNodeBlock,
) -> anyhow::Result<()> {
    debug!("updating base fee over time");

    debug!("barrier: {barrier}");

    let (m5, h1, d1, d7, d30, since_burn, since_merge) = try_join!(
        get_base_fee_per_gas_stats_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        ),
        get_base_fee_per_gas_stats_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        ),
        get_base_fee_per_gas_stats_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Day1),
        ),
        get_base_fee_per_gas_stats_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Day7),
        ),
        get_base_fee_per_gas_stats_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Day30),
        ),
        get_base_fee_per_gas_stats_time_frame(executor, &TimeFrame::SinceBurn),
        get_base_fee_per_gas_stats_time_frame(executor, &TimeFrame::SinceMerge)
    )?;

    let base_fee_per_gas_stats = BaseFeePerGasStats {
        all: Some(since_burn.clone()),
        barrier,
        block_number: block.number,
        d1,
        d30,
        d7,
        h1,
        m5,
        timestamp: block.timestamp,
        since_burn,
        since_merge: Some(since_merge),
    };

    key_value_store::set_value(
        executor,
        CacheKey::BaseFeePerGasStats.to_db_key(),
        &serde_json::to_value(base_fee_per_gas_stats).unwrap(),
    )
    .await?;

    caching::publish_cache_update(executor, CacheKey::BaseFeePerGasStats).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, SubsecRound};
    use sqlx::Acquire;

    use crate::{db, execution_chain::BlockStore};

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
    async fn get_average_fee_test() {
        let mut connection = db::get_test_db().await;
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
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(average_base_fee_per_gas, 16.666666666666668);
    }

    #[tokio::test]
    async fn get_average_fee_within_range_test() {
        let mut connection = db::get_test_db().await;
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
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        )
        .await
        .unwrap();

        assert_eq!(average_base_fee_per_gas, 10.0);
    }

    #[tokio::test]
    async fn get_base_fee_per_gas_min_max_test() {
        let mut connection = db::get_test_db().await;
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
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
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
