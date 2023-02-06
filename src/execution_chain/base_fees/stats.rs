use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use tracing::{debug, warn};

use crate::{
    caching::{self, CacheKey},
    execution_chain::{BlockNumber, ExecutionNodeBlock},
    key_value_store,
    performance::TimedExt,
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
    units::WeiF64,
};

use GrowingTimeFrame::*;

async fn get_base_fee_per_gas_average(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> WeiF64 {
    match time_frame {
        TimeFrame::Growing(growing_time_frame) => {
                warn!(time_frame = %growing_time_frame, "getting average fee for growing time frame is slow");
                sqlx::query!(
                    r#"
                    SELECT
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS "average!"
                    FROM
                        blocks_next
                    WHERE
                        number >= $1
                    "#,
                    growing_time_frame.start_block_number()
                )
                .fetch_one(executor)
                .await.unwrap().average
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
            .bind(limited_time_frame.postgres_interval())
            .map(|row: PgRow| row.get::<f64, _>("average"))
            .fetch_one(executor)
            .await.unwrap()
        }
    }
}

#[derive(Debug, PartialEq)]
struct BaseFeePerGasMinMax {
    max: WeiF64,
    max_block_number: BlockNumber,
    min: WeiF64,
    min_block_number: BlockNumber,
}

// This fn expects to be called after the `blocks` table is in sync.
async fn get_base_fee_per_gas_min(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> (BlockNumber, WeiF64) {
    match time_frame {
        TimeFrame::Growing(SinceMerge) => {
            let row = sqlx::query!(
                r#"
                SELECT
                    number,
                    base_fee_per_gas AS "base_fee_per_gas!"
                FROM
                    blocks_next
                WHERE
                    timestamp >= '2022-09-15T06:42:42Z'::TIMESTAMPTZ
                ORDER BY base_fee_per_gas ASC
                LIMIT 1
                "#,
            )
            .fetch_one(executor)
            .await
            .unwrap();

            (row.number, row.base_fee_per_gas as f64)
        }
        TimeFrame::Growing(SinceBurn) => {
            let row = sqlx::query!(
                r#"
                SELECT
                    number,
                    base_fee_per_gas AS "base_fee_per_gas!"
                FROM
                    blocks_next
                ORDER BY base_fee_per_gas ASC
                LIMIT 1
                "#,
            )
            .fetch_one(executor)
            .await
            .unwrap();

            (row.number, row.base_fee_per_gas as f64)
        }
        TimeFrame::Limited(limited_time_frame) => {
            // Using a CTE because Postgres will ...
            let row = sqlx::query!(
                r#"
                WITH blocks_in_time_frame AS (
                    SELECT
                        number,
                        base_fee_per_gas
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1::INTERVAL
                )
                SELECT
                    number,
                    base_fee_per_gas AS "base_fee_per_gas!"
                FROM
                    blocks_in_time_frame
                ORDER BY base_fee_per_gas ASC
                LIMIT 1
                "#,
                limited_time_frame.postgres_interval(),
            )
            .fetch_one(executor)
            .await
            .unwrap();

            (row.number, row.base_fee_per_gas as f64)
        }
    }
}

// This fn expects to be called after the `blocks` table is in sync.
async fn get_base_fee_per_gas_max(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> (BlockNumber, WeiF64) {
    match time_frame {
        TimeFrame::Growing(SinceMerge) => {
            let row = sqlx::query!(
                r#"
                SELECT
                    number,
                    base_fee_per_gas AS "base_fee_per_gas!"
                FROM
                    blocks_next
                WHERE
                    timestamp >= '2022-09-15T06:42:42Z'::TIMESTAMPTZ
                ORDER BY base_fee_per_gas DESC
                LIMIT 1
                "#,
            )
            .fetch_one(executor)
            .await
            .unwrap();

            (row.number, row.base_fee_per_gas as f64)
        }
        TimeFrame::Growing(SinceBurn) => {
            let row = sqlx::query!(
                r#"
                SELECT
                    number,
                    base_fee_per_gas AS "base_fee_per_gas!"
                FROM
                    blocks_next
                ORDER BY base_fee_per_gas DESC
                LIMIT 1
                "#,
            )
            .fetch_one(executor)
            .await
            .unwrap();

            (row.number, row.base_fee_per_gas as f64)
        }
        TimeFrame::Limited(limited_time_frame) => {
            let row = sqlx::query!(
                r#"
                WITH blocks_in_time_frame AS (
                    SELECT
                        number,
                        base_fee_per_gas
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1::INTERVAL
                )
                    SELECT
                        number,
                        base_fee_per_gas AS "base_fee_per_gas!"
                    FROM
                        blocks_in_time_frame
                    ORDER BY base_fee_per_gas DESC
                    LIMIT 1
                "#,
                limited_time_frame.postgres_interval(),
            )
            .fetch_one(executor)
            .await
            .unwrap();

            (row.number, row.base_fee_per_gas as f64)
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct BaseFeePerGasStatsTimeFrame {
    average: WeiF64,
    max: WeiF64,
    max_block_number: BlockNumber,
    min: WeiF64,
    min_block_number: BlockNumber,
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

async fn base_fee_per_gas_stats_from_time_frame(
    executor: &PgPool,
    time_frame: &TimeFrame,
) -> BaseFeePerGasStatsTimeFrame {
    let ((min_block_number, min), (max_block_number, max), average) = join!(
        get_base_fee_per_gas_min(executor, time_frame)
            .timed(&format!("get_base_fee_per_gas_min_{time_frame}")),
        get_base_fee_per_gas_max(executor, time_frame)
            .timed(&format!("get_base_fee_per_gas_max_{time_frame}")),
        get_base_fee_per_gas_average(executor, time_frame)
            .timed(&format!("get_base_fee_per_gas_average_{time_frame}"))
    );

    BaseFeePerGasStatsTimeFrame {
        average,
        max,
        max_block_number,
        min,
        min_block_number,
    }
}

pub async fn update_base_fee_stats(executor: &PgPool, barrier: f64, block: &ExecutionNodeBlock) {
    debug!("updating base fee over time");

    let (m5, h1, d1, d7, d30, since_burn, since_merge) = join!(
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        )
        .timed("base_fee_per_gas_stats_minute5"),
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1)
        )
        .timed("base_fee_per_gas_stats_hour1"),
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Day1),
        )
        .timed("base_fee_per_gas_stats_day1"),
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Day7),
        )
        .timed("base_fee_per_gas_stats_day7"),
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Limited(LimitedTimeFrame::Day30),
        )
        .timed("base_fee_per_gas_stats_day30"),
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Growing(GrowingTimeFrame::SinceBurn)
        )
        .timed("base_fee_per_gas_stats_since_burn"),
        base_fee_per_gas_stats_from_time_frame(
            executor,
            &TimeFrame::Growing(GrowingTimeFrame::SinceMerge)
        )
        .timed("base_fee_per_gas_stats_since_merge"),
    );

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
    .await
    .unwrap();

    caching::publish_cache_update(executor, &CacheKey::BaseFeePerGasStats)
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, SubsecRound};
    use sqlx::Acquire;

    use crate::{db, execution_chain};

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
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

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

        execution_chain::store_block(&mut transaction, &test_block_1, 0.0).await;
        execution_chain::store_block(&mut transaction, &test_block_2, 0.0).await;

        let average_base_fee_per_gas = get_base_fee_per_gas_average(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await;

        assert_eq!(average_base_fee_per_gas, 16.666666666666668);
    }

    #[tokio::test]
    async fn get_average_fee_within_range_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

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

        execution_chain::store_block(&mut transaction, &test_block_in_range, 0.0).await;
        execution_chain::store_block(&mut transaction, &test_block_outside_range, 0.0).await;

        let average_base_fee_per_gas = get_base_fee_per_gas_average(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Minute5),
        )
        .await;

        assert_eq!(average_base_fee_per_gas, 10.0);
    }

    #[tokio::test]
    async fn base_fee_per_gas_min_max_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

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

        execution_chain::store_block(&mut transaction, &test_block_1, 0.0).await;
        execution_chain::store_block(&mut transaction, &test_block_2, 0.0).await;

        let base_fee_per_gas_min = get_base_fee_per_gas_min(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await;
        assert_eq!(base_fee_per_gas_min, (0, 10.0));

        let base_fee_per_gas_max = get_base_fee_per_gas_max(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await;
        assert_eq!(base_fee_per_gas_max, (1, 20.0));
    }
}
