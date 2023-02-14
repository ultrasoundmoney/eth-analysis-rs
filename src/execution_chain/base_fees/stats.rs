use std::collections::HashMap;

use cached::proc_macro::cached;
use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use tracing::{debug, warn};

use crate::{
    caching::{self, CacheKey},
    execution_chain::{BlockNumber, ExecutionNodeBlock},
    performance::TimedExt,
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
    units::WeiF64,
};

use GrowingTimeFrame::*;

use super::barrier::Barrier;

async fn base_fee_per_gas_average(executor: impl PgExecutor<'_>, time_frame: &TimeFrame) -> WeiF64 {
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
async fn base_fee_per_gas_min(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> (BlockNumber, WeiF64) {
    match time_frame {
        TimeFrame::Growing(SinceMerge) => sqlx::query!(
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
        .map(|row| (row.number, row.base_fee_per_gas as f64))
        .unwrap(),
        TimeFrame::Growing(SinceBurn) => sqlx::query!(
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
        .map(|row| (row.number, row.base_fee_per_gas as f64))
        .unwrap(),
        TimeFrame::Limited(limited_time_frame) => sqlx::query!(
            r#"
            SELECT
                number,
                base_fee_per_gas AS "base_fee_per_gas!"
            FROM
                blocks_next
            WHERE
                timestamp >= NOW() - $1::INTERVAL
            ORDER BY base_fee_per_gas ASC
            LIMIT 1
            "#,
            limited_time_frame.postgres_interval(),
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.base_fee_per_gas as f64))
        .unwrap(),
    }
}

// This fn expects to be called after the `blocks` table is in sync.
async fn base_fee_per_gas_max(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> (BlockNumber, WeiF64) {
    match time_frame {
        TimeFrame::Growing(growing_time_frame) => sqlx::query!(
            r#"
            SELECT
                number,
                base_fee_per_gas AS "base_fee_per_gas!"
            FROM
                blocks_next
            WHERE
                timestamp >= $1
            ORDER BY base_fee_per_gas DESC
            LIMIT 1
            "#,
            growing_time_frame.start_timestamp()
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.base_fee_per_gas as f64))
        .unwrap(),
        TimeFrame::Limited(limited_time_frame) => sqlx::query!(
            r#"
            SELECT
                number,
                base_fee_per_gas AS "base_fee_per_gas!"
            FROM
                blocks_next
            WHERE
                timestamp >= NOW() - $1::INTERVAL
            ORDER BY base_fee_per_gas DESC
            LIMIT 1
            "#,
            limited_time_frame.postgres_interval(),
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.base_fee_per_gas as f64))
        .unwrap(),
    }
}

#[derive(Clone, Debug, Serialize)]
struct BaseFeePerGasStats {
    average: WeiF64,
    block_number: BlockNumber,
    max: WeiF64,
    max_block_number: BlockNumber,
    min: WeiF64,
    min_block_number: BlockNumber,
    timestamp: DateTime<Utc>,
}

#[cached(key = "String", convert = r#"{time_frame.to_string()}"#, time = 3600)]
async fn base_fee_per_gas_stats_one_hour_cached(
    db_pool: &PgPool,
    time_frame: &TimeFrame,
    block: &ExecutionNodeBlock,
) -> BaseFeePerGasStats {
    BaseFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
}

#[cached(key = "String", convert = r#"{time_frame.to_string()}"#, time = 600)]
async fn base_fee_per_gas_stats_ten_minutes_cached(
    db_pool: &PgPool,
    time_frame: &TimeFrame,
    block: &ExecutionNodeBlock,
) -> BaseFeePerGasStats {
    BaseFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
}

impl BaseFeePerGasStats {
    async fn from_time_frame(
        executor: &PgPool,
        time_frame: &TimeFrame,
        block: &ExecutionNodeBlock,
    ) -> Self {
        let ((min_block_number, min), (max_block_number, max), average) = join!(
            base_fee_per_gas_min(executor, time_frame)
                .timed(&format!("base_fee_per_gas_min_{time_frame}")),
            base_fee_per_gas_max(executor, time_frame)
                .timed(&format!("base_fee_per_gas_max_{time_frame}")),
            base_fee_per_gas_average(executor, time_frame)
                .timed(&format!("base_fee_per_gas_average_{time_frame}")),
        );

        Self {
            average,
            block_number: block.number,
            max,
            max_block_number,
            min,
            min_block_number,
            timestamp: block.timestamp,
        }
    }

    async fn from_time_frame_cached(
        db_pool: &PgPool,
        time_frame: &TimeFrame,
        block: &ExecutionNodeBlock,
    ) -> Self {
        match time_frame {
            TimeFrame::Growing(SinceBurn) => {
                base_fee_per_gas_stats_one_hour_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Growing(SinceMerge) => {
                base_fee_per_gas_stats_one_hour_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Day30) => {
                base_fee_per_gas_stats_one_hour_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Day7) => {
                base_fee_per_gas_stats_ten_minutes_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Day1) => {
                base_fee_per_gas_stats_ten_minutes_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Hour1) => {
                BaseFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Minute5) => {
                BaseFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
            }
        }
    }
}

// TODO: top level time frames should be removed once the frontend has switched over to
// base_fee_per_gas_stats. Barrier should be its own endpoint.
#[derive(Serialize)]
struct BaseFeePerGasStatsEnvelope {
    all: Option<BaseFeePerGasStats>,
    barrier: Barrier,
    base_fee_per_gas_stats: HashMap<TimeFrame, BaseFeePerGasStats>,
    block_number: BlockNumber,
    d1: BaseFeePerGasStats,
    d30: BaseFeePerGasStats,
    d7: BaseFeePerGasStats,
    h1: BaseFeePerGasStats,
    m5: BaseFeePerGasStats,
    since_burn: BaseFeePerGasStats,
    since_merge: Option<BaseFeePerGasStats>,
    timestamp: DateTime<Utc>,
}

pub async fn update_base_fee_stats(
    executor: &PgPool,
    barrier: Barrier,
    block: &ExecutionNodeBlock,
) {
    use GrowingTimeFrame::*;
    use LimitedTimeFrame::*;
    use TimeFrame::*;

    debug!("updating base fee over time");

    let (since_burn, since_merge, d30, d7, d1, h1, m5) = join!(
        BaseFeePerGasStats::from_time_frame_cached(executor, &Growing(SinceBurn), block,),
        BaseFeePerGasStats::from_time_frame_cached(executor, &Growing(SinceMerge), block,),
        BaseFeePerGasStats::from_time_frame_cached(executor, &Limited(Day30), block,),
        BaseFeePerGasStats::from_time_frame_cached(executor, &Limited(Day7), block,),
        BaseFeePerGasStats::from_time_frame_cached(executor, &Limited(Day1), block,),
        BaseFeePerGasStats::from_time_frame_cached(executor, &Limited(Hour1), block,),
        BaseFeePerGasStats::from_time_frame_cached(executor, &Limited(Minute5), block,),
    );

    // TODO: after the frontend converts to using the hashmap, remove the individual time frame
    // calls above and simply iterate through all time frames, then collect the Vec into a HashMap.
    let mut base_fee_per_gas_stats = HashMap::new();
    base_fee_per_gas_stats.insert(Growing(SinceBurn), since_burn.clone());
    base_fee_per_gas_stats.insert(Growing(SinceMerge), since_merge.clone());
    base_fee_per_gas_stats.insert(Limited(Day30), d30.clone());
    base_fee_per_gas_stats.insert(Limited(Day7), d7.clone());
    base_fee_per_gas_stats.insert(Limited(Day1), d1.clone());
    base_fee_per_gas_stats.insert(Limited(Hour1), h1.clone());
    base_fee_per_gas_stats.insert(Limited(Minute5), m5.clone());

    // Update the cache for each time frame, stats pair.
    for (time_frame, stats) in base_fee_per_gas_stats.iter() {
        caching::update_and_publish(
            executor,
            &CacheKey::BaseFeePerGasStatsTimeFrame(*time_frame),
            stats,
        )
        .await
        .unwrap();
    }

    let base_fee_per_gas_stats_envelope = BaseFeePerGasStatsEnvelope {
        all: Some(since_burn.clone()),
        barrier,
        base_fee_per_gas_stats,
        block_number: block.number,
        d1,
        d30,
        d7,
        h1,
        m5,
        since_burn,
        since_merge: Some(since_merge),
        timestamp: block.timestamp,
    };

    caching::update_and_publish(
        executor,
        &CacheKey::BaseFeePerGasStats,
        &base_fee_per_gas_stats_envelope,
    )
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

        let average_base_fee_per_gas = base_fee_per_gas_average(
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

        let average_base_fee_per_gas = base_fee_per_gas_average(
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

        let base_fee_per_gas_min = base_fee_per_gas_min(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await;
        assert_eq!(base_fee_per_gas_min, (0, 10.0));

        let base_fee_per_gas_max = base_fee_per_gas_max(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await;
        assert_eq!(base_fee_per_gas_max, (1, 20.0));
    }
}
