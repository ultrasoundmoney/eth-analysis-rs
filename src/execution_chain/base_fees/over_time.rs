use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use tracing::debug;

use crate::time_frames::LimitedTimeFrame::*;
use crate::{
    caching::{self, CacheKey},
    eth_units::WeiF64,
    execution_chain::BlockNumber,
    key_value_store,
    time_frames::TimeFrame,
};

#[derive(Debug, PartialEq, Serialize)]
struct BaseFeeAtTime {
    block_number: Option<BlockNumber>,
    timestamp: DateTime<Utc>,
    wei: WeiF64,
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
    time_frame: &TimeFrame,
) -> sqlx::Result<Vec<BaseFeeAtTime>> {
    match time_frame {
        TimeFrame::All => {
            debug!("getting base fee over time since burn is slow");
            // Getting base fees since burn is slow. ~5s as of Oct 24 2022 or 2.7M blocks.
            // To improve performance, switch to calculating aggregates once, and storing them in a
            // table.
            sqlx::query(
                "
                    SELECT
                        DATE_TRUNC('day', mined_at) AS day_timestamp,
                        SUM(base_fee_per_gas::float8 * gas_used::float8) / SUM(gas_used::float8) AS base_fee_per_gas
                    FROM
                        blocks
                    GROUP BY day_timestamp
                    ORDER BY day_timestamp ASC
                ",
            )
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("day_timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    block_number: None,
                    timestamp,
                    wei,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::Limited(ltf @ Minute5)
        | TimeFrame::Limited(ltf @ Hour1) => {
            sqlx::query(
                "
                    SELECT
                        timestamp,
                        base_fee_per_gas::FLOAT8,
                        number
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1
                    ORDER BY number ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let block_number: BlockNumber = row
                    .get::<i32, _>("number")
                    .try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    block_number: Some(block_number),
                    timestamp,
                    wei,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::Limited(ltf @ Day1) => {
            sqlx::query(
                "
                    SELECT
                        DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS base_fee_per_gas
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1
                    GROUP BY minute_timestamp
                    ORDER BY minute_timestamp ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("minute_timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    block_number: None,
                    timestamp,
                    wei,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::Limited(ltf @ Day7) => {
            sqlx::query(
                "
                    SELECT
                        DATE_BIN('5 minutes', timestamp, '2022-01-01') AS five_minute_timestamp,
                        SUM(base_fee_per_gas::FLOAT8 * gas_used::FLOAT8) / SUM(gas_used::FLOAT8) AS base_fee_per_gas
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1
                    GROUP BY five_minute_timestamp
                    ORDER BY five_minute_timestamp ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("five_minute_timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    block_number: None,
                    timestamp,
                    wei,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::Limited(ltf @ Day30) => {
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas)::FLOAT8 AS base_fee_per_gas,
                        DATE_TRUNC('hour', timestamp) AS timestamp
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= NOW() - $1
                    GROUP BY 2
                    ORDER BY 2 ASC
                ",
            )
            .bind(ltf.get_postgres_interval())
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    block_number: None,
                    timestamp,
                    wei,
                }
            })
            .fetch_all(executor)
            .await
        }
    }
}

pub async fn update_base_fee_over_time(
    executor: &PgPool,
    barrier: f64,
    block_number: &BlockNumber,
) -> Result<()> {
    let (m5, h1, d1, d7, d30, all) = try_join!(
        get_base_fee_over_time(executor, &TimeFrame::Limited(Minute5)),
        get_base_fee_over_time(executor, &TimeFrame::Limited(Hour1)),
        get_base_fee_over_time(executor, &TimeFrame::Limited(Day1),),
        get_base_fee_over_time(executor, &TimeFrame::Limited(Day7),),
        get_base_fee_over_time(executor, &TimeFrame::Limited(Day30)),
        get_base_fee_over_time(executor, &TimeFrame::All)
    )?;

    let base_fee_over_time = BaseFeeOverTime {
        barrier,
        block_number: *block_number,
        m5,
        h1,
        d1,
        d7,
        d30,
        all: Some(all),
    };

    key_value_store::set_value(
        executor,
        CacheKey::BaseFeeOverTime.to_db_key(),
        &serde_json::to_value(base_fee_over_time).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeeOverTime).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, SubsecRound};
    use sqlx::Acquire;

    use crate::{
        db_testing,
        execution_chain::{block_store::BlockStore, ExecutionNodeBlock},
        time_frames::LimitedTimeFrame,
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
        let excluded_block = ExecutionNodeBlock {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(2),
            ..make_test_block()
        };
        let included_block = ExecutionNodeBlock {
            hash: "0xtest2".to_string(),
            parent_hash: excluded_block.hash.clone(),
            number: excluded_block.number + 1,
            ..make_test_block()
        };

        block_store.store_block(&excluded_block, 0.0).await;
        block_store.store_block(&included_block, 0.0).await;

        let base_fees_d1 = get_base_fee_over_time(
            &mut transaction,
            &TimeFrame::Limited(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(
            base_fees_d1,
            vec![BaseFeeAtTime {
                block_number: Some(included_block.number),
                timestamp: included_block.timestamp,
                wei: 1.0,
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

        let base_fees_h1 = get_base_fee_over_time(&mut transaction, &TimeFrame::Limited(Hour1))
            .await
            .unwrap();

        assert_eq!(
            base_fees_h1,
            vec![
                BaseFeeAtTime {
                    wei: 1.0,
                    block_number: Some(0),
                    timestamp: test_block_1.timestamp
                },
                BaseFeeAtTime {
                    wei: 1.0,
                    block_number: Some(1),
                    timestamp: test_block_2.timestamp
                }
            ]
        );
    }
}
