use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use tracing::warn;

use crate::{
    caching::{self, CacheKey},
    eth_units::WeiF64,
    execution_chain::BlockNumber,
    key_value_store,
    time_frames::{LimitedTimeFrame, TimeFrame},
};

#[derive(Debug, PartialEq, Serialize)]
struct BaseFeeAtTime {
    block_number: BlockNumber,
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
    time_frame: TimeFrame,
) -> sqlx::Result<Vec<BaseFeeAtTime>> {
    match time_frame {
        TimeFrame::All => {
            warn!("getting base fee over time for time frame 'all' is slow, and may be incorrect depending on blocks_next backfill status");
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas)::FLOAT8 AS base_fee_per_gas,
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
                let wei = row.get::<f64, _>("base_fee_per_gas");
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
                        base_fee_per_gas::FLOAT8,
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
                let wei = row.get::<f64, _>("base_fee_per_gas");
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
                        AVG(base_fee_per_gas)::FLOAT8 AS base_fee_per_gas,
                        DATE_TRUNC('minute', timestamp) AS timestamp,
                        MAX(number) AS number
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
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    wei,
                    block_number,
                    timestamp,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Day7) => {
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas)::FLOAT8 AS base_fee_per_gas,
                        DATE_BIN('5 minutes', timestamp, '2022-01-01') AS timestamp,
                        MAX(number) AS number
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
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
                BaseFeeAtTime {
                    wei,
                    block_number,
                    timestamp,
                }
            })
            .fetch_all(executor)
            .await
        }
        TimeFrame::LimitedTimeFrame(ltf @ LimitedTimeFrame::Day30) => {
            sqlx::query(
                "
                    SELECT
                        AVG(base_fee_per_gas)::FLOAT8 AS base_fee_per_gas,
                        DATE_TRUNC('hour', timestamp) AS timestamp,
                        MAX(number) AS number
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
                let block_number: BlockNumber = row.get::<i32, _>("number").try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let wei = row.get::<f64, _>("base_fee_per_gas");
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

async fn drop_before(
    executor: impl PgExecutor<'_>,
    limited_time_frame: &LimitedTimeFrame,
    less_than: &BlockNumber,
) -> sqlx::Result<()> {
    sqlx::query(
        "
            DELETE FROM
                base_fee_over_time
            JOIN
                blocks_next
            ON
                base_fee_over_time.block_number = blocks_next.number
            WHERE
                timestamp < NOW() - $1
                time_frame = $2
        ",
    )
    .bind(limited_time_frame.get_postgres_interval())
    .bind(*less_than as i32)
    .execute(executor)
    .await?;

    Ok(())
}

async fn add_new(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
    last_added: &Option<BlockNumber>,
    block_number: &BlockRange,
) -> sqlx::Result<()> {
    unimplemented!()
}

async fn update_base_fee_over_time_time_frame(
    mut executor: PgConnection,
    time_frame: &TimeFrame,
    block_number: &BlockNumber,
) -> Result<()> {
    use TimeFrame::*;

    let mut tx = executor.begin().await?;

    // Get the state of block analysis for this time frame.
    let analysis_state =
        analysis_state::get_analysis_state(&mut tx, &AnalysisKey::BaseFeeOverTime, time_frame)
            .await?;

        let earliest_block_number_for_time_frame = match time_frame {
            All => LONDON_HARDFORK_BLOCK_NUMBER, 
            LimitedTimeFrame(limited_time_frame) => time_frames::get_earliest_block_number(
                &mut tx,
                limited_time_frame,
            )
            .await?
            .expect(
&format!("tried to expire base fees for limited time frame but no blocks within time frame {limited_time_frame}"),
            )
        };

    // If we're dealing with a limited time frame, ensure all previously analyzed base fees over
    // time within the time frame that are now outside the time frame are dropped.
    if let TimeFrame::LimitedTimeFrame(limited_time_frame) = time_frame {
        drop_before(&mut tx, limited_time_frame, &earliest_block_number_for_time_frame).await?;
        analysis_state::set_analysis_state_first(
            &mut tx,
            time_frame,
            &earliest_block_number_for_time_frame,
        )
        .await?;
    }

    // Add base fees over time bassed on blocks we haven't added yet within the time frame.
    let earliest_not_included = analysis_state.last.unwrap_or(earliest_block_number_for_time_frame).max(earliest_block_number_for_time_frame);
    let blocks_to_add = BlockRange::new(earliest_not_included, *block_number);
    add_new(&mut tx, &time_frame, &analysis_state.last, &blocks_to_add).await?;
    analysis_state::set_analysis_state_last(&mut tx, time_frame, block_number).await?;

    tx.commit().await?;

    Ok(())
}

pub async fn update_base_fee_over_time(
    executor: &PgPool,
    barrier: f64,
    block_number: u32,
) -> anyhow::Result<()> {
    let (m5, h1, d1, d7, d30) = try_join!(
        get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Minute5),
        ),
        get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        ),
        get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Day1),
        ),
        get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Day7),
        ),
        get_base_fee_over_time(
            executor,
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Day30)
        ),
    )?;

    let base_fee_over_time = BaseFeeOverTime {
        barrier,
        block_number,
        m5,
        h1,
        d1,
        d7,
        d30,
        all: None,
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
            TimeFrame::LimitedTimeFrame(LimitedTimeFrame::Hour1),
        )
        .await
        .unwrap();

        assert_eq!(
            base_fees_d1,
            vec![BaseFeeAtTime {
                block_number: included_block.number,
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
                    wei: 1.0,
                    block_number: 0,
                    timestamp: test_block_1.timestamp
                },
                BaseFeeAtTime {
                    wei: 1.0,
                    block_number: 1,
                    timestamp: test_block_2.timestamp
                }
            ]
        );
    }
}
