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

async fn blob_base_fee_average(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> Option<WeiF64> {
    match time_frame {
        TimeFrame::Growing(growing_time_frame) => {
                warn!(time_frame = %growing_time_frame, "getting average fee for growing time frame is slow");
                sqlx::query!(
                    r#"
                    SELECT
                        SUM(blob_base_fee::FLOAT8 * blob_gas_used::FLOAT8) / SUM(blob_gas_used::FLOAT8) AS average
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
                    SUM(blob_base_fee::FLOAT8 * blob_gas_used::FLOAT8) / SUM(blob_gas_used::FLOAT8) AS average
                FROM
                    blocks_next
                WHERE
                    timestamp >= NOW() - $1
                ",
            )
            .bind(limited_time_frame.postgres_interval())
            .map(|row: PgRow| row.get::<Option<f64>, _>("average"))
            .fetch_one(executor)
            .await.unwrap()
        }
    }
}

// This fn expects to be called after the `blocks` table is in sync.
async fn blob_base_fee_min(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> Option<(BlockNumber, WeiF64)> {
    match time_frame {
        TimeFrame::Growing(SinceMerge) => sqlx::query!(
            r#"
            SELECT
                number,
                blob_base_fee AS "blob_base_fee!"
            FROM
                blocks_next
            WHERE
                timestamp >= '2022-09-15T06:42:42Z'::TIMESTAMPTZ
                AND blob_base_fee IS NOT NULl
            ORDER BY blob_base_fee ASC
            LIMIT 1
            "#,
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.blob_base_fee as f64))
        .ok(),
        TimeFrame::Growing(SinceBurn) => sqlx::query!(
            r#"
            SELECT
                number,
                blob_base_fee AS "blob_base_fee!"
            FROM
                blocks_next
            WHERE blob_base_fee IS NOT NULL
            ORDER BY blob_base_fee ASC
            LIMIT 1
            "#,
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.blob_base_fee as f64))
        .ok(),
        TimeFrame::Limited(limited_time_frame) => sqlx::query!(
            r#"
            SELECT
                number,
                blob_base_fee AS "blob_base_fee!"
            FROM
                blocks_next
            WHERE
                timestamp >= NOW() - $1::INTERVAL
                AND blob_base_fee IS NOT NULL
            ORDER BY blob_base_fee ASC
            LIMIT 1
            "#,
            limited_time_frame.postgres_interval(),
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.blob_base_fee as f64))
        .ok(),
    }
}

// This fn expects to be called after the `blocks` table is in sync.
async fn blob_base_fee_max(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> Option<(BlockNumber, WeiF64)> {
    match time_frame {
        TimeFrame::Growing(growing_time_frame) => sqlx::query!(
            r#"
            SELECT
                number,
                blob_base_fee AS "blob_base_fee!"
            FROM
                blocks_next
            WHERE
                timestamp >= $1
                AND blob_base_fee IS NOT NULL
            ORDER BY blob_base_fee DESC
            LIMIT 1
            "#,
            growing_time_frame.start_timestamp()
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.blob_base_fee as f64))
        .ok(),
        TimeFrame::Limited(limited_time_frame) => sqlx::query!(
            r#"
            SELECT
                number,
                blob_base_fee AS "blob_base_fee!"
            FROM
                blocks_next
            WHERE
                timestamp >= NOW() - $1::INTERVAL
                AND blob_base_fee IS NOT NULL
            ORDER BY blob_base_fee DESC
            LIMIT 1
            "#,
            limited_time_frame.postgres_interval(),
        )
        .fetch_one(executor)
        .await
        .map(|row| (row.number, row.blob_base_fee as f64))
        .ok(),
    }
}

#[derive(Clone, Debug, Serialize)]
struct BlobFeePerGasStats {
    average: Option<WeiF64>,
    block_number: BlockNumber,
    max: Option<WeiF64>,
    max_block_number: Option<BlockNumber>,
    min: Option<WeiF64>,
    min_block_number: Option<BlockNumber>,
    timestamp: DateTime<Utc>,
}

#[cached(key = "String", convert = r#"{time_frame.to_string()}"#, time = 3600)]
async fn blob_base_fee_stats_one_hour_cached(
    db_pool: &PgPool,
    time_frame: &TimeFrame,
    block: &ExecutionNodeBlock,
) -> BlobFeePerGasStats {
    BlobFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
}

#[cached(key = "String", convert = r#"{time_frame.to_string()}"#, time = 600)]
async fn blob_base_fee_stats_ten_minutes_cached(
    db_pool: &PgPool,
    time_frame: &TimeFrame,
    block: &ExecutionNodeBlock,
) -> BlobFeePerGasStats {
    BlobFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
}

impl BlobFeePerGasStats {
    async fn from_time_frame(
        executor: &PgPool,
        time_frame: &TimeFrame,
        block: &ExecutionNodeBlock,
    ) -> Self {
        let (min_opt, max_opt, average) = join!(
            blob_base_fee_min(executor, time_frame)
                .timed(&format!("blob_base_fee_min_{time_frame}")),
            blob_base_fee_max(executor, time_frame)
                .timed(&format!("blob_base_fee_max_{time_frame}")),
            blob_base_fee_average(executor, time_frame)
                .timed(&format!("blob_base_fee_average_{time_frame}")),
        );

        Self {
            average,
            block_number: block.number,
            max: max_opt.map(|t| t.1),
            max_block_number: max_opt.map(|t| t.0),
            min: min_opt.map(|t| t.1),
            min_block_number: min_opt.map(|t| t.0),
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
                blob_base_fee_stats_one_hour_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Growing(SinceMerge) => {
                blob_base_fee_stats_one_hour_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Day30) => {
                blob_base_fee_stats_one_hour_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Day7) => {
                blob_base_fee_stats_ten_minutes_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Day1) => {
                blob_base_fee_stats_ten_minutes_cached(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Hour1) => {
                BlobFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
            }
            TimeFrame::Limited(LimitedTimeFrame::Minute5) => {
                BlobFeePerGasStats::from_time_frame(db_pool, time_frame, block).await
            }
        }
    }
}

// TODO: top level time frames should be removed once the frontend has switched over to
// blob_base_fee_stats. Barrier should be its own endpoint.
#[derive(Serialize)]
struct BlobFeePerGasStatsEnvelope {
    all: Option<BlobFeePerGasStats>,
    barrier: f64,
    blob_base_fee_stats: HashMap<TimeFrame, BlobFeePerGasStats>,
    block_number: BlockNumber,
    d1: BlobFeePerGasStats,
    d30: BlobFeePerGasStats,
    d7: BlobFeePerGasStats,
    h1: BlobFeePerGasStats,
    m5: BlobFeePerGasStats,
    since_burn: BlobFeePerGasStats,
    since_merge: Option<BlobFeePerGasStats>,
    timestamp: DateTime<Utc>,
}

pub async fn update_blob_fee_stats(
    executor: &PgPool,
    barrier: &Barrier,
    block: &ExecutionNodeBlock,
) {
    use GrowingTimeFrame::*;
    use LimitedTimeFrame::*;
    use TimeFrame::*;

    debug!("updating base fee over time");

    let (since_burn, since_merge, d30, d7, d1, h1, m5) = join!(
        BlobFeePerGasStats::from_time_frame_cached(executor, &Growing(SinceBurn), block,),
        BlobFeePerGasStats::from_time_frame_cached(executor, &Growing(SinceMerge), block,),
        BlobFeePerGasStats::from_time_frame_cached(executor, &Limited(Day30), block,),
        BlobFeePerGasStats::from_time_frame_cached(executor, &Limited(Day7), block,),
        BlobFeePerGasStats::from_time_frame_cached(executor, &Limited(Day1), block,),
        BlobFeePerGasStats::from_time_frame_cached(executor, &Limited(Hour1), block,),
        BlobFeePerGasStats::from_time_frame_cached(executor, &Limited(Minute5), block,),
    );

    // TODO: after the frontend converts to using the hashmap, remove the individual time frame
    // calls above and simply iterate through all time frames, then collect the Vec into a HashMap.
    let mut blob_base_fee_stats = HashMap::new();
    blob_base_fee_stats.insert(Growing(SinceBurn), since_burn.clone());
    blob_base_fee_stats.insert(Growing(SinceMerge), since_merge.clone());
    blob_base_fee_stats.insert(Limited(Day30), d30.clone());
    blob_base_fee_stats.insert(Limited(Day7), d7.clone());
    blob_base_fee_stats.insert(Limited(Day1), d1.clone());
    blob_base_fee_stats.insert(Limited(Hour1), h1.clone());
    blob_base_fee_stats.insert(Limited(Minute5), m5.clone());

    // Update the cache for each time frame, stats pair.
    for (time_frame, stats) in blob_base_fee_stats.iter() {
        caching::update_and_publish(
            executor,
            &CacheKey::BlobFeePerGasStatsTimeFrame(*time_frame),
            stats,
        )
        .await;
    }

    let blob_base_fee_stats_envelope = BlobFeePerGasStatsEnvelope {
        all: Some(since_burn.clone()),
        barrier: barrier.blob_fee_barrier,
        blob_base_fee_stats,
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
        &CacheKey::BlobFeePerGasStats,
        &blob_base_fee_stats_envelope,
    )
    .await;
}
