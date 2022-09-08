use std::ops::Mul;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::caching::{self, CacheKey};
use crate::json_codecs::{to_u128_string, to_u64_string};
use crate::key_value_store;

use super::node::{BlockNumber, Difficulty, ExecutionNodeBlock, TotalDifficulty};

const TOTAL_TERMINAL_DIFFICULTY: u128 = 58750000000000000000000;

const AVERAGE_BLOCK_TIME_ESTIMATE: i64 = 13500;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MergeEstimate {
    block_number: BlockNumber,
    blocks_left: u32,
    #[serde(serialize_with = "to_u64_string")]
    difficulty: Difficulty,
    estimated_date_time: DateTime<Utc>,
    timestamp: DateTime<Utc>,
    #[serde(serialize_with = "to_u128_string")]
    total_difficulty: TotalDifficulty,
}

pub async fn on_new_block(executor: &PgPool, block: &ExecutionNodeBlock) {
    tracing::debug!("updating merge TTD countdown");

    let blocks_left =
        ((TOTAL_TERMINAL_DIFFICULTY - block.total_difficulty) / block.difficulty as u128) as u32;
    let time_left = Duration::milliseconds(AVERAGE_BLOCK_TIME_ESTIMATE).mul(blocks_left as i32);
    let estimated_date_time = Utc::now().trunc_subsecs(0) + time_left;

    let merge_ttd_countdown = MergeEstimate {
        block_number: block.number,
        blocks_left,
        difficulty: block.difficulty,
        estimated_date_time,
        timestamp: block.timestamp,
        total_difficulty: block.total_difficulty,
    };

    key_value_store::set_value(
        executor,
        &CacheKey::MergeEstimate.to_db_key(),
        &serde_json::to_value(merge_ttd_countdown).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::MergeEstimate).await;
}
