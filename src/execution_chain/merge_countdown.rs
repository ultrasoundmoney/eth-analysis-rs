use std::ops::Mul;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::{
    caching,
    key_value_store::{self, KeyValue},
};

use super::node::{BlockNumber, Difficulty, ExecutionNodeBlock, TotalDifficulty};

const TOTAL_TERMINAL_DIFFICULTY: u128 = 58750000000000000000000;

const MERGE_TTD_COUNTDOWN_CACHE_KEY: &str = "merge-ttd-countdown";

#[derive(Serialize)]
struct MergeTTDCountdown {
    block_number: BlockNumber,
    difficulty: Difficulty,
    total_difficulty: TotalDifficulty,
    estimated_date_time: DateTime<Utc>,
}

fn estimate_merge(block: &ExecutionNodeBlock) -> DateTime<Utc> {
    let blocks_left =
        (TOTAL_TERMINAL_DIFFICULTY - block.total_difficulty) / block.difficulty as u128;
    let time_left = Duration::seconds(13).mul(blocks_left as i32);
    Utc::now() + time_left
}

pub async fn on_new_head(executor: &PgPool, block: &ExecutionNodeBlock) {
    tracing::debug!("updating merge TTD countdown");

    let merge_ttd_countdown = MergeTTDCountdown {
        block_number: block.number,
        difficulty: block.difficulty,
        total_difficulty: block.total_difficulty,
        estimated_date_time: estimate_merge(block),
    };

    key_value_store::set_value(
        executor,
        KeyValue {
            key: MERGE_TTD_COUNTDOWN_CACHE_KEY,
            value: serde_json::to_value(merge_ttd_countdown).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(executor, MERGE_TTD_COUNTDOWN_CACHE_KEY).await;
}
