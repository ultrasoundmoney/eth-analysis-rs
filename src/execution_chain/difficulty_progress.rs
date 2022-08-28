use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Connection, FromRow, PgConnection, PgExecutor};

use crate::{
    caching, config,
    key_value_store::{self, KeyValue},
};

pub const DIFFICULTY_PROGRESS_CACHE_KEY: &str = "difficulty-progress";

#[derive(Debug, FromRow, PartialEq, Serialize)]
struct ProgressForDay {
    number: i32,
    timestamp: DateTime<Utc>,
    total_difficulty: f64,
}

#[derive(Debug, Serialize)]
struct DifficultyProgress {
    block_number: u32,
    total_difficulty_by_day: Vec<ProgressForDay>,
}

async fn get_total_difficulty_by_day<'a>(executor: impl PgExecutor<'a>) -> Vec<ProgressForDay> {
    sqlx::query_as::<_, ProgressForDay>(
        "
            SELECT DISTINCT ON
                (timestamp::DATE) timestamp, total_difficulty::FLOAT8, number
            FROM
                blocks_next
            WHERE
                timestamp >= '2022-08-01'::DATE
            ORDER BY timestamp::DATE ASC
        ",
    )
    .fetch_all(executor)
    .await
    .unwrap()
}

pub async fn update_difficulty_progress() {
    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();

    let total_difficulty_by_day = get_total_difficulty_by_day(&mut connection).await;
    let block_number = total_difficulty_by_day
        .last()
        .expect("at least one block to be stored before updating difficulty progress")
        .number
        .clone() as u32;

    let total_difficulty_progress = DifficultyProgress {
        block_number,
        total_difficulty_by_day,
    };

    key_value_store::set_value(
        &mut connection,
        KeyValue {
            key: DIFFICULTY_PROGRESS_CACHE_KEY,
            value: serde_json::to_value(&total_difficulty_progress).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(&mut connection, DIFFICULTY_PROGRESS_CACHE_KEY).await;
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound};

    use crate::{
        db_testing,
        execution_chain::{block_store::BlockStore, node::ExecutionNodeBlock},
    };

    use super::*;

    #[tokio::test]
    async fn get_difficulty_by_day_test() {
        let mut db = db_testing::get_test_db().await;
        let mut transaction = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *transaction);
        let test_block = ExecutionNodeBlock {
            base_fee_per_gas: 0,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().duration_round(Duration::days(1)).unwrap(),
            total_difficulty: 10,
        };

        block_store.store_block(&test_block, 0.0).await;
        let progress_by_day = get_total_difficulty_by_day(&mut *transaction).await;
        assert_eq!(progress_by_day, vec![]);
    }
}
