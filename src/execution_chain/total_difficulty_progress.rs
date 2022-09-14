use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Connection, FromRow, PgConnection, PgExecutor};

use crate::{
    caching::{self, CacheKey},
    config, key_value_store,
};

#[derive(Debug, FromRow, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgressForDay {
    number: i32,
    timestamp: DateTime<Utc>,
    total_difficulty: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DifficultyProgress {
    block_number: u32,
    timestamp: DateTime<Utc>,
    total_difficulty_by_day: Vec<ProgressForDay>,
}

async fn get_total_difficulty_by_hour(executor: impl PgExecutor<'_>) -> Vec<ProgressForDay> {
    sqlx::query_as::<_, ProgressForDay>(
        "
            SELECT
                DISTINCT ON (DATE_TRUNC('hour', timestamp))
                timestamp,
                total_difficulty::FLOAT8,
                number
            FROM
                blocks_next 
            WHERE
                timestamp >= '2022-09-10'::DATE
            ORDER BY
                DATE_TRUNC('hour', timestamp), timestamp
        ",
    )
    .fetch_all(executor)
    .await
    .unwrap()
}

async fn get_current_total_difficulty<'a>(executor: impl PgExecutor<'a>) -> ProgressForDay {
    sqlx::query_as::<_, ProgressForDay>(
        "
            SELECT
                timestamp,
                total_difficulty::FLOAT8,
                number
            FROM
                blocks_next
            ORDER BY
                timestamp DESC
            LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap()
}

pub async fn update_total_difficulty_progress() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating total difficulty progress");

    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();

    let mut total_difficulty_by_hour = get_total_difficulty_by_hour(&mut connection).await;

    // Get the most recent total difficulty for today.
    let current_total_difficulty = get_current_total_difficulty(&mut connection).await;

    let block_number = current_total_difficulty.number.clone() as u32;
    let timestamp = current_total_difficulty.timestamp.clone();

    // Replace today's total difficulty with the most current one, this let's us report that our
    // graph is updated every 10 min.
    total_difficulty_by_hour.pop();
    total_difficulty_by_hour.push(current_total_difficulty);

    let total_difficulty_progress = DifficultyProgress {
        block_number,
        timestamp,
        total_difficulty_by_day: total_difficulty_by_hour,
    };

    key_value_store::set_value(
        &mut connection,
        &CacheKey::TotalDifficultyProgress.to_db_key(),
        &serde_json::to_value(&total_difficulty_progress).unwrap(),
    )
    .await;

    caching::publish_cache_update(&mut connection, CacheKey::TotalDifficultyProgress).await;
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, SubsecRound};

    use crate::{
        db_testing,
        execution_chain::{block_store::BlockStore, node::ExecutionNodeBlock},
    };

    use super::*;

    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 0,
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
    async fn get_difficulty_by_day_test() {
        let mut db = db_testing::get_test_db().await;
        let mut transaction = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *transaction);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        let progress_by_day = get_total_difficulty_by_hour(&mut *transaction).await;
        assert_eq!(
            progress_by_day,
            vec![ProgressForDay {
                number: 0,
                timestamp: test_block.timestamp,
                total_difficulty: 10.0
            }]
        );
    }

    #[tokio::test]
    async fn get_difficulty_by_day_asc_test() {
        let mut db = db_testing::get_test_db().await;
        let mut transaction = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *transaction);
        let test_block_1 = make_test_block();
        let test_block_2 = ExecutionNodeBlock {
            hash: "0xtest2".to_owned(),
            number: 1,
            parent_hash: "0xtest".to_owned(),
            timestamp: Utc::now().trunc_subsecs(0) + Duration::days(1),
            total_difficulty: 20,
            ..make_test_block()
        };

        block_store.store_block(&test_block_1, 0.0).await;
        block_store.store_block(&test_block_2, 0.0).await;

        let progress_by_day = get_total_difficulty_by_hour(&mut *transaction).await;

        assert_eq!(
            progress_by_day,
            vec![
                ProgressForDay {
                    number: 0,
                    timestamp: test_block_1.timestamp,
                    total_difficulty: 10.0
                },
                ProgressForDay {
                    number: 1,
                    timestamp: test_block_2.timestamp,
                    total_difficulty: 20.0
                }
            ]
        );
    }

    #[tokio::test]
    async fn get_current_total_difficulty_test() {
        let mut db = db_testing::get_test_db().await;
        let mut transaction = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *transaction);
        let test_block = make_test_block();
        let test_block_2 = ExecutionNodeBlock {
            hash: "0xtest2".to_owned(),
            number: 1,
            parent_hash: "0xtest".to_owned(),
            timestamp: Utc::now().trunc_subsecs(0) + Duration::seconds(1),
            total_difficulty: 20,
            ..make_test_block()
        };

        block_store.store_block(&test_block, 0.0).await;
        block_store.store_block(&test_block_2, 0.0).await;
        let progress_by_day = get_current_total_difficulty(&mut *transaction).await;
        assert_eq!(
            progress_by_day,
            ProgressForDay {
                number: 1,
                timestamp: test_block_2.timestamp,
                total_difficulty: 20.0
            }
        );
    }
}
