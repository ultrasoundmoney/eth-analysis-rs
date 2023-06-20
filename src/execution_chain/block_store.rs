use chrono::{DateTime, SubsecRound, Utc};
use sqlx::PgExecutor;

use super::node::{BlockNumber, ExecutionNodeBlock};

struct ExecutionBlockRow {
    base_fee_per_gas: i64,
    difficulty: i64,
    gas_used: i32,
    hash: String,
    number: i32,
    parent_hash: String,
    timestamp: DateTime<Utc>,
    total_difficulty: String,
}

impl From<ExecutionBlockRow> for ExecutionNodeBlock {
    fn from(row: ExecutionBlockRow) -> Self {
        Self {
            base_fee_per_gas: row.base_fee_per_gas as u64,
            difficulty: row.difficulty as u64,
            gas_used: row.gas_used,
            hash: row.hash,
            number: row.number,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            total_difficulty: row.total_difficulty.parse::<u128>().unwrap(),
            // Types for blocks coming from the node and from our DB should be split.
            transactions: vec![],
        }
    }
}

pub async fn delete_blocks(executor: impl PgExecutor<'_>, greater_than_or_equal: &BlockNumber) {
    sqlx::query!(
        "
        DELETE FROM blocks_next
        WHERE number >= $1
        ",
        *greater_than_or_equal
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn get_last_block_number(executor: impl PgExecutor<'_>) -> Option<BlockNumber> {
    sqlx::query!(
        "
        SELECT
            number
        FROM
            blocks_next
        ORDER BY number DESC
        LIMIT 1
        "
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| row.number)
}

pub async fn store_block(
    executor: impl PgExecutor<'_>,
    block: &ExecutionNodeBlock,
    eth_price: f64,
) {
    sqlx::query(
        "
        INSERT INTO
            blocks_next (
                base_fee_per_gas,
                difficulty,
                eth_price,
                gas_used,
                hash,
                number,
                parent_hash,
                timestamp,
                total_difficulty
            )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::NUMERIC)
        ",
    )
    .bind(block.base_fee_per_gas as i64)
    .bind(block.difficulty as i64)
    .bind(eth_price)
    .bind(block.gas_used)
    .bind(block.hash.clone())
    .bind(block.number)
    .bind(block.parent_hash.clone())
    .bind(block.timestamp.trunc_subsecs(0))
    .bind(block.total_difficulty.to_string())
    .execute(executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound, Utc};
    use sqlx::Acquire;

    use crate::db;

    use super::*;

    async fn len(executor: impl PgExecutor<'_>) -> i64 {
        sqlx::query!(
            r#"
            SELECT COUNT(*) AS "count!" FROM blocks_next
            "#,
        )
        .fetch_one(executor)
        .await
        .unwrap()
        .count
    }

    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 0,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().duration_round(Duration::seconds(1)).unwrap(),
            total_difficulty: 0,
            transactions: vec![],
        }
    }

    pub async fn get_block_by_number(
        executor: impl PgExecutor<'_>,
        block_number: &BlockNumber,
    ) -> Option<ExecutionNodeBlock> {
        sqlx::query_as!(
            ExecutionBlockRow,
            r#"
            SELECT
                base_fee_per_gas,
                difficulty,
                gas_used,
                hash,
                number,
                parent_hash,
                timestamp,
                total_difficulty::TEXT AS "total_difficulty!"
            FROM
                blocks_next
            WHERE
                number = $1
            "#,
            *block_number
        )
        .fetch_optional(executor)
        .await
        .unwrap()
        .map(|row| row.into())
    }

    #[tokio::test]
    async fn store_block_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        assert_eq!(len(&mut tx).await, 0);

        store_block(&mut tx, &test_block, 0.0).await;
        assert_eq!(get_block_by_number(&mut tx, &0).await.unwrap(), test_block);
    }

    #[tokio::test]
    async fn delete_blocks_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();
        store_block(&mut tx, &test_block, 0.0).await;
        store_block(
            &mut tx,
            &ExecutionNodeBlock {
                hash: "0xtest1".to_string(),
                number: 1,
                parent_hash: "0xtest".to_string(),
                ..test_block
            },
            0.0,
        )
        .await;

        assert_eq!(len(&mut tx,).await, 2);

        delete_blocks(&mut tx, &0).await;
        assert_eq!(len(&mut tx,).await, 0);
    }

    pub async fn get_block_by_hash(
        executor: impl PgExecutor<'_>,
        hash: &str,
    ) -> Option<ExecutionNodeBlock> {
        sqlx::query_as!(
            ExecutionBlockRow,
            r#"
            SELECT
                base_fee_per_gas,
                difficulty,
                gas_used,
                hash,
                number,
                parent_hash,
                timestamp,
                total_difficulty::TEXT AS "total_difficulty!"
            FROM
                blocks_next
            WHERE
                hash = $1
            "#,
            hash
        )
        .fetch_optional(executor)
        .await
        .unwrap()
        .map(|row| row.into())
    }

    #[tokio::test]
    async fn get_block_by_hash_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_block(&mut *tx, &test_block, 0.0).await;
        let stored_block = get_block_by_hash(&mut *tx, &test_block.hash).await;
        assert_eq!(stored_block, Some(test_block));
    }

    #[tokio::test]
    async fn get_block_by_number_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_block(&mut *tx, &test_block, 0.0).await;
        let stored_block = get_block_by_number(&mut *tx, &test_block.number).await;
        assert_eq!(stored_block, Some(test_block));
    }

    #[tokio::test]
    async fn get_empty_last_block_number_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();

        assert_eq!(get_last_block_number(&mut *tx,).await, None);
    }

    #[tokio::test]
    async fn get_last_block_number_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_block(&mut *tx, &test_block, 0.0).await;
        let last_block_number = get_last_block_number(&mut tx).await;
        assert_eq!(last_block_number, Some(0));
    }
}
