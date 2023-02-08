use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::execution_chain::{BlockNumber, BlockRange, ExecutionNodeBlock};

use super::block_store;

pub struct BlockStore<'a> {
    db_pool: &'a PgPool,
}

impl<'a> BlockStore<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }

    #[allow(dead_code)]
    pub async fn number_exists(&self, block_number: &BlockNumber) -> bool {
        sqlx::query!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blocks_next
                WHERE number = $1
            ) AS "exists!"
            "#,
            block_number
        )
        .fetch_one(self.db_pool)
        .await
        .unwrap()
        .exists
    }

    #[allow(dead_code)]
    pub async fn hash_exists(&self, block_hash: &str) -> bool {
        sqlx::query!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM blocks_next
                WHERE hash = $1
            ) AS "exists!"
            "#,
            block_hash
        )
        .fetch_one(self.db_pool)
        .await
        .unwrap()
        .exists
    }

    #[allow(dead_code)]
    pub async fn delete_from_range(&self, block_range: &BlockRange) {
        sqlx::query!(
            "
            DELETE FROM
                blocks_next
            WHERE
                number >= $1
            AND
                number <= $2
            ",
            block_range.start,
            block_range.end
        )
        .execute(self.db_pool)
        .await
        .unwrap();
    }

    #[allow(dead_code)]
    pub async fn add(&self, block: &ExecutionNodeBlock, eth_price: f64) {
        block_store::store_block(self.db_pool, block, eth_price).await
    }

    pub async fn first_number_after_or_at(&self, timestamp: &DateTime<Utc>) -> Option<BlockNumber> {
        sqlx::query!(
            "
            SELECT
                number
            FROM
                blocks_next
            WHERE
                timestamp >= $1
            ORDER BY
                timestamp ASC
            LIMIT 1
            ",
            timestamp
        )
        .fetch_optional(self.db_pool)
        .await
        .unwrap()
        .map(|row| row.number)
    }

    pub async fn last(&self) -> ExecutionNodeBlock {
        sqlx::query!(
            r#"
            SELECT
                base_fee_per_gas,
                difficulty,
                eth_price,
                gas_used,
                hash,
                number,
                parent_hash,
                timestamp,
                total_difficulty::TEXT AS "total_difficulty!"
            FROM
                blocks_next
            ORDER BY
                number DESC
            LIMIT 1
            "#
        )
        .fetch_one(self.db_pool)
        .await
        .map(|row| ExecutionNodeBlock {
            base_fee_per_gas: row.base_fee_per_gas as u64,
            difficulty: row.difficulty as u64,
            gas_used: row.gas_used,
            hash: row.hash,
            number: row.number,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            total_difficulty: row.total_difficulty.parse().unwrap(),
        })
        .unwrap()
    }

    pub async fn hash_from_number(&self, block_number: &BlockNumber) -> Option<String> {
        sqlx::query!(
            r#"
            SELECT
                hash
            FROM
                blocks_next
            WHERE
                number = $1
            "#,
            block_number
        )
        .fetch_optional(self.db_pool)
        .await
        .unwrap()
        .map(|row| row.hash)
    }

    pub async fn timestamp_from_number(&self, block_number: &BlockNumber) -> DateTime<Utc> {
        sqlx::query!(
            r#"
            SELECT
                timestamp
            FROM
                blocks_next
            WHERE
                number = $1
            "#,
            block_number
        )
        .fetch_one(self.db_pool)
        .await
        .unwrap()
        .timestamp
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;

    use crate::{db::tests::TestDb, execution_chain::ExecutionNodeBlockBuilder};

    use super::*;

    #[tokio::test]
    async fn number_exists_test() {
        let test_db = TestDb::new().await;
        let test_id = "block_number_exists";
        let block_store = BlockStore::new(test_db.pool());

        let test_number = 1351;
        let test_block = ExecutionNodeBlockBuilder::new(test_id)
            .with_number(test_number)
            .build();

        assert!(!block_store.number_exists(&test_number).await);

        block_store.add(&test_block, 0.0).await;

        assert!(block_store.number_exists(&test_number).await);

        test_db.cleanup().await;
    }

    #[tokio::test]
    async fn hash_exists_test() {
        let test_db = TestDb::new().await;
        let test_id = "block_hash_exists";
        let block_store = BlockStore::new(test_db.pool());

        let test_block = ExecutionNodeBlockBuilder::new(test_id).build();

        // Test the hash does not exist.
        assert!(!block_store.hash_exists(&test_block.hash).await);

        block_store.add(&test_block, 0.0).await;

        // Test the hash exists.
        assert!(block_store.hash_exists(&test_block.hash).await);

        test_db.cleanup().await;
    }

    #[tokio::test]
    async fn last_test() {
        let test_db = TestDb::new().await;
        let test_id = "last";
        let block_store = BlockStore::new(test_db.pool());

        let test_block = ExecutionNodeBlockBuilder::new(test_id).build();

        block_store.add(&test_block, 0.0).await;

        let last_block = block_store.last().await;

        assert_eq!(last_block, test_block);

        test_db.cleanup().await;
    }

    #[tokio::test]
    async fn hash_from_number_test() {
        let test_db = TestDb::new().await;
        let test_id = "hash_from_number";
        let block_store = BlockStore::new(test_db.pool());

        let test_block = ExecutionNodeBlockBuilder::new(test_id).build();

        assert!(block_store
            .hash_from_number(&test_block.number)
            .await
            .is_none());

        block_store.add(&test_block, 0.0).await;

        let hash = block_store
            .hash_from_number(&test_block.number)
            .await
            .unwrap();

        assert_eq!(hash, test_block.hash);

        test_db.cleanup().await;
    }

    #[tokio::test]
    async fn timestamp_from_number_test() {
        let test_db = TestDb::new().await;
        let test_id = "timestamp_from_number";
        let block_store = BlockStore::new(test_db.pool());

        let test_block = ExecutionNodeBlockBuilder::new(test_id)
            .with_number(8275)
            .with_timestamp(
                &"2022-01-01T00:00:00Z"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
                    .round_subsecs(0),
            )
            .build();

        block_store.add(&test_block, 0.0).await;

        let timestamp = block_store.timestamp_from_number(&test_block.number).await;

        assert_eq!(timestamp, test_block.timestamp);

        test_db.cleanup().await;
    }
}
