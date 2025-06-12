use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::join;
use sqlx::PgPool;

use crate::execution_chain::{BlockNumber, BlockRange, ExecutionNodeBlock};

use super::block_store;

#[async_trait]
pub trait BlockStore {
    async fn number_exists(&self, block_number: &BlockNumber) -> bool;
    // async fn hash_exists(&self, block_hash: &str) -> bool;
    // async fn delete_from_range(&self, block_range: &BlockRange);
    // async fn add(&self, block: &ExecutionNodeBlock, eth_price: f64);
    async fn first_number_after_or_at(&self, timestamp: &DateTime<Utc>) -> Option<BlockNumber>;
    async fn last(&self) -> Result<Option<ExecutionNodeBlock>>;
    async fn hash_from_number(&self, block_number: &BlockNumber) -> Option<String>;
}

pub struct BlockStorePostgres {
    db_pool: PgPool,
}

impl BlockStorePostgres {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
    }

    #[allow(dead_code)]
    async fn hash_exists(&self, block_hash: &str) -> bool {
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
        .fetch_one(&self.db_pool)
        .await
        .unwrap()
        .exists
    }

    #[allow(dead_code)]
    async fn delete_from_range(&self, block_range: &BlockRange) {
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
        .execute(&self.db_pool)
        .await
        .unwrap();
    }

    #[allow(dead_code)]
    async fn add(&self, block: &ExecutionNodeBlock, eth_price: f64) {
        block_store::store_block(&self.db_pool, block, eth_price).await
    }

    #[allow(dead_code)]
    async fn timestamp_from_number(&self, block_number: &BlockNumber) -> DateTime<Utc> {
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
        .fetch_one(&self.db_pool)
        .await
        .unwrap()
        .timestamp
    }

    #[allow(dead_code)]
    async fn time_range_from_block_range(
        &self,
        block_range: &BlockRange,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        join!(
            self.timestamp_from_number(&block_range.start),
            self.timestamp_from_number(&block_range.end)
        )
    }
}

#[async_trait]
impl BlockStore for BlockStorePostgres {
    async fn first_number_after_or_at(&self, timestamp: &DateTime<Utc>) -> Option<BlockNumber> {
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
        .fetch_optional(&self.db_pool)
        .await
        .unwrap()
        .map(|row| row.number)
    }

    async fn last(&self) -> Result<Option<ExecutionNodeBlock>> {
        let row = sqlx::query!(
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
                blob_gas_used,
                excess_blob_gas,
                total_difficulty::TEXT AS "total_difficulty!"
            FROM
                blocks_next
            ORDER BY
                number DESC
            LIMIT 1
            "#
        )
        .fetch_optional(&self.db_pool)
        .await?;

        let block = row.map(|row| ExecutionNodeBlock {
            base_fee_per_gas: row.base_fee_per_gas as u64,
            difficulty: row.difficulty as u64,
            gas_used: row.gas_used,
            blob_gas_used: row.blob_gas_used,
            excess_blob_gas: row.excess_blob_gas,
            hash: row.hash,
            number: row.number,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            total_difficulty: row.total_difficulty.parse().unwrap(),
            transactions: vec![],
        });

        Ok(block)
    }

    async fn hash_from_number(&self, block_number: &BlockNumber) -> Option<String> {
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
        .fetch_optional(&self.db_pool)
        .await
        .unwrap()
        .map(|row| row.hash)
    }

    async fn number_exists(&self, block_number: &BlockNumber) -> bool {
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
        .fetch_one(&self.db_pool)
        .await
        .unwrap()
        .exists
    }
}

#[cfg(test)]
mod tests {
    use test_context::test_context;

    use crate::{db::tests::TestDb, execution_chain::ExecutionNodeBlockBuilder};

    use super::*;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn number_exists_test(test_db: &TestDb) {
        let test_id = "block_number_exists";
        let block_store = BlockStorePostgres::new(test_db.pool.clone());

        let test_number = 1351;
        let test_block = ExecutionNodeBlockBuilder::new(test_id)
            .with_number(test_number)
            .build();

        assert!(!block_store.number_exists(&test_number).await);

        block_store.add(&test_block, 0.0).await;

        assert!(block_store.number_exists(&test_number).await);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn hash_exists_test(test_db: &TestDb) {
        let test_id = "block_hash_exists";
        let block_store = BlockStorePostgres::new(test_db.pool.clone());

        let test_block = ExecutionNodeBlockBuilder::new(test_id).build();

        // Test the hash does not exist.
        assert!(!block_store.hash_exists(&test_block.hash).await);

        block_store.add(&test_block, 0.0).await;

        // Test the hash exists.
        assert!(block_store.hash_exists(&test_block.hash).await);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn last_test(test_db: &TestDb) {
        let test_id = "last";
        let block_store = BlockStorePostgres::new(test_db.pool.clone());

        let test_block = ExecutionNodeBlockBuilder::new(test_id).build();

        block_store.add(&test_block, 0.0).await;

        let last_block = block_store.last().await.unwrap().unwrap();

        assert_eq!(last_block, test_block);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn hash_from_number_test(test_db: &TestDb) {
        let test_id = "hash_from_number";
        let block_store = BlockStorePostgres::new(test_db.pool.clone());

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
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn time_range_from_block_range_test(test_db: &TestDb) {
        let test_id = "time_range_from_block_range";
        let block_store = BlockStorePostgres::new(test_db.pool.clone());

        let test_block_1 = ExecutionNodeBlockBuilder::new(test_id).build();
        let test_block_2 = ExecutionNodeBlockBuilder::new(test_id)
            .with_parent(&test_block_1)
            .build();

        block_store.add(&test_block_1, 0.0).await;
        block_store.add(&test_block_2, 0.0).await;

        let block_range = BlockRange {
            start: test_block_1.number,
            end: test_block_2.number,
        };

        let (start_time, end_time) = block_store.time_range_from_block_range(&block_range).await;

        assert_eq!(start_time, test_block_1.timestamp);
        assert_eq!(end_time, test_block_2.timestamp);
    }
}
