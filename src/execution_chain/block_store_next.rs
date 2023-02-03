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

    pub async fn block_number_exists(&self, block_number: &BlockNumber) -> bool {
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

    pub async fn block_hash_exists(&self, block_hash: &str) -> bool {
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

    pub async fn delete_blocks_from_range(&self, block_range: &BlockRange) {
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

    pub async fn store_block(&self, block: &ExecutionNodeBlock, eth_price: f64) {
        block_store::store_block(self.db_pool, block, eth_price).await
    }

    pub async fn first_block_number_after_or_at(
        &self,
        timestamp: &DateTime<Utc>,
    ) -> Option<BlockNumber> {
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
}

#[cfg(test)]
mod tests {
    use crate::execution_chain::ExecutionNodeBlockBuilder;

    use super::*;

    #[sqlx::test]
    async fn block_number_exists_test(pool: PgPool) {
        let test_id = "block_number_exists";
        let block_store = BlockStore::new(&pool);

        let test_number = 1351;
        let test_block = ExecutionNodeBlockBuilder::new(test_id)
            .with_number(test_number)
            .build();

        assert!(!block_store.block_number_exists(&test_number).await);

        block_store.store_block(&test_block, 0.0).await;

        assert!(block_store.block_number_exists(&test_number).await);
    }

    #[sqlx::test]
    async fn block_hash_exists_test(pool: PgPool) {
        let test_id = "block_hash_exists";
        let block_store = BlockStore::new(&pool);

        let test_block = ExecutionNodeBlockBuilder::new(test_id).build();

        // Test the hash does not exist.
        assert!(!block_store.block_hash_exists(&test_block.hash).await);

        block_store.store_block(&test_block, 0.0).await;

        // Test the hash exists.
        assert!(block_store.block_hash_exists(&test_block.hash).await);
    }
}
