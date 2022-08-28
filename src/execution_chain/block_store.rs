use chrono::{DateTime, SubsecRound, Utc};
use sqlx::{Acquire, PgConnection};

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
            gas_used: row.gas_used as u32,
            hash: row.hash,
            number: row.number as u32,
            parent_hash: row.parent_hash,
            timestamp: row.timestamp,
            total_difficulty: row.total_difficulty.parse::<u128>().unwrap(),
        }
    }
}

pub struct BlockStore<'a> {
    connection: &'a mut PgConnection,
}

impl BlockStore<'_> {
    #[allow(dead_code)]
    pub async fn delete_block_by_number(&mut self, block_number: &BlockNumber) {
        sqlx::query(
            "
                DELETE FROM blocks_next
                WHERE number = $1
            ",
        )
        .bind(*block_number as i32)
        .execute(self.connection.acquire().await.unwrap())
        .await
        .unwrap();
    }

    pub async fn delete_blocks(&mut self, greater_than_or_equal: &BlockNumber) {
        sqlx::query!(
            "
                DELETE FROM blocks_next
                WHERE number >= $1
            ",
            *greater_than_or_equal as i32
        )
        .execute(self.connection.acquire().await.unwrap())
        .await
        .unwrap();
    }

    #[allow(dead_code)]
    pub async fn get_block_by_hash(&mut self, hash: &str) -> Option<ExecutionNodeBlock> {
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
        .fetch_optional(self.connection.acquire().await.unwrap())
        .await
        .unwrap()
        .map(|row| row.into())
    }

    #[allow(dead_code)]
    pub async fn get_block_by_number(
        &mut self,
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
            *block_number as i32
        )
        .fetch_optional(self.connection.acquire().await.unwrap())
        .await
        .unwrap()
        .map(|row| row.into())
    }

    pub async fn get_is_parent_hash_known(&mut self, hash: &str) -> bool {
        // TODO: once 12965000 the london hardfork block is in the DB, we can hardcode and check
        // the hash, skipping a DB roundtrip.
        if self.is_empty().await {
            true
        } else {
            sqlx::query!(
                r#"
                    SELECT
                      EXISTS(
                        SELECT
                          hash
                        FROM
                          blocks_next
                        WHERE
                          hash = $1
                      ) AS "exists!"
                "#,
                hash
            )
            .fetch_one(self.connection.acquire().await.unwrap())
            .await
            .unwrap()
            .exists
        }
    }

    pub async fn get_is_number_known(&mut self, block_number: &BlockNumber) -> bool {
        sqlx::query!(
            r#"
                SELECT
                  EXISTS(
                    SELECT
                      hash
                    FROM
                      blocks_next
                    WHERE
                      number = $1
                  ) AS "exists!"
            "#,
            *block_number as i32
        )
        .fetch_one(self.connection.acquire().await.unwrap())
        .await
        .unwrap()
        .exists
    }

    pub async fn get_last_block_number(&mut self) -> Option<BlockNumber> {
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
        .fetch_optional(self.connection.acquire().await.unwrap())
        .await
        .unwrap()
        .map(|row| row.number as u32)
    }

    #[allow(dead_code)]
    async fn is_empty(&mut self) -> bool {
        let count = sqlx::query!(
            r#"
                SELECT
                    COUNT(*) AS "count!"
                FROM
                    blocks_next
                LIMIT 1
            "#
        )
        .fetch_one(self.connection.acquire().await.unwrap())
        .await
        .unwrap()
        .count;

        count == 0
    }

    #[allow(dead_code)]
    async fn len(&mut self) -> i64 {
        sqlx::query!(
            r#"
                SELECT COUNT(*) AS "count!" FROM blocks_next
            "#,
        )
        .fetch_one(self.connection.acquire().await.unwrap())
        .await
        .unwrap()
        .count
    }

    pub fn new(connection: &mut PgConnection) -> BlockStore {
        BlockStore { connection }
    }

    pub async fn store_block(&mut self, block: &ExecutionNodeBlock, eth_price: f64) {
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
        .bind(block.gas_used as i32)
        .bind(block.hash.clone())
        .bind(block.number as i32)
        .bind(block.parent_hash.clone())
        .bind(block.timestamp.trunc_subsecs(0))
        .bind(block.total_difficulty.to_string())
        .execute(self.connection.acquire().await.unwrap())
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound, Utc};

    use super::*;
    use crate::db_testing::get_test_db;

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
        }
    }

    #[tokio::test]
    async fn store_block_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        assert_eq!(block_store.len().await, 0);

        block_store.store_block(&test_block, 0.0).await;
        assert_eq!(
            block_store.get_block_by_number(&0).await.unwrap(),
            test_block
        );
    }

    #[tokio::test]
    async fn delete_block_by_number_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        assert_eq!(block_store.len().await, 1);

        block_store.delete_block_by_number(&test_block.number).await;
        assert_eq!(block_store.len().await, 0);
    }

    #[tokio::test]
    async fn delete_blocks_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();
        block_store.store_block(&test_block, 0.0).await;
        block_store
            .store_block(
                &ExecutionNodeBlock {
                    hash: "0xtest1".to_string(),
                    number: 1,
                    parent_hash: "0xtest".to_string(),
                    ..test_block
                },
                0.0,
            )
            .await;

        assert_eq!(block_store.len().await, 2);

        block_store.delete_blocks(&0).await;
        assert_eq!(block_store.len().await, 0);
    }

    #[tokio::test]
    async fn get_block_by_hash_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        let stored_block = block_store.get_block_by_hash(&test_block.hash).await;
        assert_eq!(stored_block, Some(test_block));
    }

    #[tokio::test]
    async fn get_block_by_number_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        let stored_block = block_store.get_block_by_number(&test_block.number).await;
        assert_eq!(stored_block, Some(test_block));
    }

    #[tokio::test]
    async fn get_is_first_parent_hash_known_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);

        let is_parent_known = block_store.get_is_parent_hash_known("0xnotthere").await;
        assert!(is_parent_known);
    }

    #[tokio::test]
    async fn get_is_parent_hash_known_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        assert!(!block_store.get_is_parent_hash_known("0xnotthere").await);
        assert!(block_store.get_is_parent_hash_known(&test_block.hash).await);
    }

    #[tokio::test]
    async fn get_is_number_known_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        assert!(!block_store.get_is_number_known(&1).await);
        assert!(block_store.get_is_number_known(&0).await);
    }

    #[tokio::test]
    async fn get_empty_last_block_number_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);

        assert_eq!(block_store.get_last_block_number().await, None);
    }

    #[tokio::test]
    async fn get_last_block_number_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        let last_block_number = block_store.get_last_block_number().await;
        assert_eq!(last_block_number, Some(0));
    }

    #[tokio::test]
    async fn is_empty_empty_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);

        assert!(block_store.is_empty().await);
    }

    #[tokio::test]
    async fn is_empty_not_empty_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let mut block_store = BlockStore::new(&mut *tx);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;
        assert!(!block_store.is_empty().await);
    }
}
