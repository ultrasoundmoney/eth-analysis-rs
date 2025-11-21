use chrono::{DateTime, SubsecRound, Utc};
use sqlx::PgExecutor;

use super::node::{BlockNumber, ExecutionNodeBlock};

struct ExecutionBlockRow {
    base_fee_per_gas: i64,
    difficulty: i64,
    gas_used: i32,
    blob_gas_used: Option<i32>,
    excess_blob_gas: Option<i32>,
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
            blob_gas_used: row.blob_gas_used,
            excess_blob_gas: row.excess_blob_gas,
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
                total_difficulty,
                blob_gas_used,
                excess_blob_gas,
                blob_base_fee
            )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::NUMERIC, $10, $11, $12)
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
    .bind(block.blob_gas_used)
    .bind(block.excess_blob_gas)
    .bind(calc_blob_base_fee(block.excess_blob_gas, block.number))
    .execute(executor)
    .await
    .unwrap();
}

fn blob_update_fraction_from_block(block_number: i32) -> u128 {
    const OLD_FRACTION: u128 = 3_338_477;
    const NEW_FRACTION: u128 = 5_007_716;
    const PECTRA_FORK_BLOCK: i32 = 22_431_084;
    if block_number >= PECTRA_FORK_BLOCK {
        NEW_FRACTION
    } else {
        OLD_FRACTION
    }
}

fn calc_blob_base_fee(excess_blob_gas: Option<i32>, block_number: i32) -> Option<i64> {
    let excess_blob_gas: u128 = excess_blob_gas?
        .try_into()
        .expect("expect excess_blob_gas to be positive");
    const MIN_BLOB_BASE_FEE: u128 = 1;
    let blob_base_fee = fake_exponential(
        MIN_BLOB_BASE_FEE,
        excess_blob_gas,
        blob_update_fraction_from_block(block_number),
    )
    .try_into()
    .expect("overflow of blob_base_fee");
    Some(blob_base_fee)
}

fn fake_exponential(factor: u128, numerator: u128, denominator: u128) -> u128 {
    let mut i: u128 = 1;
    let mut output: u128 = 0;
    let mut numerator_accum: u128 = factor * denominator;
    while numerator_accum > 0 {
        output += numerator_accum;
        numerator_accum = (numerator_accum * numerator) / (denominator * i);
        i += 1;
    }
    output / denominator
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
            blob_gas_used: None,
            excess_blob_gas: None,
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
                total_difficulty::TEXT AS "total_difficulty!",
                blob_gas_used,
                excess_blob_gas
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
        let mut transaction = db.begin().await.unwrap();
        let test_block = make_test_block();

        assert_eq!(len(&mut *transaction).await, 0);

        store_block(&mut *transaction, &test_block, 0.0).await;
        assert_eq!(
            get_block_by_number(&mut *transaction, &0).await.unwrap(),
            test_block
        );
    }

    #[tokio::test]
    async fn delete_blocks_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut transaction = db.begin().await.unwrap();
        let test_block = make_test_block();
        store_block(&mut *transaction, &test_block, 0.0).await;
        store_block(
            &mut *transaction,
            &ExecutionNodeBlock {
                hash: "0xtest1".to_string(),
                number: 1,
                parent_hash: "0xtest".to_string(),
                ..test_block
            },
            0.0,
        )
        .await;

        assert_eq!(len(&mut *transaction,).await, 2);

        delete_blocks(&mut *transaction, &0).await;
        assert_eq!(len(&mut *transaction,).await, 0);
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
                total_difficulty::TEXT AS "total_difficulty!",
                blob_gas_used,
                excess_blob_gas
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
        let mut transaction = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_block(&mut *transaction, &test_block, 0.0).await;
        let stored_block = get_block_by_hash(&mut *transaction, &test_block.hash).await;
        assert_eq!(stored_block, Some(test_block));
    }

    #[tokio::test]
    async fn get_block_by_number_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut transaction = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_block(&mut *transaction, &test_block, 0.0).await;
        let stored_block = get_block_by_number(&mut *transaction, &test_block.number).await;
        assert_eq!(stored_block, Some(test_block));
    }

    #[tokio::test]
    async fn get_empty_last_block_number_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut transaction = db.begin().await.unwrap();

        assert_eq!(get_last_block_number(&mut *transaction,).await, None);
    }

    #[tokio::test]
    async fn get_last_block_number_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut transaction = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_block(&mut *transaction, &test_block, 0.0).await;
        let last_block_number = get_last_block_number(&mut *transaction).await;
        assert_eq!(last_block_number, Some(0));
    }
}
