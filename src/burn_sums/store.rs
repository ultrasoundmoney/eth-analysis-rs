use sqlx::{PgConnection, PgPool};
use tracing::debug;

use crate::{
    execution_chain::{BlockNumber, BlockRange},
    time_frames::TimeFrame,
    units::WeiNewtype,
};

use super::BurnSumRecord;

const REORG_LIMIT: i32 = 100;

pub struct BurnSumStore<'a> {
    db_pool: &'a PgPool,
}

impl<'a> BurnSumStore<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }

    pub async fn burn_sum_from_block_range(&self, block_range: &BlockRange) -> WeiNewtype {
        sqlx::query!(
            r#"
            SELECT
                SUM(base_fee_per_gas::NUMERIC(78) * gas_used::NUMERIC(78))::TEXT AS "burn_sum!"
            FROM
                blocks_next
            WHERE
                number >= $1 AND number <= $2
            "#,
            block_range.start,
            block_range.end
        )
        .fetch_one(self.db_pool)
        .await
        .unwrap()
        .burn_sum
        .parse()
        .unwrap()
    }

    pub async fn delete_old_sums(&self, last_block: BlockNumber) {
        let block_number_limit = last_block - REORG_LIMIT;

        debug!(block_number_limit, "deleting old sums");

        sqlx::query!(
            "
            DELETE FROM burn_sums
            WHERE last_included_block_number < $1
            ",
            block_number_limit
        )
        .execute(self.db_pool)
        .await
        .unwrap();
    }

    /// Returns the last sum which relates to a canonical block.
    pub async fn last_burn_sum(&self, time_frame: &TimeFrame) -> Option<BurnSumRecord> {
        let row = sqlx::query!(
            r#"
            SELECT
                first_included_block_number,
                last_included_block_number,
                last_included_block_hash,
                timestamp,
                sum::TEXT AS "sum!"
            FROM burn_sums
            WHERE time_frame = $1
            ORDER BY last_included_block_number DESC
            LIMIT 1
            "#,
            time_frame.to_string()
        )
        .fetch_optional(self.db_pool)
        .await
        .unwrap();

        row.map(|row| BurnSumRecord {
            first_included_block_number: row.first_included_block_number,
            last_included_block_hash: row.last_included_block_hash,
            last_included_block_number: row.last_included_block_number,
            sum: row.sum.parse().unwrap(),
            time_frame: *time_frame,
            timestamp: row.timestamp,
        })
    }

    async fn store_burn_sum(&self, burn_sum: &BurnSumRecord) {
        sqlx::query!(
            "
            INSERT INTO burn_sums (
                time_frame,
                first_included_block_number,
                last_included_block_number,
                last_included_block_hash,
                timestamp,
                sum
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6::NUMERIC
            )
            ",
            burn_sum.time_frame.to_string(),
            burn_sum.first_included_block_number,
            burn_sum.last_included_block_number,
            burn_sum.last_included_block_hash,
            burn_sum.timestamp,
            Into::<String>::into(burn_sum.sum) as String
        )
        .execute(self.db_pool)
        .await
        .unwrap();
    }

    pub async fn store_burn_sums(&self, burn_sums: [&BurnSumRecord; 7]) {
        for burn_sum in burn_sums {
            self.store_burn_sum(burn_sum).await;
        }
    }

    pub async fn delete_new_sums_tx(
        transaction: &mut PgConnection,
        block_number_gte: &BlockNumber,
    ) {
        sqlx::query!(
            "
            DELETE FROM
                burn_sums
            WHERE
                last_included_block_number >= $1
            ",
            block_number_gte
        )
        .execute(transaction)
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::tests::TestDb,
        execution_chain::{block_store, ExecutionNodeBlockBuilder},
    };

    use super::*;

    #[tokio::test]
    async fn burn_sum_from_block_range_test() {
        let test_db = TestDb::new().await;
        let pool = test_db.pool();
        let burn_sum_store = BurnSumStore::new(pool);

        let test_id = "burn_sum_from_time_frame";

        let block_1 = ExecutionNodeBlockBuilder::new(test_id)
            .with_burn(100)
            .build();
        let block_2 = ExecutionNodeBlockBuilder::from_parent(&block_1)
            .with_burn(200)
            .build();

        block_store::store_block(pool, &block_1, 0.0).await;
        block_store::store_block(pool, &block_2, 0.0).await;

        let burn_sum = burn_sum_store
            .burn_sum_from_block_range(&BlockRange {
                start: block_1.number,
                end: block_2.number,
            })
            .await;

        assert_eq!(burn_sum, WeiNewtype::from(300));
    }
}
