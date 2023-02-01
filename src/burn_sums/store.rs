use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use tracing::debug;

use crate::{
    execution_chain::{BlockNumber, BlockRange, ExecutionNodeBlock},
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

    pub async fn burn_sum_from_time_frame(
        &self,
        time_frame: &TimeFrame,
        block: &ExecutionNodeBlock,
    ) -> WeiNewtype {
        match time_frame {
            TimeFrame::Growing(growing_time_frame) => sqlx::query!(
                r#"
                    SELECT
                        SUM(base_fee_per_gas::NUMERIC(78) * gas_used::NUMERIC(78))::TEXT AS "burn_sum!"
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= $1
                "#,
                growing_time_frame.start()
            )
            .fetch_one(self.db_pool)
            .await
            .unwrap()
            .burn_sum
            .parse()
            .unwrap(),
            TimeFrame::Limited(limited_time_frame) => sqlx::query!(
                r#"
                    SELECT 
                        SUM(base_fee_per_gas::NUMERIC(78) * gas_used::NUMERIC(78))::TEXT AS "burn_sum!"
                    FROM
                        blocks_next
                    WHERE
                        timestamp >= $1::TIMESTAMPTZ - $2::INTERVAL AND timestamp <= $1
                "#,
                block.timestamp,
                limited_time_frame.postgres_interval()
            )
            .fetch_one(self.db_pool)
            .await
            .unwrap()
            .burn_sum
            .parse()
            .unwrap(),
        }
    }

    pub async fn burn_sum_from_time_range(
        &self,
        from_gte: DateTime<Utc>,
        to_lt: DateTime<Utc>,
    ) -> WeiNewtype {
        sqlx::query!(
            r#"
                SELECT
                    SUM(base_fee_per_gas::NUMERIC(78) * gas_used::NUMERIC(78))::TEXT AS "burn_sum!"
                FROM
                    blocks_next
                WHERE
                    timestamp >= $1 AND timestamp < $2
            "#,
            from_gte,
            to_lt
        )
        .fetch_one(self.db_pool)
        .await
        .unwrap()
        .burn_sum
        .parse()
        .unwrap()
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
            block_range.lowest,
            block_range.highest
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
                WHERE block_number < $1
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
                    block_number,
                    block_hash,
                    timestamp,
                    sum::TEXT AS "sum!"
                FROM burn_sums
                WHERE time_frame = $1
                ORDER BY block_number DESC
                LIMIT 1
            "#,
            time_frame.to_string()
        )
        .fetch_optional(self.db_pool)
        .await
        .unwrap();

        row.map(|row| BurnSumRecord {
            time_frame: *time_frame,
            block_number: row.block_number,
            block_hash: row.block_hash,
            timestamp: row.timestamp,
            sum: row.sum.parse().unwrap(),
        })
    }

    async fn store_burn_sum(&self, burn_sum: &BurnSumRecord) {
        sqlx::query!(
            "
                INSERT INTO burn_sums (
                    time_frame,
                    block_number,
                    block_hash,
                    timestamp,
                    sum
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5::NUMERIC
                )
            ",
            burn_sum.time_frame.to_string(),
            burn_sum.block_number,
            burn_sum.block_hash,
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

    // For debugging pursposes only
    pub async fn blocks_from_time_range(
        &self,
        from_gte: DateTime<Utc>,
        to_lt: DateTime<Utc>,
    ) -> Vec<ExecutionNodeBlock> {
        let rows = sqlx::query!(
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
                FROM blocks_next
                WHERE timestamp >= $1 AND timestamp < $2
            "#,
            from_gte,
            to_lt
        )
        .fetch_all(self.db_pool)
        .await
        .unwrap();

        rows.into_iter()
            .map(|row| ExecutionNodeBlock {
                hash: row.hash,
                number: row.number,
                parent_hash: row.parent_hash,
                timestamp: row.timestamp,
                difficulty: row.difficulty as u64,
                total_difficulty: row.total_difficulty.parse().unwrap(),
                gas_used: row.gas_used,
                base_fee_per_gas: row.base_fee_per_gas as u64,
            })
            .collect()
    }

    pub async fn delete_new_sums_tx(
        transaction: &mut PgConnection,
        block_number_gte: &BlockNumber,
    ) {
        sqlx::query!(
            "
                DELETE FROM burn_sums
                WHERE block_number >= $1
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
    use crate::execution_chain::{block_store, ExecutionNodeBlockBuilder};
    use crate::time_frames::{GrowingTimeFrame, TimeFrame};

    use TimeFrame::*;

    use super::*;

    #[ignore]
    #[sqlx::test]
    fn burn_sum_from_time_frame_test(pool: PgPool) {
        let burn_sum_store = BurnSumStore::new(&pool);

        let test_id = "burn_sum_from_time_frame";

        let block_1 = ExecutionNodeBlockBuilder::new(test_id)
            .with_burn(100)
            .build();
        let block_2 = ExecutionNodeBlockBuilder::from_parent(&block_1)
            .with_burn(200)
            .build();

        block_store::store_block(&pool, &block_1, 0.0).await;
        block_store::store_block(&pool, &block_2, 0.0).await;

        let burn_sum = burn_sum_store
            .burn_sum_from_time_frame(&Growing(GrowingTimeFrame::SinceBurn), &block_2)
            .await;

        assert_eq!(burn_sum, WeiNewtype::from(300));
    }
}
