use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};
use tracing::debug;

use crate::{
    execution_chain::{BlockNumber, BlockRange},
    time_frames::TimeFrame,
    units::{UsdNewtype, WeiNewtype},
};

use super::BurnSumRecord;

const REORG_LIMIT: i32 = 100;

#[async_trait]
pub trait BurnSumStore {
    async fn burn_sum_from_block_range(&self, block_range: &BlockRange)
        -> (WeiNewtype, UsdNewtype);
    async fn delete_old_sums(&self, last_block: BlockNumber);
    async fn last_burn_sum(&self, time_frame: &TimeFrame) -> Option<BurnSumRecord>;
    async fn store_burn_sums(&self, burn_sum: &[BurnSumRecord]);
    async fn delete_new_sums_tx<'a>(
        transaction: &'a mut PgConnection,
        block_number_gte: &'a BlockNumber,
    );
}

pub struct BurnSumStorePostgres<'a> {
    db_pool: &'a PgPool,
}

impl<'a> BurnSumStorePostgres<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl BurnSumStore for BurnSumStorePostgres<'_> {
    async fn burn_sum_from_block_range(
        &self,
        block_range: &BlockRange,
    ) -> (WeiNewtype, UsdNewtype) {
        let row = sqlx::query!(
            r#"
            SELECT
                SUM(base_fee_per_gas::NUMERIC(78) * gas_used::NUMERIC(78))::TEXT
                    AS "burn_sum_wei!",
                SUM(base_fee_per_gas::NUMERIC(78) * gas_used::NUMERIC(78) / 1e18 * eth_price)
                    AS "burn_sum_usd!"
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
        .unwrap();

        let wei = row.burn_sum_wei.parse().unwrap();
        let usd = row.burn_sum_usd.into();

        (wei, usd)
    }

    async fn delete_old_sums(&self, last_block: BlockNumber) {
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
    async fn last_burn_sum(&self, time_frame: &TimeFrame) -> Option<BurnSumRecord> {
        let row = sqlx::query!(
            r#"
            SELECT
                first_included_block_number,
                last_included_block_number,
                last_included_block_hash,
                timestamp,
                sum_usd,
                sum_wei::TEXT AS "sum_wei!"
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
            sum_usd: row.sum_usd.into(),
            sum_wei: row.sum_wei.parse().unwrap(),
            time_frame: *time_frame,
            timestamp: row.timestamp,
        })
    }

    async fn store_burn_sums(&self, burn_sum: &[BurnSumRecord]) {
        let mut v1: Vec<String> = Vec::new();
        let mut v2: Vec<BlockNumber> = Vec::new();
        let mut v3: Vec<BlockNumber> = Vec::new();
        let mut v4: Vec<String> = Vec::new();
        let mut v5: Vec<DateTime<Utc>> = Vec::new();
        let mut v6: Vec<f64> = Vec::new();
        let mut v7: Vec<String> = Vec::new();
        burn_sum.iter().for_each(|burn_sum| {
            v1.push(burn_sum.time_frame.to_string());
            v2.push(burn_sum.first_included_block_number);
            v3.push(burn_sum.last_included_block_number);
            v4.push(burn_sum.last_included_block_hash.to_owned());
            v5.push(burn_sum.timestamp);
            v6.push(burn_sum.sum_usd.0);
            v7.push(Into::<String>::into(burn_sum.sum_wei) as String);
        });
        sqlx::query!(
            "
            INSERT INTO burn_sums (
                time_frame,
                first_included_block_number,
                last_included_block_number,
                last_included_block_hash,
                timestamp,
                sum_usd,
                sum_wei
            )
            SELECT * FROM UNNEST (
                $1::text[],
                $2::int[],
                $3::int[],
                $4::text[],
                $5::timestamptz[],
                $6::float8[],
                $7::numeric[]
            )
            ",
            &v1,
            &v2,
            &v3,
            &v4,
            &v5,
            &v6,
            &v7 as &[String]
        )
        .execute(self.db_pool)
        .await
        .unwrap();
    }

    async fn delete_new_sums_tx<'a>(
        transaction: &'a mut PgConnection,
        block_number_gte: &'a BlockNumber,
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
    use test_context::test_context;

    use crate::{
        db::tests::TestDb,
        execution_chain::{block_store, ExecutionNodeBlockBuilder},
    };

    use super::*;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn burn_sum_from_block_range_test(test_db: &TestDb) {
        let pool = &test_db.pool;
        let burn_sum_store = BurnSumStorePostgres::new(pool);

        let test_id = "burn_sum_from_time_frame";

        let block_1 = ExecutionNodeBlockBuilder::new(test_id)
            .with_burn(WeiNewtype::from_eth(1))
            .build();
        let block_2 = ExecutionNodeBlockBuilder::from_parent(&block_1)
            .with_burn(WeiNewtype::from_eth(2))
            .build();

        block_store::store_block(pool, &block_1, 1.0).await;
        block_store::store_block(pool, &block_2, 2.0).await;

        let (burn_sum_wei, burn_sum_usd) = burn_sum_store
            .burn_sum_from_block_range(&BlockRange {
                start: block_1.number,
                end: block_2.number,
            })
            .await;

        assert_eq!(burn_sum_wei, WeiNewtype::from_eth(3));
        assert_eq!(burn_sum_usd, UsdNewtype(5.0));
    }
}
