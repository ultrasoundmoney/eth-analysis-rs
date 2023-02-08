use futures::join;
use sqlx::PgExecutor;

use crate::{
    execution_chain::{BlockRange, BlockStore},
    units::UsdNewtype,
};

#[allow(dead_code)]
pub async fn average_from_block_range(
    executor: impl PgExecutor<'_>,
    block_store: &BlockStore<'_>,
    block_range: &BlockRange,
) -> Option<UsdNewtype> {
    let (start_timestamp, end_timestamp) = join!(
        block_store.timestamp_from_number(&block_range.start),
        block_store.timestamp_from_number(&block_range.end)
    );

    sqlx::query!(
        "
        SELECT
            AVG(ethusd)
        FROM
            eth_prices
        WHERE
            timestamp >= $1
            AND timestamp <= $2
        ",
        start_timestamp,
        end_timestamp
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .avg
    .map(|avg| avg.into())
}

#[cfg(test)]
mod tests {
    use crate::{db::tests::TestDb, execution_chain::ExecutionNodeBlockBuilder, usd_price};

    use super::*;

    #[tokio::test]
    async fn average_from_block_range_test() {
        let test_db = TestDb::new().await;
        let db_pool = test_db.pool();
        let test_id = "average_from_block_range_test";
        let block_store = BlockStore::new(db_pool);

        let test_block_1 = ExecutionNodeBlockBuilder::new(test_id)
            .with_number(1)
            .build();
        let test_block_2 = ExecutionNodeBlockBuilder::from_parent(&test_block_1).build();
        let test_block_3 = ExecutionNodeBlockBuilder::from_parent(&test_block_2).build();

        usd_price::store_price(
            &mut db_pool.acquire().await.unwrap(),
            test_block_1.timestamp,
            1.0,
        )
        .await;
        usd_price::store_price(
            &mut db_pool.acquire().await.unwrap(),
            test_block_2.timestamp,
            2.0,
        )
        .await;
        usd_price::store_price(
            &mut db_pool.acquire().await.unwrap(),
            test_block_3.timestamp,
            3.0,
        )
        .await;

        let block_range = BlockRange {
            start: test_block_1.number,
            end: test_block_2.number,
        };

        let average = average_from_block_range(db_pool, &block_store, &block_range)
            .await
            .unwrap();

        assert_eq!(average.0, 1.5);

        test_db.cleanup().await;
    }
}
