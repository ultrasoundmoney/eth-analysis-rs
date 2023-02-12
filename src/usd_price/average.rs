use chrono::{DateTime, Utc};
use sqlx::PgExecutor;

use crate::units::UsdNewtype;

pub async fn average_from_time_range(
    executor: impl PgExecutor<'_>,
    start_timestamp: DateTime<Utc>,
    end_timestamp: DateTime<Utc>,
) -> UsdNewtype {
    sqlx::query!( r#" SELECT AVG(ethusd) AS "avg!" FROM eth_prices WHERE timestamp >= $1 AND timestamp <= $2 "#, start_timestamp, end_timestamp,).fetch_one(executor).await.unwrap().avg.into()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use crate::{
        db::tests::TestDb,
        execution_chain::{BlockStore, ExecutionNodeBlockBuilder},
        usd_price,
    };

    use super::*;

    #[tokio::test]
    async fn average_from_time_range_test() {
        let test_db = TestDb::new().await;
        let db_pool = test_db.pool();
        let test_id = "average_from_block_range_test";
        let block_store = BlockStore::new(db_pool);

        let test_block_1 = ExecutionNodeBlockBuilder::new(test_id)
            .with_number(1)
            .with_timestamp(&"2023-02-08T00:00:00Z".parse::<DateTime<Utc>>().unwrap())
            .build();
        let test_block_2 = ExecutionNodeBlockBuilder::from_parent(&test_block_1)
            .with_timestamp(&"2023-02-08T00:00:10Z".parse::<DateTime<Utc>>().unwrap())
            .build();
        let test_block_3 = ExecutionNodeBlockBuilder::from_parent(&test_block_2)
            .with_timestamp(&"2023-02-08T00:00:20Z".parse::<DateTime<Utc>>().unwrap())
            .build();

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

        let average =
            average_from_time_range(db_pool, test_block_1.timestamp, test_block_2.timestamp).await;

        assert_eq!(average.0, 1.5);

        test_db.cleanup().await;
    }
}
