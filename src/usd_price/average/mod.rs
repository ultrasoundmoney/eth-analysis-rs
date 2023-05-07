use std::collections::HashMap;

use enum_iterator::all;
use futures::future::join_all;
use sqlx::PgPool;
use tracing::debug;

use super::store::EthPriceStore;
use crate::{
    caching::{self, CacheKey},
    execution_chain::ExecutionNodeBlock,
    performance::TimedExt,
    time_frames::TimeFrame,
};

pub async fn on_new_block(
    db_pool: &PgPool,
    eth_price_store: &impl EthPriceStore,
    block: &ExecutionNodeBlock,
) {
    let futures = all::<TimeFrame>().map(|time_frame| async move {
        let average = eth_price_store
            .average_from_block_plus_time_range(block, &time_frame)
            .timed(&format!(
                "average_price_from_block {} {}",
                block.number, time_frame,
            ))
            .await;
        (time_frame, average)
    });

    let pairs = join_all(futures).await;
    let eth_prices = pairs.into_iter().collect::<HashMap<_, _>>();

    debug!("calculated new average prices");

    caching::update_and_publish(db_pool, &CacheKey::AverageEthPrice, &eth_prices)
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use test_context::test_context;

    use crate::{
        db::tests::TestDb,
        execution_chain::ExecutionNodeBlockBuilder,
        usd_price::store::{EthPriceStore, EthPriceStorePostgres},
    };

    #[test_context(TestDb)]
    #[tokio::test]
    async fn average_from_time_range_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_id = "average_from_block_range_test";

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

        eth_price_store
            .store_price(&test_block_1.timestamp, 1.0)
            .await;
        eth_price_store
            .store_price(&test_block_2.timestamp, 2.0)
            .await;
        eth_price_store
            .store_price(&test_block_3.timestamp, 3.0)
            .await;

        let average = eth_price_store
            .average_from_time_range(test_block_1.timestamp, test_block_2.timestamp)
            .await;

        assert_eq!(average.0, 1.5);
    }
}
