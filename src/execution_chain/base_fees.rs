use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};

use crate::{
    caching::{self, CacheKey},
    key_value_store,
};

use super::node::ExecutionNodeBlock;

#[derive(Debug, Serialize)]
struct BaseFeePerGas {
    timestamp: DateTime<Utc>,
    wei: u64,
}

async fn update_last_base_fee(executor: &PgPool, block: &ExecutionNodeBlock) {
    tracing::debug!("updating current base fee");

    let base_fee_per_gas = BaseFeePerGas {
        timestamp: block.timestamp,
        wei: block.base_fee_per_gas,
    };

    key_value_store::set_value(
        executor,
        &CacheKey::BaseFeePerGas.to_db_key(),
        &serde_json::to_value(base_fee_per_gas).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeePerGas).await;
}

type WeiU64 = u64;

#[derive(Debug, PartialEq, Serialize)]
struct BaseFeeAtTime {
    wei: WeiU64,
    timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
struct BaseFeesOverTime {
    block_number: u32,
    d1: Vec<BaseFeeAtTime>,
}

async fn get_base_fee_over_time_d1<'a>(executor: impl PgExecutor<'a>) -> Vec<BaseFeeAtTime> {
    sqlx::query(
        "
            SELECT
                timestamp,
                base_fee_per_gas
            FROM
                blocks_next
            WHERE
                timestamp >= NOW() - '1 day'::INTERVAL
            ORDER BY number DESC
        ",
    )
    .map(|row: PgRow| {
        let wei = row.get::<i64, _>("base_fee_per_gas") as u64;
        let timestamp = row.get::<DateTime<Utc>, _>("timestamp");
        BaseFeeAtTime { wei, timestamp }
    })
    .fetch_all(executor)
    .await
    .unwrap()
}

async fn update_base_fee_over_time(executor: &PgPool, block: &ExecutionNodeBlock) {
    tracing::debug!("updating base fee over time");

    let base_fees_d1 = get_base_fee_over_time_d1(executor).await;

    let base_fees_over_time = BaseFeesOverTime {
        block_number: block.number,
        d1: base_fees_d1,
    };

    key_value_store::set_value(
        executor,
        CacheKey::BaseFeeOverTime.to_db_key(),
        &serde_json::to_value(base_fees_over_time).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::BaseFeeOverTime).await;
}

pub async fn on_new_block<'a>(db_pool: &PgPool, block: &ExecutionNodeBlock) {
    join!(
        update_last_base_fee(db_pool, block),
        update_base_fee_over_time(db_pool, block)
    );
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use sqlx::Acquire;

    use crate::{db_testing, execution_chain::block_store::BlockStore};

    use super::*;

    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 1,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            total_difficulty: 10,
        }
    }

    #[tokio::test]
    async fn get_base_fee_over_time_d1_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let mut block_store = BlockStore::new(&mut *transaction);
        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;

        let base_fees_d1 = get_base_fee_over_time_d1(&mut transaction).await;

        assert_eq!(
            base_fees_d1,
            vec![BaseFeeAtTime {
                wei: 1,
                timestamp: test_block.timestamp
            }]
        );
    }
}
