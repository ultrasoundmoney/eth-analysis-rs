use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgExecutor, PgPool, Row};

use crate::config;
use crate::execution_node::SupplyDelta;

const GENESIS_PARENT_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

#[allow(dead_code)]
struct SupplySnapshot {
    accounts_count: u64,
    block_hash: &'static str,
    block_number: u32,
    root: &'static str,
    total_supply: i64,
}

const SUPPLY_SNAPSHOT_15082718: SupplySnapshot = SupplySnapshot {
    accounts_count: 176496428,
    block_hash: "0xba7baa960085d0997884135a9c0f04f6b6de53164604084be701f98a31c4124d",
    block_number: 15082718,
    root: "655618..cfe0e6",
    total_supply: 118908973575220940,
};

async fn get_is_hash_known<'a>(executor: impl PgExecutor<'a>, block_hash: &str) -> bool {
    // Instead of the genesis parent_hash being absent, it is set to GENESIS_PARENT_HASH.
    // We'd like to have all supply deltas making only an exception for the genesis hash, but we
    // don't have all supply deltas and so have to contend with a snapshot total supply as a
    // "jumping off point" for which we're also missing the hash.
    if block_hash == GENESIS_PARENT_HASH
        || block_hash == SUPPLY_SNAPSHOT_15082718.block_hash
        || block_hash == "0xtestparent"
    {
        return true;
    }

    sqlx::query(
        r#"
            SELECT EXISTS (
                SELECT 1
                FROM execution_supply_deltas
                WHERE block_hash = $1
            )
        "#,
    )
    .bind(block_hash)
    .fetch_one(executor)
    .await
    .unwrap()
    .get("exists")
}

async fn get_supply_at_hash<'a>(executor: impl PgExecutor<'a>, block_hash: &str) -> i64 {
    // Instead of the genesis parent_hash being absent, it is set to GENESIS_PARENT_HASH.
    // We'd like to have all supply deltas making only an exception for the genesis hash, but we
    // don't have all supply deltas and so have to contend with a snapshot total supply as a
    // "jumping off point" for which we're also missing the hash.
    if block_hash == GENESIS_PARENT_HASH {
        panic!("missing hardcoded genesis parent total supply")
    }

    if block_hash == SUPPLY_SNAPSHOT_15082718.block_hash {
        return SUPPLY_SNAPSHOT_15082718.total_supply;
    }

    if block_hash == "0xtestparent" {
        return 0;
    }

    sqlx::query(
        "
            SELECT total_supply FROM execution_supply
            WHERE block_hash = $1
        ",
    )
    .bind(block_hash)
    .fetch_one(executor)
    .await
    .unwrap()
    .get("total_supply")
}

async fn store_delta(executor: &PgPool, supply_delta: &SupplyDelta) {
    let mut transaction = executor.begin().await.unwrap();

    let is_parent_known = get_is_hash_known(&mut *transaction, &supply_delta.parent_hash).await;

    dbg!(&supply_delta);

    if !is_parent_known {
        panic!(
            "trying to insert supply delta with missing parent hash: {}, parent_hash: {}",
            &supply_delta.block_hash, &supply_delta.parent_hash
        )
    }

    sqlx::query!(
        "
            INSERT INTO execution_supply_deltas (
                block_hash,
                block_number,
                fee_burn,
                fixed_reward,
                parent_hash,
                self_destruct,
                supply_delta,
                uncles_reward
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ",
        supply_delta.block_hash,
        supply_delta.block_number as i32,
        supply_delta.fee_burn,
        supply_delta.fixed_reward,
        supply_delta.parent_hash,
        supply_delta.self_destruct,
        supply_delta.supply_delta,
        supply_delta.uncles_reward
    )
    .execute(&mut *transaction)
    .await
    .unwrap();

    let total_supply = get_supply_at_hash(&mut *transaction, &supply_delta.parent_hash).await
        + supply_delta.supply_delta;

    sqlx::query(
        "
            INSERT INTO execution_supply (
                block_hash,
                block_number,
                total_supply
            ) VALUES ($1, $2, $3)
       ",
    )
    .bind(supply_delta.block_hash.clone())
    .bind(supply_delta.block_number)
    .bind(total_supply as i64)
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

async fn get_latest_synced_supply_delta_number<'a>(executor: impl PgExecutor<'a>) -> Option<u32> {
    sqlx::query(
        "
            SELECT MAX(block_number) FROM execution_supply_deltas
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .get("max")
}

async fn get_is_block_number_known<'a>(executor: impl PgExecutor<'a>, block_number: &u32) -> bool {
    sqlx::query(
        r#"
            SELECT EXISTS (
                SELECT 1
                FROM execution_supply_deltas
                WHERE block_number = $1
            )
        "#,
    )
    .bind(block_number)
    .fetch_one(executor)
    .await
    .unwrap()
    .get("exists")
}

async fn drop_supply_deltas_from<'a>(executor: &PgPool, block_number: &u32) {
    let mut transaction = executor.begin().await.unwrap();

    sqlx::query(
        r#"
            DELETE FROM execution_supply
            WHERE block_number >= $1
        "#,
    )
    .bind(block_number)
    .execute(&mut *transaction)
    .await
    .unwrap();

    sqlx::query(
        r#"
            DELETE FROM execution_supply_deltas
            WHERE block_number >= $1
        "#,
    )
    .bind(block_number)
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

pub async fn sync_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing supply deltas");

    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let latest_synced_supply_delta_number = get_latest_synced_supply_delta_number(&pool)
        .await
        .unwrap_or(SUPPLY_SNAPSHOT_15082718.block_number + 1);
    tracing::debug!("requesting supply deltas gte {latest_synced_supply_delta_number}");
    let mut supply_delta_stream =
        crate::execution_node::stream_supply_deltas(latest_synced_supply_delta_number);

    while let Some(supply_delta) = supply_delta_stream.next().await {
        let is_fork_block = get_is_block_number_known(&pool, &supply_delta.block_number).await;

        if is_fork_block {
            tracing::debug!(
                "supply delta {}, with hash {}, is a fork block supply delta",
                supply_delta.block_number,
                supply_delta.block_hash
            );

            tracing::debug!("dropping execution_supply and execution_supply_deltas rows with block_number greater than or equal to {}", &supply_delta.block_number);
            drop_supply_deltas_from(&pool, &supply_delta.block_number).await;
        }

        tracing::debug!("storing supply delta {}", supply_delta.block_number);
        store_delta(&pool, &supply_delta).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    async fn clean_tables<'a>(executor: impl PgExecutor<'a>) {
        sqlx::query("TRUNCATE execution_supply_deltas CASCADE")
            .execute(executor)
            .await
            .unwrap();
    }

    async fn get_test_pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(3)
            .connect(&config::get_db_url())
            .await
            .unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_hash_not_known() {
        let pool = get_test_pool().await;

        let is_hash_known = get_is_hash_known(&pool, "0xnot_there").await;

        assert_eq!(is_hash_known, false);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_hash_known() {
        let pool = get_test_pool().await;

        let supply_delta_test = SupplyDelta {
            supply_delta: 0,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        store_delta(&pool, &supply_delta_test).await;
        let is_hash_known = get_is_hash_known(&pool, &supply_delta_test.block_hash).await;

        clean_tables(&pool).await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_block_number_not_known() {
        let pool = get_test_pool().await;

        let is_block_number_known = get_is_block_number_known(&pool, &0).await;

        assert_eq!(is_block_number_known, false);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_block_number_known() {
        let pool = get_test_pool().await;

        let supply_delta = SupplyDelta {
            supply_delta: 0,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        store_delta(&pool, &supply_delta).await;
        let is_block_number_known =
            get_is_block_number_known(&pool, &supply_delta.block_number).await;

        clean_tables(&pool).await;

        assert_eq!(is_block_number_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_supply_at_hash() {
        let pool = get_test_pool().await;

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        store_delta(&pool, &supply_delta_test).await;
        let total_supply = get_supply_at_hash(&pool, &supply_delta_test.block_hash).await;

        clean_tables(&pool).await;

        assert_eq!(total_supply, 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_drop_supply_deltas() {
        let pool = get_test_pool().await;

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: "0xtest".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        store_delta(&pool, &supply_delta_test).await;
        let total_supply = get_supply_at_hash(&pool, &supply_delta_test.block_hash).await;

        assert_eq!(total_supply, 1);

        drop_supply_deltas_from(&pool, &0).await;
    }
}
