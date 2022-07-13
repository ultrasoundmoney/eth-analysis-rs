use futures::StreamExt;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::{Connection, PgExecutor, Row};
use std::str::FromStr;

use crate::config;
use crate::eth_units::Wei;
use crate::execution_chain::node;

use super::SupplyDelta;

const GENESIS_PARENT_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

#[allow(dead_code)]
struct SupplySnapshot {
    accounts_count: u64,
    block_hash: &'static str,
    block_number: u32,
    root: &'static str,
    balances_sum: Wei,
}

const SUPPLY_SNAPSHOT_15082718: SupplySnapshot = SupplySnapshot {
    accounts_count: 176496428,
    block_hash: "0xba7baa960085d0997884135a9c0f04f6b6de53164604084be701f98a31c4124d",
    block_number: 15082718,
    root: "655618..cfe0e6",
    balances_sum: 118908973575220940,
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

async fn get_balances_at_hash<'a>(executor: impl PgExecutor<'a>, block_hash: &str) -> i128 {
    // Instead of the genesis parent_hash being absent, it is set to GENESIS_PARENT_HASH.
    // We'd like to have all supply deltas making only an exception for the genesis hash, but we
    // don't have all supply deltas and so have to contend with a snapshot total supply as a
    // "jumping off point" for which we're also missing the hash.
    if block_hash == GENESIS_PARENT_HASH {
        panic!("missing hardcoded genesis parent total supply")
    }

    if block_hash == SUPPLY_SNAPSHOT_15082718.block_hash {
        return SUPPLY_SNAPSHOT_15082718.balances_sum;
    }

    if block_hash == "0xtestparent" {
        return 0;
    }

    sqlx::query(
        "
            SELECT balances_sum::TEXT FROM execution_supply
            WHERE block_hash = $1
        ",
    )
    .bind(block_hash)
    .map(|row: PgRow| {
        let balances_str = row.get::<String, _>("balances_sum");
        i128::from_str(&balances_str).unwrap()
    })
    .fetch_one(executor)
    .await
    .unwrap()
}

pub async fn store_delta<'a>(executor: &mut PgConnection, supply_delta: &SupplyDelta) {
    let mut transaction = executor.begin().await.unwrap();

    let is_parent_known = get_is_hash_known(&mut *transaction, &supply_delta.parent_hash).await;

    if !is_parent_known {
        panic!(
            "trying to insert supply delta with missing parent hash: {}, parent_hash: {}",
            &supply_delta.block_hash, &supply_delta.parent_hash
        )
    }

    sqlx::query(
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
            VALUES (
                $1,
                $2,
                $3::NUMERIC,
                $4::NUMERIC,
                $5,
                $6::NUMERIC,
                $7::NUMERIC,
                $8::NUMERIC
            )
        ",
    )
    .bind(supply_delta.block_hash.clone())
    .bind(supply_delta.block_number as i32)
    .bind(supply_delta.fee_burn.to_string())
    .bind(supply_delta.fixed_reward.to_string())
    .bind(supply_delta.parent_hash.to_string())
    .bind(supply_delta.self_destruct.to_string())
    .bind(supply_delta.supply_delta.to_string())
    .bind(supply_delta.uncles_reward.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();

    let balances = get_balances_at_hash(&mut *transaction, &supply_delta.parent_hash).await
        + supply_delta.supply_delta;

    sqlx::query(
        "
            INSERT INTO execution_supply (
                block_hash,
                block_number,
                balances_sum
            ) VALUES ($1, $2, $3::NUMERIC)
       ",
    )
    .bind(supply_delta.block_hash.clone())
    .bind(supply_delta.block_number as i32)
    .bind(balances.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

async fn get_latest_synced_supply_delta_number<'a>(executor: impl PgExecutor<'a>) -> Option<u32> {
    let max_opt: Option<i32> = sqlx::query(
        "
            SELECT MAX(block_number) FROM execution_supply_deltas
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .get("max");

    max_opt.map(|max| max as u32)
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
    .bind(*block_number as i32)
    .fetch_one(executor)
    .await
    .unwrap()
    .get("exists")
}

async fn drop_supply_deltas_from<'a>(executor: &mut PgConnection, block_number: &u32) {
    let mut transaction = executor.begin().await.unwrap();

    sqlx::query(
        r#"
            DELETE FROM execution_supply
            WHERE block_number >= $1
        "#,
    )
    .bind(*block_number as i32)
    .execute(&mut *transaction)
    .await
    .unwrap();

    sqlx::query(
        r#"
            DELETE FROM execution_supply_deltas
            WHERE block_number >= $1
        "#,
    )
    .bind(*block_number as i32)
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

pub async fn sync_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing supply deltas");

    let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let latest_synced_supply_delta_number = get_latest_synced_supply_delta_number(&mut connection)
        .await
        .unwrap_or(SUPPLY_SNAPSHOT_15082718.block_number + 1);
    tracing::debug!("requesting supply deltas gte {latest_synced_supply_delta_number}");
    let mut supply_delta_stream = node::stream_supply_deltas(latest_synced_supply_delta_number);

    while let Some(supply_delta) = supply_delta_stream.next().await {
        let is_fork_block =
            get_is_block_number_known(&mut connection, &supply_delta.block_number).await;

        if is_fork_block {
            tracing::debug!(
                "supply delta {}, with hash {}, is a fork block supply delta",
                supply_delta.block_number,
                supply_delta.block_hash
            );

            tracing::debug!("dropping execution_supply and execution_supply_deltas rows with block_number greater than or equal to {}", &supply_delta.block_number);
            drop_supply_deltas_from(&mut connection, &supply_delta.block_number).await;
        }

        tracing::debug!("storing supply delta {}", supply_delta.block_number);
        store_delta(&mut connection, &supply_delta).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use sqlx::PgConnection;

    #[tokio::test]
    #[serial]
    async fn test_get_is_hash_not_known() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, "0xnot_there").await;

        assert_eq!(is_hash_known, false);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_hash_known() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

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

        store_delta(&mut transaction, &supply_delta_test).await;
        let is_hash_known =
            get_is_hash_known(&mut transaction, &supply_delta_test.block_hash).await;

        assert_eq!(is_hash_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_block_number_not_known() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let is_block_number_known = get_is_block_number_known(&mut transaction, &0).await;

        assert_eq!(is_block_number_known, false);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_is_block_number_known() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

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

        store_delta(&mut transaction, &supply_delta).await;
        let is_block_number_known =
            get_is_block_number_known(&mut transaction, &supply_delta.block_number).await;

        assert_eq!(is_block_number_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_supply_at_hash() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

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

        store_delta(&mut transaction, &supply_delta_test).await;
        let total_supply =
            get_balances_at_hash(&mut transaction, &supply_delta_test.block_hash).await;

        assert_eq!(total_supply, 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_drop_supply_deltas() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

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

        store_delta(&mut transaction, &supply_delta_test).await;
        let total_supply =
            get_balances_at_hash(&mut transaction, &supply_delta_test.block_hash).await;

        assert_eq!(total_supply, 1);

        drop_supply_deltas_from(&mut transaction, &0).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_get_latest_synced_supply_delta_number_empty() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let latest_synced_supply_delta_number =
            get_latest_synced_supply_delta_number(&mut transaction).await;

        assert_eq!(latest_synced_supply_delta_number, None);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_latest_synced_supply_delta_number() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

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

        store_delta(&mut transaction, &supply_delta_test).await;

        let latest_synced_supply_delta_number =
            get_latest_synced_supply_delta_number(&mut transaction).await;

        assert_eq!(latest_synced_supply_delta_number, Some(0));
    }
}
