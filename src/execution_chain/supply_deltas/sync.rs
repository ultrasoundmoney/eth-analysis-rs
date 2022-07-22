use futures::StreamExt;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::{Connection, PgExecutor, Row};
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use crate::config;
use crate::eth_units::Wei;
use crate::execution_chain::supply_deltas::node::get_supply_delta_by_block_number;
use crate::performance::LifetimeMeasure;

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
    accounts_count: 176_496_428,
    block_hash: "0xba7baa960085d0997884135a9c0f04f6b6de53164604084be701f98a31c4124d",
    block_number: 15_082_718,
    root: "655618..cfe0e6",
    balances_sum: 118908973575220938641041929,
};

async fn get_is_hash_known<'a>(executor: impl PgExecutor<'a>, block_hash: &str) -> bool {
    // Instead of the genesis parent_hash being absent, it is set to GENESIS_PARENT_HASH.
    // We'd like to have all supply deltas making only an exception for the genesis hash, but we
    // don't have all supply deltas and so have to contend with a snapshot eth supply as a
    // "jumping off point".
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

async fn get_last_synced_supply_delta_number<'a>(executor: impl PgExecutor<'a>) -> Option<u32> {
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

async fn get_last_synced_supply_delta<'a>(executor: impl PgExecutor<'a>) -> SupplyDelta {
    sqlx::query(
        "
            SELECT
                block_hash,
                block_number,
                parent_hash,
                supply_delta::TEXT,
                fee_burn::TEXT,
                fixed_reward::TEXT,
                self_destruct::TEXT,
                uncles_reward::TEXT
            FROM execution_supply_deltas
        ",
    )
    .map(|row: PgRow| SupplyDelta {
        block_number: row.get::<i32, _>("block_number") as u32,
        parent_hash: row.get("parent_hash"),
        block_hash: row.get("block_hash"),
        supply_delta: row
            .get::<String, _>("supply_delta")
            .parse::<i128>()
            .unwrap(),
        fee_burn: row.get::<String, _>("fee_burn").parse::<i128>().unwrap(),
        fixed_reward: row
            .get::<String, _>("fixed_reward")
            .parse::<i128>()
            .unwrap(),
        self_destruct: row
            .get::<String, _>("self_destruct")
            .parse::<i128>()
            .unwrap(),
        uncles_reward: row
            .get::<String, _>("uncles_reward")
            .parse::<i128>()
            .unwrap(),
    })
    .fetch_one(executor)
    .await
    .unwrap()
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

async fn store_delta<'a>(executor: impl PgExecutor<'a>, supply_delta: &SupplyDelta) {
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
    .execute(executor)
    .await
    .unwrap();
}

async fn get_balances_at_hash<'a>(executor: impl PgExecutor<'a>, block_hash: &str) -> i128 {
    // Instead of the genesis parent_hash being absent, it is set to GENESIS_PARENT_HASH.
    // We'd like to have all supply deltas making only an exception for the genesis hash, but we
    // don't have all supply deltas and so have to contend with a snapshot eth supply as a
    // "jumping off point".
    if block_hash == GENESIS_PARENT_HASH {
        panic!("missing hardcoded genesis parent balances sum")
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

pub async fn add_delta<'a>(executor: &mut PgConnection, supply_delta: &SupplyDelta) {
    let _ = LifetimeMeasure::log_lifetime("store delta");
    let mut transaction = executor.begin().await.unwrap();

    let is_parent_known = get_is_hash_known(&mut transaction, &supply_delta.parent_hash).await;

    if !is_parent_known {
        panic!(
            "trying to insert supply delta with missing parent hash: {}, parent_hash: {}",
            &supply_delta.block_hash, &supply_delta.parent_hash
        )
    }

    store_delta(&mut transaction, supply_delta).await;

    let balances = get_balances_at_hash(&mut transaction, &supply_delta.parent_hash).await
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
    .execute(&mut transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

async fn drop_supply_deltas_from<'a>(executor: &mut PgConnection, gte_block_number: &u32) {
    let mut transaction = executor.begin().await.unwrap();

    sqlx::query(
        r#"
            DELETE FROM execution_supply
            WHERE block_number >= $1
        "#,
    )
    .bind(*gte_block_number as i32)
    .execute(&mut *transaction)
    .await
    .unwrap();

    sqlx::query(
        r#"
            DELETE FROM execution_supply_deltas
            WHERE block_number >= $1
        "#,
    )
    .bind(*gte_block_number as i32)
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

enum NextStep {
    RollbackLastAndParent,
    RollbackLast,
    AddToCurrent,
}

async fn get_next_step(connection: &mut PgConnection, supply_delta: &SupplyDelta) -> NextStep {
    use sqlx::Acquire;

    let is_parent_known = get_is_hash_known(
        connection.acquire().await.unwrap(),
        &supply_delta.parent_hash,
    )
    .await;

    if !is_parent_known {
        return NextStep::RollbackLastAndParent;
    }

    let is_fork_block = get_is_block_number_known(
        connection.acquire().await.unwrap(),
        &supply_delta.block_number,
    )
    .await;

    if is_fork_block {
        return NextStep::RollbackLast;
    }

    return NextStep::AddToCurrent;
}

pub async fn sync_delta_next(
    connection: &mut PgConnection,
    deltas_queue: DeltasQueue,
    delta_to_sync: DeltaToSync,
) {
    use sqlx::Acquire;

    let supply_delta = match delta_to_sync {
        DeltaToSync::WithData(supply_delta) => supply_delta,
        DeltaToSync::WithoutData(supply_delta_number) => {
            get_supply_delta_by_block_number(supply_delta_number)
                .await
                .unwrap()
        }
    };

    match get_next_step(connection, &supply_delta).await {
        NextStep::RollbackLastAndParent => {
            tracing::info!(
                "parent of delta {} is missing, rolling back current and parent block",
                supply_delta.block_number
            );

            // Roll back current and parent supply delta in search of a common ancestor.
            drop_supply_deltas_from(connection, &(supply_delta.block_number - 1)).await;

            // Queue dropped deltas for sync. We can't be sure the delta we received will still be the
            // canonical delta when we sync this delta again, therefore we queue without data.
            tracing::debug!("queueing current and parent block for sync");
            deltas_queue
                .lock()
                .unwrap()
                .push_front(DeltaToSync::WithoutData(supply_delta.block_number));

            deltas_queue
                .lock()
                .unwrap()
                .push_front(DeltaToSync::WithoutData(supply_delta.block_number - 1));
        }
        NextStep::RollbackLast => {
            tracing::info!(
                "delta {} creates a fork, rolling back our last block",
                &supply_delta.block_number
            );
            // Roll back last synced, and queue for sync.
            drop_supply_deltas_from(
                connection.acquire().await.unwrap(),
                &supply_delta.block_number,
            )
            .await;

            // Queue dropped deltas for sync. We can't be sure the delta we received will still be the
            // canonical delta when we sync this delta again, therefore we queue without data.
            deltas_queue
                .lock()
                .unwrap()
                .push_front(DeltaToSync::WithoutData(supply_delta.block_number));
        }
        NextStep::AddToCurrent => {
            // Progress as usual.
            tracing::debug!("storing supply delta {}", supply_delta.block_number);
            add_delta(connection, &supply_delta).await;
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DeltaToSync {
    WithData(SupplyDelta),
    WithoutData(u32),
}

type DeltasQueue = Arc<Mutex<VecDeque<DeltaToSync>>>;

pub async fn sync_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing supply deltas");

    let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let last_synced_supply_delta_number_on_start =
        get_last_synced_supply_delta_number(&mut connection)
            .await
            .unwrap_or(SUPPLY_SNAPSHOT_15082718.block_number + 1);
    tracing::debug!("requesting supply deltas gte {last_synced_supply_delta_number_on_start}");
    let mut supply_delta_stream =
        super::stream_supply_deltas(last_synced_supply_delta_number_on_start + 1);

    let deltas_queue: DeltasQueue = Arc::new(Mutex::new(VecDeque::new()));

    while let Some(supply_delta) = supply_delta_stream.next().await {
        deltas_queue
            .lock()
            .unwrap()
            .push_back(DeltaToSync::WithData(supply_delta));

        // Work through the queue until it's empty.
        loop {
            match deltas_queue.lock().unwrap().pop_front() {
                None => {
                    // Continue syncing deltas that we've received from the node.
                    break;
                }
                Some(delta_to_sync) => {
                    // Because we may encounter rollbacks, this step may add more deltas to sync to
                    // the front of the queue.
                    sync_delta_next(&mut connection, deltas_queue.clone(), delta_to_sync).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use serial_test::serial;
    use sqlx::PgConnection;

    use super::*;

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

        add_delta(&mut transaction, &supply_delta_test).await;
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

        add_delta(&mut transaction, &supply_delta).await;
        let is_block_number_known =
            get_is_block_number_known(&mut transaction, &supply_delta.block_number).await;

        assert_eq!(is_block_number_known, true);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_balances_at_hash() {
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

        add_delta(&mut transaction, &supply_delta_test).await;
        let balances_sum =
            get_balances_at_hash(&mut transaction, &supply_delta_test.block_hash).await;

        assert_eq!(balances_sum, 1);
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

        add_delta(&mut transaction, &supply_delta_test).await;
        let balances_sum =
            get_balances_at_hash(&mut transaction, &supply_delta_test.block_hash).await;

        assert_eq!(balances_sum, 1);

        drop_supply_deltas_from(&mut transaction, &0).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_get_latest_synced_supply_delta_number_empty() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let latest_synced_supply_delta_number =
            get_last_synced_supply_delta_number(&mut transaction).await;

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

        add_delta(&mut transaction, &supply_delta_test).await;

        let latest_synced_supply_delta_number =
            get_last_synced_supply_delta_number(&mut transaction).await;

        assert_eq!(latest_synced_supply_delta_number, Some(0));
    }

    #[tokio::test]
    #[serial]
    async fn test_get_last_synced_supply_delta() {
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

        let supply_delta = get_last_synced_supply_delta(&mut transaction).await;
        assert_eq!(supply_delta, supply_delta_test);
    }

    #[ignore]
    #[tokio::test]
    #[serial]
    async fn test_reverted_fork() {
        // We test:
        // A -> B ---> C
        //   \---> B'
        // In this scenario we drop B when receiving B', but then get C, which expects parent B.
        // Because we are now missing B, we crash. We're fine on restart, but constant restarting
        // is bad. Our cluster scheduler may stop trying to restart our app if this happens a lot.
        let delta_b = SupplyDelta {
            supply_delta: 0,
            block_number: 15_082_719,
            block_hash: "0x5c47be526e24f7b58a27d5f6ad16e70df4ffc3bc3e3d3558267e92a4c4fff063"
                .to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };
        let delta_b_prime = SupplyDelta {
            supply_delta: 0,
            block_number: 15_082_719,
            block_hash: "0xB_PRIME".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };
        let delta_c = SupplyDelta {
            supply_delta: 0,
            block_number: 15_082_720,
            block_hash: "0x422fa5aed7e38dac0246e160472962fd385833c6ef0333d7c767e8729e5442c3"
                .to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0x5c47be526e24f7b58a27d5f6ad16e70df4ffc3bc3e3d3558267e92a4c4fff063"
                .to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let deltas_queue: DeltasQueue = Arc::new(Mutex::new(VecDeque::new()));

        // Sync a delta.
        sync_delta_next(
            &mut transaction,
            deltas_queue.clone(),
            DeltaToSync::WithData(delta_b),
        )
        .await;
        // Fork that delta.
        sync_delta_next(
            &mut transaction,
            deltas_queue.clone(),
            DeltaToSync::WithData(delta_b_prime.clone()),
        )
        .await;

        let expected_1 = VecDeque::from(vec![DeltaToSync::WithoutData(15_082_719)]);
        assert_eq!(deltas_queue.lock().unwrap().clone(), expected_1);

        // B 15_082_719, has been dropped, because syncing B' 15_082_719 talks to a real node, it
        // would sync the real 15_082_719, but we want C 15_082_720 to be forking B' 15_082_719, only
        // accepting building on B. We force insert B' as 15_082_719.
        // real 15_082_719.
        deltas_queue.lock().unwrap().pop_front();
        add_delta(&mut transaction, &delta_b_prime).await;

        // Now try to process C which depends on B which we've dropped.
        sync_delta_next(
            &mut transaction,
            deltas_queue.clone(),
            DeltaToSync::WithData(delta_c),
        )
        .await;

        let expected = VecDeque::from(vec![
            DeltaToSync::WithoutData(15_082_719),
            DeltaToSync::WithoutData(15_082_720),
        ]);
        let actual = deltas_queue.lock().unwrap().clone();
        assert_eq!(actual, expected);
    }
}
