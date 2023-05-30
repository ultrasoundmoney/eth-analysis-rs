use futures::StreamExt;
use sqlx::postgres::{PgConnection, PgQueryResult, PgRow};
use sqlx::{Connection, PgExecutor, Row};
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use super::node::{get_supply_delta_by_block_number, stream_supply_deltas_from_last};
use super::snapshot::SUPPLY_SNAPSHOT_15082718;
use crate::execution_chain::node::BlockNumber;
use crate::performance::TimedExt;
use crate::{db, log};

use super::SupplyDelta;

const GENESIS_PARENT_HASH: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

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

    sqlx::query!(
        "
        SELECT EXISTS (
            SELECT 1
            FROM execution_supply_deltas
            WHERE block_hash = $1
        ) AS \"exists!\"
        ",
        block_hash
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .exists
}

async fn get_is_block_number_known<'a>(
    executor: impl PgExecutor<'a>,
    block_number: &BlockNumber,
) -> bool {
    sqlx::query!(
        "
        SELECT EXISTS (
            SELECT 1
            FROM execution_supply_deltas
            WHERE block_number = $1
        ) AS \"exists!\"
        ",
        *block_number
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .exists
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
    .bind(supply_delta.block_number)
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

async fn store_execution_supply(
    executor: impl PgExecutor<'_>,
    supply_delta: &SupplyDelta,
    balances: &i128,
) -> sqlx::Result<PgQueryResult> {
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
    .bind(supply_delta.block_number)
    .bind(balances.to_string())
    .execute(executor)
    .await
}

/// Gets the balances for a given execution layer hash. Only call for known balances.
async fn get_balances_at_hash<'a>(executor: impl PgExecutor<'a>, block_hash: &str) -> i128 {
    // Instead of the genesis parent_hash being absent, it is set to GENESIS_PARENT_HASH.
    // We'd like to have all supply deltas making only an exception for the genesis hash, but we
    // don't have all supply deltas and so have to contend with a snapshot eth supply as a
    // "jumping off point".
    if block_hash == GENESIS_PARENT_HASH {
        unimplemented!("missing hardcoded genisis parent, that is, pre-genesis balances sum");
    }

    if block_hash == SUPPLY_SNAPSHOT_15082718.block_hash {
        return SUPPLY_SNAPSHOT_15082718.balances_sum;
    }

    // Missing this special hash used in testing is okay.
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

pub async fn add_delta(connection: &mut PgConnection, supply_delta: &SupplyDelta) {
    let mut transaction = connection.begin().await.unwrap();

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

    store_execution_supply(&mut transaction, supply_delta, &balances)
        .await
        .unwrap();

    transaction.commit().await.unwrap();
}

async fn drop_supply_deltas_from<'a>(executor: &mut PgConnection, gte_block_number: &BlockNumber) {
    let mut transaction = executor.begin().await.unwrap();

    sqlx::query(
        "
        DELETE FROM execution_supply
        WHERE block_number >= $1
        ",
    )
    .bind(*gte_block_number)
    .execute(&mut *transaction)
    .await
    .unwrap();

    sqlx::query(
        "
        DELETE FROM execution_supply_deltas
        WHERE block_number >= $1
        ",
    )
    .bind(*gte_block_number)
    .execute(&mut *transaction)
    .await
    .unwrap();

    transaction.commit().await.unwrap();
}

pub async fn get_last_synced_supply_delta_number(
    executor: &mut PgConnection,
) -> Option<BlockNumber> {
    sqlx::query!(
        "
        SELECT
            MAX(block_number)
        FROM
            execution_supply_deltas
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .max
}

enum NextStep {
    HandleGap,
    HandleHeadFork,
    AddToExisting,
}

async fn get_next_step(connection: &mut PgConnection, supply_delta: &SupplyDelta) -> NextStep {
    use sqlx::Acquire;

    // If the deltas in our DB have been forked, a new supply delta will contain a parent_hash we
    // don't have. When this happens, we roll our DB back one step, and try to sync the n - 1 block
    // again. Here the same might happen, until we find ourselves syncing a block that we do have a
    // parent hash for.
    let is_parent_known = get_is_hash_known(
        connection.acquire().await.unwrap(),
        &supply_delta.parent_hash,
    )
    .await;

    if !is_parent_known {
        return NextStep::HandleGap;
    }

    let is_fork_block = get_is_block_number_known(
        connection.acquire().await.unwrap(),
        &supply_delta.block_number,
    )
    .await;

    if is_fork_block {
        return NextStep::HandleHeadFork;
    }

    NextStep::AddToExisting
}

async fn sync_delta(
    connection: &mut PgConnection,
    deltas_queue: DeltasQueue,
    delta_to_sync: DeltaToSync,
) {
    let supply_delta = match delta_to_sync {
        DeltaToSync::Fetched(supply_delta) => supply_delta,
        DeltaToSync::Refetch(supply_delta_number) => {
            get_supply_delta_by_block_number(supply_delta_number)
                .timed("get supply delta by block number")
                .await
                .unwrap()
        }
    };

    match get_next_step(connection, &supply_delta).await {
        // We may have missed a block, the chain may have forked n blocks back, either way, we'll
        // have to rollback our deltas table one by one, until we hit a delta with a known parent,
        // at which point we'll be able to make progress again.
        NextStep::HandleGap => {
            tracing::warn!(
                "parent of delta {} is missing, dropping min(our last delta, new delta) and queuing all blocks gte the received delta",
                supply_delta.block_number
            );

            let last_supply_delta_number = get_last_synced_supply_delta_number(connection)
                .await
                .expect("at least one supply delta to be synced before rolling back");

            // Supply delta block number may be lower than our last synced. Roll back to the
            // lowest of the two.
            let lowest_block_number = last_supply_delta_number.min(supply_delta.block_number);
            tracing::debug!("dropping supply deltas gte {lowest_block_number}");
            drop_supply_deltas_from(connection, &lowest_block_number).await;
            for block_number in (lowest_block_number..=supply_delta.block_number).rev() {
                tracing::debug!("queuing {block_number} for sync after dropping");
                deltas_queue
                    .lock()
                    .unwrap()
                    .push_front(DeltaToSync::Refetch(block_number));
            }
        }
        NextStep::HandleHeadFork => {
            tracing::info!(
                "delta {} creates a fork, rolling back our last block",
                &supply_delta.block_number
            );

            drop_supply_deltas_from(connection, &supply_delta.block_number).await;

            deltas_queue
                .lock()
                .unwrap()
                .push_front(DeltaToSync::Fetched(supply_delta));
        }
        NextStep::AddToExisting => {
            // Progress as usual.
            tracing::debug!("storing supply delta {}", supply_delta.block_number);
            add_delta(connection, &supply_delta)
                .timed("add delta")
                .await;
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum DeltaToSync {
    Fetched(SupplyDelta),
    Refetch(BlockNumber),
}

type DeltasQueue = Arc<Mutex<VecDeque<DeltaToSync>>>;

pub async fn sync_deltas() {
    log::init_with_env();

    tracing::info!("syncing supply deltas");

    let mut connection =
        PgConnection::connect(&db::get_db_url_with_name("sync-execution-supply-deltas"))
            .await
            .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let mut supply_delta_stream = stream_supply_deltas_from_last(&mut connection).await;

    let deltas_queue: DeltasQueue = Arc::new(Mutex::new(VecDeque::new()));

    while let Some(supply_delta) = supply_delta_stream.next().await {
        deltas_queue
            .lock()
            .unwrap()
            .push_back(DeltaToSync::Fetched(supply_delta));

        // Work through the queue until it's empty.
        loop {
            let next_delta = { deltas_queue.lock().unwrap().pop_front() };
            match next_delta {
                None => {
                    // Continue syncing deltas from the stream.
                    break;
                }
                Some(delta_to_sync) => {
                    // Because we may encounter rollbacks, this step may add more deltas to sync to
                    // the front of the queue.
                    sync_delta(&mut connection, deltas_queue.clone(), delta_to_sync).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_is_hash_not_known() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_hash_known = get_is_hash_known(&mut transaction, "0xnot_there").await;

        assert!(!is_hash_known);
    }

    #[tokio::test]
    async fn test_get_is_hash_known() {
        let mut connection = db::tests::get_test_db_connection().await;
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

        assert!(is_hash_known);
    }

    #[tokio::test]
    async fn test_get_is_block_number_not_known() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let is_block_number_known = get_is_block_number_known(&mut transaction, &0).await;

        assert!(!is_block_number_known);
    }

    #[tokio::test]
    async fn test_get_is_block_number_known() {
        let mut connection = db::tests::get_test_db_connection().await;
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

        assert!(is_block_number_known);
    }

    #[tokio::test]
    async fn test_get_balances_at_hash() {
        let mut connection = db::tests::get_test_db_connection().await;
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
    async fn test_drop_supply_deltas() {
        let mut connection = db::tests::get_test_db_connection().await;
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

    #[ignore]
    #[tokio::test]
    async fn test_reverted_fork() {
        log::init_with_env();
        // We test:
        // A -> B ---> C
        //   \--> B'
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

        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let deltas_queue: DeltasQueue = Arc::new(Mutex::new(VecDeque::new()));

        // Sync a delta.
        sync_delta(
            &mut transaction,
            deltas_queue.clone(),
            DeltaToSync::Fetched(delta_b),
        )
        .await;
        // Fork that delta.
        sync_delta(
            &mut transaction,
            deltas_queue.clone(),
            DeltaToSync::Fetched(delta_b_prime.clone()),
        )
        .await;

        let expected_1 = VecDeque::from(vec![DeltaToSync::Fetched(delta_b_prime.clone())]);
        assert_eq!(deltas_queue.lock().unwrap().clone(), expected_1);

        // B 15_082_719, has been dropped, because syncing B' 15_082_719 talks to a real node, it
        // would sync the real 15_082_719, but we want C 15_082_720 to be forking B' 15_082_719, only
        // accepting building on B. We force insert B' as 15_082_719.
        // real 15_082_719.
        deltas_queue.lock().unwrap().pop_front();
        add_delta(&mut transaction, &delta_b_prime).await;

        // Now try to process C which depends on B which we've dropped.
        sync_delta(
            &mut transaction,
            deltas_queue.clone(),
            DeltaToSync::Fetched(delta_c),
        )
        .await;

        let expected = VecDeque::from(vec![
            DeltaToSync::Refetch(15_082_719),
            DeltaToSync::Refetch(15_082_720),
        ]);
        let actual = deltas_queue.lock().unwrap().clone();
        assert_eq!(actual, expected);
    }
}
