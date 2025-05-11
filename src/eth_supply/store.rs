use anyhow::Result;
use sqlx::postgres::PgQueryResult;
use sqlx::{Acquire, PgConnection, PgExecutor};
use tracing::{debug, warn};

use crate::beacon_chain::Slot;
use crate::units::{GweiNewtype, WeiNewtype};

use crate::execution_chain::BlockNumber;

use super::SupplyPartsStore;

pub async fn rollback_supply_from_slot(
    executor: &mut PgConnection,
    greater_than_or_equal: Slot,
) -> sqlx::Result<PgQueryResult> {
    sqlx::query!(
        "
        DELETE FROM
            eth_supply
        WHERE
            deposits_slot >= $1
            OR balances_slot >= $1
        ",
        greater_than_or_equal.0
    )
    .execute(executor)
    .await
}

pub async fn rollback_supply_slot(
    executor: &mut PgConnection,
    slot: Slot,
) -> sqlx::Result<PgQueryResult> {
    sqlx::query!(
        "
        DELETE FROM
            eth_supply
        WHERE
            deposits_slot = $1
            OR balances_slot = $1
        ",
        slot.0
    )
    .execute(executor)
    .await
}

pub async fn store(
    executor: impl PgExecutor<'_>,
    slot: Slot,
    block_number: &BlockNumber,
    execution_balances_sum: &WeiNewtype,
    beacon_balances_sum: &GweiNewtype,
    beacon_deposits_sum: &GweiNewtype,
) -> sqlx::Result<PgQueryResult> {
    let timestamp = slot.date_time();

    debug!(%timestamp, %slot, block_number, %execution_balances_sum, %beacon_deposits_sum, %beacon_balances_sum, "storing eth supply");

    let supply = *execution_balances_sum + beacon_balances_sum.into() - beacon_deposits_sum.into();

    sqlx::query!(
        "
        INSERT INTO eth_supply (
            timestamp,
            block_number,
            deposits_slot,
            balances_slot,
            supply
        )
        VALUES ($1, $2, $3, $4, $5::NUMERIC)
        ",
        timestamp,
        *block_number,
        slot.0,
        slot.0,
        Into::<String>::into(supply) as String,
    )
    .execute(executor)
    .await
}

pub async fn get_supply_exists_by_slot(
    executor: impl PgExecutor<'_>,
    slot: Slot,
) -> sqlx::Result<bool> {
    sqlx::query!(
        "
        SELECT
            balances_slot
        FROM
            eth_supply
        WHERE
            balances_slot = $1
        ",
        slot.0
    )
    .fetch_optional(executor)
    .await
    .map(|row| row.is_some())
}

pub async fn get_last_stored_supply_slot(executor: impl PgExecutor<'_>) -> Result<Option<Slot>> {
    let slot = sqlx::query!(
        "
        SELECT
            MAX(balances_slot)
        FROM
            eth_supply
        ",
    )
    .fetch_one(executor)
    .await?
    .max;

    Ok(slot.map(Slot))
}

pub async fn store_supply_for_slot(executor_acq: &mut PgConnection, slot: Slot) -> Result<()> {
    let supply_parts_result =
        SupplyPartsStore::get_with_transaction(executor_acq.acquire().await.unwrap(), slot).await?;

    match supply_parts_result {
        Some(supply_parts) => {
            match store(
                executor_acq.acquire().await.unwrap(),
                slot,
                &supply_parts.block_number(),
                &supply_parts.execution_balances_sum,
                &supply_parts.beacon_balances_sum,
                &supply_parts.beacon_deposits_sum,
            )
            .await
            {
                Ok(_) => debug!(%slot, "successfully stored eth supply"),
                Err(e) => warn!(%slot, "failed to store eth supply after computing parts: {}", e),
            }
        }
        None => {
            debug!(%slot, "supply parts not computed (e.g., no block/state for slot), skipping store");
        }
    };

    Ok(())
}

pub async fn get_last_stored_balances_slot(executor: impl PgExecutor<'_>) -> Option<Slot> {
    sqlx::query!(
        "
        SELECT
            beacon_states.slot
        FROM
            beacon_states
        JOIN beacon_blocks ON
            beacon_states.state_root = beacon_blocks.state_root
        JOIN execution_supply ON
            beacon_blocks.block_hash = execution_supply.block_hash
        WHERE
            execution_supply.block_hash IS NOT NULL
        ORDER BY beacon_states.slot DESC
        LIMIT 1
        ",
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| Slot(row.slot))
}

pub async fn last_eth_supply(executor: impl PgExecutor<'_>) -> WeiNewtype {
    sqlx::query!(
        r#"
        SELECT
            supply::TEXT AS "supply!"
        FROM
            eth_supply
        ORDER BY
            timestamp DESC
        LIMIT 1
        "#,
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .supply
    .parse::<WeiNewtype>()
    .unwrap()
}

#[cfg(test)]
mod tests {
    use chrono::{SubsecRound, Utc};
    use sqlx::Acquire;

    use crate::{
        beacon_chain::{self, BeaconBlockBuilder, BeaconHeaderSignedEnvelopeBuilder, Slot},
        db,
        eth_supply::SupplyParts,
        execution_chain::{self, add_delta, ExecutionNodeBlock, SupplyDelta},
        units::{GweiNewtype, WeiNewtype},
    };

    use super::*;

    // Replace with shared testing helper that helps easily build the right mock block.
    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 0,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            total_difficulty: 10,
            transactions: vec![],
            blob_gas_used: None,
            excess_blob_gas: None,
        }
    }

    #[tokio::test]
    async fn get_supply_parts_test() {
        let test_db = db::tests::TestDb::new().await;
        let supply_parts_store = SupplyPartsStore::new(&test_db.pool);

        let test_id = "get_supply_parts";
        let slot = Slot(0);
        let test_header = BeaconHeaderSignedEnvelopeBuilder::new(test_id)
            .slot(slot)
            .build();
        let test_block = Into::<BeaconBlockBuilder>::into(&test_header)
            .block_hash(&format!("0x{test_id}_block_hash"))
            .build();

        let execution_test_block = make_test_block();

        execution_chain::store_block(&test_db.pool, &execution_test_block, 0.0).await;

        beacon_chain::store_state(&test_db.pool, &test_header.state_root(), slot).await;

        beacon_chain::store_block(
            &test_db.pool,
            &test_block,
            &GweiNewtype(0),
            &GweiNewtype(5),
            &GweiNewtype(0),
            &GweiNewtype(5),
            &test_header,
        )
        .await;

        beacon_chain::store_validators_balance(
            &test_db.pool,
            &test_header.state_root(),
            slot,
            &GweiNewtype(20),
        )
        .await;

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: test_block.block_hash().unwrap().to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };

        add_delta(
            &mut test_db.pool.acquire().await.unwrap(),
            &supply_delta_test,
        )
        .await;

        let execution_balances = execution_chain::get_execution_balances_by_hash(
            &test_db.pool,
            test_block.block_hash().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        let beacon_deposits_sum =
            beacon_chain::get_deposits_sum_by_state_root(&test_db.pool, &test_block.state_root)
                .await
                .unwrap()
                .unwrap();

        let expected_supply_parts = SupplyParts::new(
            slot,
            &execution_balances.block_number,
            execution_balances.balances_sum,
            GweiNewtype(20),
            beacon_deposits_sum,
        );

        let result_option_supply_parts = supply_parts_store.get(slot).await.unwrap();

        assert_eq!(result_option_supply_parts, Some(expected_supply_parts));
    }

    #[tokio::test]
    async fn get_eth_supply_not_exists_test() -> Result<()> {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let eth_supply_exists = get_supply_exists_by_slot(&mut *transaction, Slot(0)).await?;

        assert!(!eth_supply_exists);

        Ok(())
    }

    #[tokio::test]
    async fn get_eth_supply_exists_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_block = make_test_block();
        let state_root = "0xstate_root";
        let slot = Slot(0);

        execution_chain::store_block(&mut *transaction, &test_block, 0.0).await;

        beacon_chain::store_state(&mut *transaction, state_root, slot).await;

        let supply_parts = SupplyParts::new(
            slot,
            &0,
            Into::<WeiNewtype>::into(GweiNewtype(10)),
            GweiNewtype(20),
            GweiNewtype(5),
        );

        store(
            &mut *transaction,
            slot,
            &0,
            &supply_parts.execution_balances_sum,
            &supply_parts.beacon_balances_sum,
            &supply_parts.beacon_deposits_sum,
        )
        .await
        .unwrap();

        let eth_supply_exists = get_supply_exists_by_slot(&mut *transaction, slot)
            .await
            .unwrap();

        assert!(eth_supply_exists);
    }

    #[tokio::test]
    async fn get_last_stored_supply_slot_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_last_stored_supply_slot";
        let test_block = make_test_block();
        let state_root = format!("0x{test_id}_state_root");
        let slot = Slot(0);

        execution_chain::store_block(&mut *transaction, &test_block, 0.0).await;

        beacon_chain::store_state(&mut *transaction, &state_root, slot).await;

        let supply_parts = SupplyParts::new(
            slot,
            &0,
            GweiNewtype(10).into(),
            GweiNewtype(20),
            GweiNewtype(5),
        );

        store(
            &mut *transaction,
            slot,
            &0,
            &supply_parts.execution_balances_sum,
            &supply_parts.beacon_balances_sum,
            &supply_parts.beacon_deposits_sum,
        )
        .await
        .unwrap();

        let last_stored_slot = get_last_stored_supply_slot(&mut *transaction)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(Slot(0), last_stored_slot);
    }
}
