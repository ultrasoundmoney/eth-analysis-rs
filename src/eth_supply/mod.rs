mod gaps;
mod over_time;
mod parts;

pub use gaps::fill_gaps as fill_eth_supply_gaps;

pub use over_time::get_supply_over_time;
pub use over_time::update_cache as update_supply_over_time_cache;
pub use over_time::SupplyOverTime;

pub use parts::get_supply_parts;
pub use parts::update_cache as update_supply_parts_cache;
pub use parts::SupplyParts;

use std::cmp::Ordering;

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgQueryResult;
use sqlx::{Acquire, PgConnection, PgExecutor};
use tracing::{debug, error, warn};

use crate::beacon_chain::{beacon_time, Slot, FIRST_POST_MERGE_SLOT};
use crate::eth_units::{EthF64, GweiNewtype, Wei};
use crate::execution_chain::{self, BlockNumber};

pub async fn rollback_supply_from_slot(
    executor: &mut PgConnection,
    greater_than_or_equal: &Slot,
) -> sqlx::Result<PgQueryResult> {
    sqlx::query!(
        "
            DELETE FROM
                eth_supply
            WHERE
                deposits_slot >= $1
                OR balances_slot >= $1
        ",
        *greater_than_or_equal
    )
    .execute(executor)
    .await
}

pub async fn rollback_supply_slot(
    executor: &mut PgConnection,
    greater_than_or_equal: &Slot,
) -> sqlx::Result<PgQueryResult> {
    sqlx::query!(
        "
            DELETE FROM
                eth_supply
            WHERE
                deposits_slot = $1
                OR balances_slot = $1
        ",
        *greater_than_or_equal
    )
    .execute(executor)
    .await
}

pub async fn store(
    executor: impl PgExecutor<'_>,
    slot: &Slot,
    block_number: &BlockNumber,
    execution_balances_sum: &Wei,
    beacon_balances_sum: &GweiNewtype,
    beacon_deposits_sum: &GweiNewtype,
) -> sqlx::Result<PgQueryResult> {
    let timestamp = beacon_time::get_date_time_from_slot(&slot);

    debug!(%timestamp, slot, block_number, execution_balances_sum, %beacon_deposits_sum, %beacon_balances_sum, "storing eth supply");

    let supply =
        execution_balances_sum + beacon_balances_sum.into_wei() - beacon_deposits_sum.into_wei();

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
        *slot,
        *slot,
        supply.to_string() as String
    )
    .execute(executor)
    .await
}

#[derive(Debug, PartialEq)]
pub struct EthSupply {
    balances_slot: Slot,
    block_number: BlockNumber,
    deposits_slot: Slot,
    pub supply: EthF64,
    timestamp: DateTime<Utc>,
}

pub async fn get_supply_exists_by_slot(
    executor: impl PgExecutor<'_>,
    slot: &Slot,
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
        *slot
    )
    .fetch_optional(executor)
    .await
    .map(|row| row.is_some())
}

async fn get_last_stored_supply_slot(executor: impl PgExecutor<'_>) -> sqlx::Result<Option<Slot>> {
    sqlx::query!(
        "
            SELECT
                MAX(balances_slot)
            FROM
                eth_supply
        ",
    )
    .fetch_one(executor)
    .await
    .map(|row| row.max)
}

pub async fn store_supply_for_slot(executor: &mut PgConnection, slot: &Slot) -> Result<()> {
    let eth_supply_parts = get_supply_parts(executor, slot).await?;

    match eth_supply_parts {
        None => {
            debug!(slot, "supply parts unavailable skipping");
        }
        Some(supply_parts) => {
            store(
                executor.acquire().await?,
                &slot,
                &supply_parts.block_number(),
                &supply_parts.execution_balances_sum_next,
                &supply_parts.beacon_balances_sum_next,
                &supply_parts.beacon_deposits_sum_next,
            )
            .await?;
        }
    };

    Ok(())
}

/// Stores an eth supply for a given slot.
pub async fn sync_eth_supply(executor: &mut PgConnection, slot: &Slot) -> Result<()> {
    let last_stored_execution_balances_slot =
        execution_chain::get_last_stored_balances_slot(executor.acquire().await?).await?;

    let slots_to_store = match last_stored_execution_balances_slot {
        None => {
            warn!("no execution balances have ever been stored, skipping store eth supply");
            vec![]
        }
        Some(last_stored_execution_supply_slot) => {
            let last_stored_supply_slot =
                get_last_stored_supply_slot(executor.acquire().await?).await?;

            // Don't exceed currently syncing slot. We wouldn't have the balances.
            // TODO: make sync limit more obvious.
            let last = last_stored_execution_supply_slot.max(*slot);

            match last_stored_supply_slot {
                None => {
                    debug!(
                        FIRST_POST_MERGE_SLOT,
                        "eth supply has never been stored, starting from FIRST_POST_MERGE_SLOT"
                    );
                    let range = FIRST_POST_MERGE_SLOT..=last;
                    range.collect()
                }
                Some(last_stored_supply_slot) => match last_stored_supply_slot.cmp(&last) {
                    Ordering::Less => {
                        debug!("execution balances have updated, storing eth supply for new slots");
                        let first = last_stored_supply_slot + 1;
                        let range = first..=last_stored_execution_supply_slot;
                        range.collect()
                    }
                    Ordering::Equal => {
                        debug!("no new execution balances stored since last slot sync, skipping store eth supply");
                        vec![]
                    }
                    Ordering::Greater => {
                        error!("eth supply table is ahead of execution supply, did we miss a rollback? skipping store eth supply");
                        vec![]
                    }
                },
            }
        }
    };

    for slot in slots_to_store {
        debug!(
            slot,
            "storing eth supply for newly available execution balance slot"
        );
        store_supply_for_slot(executor, &slot).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use sqlx::Acquire;

    use crate::{
        beacon_chain::{
            self, BeaconBalancesSum, BeaconBlockBuilder, BeaconDepositsSum,
            BeaconHeaderSignedEnvelopeBuilder,
        },
        db,
        eth_units::GweiNewtype,
        execution_chain::{add_delta, ExecutionBalancesSum, ExecutionNodeBlock, SupplyDelta},
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
        }
    }

    #[tokio::test]
    async fn get_supply_parts_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let mut block_store = execution_chain::BlockStore::new(&mut transaction);

        let test_id = "get_supply_parts";
        let test_header = BeaconHeaderSignedEnvelopeBuilder::new(test_id).build();
        let test_block = Into::<BeaconBlockBuilder>::into(&test_header)
            .block_hash(&format!("0x{test_id}_block_hash"))
            .build();

        let execution_test_block = make_test_block();

        block_store.store_block(&execution_test_block, 0.0).await;

        beacon_chain::store_state(
            &mut transaction,
            &test_header.state_root(),
            &test_header.slot(),
        )
        .await
        .unwrap();

        beacon_chain::store_block(
            &mut transaction,
            &test_block,
            &GweiNewtype(0),
            &GweiNewtype(5),
            &test_header,
        )
        .await
        .unwrap();

        beacon_chain::store_validators_balance(
            &mut transaction,
            &test_header.state_root(),
            &test_header.slot(),
            &GweiNewtype(20),
        )
        .await
        .unwrap();

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

        add_delta(&mut transaction, &supply_delta_test).await;

        let execution_balances_sum = execution_chain::get_execution_balances_by_hash(
            &mut transaction,
            &test_block.block_hash().unwrap(),
        )
        .await
        .unwrap();
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: GweiNewtype(20),
            slot: 0,
        };
        let beacon_deposits_sum = beacon_chain::get_deposits_sum_by_state_root(
            &mut transaction,
            &test_header.state_root(),
        )
        .await
        .unwrap();

        let eth_supply_parts_test = SupplyParts {
            beacon_balances_sum: beacon_balances_sum.clone(),
            beacon_balances_sum_next: beacon_balances_sum.balances_sum,
            beacon_deposits_sum_next: beacon_deposits_sum,
            beacon_deposits_sum: BeaconDepositsSum {
                deposits_sum: beacon_deposits_sum,
                slot: test_header.slot(),
            },
            execution_balances_sum_next: execution_balances_sum.balances_sum,
            execution_balances_sum: ExecutionBalancesSum {
                block_number: execution_balances_sum.block_number,
                balances_sum: execution_balances_sum.balances_sum,
            },
        };

        let eth_supply_parts = get_supply_parts(&mut transaction, &test_header.slot())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(eth_supply_parts, eth_supply_parts_test);
    }

    #[tokio::test]
    async fn get_eth_supply_not_exists_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let eth_supply_exists = get_supply_exists_by_slot(&mut transaction, &0).await?;

        assert!(!eth_supply_exists);

        Ok(())
    }

    #[tokio::test]
    async fn get_eth_supply_exists_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let mut block_store = execution_chain::BlockStore::new(&mut transaction);

        let test_block = make_test_block();
        let state_root = "0xstate_root";
        let slot = 0;

        block_store.store_block(&test_block, 0.0).await;

        beacon_chain::store_state(&mut transaction, state_root, &slot)
            .await
            .unwrap();

        let execution_balances_sum = ExecutionBalancesSum {
            block_number: 0,
            balances_sum: GweiNewtype(10).into_wei(),
        };
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: GweiNewtype(20),
            slot: 0,
        };
        let beacon_deposits_sum = BeaconDepositsSum {
            slot: 0,
            deposits_sum: GweiNewtype(5),
        };

        let eth_supply_parts = SupplyParts {
            beacon_balances_sum,
            beacon_balances_sum_next: beacon_balances_sum.balances_sum,
            beacon_deposits_sum,
            beacon_deposits_sum_next: beacon_deposits_sum.deposits_sum,
            execution_balances_sum,
            execution_balances_sum_next: execution_balances_sum.balances_sum,
        };

        store(
            &mut transaction,
            &slot,
            &0,
            &eth_supply_parts.execution_balances_sum.balances_sum,
            &eth_supply_parts.beacon_balances_sum_next,
            &eth_supply_parts.beacon_deposits_sum_next,
        )
        .await
        .unwrap();

        let eth_supply_exists = get_supply_exists_by_slot(&mut transaction, &0)
            .await
            .unwrap();

        assert!(eth_supply_exists);
    }

    #[tokio::test]
    async fn get_last_stored_supply_slot_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let mut block_store = execution_chain::BlockStore::new(&mut transaction);

        let test_id = "get_last_stored_supply_slot";
        let test_block = make_test_block();
        let state_root = format!("0x{test_id}_state_root");
        let slot = 0;

        block_store.store_block(&test_block, 0.0).await;

        beacon_chain::store_state(&mut transaction, &state_root, &slot)
            .await
            .unwrap();

        let execution_balances_sum = ExecutionBalancesSum {
            block_number: 0,
            balances_sum: GweiNewtype(10).into_wei(),
        };
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: GweiNewtype(20),
            slot: 0,
        };
        let beacon_deposits_sum = BeaconDepositsSum {
            slot: 0,
            deposits_sum: GweiNewtype(5),
        };

        let eth_supply_parts = SupplyParts {
            beacon_balances_sum,
            beacon_balances_sum_next: beacon_balances_sum.balances_sum,
            beacon_deposits_sum,
            beacon_deposits_sum_next: beacon_deposits_sum.deposits_sum,
            execution_balances_sum,
            execution_balances_sum_next: execution_balances_sum.balances_sum,
        };

        store(
            &mut transaction,
            &slot,
            &0,
            &eth_supply_parts.execution_balances_sum.balances_sum,
            &eth_supply_parts.beacon_balances_sum_next,
            &eth_supply_parts.beacon_deposits_sum_next,
        )
        .await
        .unwrap();

        let last_stored_slot = get_last_stored_supply_slot(&mut transaction)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(0, last_stored_slot);
    }
}
