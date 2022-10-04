use anyhow::{Ok, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::{PgQueryResult, PgRow};
use sqlx::{Acquire, PgConnection, Row};
use tracing::debug;

use crate::beacon_chain::{self, beacon_time, BeaconBalancesSum, BeaconDepositsSum, Slot};
use crate::caching::{self, CacheKey};
use crate::eth_units::{EthF64, Wei};
use crate::execution_chain::ExecutionBalancesSum;
use crate::execution_chain::{self, BlockNumber};
use crate::key_value_store;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct EthSupplyParts {
    beacon_balances_sum: BeaconBalancesSum,
    beacon_deposits_sum: BeaconDepositsSum,
    execution_balances_sum: ExecutionBalancesSum,
}

fn get_supply(eth_supply_parts: &EthSupplyParts) -> Wei {
    eth_supply_parts.execution_balances_sum.balances_sum
        + eth_supply_parts.beacon_balances_sum.balances_sum.into_wei()
        - eth_supply_parts.beacon_deposits_sum.deposits_sum.into_wei()
}

pub async fn rollback_supply_by_block(
    executor: &mut PgConnection,
    greater_than_or_equal: &BlockNumber,
) -> sqlx::Result<PgQueryResult> {
    sqlx::query(
        "
            DELETE FROM
                eth_supply
            WHERE
                block_number >= $1
        ",
    )
    .bind(*greater_than_or_equal as i32)
    .execute(executor)
    .await
}

pub async fn rollback_supply_by_slot(
    executor: &mut PgConnection,
    greater_than_or_equal: &Slot,
) -> sqlx::Result<PgQueryResult> {
    sqlx::query(
        "
            DELETE FROM
                eth_supply
            WHERE
                deposits_slot >= $1
                OR balances_slot >= $1
        ",
    )
    .bind(*greater_than_or_equal as i32)
    .execute(executor)
    .await
}

async fn store(
    executor: &mut PgConnection,
    eth_supply_parts: &EthSupplyParts,
) -> sqlx::Result<PgQueryResult> {
    let timestamp =
        beacon_time::get_date_time_from_slot(&eth_supply_parts.beacon_balances_sum.slot);
    let block_number = eth_supply_parts.execution_balances_sum.block_number;
    let deposits_slot = eth_supply_parts.beacon_deposits_sum.slot;
    let balances_slot = eth_supply_parts.beacon_balances_sum.slot;

    debug!(
        timestamp = timestamp.to_string(),
        block_number, deposits_slot, balances_slot, "storing new eth supply"
    );

    sqlx::query(
        "
            INSERT INTO
                eth_supply (timestamp, block_number, deposits_slot, balances_slot, supply)
            VALUES
                ($1, $2, $3, $4, $5::NUMERIC)
        ",
    )
    .bind(timestamp)
    .bind(block_number as i32)
    .bind(deposits_slot as i32)
    .bind(balances_slot as i32)
    .bind(get_supply(&eth_supply_parts).to_string())
    .execute(executor)
    .await
}

#[derive(Clone, Serialize)]
struct SupplyAtTime {
    timestamp: DateTime<Utc>,
    supply: EthF64,
}

#[derive(Serialize)]
struct SupplySinceMerge {
    balances_slot: Slot,
    block_number: BlockNumber,
    deposits_slot: Slot,
    supply_by_hour: Vec<SupplyAtTime>,
    timestamp: DateTime<Utc>,
}

async fn get_supply_since_merge_by_hour(
    executor: &mut PgConnection,
) -> sqlx::Result<Vec<SupplyAtTime>> {
    sqlx::query(
        "
            SELECT
                DISTINCT ON (DATE_TRUNC('hour', timestamp))
                DATE_TRUNC('hour', timestamp) AS hour_timestamp,
                supply::FLOAT8 / 1e18 AS supply
            FROM
                eth_supply 
            WHERE
                timestamp >= '2022-09-15T05:00:00'::TIMESTAMPTZ
            ORDER BY
                DATE_TRUNC('hour', timestamp), timestamp
        ",
    )
    .map(|row: PgRow| {
        let timestamp = row.get::<DateTime<Utc>, _>("hour_timestamp");
        let supply = (row.get::<f64, _>("supply") * 100.0).round() / 100.0;
        SupplyAtTime { timestamp, supply }
    })
    .fetch_all(executor)
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

pub async fn get_current_supply(executor: &mut PgConnection) -> sqlx::Result<EthSupply> {
    sqlx::query(
        "
            SELECT
                balances_slot,
                deposits_slot,
                block_number,
                supply::FLOAT8 / 1e18 AS supply,
                timestamp
            FROM
                eth_supply
            ORDER BY timestamp DESC
            LIMIT 1
        ",
    )
    .map(|row: PgRow| {
        let timestamp = row.get::<DateTime<Utc>, _>("timestamp");
        let supply = row.get::<f64, _>("supply");
        let balances_slot = row.get::<i32, _>("balances_slot") as Slot;
        let block_number = row.get::<i32, _>("block_number") as BlockNumber;
        let deposits_slot = row.get::<i32, _>("deposits_slot") as Slot;
        EthSupply {
            timestamp,
            supply,
            balances_slot,
            block_number,
            deposits_slot,
        }
    })
    .fetch_one(executor)
    .await
}

async fn update_supply_since_merge(
    executor: &mut PgConnection,
    eth_supply_parts: &EthSupplyParts,
) -> Result<()> {
    store(executor, eth_supply_parts).await?;

    let mut supply_by_hour = get_supply_since_merge_by_hour(executor).await?;

    let most_recent_supply = get_current_supply(executor.acquire().await?).await?;

    supply_by_hour.push(SupplyAtTime {
        timestamp: most_recent_supply.timestamp,
        supply: most_recent_supply.supply,
    });

    let supply_since_merge = SupplySinceMerge {
        balances_slot: most_recent_supply.balances_slot,
        block_number: most_recent_supply.block_number,
        deposits_slot: most_recent_supply.deposits_slot,
        supply_by_hour: supply_by_hour.clone(),
        timestamp: most_recent_supply.timestamp,
    };

    key_value_store::set_caching_value(executor, &CacheKey::SupplySinceMerge, supply_since_merge)
        .await?;

    caching::publish_cache_update(executor, CacheKey::SupplySinceMerge).await;

    Ok(())
}

async fn update_supply_parts(
    executor: &mut PgConnection,
    eth_supply_parts: &EthSupplyParts,
) -> Result<()> {
    debug!("updating supply parts");

    key_value_store::set_value_str(
        executor.acquire().await.unwrap(),
        &CacheKey::EthSupplyParts.to_db_key(),
        // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
        // as string however.
        &serde_json::to_string(&eth_supply_parts).unwrap(),
    )
    .await;

    caching::publish_cache_update(executor, CacheKey::EthSupplyParts).await;

    Ok(())
}

async fn get_supply_parts(
    executor: &mut PgConnection,
    beacon_balances_sum: BeaconBalancesSum,
) -> Result<EthSupplyParts> {
    // We have two options here, we take the most recent, the balances slot.
    let point_in_time = beacon_time::get_date_time_from_slot(&beacon_balances_sum.slot);

    let execution_balances_sum =
        execution_chain::get_closest_balances_sum(executor, point_in_time).await?;

    // We get the most recent deposit sum, not every slot has to have a block for which we can
    // determine the deposit sum.
    let beacon_deposits_sum = beacon_chain::get_deposits_sum(executor).await;

    let eth_supply_parts = EthSupplyParts {
        execution_balances_sum,
        beacon_balances_sum,
        beacon_deposits_sum,
    };

    Ok(eth_supply_parts)
}

pub async fn update(
    executor: &mut PgConnection,
    beacon_balances_sum: BeaconBalancesSum,
) -> Result<()> {
    debug!(slot = beacon_balances_sum.slot, "updating eth supply");

    let eth_supply_parts = get_supply_parts(executor, beacon_balances_sum).await?;

    update_supply_parts(executor, &eth_supply_parts).await?;

    update_supply_since_merge(executor, &eth_supply_parts).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;

    use crate::{
        beacon_chain::{
            BeaconHeader, BeaconHeaderEnvelope, BeaconHeaderSignedEnvelope, GENESIS_PARENT_ROOT,
        },
        db_testing,
        eth_units::{GweiNewtype, WEI_PER_GWEI},
        execution_chain::{add_delta, ExecutionNodeBlock, SupplyDelta},
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

    #[test]
    fn get_supply_test() {
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

        let eth_supply_parts = EthSupplyParts {
            beacon_balances_sum,
            beacon_deposits_sum,
            execution_balances_sum,
        };

        let supply = get_supply(&eth_supply_parts);

        assert_eq!(supply, 25_i128 * WEI_PER_GWEI as i128);
    }

    #[tokio::test]
    async fn get_supply_parts_test() -> Result<()> {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let mut block_store = execution_chain::BlockStore::new(&mut transaction);

        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;

        beacon_chain::store_state(&mut transaction, "0xstate_root", &0).await?;

        beacon_chain::store_block(
            &mut transaction,
            "0xstate_root",
            &BeaconHeaderSignedEnvelope {
                root: "0xblock_root".to_string(),
                header: BeaconHeaderEnvelope {
                    message: BeaconHeader {
                        slot: 0,
                        parent_root: GENESIS_PARENT_ROOT.to_string(),
                        state_root: "0xstate_root".to_string(),
                    },
                },
            },
            &GweiNewtype(0),
            &GweiNewtype(5),
        )
        .await;

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

        let execution_balances_sum =
            execution_chain::get_closest_balances_sum(&mut transaction, Utc::now()).await?;
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: GweiNewtype(20),
            slot: 0,
        };
        let beacon_deposits_sum = beacon_chain::get_deposits_sum(&mut transaction).await;

        let eth_supply_parts_test = EthSupplyParts {
            beacon_balances_sum: beacon_balances_sum.clone(),
            beacon_deposits_sum,
            execution_balances_sum,
        };

        let eth_supply_parts = get_supply_parts(&mut transaction, beacon_balances_sum).await?;

        dbg!(&eth_supply_parts_test);

        assert_eq!(eth_supply_parts, eth_supply_parts_test);

        Ok(())
    }

    #[tokio::test]
    async fn get_set_eth_supply_test() -> Result<()> {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let mut block_store = execution_chain::BlockStore::new(&mut transaction);

        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;

        beacon_chain::store_state(&mut transaction, "0xstate_root", &0).await?;

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

        let eth_supply_parts = EthSupplyParts {
            beacon_balances_sum,
            beacon_deposits_sum,
            execution_balances_sum,
        };

        let test_eth_supply = EthSupply {
            balances_slot: 0,
            block_number: 0,
            deposits_slot: 0,
            supply: (GweiNewtype(25).into_eth()),
            timestamp: beacon_time::get_date_time_from_slot(&0),
        };

        store(&mut transaction, &eth_supply_parts).await?;

        let eth_supply = get_current_supply(&mut transaction).await?;

        dbg!(&eth_supply);

        assert_eq!(eth_supply, test_eth_supply);

        Ok(())
    }
}
