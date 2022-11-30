use anyhow::Result;
use serde::Serialize;
use sqlx::{Acquire, PgConnection, PgPool};
use tracing::debug;

use crate::{
    beacon_chain::{self, BeaconBalancesSum, BeaconDepositsSum, Slot},
    caching::{self, CacheKey},
    eth_units::{GweiNewtype, Wei},
    execution_chain::{self, BlockNumber, ExecutionBalancesSum},
    key_value_store,
};

// Remove deprecated fields after frontend switches over.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyParts {
    #[deprecated = "switch to beacon_balances_sum_next"]
    beacon_balances_sum: BeaconBalancesSum,
    pub beacon_balances_sum_next: GweiNewtype,
    #[deprecated = "switch to beacon_deposits_sum_next"]
    beacon_deposits_sum: BeaconDepositsSum,
    pub beacon_deposits_sum_next: GweiNewtype,
    #[deprecated = "switch to execution_balances_sum_next"]
    pub execution_balances_sum: ExecutionBalancesSum,
    pub execution_balances_sum_next: Wei,
}

impl SupplyParts {
    pub fn new(
        slot: &Slot,
        block_number: &BlockNumber,
        execution_balances_sum: Wei,
        beacon_balances_sum: GweiNewtype,
        beacon_deposits_sum: GweiNewtype,
    ) -> Self {
        Self {
            beacon_balances_sum: BeaconBalancesSum {
                slot: *slot,
                balances_sum: beacon_balances_sum,
            },
            beacon_balances_sum_next: beacon_balances_sum,
            beacon_deposits_sum: BeaconDepositsSum {
                deposits_sum: beacon_deposits_sum,
                slot: *slot,
            },
            beacon_deposits_sum_next: beacon_deposits_sum,
            execution_balances_sum: ExecutionBalancesSum {
                block_number: *block_number,
                balances_sum: execution_balances_sum,
            },
            execution_balances_sum_next: execution_balances_sum,
        }
    }

    pub fn block_number(&self) -> BlockNumber {
        self.execution_balances_sum.block_number
    }
}

/// Retrieves the three components that make up the eth supply for a given slot.
/// Validator balances are unreliable, which means getting supply parts is unreliable.
/// be called for slots where the execution balances are known.
/// TODO: rewrite so the argument passed forces the caller to verify the execution balances are
/// known.
pub async fn get_supply_parts(
    executor: &mut PgConnection,
    slot: &Slot,
) -> Result<Option<SupplyParts>> {
    let state_root = beacon_chain::get_state_root_by_slot(executor.acquire().await?, slot)
        .await?
        .expect("expect state_root to exist when getting supply parts for slot");

    // Most slots have a block, we try to retrieve a block, if we fail, we use the most recent one
    // instead.
    let block = match beacon_chain::get_block_by_slot(executor.acquire().await?, slot).await? {
        None => {
            debug!(
                slot,
                state_root, "no block available for slot, using most recent block before this slot"
            );
            beacon_chain::get_block_before_slot(executor.acquire().await?, slot).await?
        }
        Some(block) => block,
    };

    let block_hash = block.block_hash.expect("expect block hash to be available when updating eth supply for newly available execution balance slots");

    let beacon_balances_sum =
        beacon_chain::get_balances_by_state_root(executor.acquire().await?, &state_root).await?;

    match beacon_balances_sum {
        None => Ok(None),
        Some(beacon_balances_sum) => {
            let execution_balances = execution_chain::get_execution_balances_by_hash(
                executor.acquire().await?,
                &block_hash,
            )
            .await?;
            let beacon_deposits_sum = beacon_chain::get_deposits_sum_by_state_root(
                executor.acquire().await?,
                &block.state_root,
            )
            .await?;

            let supply_parts = SupplyParts::new(
                slot,
                &execution_balances.block_number,
                execution_balances.balances_sum,
                beacon_balances_sum,
                beacon_deposits_sum,
            );

            Ok(Some(supply_parts))
        }
    }
}

pub async fn update_cache(db_pool: &PgPool, eth_supply_parts: &SupplyParts) -> Result<()> {
    key_value_store::set_value_str(
        db_pool,
        &CacheKey::EthSupplyParts.to_db_key(),
        // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
        // as string however.
        &serde_json::to_string(eth_supply_parts).unwrap(),
    )
    .await;

    caching::publish_cache_update(db_pool, CacheKey::EthSupplyParts).await?;

    Ok(())
}
