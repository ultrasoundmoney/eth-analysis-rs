use serde::Serialize;
use sqlx::{Acquire, PgConnection, PgPool};
use thiserror::Error;
use tracing::debug;

use crate::{
    beacon_chain::{self, Slot},
    execution_chain::{self, BlockNumber},
    units::{GweiNewtype, WeiNewtype},
};

// Remove deprecated fields after frontend switches over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupplyParts {
    pub beacon_balances_sum: GweiNewtype,
    #[deprecated = "switch to beacon_balances_sum"]
    beacon_balances_sum_next: GweiNewtype,
    pub beacon_deposits_sum: GweiNewtype,
    #[deprecated = "switch to beacon_deposits_sum"]
    beacon_deposits_sum_next: GweiNewtype,
    pub block_number: BlockNumber,
    pub execution_balances_sum: WeiNewtype,
    #[deprecated = "switch to execution_balances_sum"]
    execution_balances_sum_next: WeiNewtype,
    pub slot: Slot,
}

impl SupplyParts {
    pub fn new(
        slot: &Slot,
        block_number: &BlockNumber,
        execution_balances_sum: WeiNewtype,
        beacon_balances_sum: GweiNewtype,
        beacon_deposits_sum: GweiNewtype,
    ) -> Self {
        Self {
            beacon_balances_sum,
            beacon_balances_sum_next: beacon_balances_sum,
            beacon_deposits_sum,
            beacon_deposits_sum_next: beacon_deposits_sum,
            block_number: *block_number,
            execution_balances_sum,
            execution_balances_sum_next: execution_balances_sum,
            slot: *slot,
        }
    }

    pub fn block_number(&self) -> BlockNumber {
        self.block_number
    }

    pub fn supply(&self) -> WeiNewtype {
        self.execution_balances_sum + self.beacon_balances_sum.into()
            - self.beacon_deposits_sum.into()
    }
}

#[derive(Debug, Error)]
pub enum SupplyPartsError {
    #[error("no validator balances available for slot {0}")]
    NoValidatorBalancesAvailable(Slot),
}

async fn get_supply_parts(
    executor_acq: &mut PgConnection,
    slot: &Slot,
) -> Result<SupplyParts, SupplyPartsError> {
    let state_root =
        beacon_chain::get_state_root_by_slot(executor_acq.acquire().await.unwrap(), slot)
            .await
            .expect("expect state_root to exist when getting supply parts for slot");

    // Most slots have a block, empty slots do not. In the case of an empty slot we use the most
    // recent block instead to have an eth supply for every slot.
    let block = match beacon_chain::get_block_by_slot(executor_acq.acquire().await.unwrap(), slot)
        .await
    {
        None => {
            debug!(
                %slot,
                state_root,
                "no block available for slot, using most recent block before this slot"
            );
            beacon_chain::get_block_before_slot(executor_acq.acquire().await.unwrap(), slot).await
        }
        Some(block) => block,
    };

    let block_hash = block
        .block_hash
        .expect("expect block hash to be available when getting supply parts");

    let beacon_balances_sum = beacon_chain::get_balances_by_state_root(
        executor_acq.acquire().await.unwrap(),
        &state_root,
    )
    .await
    .ok_or(SupplyPartsError::NoValidatorBalancesAvailable(*slot))?;

    debug!(
        %slot,
        state_root,
        block_hash,
        "looking up execution balances by hash"
    );

    let execution_balances = execution_chain::get_execution_balances_by_hash(
        executor_acq.acquire().await.unwrap(),
        &block_hash,
    )
    .await
    .unwrap();

    let beacon_deposits_sum = beacon_chain::get_deposits_sum_by_state_root(
        executor_acq.acquire().await.unwrap(),
        &block.state_root,
    )
    .await
    .unwrap();

    let supply_parts = SupplyParts::new(
        slot,
        &execution_balances.block_number,
        execution_balances.balances_sum,
        beacon_balances_sum,
        beacon_deposits_sum,
    );

    Ok(supply_parts)
}

pub struct SupplyPartsStore<'a> {
    db_pool: &'a PgPool,
}

impl<'a> SupplyPartsStore<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }

    /// Retrieves the three components that make up the eth supply for a given slot.
    pub async fn get(&self, slot: &Slot) -> Result<SupplyParts, SupplyPartsError> {
        get_supply_parts(&mut self.db_pool.acquire().await.unwrap(), slot).await
    }

    pub async fn get_with_transaction(
        transaction: &mut PgConnection,
        slot: &Slot,
    ) -> Result<SupplyParts, SupplyPartsError> {
        get_supply_parts(transaction, slot).await
    }
}
