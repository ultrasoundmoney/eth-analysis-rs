use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use sqlx::{Acquire, PgConnection, PgPool};
use tracing::{debug, warn};

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

/// Retrieves the three components that make up the eth supply for a given slot.
/// Validator balances are unreliable, which means getting supply parts is unreliable.
/// be called for slots where the execution balances are known.
/// TODO: rewrite so the argument passed forces the caller to verify the execution balances are
/// known.
pub async fn get_supply_parts(
    connection: &mut PgConnection,
    slot: &Slot,
) -> Result<Option<SupplyParts>> {
    let state_root = beacon_chain::get_state_root_by_slot(connection.acquire().await?, slot)
        .await
        .expect("expect state_root to exist when getting supply parts for slot");

    // Most slots have a block, we try to retrieve a block, if we fail, we use the most recent one
    // instead.
    let block = match beacon_chain::get_block_by_slot(connection.acquire().await?, slot).await {
        None => {
            debug!(
                %slot,
                state_root,
                "no block available for slot, using most recent block before this slot"
            );
            beacon_chain::get_block_before_slot(connection.acquire().await?, slot).await
        }
        Some(block) => block,
    };

    let block_hash = block.block_hash.expect("expect block hash to be available when updating eth supply for newly available execution balance slots");

    let beacon_balances_sum =
        beacon_chain::get_balances_by_state_root(connection.acquire().await?, &state_root).await;

    match beacon_balances_sum {
        None => {
            warn!(%slot, "no beacon balances sum available for slot");
            Ok(None)
        }
        Some(beacon_balances_sum) => {
            debug!(
                %slot,
                state_root,
                block_hash,
                "looking up execution balances by hash"
            );
            let execution_balances = execution_chain::get_execution_balances_by_hash(
                connection.acquire().await?,
                &block_hash,
            )
            .await?;
            let beacon_deposits_sum = beacon_chain::get_deposits_sum_by_state_root(
                connection.acquire().await?,
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

#[async_trait]
pub trait SupplyPartsStore {
    async fn get(&self, slot: &Slot) -> Result<Option<SupplyParts>>;
}

pub struct SupplyPartsStorePostgres<'a> {
    db_pool: &'a PgPool,
}

impl<'a> SupplyPartsStorePostgres<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl SupplyPartsStore for SupplyPartsStorePostgres<'_> {
    async fn get(&self, slot: &Slot) -> Result<Option<SupplyParts>> {
        get_supply_parts(&mut self.db_pool.acquire().await?.detach(), slot).await
    }
}
