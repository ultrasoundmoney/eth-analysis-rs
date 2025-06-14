use anyhow::{bail, Context, Result};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use tracing::{debug, error, warn};

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
    pub beacon_deposits_sum: GweiNewtype,
    pub block_number: BlockNumber,
    pub execution_balances_sum: WeiNewtype,
    pub slot: Slot,
}

impl SupplyParts {
    pub fn new(
        slot: Slot,
        block_number: &BlockNumber,
        execution_balances_sum: WeiNewtype,
        beacon_balances_sum: GweiNewtype,
        beacon_deposits_sum: GweiNewtype,
    ) -> Self {
        Self {
            beacon_balances_sum,
            beacon_deposits_sum,
            block_number: *block_number,
            execution_balances_sum,
            slot,
        }
    }

    pub fn block_number(&self) -> BlockNumber {
        self.block_number
    }
}

// Our parent callers take care to try not to call us when beacon and execution have not both been synced.
// This is bad. Let's try to slowly work towards taking what we need as arguments.
// This way the caller is explicitly responsible for gathering what we need.
async fn gather_supply_parts(
    executor: &mut PgConnection,
    target_slot: Slot,
) -> Result<Option<SupplyParts>> {
    // 1. Find a block for the `target_slot`.
    // None found is acceptable for empty slots.
    let block = match beacon_chain::get_block_by_slot(&mut *executor, target_slot).await? {
        Some(b) => b,
        None => {
            debug!(%target_slot, "no block found for slot, cannot compute supply parts");
            return Ok(None);
        }
    };

    // For very old slots, pre-merge, no block hash is available.
    // We don't currently support these slots.
    let block_hash = block.block_hash.ok_or(anyhow::anyhow!(
        "no block hash found for slot, cannot compute supply parts"
    ))?;

    debug!(
        %target_slot,
        %block_hash,
        "found block for supply parts calculation"
    );

    // 2. Get beacon balances. We don't expect to have these for every slot.
    // If we don't find any, return None.
    let Some(beacon_balances_sum) =
        beacon_chain::get_balances_by_state_root(&mut *executor, &block.state_root).await?
    else {
        debug!(%target_slot, "no beacon balances found for state root, returning None");
        return Ok(None);
    };

    debug!(%target_slot, %beacon_balances_sum, "found beacon balances");

    // 3. Get execution balances. If missing, bail.
    let execution_balances_data =
        execution_chain::get_execution_balances_by_hash(&mut *executor, &block_hash)
            .await?
            .ok_or(anyhow::anyhow!(
                "failed to get execution balances for block hash {}",
                block_hash
            ))?;

    // 4. Get beacon deposits sum. If missing, bail.
    let beacon_deposits_sum = match beacon_chain::get_deposits_sum_by_state_root(
        &mut *executor,
        &block.state_root,
    )
    .await
    .context(format!(
        "failed to get beacon deposits for state root {}",
        block.state_root
    ))? {
        Some(sum) => sum,
        None => {
            error!(%target_slot, block_state_root = %block.state_root, "no beacon deposits sum found for block's state_root");
            bail!(
                "no beacon deposits sum found for block's state_root: {}",
                block.state_root
            );
        }
    };

    // 5. Obtain pending deposits sum — prefer the value we stored at ingest
    // time (beacon_blocks.pending_deposits_sum_gwei). If we cannot obtain a
    // value we *skip* supply calculation for this slot to avoid publishing an
    // incorrect number.

    let pending_deposits_sum_db = sqlx::query!(
        r#"
            SELECT
                pending_deposits_sum_gwei
            FROM
                beacon_blocks
            WHERE
                state_root = $1
        "#,
        &block.state_root
    )
    .fetch_optional(&mut *executor)
    .await?
    .and_then(|row| row.pending_deposits_sum_gwei.map(GweiNewtype));

    let pending_deposits_sum = match pending_deposits_sum_db {
        Some(sum) => {
            debug!(%target_slot, state_root = %block.state_root, pending_deposits_sum = %sum, "fetched pending deposits sum from beacon node");
            sum
        }
        None => {
            warn!(%target_slot, state_root = %block.state_root, "pending deposits sum unavailable from db and beacon node; skipping supply calculation for this slot");
            return Ok(None);
        }
    };

    let net_deposits_sum = beacon_deposits_sum - pending_deposits_sum;
    debug!(%target_slot, %beacon_deposits_sum, %pending_deposits_sum, %net_deposits_sum, "calculated net_deposits_sum");

    let supply_parts = SupplyParts::new(
        target_slot,
        &execution_balances_data.block_number,
        execution_balances_data.balances_sum,
        beacon_balances_sum,
        net_deposits_sum,
    );

    Ok(Some(supply_parts))
}

pub struct SupplyPartsStore<'a> {
    db_pool: &'a PgPool,
}

impl<'a> SupplyPartsStore<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }

    pub async fn get(&self, slot: Slot) -> Result<Option<SupplyParts>> {
        let mut conn = self
            .db_pool
            .acquire()
            .await
            .context("failed to acquire db connection for get supply parts")?;
        gather_supply_parts(&mut conn, slot).await
    }

    pub async fn get_with_transaction(
        transaction: &mut PgConnection,
        slot: Slot,
    ) -> Result<Option<SupplyParts>> {
        gather_supply_parts(transaction, slot).await
    }
}
