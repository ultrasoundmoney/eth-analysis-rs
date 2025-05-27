use anyhow::Result;
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info};

use crate::{
    beacon_chain::Slot,
    eth_supply::{parts::SupplyPartsStore, store},
};

/// Recomputes and updates `eth_supply` rows starting from `start_slot` (inclusive)
/// up to the latest slot already present in the `eth_supply` table.
///
/// This is useful after running `heal_deposit_sums`, which may have altered the
/// underlying data that `eth_supply` is derived from. For every affected slot we
/// 1. delete the existing `eth_supply` row, if any, and
/// 2. recompute the supply parts and store the corrected value.
///
/// The function is idempotent and only operates up to the latest stored slot so
/// it will never create new `eth_supply` entries beyond the current tip of the
/// table.
pub async fn heal_eth_supply(db_pool: &PgPool, start_slot: Slot) -> Result<()> {
    info!(%start_slot, "starting eth supply healing process");

    // Determine the upper bound – the latest slot for which we have already
    // stored an eth supply. If nothing has been stored yet there is nothing to
    // heal.
    let last_stored_supply_slot_opt = store::get_last_stored_supply_slot(db_pool).await?;

    let last_stored_supply_slot = match last_stored_supply_slot_opt {
        Some(slot) => slot,
        None => {
            info!("no eth supply stored, nothing to heal");
            return Ok(());
        }
    };

    if start_slot > last_stored_supply_slot {
        info!(
            %start_slot,
            %last_stored_supply_slot,
            "start_slot is after last stored eth supply slot – nothing to heal"
        );
        return Ok(());
    }

    let total_slots_to_process = (last_stored_supply_slot.0 - start_slot.0 + 1) as u64;
    let mut progress = Progress::new("heal-eth-supply", total_slots_to_process);
    let supply_parts_store = SupplyPartsStore::new(db_pool);
    let mut conn = db_pool.acquire().await?;

    for current_slot_val in start_slot.0..=last_stored_supply_slot.0 {
        let current_slot = Slot(current_slot_val);

        // Remove existing supply row so we can insert the corrected value.
        // It's important to use a connection that can be part of a transaction if needed by the called function,
        // but here we're explicitly not managing the transaction at this level.
        // The `rollback_supply_slot` function takes `&mut PgConnection`.
        store::rollback_supply_slot(&mut conn, current_slot).await?;

        // Recalculate supply parts using the *current* database state (which
        // includes healed deposit sums).
        let supply_parts_opt = supply_parts_store.get(current_slot).await?;

        // Drop the connection so it's returned to the pool before the next operation if it also acquires.
        // Or, keep it if the next operation can use PgExecutor, which &PgPool implements.
        // In this case, store::store takes PgExecutor, so we can pass db_pool directly.

        match supply_parts_opt {
            Some(supply_parts) => {
                store::store(
                    db_pool,
                    current_slot,
                    &supply_parts.block_number(),
                    &supply_parts.execution_balances_sum,
                    &supply_parts.beacon_balances_sum,
                    &supply_parts.beacon_deposits_sum,
                )
                .await?;
                debug!(%current_slot, "healed eth supply for slot");
            }
            None => {
                debug!(
                    %current_slot,
                    "supply parts not available – skipping heal for this slot"
                );
            }
        }

        progress.inc_work_done();
        if progress.work_done % 100 == 0 || progress.work_done == total_slots_to_process {
            info!("{}", progress.get_progress_string());
        }
    }

    info!("eth supply healing finished");
    Ok(())
}
