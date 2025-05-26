use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::beacon_chain::Slot;

#[derive(Debug, Clone)]
struct BlockSlotData {
    state_root: String,
    slot: Slot,
}

async fn bulk_update_slots(db_pool: &PgPool, updates: Vec<BlockSlotData>) -> sqlx::Result<u64> {
    if updates.is_empty() {
        return Ok(0);
    }

    let mut state_roots: Vec<String> = Vec::with_capacity(updates.len());
    let mut slots: Vec<i32> = Vec::with_capacity(updates.len());

    for update in updates {
        state_roots.push(update.state_root);
        slots.push(update.slot.0);
    }

    let rows_affected = sqlx::query!(
        r#"
        UPDATE beacon_blocks AS bb
        SET slot = upd.slot
        FROM UNNEST($1::text[], $2::int4[]) AS upd(state_root, slot)
        WHERE bb.state_root = upd.state_root
        "#,
        &state_roots,
        &slots
    )
    .execute(db_pool)
    .await?
    .rows_affected();

    Ok(rows_affected)
}

pub async fn backfill_beacon_block_slots(db_pool: &PgPool, start_slot: Slot) {
    info!("starting beacon_block slot backfill process");

    let state_roots_to_backfill_res: Result<Vec<String>, sqlx::Error> = sqlx::query_scalar!(
        "SELECT state_root FROM beacon_blocks WHERE slot IS NULL AND slot >= $1",
        start_slot.0
    )
    .fetch_all(db_pool)
    .await;

    let state_roots_to_backfill = match state_roots_to_backfill_res {
        Ok(roots) => roots,
        Err(e) => {
            warn!(error = %e, "failed to query beacon_blocks for state_roots with NULL slots. aborting backfill.");
            return;
        }
    };

    if state_roots_to_backfill.is_empty() {
        info!("no beacon_blocks found with NULL slots. backfill not needed.");
        return;
    }

    let initial_blocks_to_process_count = state_roots_to_backfill.len() as u64;
    info!(
        initial_blocks_to_process_count,
        "found blocks with NULL slot. attempting to find their slots in beacon_states."
    );

    let mut progress = Progress::new(
        "backfill-beacon-block-slots",
        initial_blocks_to_process_count,
    );
    let mut total_blocks_successfully_updated: u64 = 0;
    let mut updates_for_bulk_op: Vec<BlockSlotData> =
        Vec::with_capacity(state_roots_to_backfill.len());
    let mut processed_count: u64 = 0;

    for state_root_from_blocks in state_roots_to_backfill {
        let slot_from_states_res: Result<Option<i32>, sqlx::Error> = sqlx::query_scalar!(
            "SELECT slot FROM beacon_states WHERE state_root = $1",
            &state_root_from_blocks
        )
        .fetch_optional(db_pool)
        .await;

        match slot_from_states_res {
            Ok(Some(slot_value)) => {
                updates_for_bulk_op.push(BlockSlotData {
                    state_root: state_root_from_blocks.clone(),
                    slot: Slot(slot_value),
                });
            }
            Ok(None) => {
                warn!(
                    state_root = %state_root_from_blocks,
                    "state_root from beacon_blocks (with NULL slot) not found in beacon_states. cannot backfill this block's slot."
                );
            }
            Err(e) => {
                warn!(
                    state_root = %state_root_from_blocks,
                    error = %e,
                    "db error fetching slot from beacon_states for state_root. skipping this block."
                );
            }
        }
        processed_count += 1;
        progress.set_work_done(processed_count);
    }

    if !updates_for_bulk_op.is_empty() {
        let num_to_update = updates_for_bulk_op.len();
        info!(
            updates_to_attempt = num_to_update,
            "gathered all potential updates. proceeding with bulk update operation."
        );
        match bulk_update_slots(db_pool, updates_for_bulk_op).await {
            Ok(rows_affected) => {
                total_blocks_successfully_updated = rows_affected;
                info!(
                    attempted_updates = num_to_update,
                    actual_rows_affected = rows_affected,
                    "bulk update of beacon_blocks slots complete."
                );
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "error during bulk update of slots. some slots may not have been backfilled."
                );
            }
        }
    } else {
        info!("no updatable records found (e.g., all target blocks were missing from beacon_states or had db errors). no bulk update performed.");
    }

    info!(
        "beacon_block_slots backfill process finished. processed {} potential blocks. successfully updated {} blocks. progress: {}",
        processed_count,
        total_blocks_successfully_updated,
        progress.get_progress_string()
    );
}
