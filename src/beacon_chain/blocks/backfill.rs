use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::Slot;

const SLOT_RANGE_SIZE: i32 = 1000;

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

#[derive(sqlx::FromRow, Debug)]
struct StateRootAndSlotFromStates {
    state_root: String,
    slot_from_states: i32,
}

pub async fn backfill_beacon_block_slots(db_pool: &PgPool) {
    info!("starting beacon_block slot backfill process.");

    let max_slot_in_states_opt: Option<i32> =
        sqlx::query_scalar!("SELECT MAX(slot) FROM beacon_states")
            .fetch_one(db_pool)
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "failed to query max slot from beacon_states");
                None
            });

    let max_slot_in_states = match max_slot_in_states_opt {
        Some(max_slot) => max_slot,
        None => {
            info!("no slots found in beacon_states. nothing to iterate over for backfill.");
            return;
        }
    };

    if max_slot_in_states < 0 {
        // Or some other sensible minimum, e.g. if starting slot is > 0
        info!(%max_slot_in_states, "max slot in beacon_states is less than zero. nothing to do.");
        return;
    }

    // Get initial count of blocks needing backfill for progress reporting
    let initial_blocks_to_backfill_count_res: Result<Option<i64>, sqlx::Error> =
        sqlx::query_scalar("SELECT COUNT(*) FROM beacon_blocks WHERE slot IS NULL")
            .fetch_optional(db_pool) // COUNT(*) always returns a row, but fetch_optional is safer with Option<T>
            .await;

    let initial_blocks_to_backfill_count = match initial_blocks_to_backfill_count_res {
        Ok(Some(count)) if count > 0 => count as u64,
        Ok(Some(count)) => {
            // count is 0 or negative
            info!(
                "no beacon_blocks found with NULL slots (count is {}). backfill not needed.",
                count
            );
            return;
        }
        Ok(None) => {
            // Should not happen with COUNT(*), but defensive
            info!("COUNT(*) query returned no row, which is unexpected. aborting backfill.");
            return;
        }
        Err(e) => {
            warn!(error = %e, "failed to query count of blocks with null slots. aborting backfill.");
            return;
        }
    };

    info!(
        initial_blocks_to_backfill_count,
        "determined number of blocks requiring slot backfill."
    );

    let mut progress = Progress::new(
        "backfill-beacon-block-slots",
        initial_blocks_to_backfill_count,
    );
    let mut total_blocks_backfilled_count: u64 = 0;
    let mut current_head_slot: i32 = max_slot_in_states;

    while current_head_slot >= 0 {
        let current_range_max_slot = current_head_slot;
        let current_range_min_slot = std::cmp::max(0, current_head_slot - SLOT_RANGE_SIZE + 1);

        debug!(
            "processing slot range: [{}, {}]",
            current_range_min_slot, current_range_max_slot
        );

        let mut is_triggered = false;
        let trigger_check_slot = current_range_max_slot;

        // Check Trigger Condition for the end of the range (highest slot in current batch)
        let trigger_state_root_opt: Option<String> = sqlx::query_scalar!(
            "SELECT state_root FROM beacon_states WHERE slot = $1",
            trigger_check_slot
        )
        .fetch_optional(db_pool)
        .await
        .unwrap_or_else(|e| {
            warn!(slot = trigger_check_slot, error = %e, "db error fetching state_root for trigger check");
            None
        });

        if let Some(trigger_state_root) = trigger_state_root_opt {
            // Check if this state_root is in beacon_blocks and its slot is NULL
            let block_slot_record_opt = sqlx::query!(
                "SELECT slot FROM beacon_blocks WHERE state_root = $1",
                &trigger_state_root
            )
            .fetch_optional(db_pool)
            .await
            .unwrap_or_else(|e| {
                warn!(state_root = %trigger_state_root, error = %e, "db error checking beacon_blocks for trigger");
                None
            });

            if let Some(record) = block_slot_record_opt {
                if record.slot.is_none() {
                    is_triggered = true;
                    info!(
                        slot_range =
                            format!("[{}, {}]", current_range_min_slot, current_range_max_slot),
                        trigger_slot = trigger_check_slot,
                        trigger_state_root,
                        "backfill triggered for slot range."
                    );
                }
            } else {
                // State root from beacon_states not found in beacon_blocks, so cannot be a trigger.
                debug!(
                    slot = trigger_check_slot,
                    %trigger_state_root,
                    "trigger state_root not found in beacon_blocks or already has slot, not triggering."
                );
            }
        } else {
            debug!(
                slot = trigger_check_slot,
                "no state_root found in beacon_states for trigger slot, not triggering."
            );
        }

        if is_triggered {
            let updates_to_make: Vec<StateRootAndSlotFromStates> = sqlx::query_as!(
                StateRootAndSlotFromStates,
                r#"
                SELECT bs.state_root, bs.slot as "slot_from_states!"
                FROM beacon_states bs
                INNER JOIN beacon_blocks bb ON bs.state_root = bb.state_root
                WHERE bs.slot >= $1 AND bs.slot <= $2 AND bb.slot IS NULL
                "#,
                current_range_min_slot,
                current_range_max_slot
            )
            .fetch_all(db_pool)
            .await
            .unwrap_or_else(|e| {
                warn!(
                    slot_range = format!("[{}, {}]", current_range_min_slot, current_range_max_slot),
                    error = %e,
                    "failed to fetch records for backfill in triggered range"
                );
                Vec::new()
            });

            if !updates_to_make.is_empty() {
                let block_slot_data_vec: Vec<BlockSlotData> = updates_to_make
                    .into_iter()
                    .map(|row| BlockSlotData {
                        state_root: row.state_root,
                        slot: Slot(row.slot_from_states),
                    })
                    .collect();

                let num_to_update = block_slot_data_vec.len();
                match bulk_update_slots(db_pool, block_slot_data_vec).await {
                    Ok(rows_affected) => {
                        total_blocks_backfilled_count += rows_affected;
                        info!(
                            slot_range =
                                format!("[{}, {}]", current_range_min_slot, current_range_max_slot),
                            attempted_updates = num_to_update,
                            actual_rows_affected = rows_affected,
                            "bulk updated beacon_blocks slots. total backfilled so far: {}",
                            total_blocks_backfilled_count
                        );
                    }
                    Err(e) => {
                        warn!(
                            slot_range = format!("[{}, {}]", current_range_min_slot, current_range_max_slot),
                            error = %e,
                            "error during bulk update of slots for range."
                        );
                    }
                }
            } else {
                info!(
                    slot_range = format!("[{}, {}]", current_range_min_slot, current_range_max_slot),
                    "triggered backfill, but no updatable (NULL slot in beacon_blocks) records found in range."
                );
            }
        } else {
            debug!(
                slot_range = format!("[{}, {}]", current_range_min_slot, current_range_max_slot),
                "backfill not triggered for slot range."
            );
        }

        progress.set_work_done(total_blocks_backfilled_count);
        info!(
            "slot backfill progress: {} (processed slots down to {})",
            progress.get_progress_string(),
            current_range_min_slot
        );

        current_head_slot = current_range_min_slot - 1;
    }

    progress.set_work_done(total_blocks_backfilled_count); // Ensure final progress reflects actual updates
    info!(
        "beacon_block_slots backfill process finished. final progress: {}. total blocks updated: {}",
        progress.get_progress_string(),
        total_blocks_backfilled_count
    );
}
