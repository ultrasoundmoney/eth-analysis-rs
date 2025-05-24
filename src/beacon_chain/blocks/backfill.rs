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
    info!("starting beacon_block slot backfill process using range-based scan of beacon_states.");

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
            info!("no slots found in beacon_states. nothing to do.");
            return;
        }
    };

    if max_slot_in_states < 0 {
        // Or some other sensible minimum, e.g. if starting slot is > 0
        info!(%max_slot_in_states, "max slot in beacon_states is less than zero. nothing to do.");
        return;
    }

    let mut progress = Progress::new(
        "backfill-beacon-block-slots-by-range",
        max_slot_in_states as u64,
    );
    let mut current_min_slot_in_range: i32 = 0; // Assuming we start checking from slot 0

    while current_min_slot_in_range <= max_slot_in_states {
        let current_max_slot_in_range = current_min_slot_in_range + SLOT_RANGE_SIZE - 1;
        debug!(
            "processing slot range: [{}, {}]",
            current_min_slot_in_range, current_max_slot_in_range
        );

        let mut is_triggered = false;

        // Check Trigger Condition for the start of the range
        let trigger_state_root_opt: Option<String> = sqlx::query_scalar!(
            "SELECT state_root FROM beacon_states WHERE slot = $1",
            current_min_slot_in_range
        )
        .fetch_optional(db_pool)
        .await
        .unwrap_or_else(|e| {
            warn!(slot = current_min_slot_in_range, error = %e, "db error fetching state_root for trigger check");
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
                        slot_range = format!(
                            "[{}, {}]",
                            current_min_slot_in_range, current_max_slot_in_range
                        ),
                        trigger_slot = current_min_slot_in_range,
                        trigger_state_root,
                        "backfill triggered for slot range."
                    );
                }
            } else {
                // State root from beacon_states not found in beacon_blocks, so cannot be a trigger.
                debug!(
                    slot = current_min_slot_in_range,
                    %trigger_state_root,
                    "trigger state_root not found in beacon_blocks or already has slot, not triggering."
                );
            }
        } else {
            debug!(
                slot = current_min_slot_in_range,
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
                current_min_slot_in_range,
                current_max_slot_in_range
            )
            .fetch_all(db_pool)
            .await
            .unwrap_or_else(|e| {
                warn!(
                    slot_range = format!("[{}, {}]", current_min_slot_in_range, current_max_slot_in_range),
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
                        info!(
                            slot_range = format!(
                                "[{}, {}]",
                                current_min_slot_in_range, current_max_slot_in_range
                            ),
                            attempted_updates = num_to_update,
                            actual_rows_affected = rows_affected,
                            "bulk updated beacon_blocks slots."
                        );
                    }
                    Err(e) => {
                        warn!(
                            slot_range = format!("[{}, {}]", current_min_slot_in_range, current_max_slot_in_range),
                            error = %e,
                            "error during bulk update of slots for range."
                        );
                    }
                }
            } else {
                info!(
                    slot_range = format!("[{}, {}]", current_min_slot_in_range, current_max_slot_in_range),
                    "triggered backfill, but no updatable (NULL slot in beacon_blocks) records found in range."
                );
            }
        } else {
            debug!(
                slot_range = format!(
                    "[{}, {}]",
                    current_min_slot_in_range, current_max_slot_in_range
                ),
                "backfill not triggered for slot range."
            );
        }

        progress.set_work_done(std::cmp::min(
            current_max_slot_in_range as u64,
            max_slot_in_states as u64,
        ));
        info!(
            "slot backfill progress by range: {}",
            progress.get_progress_string()
        );

        current_min_slot_in_range += SLOT_RANGE_SIZE;
    }

    progress.set_work_done(max_slot_in_states as u64); // Ensure progress shows 100%
    info!(
        "beacon_block_slots backfill by range process finished. final progress: {}",
        progress.get_progress_string()
    );
}
