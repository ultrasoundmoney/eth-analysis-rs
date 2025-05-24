use futures::StreamExt;
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::node::{BeaconNode, BeaconNodeHttp};
use crate::beacon_chain::{BlockId, Slot};

async fn estimate_work_todo(db_pool: &PgPool) -> u64 {
    let count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM beacon_blocks
        WHERE slot IS NULL
        "#
    )
    .fetch_one(db_pool)
    .await
    .unwrap_or(0); // Default to 0 if query fails or table is empty

    count.try_into().unwrap_or(0)
}

pub async fn backfill_blocks(db_pool: &PgPool) {
    let beacon_node = BeaconNodeHttp::new();

    debug!("estimating work for backfilling missing block slots");
    let work_todo = estimate_work_todo(db_pool).await;
    if work_todo == 0 {
        info!("no beacon_blocks rows found with missing slots. nothing to do.");
        return;
    }
    debug!(
        "estimated work to be done for block slot backfill: {} rows",
        work_todo
    );
    let mut progress = Progress::new("backfill-missing-block-slots", work_todo);

    let mut rows_stream =
        sqlx::query!("SELECT state_root FROM beacon_blocks WHERE slot IS NULL ORDER BY state_root")
            .fetch(db_pool);

    while let Some(row_result) = rows_stream.next().await {
        match row_result {
            Ok(row) => {
                let state_root_to_process = row.state_root;
                let mut slot_found: Option<Slot> = None;

                // 1. Try to find slot in beacon_states table
                match sqlx::query_scalar!(
                    "SELECT slot FROM beacon_states WHERE state_root = $1",
                    &state_root_to_process
                )
                .fetch_optional(db_pool)
                .await
                {
                    Ok(Some(slot_val)) => {
                        debug!(state_root = %state_root_to_process, slot = slot_val, "slot found in beacon_states");
                        slot_found = Some(Slot(slot_val));
                    }
                    Ok(None) => {
                        debug!(state_root = %state_root_to_process, "slot not found in beacon_states, trying beacon node");
                    }
                    Err(e) => {
                        warn!(state_root = %state_root_to_process, error = %e, "error querying beacon_states for slot");
                    }
                }

                // 2. If not found in beacon_states, try beacon node
                if slot_found.is_none() {
                    match beacon_node
                        .get_header(&BlockId::BlockRoot(state_root_to_process.clone()))
                        .await
                    {
                        Ok(Some(header_envelope)) => {
                            let node_slot = header_envelope.slot();
                            debug!(state_root = %state_root_to_process, slot = %node_slot, "slot found via beacon node header");
                            slot_found = Some(node_slot);
                        }
                        Ok(None) => {
                            warn!(state_root = %state_root_to_process, "header (and slot) not found via beacon node");
                        }
                        Err(e) => {
                            warn!(state_root = %state_root_to_process, error = %e, "error fetching header from beacon node");
                        }
                    }
                }

                // 3. If slot was found, update beacon_blocks
                if let Some(slot_to_update) = slot_found {
                    match sqlx::query!(
                        "UPDATE beacon_blocks SET slot = $1 WHERE state_root = $2",
                        slot_to_update.0,
                        &state_root_to_process
                    )
                    .execute(db_pool)
                    .await
                    {
                        Ok(update_result) => {
                            if update_result.rows_affected() > 0 {
                                info!(state_root = %state_root_to_process, slot = %slot_to_update, "successfully updated slot in beacon_blocks");
                            } else {
                                warn!(state_root = %state_root_to_process, slot = %slot_to_update, "slot found, but no rows updated in beacon_blocks (state_root may have been processed or deleted)");
                            }
                        }
                        Err(e) => {
                            warn!(state_root = %state_root_to_process, slot = %slot_to_update, error = %e, "failed to update slot in beacon_blocks");
                        }
                    }
                } else {
                    warn!(state_root = %state_root_to_process, "slot could not be determined from any source");
                }
            }
            Err(e) => {
                warn!(error = %e, "error fetching row from beacon_blocks for slot backfill");
            }
        }
        progress.inc_work_done();
        if progress.work_done % 100 == 0 || progress.work_done == work_todo {
            info!(
                "missing block slots backfill progress: {}",
                progress.get_progress_string()
            );
        }
    }

    info!(
        "beacon_blocks missing slots backfill process finished. final progress: {}",
        progress.get_progress_string()
    );
}
