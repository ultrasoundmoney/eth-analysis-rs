use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::node::{BeaconNode, BeaconNodeHttp};
use crate::beacon_chain::{BlockId, Slot};

const DB_CHUNK_SIZE: usize = 1000;
const NODE_LOOKUP_CONCURRENCY_LIMIT: usize = 4;

async fn estimate_total_missing_slots(db_pool: &PgPool) -> u64 {
    sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!" FROM beacon_blocks WHERE slot IS NULL"#)
        .fetch_one(db_pool)
        .await
        .unwrap_or(0)
        .try_into()
        .unwrap_or(0)
}

#[derive(Debug)]
struct BlockSlotData {
    state_root: String,
    slot: Slot,
}

async fn find_slot_for_state_root(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
    state_root: String,
) -> Option<BlockSlotData> {
    // 1. Try beacon_states table first
    match sqlx::query_scalar!(
        "SELECT slot FROM beacon_states WHERE state_root = $1",
        &state_root
    )
    .fetch_optional(db_pool)
    .await
    {
        Ok(Some(slot_val)) => {
            debug!(%state_root, slot = slot_val, "slot found in beacon_states");
            return Some(BlockSlotData {
                state_root,
                slot: Slot(slot_val),
            });
        }
        Ok(None) => {
            debug!(%state_root, "slot not found in beacon_states, trying beacon node");
        }
        Err(e) => {
            warn!(%state_root, error = %e, "db error querying beacon_states for slot, trying beacon node");
        }
    }

    // 2. If not in beacon_states, try beacon node
    warn!(%state_root, "falling back to beacon node to find slot for state_root");
    match beacon_node
        .get_header(&BlockId::BlockRoot(state_root.clone()))
        .await
    {
        Ok(Some(header_envelope)) => {
            let node_slot = header_envelope.slot();
            debug!(%state_root, slot = %node_slot, "slot found via beacon node header");
            return Some(BlockSlotData {
                state_root,
                slot: node_slot,
            });
        }
        Ok(None) => {
            warn!(%state_root, "header (and slot) not found via beacon node for state_root");
        }
        Err(e) => {
            warn!(%state_root, error = %e, "error fetching header from beacon node for state_root");
        }
    }

    None
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

    // Using UNNEST for bulk update as per SQLx FAQ
    // Assumes `slot` column in `beacon_blocks` is of a type compatible with i32 (e.g., INTEGER)
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

pub async fn backfill_beacon_block_slots(db_pool: &PgPool) {
    let beacon_node = BeaconNodeHttp::new();

    debug!("estimating total work for backfilling missing beacon_block slots");
    let total_work = estimate_total_missing_slots(db_pool).await;
    if total_work == 0 {
        info!("no beacon_blocks rows found with missing slots. nothing to do.");
        return;
    }
    debug!(
        "total beacon_blocks with missing slots to process: {}",
        total_work
    );
    let mut progress = Progress::new("backfill-beacon-block-slots", total_work);
    let mut overall_processed_count: u64 = 0;

    loop {
        let state_roots_to_process: Vec<String> = sqlx::query_scalar!(
            "SELECT state_root FROM beacon_blocks WHERE slot IS NULL LIMIT $1",
            DB_CHUNK_SIZE as i64
        )
        .fetch_all(db_pool)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to fetch chunk of state_roots, ending backfill early");
            Vec::new()
        });

        if state_roots_to_process.is_empty() {
            info!("no more state_roots with missing slots found to process.");
            break;
        }

        let chunk_size = state_roots_to_process.len() as u64;
        debug!("processing a new chunk of {} state_roots", chunk_size);

        let tasks =
            stream::iter(state_roots_to_process).map(|state_root| {
                let db_pool_clone = db_pool.clone();
                let beacon_node_clone = beacon_node.clone();
                async move {
                    find_slot_for_state_root(&db_pool_clone, &beacon_node_clone, state_root).await
                }
            });

        let results: Vec<Option<BlockSlotData>> = tasks
            .buffer_unordered(NODE_LOOKUP_CONCURRENCY_LIMIT)
            .collect()
            .await;

        let successful_updates: Vec<BlockSlotData> = results.into_iter().flatten().collect();

        if !successful_updates.is_empty() {
            match bulk_update_slots(db_pool, successful_updates).await {
                Ok(rows_affected) => {
                    info!("bulk updated {} rows with new slots.", rows_affected);
                }
                Err(e) => {
                    warn!(error = %e, "error during bulk update of slots");
                }
            }
        }

        overall_processed_count += chunk_size; // Count all attempted in chunk, not just successfully updated
        progress.set_work_done(overall_processed_count);
        info!("slot backfill progress: {}", progress.get_progress_string());

        if chunk_size < DB_CHUNK_SIZE as u64 {
            info!("processed the last chunk of state_roots.");
            break; // Likely the last chunk
        }
    }

    progress.set_work_done(total_work); // Ensure progress shows 100% if loop finished
    info!(
        "beacon_block_slots backfill process finished. final progress: {}",
        progress.get_progress_string()
    );
}
