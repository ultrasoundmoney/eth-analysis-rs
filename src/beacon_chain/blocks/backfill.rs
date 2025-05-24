use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::node::{BeaconNode, BeaconNodeHttp};
use crate::beacon_chain::{BlockId, Slot};

const DB_CHUNK_SIZE: usize = 100;
const NODE_LOOKUP_CONCURRENCY_LIMIT: usize = 4;

async fn estimate_total_missing_slots(db_pool: &PgPool) -> u64 {
    sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!" FROM beacon_blocks WHERE slot IS NULL"#)
        .fetch_one(db_pool)
        .await
        .unwrap_or(0)
        .try_into()
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
struct BlockSlotData {
    state_root: String,
    slot: Slot,
}

async fn fetch_slot_from_node_for_state_root(
    beacon_node: &BeaconNodeHttp,
    state_root: String,
) -> Option<BlockSlotData> {
    warn!(%state_root, "falling back to beacon node to find slot for state_root");
    match beacon_node
        .get_header(&BlockId::BlockRoot(state_root.clone()))
        .await
    {
        Ok(Some(header_envelope)) => {
            let node_slot = header_envelope.slot();
            debug!(%state_root, slot = %node_slot, "slot found via beacon node header");
            Some(BlockSlotData {
                state_root,
                slot: node_slot,
            })
        }
        Ok(None) => {
            warn!(%state_root, "header (and slot) not found via beacon node for state_root");
            None
        }
        Err(e) => {
            warn!(%state_root, error = %e, "error fetching header from beacon node for state_root");
            None
        }
    }
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
struct StateRootAndOptionalDbSlot {
    state_root: String,
    slot_from_db: Option<i32>,
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
        // Fetch a chunk of beacon_blocks with NULL slots
        let state_roots_to_process: Vec<String> = sqlx::query_scalar!(
            r#"
            SELECT state_root
            FROM beacon_blocks
            WHERE slot IS NULL
            ORDER BY pending_deposits_sum_gwei DESC
            LIMIT $1
            "#,
            DB_CHUNK_SIZE as i64
        )
        .fetch_all(db_pool)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to fetch chunk of state_roots with null slots, ending backfill early");
            Vec::new()
        });

        if state_roots_to_process.is_empty() {
            info!("no more beacon_blocks with missing slots found to process.");
            break;
        }

        let chunk_size = state_roots_to_process.len() as u64;
        debug!("processing a new chunk of {} state_roots", chunk_size);

        // Fetch slots from beacon_states for the current chunk of state_roots
        // We need a way to associate the fetched slots back to their state_roots
        let slots_from_db_result: Vec<(String, Option<i32>)> = if !state_roots_to_process.is_empty()
        {
            sqlx::query_as!(
                StateRootAndOptionalDbSlot,
                r#"
                SELECT bs.state_root AS "state_root!", bs.slot AS "slot_from_db?"
                FROM beacon_states bs
                WHERE bs.state_root = ANY($1)
                "#,
                &state_roots_to_process
            )
            .fetch_all(db_pool)
            .await
            .map(|records| records.into_iter().map(|r| (r.state_root, r.slot_from_db)).collect())
            .unwrap_or_else(|e| {
                warn!(error = %e, "failed to fetch slots from beacon_states for chunk, will attempt node lookup for all");
                Vec::new()
            })
        } else {
            Vec::new()
        };

        // Create a map for quick lookup of slots from beacon_states
        let slots_map: std::collections::HashMap<String, i32> = slots_from_db_result
            .into_iter()
            .filter_map(|(sr, opt_slot)| opt_slot.map(|slot_val| (sr, slot_val)))
            .collect();

        let mut successfully_found_slots: Vec<BlockSlotData> = Vec::new();
        let mut state_roots_needing_node_lookup: Vec<String> = Vec::new();

        for state_root_to_process in state_roots_to_process {
            if let Some(&slot_val) = slots_map.get(&state_root_to_process) {
                successfully_found_slots.push(BlockSlotData {
                    state_root: state_root_to_process,
                    slot: Slot(slot_val),
                });
            } else {
                state_roots_needing_node_lookup.push(state_root_to_process);
            }
        }

        debug!(
            "from db: {} slots found directly, {} need node lookup",
            successfully_found_slots.len(),
            state_roots_needing_node_lookup.len()
        );

        if !state_roots_needing_node_lookup.is_empty() {
            let node_lookups = stream::iter(state_roots_needing_node_lookup)
                .map(|sr_to_check| {
                    let beacon_node_clone = beacon_node.clone();
                    async move {
                        fetch_slot_from_node_for_state_root(&beacon_node_clone, sr_to_check).await
                    }
                })
                .buffer_unordered(NODE_LOOKUP_CONCURRENCY_LIMIT)
                .filter_map(|opt_data| async { opt_data })
                .collect::<Vec<BlockSlotData>>()
                .await;

            debug!("from node: {} slots found", node_lookups.len());
            successfully_found_slots.extend(node_lookups);
        }

        if !successfully_found_slots.is_empty() {
            let num_successful = successfully_found_slots.len();
            match bulk_update_slots(db_pool, successfully_found_slots).await {
                Ok(rows_affected) => {
                    info!(
                        "bulk updated {} rows with new slots ({} successful lookups this chunk).",
                        rows_affected, num_successful
                    );
                }
                Err(e) => {
                    warn!(error = %e, "error during bulk update of slots");
                }
            }
        } else {
            debug!("no slots found for the current chunk of state_roots (neither DB nor node).");
        }

        overall_processed_count += chunk_size;
        progress.set_work_done(overall_processed_count);
        info!("slot backfill progress: {}", progress.get_progress_string());

        if chunk_size < DB_CHUNK_SIZE as u64 {
            info!("processed the last chunk of state_roots.");
            break;
        }
    }

    progress.set_work_done(total_work);
    info!(
        "beacon_block_slots backfill process finished. final progress: {}",
        progress.get_progress_string()
    );
}
