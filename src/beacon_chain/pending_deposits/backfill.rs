use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::balances::backfill::Granularity;
use crate::{
    beacon_chain::{node::BeaconNodeHttp, BeaconNode, Slot, PECTRA_SLOT},
    units::GweiNewtype,
};

// const DB_SLOT_FETCH_CHUNK_SIZE: i64 = 128; // REMOVED
const NODE_FETCH_CONCURRENCY_LIMIT: usize = 8;

async fn get_highest_beacon_block_slot(db_pool: &PgPool) -> sqlx::Result<Option<Slot>> {
    let max_slot_opt: Option<i32> = sqlx::query_scalar!(
        r#"
        SELECT MAX(slot) FROM beacon_blocks
        "#
    )
    .fetch_optional(db_pool)
    .await?
    .flatten();

    Ok(max_slot_opt.map(Slot))
}

// REMOVED estimate_total_work_for_pending_deposits_sum function
// async fn estimate_total_work_for_pending_deposits_sum(...) { ... }

#[derive(Debug)]
struct PendingDepositUpdateData {
    state_root: String,
    pending_deposits_sum: GweiNewtype,
}

#[derive(Debug, sqlx::FromRow)]
struct BlockToProcess {
    state_root: String,
    slot: Option<i32>,
}

async fn fetch_pending_deposits_for_state_root(
    beacon_node: &BeaconNodeHttp,
    state_root: String,
) -> Option<PendingDepositUpdateData> {
    match beacon_node.pending_deposits_sum(&state_root).await {
        Ok(Some(sum)) => {
            debug!(%state_root, pending_deposits_sum = %sum, "pending deposits sum found via beacon node");
            Some(PendingDepositUpdateData {
                state_root,
                pending_deposits_sum: sum,
            })
        }
        Ok(None) => {
            warn!(%state_root, "beacon node reported no pending deposits sum for state_root");
            None
        }
        Err(e) => {
            warn!(%state_root, error = %e, "error fetching pending deposits sum from beacon node");
            None
        }
    }
}

async fn update_single_pending_deposit_sum(
    db_pool: &PgPool,
    state_root: &str,
    pending_deposits_sum: GweiNewtype,
) -> sqlx::Result<()> {
    let sum_gwei = i64::from(pending_deposits_sum);
    sqlx::query!(
        r#"
        UPDATE beacon_blocks
        SET pending_deposits_sum_gwei = $1
        WHERE state_root = $2
        "#,
        sum_gwei,
        state_root
    )
    .execute(db_pool)
    .await
    .map(|_| ())
}

// Helper function to process a single state root: fetch its pending deposits sum and update the DB.
async fn process_single_state_root_and_update(
    beacon_node: &BeaconNodeHttp,
    db_pool: &PgPool,
    state_root_to_process: String,
) {
    let update_data =
        match fetch_pending_deposits_for_state_root(beacon_node, state_root_to_process).await {
            Some(data) => data,
            None => {
                // fetch_pending_deposits_for_state_root already logs issues
                return;
            }
        };

    if let Err(e) = update_single_pending_deposit_sum(
        db_pool,
        &update_data.state_root,
        update_data.pending_deposits_sum,
    )
    .await
    {
        warn!(
            state_root = %update_data.state_root,
            error = %e,
            "error updating pending deposits sum for state root"
        );
        // No explicit false return, function just ends
    }
    // No explicit true return, function just ends
}

pub async fn backfill_pending_deposits_sum(
    db_pool: &PgPool,
    granularity: &Granularity,
    start_slot_override: Option<Slot>,
    end_slot_override: Option<Slot>,
) {
    let beacon_node = BeaconNodeHttp::new_from_env();

    let effective_start_slot = start_slot_override
        .unwrap_or(*PECTRA_SLOT)
        .max(*PECTRA_SLOT);
    let mut effective_end_slot = end_slot_override;

    if effective_end_slot.is_none() {
        // If no end_slot_override, use highest slot in DB
        match get_highest_beacon_block_slot(db_pool).await {
            Ok(Some(slot)) => effective_end_slot = Some(slot),
            Ok(None) => {
                info!(
                    "no beacon blocks found in db, cannot proceed with pending deposits sum backfill."
                );
                return;
            }
            Err(e) => {
                warn!(error = %e, "failed to get highest slot from db, cannot proceed with pending deposits sum backfill.");
                return;
            }
        }
    }

    // Ensure effective_end_slot is not before effective_start_slot
    if let Some(end_s) = effective_end_slot {
        if end_s < effective_start_slot {
            info!(
                start_slot = %effective_start_slot,
                end_slot = %end_s,
                "effective end slot is before effective start slot, nothing to backfill."
            );
            return;
        }
    } else {
        // This case should ideally not be reached if logic above is correct, but as a safeguard.
        info!("effective end slot could not be determined, cannot backfill.");
        return;
    }

    if effective_start_slot >= effective_end_slot.unwrap_or(Slot(i32::MAX)) {
        // unwrap_or for comparison if effective_end_slot is still None (should not happen)
        info!(start_slot = %effective_start_slot, end_slot = ?effective_end_slot, "start slot is not before end slot, nothing to backfill.");
        return;
    }

    debug!(
        granularity = ?granularity,
        effective_start_slot = %effective_start_slot,
        effective_end_slot = %effective_end_slot.unwrap(), // Safe to unwrap due to checks above
        "fetching all candidate blocks for pending deposits sum backfill."
    );

    let all_candidate_blocks: Vec<BlockToProcess> = match sqlx::query_as!(
        BlockToProcess,
        r#"
        SELECT state_root, slot FROM beacon_blocks
        WHERE slot >= $1 AND slot <= $2 AND pending_deposits_sum_gwei IS NULL
        ORDER BY slot ASC
        "#,
        effective_start_slot.0,
        effective_end_slot.unwrap().0 // Safe to unwrap
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(blocks) => blocks,
        Err(e) => {
            warn!(
                error = %e,
                "failed to fetch all candidate blocks for pending deposits sum backfill. stopping."
            );
            return;
        }
    };

    if all_candidate_blocks.is_empty() {
        info!(
            granularity = ?granularity,
            start_slot = %effective_start_slot,
            end_slot = %effective_end_slot.unwrap(), // Safe to unwrap
            "no candidate blocks found with missing pending_deposits_sum_gwei in the specified range. nothing to do."
        );
        return;
    }

    let blocks_to_process_filtered: Vec<BlockToProcess> = all_candidate_blocks
        .into_iter()
        .filter(|block| {
            let slot_obj = Slot(
                block
                    .slot
                    .expect("slot should be present in BlockToProcess for filtering"),
            );
            match granularity {
                Granularity::Slot => true,
                Granularity::Epoch => slot_obj.is_first_of_epoch(),
                Granularity::Hour => slot_obj.is_first_of_hour(),
                Granularity::Day => slot_obj.is_first_of_day(),
            }
        })
        .collect();

    if blocks_to_process_filtered.is_empty() {
        info!(
            granularity = ?granularity,
            start_slot = %effective_start_slot,
            end_slot = %effective_end_slot.unwrap(), // Safe to unwrap
            "no blocks matched the specified granularity after fetching. nothing to do."
        );
        return;
    }

    let total_work = blocks_to_process_filtered.len() as u64;
    let mut progress = Progress::new("backfill-pending-deposits-sum", total_work);

    debug!(
        "processing {} state_roots (filtered by {:?}) for pending deposits sum.",
        total_work, granularity
    );

    // Extract just the state roots for processing
    let state_roots_to_process_this_run: Vec<String> = blocks_to_process_filtered
        .into_iter()
        .map(|block| block.state_root)
        .collect();

    stream::iter(state_roots_to_process_this_run)
        .map(|state_root_for_task| {
            let beacon_node_clone = beacon_node.clone();
            let db_pool_clone = db_pool.clone();
            async move {
                process_single_state_root_and_update(
                    &beacon_node_clone,
                    &db_pool_clone,
                    state_root_for_task,
                )
                .await;
            }
        })
        .buffer_unordered(NODE_FETCH_CONCURRENCY_LIMIT)
        .collect::<Vec<()>>()
        .await;

    progress.set_work_done(total_work);

    info!(
        granularity = ?granularity,
        processed_items = total_work,
        total_estimated_at_start = total_work,
        "pending_deposits_sum_gwei backfill for {:?} finished. final progress: {}",
        granularity, progress.get_progress_string()
    );
}
