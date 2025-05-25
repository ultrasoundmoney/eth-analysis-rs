use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::balances::backfill::Granularity;
use crate::{
    beacon_chain::{node::BeaconNodeHttp, Slot, PECTRA_SLOT},
    units::GweiNewtype,
};

const DB_CHUNK_SIZE: usize = 1000;
const NODE_FETCH_CONCURRENCY_LIMIT: usize = 4;

async fn estimate_total_work_for_pending_deposits_sum(
    db_pool: &PgPool,
    granularity: &Granularity,
) -> u64 {
    let candidate_slots: Vec<i32> = sqlx::query_scalar!(
        r#"
        SELECT slot FROM beacon_blocks
        WHERE slot IS NOT NULL AND slot >= $1 AND pending_deposits_sum_gwei IS NULL
        "#,
        PECTRA_SLOT.0
    )
    .fetch_all(db_pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| row.expect("beacon block slot should not be null for backfill range"))
            .collect()
    })
    .unwrap_or_else(|e| {
        warn!(error = %e, "failed to fetch slots for work estimation in pending deposits backfill");
        Vec::new()
    });

    if candidate_slots.is_empty() {
        return 0;
    }

    let mut count = 0;
    for slot_val in candidate_slots {
        let slot_obj = Slot(slot_val);
        let should_process = match granularity {
            Granularity::Slot => true,
            Granularity::Epoch => slot_obj.is_first_of_epoch(),
            Granularity::Hour => slot_obj.is_first_of_hour(),
            Granularity::Day => slot_obj.is_first_of_day(),
        };
        if should_process {
            count += 1;
        }
    }
    count
}

#[derive(Debug)]
struct PendingDepositUpdateData {
    state_root: String,
    pending_deposits_sum: GweiNewtype,
}

#[derive(Debug, sqlx::FromRow)]
struct BlockToProcess {
    state_root: String,
    slot: i32,
}

async fn fetch_pending_deposits_for_state_root(
    beacon_node: &BeaconNodeHttp,
    state_root: String,
) -> Option<PendingDepositUpdateData> {
    match beacon_node.get_pending_deposits_sum(&state_root).await {
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

async fn bulk_update_pending_deposits(
    db_pool: &PgPool,
    updates: Vec<PendingDepositUpdateData>,
) -> sqlx::Result<u64> {
    if updates.is_empty() {
        return Ok(0);
    }

    let mut state_roots: Vec<String> = Vec::with_capacity(updates.len());
    let mut sums_gwei: Vec<i64> = Vec::with_capacity(updates.len());

    for update in updates {
        state_roots.push(update.state_root);
        sums_gwei.push(i64::from(update.pending_deposits_sum));
    }

    let rows_affected = sqlx::query!(
        r#"
        UPDATE beacon_blocks AS bb
        SET pending_deposits_sum_gwei = upd.sum_gwei
        FROM UNNEST($1::text[], $2::int8[]) AS upd(state_root, sum_gwei)
        WHERE bb.state_root = upd.state_root
        "#,
        &state_roots,
        &sums_gwei
    )
    .execute(db_pool)
    .await?
    .rows_affected();

    Ok(rows_affected)
}

pub async fn backfill_pending_deposits_sum(db_pool: &PgPool, granularity: &Granularity) {
    let beacon_node = BeaconNodeHttp::new();

    debug!("estimating total work for backfilling missing pending_deposits_sum_gwei (granularity: {:?})", granularity);
    let total_work = estimate_total_work_for_pending_deposits_sum(db_pool, granularity).await;
    if total_work == 0 {
        info!("no beacon_blocks rows found with missing pending_deposits_sum_gwei for post-pectra slots, matching granularity {:?}. nothing to do.", granularity);
        return;
    }
    debug!(
        "total beacon_blocks with missing pending_deposits_sum_gwei to process (matching granularity {:?}): {}",
        granularity, total_work
    );
    let mut progress = Progress::new("backfill-pending-deposits-sum", total_work);
    let mut processed_items_count: u64 = 0;

    loop {
        let blocks_to_consider: Vec<BlockToProcess> = sqlx::query!(
            r#"
            SELECT state_root, slot FROM beacon_blocks
            WHERE slot IS NOT NULL AND slot >= $1 AND pending_deposits_sum_gwei IS NULL
            ORDER BY slot DESC
            LIMIT $2
            "#,
            PECTRA_SLOT.0,
            DB_CHUNK_SIZE as i64
        )
        .fetch_all(db_pool)
        .await
        .map(|rows| rows.into_iter().map(|row| BlockToProcess {
            state_root: row.state_root,
            // this expect is temporary until we have all block slots backfilled and a not null constraint.
            slot: row.slot.expect("beacon block slot should not be null for backfill range"),
        }).collect())
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to fetch chunk of blocks for pending deposits sum backfill, ending early");
            Vec::new()
        });

        if blocks_to_consider.is_empty() {
            info!(
                "no more candidate blocks with missing pending_deposits_sum_gwei found by query."
            );
            break;
        }

        let num_considered_this_chunk = blocks_to_consider.len() as u64;

        let state_roots_to_process: Vec<String> = blocks_to_consider
            .into_iter()
            .filter_map(|block| {
                let slot_obj = Slot(block.slot);
                let should_process = match granularity {
                    Granularity::Slot => true,
                    Granularity::Epoch => slot_obj.is_first_of_epoch(),
                    Granularity::Hour => slot_obj.is_first_of_hour(),
                    Granularity::Day => slot_obj.is_first_of_day(),
                };
                if should_process {
                    Some(block.state_root)
                } else {
                    None
                }
            })
            .collect();

        if state_roots_to_process.is_empty() {
            debug!(
                "no state_roots matched granularity {:?} in this chunk of {} candidates. fetching next chunk.",
                granularity, num_considered_this_chunk
            );
            if num_considered_this_chunk < DB_CHUNK_SIZE as u64 {
                info!("processed the last chunk of candidate state_roots from db, none matched granularity {:?}.", granularity);
                break;
            }
            continue;
        }

        let actual_chunk_to_process_size = state_roots_to_process.len() as u64;
        debug!(
            "processing a new chunk of {} state_roots (filtered by {:?} from {} candidates) for pending deposits sum",
            actual_chunk_to_process_size, granularity, num_considered_this_chunk
        );

        let tasks =
            stream::iter(state_roots_to_process).map(|state_root| {
                let beacon_node_clone = beacon_node.clone();
                async move {
                    fetch_pending_deposits_for_state_root(&beacon_node_clone, state_root).await
                }
            });

        let results: Vec<Option<PendingDepositUpdateData>> = tasks
            .buffer_unordered(NODE_FETCH_CONCURRENCY_LIMIT)
            .collect()
            .await;

        let successful_updates: Vec<PendingDepositUpdateData> =
            results.into_iter().flatten().collect();

        if !successful_updates.is_empty() {
            let update_count = successful_updates.len();
            match bulk_update_pending_deposits(db_pool, successful_updates).await {
                Ok(rows_affected) => {
                    info!(
                        "bulk updated {} rows with new pending_deposits_sum_gwei ({} sums found).",
                        rows_affected, update_count
                    );
                }
                Err(e) => {
                    warn!(error = %e, "error during bulk update of pending_deposits_sum_gwei");
                }
            }
        }

        processed_items_count += actual_chunk_to_process_size;
        progress.set_work_done(processed_items_count);
        info!(
            "pending deposits sum backfill progress: {}",
            progress.get_progress_string()
        );

        if num_considered_this_chunk < DB_CHUNK_SIZE as u64 {
            info!("processed the last chunk of candidate state_roots from db for pending deposits sum (granularity: {:?}).", granularity);
            break;
        }
    }

    progress.set_work_done(total_work);
    info!(
        "pending_deposits_sum_gwei backfill process finished (granularity: {:?}). final progress: {}",
        granularity, progress.get_progress_string()
    );
}
