use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{node::BeaconNodeHttp, PECTRA_SLOT},
    units::GweiNewtype,
};

const DB_CHUNK_SIZE: usize = 1000;
const NODE_FETCH_CONCURRENCY_LIMIT: usize = 4;

async fn estimate_total_missing_pending_deposits_sum(db_pool: &PgPool) -> u64 {
    sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) as "count!"
        FROM beacon_blocks
        WHERE slot IS NOT NULL AND slot >= $1 AND pending_deposits_sum_gwei IS NULL
        "#,
        PECTRA_SLOT.0 // PECTRA_SLOT is a Slot, .0 gives the i32 value
    )
    .fetch_one(db_pool)
    .await
    .unwrap_or(0)
    .try_into()
    .unwrap_or(0)
}

#[derive(Debug)]
struct PendingDepositUpdateData {
    state_root: String,
    pending_deposits_sum: GweiNewtype,
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

pub async fn backfill_pending_deposits_sum(db_pool: &PgPool) {
    let beacon_node = BeaconNodeHttp::new();

    debug!("estimating total work for backfilling missing pending_deposits_sum_gwei");
    let total_work = estimate_total_missing_pending_deposits_sum(db_pool).await;
    if total_work == 0 {
        info!("no beacon_blocks rows found with missing pending_deposits_sum_gwei for post-Pectra slots. nothing to do.");
        return;
    }
    debug!(
        "total beacon_blocks with missing pending_deposits_sum_gwei to process: {}",
        total_work
    );
    let mut progress = Progress::new("backfill-pending-deposits-sum", total_work);
    let mut overall_processed_count: u64 = 0;

    loop {
        let state_roots_to_process: Vec<String> = sqlx::query_scalar!(
            r#"
            SELECT state_root FROM beacon_blocks
            WHERE slot IS NOT NULL AND slot >= $1 AND pending_deposits_sum_gwei IS NULL
            ORDER BY slot DESC
            LIMIT $2
            "#,
            PECTRA_SLOT.0,
            DB_CHUNK_SIZE as i64
        )
        .fetch_all(db_pool)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to fetch chunk of state_roots for pending deposits sum backfill, ending early");
            Vec::new()
        });

        if state_roots_to_process.is_empty() {
            info!("no more state_roots with missing pending_deposits_sum_gwei found to process.");
            break;
        }

        let chunk_size = state_roots_to_process.len() as u64;
        debug!(
            "processing a new chunk of {} state_roots for pending deposits sum",
            chunk_size
        );

        let tasks =
            stream::iter(state_roots_to_process).map(|state_root| {
                let beacon_node_clone = beacon_node.clone(); // Clone for each async task
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

        overall_processed_count += chunk_size;
        progress.set_work_done(overall_processed_count);
        info!(
            "pending deposits sum backfill progress: {}",
            progress.get_progress_string()
        );

        if chunk_size < DB_CHUNK_SIZE as u64 {
            info!("processed the last chunk of state_roots for pending deposits sum.");
            break;
        }
    }

    progress.set_work_done(total_work);
    info!(
        "pending_deposits_sum_gwei backfill process finished. final progress: {}",
        progress.get_progress_string()
    );
}
