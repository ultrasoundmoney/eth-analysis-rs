use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, error, info, warn};

use crate::{
    beacon_chain::{Slot, FIRST_POST_MERGE_SLOT},
    eth_supply::store,
};

const DB_CHUNK_SIZE: usize = 100; // Number of slots to fetch from DB in one go
const PROCESSING_CONCURRENCY_LIMIT: usize = 8; // Number of slots to process concurrently

/// Estimates the number of slots that have prerequisite data but are missing from eth_supply.
async fn estimate_verifiable_missing_slots_count(
    db_pool: &PgPool,
    start_slot_inclusive: Slot,
    potential_end_slot_inclusive: Slot,
) -> u64 {
    if start_slot_inclusive > potential_end_slot_inclusive {
        return 0;
    }
    sqlx::query_scalar!(
        r#"
        SELECT
            COUNT(DISTINCT bb.slot) AS "count!"
        FROM
            beacon_blocks bb
        JOIN
            beacon_states bs ON bb.state_root = bs.state_root
        JOIN
            execution_supply esupply ON bb.block_hash = esupply.block_hash
        LEFT JOIN
            eth_supply es ON bb.slot = es.balances_slot
        WHERE
            bb.slot >= $1
            AND bb.slot <= $2
            AND es.balances_slot IS NULL
        "#,
        start_slot_inclusive.0,
        potential_end_slot_inclusive.0
    )
    .fetch_one(db_pool)
    .await
    .map(|count: i64| count as u64)
    .unwrap_or_else(|e| {
        warn!(error = %e, "failed to estimate missing eth supply slots, assuming 0");
        0u64
    })
}

/// Fetches a chunk of slots that have prerequisite data but are missing from eth_supply.
async fn fetch_verifiable_missing_slots_chunk(
    db_pool: &PgPool,
    start_slot_inclusive: Slot,
    potential_end_slot_inclusive: Slot,
    limit: i64,
) -> sqlx::Result<Vec<Slot>> {
    if start_slot_inclusive > potential_end_slot_inclusive {
        return Ok(Vec::new());
    }
    let slot_numbers: Vec<i32> = sqlx::query_scalar!(
        r#"
        SELECT
            bb.slot AS "slot!"
        FROM
            beacon_blocks bb
        JOIN
            execution_supply esupply ON bb.block_hash = esupply.block_hash
        LEFT JOIN
            eth_supply es ON bb.slot = es.balances_slot
        WHERE
            bb.slot >= $1 -- current_start_slot_for_fetch
            AND bb.slot <= $2 -- overall_upper_bound_slot
            AND es.balances_slot IS NULL
        ORDER BY
            bb.slot ASC
        LIMIT $3;
        "#,
        start_slot_inclusive.0,
        potential_end_slot_inclusive.0,
        limit
    )
    .fetch_all(db_pool)
    .await?;

    Ok(slot_numbers.into_iter().map(Slot).collect())
}

/// Backfills missing eth_supply entries for slots that have all prerequisite data.
pub async fn backfill_eth_supply(db_pool: &PgPool, start_slot_override: Option<Slot>) {
    info!("starting eth supply backfill process");

    let last_known_good_slot = match store::get_last_stored_balances_slot(db_pool).await {
        Some(slot) => slot,
        None => {
            warn!("no execution balances have ever been stored, cannot determine range for eth supply backfill. skipping.");
            return;
        }
    };

    let first_slot_to_check = start_slot_override.unwrap_or(FIRST_POST_MERGE_SLOT);

    if first_slot_to_check > last_known_good_slot {
        info!(%first_slot_to_check, %last_known_good_slot, "first slot to check is after the last known good slot, nothing to backfill.");
        return;
    }

    debug!(%first_slot_to_check, %last_known_good_slot, "determined slot range for eth supply backfill");

    let total_work =
        estimate_verifiable_missing_slots_count(db_pool, first_slot_to_check, last_known_good_slot)
            .await;

    if total_work == 0 {
        info!(%first_slot_to_check, %last_known_good_slot, "no missing eth_supply entries found in the range where prerequisites are met. nothing to do.");
        return;
    }

    info!(
        "estimated {} missing eth_supply entries to backfill between {} and {}",
        total_work, first_slot_to_check, last_known_good_slot
    );

    let mut progress = Progress::new("backfill-eth-supply", total_work);
    let mut overall_processed_slots_count: u64 = 0;
    let mut successfully_stored_count: u64 = 0;

    let mut current_start_slot_for_fetch = first_slot_to_check;

    loop {
        if current_start_slot_for_fetch > last_known_good_slot {
            debug!("current start slot for fetch is beyond last known good slot, completing.");
            break;
        }

        let slots_to_process = match fetch_verifiable_missing_slots_chunk(
            db_pool,
            current_start_slot_for_fetch,
            last_known_good_slot,
            DB_CHUNK_SIZE as i64,
        )
        .await
        {
            Ok(slots) => slots,
            Err(e) => {
                error!(error = %e, "failed to fetch chunk of slots for eth supply backfill, ending early");
                break;
            }
        };

        if slots_to_process.is_empty() {
            info!("no more missing eth_supply slots found to process in the current range query.");
            break;
        }

        let chunk_size = slots_to_process.len() as u64;
        debug!(
            "processing a new chunk of {} slots for eth supply, starting from slot {}",
            chunk_size,
            slots_to_process.first().map_or(Slot(0), |s| *s)
        );

        let tasks = stream::iter(slots_to_process.clone()).map(|slot_to_process| {
            let pool_clone = db_pool.clone();
            async move {
                let mut conn = match pool_clone.acquire().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(%slot_to_process, error = %e, "failed to acquire db connection for slot");
                        return Err(anyhow::Error::from(e));
                    }
                };
                match store::store_supply_for_slot(&mut conn, slot_to_process).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!(%slot_to_process, error = %e, "error trying to store eth supply for slot");
                        Err(e)
                    }
                }
            }
        });

        let results: Vec<Result<(), anyhow::Error>> = tasks
            .buffer_unordered(PROCESSING_CONCURRENCY_LIMIT)
            .collect()
            .await;

        let mut current_chunk_success_count = 0;
        for result in results {
            if result.is_ok() {
                current_chunk_success_count += 1;
            }
        }
        successfully_stored_count += current_chunk_success_count;
        overall_processed_slots_count += chunk_size;
        progress.set_work_done(overall_processed_slots_count);

        info!(
            "eth supply backfill: {}/{} slots in chunk successfully processed (or skipped due to missing parts). progress: {}",
            current_chunk_success_count, chunk_size, progress.get_progress_string()
        );

        if let Some(last_slot_in_chunk) = slots_to_process.last() {
            current_start_slot_for_fetch = *last_slot_in_chunk + 1;
        } else {
            error!(
                "slots_to_process was not empty but failed to get last element, stopping backfill"
            );
            break;
        }
    }

    progress.set_work_done(total_work);
    info!(
        "eth supply backfill process finished. attempted to store for {} slots, {} were potentially new entries. final progress: {}",
        overall_processed_slots_count,
        successfully_stored_count,
        progress.get_progress_string()
    );
}
