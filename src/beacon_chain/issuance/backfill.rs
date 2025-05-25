use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, instrument, warn};

use crate::{
    beacon_chain::{
        issuance::{calc_issuance, store_issuance},
        node::{BeaconNode, BeaconNodeHttp},
        Slot, PECTRA_SLOT,
    },
    units::GweiNewtype,
};

const DB_CHUNK_SIZE: usize = 1000; // How many slots/hours to process in one DB query
const NODE_FETCH_CONCURRENCY_LIMIT: usize = 10; // How many concurrent requests to the beacon node
const SLOTS_PER_HOUR: i32 = 300; // 12 seconds per slot, 5 slots per minute, 300 slots per hour

#[derive(Debug)]
struct IssuanceBackfillData {
    slot: Slot,
    state_root: String,
    beacon_balances_sum: GweiNewtype,
    deposit_requests_running_total: GweiNewtype,
    pending_deposits_sum: GweiNewtype,
    withdrawals_running_total: GweiNewtype,
}

#[instrument(skip(db_pool, beacon_node), fields(slot = %slot))]
async fn fetch_issuance_data_for_slot(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
    slot: Slot,
) -> anyhow::Result<Option<IssuanceBackfillData>> {
    // Removed Pectra slot check here to allow backfilling earlier slots.
    // The pending_deposits_sum logic below correctly handles pre-Pectra cases.

    // 1. Get state_root and aggregated deposit/withdrawal sums from beacon_blocks table
    let block_info = sqlx::query!(
        r#"
        SELECT
            state_root,
            deposit_sum_aggregated,
            withdrawal_sum_aggregated
        FROM beacon_blocks
        WHERE slot = $1
        "#,
        slot.0
    )
    .fetch_optional(db_pool)
    .await?;

    let Some(block_row) = block_info else {
        debug!(%slot, "no block found in beacon_blocks table for slot, cannot backfill issuance");
        return Ok(None);
    };

    let state_root = block_row.state_root;
    let deposit_requests_running_total = GweiNewtype(block_row.deposit_sum_aggregated);
    let withdrawals_running_total = GweiNewtype(block_row.withdrawal_sum_aggregated.unwrap_or(0));

    // 2. Fetch validator balances sum
    let validator_balances = beacon_node.get_validator_balances(&state_root).await?;
    let Some(balances) = validator_balances else {
        warn!(%slot, %state_root, "beacon node reported no validator balances for state_root, skipping issuance calculation for this slot");
        return Ok(None);
    };
    let beacon_balances_sum = balances
        .iter()
        .fold(GweiNewtype(0), |acc, b| acc + b.balance);

    // 3. Fetch pending deposits sum
    let pending_deposits_sum = if slot < *PECTRA_SLOT {
        // Correctly sets to 0 for pre-Pectra slots.
        GweiNewtype(0)
    } else {
        match beacon_node.get_pending_deposits_sum(&state_root).await {
            Ok(Some(sum)) => sum,
            Ok(None) => {
                warn!(%slot, %state_root, "beacon node reported no pending deposits sum for state_root (post-pectra), using 0");
                GweiNewtype(0)
            }
            Err(e) => {
                warn!(%slot, %state_root, error = %e, "error fetching pending deposits sum, using 0");
                GweiNewtype(0)
            }
        }
    };

    Ok(Some(IssuanceBackfillData {
        slot,
        state_root,
        beacon_balances_sum,
        deposit_requests_running_total,
        pending_deposits_sum,
        withdrawals_running_total,
    }))
}

async fn get_slots_to_process_for_missing_issuance(
    db_pool: &PgPool,
    limit: i64,
) -> sqlx::Result<Vec<Slot>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            bb.slot AS "slot_to_process!"
        FROM beacon_blocks AS bb
        WHERE bb.slot % $1 = 0
        AND bb.slot >= $2 -- Pectra Slot check
        AND NOT EXISTS (
            SELECT 1
            FROM beacon_issuance AS bi
            WHERE bi.state_root = bb.state_root
        )
        ORDER BY bb.slot ASC
        LIMIT $3
        "#,
        SLOTS_PER_HOUR,
        PECTRA_SLOT.0,
        limit
    )
    .fetch_all(db_pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Slot(row.slot_to_process))
        .collect())
}

async fn estimate_total_missing_hours(db_pool: &PgPool) -> u64 {
    let count_opt: Option<Option<i64>> = sqlx::query_scalar!(
        r#"
        SELECT COUNT(DISTINCT bb.slot)
        FROM beacon_blocks AS bb
        WHERE bb.slot % $1 = 0
        AND bb.slot >= $2 -- Pectra Slot check
        AND NOT EXISTS (
            SELECT 1
            FROM beacon_issuance AS bi
            WHERE bi.state_root = bb.state_root
        )
        "#,
        SLOTS_PER_HOUR,
        PECTRA_SLOT.0
    )
    .fetch_one(db_pool)
    .await
    .ok();
    count_opt.flatten().unwrap_or(0).try_into().unwrap_or(0)
}

pub async fn backfill_missing_issuance(db_pool: &PgPool) {
    info!("starting backfill for missing hourly beacon issuance (post-Pectra only, using upsert)");
    let beacon_node = BeaconNodeHttp::new_from_env();

    let total_missing_representative_slots = estimate_total_missing_hours(db_pool).await;
    if total_missing_representative_slots == 0 {
        info!("no missing hourly issuance (post-Pectra) found. nothing to do.");
        return;
    }
    debug!(
        "total estimated representative slots (post-Pectra) with missing issuance to process: {}",
        total_missing_representative_slots
    );
    let mut progress = Progress::new(
        "backfill-missing-issuance",
        total_missing_representative_slots,
    );
    let mut overall_processed_count: u64 = 0;
    let mut successful_updates_count: u64 = 0;

    loop {
        let slots_to_process = match get_slots_to_process_for_missing_issuance(
            db_pool,
            DB_CHUNK_SIZE as i64,
        )
        .await
        {
            Ok(slots) => slots,
            Err(e) => {
                warn!(error = %e, "failed to fetch chunk of slots for missing issuance backfill, ending early");
                break;
            }
        };

        if slots_to_process.is_empty() {
            info!("no more slots (post-Pectra) with missing hourly issuance found to process.");
            break;
        }

        let chunk_size = slots_to_process.len() as u64;
        debug!(
            "processing a new chunk of {} slots (post-Pectra) for missing hourly issuance",
            chunk_size
        );

        let tasks = stream::iter(slots_to_process).map(|slot_to_process| {
            let db_pool_clone = db_pool.clone();
            let beacon_node_clone = beacon_node.clone();
            async move {
                fetch_issuance_data_for_slot(&db_pool_clone, &beacon_node_clone, slot_to_process)
                    .await
            }
        });

        let results: Vec<Result<Option<IssuanceBackfillData>, _>> = tasks
            .buffer_unordered(NODE_FETCH_CONCURRENCY_LIMIT)
            .collect()
            .await;

        let mut issuance_calculated_count = 0;
        for result in results {
            match result {
                Ok(Some(data)) => {
                    let issuance_gwei = calc_issuance(
                        &data.beacon_balances_sum,
                        &data.deposit_requests_running_total,
                        &data.pending_deposits_sum,
                        &data.withdrawals_running_total,
                    );
                    store_issuance(db_pool, &data.state_root, data.slot, &issuance_gwei).await;
                    issuance_calculated_count += 1;
                    successful_updates_count += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "error processing a slot during issuance backfill task");
                }
            }
        }

        if issuance_calculated_count > 0 {
            info!(
                "successfully calculated and stored issuance for {} slots (post-Pectra) in this chunk.",
                issuance_calculated_count
            );
        }

        overall_processed_count += chunk_size;
        progress.set_work_done(overall_processed_count);
        info!(
            "missing issuance backfill (post-Pectra) progress: {}",
            progress.get_progress_string()
        );

        if chunk_size < DB_CHUNK_SIZE as u64 {
            info!("processed the last chunk of slots (post-Pectra) for missing hourly issuance.");
            break;
        }
    }

    progress.set_work_done(total_missing_representative_slots);
    info!(
        "missing hourly issuance backfill (post-Pectra) process finished. successfully updated {} representative_slots. final progress: {}",
        successful_updates_count,
        progress.get_progress_string()
    );
}

async fn get_slots_to_process_for_range(
    db_pool: &PgPool,
    start_slot: Slot, // This will be the Pectra-adjusted start_slot
    end_slot: Slot,
    batch_offset: i64,
    batch_limit: i64,
) -> sqlx::Result<Vec<Slot>> {
    if start_slot > end_slot {
        return Ok(Vec::new());
    }
    let rows = sqlx::query!(
        r#"
        SELECT
            bb.slot AS "representative_slot!"
        FROM beacon_blocks AS bb
        WHERE bb.slot >= $1 AND bb.slot <= $2
          AND bb.slot % $3 = 0
        ORDER BY bb.slot ASC
        LIMIT $4 OFFSET $5
        "#,
        start_slot.0, // Already Pectra-adjusted by caller
        end_slot.0,
        SLOTS_PER_HOUR,
        batch_limit,
        batch_offset
    )
    .fetch_all(db_pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Slot(row.representative_slot))
        .collect())
}

async fn estimate_total_hours_in_range(
    db_pool: &PgPool,
    start_slot: Slot, // This will be the Pectra-adjusted start_slot
    end_slot: Slot,
) -> u64 {
    if start_slot > end_slot {
        return 0;
    }
    let count_opt: Option<Option<i64>> = sqlx::query_scalar!(
        r#"
        SELECT COUNT(DISTINCT bb.slot)
        FROM beacon_blocks AS bb
        WHERE bb.slot >= $1 AND bb.slot <= $2
          AND bb.slot % $3 = 0
        "#,
        start_slot.0, // Already Pectra-adjusted by caller
        end_slot.0,
        SLOTS_PER_HOUR
    )
    .fetch_one(db_pool)
    .await
    .ok();
    count_opt.flatten().unwrap_or(0).try_into().unwrap_or(0)
}

pub async fn backfill_slot_range_issuance(
    db_pool: &PgPool,
    start_slot_config: Slot,
    end_slot_config: Slot,
) {
    let effective_start_slot = start_slot_config; // Use user-defined start_slot directly

    let run_description = if effective_start_slot < *PECTRA_SLOT {
        format!(
            "includes pre-pectra slots (note: pending_deposits_sum is zero for slots before {})",
            *PECTRA_SLOT
        )
    } else {
        format!("for slots from {} onwards", *PECTRA_SLOT)
    };
    info!(
        %start_slot_config, %end_slot_config, %effective_start_slot,
        "starting backfill for beacon issuance in specified slot range [{}, {}] (one per hour, {}, using upsert)",
        effective_start_slot, end_slot_config, run_description
    );

    if effective_start_slot > end_slot_config {
        info!(
            "effective start slot ({}) is after end slot ({}), nothing to do.",
            effective_start_slot, end_slot_config
        );
        return;
    }

    let beacon_node = BeaconNodeHttp::new_from_env();

    let total_representative_slots_to_process =
        estimate_total_hours_in_range(db_pool, effective_start_slot, end_slot_config).await;
    if total_representative_slots_to_process == 0 {
        info!(%effective_start_slot, %end_slot_config, "no representative slots (slot % {} == 0) found in the specified slot range [{}, {}] to backfill issuance for. nothing to do.", SLOTS_PER_HOUR, effective_start_slot, end_slot_config);
        return;
    }
    debug!(
        "total estimated representative_slots in range [{}-{}] to process: {}",
        effective_start_slot, end_slot_config, total_representative_slots_to_process
    );
    let mut progress = Progress::new(
        "backfill-range-issuance",
        total_representative_slots_to_process,
    );
    let mut overall_processed_slots_in_db_chunks: u64 = 0;
    let mut successful_updates_count: u64 = 0;

    loop {
        let slots_to_process = match get_slots_to_process_for_range(
            db_pool,
            effective_start_slot, // Use adjusted slot
            end_slot_config,      // Original end slot
            overall_processed_slots_in_db_chunks as i64,
            DB_CHUNK_SIZE as i64,
        )
        .await
        {
            Ok(slots) => slots,
            Err(e) => {
                warn!(error = %e, "failed to fetch chunk of slots for range issuance backfill, ending early");
                break;
            }
        };

        if slots_to_process.is_empty() {
            info!(
                "no more slots in range [{}-{}] found to process for hourly issuance.",
                effective_start_slot, end_slot_config
            );
            break;
        }

        let current_chunk_size = slots_to_process.len() as u64;
        debug!(
            "processing a new chunk of {} slots for range [{}-{}] for hourly issuance",
            current_chunk_size, effective_start_slot, end_slot_config
        );

        let tasks = stream::iter(slots_to_process).map(|slot_to_process| {
            let db_pool_clone = db_pool.clone();
            let beacon_node_clone = beacon_node.clone();
            async move {
                fetch_issuance_data_for_slot(&db_pool_clone, &beacon_node_clone, slot_to_process)
                    .await
            }
        });

        let results: Vec<Result<Option<IssuanceBackfillData>, _>> = tasks
            .buffer_unordered(NODE_FETCH_CONCURRENCY_LIMIT)
            .collect()
            .await;

        let mut issuance_calculated_count_in_chunk = 0;
        for result in results {
            match result {
                Ok(Some(data)) => {
                    let issuance_gwei = calc_issuance(
                        &data.beacon_balances_sum,
                        &data.deposit_requests_running_total,
                        &data.pending_deposits_sum,
                        &data.withdrawals_running_total,
                    );
                    store_issuance(db_pool, &data.state_root, data.slot, &issuance_gwei).await;
                    issuance_calculated_count_in_chunk += 1;
                    successful_updates_count += 1;
                }
                Ok(None) => { /* fetch_issuance_data_for_slot already logs */ }
                Err(e) => {
                    warn!(error = %e, "error processing a slot during range issuance backfill task");
                }
            }
        }

        if issuance_calculated_count_in_chunk > 0 {
            info!(
                "successfully calculated and stored issuance for {} slots in this chunk for range [{}-{}].",
                issuance_calculated_count_in_chunk, effective_start_slot, end_slot_config
            );
        }

        overall_processed_slots_in_db_chunks += current_chunk_size;
        progress.set_work_done(successful_updates_count);
        info!(
            "range [{}-{}] issuance backfill progress: {}",
            effective_start_slot,
            end_slot_config,
            progress.get_progress_string()
        );

        if current_chunk_size < DB_CHUNK_SIZE as u64 {
            info!(
                "processed the last chunk of slots for range [{}-{}].",
                effective_start_slot, end_slot_config
            );
            break;
        }
    }

    progress.set_work_done(total_representative_slots_to_process);
    info!(
        "slot range [{}-{}] hourly issuance backfill process finished. successfully updated {} representative_slots. final progress: {}",
        effective_start_slot, end_slot_config, successful_updates_count,
        progress.get_progress_string()
    );
}
