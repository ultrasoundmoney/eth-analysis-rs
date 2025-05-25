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

async fn get_beacon_balances_sum_from_db(
    db_pool: &PgPool,
    state_root: &str,
) -> sqlx::Result<Option<GweiNewtype>> {
    // Fetches the pre-calculated sum of all validator balances (in Gwei)
    // from the beacon_validators_balance table for a given state_root.
    let gwei_opt: Option<i64> = sqlx::query_scalar!(
        r#"
        SELECT gwei
        FROM beacon_validators_balance
        WHERE state_root = $1
        "#,
        state_root
    )
    .fetch_optional(db_pool) // It's optional as an entry might not exist for the state_root
    .await?;

    Ok(gwei_opt.map(GweiNewtype))
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
    let beacon_balances_sum = match get_beacon_balances_sum_from_db(db_pool, &state_root).await {
        Ok(Some(sum_from_db)) => {
            debug!(%slot, %state_root, "found beacon_balances_sum in db (beacon_validators_balance table)");
            sum_from_db
        }
        Ok(None) => {
            debug!(%slot, %state_root, "beacon_balances_sum not found in db (beacon_validators_balance table), fetching from beacon node");
            // Fetch from node if not in DB
            let validator_balances_from_node =
                beacon_node.get_validator_balances(&state_root).await?;
            match validator_balances_from_node {
                Some(balances) => balances
                    .iter()
                    .fold(GweiNewtype(0), |acc, b| acc + b.balance),
                None => {
                    warn!(%slot, %state_root, "beacon node reported no validator balances for state_root (after db miss), skipping issuance calculation for this slot");
                    return Ok(None);
                }
            }
        }
        Err(e) => {
            warn!(error = %e, %slot, %state_root, "failed to query beacon_balances_sum from db (beacon_validators_balance table), fetching from beacon node as fallback");
            // Fetch from node as fallback on DB error
            let validator_balances_from_node =
                beacon_node.get_validator_balances(&state_root).await?;
            match validator_balances_from_node {
                Some(balances) => balances
                    .iter()
                    .fold(GweiNewtype(0), |acc, b| acc + b.balance),
                None => {
                    warn!(%slot, %state_root, "beacon node reported no validator balances for state_root (after db error), skipping issuance calculation for this slot");
                    return Ok(None);
                }
            }
        }
    };

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
    limit: i64, // This limit might fetch more than needed before filtering
) -> sqlx::Result<Vec<Slot>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            bb.slot AS "slot_to_process!"
        FROM beacon_blocks AS bb
        WHERE bb.slot >= $1 -- Pectra Slot check
        AND NOT EXISTS (
            SELECT 1
            FROM beacon_issuance AS bi
            WHERE bi.state_root = bb.state_root
        )
        ORDER BY bb.slot ASC
        LIMIT $2 -- Fetch a chunk, will filter for is_first_of_hour in code
        "#,
        PECTRA_SLOT.0,
        limit // Use the original limit, filtering happens next
    )
    .fetch_all(db_pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Slot(row.slot_to_process))
        .filter(|slot| slot.is_first_of_hour()) // Filter here
        .collect())
}

async fn estimate_total_missing_hours(db_pool: &PgPool) -> u64 {
    let candidate_slots: Vec<i32> = sqlx::query_scalar!(
        r#"
        SELECT DISTINCT bb.slot AS "slot!"
        FROM beacon_blocks AS bb
        WHERE bb.slot >= $1 -- Pectra Slot check
        AND bb.slot IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM beacon_issuance AS bi
            WHERE bi.state_root = bb.state_root
        )
        "#,
        PECTRA_SLOT.0
    )
    .fetch_all(db_pool)
    .await
    .unwrap_or_else(|e| {
        warn!(
            "failed to fetch slots for missing hours estimation: {:?}",
            e
        );
        Vec::new()
    });

    candidate_slots
        .into_iter()
        .map(Slot)
        .filter(|slot| slot.is_first_of_hour())
        .count() as u64
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
    start_slot: Slot,  // This will be the Pectra-adjusted start_slot
    end_slot: Slot,    // This will be the dynamically fetched highest slot
    batch_offset: i64, // Offset will apply to the pre-filtered list
    batch_limit: i64,  // Limit will apply to the pre-filtered list
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
        -- Removed: AND bb.slot % $3 = 0 (filtering will be done in Rust code)
        ORDER BY bb.slot ASC
        LIMIT $3 OFFSET $4 -- Apply limit and offset to the broader set
        "#,
        start_slot.0, // Already Pectra-adjusted by caller
        end_slot.0,
        // SLOTS_PER_HOUR, -- No longer needed for SQL query
        batch_limit,  // Limit for the broader fetch
        batch_offset  // Offset for the broader fetch
    )
    .fetch_all(db_pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Slot(row.representative_slot))
        .filter(|slot| slot.is_first_of_hour()) // Filter here
        .collect())
}

async fn estimate_total_hours_in_range(
    db_pool: &PgPool,
    start_slot: Slot, // This will be the Pectra-adjusted start_slot
    end_slot: Slot,   // This will be the dynamically fetched highest slot
) -> u64 {
    if start_slot > end_slot {
        return 0;
    }
    let candidate_slots_options: Vec<Option<i32>> = sqlx::query_scalar!(
        r#"
        SELECT DISTINCT bb.slot
        FROM beacon_blocks AS bb
        WHERE bb.slot >= $1 AND bb.slot <= $2
        "#,
        start_slot.0,
        end_slot.0
    )
    .fetch_all(db_pool)
    .await
    .unwrap_or_else(|e| {
        warn!("failed to fetch slots for range hours estimation: {:?}", e);
        Vec::new()
    });

    let candidate_slots: Vec<i32> = candidate_slots_options
        .into_iter()
        .flatten() // Converts Vec<Option<i32>> to Vec<i32> by removing Nones
        .collect();

    candidate_slots
        .into_iter()
        .map(Slot)
        .filter(|slot| slot.is_first_of_hour())
        .count() as u64
}

async fn get_highest_beacon_block_slot(db_pool: &PgPool) -> sqlx::Result<Option<Slot>> {
    let max_slot_opt: Option<i32> = sqlx::query_scalar!(
        r#"
        SELECT MAX(slot) FROM beacon_blocks
        "#
    )
    .fetch_optional(db_pool)
    .await?
    .flatten(); // Option<Option<i32>> to Option<i32>

    Ok(max_slot_opt.map(Slot))
}

pub async fn backfill_slot_range_issuance(
    db_pool: &PgPool,
    start_slot_config: Slot,
    // end_slot_config: Slot, // Removed
) {
    let effective_start_slot = start_slot_config; // Use user-defined start_slot directly

    let Some(highest_slot_in_db) = get_highest_beacon_block_slot(db_pool).await.unwrap_or_else(|e| {
        warn!(error = %e, "failed to get highest slot from beacon_blocks, cannot proceed with range backfill");
        None
    }) else {
        info!("no blocks found in beacon_blocks, cannot determine range for backfill. nothing to do.");
        return;
    };

    let end_slot_effective = highest_slot_in_db;

    let run_description = if effective_start_slot < *PECTRA_SLOT {
        format!(
            "includes pre-pectra slots (note: pending_deposits_sum is zero for slots before {}) up to db tip: {}",
            *PECTRA_SLOT, end_slot_effective
        )
    } else {
        format!(
            "for slots from {} onwards up to db tip: {}",
            *PECTRA_SLOT, end_slot_effective
        )
    };
    info!(
        %start_slot_config, determined_end_slot = %end_slot_effective, %effective_start_slot,
        "starting backfill for beacon issuance in specified slot range [{}, {}] (one per hour, {}, using upsert)",
        effective_start_slot, end_slot_effective, run_description
    );

    if effective_start_slot > end_slot_effective {
        info!(
            "effective start slot ({}) is after determined end slot ({}), nothing to do.",
            effective_start_slot, end_slot_effective
        );
        return;
    }

    let beacon_node = BeaconNodeHttp::new_from_env();

    let total_representative_slots_to_process =
        estimate_total_hours_in_range(db_pool, effective_start_slot, end_slot_effective).await;
    if total_representative_slots_to_process == 0 {
        info!(%effective_start_slot, %end_slot_effective, "no representative slots (slot % {} == 0) found in the specified slot range [{}, {}] to backfill issuance for. nothing to do.", SLOTS_PER_HOUR, effective_start_slot, end_slot_effective);
        return;
    }
    debug!(
        "total estimated representative_slots in range [{}-{}] to process: {}",
        effective_start_slot, end_slot_effective, total_representative_slots_to_process
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
            end_slot_effective,   // Use determined end slot
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
                effective_start_slot, end_slot_effective
            );
            break;
        }

        let current_chunk_size = slots_to_process.len() as u64;
        debug!(
            "processing a new chunk of {} slots for range [{}-{}] for hourly issuance",
            current_chunk_size, effective_start_slot, end_slot_effective
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
                issuance_calculated_count_in_chunk, effective_start_slot, end_slot_effective
            );
        }

        overall_processed_slots_in_db_chunks += current_chunk_size;
        progress.set_work_done(successful_updates_count);
        info!(
            "range [{}-{}] issuance backfill progress: {}",
            effective_start_slot,
            end_slot_effective,
            progress.get_progress_string()
        );

        if current_chunk_size < DB_CHUNK_SIZE as u64 {
            info!(
                "processed the last chunk of slots for range [{}-{}].",
                effective_start_slot, end_slot_effective
            );
            break;
        }
    }

    progress.set_work_done(total_representative_slots_to_process);
    info!(
        "slot range [{}-{}] hourly issuance backfill process finished. successfully updated {} representative_slots. final progress: {}",
        effective_start_slot, end_slot_effective, successful_updates_count,
        progress.get_progress_string()
    );
}
