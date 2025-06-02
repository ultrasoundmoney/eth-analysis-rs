use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, instrument, warn};

use crate::{
    beacon_chain::{
        blocks,
        issuance::{calc_issuance, store_issuance},
        node::{BeaconNode, BeaconNodeHttp},
        Slot, PECTRA_SLOT,
    },
    units::GweiNewtype,
};

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

    // 1. Get state_root and other sums from beacon_blocks table
    let block_info = sqlx::query!(
        r#"
        SELECT
            state_root,
            deposit_sum_aggregated,
            withdrawal_sum_aggregated,
            pending_deposits_sum_gwei
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

    // 2. Fetch validator balances sum ONLY from DB
    let beacon_balances_sum = match get_beacon_balances_sum_from_db(db_pool, &state_root).await {
        Ok(Some(sum_from_db)) => {
            debug!(%slot, %state_root, "found beacon_balances_sum in db (beacon_validators_balance table)");
            sum_from_db
        }
        Ok(None) => {
            warn!(%slot, %state_root, "beacon_balances_sum not found in db (beacon_validators_balance table) for state_root, skipping issuance calculation for this slot");
            return Ok(None);
        }
        Err(e) => {
            warn!(error = %e, %slot, %state_root, "failed to query beacon_balances_sum from db (beacon_validators_balance table), skipping issuance calculation for this slot");
            return Ok(None);
        }
    };

    // 3. Fetch pending deposits sum
    let pending_deposits_sum = if slot < *PECTRA_SLOT {
        // Correctly sets to 0 for pre-Pectra slots.
        GweiNewtype(0)
    } else {
        // For post-Pectra, try DB first
        if let Some(sum_gwei_db) = block_row.pending_deposits_sum_gwei {
            debug!(%slot, %state_root, "found pending_deposits_sum in db (beacon_blocks table)");
            GweiNewtype(sum_gwei_db)
        } else {
            // Fallback to beacon node if not in DB
            debug!(%slot, %state_root, "pending_deposits_sum not found in db (beacon_blocks table for post-pectra slot), fetching from beacon node");
            match beacon_node.pending_deposits_sum(&state_root).await {
                Ok(Some(sum_node)) => {
                    debug!(%slot, %state_root, pending_deposits_sum = %sum_node, "pending deposits sum found via beacon node (after db miss)");
                    sum_node
                }
                Ok(None) => {
                    warn!(%slot, %state_root, "beacon node reported no pending deposits sum for state_root (post-pectra, after db miss), using 0");
                    GweiNewtype(0)
                }
                Err(e) => {
                    warn!(%slot, %state_root, error = %e, "error fetching pending deposits sum from beacon node (post-pectra, after db miss), using 0");
                    GweiNewtype(0)
                }
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

async fn get_slots_to_process_for_missing_issuance(db_pool: &PgPool) -> sqlx::Result<Vec<Slot>> {
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
        "#,
        PECTRA_SLOT.0,
    )
    .fetch_all(db_pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Slot(row.slot_to_process))
        .filter(|slot| slot.is_first_of_hour()) // Filter here
        .collect())
}

pub async fn backfill_missing_issuance(db_pool: &PgPool) -> anyhow::Result<()> {
    info!("starting backfill for missing hourly beacon issuance (post-Pectra only, using upsert)");
    let beacon_node = BeaconNodeHttp::new_from_env();

    // Get all representative slots to process at once
    let slots_to_process = match get_slots_to_process_for_missing_issuance(db_pool).await {
        Ok(slots) => slots,
        Err(e) => {
            warn!(error = %e, "failed to fetch slots for missing issuance backfill, ending early");
            return Ok(());
        }
    };

    if slots_to_process.is_empty() {
        info!("no missing hourly issuance (post-Pectra) found. nothing to do.");
        return Ok(());
    }

    let total_missing_representative_slots = slots_to_process.len() as u64;
    debug!(
        "total estimated representative slots (post-Pectra) with missing issuance to process: {}",
        total_missing_representative_slots
    );

    let mut progress = Progress::new(
        "backfill-missing-issuance",
        total_missing_representative_slots,
    );
    let mut successful_updates_count: u64 = 0;

    debug!(
        "processing {} slots (post-Pectra) for missing hourly issuance sequentially",
        total_missing_representative_slots
    );

    let mut issuance_calculated_this_run = 0;
    // Process slots sequentially
    for slot_to_process in slots_to_process {
        match fetch_issuance_data_for_slot(db_pool, &beacon_node, slot_to_process).await {
            Ok(Some(data)) => {
                let issuance_gwei = calc_issuance(
                    &data.beacon_balances_sum,
                    &data.deposit_requests_running_total,
                    &data.pending_deposits_sum,
                    &data.withdrawals_running_total,
                );
                store_issuance(db_pool, &data.state_root, data.slot, &issuance_gwei).await;
                issuance_calculated_this_run += 1;
                successful_updates_count += 1;
                progress.set_work_done(successful_updates_count);
            }
            Ok(None) => { /* fetch_issuance_data_for_slot already logs */ }
            Err(e) => {
                warn!(error = %e, slot = %slot_to_process, "error processing a slot during missing issuance backfill task");
            }
        }
    }

    if issuance_calculated_this_run > 0 {
        info!(
            "successfully calculated and stored issuance for {} slots (post-Pectra).",
            issuance_calculated_this_run
        );
    }

    info!(
        "missing issuance backfill (post-Pectra) progress: {}",
        progress.get_progress_string()
    );

    progress.set_work_done(successful_updates_count); // Ensure final progress is accurate using successful_updates_count
    info!(
        "missing hourly issuance backfill (post-Pectra) process finished. successfully updated {} representative_slots. final progress: {}",
        successful_updates_count, // Use successful_updates_count in the log
        progress.get_progress_string()
    );

    Ok(())
}

async fn get_slots_to_process_for_range(
    db_pool: &PgPool,
    start_slot: Slot,
    end_slot: Slot,
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
        ORDER BY bb.slot ASC
        "#,
        start_slot.0,
        end_slot.0,
    )
    .fetch_all(db_pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| Slot(row.representative_slot))
        .filter(|slot| slot.is_first_of_hour()) // Filter here
        .collect())
}

pub async fn backfill_slot_range_issuance(
    db_pool: &PgPool,
    start_slot_config: Slot,
) -> anyhow::Result<()> {
    let highest_slot_in_db = blocks::get_latest_beacon_block_slot(db_pool).await?;

    let run_description = if start_slot_config < *PECTRA_SLOT {
        format!(
            "includes pre-pectra slots (note: pending_deposits_sum is zero for slots before {}) up to db tip: {}",
            *PECTRA_SLOT, highest_slot_in_db
        )
    } else {
        format!(
            "for slots from {} onwards up to db tip: {}",
            *PECTRA_SLOT, highest_slot_in_db
        )
    };
    info!(
        %start_slot_config, determined_end_slot = %highest_slot_in_db,
        "starting backfill for beacon issuance in specified slot range [{}, {}] (one per hour, {}, using upsert)",
        start_slot_config, highest_slot_in_db, run_description
    );

    if start_slot_config > highest_slot_in_db {
        info!(
            "start slot ({}) is after determined end slot ({}), nothing to do.",
            start_slot_config, highest_slot_in_db
        );
        return Ok(());
    }

    let beacon_node = BeaconNodeHttp::new_from_env();

    // Get all representative slots to process at once
    let slots_to_process = match get_slots_to_process_for_range(
        db_pool,
        start_slot_config,
        highest_slot_in_db,
    )
    .await
    {
        Ok(slots) => slots,
        Err(e) => {
            warn!(error = %e, "failed to fetch slots for range issuance backfill, ending early");
            return Ok(());
        }
    };

    if slots_to_process.is_empty() {
        info!(
            %start_slot_config, %highest_slot_in_db,
            "no representative slots (is_first_of_hour) found in the specified slot range [{}, {}] to backfill issuance for. nothing to do.",
            start_slot_config, highest_slot_in_db
        );
        return Ok(());
    }

    let total_representative_slots_to_process = slots_to_process.len() as u64;
    debug!(
        "total representative_slots in range [{}-{}] to process: {}",
        start_slot_config, highest_slot_in_db, total_representative_slots_to_process
    );

    let mut progress = Progress::new(
        "backfill-range-issuance",
        total_representative_slots_to_process,
    );
    let mut successful_updates_count: u64 = 0;

    debug!(
        "processing {} slots for range [{}-{}] for hourly issuance sequentially",
        total_representative_slots_to_process, start_slot_config, highest_slot_in_db
    );

    let mut issuance_calculated_count_total = 0;
    // Process slots sequentially
    for slot_to_process in slots_to_process {
        match fetch_issuance_data_for_slot(db_pool, &beacon_node, slot_to_process).await {
            Ok(Some(data)) => {
                let issuance_gwei = calc_issuance(
                    &data.beacon_balances_sum,
                    &data.deposit_requests_running_total,
                    &data.pending_deposits_sum,
                    &data.withdrawals_running_total,
                );
                store_issuance(db_pool, &data.state_root, data.slot, &issuance_gwei).await;
                issuance_calculated_count_total += 1;
                successful_updates_count += 1;
                progress.set_work_done(successful_updates_count);
            }
            Ok(None) => { /* fetch_issuance_data_for_slot already logs */ }
            Err(e) => {
                warn!(error = %e, slot = %slot_to_process, "error processing a slot during range issuance backfill task");
            }
        }
    }

    if issuance_calculated_count_total > 0 {
        info!(
            "successfully calculated and stored issuance for {} slots in range [{}-{}].",
            issuance_calculated_count_total, start_slot_config, highest_slot_in_db
        );
    }

    info!(
        "range [{}-{}] issuance backfill progress: {}",
        start_slot_config,
        highest_slot_in_db,
        progress.get_progress_string()
    );

    progress.set_work_done(successful_updates_count); // Ensure progress is final
    info!(
        "slot range [{}-{}] hourly issuance backfill process finished. successfully updated {} representative_slots. final progress: {}",
        start_slot_config, highest_slot_in_db, successful_updates_count,
        progress.get_progress_string()
    );

    Ok(())
}
