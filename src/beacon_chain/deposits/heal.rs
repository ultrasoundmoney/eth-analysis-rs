use anyhow::{Context, Result};
use pit_wall::Progress;
use sqlx::{PgExecutor, PgPool};
use tracing::{debug, info};

use crate::{
    beacon_chain::{
        blocks,
        node::{BeaconNode, BeaconNodeHttp},
        Slot,
    },
    units::GweiNewtype,
};

// Fetches the deposit_sum_aggregated of the latest block strictly before the given slot.
async fn get_aggregated_sum_of_latest_block_before_slot(
    executor: impl PgExecutor<'_>, // Changed to impl PgExecutor
    slot: Slot,
) -> Result<GweiNewtype> {
    if slot == Slot(0) {
        // No slots before genesis, so aggregated sum is 0.
        return Ok(GweiNewtype(0));
    }

    let result = sqlx::query!(
        r#"
        SELECT deposit_sum_aggregated
        FROM beacon_blocks
        WHERE slot < $1
        ORDER BY slot DESC
        LIMIT 1
        "#,
        slot.0
    )
    .fetch_optional(executor)
    .await?;

    match result {
        Some(row) => Ok(row.deposit_sum_aggregated.into()),
        None => {
            // No blocks found before the given slot (e.g., slot is very early).
            debug!(
                target_slot = %slot,
                "no blocks found before target_slot, initial aggregated deposit sum will be 0."
            );
            Ok(GweiNewtype(0))
        }
    }
}

async fn update_deposit_sums(
    executor: impl PgExecutor<'_>, // Changed to impl PgExecutor
    block_root: &str,
    slot: Slot,
    deposit_sum: GweiNewtype,
    deposit_sum_aggregated: GweiNewtype,
) -> Result<()> {
    debug!(%slot, %block_root, %deposit_sum, %deposit_sum_aggregated, "updating deposit sums in db");
    sqlx::query!(
        r#"
        UPDATE beacon_blocks
        SET
            deposit_sum = $1,
            deposit_sum_aggregated = $2
        WHERE
            block_root = $3
        "#,
        i64::from(deposit_sum),
        i64::from(deposit_sum_aggregated),
        block_root
    )
    .execute(executor)
    .await?;
    Ok(())
}

pub async fn heal_deposit_sums(db_pool: &PgPool, start_slot: Slot) -> Result<()> {
    info!(%start_slot, "starting deposit sum healing process");

    // Removed transaction, operations will be on db_pool directly or helpers taking impl PgExecutor
    let beacon_node = BeaconNodeHttp::new_from_env();

    let latest_slot_in_db = blocks::get_latest_beacon_block_slot(db_pool).await?;

    if start_slot > latest_slot_in_db {
        info!(
            %start_slot, %latest_slot_in_db,
            "start_slot is after the latest slot in the database. nothing to heal."
        );
        return Ok(());
    }

    let mut running_total_aggregated_sum =
        get_aggregated_sum_of_latest_block_before_slot(db_pool, start_slot).await?;

    info!(%start_slot, initial_running_agg_sum = %running_total_aggregated_sum, "established jumping-off point for aggregated deposits");

    let total_slots_to_process = (latest_slot_in_db.0 - start_slot.0 + 1) as u64;
    let mut progress = Progress::new("heal-deposit-sums", total_slots_to_process);

    for current_slot_val in start_slot.0..=latest_slot_in_db.0 {
        let current_slot = Slot(current_slot_val);

        match beacon_node.get_block_by_slot(current_slot).await? {
            Some(block) => {
                let deposit_sum_for_current_block = block.total_deposits_amount();

                // The aggregated sum for the current block is the running total + its own deposits.
                let aggregated_sum_for_current_block =
                    running_total_aggregated_sum + deposit_sum_for_current_block;

                let header = beacon_node
                    .get_header_by_slot(current_slot)
                    .await?
                    .with_context(|| {
                        format!(
                            "failed to get header for slot {current_slot} to find block_root for update"
                        )
                    })?;
                let block_root_of_current_block = header.root;

                debug!(
                    slot = %current_slot,
                    block_root = %block_root_of_current_block,
                    deposit_sum = %deposit_sum_for_current_block,
                    deposit_sum_aggregated = %aggregated_sum_for_current_block,
                    parent_agg_sum_used = %running_total_aggregated_sum,
                    "healed deposit sums for block"
                );

                update_deposit_sums(
                    db_pool,
                    &block_root_of_current_block,
                    current_slot,
                    deposit_sum_for_current_block,
                    aggregated_sum_for_current_block,
                )
                .await?;
                // After processing this block, its aggregated sum becomes the new running total.
                running_total_aggregated_sum = aggregated_sum_for_current_block;
            }
            None => {
                debug!(slot = %current_slot, "slot was skipped. running_total_aggregated_sum ({}) carries over.", running_total_aggregated_sum);
            }
        }

        progress.inc_work_done();
        if progress.work_done % 100 == 0 || progress.work_done == total_slots_to_process {
            info!("{}", progress.get_progress_string());
        }
    }

    // Removed transaction.commit()

    info!("deposit sum healing finished.");
    Ok(())
}
