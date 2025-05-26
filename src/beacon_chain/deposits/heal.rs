use anyhow::{Context, Result};
use pit_wall::Progress;
use sqlx::{PgExecutor, PgPool};
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{node::BeaconNodeHttp, BeaconNode, Slot},
    units::GweiNewtype,
};

async fn get_latest_slot_in_db(executor: impl PgExecutor<'_>) -> Result<Option<Slot>> {
    let row = sqlx::query!(
        r#"
        SELECT MAX(bs.slot) AS "max_slot?: Slot"
        FROM beacon_blocks bb
        JOIN beacon_states bs ON bb.state_root = bs.state_root
        "#
    )
    .fetch_optional(executor)
    .await?;

    Ok(row.and_then(|r| r.max_slot))
}

async fn get_deposit_sum_before_slot(
    executor: impl PgExecutor<'_>,
    slot: Slot,
) -> Result<GweiNewtype> {
    if slot == Slot(0) {
        return Ok(GweiNewtype(0));
    }

    let result = sqlx::query!(
        r#"
        SELECT bb.deposit_sum_aggregated
        FROM beacon_blocks bb
        JOIN beacon_states bs ON bb.state_root = bs.state_root
        WHERE bs.slot < $1
        ORDER BY bs.slot DESC
        LIMIT 1
        "#,
        slot.0
    )
    .fetch_optional(executor)
    .await?;

    match result {
        Some(row) => Ok(row.deposit_sum_aggregated.into()),
        None => Ok(GweiNewtype(0)), // No block before, so starting from 0
    }
}

async fn update_deposit_sums(
    executor: impl PgExecutor<'_>,
    block_root: &str, // We need block_root to update the correct row
    slot: Slot,       // For logging and potentially for more precise updates if needed
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

pub async fn recompute_deposit_sums(db_pool: &PgPool, start_slot: Slot) -> Result<()> {
    info!(%start_slot, "starting deposit sum recomputation process");

    let mut transaction = db_pool.begin().await?;
    let beacon_node = BeaconNodeHttp::new_from_env();

    let latest_slot_in_db = match get_latest_slot_in_db(&mut *transaction).await? {
        Some(s) => s,
        None => {
            info!("no blocks found in the database. nothing to recompute.");
            return Ok(());
        }
    };

    if start_slot > latest_slot_in_db {
        info!(
            %start_slot, %latest_slot_in_db,
            "start_slot is after the latest slot in the database. nothing to recompute."
        );
        return Ok(());
    }

    let mut current_deposit_sum_aggregated =
        get_deposit_sum_before_slot(&mut *transaction, start_slot).await?;

    info!(%start_slot, initial_aggregated_sum = %current_deposit_sum_aggregated, "established jumping-off point for aggregated deposits");

    let total_slots_to_process = (latest_slot_in_db.0 - start_slot.0 + 1) as u64;
    let mut progress = Progress::new("recompute-deposit-sums", total_slots_to_process);

    for current_slot_val in start_slot.0..=latest_slot_in_db.0 {
        let current_slot = Slot(current_slot_val);

        match beacon_node.get_block_by_slot(current_slot).await {
            Ok(Some(block)) => {
                let new_deposit_sum = block.total_deposits_amount();
                current_deposit_sum_aggregated = current_deposit_sum_aggregated + new_deposit_sum;

                // We need the block_root to update the database.
                // The block returned by get_block_by_slot might not have the block_root directly
                // if it's just a BeaconBlock. We need to fetch the header to get the root.
                // This is inefficient. It's better if `store_block` also stored block_root, or if get_block_by_slot returned it.
                // For now, let's try to get header for the root.
                let header = beacon_node
                    .get_header_by_slot(current_slot)
                    .await?
                    .with_context(|| {
                        format!(
                            "failed to get header for slot {} to find block_root for update",
                            current_slot
                        )
                    })?;
                let block_root = header.root;

                debug!(
                    slot = %current_slot,
                    %block_root,
                    %new_deposit_sum,
                    %current_deposit_sum_aggregated,
                    "recomputed deposit sums for block"
                );

                update_deposit_sums(
                    &mut *transaction,
                    &block_root, // Pass the fetched block_root
                    current_slot,
                    new_deposit_sum,
                    current_deposit_sum_aggregated,
                )
                .await?;
            }
            Ok(None) => {
                // Skipped slot, deposit_sum_aggregated carries over from the previous block.
                debug!(slot = %current_slot, "slot was skipped, carrying over aggregated deposit sum");
            }
            Err(e) => {
                warn!(slot = %current_slot, error = %e, "failed to get block by slot, skipping deposit recomputation for this slot.");
            }
        }

        progress.inc_work_done();
        if progress.work_done % 100 == 0 || progress.work_done == total_slots_to_process {
            info!("{}", progress.get_progress_string());
        }
    }

    transaction.commit().await?;

    info!("deposit sum recomputation finished.");
    Ok(())
}
