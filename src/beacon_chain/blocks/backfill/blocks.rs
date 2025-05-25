use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::{
    blocks::{self, StoreBlockParams},
    deposits,
    node::{BeaconNode, BeaconNodeHttp},
    withdrawals, Slot, PECTRA_SLOT,
};
use crate::units::GweiNewtype;

/// Backfills any missing beacon_blocks records between `from_slot` (inclusive)
/// and the highest slot present in beacon_states.
pub async fn backfill_missing_beacon_blocks(db_pool: &PgPool, from_slot: Slot) {
    info!(%from_slot, "starting backfill of missing beacon_blocks");

    let highest_slot_opt: Option<i32> = sqlx::query_scalar!("SELECT MAX(slot) FROM beacon_states")
        .fetch_one(db_pool)
        .await
        .unwrap_or(None);

    let highest_slot_val = match highest_slot_opt {
        Some(val) => val,
        None => {
            info!("no rows in beacon_states – nothing to backfill");
            return;
        }
    };
    let highest_slot = Slot(highest_slot_val);

    if highest_slot < from_slot {
        info!(%highest_slot, %from_slot, "from_slot is higher than db tip – nothing to do");
        return;
    }

    let missing_slots: Vec<i32> = sqlx::query_scalar!(
        r#"
        SELECT bs.slot
        FROM beacon_states bs
        LEFT JOIN beacon_blocks bb ON bs.state_root = bb.state_root
        WHERE bs.slot >= $1 AND bs.slot <= $2 AND bb.state_root IS NULL
        ORDER BY bs.slot ASC
        "#,
        from_slot.0,
        highest_slot.0
    )
    .fetch_all(db_pool)
    .await
    .unwrap_or_else(|e| {
        warn!(error = %e, "failed to fetch list of missing slots");
        Vec::new()
    });

    if missing_slots.is_empty() {
        info!("no missing beacon_blocks detected in requested range");
        return;
    }

    let total_missing = missing_slots.len() as u64;
    let mut progress = Progress::new("backfill-missing-beacon-blocks", total_missing);
    let beacon_node = BeaconNodeHttp::new();

    for slot_i32 in missing_slots {
        let slot = Slot(slot_i32);
        debug!(%slot, "processing slot");

        match beacon_node.get_header_by_slot(slot).await {
            Ok(Some(header_env)) => {
                let block_root_for_fetch = header_env.root.clone();
                match beacon_node
                    .get_block_by_block_root(&block_root_for_fetch)
                    .await
                {
                    Ok(Some(block)) => {
                        let mut tx = match db_pool.begin().await {
                            Ok(tx) => tx,
                            Err(e) => {
                                warn!(%slot, error = %e, "failed to open db transaction – skipping slot");
                                break;
                            }
                        };

                        if !blocks::get_is_hash_known(&mut *tx, &header_env.parent_root()).await {
                            warn!(%slot, parent_root = %header_env.parent_root(), "parent block not yet in db – skipping");
                            break;
                        }

                        let deposit_sum_aggregated =
                            deposits::get_deposit_sum_aggregated(&mut *tx, &block).await;
                        let withdrawal_sum_aggregated =
                            withdrawals::get_withdrawal_sum_aggregated(&mut *tx, &block).await;

                        let mut pending_deposits_sum: Option<GweiNewtype> = None;
                        if slot >= *PECTRA_SLOT {
                            match beacon_node
                                .get_pending_deposits_sum(&header_env.state_root())
                                .await
                            {
                                Ok(sum) => pending_deposits_sum = sum,
                                Err(e) => {
                                    warn!(%slot, error = %e, "failed to fetch pending deposits sum")
                                }
                            }
                        }

                        let store_params = StoreBlockParams {
                            deposit_sum: deposits::get_deposit_sum_from_block(&block),
                            deposit_sum_aggregated,
                            withdrawal_sum: withdrawals::get_withdrawal_sum_from_block(&block),
                            withdrawal_sum_aggregated,
                            pending_deposits_sum,
                        };

                        blocks::store_block(&mut *tx, &block, store_params, &header_env).await;

                        if let Err(e) = tx.commit().await {
                            warn!(%slot, error = %e, "db commit failed");
                        } else {
                            info!(%slot, "inserted missing beacon_block");
                        }
                    }
                    Ok(None) => debug!(%slot, "on-chain slot has no block"),
                    Err(e) => warn!(%slot, error = %e, "error fetching block by root"),
                }
            }
            Ok(None) => debug!(%slot, "on-chain slot has no header"),
            Err(e) => warn!(%slot, error = %e, "error fetching header"),
        }

        progress.inc_work_done();
        if progress.work_done % 100 == 0 || progress.work_done >= total_missing {
            info!("progress: {}", progress.get_progress_string());
        }
    }

    info!("backfill complete: {}", progress.get_progress_string());
}
