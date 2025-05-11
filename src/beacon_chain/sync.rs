use anyhow::{anyhow, Result};
use chrono::Duration;
use lazy_static::lazy_static;
use sqlx::PgPool;
use sqlx::{PgConnection, PgExecutor};
use tracing::{debug, info, warn};

use crate::beacon_chain::withdrawals;
use crate::{
    beacon_chain::{balances, deposits, issuance},
    db, log,
    performance::TimedExt,
};
use crate::{eth_supply, supply_dashboard_analysis};

use super::node::{BeaconBlock, BeaconNode, BeaconNodeHttp, StateRoot, ValidatorBalance};
use super::{blocks, states, BeaconHeaderSignedEnvelope, Slot};

lazy_static! {
    static ref BLOCK_LAG_LIMIT: Duration = Duration::minutes(5);
}

struct SyncData {
    header_block_tuple: Option<(BeaconHeaderSignedEnvelope, BeaconBlock)>,
    validator_balances: Option<Vec<ValidatorBalance>>,
}

async fn gather_sync_data(
    beacon_node: &BeaconNodeHttp,
    state_root: &StateRoot,
    slot: Slot,
    sync_lag: &Duration,
) -> Result<SyncData> {
    // The beacon node won't clearly tell us if a header is unavailable because the slot was missed
    // or because the state_root we have got dropped. To determine which, we ask to identify the
    // state_root after we got the header. If the state_root still matches, the slot was missed.
    let header = beacon_node.get_header_by_slot(slot).await?;

    let state_root_check = beacon_node
        .get_state_root_by_slot(slot)
        .await?
        .expect("expect state_root to be available for currently syncing slot");

    if *state_root != state_root_check {
        return Err(anyhow!(
            "slot reorged during gather_sync_data phase, can't continue sync of current state_root {}",
            state_root
        ));
    }

    let header_block_tuple = match header {
        None => None,
        Some(header) => {
            let block = beacon_node
                .get_block_by_block_root(&header.root)
                .await?
                .unwrap_or_else(|| {
                    panic!(
                        "expect a block to be ava)ilable for currently syncing block_root {}",
                        header.root
                    )
                });
            Some((header, block))
        }
    };

    // Whenever we fall behind, getting validator balances for older slots from lighthouse takes a
    // long time. This means if we fall behind too far we never catch up, as syncing one slot now
    // takes longer than it takes for a new slot to appear (12 seconds).
    let validator_balances = {
        if sync_lag > &BLOCK_LAG_LIMIT {
            warn!(
                %sync_lag,
                "block lag over limit, skipping get_validator_balances"
            );
            None
        } else {
            let validator_balances = beacon_node
                .get_validator_balances(state_root)
                .await?
                .expect("expect validator balances to exist for given state_root");
            Some(validator_balances)
        }
    };

    let sync_data = SyncData {
        header_block_tuple,
        validator_balances,
    };

    Ok(sync_data)
}

async fn get_sync_lag(beacon_node: &BeaconNodeHttp, syncing_slot: Slot) -> Result<Duration> {
    let last_header = beacon_node.get_last_header().await?;
    let last_on_chain_slot = last_header.header.message.slot;
    let last_on_chain_slot_date_time = last_on_chain_slot.date_time();
    let slot_date_time = syncing_slot.date_time();
    let lag = last_on_chain_slot_date_time - slot_date_time;
    Ok(lag)
}

pub async fn sync_slot_by_state_root(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
    state_root: &StateRoot,
    slot: Slot,
) -> Result<()> {
    // If we are falling too far behind the head of the chain, skip storing validator balances.
    let sync_lag = get_sync_lag(beacon_node, slot).await?;
    debug!(%sync_lag, "beacon sync lag");

    let SyncData {
        header_block_tuple,
        validator_balances,
    } = gather_sync_data(beacon_node, state_root, slot, &sync_lag).await?;

    // Now that we have all the data, we start storing it.
    let mut transaction = db_pool.begin().await?;

    match header_block_tuple {
        None => {
            debug!(
                "storing slot without block, slot: {:?}, state_root: {}",
                slot, state_root
            );
            states::store_state(&mut *transaction, state_root, slot)
                .timed("store state without block")
                .await;
        }
        Some((ref header, ref block)) => {
            let deposit_sum_aggregated =
                deposits::get_deposit_sum_aggregated(&mut *transaction, block).await;

            let withdrawal_sum_aggregated =
                withdrawals::get_withdrawal_sum_aggregated(&mut *transaction, block).await;

            debug!(
                %slot,
                state_root,
                block_root = header.root,
                "storing slot with block"
            );
            let is_parent_known =
                blocks::get_is_hash_known(&mut *transaction, &header.parent_root()).await;
            if !is_parent_known {
                return Err(anyhow!(
            "trying to insert beacon block with missing parent, block_root: {}, parent_root: {:?}",
            header.root,
            header.header.message.parent_root
        ));
            }

            states::store_state(&mut *transaction, &header.state_root(), header.slot()).await;

            blocks::store_block(
                &mut *transaction,
                block,
                &deposits::get_deposit_sum_from_block(block),
                &deposit_sum_aggregated,
                &withdrawals::get_withdrawal_sum_from_block(block),
                &withdrawal_sum_aggregated,
                header,
            )
            .await;
        }
    }

    if let Some(ref validator_balances) = validator_balances {
        debug!("validator balances present");
        let validator_balances_sum = balances::sum_validator_balances(validator_balances);
        balances::store_validators_balance(
            &mut *transaction,
            state_root,
            slot,
            &validator_balances_sum,
        )
        .await;

        if let Some((_, block)) = header_block_tuple {
            let deposit_sum_aggregated =
                deposits::get_deposit_sum_aggregated(&mut *transaction, &block).await;
            let withdrawal_sum_aggregated =
                withdrawals::get_withdrawal_sum_aggregated(&mut *transaction, &block).await;

            issuance::store_issuance(
                &mut *transaction,
                state_root,
                slot,
                &issuance::calc_issuance(
                    &validator_balances_sum,
                    &withdrawal_sum_aggregated,
                    &deposit_sum_aggregated,
                ),
            )
            .await;
        }

        eth_supply::sync_eth_supply(&mut transaction, slot).await;
    }

    transaction.commit().await?;

    let last_on_chain_state_root = beacon_node
        .get_last_header()
        .await?
        .header
        .message
        .state_root;

    if last_on_chain_state_root == *state_root {
        debug!("sync caught up with head of chain, updating deferrable analysis");
        update_deferrable_analysis(db_pool).await?;
    } else {
        debug!("sync not yet caught up with head of chain, skipping deferrable analysis");
    }

    Ok(())
}

async fn update_deferrable_analysis(db_pool: &PgPool) -> Result<()> {
    supply_dashboard_analysis::update_cache(db_pool).await?;

    Ok(())
}

use crate::beacon_chain::blocks::GENESIS_PARENT_ROOT;
use crate::beacon_chain::node::BlockRoot; // Assuming BlockRoot is a type alias for String

const SYNC_V2_POLLING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);
const SYNC_V2_MAX_SLOTS_PER_CYCLE: usize = 64; // Sync up to N slots at a time.
const SYNC_V2_ERROR_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

/// Gets the highest slot and its block_root from beacon_blocks.
/// This serves both for startup and for finding the current DB tip during sync.
async fn get_highest_stored_block_in_db(
    executor: impl PgExecutor<'_>,
) -> Result<Option<(Slot, BlockRoot)>> {
    let row = sqlx::query!(
        r#"
        SELECT
            bs.slot AS "slot: Slot",
            bb.block_root
        FROM beacon_blocks bb
        JOIN beacon_states bs ON bb.state_root = bs.state_root
        ORDER BY bs.slot DESC
        LIMIT 1
        "#
    )
    .fetch_optional(executor)
    .await?;

    Ok(row.map(|r| (r.slot, r.block_root)))
}

/// Gets the block_root for a specific slot from the database.
async fn get_block_root_for_slot_from_db(
    executor: impl PgExecutor<'_>,
    slot: Slot,
) -> Result<Option<BlockRoot>> {
    let row = sqlx::query!(
        r#"
        SELECT bb.block_root
        FROM beacon_blocks bb
        JOIN beacon_states bs ON bb.state_root = bs.state_root
        WHERE bs.slot = $1
        "#,
        slot.0
    )
    .fetch_optional(executor)
    .await?;

    Ok(row.map(|r| r.block_root))
}

/// Rolls back a single slot from all relevant tables.
pub async fn rollback_slot(transaction: &mut PgConnection, slot: Slot) -> Result<()> {
    debug!(%slot, "rolling back slot completely from db");

    eth_supply::rollback_supply_slot(&mut *transaction, slot).await?;
    issuance::delete_issuance_by_slot(&mut *transaction, slot).await?;
    balances::delete_validator_sum_by_slot(&mut *transaction, slot).await?;
    blocks::delete_block(&mut *transaction, slot).await;
    states::delete_state(&mut *transaction, slot).await;

    Ok(())
}

/// Rolls back all slots greater than the given slot from all relevant tables.
pub async fn rollback_slots(transaction: &mut PgConnection, slot: Slot) -> Result<()> {
    debug!(%slot, "rolling back slots > {} completely from db", slot);
    let gte_slot = slot + 1;
    eth_supply::rollback_supply_from_slot(&mut *transaction, gte_slot).await?;
    issuance::delete_issuances(&mut *transaction, gte_slot).await;
    balances::delete_validator_sums(&mut *transaction, gte_slot).await;
    blocks::delete_blocks(&mut *transaction, gte_slot).await;
    states::delete_states(&mut *transaction, gte_slot).await;
    Ok(())
}

/// Finds the last slot in the DB that is consistent with the canonical chain.
/// Rolls back any inconsistent slots from the DB.
/// Returns the next slot that should be processed (i.e., consistent_slot + 1).
async fn find_last_valid_slot_after_rollback(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
) -> Result<Slot> {
    let last_state_opt = crate::beacon_chain::states::last_stored_state(db_pool).await?;

    let mut current_check_slot: Slot = match last_state_opt {
        Some(last_state) => last_state.slot,
        None => {
            info!(
                "no states found in db; validation will start effectively from genesis (slot 0)."
            );
            Slot(0)
        }
    };

    info!(%current_check_slot, "starting validation and rollback procedure from highest known state slot in db.");

    loop {
        if current_check_slot < Slot(0) {
            warn!(
                "rollback check went below slot 0. ensuring db is empty and syncing from genesis."
            );
            let mut conn = db_pool.acquire().await?;
            rollback_slots(&mut conn, Slot(-1)).await?;
            return Ok(Slot(0));
        }

        let db_block_root_for_check_slot =
            get_block_root_for_slot_from_db(db_pool, current_check_slot).await?;

        match beacon_node.get_header_by_slot(current_check_slot).await {
            Ok(Some(on_chain_header)) => {
                if db_block_root_for_check_slot.as_ref() == Some(&on_chain_header.root) {
                    info!(slot = %current_check_slot, "found consistent block in db.");
                    return Ok(current_check_slot + 1);
                } else {
                    warn!(slot = %current_check_slot, db_root = ?db_block_root_for_check_slot, chain_root = %on_chain_header.root, "mismatch or db missing chain block. rolling back this slot.");
                    let mut conn = db_pool.acquire().await?;
                    rollback_slot(&mut conn, current_check_slot).await?;
                }
            }
            Ok(None) => {
                if db_block_root_for_check_slot.is_some() {
                    warn!(slot = %current_check_slot, "db has block, chain says slot missed. rolling back this slot.");
                    let mut conn = db_pool.acquire().await?;
                    rollback_slot(&mut conn, current_check_slot).await?;
                } else {
                    debug!(slot = %current_check_slot, "slot consistently empty on db and chain. continuing search.");
                }
            }
            Err(e) => {
                warn!(slot = %current_check_slot, "error fetching header during rollback: {}. retrying after delay.", e);
                tokio::time::sleep(SYNC_V2_ERROR_RETRY_DELAY).await;
                continue;
            }
        }
        current_check_slot = current_check_slot - 1;
    }
}

pub async fn sync_beacon_states_slot_by_slot() -> Result<()> {
    log::init_with_env();
    info!("starting slot-by-slot beacon state sync (v2)");

    let db_pool = db::get_db_pool("sync-beacon-states-v2", 5).await;
    sqlx::migrate!().run(&db_pool).await.unwrap();
    let beacon_node = BeaconNodeHttp::new();

    info!("performing startup validation and potential rollback to determine sync start slot.");
    let mut next_slot_to_process =
        find_last_valid_slot_after_rollback(&db_pool, &beacon_node).await?;
    info!(
        "startup validation complete. initial next_slot_to_process: {}",
        next_slot_to_process
    );

    loop {
        let on_chain_head_header_envelope = match beacon_node.get_last_header().await {
            Ok(header_env) => header_env,
            Err(e) => {
                warn!(
                    "failed to get on-chain head header: {}. retrying after delay.",
                    e
                );
                tokio::time::sleep(SYNC_V2_POLLING_INTERVAL).await;
                continue;
            }
        };
        let target_sync_slot = on_chain_head_header_envelope.slot();

        debug!(
            "loop iteration: next_slot_to_process = {}, target_sync_slot = {}",
            next_slot_to_process, target_sync_slot
        );

        if next_slot_to_process > target_sync_slot {
            debug!("caught up to target slot. waiting for new slots.");
            tokio::time::sleep(SYNC_V2_POLLING_INTERVAL).await;
            continue;
        }

        let mut slots_processed_this_cycle = 0;
        let end_slot_for_this_cycle = std::cmp::min(
            target_sync_slot,
            next_slot_to_process + (SYNC_V2_MAX_SLOTS_PER_CYCLE as i32) - 1,
        );

        while next_slot_to_process <= end_slot_for_this_cycle {
            let current_processing_slot = next_slot_to_process;
            debug!("attempting to process slot: {}", current_processing_slot);

            let db_highest_block_details: Option<(Slot, BlockRoot)> =
                get_highest_stored_block_in_db(&db_pool).await?;

            let db_parent_block_root_expected: BlockRoot = if current_processing_slot == Slot(0) {
                GENESIS_PARENT_ROOT.to_string()
            } else {
                match &db_highest_block_details {
                    Some((highest_db_slot, highest_db_root)) => {
                        if *highest_db_slot >= current_processing_slot {
                            warn!(%current_processing_slot, %highest_db_slot, "consistency issue: current slot not ahead of db tip. triggering rollback.");
                            next_slot_to_process =
                                find_last_valid_slot_after_rollback(&db_pool, &beacon_node).await?;
                            break;
                        }
                        highest_db_root.clone()
                    }
                    None => {
                        warn!(%current_processing_slot, "no blocks found in db while processing slot > 0. expecting genesis_parent_root. this will likely trigger a rollback to genesis if inconsistent.");
                        GENESIS_PARENT_ROOT.to_string()
                    }
                }
            };

            match beacon_node
                .get_header_by_slot(current_processing_slot)
                .await
            {
                Ok(Some(on_chain_header_for_current_slot)) => {
                    let on_chain_parent_root = on_chain_header_for_current_slot.parent_root();

                    if on_chain_parent_root == db_parent_block_root_expected {
                        debug!(slot = %current_processing_slot, "parent match. syncing slot.");
                        let state_root_for_current_slot =
                            on_chain_header_for_current_slot.state_root();

                        match sync_slot_by_state_root(
                            &db_pool,
                            &beacon_node,
                            &state_root_for_current_slot,
                            current_processing_slot,
                        )
                        .await
                        {
                            Ok(_) => {
                                info!("successfully synced slot: {}", current_processing_slot);
                                next_slot_to_process = current_processing_slot + 1;
                            }
                            Err(e) => {
                                warn!("error during sync_slot_by_state_root for slot {}: {}. breaking cycle.", current_processing_slot, e);
                                tokio::time::sleep(SYNC_V2_ERROR_RETRY_DELAY).await;
                                break;
                            }
                        }
                    } else {
                        warn!(slot = %current_processing_slot, expected_parent = %db_parent_block_root_expected, actual_parent = %on_chain_parent_root, "reorg detected. parent mismatch.");
                        next_slot_to_process =
                            find_last_valid_slot_after_rollback(&db_pool, &beacon_node).await?;
                        break;
                    }
                }
                Ok(None) => {
                    info!(slot = %current_processing_slot, "slot appears to be skipped on-chain (no header). advancing.");
                    next_slot_to_process = current_processing_slot + 1;
                }
                Err(e) => {
                    warn!(slot = %current_processing_slot, "error fetching header: {}. retrying cycle after delay.", e);
                    tokio::time::sleep(SYNC_V2_ERROR_RETRY_DELAY).await;
                    break;
                }
            }
            slots_processed_this_cycle += 1;
        }

        if next_slot_to_process <= target_sync_slot
            && slots_processed_this_cycle < SYNC_V2_MAX_SLOTS_PER_CYCLE
        {
            if slots_processed_this_cycle == 0 {
                debug!("inner loop made no progress, delaying before next cycle.");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        } else if next_slot_to_process > target_sync_slot {
            tokio::time::sleep(SYNC_V2_POLLING_INTERVAL).await;
        }
    }
}
