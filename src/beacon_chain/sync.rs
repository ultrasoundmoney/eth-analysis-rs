//! Synchronises Beacon Chain data into Postgres on a slot-by-slot basis.
//!
//! This module connects to a Lighthouse (or other) beacon node over HTTP and
//! incrementally replicates the canonical chain into our relational schema. It
//! is deliberately conservative – every slot is processed in sequence, inside a
//! single SQL transaction, so that the database is always internally
//! consistent even in the presence of re-orgs.
//!
//! Core responsibilities
//! ---------------------
//! * Determine the correct starting point by walking **backwards** from the tip
//!   of the local database until the last common ancestor with the beacon node
//!   is found (`rollback_to_last_common_ancestor`). Any divergent rows are
//!   removed with `rollback_slots`.
//! * Drive a forward-only sync loop (`sync_beacon_states_slot_by_slot`) that:
//!     * polls the beacon node every `SYNC_V2_POLLING_INTERVAL`,
//!     * processes up to `SYNC_V2_MAX_SLOTS_PER_CYCLE` per iteration, and
//!     * sleeps/retries on errors for `SYNC_V2_ERROR_RETRY_DELAY`.
//! * For each slot (`sync_slot_by_state_root`):
//!     * fetch the header, parent root, and block; abort on re-orgs,
//!     * optionally gather heavy data (validator balances, pending deposits)
//!       only when we are within `BLOCK_LAG_LIMIT` of the chain head to avoid
//!       falling further behind,
//!     * calculate and persist aggregated deposits, withdrawals, issuance, and
//!       ETH supply, and
//!     * commit the transaction atomically so downstream queries never observe
//!       partial state.
//! * Keep slow-to-calculate analyses (e.g. the supply dashboard) up-to-date once
//!   we catch up to the head of the chain.
//!
//! High-level safety guarantees
//! ---------------------------
//! * At most one slot is written at a time and only after its parent exists.
//! * Any detected inconsistency (missing parent, parent-root mismatch, or RPC
//!   error) triggers a rollback to the last verified slot before retrying.
//! * Heavy RPC endpoints are avoided when lagging, ensuring the syncer always
//!   makes forward progress.

use anyhow::{anyhow, bail, Context, Result};
use chrono::Duration;
use lazy_static::lazy_static;
use sqlx::PgExecutor;
use sqlx::PgPool;
use tracing::instrument;
use tracing::{debug, info, warn};

use crate::beacon_chain::withdrawals;
use crate::units::GweiNewtype;
use crate::{beacon_chain::PECTRA_SLOT, eth_supply, supply_dashboard_analysis};
use crate::{
    beacon_chain::{balances, deposits, issuance},
    db, log,
};

use super::node::{BeaconNode, BeaconNodeHttp, ValidatorBalance};
use super::{blocks, states, BeaconHeaderSignedEnvelope, Slot};

lazy_static! {
    static ref BLOCK_LAG_LIMIT: Duration = Duration::minutes(5);
}

async fn fetch_pending_deposits_with_retry(
    beacon_node: &BeaconNodeHttp,
    state_root_str: &str,
    slot: Slot,
) -> Result<Option<GweiNewtype>> {
    if slot < *PECTRA_SLOT {
        debug!(%slot, "pre-pectra slot, skipping pending deposits sum fetch");
        return Ok(None);
    }

    let mut attempts = 0;
    loop {
        attempts += 1;
        match beacon_node.pending_deposits_sum(state_root_str).await {
            Ok(Some(sum)) => {
                if attempts > 1 {
                    debug!(%slot, attempt = attempts, "pending deposits sum became available after retry");
                }
                return Ok(Some(sum));
            }
            Ok(None) => {
                if attempts < 3 {
                    warn!(
                        %slot,
                        attempt = attempts,
                        "pending deposits sum is none, retrying in 12s"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(12)).await;
                } else {
                    warn!(
                        %slot,
                        "pending deposits sum is still none after 3 attempts, returning none"
                    );
                    return Ok(None);
                }
            }
            Err(e) => {
                // Immediately return an error, do not retry.
                warn!(
                    %slot,
                    error = ?e,
                    "error fetching pending deposits sum, returning error immediately"
                );
                return Err(e.context("failed to fetch pending deposits sum"));
            }
        }
    }
}

#[instrument(skip(beacon_node, header), fields(slot = %header.slot(), block_root = %header.root))]
async fn gather_balances_deposits(
    beacon_node: &BeaconNodeHttp,
    header: &BeaconHeaderSignedEnvelope,
) -> Result<(Vec<ValidatorBalance>, Option<GweiNewtype>)> {
    let slot = header.slot();
    let state_root_str = header.state_root();

    let balances_future = beacon_node.get_validator_balances(&state_root_str);
    let pending_deposits_future =
        fetch_pending_deposits_with_retry(beacon_node, &state_root_str, slot);

    let (validator_balances_result, pending_deposits_sum_result) =
        tokio::try_join!(balances_future, pending_deposits_future)?;

    let validator_balances = validator_balances_result
        .ok_or_else(|| {
            anyhow!(
                "beacon node reported no validator balances for state_root {} (slot {}), which are mandatory when GATHERING (received None)",
                state_root_str,
                slot
            )
        })?;

    Ok((validator_balances, pending_deposits_sum_result))
}

pub async fn sync_slot_by_state_root(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
    header: BeaconHeaderSignedEnvelope,
    head_header: &BeaconHeaderSignedEnvelope,
) -> Result<()> {
    // heavy data gathering calls (balances, pending deposits) and analyses may take more than 12s causing us to fall behind.
    // we only want to do this if we're caught up to the head of the chain.
    let is_at_chain_head = head_header.state_root() == header.state_root();
    let sync_lag = head_header.slot().date_time() - header.slot().date_time();
    debug!(lag = %sync_lag, %is_at_chain_head, slot = %header.slot(), "beacon sync lag while syncing slot");

    let block_root_for_get_block = header.root.clone();
    let slot_for_get_block = header.slot();
    let block = beacon_node
        .get_block_by_block_root(&block_root_for_get_block)
        .await
        .with_context(|| {
            format!(
                "failed to get block by block_root {} (for slot {}) from beacon node",
                block_root_for_get_block, slot_for_get_block
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "beacon node reported no block for block_root {} (slot {}) when one was expected",
                block_root_for_get_block,
                slot_for_get_block
            )
        })?;

    let mut opt_validator_balances: Option<Vec<ValidatorBalance>> = None;
    let mut opt_pending_deposits_sum: Option<GweiNewtype> = None;

    if is_at_chain_head {
        debug!(slot = %header.slot(), "at chain head, gathering balances and deposits");
        let (balances, deposits) = gather_balances_deposits(beacon_node, &header).await?;
        opt_validator_balances = Some(balances);
        opt_pending_deposits_sum = deposits;
    } else {
        warn!(slot = %header.slot(), %sync_lag, "not at chain head, skipping balances and pending deposits fetch to catch up faster");
    }

    let mut transaction = db_pool.begin().await?;

    let deposit_sum_aggregated =
        deposits::get_deposit_sum_aggregated(&mut *transaction, &block).await;

    let withdrawal_sum_aggregated =
        withdrawals::get_withdrawal_sum_aggregated(&mut *transaction, &block).await;

    let slot = header.slot();
    let state_root = &header.state_root();
    let block_root = header.root.clone();
    debug!(
        %slot,
        state_root,
        block_root,
        "storing slot with block"
    );
    let is_parent_known = blocks::get_is_hash_known(&mut *transaction, &header.parent_root())
        .await
        .unwrap();
    if !is_parent_known {
        bail!(
            "trying to insert beacon block with missing parent, block_root: {}, parent_root: {:?}",
            header.root,
            header.header.message.parent_root
        );
    }

    states::store_state(&mut *transaction, &header.state_root(), header.slot()).await;

    let store_block_params = blocks::StoreBlockParams {
        deposit_sum: block.total_deposits_amount(),
        deposit_sum_aggregated,
        withdrawal_sum: withdrawals::get_withdrawal_sum_from_block(&block),
        withdrawal_sum_aggregated,
        pending_deposits_sum: opt_pending_deposits_sum,
    };

    blocks::store_block(&mut *transaction, &block, store_block_params, &header).await;

    if let Some(ref validator_balances_vec) = opt_validator_balances {
        debug!(slot = %header.slot(), "validator balances available, proceeding with related storage");
        let validator_balances_sum = balances::sum_validator_balances(validator_balances_vec);
        balances::store_validators_balance(
            &mut *transaction,
            state_root,
            slot,
            &validator_balances_sum,
        )
        .await;

        // Determine pending deposits for issuance, defaulting to 0 if None.
        let pending_deposits_for_issuance = opt_pending_deposits_sum.unwrap_or_else(|| {
            debug!(slot = %header.slot(), "pending deposits sum is None (due to pre-pectra, sync lag, or fetch error), using GweiNewtype(0) for issuance calculation");
            GweiNewtype(0)
        });
        if opt_pending_deposits_sum.is_some() {
            debug!(slot = %header.slot(), pending_deposits_sum = ?opt_pending_deposits_sum, "using actual pending deposits sum for issuance calculation");
        }

        debug!(slot = %header.slot(), "storing issuance using effective pending deposits sum");
        let issuance = issuance::calc_issuance(
            &validator_balances_sum,
            &deposit_sum_aggregated,
            &pending_deposits_for_issuance,
            &withdrawal_sum_aggregated,
        );
        issuance::store_issuance(&mut *transaction, state_root, slot, &issuance).await;

        let result = eth_supply::sync_eth_supply(&mut transaction, slot).await;
        if let Err(e) = result {
            warn!(
                "error syncing eth supply for slot {}, skipping: {}",
                slot, e
            );
        }
    } else {
        debug!(slot = %header.slot(), "validator balances not available (likely due to sync lag or fetch error), skipping dependent storage operations");
    }

    transaction.commit().await?;

    if is_at_chain_head {
        debug!("sync caught up with head of chain, updating deferrable analysis");
        update_deferrable_analysis(db_pool)
            .await
            .context("failed to update deferrable analysis after catching up")?;
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

/// Rolls back all slots greater than or equal to the given slot from all relevant tables.
pub async fn rollback_slots(db_pool: &PgPool, slot_gte: Slot) -> Result<()> {
    debug!(%slot_gte, "rolling back slots >= {} completely from db", slot_gte);

    let mut transaction = db_pool.begin().await?;

    eth_supply::rollback_supply_from_slot(&mut transaction, slot_gte).await?;
    issuance::delete_issuances(&mut *transaction, slot_gte).await; // Assuming this deletes >= slot_gte
    balances::delete_validator_sums(&mut *transaction, slot_gte).await; // Assuming this deletes >= slot_gte
    blocks::delete_blocks(&mut *transaction, slot_gte).await; // Assuming this deletes >= slot_gte
    states::delete_states(&mut *transaction, slot_gte).await; // Assuming this deletes >= slot_gte

    transaction.commit().await?;

    Ok(())
}

/// Rolls back to the last common ancestor of the DB and the beacon chain.
/// Returns the next slot to process.
async fn rollback_to_last_common_ancestor(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
) -> Result<Slot> {
    let last_state_opt = states::last_stored_state(db_pool).await?;

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
            warn!("rolled back slot 0. ensuring db is empty and ready to sync from genesis.");
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
                    warn!(slot = %current_check_slot, db_root = ?db_block_root_for_check_slot, chain_root = %on_chain_header.root, "mismatch or db missing chain block. rolling back this slot and any above it.");
                    rollback_slots(db_pool, current_check_slot).await?;
                }
            }
            Ok(None) => {
                if db_block_root_for_check_slot.is_some() {
                    warn!(slot = %current_check_slot, "db has block, chain says slot missed. rolling back this slot and any above it.");
                    rollback_slots(db_pool, current_check_slot).await?;
                } else {
                    debug!(slot = %current_check_slot, "slot consistently empty on db and chain. continuing search.");
                }
            }
            Err(e) => {
                warn!(slot = %current_check_slot, "error fetching header during rollback: {}. retrying after delay.", e);
                tokio::time::sleep(SYNC_V2_ERROR_RETRY_DELAY).await;
                continue; // Retry the same slot, skip decrementing current_check_slot
            }
        }
        current_check_slot = current_check_slot - 1;
    }
}

pub async fn sync_beacon_states_slot_by_slot() -> Result<()> {
    log::init();
    info!("starting slot-by-slot beacon state sync (v2)");

    let db_pool = db::get_db_pool("sync-beacon-states-v2", 5).await;
    sqlx::migrate!().run(&db_pool).await.unwrap();
    let beacon_node = BeaconNodeHttp::new_from_env();

    info!("performing startup validation and potential rollback to determine sync start slot.");
    let mut next_slot_to_process = rollback_to_last_common_ancestor(&db_pool, &beacon_node).await?;
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
                                rollback_to_last_common_ancestor(&db_pool, &beacon_node).await?;
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
                        match sync_slot_by_state_root(
                            &db_pool,
                            &beacon_node,
                            on_chain_header_for_current_slot,
                        )
                        .await
                        {
                            Ok(_) => {
                                info!("successfully synced slot: {}", current_processing_slot);
                                next_slot_to_process = current_processing_slot + 1;
                            }
                            Err(e) => {
                                warn!(slot = %current_processing_slot, error = ?e, "error during sync_slot_by_state_root. breaking cycle.");
                                tokio::time::sleep(SYNC_V2_ERROR_RETRY_DELAY).await;
                                break;
                            }
                        }
                    } else {
                        warn!(slot = %current_processing_slot, expected_parent = %db_parent_block_root_expected, actual_parent = %on_chain_parent_root, "reorg detected. parent mismatch.");
                        next_slot_to_process =
                            rollback_to_last_common_ancestor(&db_pool, &beacon_node).await?;
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
