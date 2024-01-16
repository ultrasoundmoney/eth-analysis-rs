use anyhow::{anyhow, bail, Context, Result};
use chrono::Duration;
use futures::{try_join, FutureExt};
use lazy_static::lazy_static;
use serde::Deserialize;
use sqlx::PgPool;
use sqlx::{Acquire, PgConnection};
use std::time::SystemTime;
use tokio::time::sleep;
use tracing::{debug, info, instrument, warn};

use crate::beacon_chain::withdrawals;
use crate::{
    beacon_chain::{balances, deposits, issuance, slot_from_string},
    db,
    json_codecs::i32_from_string,
    performance::TimedExt,
};
use crate::{eth_supply, supply_dashboard_analysis};

use super::node::{BeaconBlock, BeaconNode, BeaconNodeHttp, ValidatorBalance};
use super::{blocks, states, BeaconHeaderSignedEnvelope, BeaconStore, BeaconStorePostgres, Slot};

lazy_static! {
    static ref BLOCK_LAG_LIMIT: Duration = Duration::minutes(5);
}

struct SyncData {
    /// Beacon header, block, and execution block data.
    /// Slots may be missed, in which case this will be None.
    block_data: Option<(
        BeaconHeaderSignedEnvelope,
        BeaconBlock,
        // f64,
        // ExecutionNodeBlock,
    )>,
    /// Balances of all validators for the given state_root. Depending on how far out of sync we
    /// are these may be too slow to fetch, and therefore unavailable.
    validator_balances: Option<Vec<ValidatorBalance>>,
}

#[instrument(skip(beacon_node))]
async fn gather_sync_data(
    beacon_node: &BeaconNodeHttp,
    slot: Slot,
    state_root: &str,
    sync_lag: &Duration,
) -> Result<SyncData> {
    // The beacon node won't clearly tell us if a header is unavailable because the slot was missed
    // or because the state_root we have got dropped. To determine which, we ask to identify the
    // state_root after we got the header. If the state_root still matches, the slot was missed.
    let header = beacon_node.get_header_by_slot(slot).await?;

    let current_state_root = beacon_node
        .get_state_root_by_slot(slot)
        .await?
        .expect("expect state_root to be available for currently syncing slot");

    if *state_root != current_state_root {
        bail!(
            "slot reorged during gather_sync_data phase, can't continue sync of current state_root {}",
            state_root
        );
    }

    let block_data = match header {
        None => None,
        Some(header) => {
            let block = beacon_node
                .get_block_by_block_root(&header.root)
                .await?
                .context("block became unavailable during gather_sync_data")?;
            // let execution_block_hash = block.block_hash().expect(
            //     "expect all beacon blocks to be post-merge and contain a execution block hash",
            // );
            // let execution_block = execution_node
            //     .get_block_by_hash(&execution_block_hash)
            //     .await
            //     // Between the time we received the beacon block and requested the execution block,
            //     // the block may have disappeared. Right now we panic, we could do better.
            //     .expect("expect execution block not to reorg after beacon block is received");
            // let eth_price = eth_price_store
            //     .get_eth_price_by_block(&execution_block)
            //     .timed("get_eth_price_by_block")
            //     .await
            //     .expect("eth price close to block to be available");
            // Some((header, block, eth_price, execution_block))
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
        block_data,
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
    beacon_node: &BeaconNodeHttp,
    db_pool: &PgPool,
    // eth_price_store: &EthPriceStorePostgres,
    // execution_node: &ExecutionNode,
    state_root: &str,
    slot: Slot,
) -> Result<()> {
    // If we are falling too far behind the head of the chain, skip storing validator balances.
    let sync_lag = get_sync_lag(beacon_node, slot).await?;
    debug!(%sync_lag, "beacon sync lag");

    let SyncData {
        block_data,
        validator_balances,
    } = gather_sync_data(
        beacon_node,
        // eth_price_store,
        // execution_node,
        slot,
        state_root,
        &sync_lag,
    )
    .await?;

    // Now that we have all the data, we start storing it.
    let mut transaction = db_pool.begin().await?;

    match block_data.as_ref() {
        None => {
            debug!(
                "storing slot without block, slot: {:?}, state_root: {}",
                slot, state_root
            );
            states::store_state(&mut *transaction, state_root, slot)
                .timed("store state without block")
                .await;
        }
        Some((header, block)) => {
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

            // execution_chain::store_block(db_pool, &block, eth_price)
            //     .timed("store_block")
            //     .await;
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

        if let Some((_, block)) = block_data {
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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FinalizedCheckpointEvent {
    block: String,
    state: String,
    #[serde(deserialize_with = "i32_from_string")]
    epoch: i32,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct HeadEvent {
    #[serde(deserialize_with = "slot_from_string")]
    slot: Slot,
    /// block_root
    block: String,
    state: String,
}

impl From<BeaconHeaderSignedEnvelope> for HeadEvent {
    fn from(envelope: BeaconHeaderSignedEnvelope) -> Self {
        Self {
            state: envelope.header.message.state_root,
            block: envelope.root,
            slot: envelope.header.message.slot,
        }
    }
}

pub async fn rollback_slots(
    executor: &mut PgConnection,
    greater_than_or_equal: Slot,
) -> Result<()> {
    debug!("rolling back data based on slots gte {greater_than_or_equal}");
    let mut transaction = executor.begin().await?;
    eth_supply::rollback_supply_from_slot(&mut transaction, greater_than_or_equal)
        .await
        .unwrap();
    blocks::delete_blocks(&mut *transaction, greater_than_or_equal).await;
    issuance::delete_issuances(&mut *transaction, greater_than_or_equal).await;
    balances::delete_validator_sums(&mut *transaction, greater_than_or_equal).await;
    states::delete_states(&mut *transaction, greater_than_or_equal).await;
    transaction.commit().await?;
    Ok(())
}

pub async fn rollback_slot(executor: &mut PgConnection, slot: Slot) -> Result<()> {
    debug!("rolling back data based on slot {slot}");
    let mut transaction = executor.begin().await?;
    eth_supply::rollback_supply_slot(&mut transaction, slot)
        .await
        .unwrap();
    blocks::delete_block(&mut *transaction, slot).await;
    issuance::delete_issuance(&mut *transaction, slot).await;
    balances::delete_validator_sum(&mut *transaction, slot).await;
    states::delete_state(&mut *transaction, slot).await;
    transaction.commit().await?;
    Ok(())
}

/// Searches backwards from the starting candidate to find a slot where stored and on-chain
/// state_roots match.
async fn find_last_matching_slot(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
    starting_candidate: Slot,
) -> Result<Slot> {
    let mut candidate_slot = starting_candidate;
    let mut stored_state_root = states::get_state_root_by_slot(db_pool, candidate_slot).await;
    let mut on_chain_state_root = beacon_node
        .get_header_by_slot(candidate_slot)
        .await?
        .map(|envelope| envelope.header.message.state_root);

    loop {
        match (stored_state_root, on_chain_state_root) {
            (Some(stored_state_root), Some(on_chain_state_root))
                if stored_state_root == on_chain_state_root =>
            {
                debug!(
                    stored_state_root,
                    on_chain_state_root, "stored and on-chain state_root match"
                );
                break;
            }
            _ => {
                candidate_slot = candidate_slot - 1;
                stored_state_root = states::get_state_root_by_slot(db_pool, candidate_slot).await;
                on_chain_state_root = beacon_node
                    .get_header_by_slot(candidate_slot)
                    .await?
                    .map(|envelope| envelope.header.message.state_root);
            }
        }
    }

    debug!(
        slot = candidate_slot.0,
        "found a state match between stored and on-chain"
    );
    Ok(candidate_slot)
}

async fn estimate_slots_remaining(
    beacon_node: &BeaconNodeHttp,
    beacon_store: &BeaconStorePostgres,
) -> i32 {
    let last_on_chain = beacon_node.get_last_header().await.unwrap();
    let last_synced_slot = beacon_store
        .get_last_state()
        .await
        .map_or(Slot(0), |state| state.slot);
    last_on_chain.slot().0 - last_synced_slot.0
}

#[instrument(skip(beacon_node, beacon_store))]
async fn check_state_root_matches(
    beacon_node: &BeaconNodeHttp,
    beacon_store: &BeaconStorePostgres,
    slot: Slot,
) -> anyhow::Result<bool> {
    let on_chain_state_root_fut = beacon_node.get_state_root_by_slot(slot);
    let stored_state_root_fut = beacon_store.get_state_root_by_slot(slot).map(Ok);

    let (on_chain_state_root, stored_state_root) =
        try_join!(on_chain_state_root_fut, stored_state_root_fut)?;

    match (on_chain_state_root, stored_state_root) {
        (Some(on_chain_state_root), Some(stored_state_root)) => {
            Ok(on_chain_state_root == stored_state_root)
        }
        (None, Some(_)) => Err(anyhow!("no on-chain state root found for slot {}", slot)),
        (Some(_), None) => Err(anyhow!("no state root found for slot {}", slot)),
        (None, None) => Err(anyhow!(
            "no on-chain or stored state root found for slot {}",
            slot
        )),
    }
}

async fn check_last_two_roots_match(
    beacon_node: &BeaconNodeHttp,
    beacon_store: &BeaconStorePostgres,
    last_stored_slot: Slot,
) -> anyhow::Result<bool> {
    let (slot_min_one_matches, slot_matches) = try_join!(
        check_state_root_matches(beacon_node, beacon_store, last_stored_slot - 1),
        check_state_root_matches(beacon_node, beacon_store, last_stored_slot)
    )?;
    Ok(slot_min_one_matches && slot_matches)
}

pub async fn analyze_states_loop() -> Result<()> {
    let db_pool = db::get_db_pool("analyze-slots", 3).await;
    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNodeHttp::new();
    let beacon_store = BeaconStorePostgres::new(db_pool.clone());
    // let eth_price_store = EthPriceStorePostgres::new(db_pool.clone());
    // let execution_node = ExecutionNode::connect().await;

    let start = SystemTime::now();

    // If we're far out of sync, give an estimate for how long it will take to catch up.
    let mut remaining_slot_estimate = estimate_slots_remaining(&beacon_node, &beacon_store)
        .await
        .try_into()
        .unwrap();
    let mut progress = {
        if remaining_slot_estimate > 16 {
            Some(pit_wall::Progress::new(
                "sync beacon states",
                remaining_slot_estimate,
            ))
        } else {
            None
        }
    };

    // Instead of checking whether we're starting from genesis every time, we check once, and then
    // assume a previous stored slot exists.
    let last_stored_state = beacon_store.get_last_state().await;
    if last_stored_state.is_none() {
        info!("no beacon states stored, syncing from genesis");
        todo!("sync slot 0");
    }

    let mut checked_once = false;

    loop {
        let last_stored_state = beacon_store
            .get_last_state()
            .await
            .expect("failed to find stored state, expected at least one before entering sync loop");

        // Because state roots don't have parents, it's tricky to check whether our last stored
        // state root matches the chain. It is always possible a previous state_root reorgs,
        // __after__ we check. If the unseen root brings in a new block, we'd notice a missing
        // parent, but if the roots reorgs a block out, we wouldn't notice the orphan and need for
        // rollback. We could track block numbers to notice this but as they are unavailable here,
        // we opt instead to check the last two state roots.
        let db_in_sync = {
            // If we've fallen far behind, we only check once whether our DB is in sync, and skip
            // checking until we're close to catching up and reorgs are a concern.
            if checked_once && remaining_slot_estimate > 16 {
                debug!("skipping state root check, we're more than 16 slots behind");
                true
            } else {
                check_last_two_roots_match(&beacon_node, &beacon_store, last_stored_state.slot)
                    .await?
            }
        };

        if db_in_sync {
            debug!("db in sync, syncing next slot");
            checked_once = true;
            let slot = last_stored_state.slot + 1;
            let state_root = {
                // Get next state root, if none is available, wait and try again.
                let mut next_state_root = None;
                while next_state_root.is_none() {
                    next_state_root = beacon_node.get_state_root_by_slot(slot).await?;
                    if next_state_root.is_none() {
                        debug!(%slot, "no state root found for slot, waiting 100ms for next slot");
                        sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
                next_state_root.unwrap()
            };
            sync_slot_by_state_root(
                &beacon_node,
                &db_pool,
                // &eth_price_store,
                // &execution_node,
                &state_root,
                slot,
            )
            .await?;
            remaining_slot_estimate -= 1;
        } else {
            debug!("last two state roots do not match chain, rolling back");
            let last_matching_slot =
                find_last_matching_slot(&db_pool, &beacon_node, last_stored_state.slot - 1).await?;
            let first_invalid_slot = last_matching_slot + 1;

            info!(%last_matching_slot, "rolling back");

            rollback_slots(&mut *db_pool.acquire().await?, first_invalid_slot).await?;
        }

        // If we're reporting progress, update progress and report periodically.
        if let Some(progress) = progress.as_mut() {
            progress.inc_work_done();
            let elapsed = start.elapsed().unwrap();
            if elapsed.as_secs() % 10 == 0 {
                info!("{}", progress.get_progress_string());
            }
        }
    }
}
