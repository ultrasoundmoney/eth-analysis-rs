use anyhow::{anyhow, Result};
use balances::BeaconBalancesSum;
use chrono::Duration;
use futures::{stream, SinkExt, Stream, StreamExt};
use lazy_static::lazy_static;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use sqlx::{Acquire, PgConnection, PgExecutor};
use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{balances, beacon_time, deposits, issuance},
    db, eth_supply,
    eth_units::GweiNewtype,
    json_codecs::from_u32_string,
    log,
    performance::TimedExt,
};

use super::node::{BeaconBlock, BlockRoot, StateRoot, ValidatorBalance};
use super::{blocks, states, BeaconHeaderSignedEnvelope, BeaconNode, Slot, BEACON_URL};

lazy_static! {
    static ref BLOCK_LAG_LIMIT: Duration = Duration::minutes(5);
}

#[derive(Clone)]
pub struct SlotRange {
    greater_than_or_equal: Slot,
    less_than_or_equal: Slot,
}

impl SlotRange {
    pub fn new(greater_than_or_equal: u32, less_than_or_equal: u32) -> Self {
        if greater_than_or_equal > less_than_or_equal {
            panic!("tried to create slot range with negative range")
        }

        Self {
            greater_than_or_equal,
            less_than_or_equal,
        }
    }
}

pub struct SlotRangeIntoIterator {
    slot_range: SlotRange,
    index: usize,
}

impl IntoIterator for SlotRange {
    type Item = u32;
    type IntoIter = SlotRangeIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            slot_range: self,
            index: 0,
        }
    }
}

impl Iterator for SlotRangeIntoIterator {
    type Item = Slot;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.slot_range.greater_than_or_equal as usize + self.index)
            .cmp(&(self.slot_range.less_than_or_equal as usize))
        {
            Ordering::Less => {
                let current = self.slot_range.greater_than_or_equal + self.index as u32;
                self.index = self.index + 1;
                Some(current)
            }
            Ordering::Equal => {
                let current = self.slot_range.greater_than_or_equal + self.index as u32;
                self.index = self.index + 1;
                Some(current)
            }
            Ordering::Greater => None,
        }
    }
}

async fn store_state_with_block(
    executor: &mut PgConnection,
    associated_block_root: &BlockRoot,
    block: &BeaconBlock,
    deposit_sum: &GweiNewtype,
    deposit_sum_aggregated: &GweiNewtype,
    header: &BeaconHeaderSignedEnvelope,
) -> Result<()> {
    let is_parent_known =
        blocks::get_is_hash_known(&mut *executor, &header.header.message.parent_root).await;
    if !is_parent_known {
        return Err(anyhow!(
            "trying to insert beacon block with missing parent, block_root: {}, parent_root: {:?}",
            header.root,
            header.header.message.parent_root
        ));
    }

    states::store_state(
        &mut *executor,
        &header.header.message.state_root,
        &header.header.message.slot,
        associated_block_root,
    )
    .await?;

    blocks::store_block(
        &mut *executor,
        block,
        deposit_sum,
        deposit_sum_aggregated,
        header,
    )
    .await?;

    Ok(())
}

struct SyncData {
    associated_block_root: BlockRoot,
    header_block_tuple: Option<(BeaconHeaderSignedEnvelope, BeaconBlock)>,
    validator_balances: Option<Vec<ValidatorBalance>>,
}

async fn gather_sync_data(
    db_pool: &PgPool,
    beacon_node: &BeaconNode,
    state_root: &StateRoot,
    slot: &Slot,
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

    // To be able to run certain calculations on any slot, we always associate a block_root
    // with a slot. If a slot was missed, we associate the most recent available block root.
    let associated_block_root = {
        match header {
            None => blocks::get_block_root_before_slot(db_pool, &(slot - 1)).await?,
            Some(ref header) => header.root.clone(),
        }
    };

    let header_block_tuple = match header {
        None => None,
        Some(header) => {
            let block = beacon_node
                .get_block_by_block_root(&header.root)
                .await?
                .expect(&format!(
                    "expect a block to be ava)ilable for currently syncing block_root {}",
                    header.root
                ));
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
                .get_validator_balances(&state_root)
                .await?
                .expect("expect validator balances to exist for given state_root");
            Some(validator_balances)
        }
    };

    let sync_data = SyncData {
        associated_block_root,
        header_block_tuple,
        validator_balances,
    };

    Ok(sync_data)
}

async fn get_sync_lag(beacon_node: &BeaconNode, syncing_slot: &Slot) -> Result<Duration> {
    let last_header = beacon_node.get_last_header().await?;
    let last_on_chain_slot = last_header.header.message.slot;
    let last_on_chain_slot_date_time = beacon_time::get_date_time_from_slot(&last_on_chain_slot);
    let slot_date_time = beacon_time::get_date_time_from_slot(&syncing_slot);
    let lag = last_on_chain_slot_date_time - slot_date_time;
    Ok(lag)
}

pub async fn sync_state_root(
    db_pool: &PgPool,
    beacon_node: &BeaconNode,
    state_root: &StateRoot,
    slot: &Slot,
) -> Result<()> {
    // If we are falling too far behind the head of the chain, skip storing validator balances.
    let sync_lag = get_sync_lag(beacon_node, slot).await?;
    debug!(%sync_lag, "beacon sync lag");

    let SyncData {
        associated_block_root,
        header_block_tuple,
        validator_balances,
    } = gather_sync_data(db_pool, beacon_node, state_root, slot, &sync_lag).await?;

    // Now that we have all the data, we start storing it.
    let mut transaction = db_pool.begin().await?;

    match header_block_tuple {
        None => {
            debug!(
                "storing slot without block, slot: {:?}, state_root: {}",
                slot, state_root
            );
            states::store_state(&mut transaction, &state_root, slot, &associated_block_root)
                .timed("store state without block")
                .await?;
        }
        Some((ref header, ref block)) => {
            let deposit_sum_aggregated =
                deposits::get_deposit_sum_aggregated(&mut transaction, &block).await?;

            debug!(slot, state_root, "storing slot with block");
            store_state_with_block(
                &mut transaction,
                &associated_block_root,
                block,
                &deposits::get_deposit_sum_from_block(&block),
                &deposit_sum_aggregated,
                header,
            )
            .timed("store state with block")
            .await?;
        }
    }

    if let Some(ref validator_balances) = validator_balances {
        let validator_balances_sum = balances::sum_validator_balances(&validator_balances);
        balances::store_validators_balance(
            &mut transaction,
            &state_root,
            &slot,
            &validator_balances_sum,
        )
        .await?;

        if let Some((_, block)) = header_block_tuple {
            let deposit_sum_aggregated =
                deposits::get_deposit_sum_aggregated(&mut transaction, &block).await?;
            issuance::store_issuance(
                &mut transaction,
                &state_root,
                slot,
                &issuance::calc_issuance(&validator_balances_sum, &deposit_sum_aggregated),
            )
            .await?;
        }

        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: validator_balances_sum,
            slot: slot.clone(),
        };

        let eth_supply_parts =
            eth_supply::get_supply_parts(&mut transaction, &beacon_balances_sum).await?;

        eth_supply::update_supply_parts(&mut transaction, &eth_supply_parts).await?;

        let in_sync_with_chain = {
            let last_state_root_on_chain = beacon_node
                .get_last_header()
                .await?
                .header
                .message
                .state_root;
            state_root == last_state_root_on_chain
        };

        if in_sync_with_chain {
            debug!("sync caught up to head of chain, computing skippable analysis");
            let eth_supply_parts = eth_supply::update_supply_over_time(
                executor,
                eth_supply_parts.beacon_balances_sum.slot,
                eth_supply_parts.execution_balances_sum.block_number,
                beacon_time::get_date_time_from_slot(&eth_supply_parts.beacon_balances_sum.slot),
            )
            .timed("update-supply-over-time")
            .await?;
        } else {
            debug!("sync not yet caught up with head of chain, skipping some analysis");
        }
    }

    transaction.commit().await?;

    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FinalizedCheckpointEvent {
    block: String,
    state: String,
    #[serde(deserialize_with = "from_u32_string")]
    epoch: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct HeadEvent {
    #[serde(deserialize_with = "from_u32_string")]
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

async fn stream_slots(slot_to_follow: Slot) -> impl Stream<Item = Slot> {
    let url_string = format!("{}/eth/v1/events/?topics=head", *BEACON_URL);
    let url = reqwest::Url::parse(&url_string).unwrap();

    let client = eventsource::reqwest::Client::new(url);
    let (mut tx, rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        let mut last_slot = slot_to_follow;

        for event in client {
            let event = event.unwrap();
            match event.event_type {
                Some(ref event_type) if event_type == "head" => {
                    let head = serde_json::from_str::<HeadEvent>(&event.data).unwrap();

                    debug!(?head, "received new head");

                    // Detect forward gaps in received slots, and fill them in.
                    if head.slot > last_slot && head.slot != last_slot + 1 {
                        debug!(
                            head_slot = head.slot,
                            last_slot, "head slot is more than one ahead of last_slot"
                        );

                        for missing_slot in (last_slot + 1)..head.slot {
                            debug!(missing_slot, "adding missing slot to slots stream");
                            tx.send(missing_slot).await.unwrap();
                        }
                    }

                    last_slot = head.slot;
                    tx.send(head.slot).await.unwrap();
                }
                Some(event) => {
                    warn!(event, "received a server event that was not a head");
                }
                None => {
                    debug!("received an empty server event");
                }
            }
        }
    });

    rx
}

async fn stream_slots_from(gte_slot: &Slot) -> impl Stream<Item = Slot> {
    debug!("streaming slots from {gte_slot}");

    let beacon_node = BeaconNode::new();
    let last_slot_on_start = beacon_node
        .get_last_header()
        .await
        .unwrap()
        .header
        .message
        .slot;
    debug!("last slot on chain: {}", &last_slot_on_start);

    // We stream heads as requested until caught up with the chain and then pass heads as they come
    // in from our node. The only way to be sure how high we should stream, is to wait for the
    // first head from the node to come in. We don't want to wait. So ask for the latest head, take
    // this as the max, and immediately start listening for new heads. Running the small risk the
    // chain has advanced between these two calls.
    let slots_stream = stream_slots(last_slot_on_start).await;

    let slot_range = SlotRange::new(*gte_slot, last_slot_on_start);

    let historic_slots_stream = stream::iter(slot_range);

    historic_slots_stream.chain(slots_stream)
}

async fn stream_slots_from_last(db_pool: &PgPool) -> impl Stream<Item = Slot> {
    let last_synced_state = states::get_last_state(db_pool).await;
    let next_slot_to_sync = last_synced_state.map_or(0, |state| state.slot + 1);
    stream_slots_from(&next_slot_to_sync).await
}

pub async fn rollback_slots(
    executor: &mut PgConnection,
    greater_than_or_equal: &Slot,
) -> Result<()> {
    debug!("rolling back data based on slots gte {greater_than_or_equal}");
    let mut transaction = executor.begin().await?;
    eth_supply::rollback_supply_from_slot(&mut transaction, greater_than_or_equal)
        .await
        .unwrap();
    blocks::delete_blocks(&mut transaction, greater_than_or_equal).await;
    issuance::delete_issuances(&mut transaction, greater_than_or_equal).await;
    balances::delete_validator_sums(&mut transaction, greater_than_or_equal).await;
    states::delete_states(&mut transaction, greater_than_or_equal).await;
    transaction.commit().await?;
    Ok(())
}

pub async fn rollback_slot(executor: &mut PgConnection, slot: &Slot) -> Result<()> {
    debug!("rolling back data based on slot {slot}");
    let mut transaction = executor.begin().await?;
    eth_supply::rollback_supply_slot(&mut transaction, slot)
        .await
        .unwrap();
    blocks::delete_block(&mut transaction, slot).await;
    issuance::delete_issuance(&mut transaction, slot).await;
    balances::delete_validator_sum(&mut transaction, slot).await;
    states::delete_state(&mut transaction, slot).await;
    transaction.commit().await?;
    Ok(())
}

async fn estimate_slots_remaining(executor: impl PgExecutor<'_>, beacon_node: &BeaconNode) -> u32 {
    let last_on_chain = beacon_node.get_last_block().await.unwrap();
    let last_synced_slot = states::get_last_state(executor)
        .await
        .map_or(0, |state| state.slot);
    last_on_chain.slot - last_synced_slot
}

/// Searches backwards from the starting candidate to find a slot where stored and on-chain
/// state_roots match.
async fn find_last_matching_slot(
    db_pool: &PgPool,
    beacon_node: &BeaconNode,
    starting_candidate: &Slot,
) -> Result<Slot> {
    let mut candidate_slot = *starting_candidate;
    let mut stored_state_root = states::get_state_root_by_slot(db_pool, &candidate_slot).await?;
    let mut on_chain_state_root = beacon_node
        .get_header_by_slot(&candidate_slot)
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
                stored_state_root =
                    states::get_state_root_by_slot(db_pool, &candidate_slot).await?;
                on_chain_state_root = beacon_node
                    .get_header_by_slot(&candidate_slot)
                    .await?
                    .map(|envelope| envelope.header.message.state_root);
            }
        }
    }

    debug!(
        slot = candidate_slot,
        "found a match state match between stored and on-chain"
    );
    Ok(candidate_slot)
}

type SlotsQueue = Arc<Mutex<VecDeque<Slot>>>;

pub async fn sync_beacon_states() -> Result<()> {
    log::init_with_env();

    info!("syncing beacon states");

    let db_pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&db::get_db_url_with_name("sync-beacon-states"))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNode::new();

    let mut slots_stream = stream_slots_from_last(&db_pool).await;

    // This queue allows us to queue slots to sync as we need between processing new head events.
    // The two expected scenarios are:
    // 1. A slot was missed, and the next head is more than one slot ahead.
    // 2. We've diverged, and need to roll back one or more slots.
    let slots_queue: SlotsQueue = Arc::new(Mutex::new(VecDeque::new()));

    let mut progress = pit_wall::Progress::new(
        "sync beacon states",
        estimate_slots_remaining(&db_pool, &beacon_node)
            .await
            .into(),
    );

    while let Some(slot_from_stream) = slots_stream.next().await {
        if &slot_from_stream % 100 == 0 {
            debug!("{}", progress.get_progress_string());
        }

        slots_queue.lock().unwrap().push_back(slot_from_stream);

        // Work through the slots queue until it's empty and we're ready to move the next head from
        // the stream to the queue.
        while let Some(slot) = slots_queue.lock().unwrap().pop_front() {
            debug!(slot, "analyzing next slot on the queue");

            let on_chain_state_root =
                beacon_node
                    .get_state_root_by_slot(&slot)
                    .await?
                    .expect(&format!(
                        "expect state_root to exist for slot {} to sync from queue",
                        slot
                    ));
            let current_slot_stored_state_root =
                states::get_state_root_by_slot(&db_pool, &slot).await?;

            let last_matches = if slot == 0 {
                true
            } else {
                let last_stored_state_root =
                    states::get_state_root_by_slot(&db_pool, &(slot - 1)).await?;
                match last_stored_state_root {
                    None => false,
                    Some(last_stored_state_root) => {
                        let previous_on_chain_state_root = beacon_node
                            .get_state_root_by_slot(&(slot - 1))
                            .await?
                            .expect("expect state slot before current head to exist");
                        last_stored_state_root == previous_on_chain_state_root
                    }
                }
            };

            // 1. current slot is empty and last state_root matches.
            if current_slot_stored_state_root.is_none() && last_matches {
                debug!("no state stored for current slot and last slots state_root matches chain");
                sync_state_root(&db_pool, &beacon_node, &on_chain_state_root, &slot).await?;
                progress.inc_work_done();

            // 2. roll back to last matching state_root, queue all slots up to and including
            //    current for sync.
            } else {
                debug!(
                    ?current_slot_stored_state_root,
                    last_matches,
                    "current slot should be empty, last stored slot state_root should match previous on-chain state_root"
                );
                let last_matching_slot =
                    find_last_matching_slot(&db_pool, &beacon_node, &(slot - 1)).await?;
                let first_invalid_slot = last_matching_slot + 1;

                warn!(slot = last_matching_slot, "rolling back to slot");

                rollback_slots(&mut *db_pool.acquire().await?, &first_invalid_slot).await?;

                dbg!("rollback complete");

                for invalid_slot in (first_invalid_slot..=slot).rev() {
                    slots_queue.lock().unwrap().push_front(invalid_slot);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn slot_range_iterable_test() {
        let range = (SlotRange::new(1, 4)).into_iter().collect::<Vec<u32>>();
        assert_eq!(range, vec![1, 2, 3, 4]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_slots_from_test() {
        let slots_stream = stream_slots_from(&4_300_000).await;
        let slots = slots_stream.take(10).collect::<Vec<Slot>>().await;
        assert_eq!(slots.len(), 10);
    }
}
