use anyhow::{anyhow, Result};
use balances::BeaconBalancesSum;
use chrono::Duration;
use futures::{SinkExt, Stream, StreamExt};
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
    beacon_chain::{
        balances,
        beacon_time::{self, FirstOfDaySlot},
        deposits, issuance,
    },
    db, eth_supply,
    eth_units::GweiNewtype,
    json_codecs::from_u32_string,
    log,
    performance::TimedExt,
};

use super::node::StateRoot;
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
    state_root: &str,
    slot: &u32,
    header: &BeaconHeaderSignedEnvelope,
    deposit_sum: &GweiNewtype,
    deposit_sum_aggregated: &GweiNewtype,
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

    states::store_state(&mut *executor, state_root, slot).await?;

    blocks::store_block(
        &mut *executor,
        state_root,
        header,
        deposit_sum,
        deposit_sum_aggregated,
    )
    .await;

    Ok(())
}

// TODO: start storing validator balances.
// TODO: extract data fetching into own function.
// TODO: rewrite to have simpler, have we stored this info for the first slot of the day with a
// block? If no, store, if yes, skip.
pub async fn sync_state_root(
    db_pool: &PgPool,
    beacon_node: &BeaconNode,
    state_root: &StateRoot,
    slot: &Slot,
) -> Result<()> {
    let header_block_tuple = {
        let header = beacon_node.get_header_by_slot(&slot).await?;
        match header {
            None => None,
            Some(header) => {
                // There is no way to request a header by a state_root, instead we retrieve it
                // using the slot and then check the state_root still matches to verify we didn't
                // fork. This could be improved syncing specifically for a slot without a header,
                // or with a header, providing the block root as an argument.
                if header.header.message.state_root != *state_root {
                    return Err(anyhow!("current header for slot {}, no longer matches the state_root {} we're syncing for it", slot, state_root));
                }
                let block = beacon_node
                    .get_block_by_block_root(&header.root)
                    .await?
                    // A reorg means it is possible to have fetched a header and then fail to fetch
                    // a block for the same state_root.
                    .expect("expect a block when header has been fetched");
                Some((header, block))
            }
        }
    };
    let last_header = beacon_node.get_last_header().await?;

    // If we are falling too far behind the head of the chain, skip storing validator balances.
    let sync_lag = {
        let last_on_chain_slot = last_header.header.message.slot;
        let last_on_chain_slot_date_time =
            beacon_time::get_date_time_from_slot(&last_on_chain_slot);
        let slot_date_time = beacon_time::get_date_time_from_slot(&slot);
        last_on_chain_slot_date_time - slot_date_time
    };

    debug!(%sync_lag, "beacon sync lag");

    let start_of_day_date_time = FirstOfDaySlot::new(&slot);

    // Whenever we fall behind, getting validator balances for older slots from lighthouse takes a
    // long time. This means if we fall behind too far we never catch up, as syncing one slot now
    // takes longer than it takes for a new slot to appear (12 seconds).
    let validator_balances = {
        // Always get validator balances for a start of day slot.
        if start_of_day_date_time.is_some() {
            let validator_balances = beacon_node
                .get_validator_balances(&state_root)
                .await?
                // The only reason a state_root does not have balances is because it reorged.
                // We can't handle reorgs here. In the future we should cleanly return an error
                // so that the sync mechanism may cleanly recover instead of crashing, and then
                // cleanly recovering.
                .expect("expect validator balances to exist for given state_root");
            Some(validator_balances)
        // If this isn't a start of day slot and we're too far behind the head of the
        // chain, skip the validator balances dependent logic.
        } else if sync_lag > *BLOCK_LAG_LIMIT {
            warn!(
                %sync_lag,
                "block lag over limit, skipping get validator balances"
            );
            None
        // Otherwise, get the validator balances.
        } else {
            let validator_balances = beacon_node
                .get_validator_balances(&state_root)
                .await?
                .expect("expect validator balances to exist for given state_root");
            Some(validator_balances)
        }
    };

    // If the slot is the first of the day, we want to calculate things which require a block. It'd
    // be nice to rewrite to a sync_slot and sync_first_of_day_slot where the later requires a slot
    // with a block or otherwise uses data from the last slot instead of crashing here.
    if let Some(_) = FirstOfDaySlot::new(slot) {
        assert!(
            validator_balances.is_some(),
            "validator balances are required to sync a first of day slot"
        );

        assert!(
            header_block_tuple.is_some(),
            "block is required to sync a first of day slot"
        );
    }

    // Now that we have all the data, we start storing it.

    let mut transaction = db_pool.begin().await?;

    match &header_block_tuple {
        None => {
            debug!(
                "storing slot without block, slot: {:?}, state_root: {}",
                slot, state_root
            );
            states::store_state(&mut transaction, &state_root, slot)
                .timed("store state without block")
                .await?;
        }
        Some((header, block)) => {
            let deposit_sum_aggregated =
                deposits::get_deposit_sum_aggregated(&mut transaction, &block).await?;

            debug!(slot, state_root, "storing slot with block");
            store_state_with_block(
                &mut transaction,
                &state_root,
                slot,
                &header,
                &deposits::get_deposit_sum_from_block(&block),
                &deposit_sum_aggregated,
            )
            .timed("store state with block")
            .await?;
        }
    }

    if let Some(start_of_day_date_time) = FirstOfDaySlot::new(slot) {
        let validator_balances = validator_balances
            .as_ref()
            .expect("expect validator balances for first of day slot");
        let (_, block) = header_block_tuple.expect("expect block for first of day slot");
        let validator_balances_sum = balances::sum_validator_balances(validator_balances);
        balances::store_validator_sum_for_day(
            &mut transaction,
            &state_root,
            &start_of_day_date_time,
            &validator_balances_sum,
        )
        .await;

        let deposit_sum_aggregated =
            deposits::get_deposit_sum_aggregated(&mut transaction, &block).await?;
        issuance::store_issuance_for_day(
            &mut transaction,
            &state_root,
            &start_of_day_date_time,
            &issuance::calc_issuance(&validator_balances_sum, &deposit_sum_aggregated),
        )
        .await;
    }

    if let Some(validator_balances) = &validator_balances {
        let validator_balances_sum = balances::sum_validator_balances(validator_balances);
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: validator_balances_sum,
            slot: slot.clone(),
        };

        let eth_supply_parts =
            eth_supply::get_supply_parts(&mut transaction, &beacon_balances_sum).await?;

        eth_supply::update(&mut transaction, &eth_supply_parts).await?;
    }

    transaction.commit().await?;

    let last_header = beacon_node.get_last_header().await?;
    let is_synced = last_header.header.message.state_root == *state_root;
    // We can't skip data collection but we can skip data analysis whenever we fall behind the head
    // of the chain.
    if is_synced {
        debug!("synced with the head of the chain, computing analysis");

        if let Some(validator_balances) = &validator_balances {
            let validator_balances_sum = balances::sum_validator_balances(validator_balances);
            let beacon_balances_sum = BeaconBalancesSum {
                balances_sum: validator_balances_sum,
                slot: slot.clone(),
            };
            let eth_supply_parts =
                eth_supply::get_supply_parts(&mut *db_pool.acquire().await?, &beacon_balances_sum)
                    .await?;
            eth_supply::update_caches(db_pool, &eth_supply_parts).await?;
        }
    } else {
        debug!("not synced with the head of the chain, skipping analysis")
    }

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

async fn stream_heads() -> impl Stream<Item = HeadEvent> {
    let url_string = format!("{}/eth/v1/events/?topics=head", *BEACON_URL);
    let url = reqwest::Url::parse(&url_string).unwrap();

    let client = eventsource::reqwest::Client::new(url);
    let (mut tx, rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        for event in client {
            let event = event.unwrap();
            match event.event_type {
                Some(ref event_type) if event_type == "head" => {
                    let head = serde_json::from_str::<HeadEvent>(&event.data).unwrap();

                    debug!("received new head {:#?}", head);

                    tx.send(head).await.unwrap();
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

fn get_historic_stream(slot_range: SlotRange) -> impl Stream<Item = HeadEvent> {
    let beacon_node = BeaconNode::new();
    let (mut tx, rx) = futures::channel::mpsc::channel(10);

    tokio::spawn(async move {
        for slot in slot_range.into_iter() {
            let header = beacon_node.get_header_by_slot(&slot).await.unwrap();
            match header {
                None => continue,
                Some(header) => {
                    let head_event = HeadEvent {
                        slot: header.header.message.slot,
                        block: header.root,
                        state: header.header.message.state_root,
                    };
                    tx.send(head_event).await.unwrap();
                }
            };
        }
    });

    rx
}

async fn stream_heads_from(gte_slot: Slot) -> impl Stream<Item = HeadEvent> {
    debug!("streaming heads from {gte_slot}");

    let beacon_node = BeaconNode::new();
    let last_block_on_start = beacon_node.get_last_block().await.unwrap();
    debug!("last block on chain: {}", &last_block_on_start.slot);

    // We stream heads as requested until caught up with the chain and then pass heads as they come
    // in from our node. The only way to be sure how high we should stream, is to wait for the
    // first head from the node to come in. We don't want to wait. So ask for the latest head, take
    // this as the max, and immediately start listening for new heads. Running the small risk the
    // chain has advanced between these two calls.
    let heads_stream = stream_heads().await;

    let slot_range = SlotRange::new(gte_slot, last_block_on_start.slot - 1);

    let historic_heads_stream = get_historic_stream(slot_range);

    historic_heads_stream.chain(heads_stream)
}

async fn stream_heads_from_last(db: &PgPool) -> impl Stream<Item = HeadEvent> {
    let last_synced_state = states::get_last_state(&mut db.acquire().await.unwrap()).await;
    let next_slot_to_sync = last_synced_state.map_or(0, |state| state.slot + 1);
    stream_heads_from(next_slot_to_sync).await
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

    let mut heads_stream = stream_heads_from_last(&db_pool).await;

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

    while let Some(head_event) = heads_stream.next().await {
        if &head_event.slot % 100 == 0 {
            debug!("{}", progress.get_progress_string());
        }

        slots_queue.lock().unwrap().push_back(head_event.slot);

        // Work through the slots queue until it's empty and we're ready to move the next head from
        // the stream to the queue.
        while let Some(slot) = slots_queue.lock().unwrap().pop_front() {
            let on_chain_state_root = beacon_node
                .get_header_by_slot(&slot)
                .await?
                .expect("expect header to exist for slot to sync from head event")
                .header
                .message
                .state_root;
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
                            .get_header_by_slot(&(slot - 1))
                            .await?
                            .expect("expect state slot before current head to exist")
                            .header
                            .message
                            .state_root;
                        last_stored_state_root == previous_on_chain_state_root
                    }
                }
            };

            // 1. current slot is empty and last state_root matches.
            if current_slot_stored_state_root.is_none() && last_matches {
                sync_state_root(&db_pool, &beacon_node, &on_chain_state_root, &slot).await?;
                progress.inc_work_done();

            // 2. roll back to last matching state_root, queue all slots up to and including
            //    current for sync.
            } else {
                let last_matching_slot =
                    find_last_matching_slot(&db_pool, &beacon_node, &(slot - 1)).await?;
                let first_invalid_slot = last_matching_slot + 1;

                rollback_slots(&mut *db_pool.acquire().await?, &first_invalid_slot).await?;

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

    // Disabled because dropping the stream results in send errors crashing the thread and failing
    // our test.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn stream_heads_from_test() {
        let heads_stream = stream_heads_from(4_300_000).await;
        let heads = heads_stream.take(10).collect::<Vec<HeadEvent>>().await;
        assert_eq!(heads.len(), 10);
    }
}
