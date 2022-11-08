use anyhow::{anyhow, Result};
use balances::BeaconBalancesSum;
use chrono::Duration;
use futures::{SinkExt, Stream, StreamExt};
use lazy_static::lazy_static;
use serde::Deserialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use sqlx::{Acquire, PgConnection, Row};
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
        blocks::get_last_block_slot,
        deposits, issuance,
    },
    db, eth_supply,
    eth_units::GweiNewtype,
    json_codecs::from_u32_string,
    log,
    performance::TimedExt,
};

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
pub async fn sync_slot(db_pool: &PgPool, beacon_node: &BeaconNode, slot: &u32) -> Result<()> {
    let state_root = beacon_node
        .get_state_root_by_slot(&slot)
        .await?
        .expect("can't handle reorg of chain whilst syncing slot");

    let header_block_tuple = {
        let header = beacon_node.get_header_by_slot(&slot).await?;
        match header {
            None => None,
            Some(header) => {
                let block = beacon_node
                    .get_block_by_block_root(&header.root)
                    .await
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
    let is_synced = last_header.header.message.state_root == state_root;
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

async fn sync_slots(
    db_pool: &PgPool,
    beacon_node: &BeaconNode,
    slot_range: &SlotRange,
) -> Result<()> {
    debug!(
        "syncing slots gte {}, and lte {}, {} slots total",
        slot_range.greater_than_or_equal,
        slot_range.less_than_or_equal,
        slot_range.less_than_or_equal - slot_range.greater_than_or_equal + 1
    );

    for slot in slot_range.greater_than_or_equal..=slot_range.less_than_or_equal {
        sync_slot(db_pool, beacon_node, &slot)
            .timed("sync slot")
            .await?;
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

async fn get_is_slot_known(connection: &mut PgConnection, slot: &Slot) -> bool {
    sqlx::query(
        r#"
            SELECT EXISTS (
                SELECT 1
                FROM beacon_states
                WHERE slot = $1
            )
        "#,
    )
    .bind(*slot as i32)
    .fetch_one(connection)
    .await
    .unwrap()
    .get("exists")
}

pub async fn rollback_slots(
    executor: &mut PgConnection,
    greater_than_or_equal: &Slot,
) -> Result<()> {
    debug!("rolling back data based on slots gte {greater_than_or_equal}");
    let mut transaction = executor.begin().await?;
    eth_supply::rollback_supply_by_slot(&mut transaction, greater_than_or_equal)
        .await
        .unwrap();
    blocks::delete_blocks(&mut transaction, greater_than_or_equal).await;
    issuance::delete_issuances(&mut transaction, greater_than_or_equal).await;
    balances::delete_validator_sums(&mut transaction, greater_than_or_equal).await;
    states::delete_states(&mut transaction, greater_than_or_equal).await;
    transaction.commit().await?;
    Ok(())
}

enum NextStep {
    HandleGap,
    HandleHeadFork,
    AddToExisting,
}

async fn get_next_step(
    connection: &mut PgConnection,
    beacon_node: &BeaconNode,
    head_event: &HeadEvent,
) -> NextStep {
    // Between the time we received the head event and requested a header for the given
    // block_root the block may have disappeared. Right now we panic, we could do better.
    let head = beacon_node
        .get_header_by_hash(&head_event.block)
        .await
        .unwrap()
        .unwrap();

    let is_parent_known = blocks::get_is_hash_known(
        connection.acquire().await.unwrap(),
        &head.header.message.parent_root,
    )
    .await;

    if !is_parent_known {
        return NextStep::HandleGap;
    }

    let is_fork_block = get_is_slot_known(
        connection.acquire().await.unwrap(),
        &head.header.message.slot,
    )
    .await;

    if is_fork_block {
        return NextStep::HandleHeadFork;
    }

    NextStep::AddToExisting
}

async fn ensure_last_synced_slot_has_block(executor: &mut PgConnection) -> Result<()> {
    debug!("ensuring we start syncing on a slot with a block");

    let last_state = states::get_last_state(executor.acquire().await?).await;

    match last_state {
        // Never synced a slot, done!
        None => {
            debug!("never synced a slot, no rollback needed");
        }
        Some(last_state) => {
            let last_block = blocks::get_last_block_slot(executor).await;
            match last_block {
                // Synced a slot but never a block, done!
                None => {
                    debug!(
                        last_state.slot,
                        "synced slots, but never blocks, rollback to genesis"
                    );
                    rollback_slots(executor, &0).await?;
                }
                Some(last_block) => {
                    // Synced a slot and a block, are slots ahead of blocks?
                    match last_state.slot.cmp(&last_block) {
                        Ordering::Less => {
                            panic!("we've synced less slots than blocks, this should never happen!")
                        }
                        Ordering::Equal => debug!(
                            state_slot = last_state.slot,
                            block_slot = last_block,
                            "equal state and block slot, no rollback needed"
                        ),
                        Ordering::Greater => {
                            debug!("states are ahead of blocks, rolling back to last block slot");
                            rollback_slots(executor, &(last_block + 1)).await?;
                        }
                    };
                }
            };
        }
    };

    Ok(())
}

async fn sync_head(
    db_pool: &PgPool,
    beacon_node: &BeaconNode,
    heads_queue: HeadsQueue,
    head_to_sync: HeadToSync,
) -> Result<()> {
    let head_event = match head_to_sync {
        HeadToSync::Fetched(head) => head,
        HeadToSync::Refetch(slot) => {
            // Between the time we received the head event and requested a header for the given
            // block_root the block may have disappeared. Right now we panic, we could simply
            // return an error instead of result, log a warning, and continue.
            let header = beacon_node.get_header_by_slot(&slot).await?.unwrap();
            header.into()
        }
    };

    debug!(slot = head_event.slot, "sync head from slot");

    match get_next_step(
        &mut db_pool.acquire().await.unwrap(),
        beacon_node,
        &head_event,
    )
    .timed("get next step")
    .await
    {
        NextStep::HandleGap => {
            warn!(slot = head_event.slot, received_block = head_event.block, "parent of block of slot is missing, dropping min(our last block.slot, new block.slot) and queueing all blocks gte the received block");

            let last_block_slot = get_last_block_slot(&mut db_pool.acquire().await.unwrap())
                .await
                .expect("at least one block to be synced before rolling back");

            // Head slot may be lower than our last synced. Roll back gte lowest of the two.
            let lowest_slot = last_block_slot.min(head_event.slot);

            rollback_slots(&mut *db_pool.acquire().await.unwrap(), &lowest_slot).await?;

            for slot in (lowest_slot..=head_event.slot).rev() {
                debug!("queueing {slot} for sync after dropping");
                heads_queue
                    .lock()
                    .unwrap()
                    .push_front(HeadToSync::Refetch(slot));
            }
        }
        NextStep::HandleHeadFork => {
            info!(
                slot = head_event.slot,
                last_block = head_event.block,
                "block at slot creates a fork, rolling back our last block",
            );

            rollback_slots(&mut db_pool.acquire().await.unwrap(), &head_event.slot).await?;

            heads_queue
                .lock()
                .unwrap()
                .push_front(HeadToSync::Fetched(head_event))
        }
        NextStep::AddToExisting => {
            let next_slot_to_sync = get_last_block_slot(&mut db_pool.acquire().await.unwrap())
                .timed("get last block slot")
                .await
                .map_or(0, |slot| slot + 1);

            sync_slots(
                &db_pool,
                beacon_node,
                &SlotRange {
                    greater_than_or_equal: next_slot_to_sync,
                    less_than_or_equal: head_event.slot,
                },
            )
            .timed("sync slots")
            .await?;
        }
    };

    Ok(())
}

async fn estimate_slots_remaining<'a>(
    executor: &mut PgConnection,
    beacon_node: &BeaconNode,
) -> u32 {
    let last_on_chain = beacon_node.get_last_block().await.unwrap();
    let last_synced_slot = states::get_last_state(executor)
        .await
        .map_or(0, |state| state.slot);
    last_on_chain.slot - last_synced_slot
}

#[derive(Clone, Debug, PartialEq)]
enum HeadToSync {
    Fetched(HeadEvent),
    Refetch(Slot),
}
type HeadsQueue = Arc<Mutex<VecDeque<HeadToSync>>>;

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

    ensure_last_synced_slot_has_block(&mut db_pool.acquire().await.unwrap()).await?;

    let mut heads_stream = stream_heads_from_last(&db_pool).await;

    let heads_queue: HeadsQueue = Arc::new(Mutex::new(VecDeque::new()));

    let mut progress = pit_wall::Progress::new(
        "sync beacon states",
        estimate_slots_remaining(&mut db_pool.acquire().await.unwrap(), &beacon_node)
            .await
            .into(),
    );

    while let Some(head_event) = heads_stream.next().await {
        if &head_event.slot % 100 == 0 {
            tracing::debug!("{}", progress.get_progress_string());
        }

        heads_queue
            .lock()
            .unwrap()
            .push_back(HeadToSync::Fetched(head_event));

        // Work through the heads queue until it's empty and we're ready to move the next head from
        // the stream to the queue.
        loop {
            let next_head = { heads_queue.lock().unwrap().pop_front() };
            match next_head {
                None => {
                    // Continue syncing heads from the stream.
                    break;
                }
                Some(next_head) => {
                    // Because we may encounter rollbacks, this step may add more heads to sync to
                    // the front of the queue.
                    sync_head(&db_pool, &beacon_node, heads_queue.clone(), next_head)
                        .timed("sync head")
                        .await?;
                }
            }

            progress.inc_work_done();
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
