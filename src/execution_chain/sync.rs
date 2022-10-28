//! Listens to a node, keeps a consistent view of the chain in our own DB and makes sure to
//! propagate updates to this view as needed by dependent modules.
//!
//! We have an existing service handling block syncing, written in TypeScript. A lot of "syncing
//! from a node" code has been rewritten for this newer service. Taking learnings from the existing
//! code, adding more tests, and improving designs. This side should slowly take over more
//! responsibilities.

use anyhow::Result;
use futures::{SinkExt, Stream, StreamExt};
use sqlx::{PgConnection, PgPool};
use std::{
    cmp::Ordering,
    collections::VecDeque,
    iter::Iterator,
    sync::{Arc, Mutex},
};
use tracing::{debug, event, info, warn, Level};

use super::{
    eth_prices,
    node::{BlockNumber, Head},
};
use crate::{
    db, eth_supply,
    execution_chain::{self, base_fees, block_store::BlockStore, ExecutionNode},
    log,
    performance::TimedExt,
};

async fn rollback_numbers<'a>(
    executor: &mut PgConnection,
    block_store: &mut BlockStore<'a>,
    greater_than_or_equal: &BlockNumber,
) -> Result<()> {
    debug!("rolling back data based on numbers gte {greater_than_or_equal}");
    eth_supply::rollback_supply_by_block(executor, greater_than_or_equal).await?;
    block_store.delete_blocks(&greater_than_or_equal).await;
    Ok(())
}

async fn sync_by_hash(
    block_store: &mut BlockStore<'_>,
    execution_node: &mut ExecutionNode,
    db_pool: &PgPool,
    hash: &str,
) {
    let block = execution_node
        .get_block_by_hash(hash)
        .await
        // Between the time we received the head event and requested a header for the given
        // block_root the block may have disappeared. Right now we panic, we could do better.
        .expect("block not to disappear between deciding to add it and adding it");

    let mut connection = db_pool.acquire().await.unwrap();
    let eth_price = eth_prices::get_eth_price_by_block(&mut *connection, &block)
        .await
        .expect("eth price close to block to be available");

    block_store.store_block(&block, eth_price).await;

    // Some computations can be skipped, others should be ran, and rolled back for every change in
    // the chain of blocks we've assembled. These are the ones that are skippable, and so skipped
    // until we're in-sync with the chain again.
    let is_synced = execution_node.get_latest_block().await.hash == hash;
    if is_synced {
        debug!("we're synced, running on_new_head for skippables");
        base_fees::on_new_block(db_pool, &block).await.unwrap();
    }
}

enum NextStep {
    HandleGap,
    HandleHeadFork,
    AddToExisting,
}

async fn get_next_step(block_store: &mut BlockStore<'_>, head: &Head) -> NextStep {
    // Between the time we received the head event and requested a header for the given
    // block_root the block may have disappeared. Right now we panic, we could do better.
    let is_parent_known = block_store
        .get_is_parent_hash_known(&head.parent_hash)
        .await;

    if !is_parent_known {
        return NextStep::HandleGap;
    }

    let is_fork_block = block_store.get_is_number_known(&head.number).await;

    if is_fork_block {
        return NextStep::HandleHeadFork;
    }

    NextStep::AddToExisting
}

async fn sync_head(
    store: &mut BlockStore<'_>,
    execution_node: &mut ExecutionNode,
    db_pool: &PgPool,
    heads_queue: HeadsQueue,
    head_to_sync: HeadToSync,
) -> Result<()> {
    let head_event = match head_to_sync {
        HeadToSync::Fetched(head) => head,
        HeadToSync::Refetch(block_number) => execution_node
            .get_block_by_number(&block_number)
            .await
            .expect("chain not to get shorter since scheduling refetch head sync")
            .into(),
    };

    debug!("sync head from number: {}", head_event.number);

    match get_next_step(store, &head_event).await {
        NextStep::HandleGap => {
            warn!("parent of block at block_number {} is missing, dropping min(our last block.block_number, new block.block_number) and queueing all blocks gte the received block, block: {}", head_event.number, head_event.hash);

            let last_block_number = store
                .get_last_block_number()
                .await
                .expect("at least one block to be synced before rolling back");

            // Head number may be lower than our last synced. Roll back gte lowest of the two.
            let lowest_number = last_block_number.min(head_event.number);

            rollback_numbers(&mut db_pool.acquire().await.unwrap(), store, &lowest_number).await?;

            for number in (lowest_number..=head_event.number).rev() {
                debug!("queueing {number} for sync after dropping");
                heads_queue
                    .lock()
                    .unwrap()
                    .push_front(HeadToSync::Refetch(number));
            }
        }
        NextStep::HandleHeadFork => {
            info!(
                "block at number {} creates a fork, rolling back our last block - {}",
                head_event.number, head_event.hash
            );

            rollback_numbers(
                &mut db_pool.acquire().await.unwrap(),
                store,
                &head_event.number,
            )
            .await?;

            heads_queue
                .lock()
                .unwrap()
                .push_front(HeadToSync::Fetched(head_event))
        }
        NextStep::AddToExisting => {
            sync_by_hash(store, execution_node, db_pool, &head_event.hash)
                .timed("sync block by hash")
                .await;
        }
    };

    Ok(())
}

#[derive(Clone)]
pub struct BlockRange {
    pub greater_than_or_equal: BlockNumber,
    pub less_than_or_equal: BlockNumber,
}

impl BlockRange {
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

pub struct BlockRangeIntoIterator {
    block_range: BlockRange,
    index: usize,
}

impl IntoIterator for BlockRange {
    type Item = u32;
    type IntoIter = BlockRangeIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            block_range: self,
            index: 0,
        }
    }
}

impl Iterator for BlockRangeIntoIterator {
    type Item = BlockNumber;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.block_range.greater_than_or_equal as usize + self.index)
            .cmp(&(self.block_range.less_than_or_equal as usize))
        {
            Ordering::Less => {
                let current = self.block_range.greater_than_or_equal + self.index as u32;
                self.index = self.index + 1;
                Some(current)
            }
            Ordering::Equal => {
                let current = self.block_range.greater_than_or_equal + self.index as u32;
                self.index = self.index + 1;
                Some(current)
            }
            Ordering::Greater => None,
        }
    }
}

fn get_historic_stream(block_range: BlockRange) -> impl Stream<Item = Head> {
    let (mut tx, rx) = futures::channel::mpsc::channel(10);

    tokio::spawn(async move {
        let mut execution_node = ExecutionNode::connect().await;
        for block_number in block_range.into_iter() {
            let block = execution_node
                .get_block_by_number(&block_number)
                .await
                .unwrap();
            tx.send(block.into()).await.unwrap();
        }
    });

    rx
}

async fn stream_heads_from(gte_slot: BlockNumber) -> impl Stream<Item = Head> {
    event!(Level::DEBUG, "streaming heads from {gte_slot}");

    let mut execution_node = ExecutionNode::connect().await;
    let last_block_on_start = execution_node.get_latest_block().await;
    event!(
        Level::DEBUG,
        "last block on chain: {}",
        &last_block_on_start.number
    );

    // We stream heads as requested until caught up with the chain and then pass heads as they come
    // in from our node. The only way to be sure how high we should stream, is to wait for the
    // first head from the node to come in. We don't want to wait. So ask for the latest head, take
    // this as the max, and immediately start listening for new heads. Running the small risk the
    // chain has advanced between these two calls.
    let heads_stream = execution_chain::stream_new_heads();

    let block_range = BlockRange::new(gte_slot, last_block_on_start.number - 1);

    let historic_heads_stream = get_historic_stream(block_range);

    historic_heads_stream.chain(heads_stream)
}

pub const EXECUTION_BLOCK_NUMBER_AUG_1ST: u32 = 15253306;

async fn stream_heads_from_last(db: &PgPool) -> impl Stream<Item = Head> {
    let mut connection = db.acquire().await.unwrap();
    let mut block_store = BlockStore::new(&mut *connection);
    let next_block_to_sync = block_store
        .get_last_block_number()
        .await
        .map_or(EXECUTION_BLOCK_NUMBER_AUG_1ST, |number| number + 1);
    stream_heads_from(next_block_to_sync).await
}

#[derive(Clone, Debug)]
enum HeadToSync {
    Fetched(Head),
    Refetch(BlockNumber),
}
type HeadsQueue = Arc<Mutex<VecDeque<HeadToSync>>>;

pub async fn sync_blocks() -> Result<()> {
    log::init_with_env();

    info!("syncing execution blocks");

    let db_pool = PgPool::connect(&db::get_db_url_with_name("sync-execution-blocks"))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let mut execution_node = ExecutionNode::connect().await;

    let mut connection = db_pool.acquire().await.unwrap();
    let mut block_store = BlockStore::new(&mut connection);

    let mut heads_stream = stream_heads_from_last(&db_pool).await;

    let heads_queue: HeadsQueue = Arc::new(Mutex::new(VecDeque::new()));

    while let Some(head_event) = heads_stream.next().await {
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
                    sync_head(
                        &mut block_store,
                        &mut execution_node,
                        &db_pool,
                        heads_queue.clone(),
                        next_head,
                    )
                    .timed("sync head")
                    .await?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sqlx::Acquire;

    use crate::execution_chain::node::ExecutionNodeBlock;

    use super::*;

    #[tokio::test]
    async fn rollback_last_first_test() -> Result<()> {
        // This test should use transiactions somehow.
        let db = PgPool::connect(&db::get_db_url_with_name("execution-sync-test")).await?;
        let mut connection = db.acquire().await.unwrap();
        let mut block_store = BlockStore::new(&mut connection);

        block_store
            .store_block(
                &ExecutionNodeBlock {
                    base_fee_per_gas: 0,
                    difficulty: 0,
                    gas_used: 0,
                    hash: "0xtest".to_string(),
                    number: 0,
                    parent_hash: "0xparent".to_string(),
                    timestamp: Utc::now(),
                    total_difficulty: 0,
                },
                0.0,
            )
            .await;

        block_store
            .store_block(
                &ExecutionNodeBlock {
                    base_fee_per_gas: 0,
                    difficulty: 0,
                    gas_used: 0,
                    hash: "0xhash2".to_string(),
                    number: 1,
                    parent_hash: "0xhash".to_string(),
                    timestamp: Utc::now(),
                    total_difficulty: 0,
                },
                0.0,
            )
            .await;

        rollback_numbers(&mut db.acquire().await.unwrap(), &mut block_store, &0).await?;

        // This should blow up if the order is backwards but its not obvious how. Consider using
        // mockall to create a mock instance of block_store so we can observe whether
        // rollback_numbers is calling it correctly.

        Ok(())
    }

    #[test]
    fn block_range_iterable_test() {
        let range = (BlockRange::new(1, 4)).into_iter().collect::<Vec<u32>>();
        assert_eq!(range, vec![1, 2, 3, 4]);
    }
}
