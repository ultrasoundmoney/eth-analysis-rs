//! Listens to a node, keeps a consistent view of the chain in our own DB and makes sure to
//! propagate updates to this view as needed by dependent modules.
//!
//! We have an existing service handling block syncing, written in TypeScript. A lot of "syncing
//! from a node" code has been rewritten for this newer service. Taking learnings from the existing
//! code, adding more tests, and improving designs. This side should slowly take over more
//! responsibilities.
//!
//! For now, blocks are stored in-memory only.
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::VecDeque;
use std::iter::Iterator;
use std::sync::Arc;
use std::sync::Mutex;

use super::merge_countdown;
use super::node::BlockNumber;
use super::node::Head;
use crate::config;
use crate::execution_chain;
use crate::execution_chain::block_store::BlockStore;
use crate::execution_chain::block_store::MemoryBlockStore;
use crate::execution_chain::ExecutionNode;
use crate::performance::TimedExt;

async fn rollback_numbers<'a>(store: &mut impl BlockStore, greater_than_or_equal: &BlockNumber) {
    tracing::debug!("rolling back data based on numbers gte {greater_than_or_equal}");
    let last_block = store.get_last_block_number();
    match last_block {
        None => {
            tracing::warn!(
                "asked to rollback numbers gte {greater_than_or_equal} but BlockStore is empty"
            );
        }
        Some(last_block_number) => {
            for block_number in (*greater_than_or_equal..=last_block_number).rev() {
                store.delete_blocks(&block_number)
            }
        }
    }
}

async fn sync_by_hash<'a>(
    block_store: &mut impl BlockStore,
    execution_node: &mut ExecutionNode,
    executor: &PgPool,
    hash: &str,
) {
    let block = execution_node
        .get_block_by_hash(hash)
        .await
        .expect("block not to disappear between deciding to add it and adding it");

    // Between the time we received the head event and requested a header for the given
    // block_root the block may have disappeared. Right now we panic, we could do better.
    block_store.store_block(block.clone());

    let is_synced = execution_node.get_latest_block().await.hash == hash;
    // Some computations can be skipped, others should be ran, and rolled back for every change in
    // the chain of blocks we've assembled. These are the ones that are skippable, and so skipped
    // until we're in-sync with the chain again.
    if is_synced {
        tracing::debug!("we're synced, running on_new_head for skippables");
        merge_countdown::on_new_head(executor, &block).await;
    }
}

enum NextStep {
    HandleGap,
    HandleHeadFork,
    AddToExisting,
}

fn get_next_step(block_store: &impl BlockStore, head: &Head) -> NextStep {
    // Between the time we received the head event and requested a header for the given
    // block_root the block may have disappeared. Right now we panic, we could do better.
    let is_parent_known = block_store.get_is_parent_hash_known(&head.parent_hash);

    if !is_parent_known {
        return NextStep::HandleGap;
    }

    let is_fork_block = block_store.get_is_number_known(&head.number);

    if is_fork_block {
        return NextStep::HandleHeadFork;
    }

    NextStep::AddToExisting
}

async fn sync_head(
    store: &mut impl BlockStore,
    execution_node: &mut ExecutionNode,
    db_pool: &PgPool,
    heads_queue: HeadsQueue,
    head_to_sync: HeadToSync,
) {
    let head_event = match head_to_sync {
        HeadToSync::Fetched(head) => head,
        HeadToSync::Refetch(block_number) => execution_node
            .get_block_by_number(&block_number)
            .await
            .expect("chain not to get shorter since scheduling refetch head sync")
            .into(),
    };

    tracing::debug!("sync head from number: {}", head_event.number);

    match get_next_step(store, &head_event) {
        NextStep::HandleGap => {
            tracing::warn!("parent of block at block_number {} is missing, dropping min(our last block.block_number, new block.block_number) and queueing all blocks gte the received block, block: {}", head_event.number, head_event.hash);

            let last_block_number = store
                .get_last_block_number()
                .expect("at least one block to be synced before rolling back");

            // Head number may be lower than our last synced. Roll back gte lowest of the two.
            let lowest_number = last_block_number.min(head_event.number);

            rollback_numbers(store, &lowest_number).await;

            for number in (lowest_number..=head_event.number).rev() {
                tracing::debug!("queueing {number} for sync after dropping");
                heads_queue
                    .lock()
                    .unwrap()
                    .push_front(HeadToSync::Refetch(number));
            }
        }
        NextStep::HandleHeadFork => {
            tracing::info!(
                "block at number {} creates a fork, rolling back our last block - {}",
                head_event.number,
                head_event.hash
            );

            rollback_numbers(store, &head_event.number).await;

            heads_queue
                .lock()
                .unwrap()
                .push_front(HeadToSync::Fetched(head_event))
        }
        NextStep::AddToExisting => {
            sync_by_hash(store, execution_node, db_pool, &head_event.hash)
                .timed("sync numbers")
                .await;
        }
    }
}

#[derive(Clone, Debug)]
enum HeadToSync {
    Fetched(Head),
    Refetch(BlockNumber),
}
type HeadsQueue = Arc<Mutex<VecDeque<HeadToSync>>>;

pub async fn sync_blocks() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing execution blocks");

    let db_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config::get_db_url_with_name("sync-execution-blocks"))
        .await
        .unwrap();

    let mut execution_node = ExecutionNode::connect().await;

    let mut heads_stream = execution_chain::stream_new_heads();

    let heads_queue: HeadsQueue = Arc::new(Mutex::new(VecDeque::new()));

    let mut block_store = MemoryBlockStore::new();

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
                    .await;
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::execution_chain::node::ExecutionNodeBlock;

    use super::*;

    #[tokio::test]
    async fn rollback_last_first_test() {
        let mut block_store = MemoryBlockStore::new();

        block_store.store_block(ExecutionNodeBlock {
            difficulty: 0,
            hash: "0xhash".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            total_difficulty: 0,
        });

        block_store.store_block(ExecutionNodeBlock {
            difficulty: 0,
            hash: "0xhash2".to_string(),
            number: 1,
            parent_hash: "0xhash".to_string(),
            timestamp: Utc::now(),
            total_difficulty: 0,
        });

        rollback_numbers(&mut block_store, &0).await;

        // This should blow up if the order is backwards but its not obvious how. Consider using
        // mockall to create a mock instance of block_store so we can observe whether
        // rollback_numbers is calling it correctly.
    }
}
