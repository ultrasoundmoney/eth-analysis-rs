//! Listens to a node, keeps a consistent view of the chain in our own DB and makes sure to
//! propagate updates to this view as needed by dependent modules.
//!
//! We have an existing service handling block syncing, written in TypeScript. A lot of "syncing
//! from a node" code has been rewritten for this newer service. Taking learnings from the existing
//! code, adding more tests, and improving designs. This side should slowly take over more
//! responsibilities.

use futures::{Stream, StreamExt};
use sqlx::PgPool;
use std::{collections::VecDeque, iter::Iterator};
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{IssuanceStore, IssuanceStorePostgres},
    burn_sums, db,
    execution_chain::{self, base_fees, BlockStore, ExecutionNode},
    log,
    performance::TimedExt,
    usd_price,
};

use super::{BlockNumber, LONDON_HARD_FORK_BLOCK_HASH};

async fn rollback_numbers(db_pool: &PgPool, greater_than_or_equal: &BlockNumber) {
    debug!("rolling back data based on numbers gte {greater_than_or_equal}");

    let mut transaction = db_pool.begin().await.unwrap();

    burn_sums::on_rollback(&mut transaction, greater_than_or_equal).await;
    execution_chain::delete_blocks(&mut transaction, greater_than_or_equal).await;

    transaction.commit().await.unwrap();
}

async fn sync_by_hash(
    issuance_store: impl IssuanceStore,
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
    let eth_price = usd_price::get_eth_price_by_block(&mut connection, &block)
        .await
        .expect("eth price close to block to be available");

    execution_chain::store_block(db_pool, &block, eth_price).await;

    // Some computations can be skipped, others should be ran, and rolled back for every change in
    // the chain of blocks we've assembled. These are the ones that are skippable, and so skipped
    // until we're in-sync with the chain again.
    let is_synced = execution_node.get_latest_block().await.hash == hash;
    if is_synced {
        debug!("we're synced, running on_new_head for skippables");
        base_fees::on_new_block(db_pool, issuance_store, &block)
            .timed("base_fees_on_new_block")
            .await;
        burn_sums::on_new_block(db_pool, &block)
            .timed("burn_sums_on_new_block")
            .await;
    } else {
        debug!("not synced, skipping skippables");
    }
}

async fn find_last_matching_block_number(
    execution_node: &mut ExecutionNode,
    block_store: &BlockStore<'_>,
    starting_candidate: BlockNumber,
) -> BlockNumber {
    let mut current_candidate_number = starting_candidate;
    loop {
        let on_chain_block = execution_node
            .get_block_by_number(&current_candidate_number)
            .await
            .unwrap();
        let current_stored_hash = block_store
            .hash_from_number(&current_candidate_number)
            .await
            .unwrap();

        if current_stored_hash == on_chain_block.hash {
            break;
        }

        current_candidate_number -= 1;
    }
    current_candidate_number
}

async fn estimate_blocks_remaining(
    block_store: &BlockStore<'_>,
    execution_node: &mut ExecutionNode,
) -> i32 {
    let last_on_chain = execution_node.get_latest_block().await;
    let last_stored = block_store.last().await;
    last_on_chain.number - last_stored.number
}

pub const EXECUTION_BLOCK_NUMBER_AUG_1ST: BlockNumber = 15253306;

async fn stream_heads_from_last(db: &PgPool) -> impl Stream<Item = BlockNumber> {
    let next_block_to_sync = execution_chain::get_last_block_number(db)
        .await
        .map_or(EXECUTION_BLOCK_NUMBER_AUG_1ST, |number| number + 1);
    execution_chain::stream_heads_from(next_block_to_sync).await
}

type HeadsQueue = VecDeque<BlockNumber>;

// TODO: one unhandled case. Heads stream jumps forwards sometimes. This causes us to roll back one
// block. Then correctly queue the needlessly rolled back block, the missing block, and the one we
// were trying to sync. Check if this is because historical and live heads have a gap.
pub async fn sync_blocks() {
    log::init_with_env();

    info!("syncing execution blocks");

    let db_pool = PgPool::connect(&db::get_db_url_with_name("sync-execution-blocks"))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let mut execution_node = ExecutionNode::connect().await;
    let issuance_store = IssuanceStorePostgres::new(&db_pool);
    let block_store = BlockStore::new(&db_pool);
    let mut heads_stream = stream_heads_from_last(&db_pool).await;
    let mut heads_queue: HeadsQueue = VecDeque::new();

    let blocks_remaining_on_start = estimate_blocks_remaining(&block_store, &mut execution_node)
        .await
        .try_into()
        .unwrap();
    debug!("blocks remaining on start: {}", blocks_remaining_on_start);
    let mut progress = pit_wall::Progress::new("sync-execution-blocks", blocks_remaining_on_start);

    while let Some(head_block_number) = heads_stream.next().await {
        heads_queue.push_back(head_block_number);

        // The heads queue allows us to walk backwards for rollbacks, then forwards to sync what we
        // dropped in the loop below, and then break to continue where we left off in the outer
        // loop, the heads stream.
        while let Some(next_block_number) = heads_queue.pop_front() {
            let next_block = execution_node
                .get_block_by_number(&next_block_number)
                .await
                .expect("expect chain to never get shorter");

            debug!(number = next_block.number, "syncing next block from queue");

            // Either we can add a block, or we need to roll back first. We can add a block when the
            // last stored block matches the one on-chain, and nothing is stored for the current block
            // number. If either condition fails, we need to roll back first, and then sync to the
            // current head.
            let last_stored_block = block_store.last().await;
            let last_matches = if next_block.hash == LONDON_HARD_FORK_BLOCK_HASH {
                true
            } else {
                last_stored_block.hash == next_block.parent_hash
            };
            let current_number_is_free = !block_store.number_exists(&next_block_number).await;

            if last_matches && current_number_is_free {
                // Add to the chain.
                sync_by_hash(
                    &issuance_store,
                    &mut execution_node,
                    &db_pool,
                    &next_block.hash,
                )
                .timed("sync_by_hash")
                .await;
            } else {
                warn!(
                    number = next_block.number,
                    forks_head = !current_number_is_free,
                    parent_mismatch = !last_matches,
                    "next block is not the next block in our copy of the chain, rolling back"
                );

                // Roll back until we're following the canonical chain again, queue all rolled
                // back blocks.
                let last_matching_block_number = find_last_matching_block_number(
                    &mut execution_node,
                    &block_store,
                    last_stored_block.number - 1,
                )
                .await;

                debug!(last_matching_block_number, "rolling back to block number");

                let first_invalid_block_number = last_matching_block_number + 1;

                // Roll back
                rollback_numbers(&db_pool, &first_invalid_block_number).await;

                // Requeue
                for block_number in (first_invalid_block_number..=next_block.number).rev() {
                    debug!(block_number, "requeueing");
                    heads_queue.push_front(block_number);
                }
            }
        }

        progress.inc_work_done();
    }
}
