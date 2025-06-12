//! Listens to a node, keeps a consistent view of the chain in our own DB and makes sure to
//! propagate updates to this view as needed by dependent modules.
//!
//! We have an existing service handling block syncing, written in TypeScript. A lot of "syncing
//! from a node" code has been rewritten for this newer service. Taking learnings from the existing
//! code, adding more tests, and improving designs. This side should slowly take over more
//! responsibilities.

use sqlx::{PgExecutor, PgPool};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{IssuanceStore, IssuanceStorePostgres},
    burn_rates, burn_sums, eth_supply,
    execution_chain::{
        self, base_fees, BlockStorePostgres, ExecutionNode, ExecutionNodeBlock,
        LONDON_HARD_FORK_BLOCK_NUMBER,
    },
    gauges,
    performance::TimedExt,
    units::EthNewtype,
    usd_price::{self, EthPriceStore, EthPriceStorePostgres},
};

use anyhow::{Context, Result};

use super::{BlockNumber, BlockStore};

pub const LONDON_HARD_FORK_BLOCK_PARENT_HASH: &str =
    "0x3de6bb3849a138e6ab0b83a3a00dc7433f1e83f7fd488e4bba78f2fe2631a633";

pub const EXECUTION_BLOCK_NUMBER_AUG_1ST: BlockNumber = 15253306;

async fn rollback_to_common_ancestor(
    db_pool: &PgPool,
    execution_node: &ExecutionNode,
    block_store: &impl BlockStore,
) -> Result<BlockNumber> {
    let mut candidate_number = match block_store.last().await? {
        Some(block) => block.number,
        None => {
            info!("no blocks in db, starting from london hard fork block");
            return Ok(LONDON_HARD_FORK_BLOCK_NUMBER);
        }
    };

    info!(
        "checking for common ancestor with node, starting from block {}",
        candidate_number
    );

    loop {
        let on_chain_block = execution_node.get_block_by_number(&candidate_number).await;
        let stored_hash = block_store.hash_from_number(&candidate_number).await;

        let on_chain_hash = on_chain_block.as_ref().map(|b| &b.hash);

        if on_chain_hash == stored_hash.as_ref() && stored_hash.is_some() {
            info!("found common ancestor at {}", candidate_number);
            let next_block_to_sync = candidate_number + 1;
            rollback_numbers(db_pool, &next_block_to_sync).await;
            return Ok(next_block_to_sync);
        }

        if stored_hash.is_some() {
            warn!(
                "mismatch or missing on-chain block for block {}, rolling back",
                candidate_number
            );
            rollback_numbers(db_pool, &candidate_number).await;
        }

        candidate_number -= 1;
    }
}

async fn rollback_numbers(db_pool: &PgPool, greater_than_or_equal: &BlockNumber) {
    debug!("rolling back data based on numbers gte {greater_than_or_equal}");

    let mut transaction = db_pool.begin().await.unwrap();

    burn_sums::on_rollback(&mut transaction, greater_than_or_equal).await;
    execution_chain::delete_blocks(&mut *transaction, greater_than_or_equal).await;

    transaction.commit().await.unwrap();
}

async fn sync_block(
    db_pool: &PgPool,
    eth_price_store: &impl EthPriceStore,
    block: &ExecutionNodeBlock,
) -> Result<()> {
    debug!(block_hash = %block.hash, "syncing block");

    let eth_price = eth_price_store
        .get_eth_price_by_block(block)
        .timed("get_eth_price_by_block")
        .await
        .context("eth price close to block to be available")?;

    execution_chain::store_block(db_pool, block, eth_price)
        .timed("store_block")
        .await;

    Ok(())
}

async fn run_skippable_calculations(
    db_pool: &PgPool,
    issuance_store: &impl IssuanceStore,
    eth_price_store: &impl EthPriceStore,
    block: &ExecutionNodeBlock,
) -> Result<()> {
    base_fees::on_new_block(db_pool, issuance_store, block).await;
    let burn_sums_envelope = burn_sums::on_new_block(db_pool, block).await;
    let eth_supply: EthNewtype = eth_supply::last_eth_supply(db_pool).await.into();
    burn_rates::on_new_block(db_pool, &burn_sums_envelope).await;
    gauges::on_new_block(
        db_pool,
        eth_price_store,
        issuance_store,
        block,
        &burn_sums_envelope,
        &eth_supply,
    )
    .await
    .unwrap_or_else(|err| warn!("gauges::on_new_block failed: {}", err));
    usd_price::on_new_block(db_pool, eth_price_store, block).await;
    Ok(())
}

async fn get_highest_stored_beacon_block_hash(
    executor: impl PgExecutor<'_>,
) -> Result<Option<String>> {
    let row = sqlx::query!(
        r#"
        SELECT
            bb.block_hash
        FROM beacon_blocks bb
        ORDER BY bb.slot DESC
        LIMIT 1
        "#
    )
    .fetch_optional(executor)
    .await?;

    Ok(row.and_then(|r| r.block_hash))
}

pub async fn sync_blocks(
    db_pool: &PgPool,
    execution_node: &ExecutionNode,
    last_synced: &Arc<Mutex<SystemTime>>,
) -> Result<()> {
    info!("syncing execution blocks");

    let issuance_store = IssuanceStorePostgres::new(db_pool.clone());
    let eth_price_store = EthPriceStorePostgres::new(db_pool.clone());
    let block_store = BlockStorePostgres::new(db_pool.clone());

    let mut next_block_to_sync =
        rollback_to_common_ancestor(db_pool, execution_node, &block_store).await?;

    info!(%next_block_to_sync, "starting execution block sync");

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let highest_beacon_block_opt = get_highest_stored_beacon_block_hash(db_pool).await?;
        if highest_beacon_block_opt.is_none() {
            warn!("no beacon blocks in db, skipping sync");
            continue;
        }
        let block_hash = highest_beacon_block_opt.unwrap();

        debug!(%block_hash, "got highest beacon block hash, using as sync target");

        let target_block = execution_node
            .get_block_by_hash(&block_hash)
            .await
            .with_context(|| "highest beacon block hash not found on execution node")?;
        let target_block_number = target_block.number;

        if next_block_to_sync > target_block_number {
            debug!("already synced to target block, waiting");
            continue;
        }

        debug!(%next_block_to_sync, %target_block_number, "syncing blocks");

        while next_block_to_sync <= target_block_number {
            let next_on_chain_block = execution_node
                .get_block_by_number(&next_block_to_sync)
                .await
                .with_context(|| {
                    format!("block {} not found on execution node", next_block_to_sync)
                })?;

            let parent_matches = match block_store.last().await? {
                Some(our_last_block) => {
                    let on_chain_parent_hash = &next_on_chain_block.parent_hash;
                    let our_last_hash = &our_last_block.hash;
                    if on_chain_parent_hash != our_last_hash {
                        warn!(
                            block_number = next_block_to_sync,
                            on_chain_parent_hash, our_last_hash, "parent hash mismatch"
                        );
                    }
                    on_chain_parent_hash == our_last_hash
                }
                None => next_on_chain_block.parent_hash == LONDON_HARD_FORK_BLOCK_PARENT_HASH,
            };

            if parent_matches {
                debug!(
                    block_number = next_block_to_sync,
                    "parent hash matches, syncing block"
                );
                sync_block(db_pool, &eth_price_store, &next_on_chain_block).await?;

                let is_at_chain_head = next_block_to_sync == target_block_number;
                let is_hourly_tick = next_block_to_sync % 300 == 0;

                if is_at_chain_head || is_hourly_tick {
                    if is_at_chain_head {
                        debug!("at chain head, running skippable calculations");
                    } else {
                        info!(
                            block_number = next_block_to_sync,
                            "at hourly tick, running skippable calculations"
                        );
                    }
                    run_skippable_calculations(
                        db_pool,
                        &issuance_store,
                        &eth_price_store,
                        &next_on_chain_block,
                    )
                    .await?;
                }

                *last_synced.lock().unwrap() = SystemTime::now();
                next_block_to_sync += 1;
            } else {
                warn!(
                    "reorg detected at block {}, rolling back",
                    next_block_to_sync
                );
                next_block_to_sync =
                    rollback_to_common_ancestor(db_pool, execution_node, &block_store).await?;
                info!(%next_block_to_sync, "rolled back to block");
                // Break inner while to re-evaluate target block.
                break;
            }
        }
    }
}
