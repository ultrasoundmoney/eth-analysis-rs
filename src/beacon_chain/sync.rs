use std::cmp::Ordering;

use futures::{channel::mpsc, SinkExt, Stream, StreamExt};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgExecutor, PgPool};
use thiserror::Error;

use crate::config;
use crate::decoders::from_u32_string;
use crate::eth_units::GweiAmount;
use crate::performance::LifetimeMeasure;
use crate::total_supply;

use super::{
    balances, beacon_time::FirstOfDaySlot, blocks, deposits, issuance, node::BeaconBlock,
    node::BeaconHeaderSignedEnvelope, states, BeaconNode,
};

pub struct SlotRange {
    pub greater_than_or_equal: u32,
    pub less_than_or_equal: u32,
}

async fn store_state_with_block(
    pool: &PgPool,
    state_root: &str,
    slot: &u32,
    header: &BeaconHeaderSignedEnvelope,
    deposit_sum: &GweiAmount,
    deposit_sum_aggregated: &GweiAmount,
) {
    let _m1 = LifetimeMeasure::log_lifetime("store state with block");
    let mut transaction = pool.begin().await.unwrap();

    states::store_state(&mut *transaction, state_root, slot)
        .await
        .unwrap();

    let is_parent_known =
        blocks::get_is_hash_known(&mut *transaction, &header.header.message.parent_root).await;
    if !is_parent_known {
        panic!(
            "trying to insert beacon block with missing parent, block_root: {}, parent_root: {:?}",
            header.root, header.header.message.parent_root
        )
    }

    blocks::store_block(
        &mut *transaction,
        state_root,
        header,
        deposit_sum,
        deposit_sum_aggregated,
    )
    .await;

    transaction.commit().await.unwrap();
}

#[derive(Error, Debug)]
pub enum SyncError {
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
}

async fn sync_slot(pool: &PgPool, beacon_node: &BeaconNode, slot: &u32) {
    let _m1 = LifetimeMeasure::log_lifetime(&format!("sync slot {}", slot));
    let state_root = beacon_node.get_state_root_by_slot(slot).await.unwrap();

    let header = beacon_node.get_header_by_slot(slot).await.unwrap();
    let block = match header {
        None => None,
        Some(ref header) => Some(beacon_node.get_block_by_root(&header.root).await.unwrap()),
    };
    let deposit_sum_aggregated = match block {
        None => None,
        Some(ref block) => Some(
            deposits::get_deposit_sum_aggregated(pool, block)
                .await
                .unwrap(),
        ),
    };

    match (header, block, deposit_sum_aggregated) {
        (Some(header), Some(block), Some(deposit_sum_aggregated)) => {
            tracing::debug!(
                "storing slot with block, slot: {:?}, state_root: {}",
                slot,
                state_root
            );
            store_state_with_block(
                pool,
                &state_root,
                slot,
                &header,
                &deposits::get_deposit_sum_from_block(&block),
                &deposit_sum_aggregated,
            )
            .await;
        }
        _ => {
            tracing::debug!(
                "storing slot without block, slot: {:?}, state_root: {}",
                slot,
                state_root
            );
            states::store_state(pool, &state_root, slot).await.unwrap();
        }
    };

    let _m2 = LifetimeMeasure::log_lifetime("store validator balances");
    let validator_balances = beacon_node
        .get_validator_balances(&state_root)
        .await
        .unwrap();
    let validator_balances_sum = balances::sum_validator_balances(validator_balances);
    super::set_balances_sum(pool, slot, validator_balances_sum).await;
    drop(_m2);

    if let Some(start_of_day_date_time) = FirstOfDaySlot::new(slot) {
        balances::store_validator_sum_for_day(
            pool,
            &state_root,
            &start_of_day_date_time,
            &validator_balances_sum,
        )
        .await;

        if let Some(deposit_sum_aggregated) = deposit_sum_aggregated {
            issuance::store_issuance_for_day(
                pool,
                &state_root,
                &start_of_day_date_time,
                &issuance::calc_issuance(&validator_balances_sum, &deposit_sum_aggregated),
            )
            .await;
        }
    }

    total_supply::update(pool).await;
}

async fn sync_slots(pool: &PgPool, beacon_node: &BeaconNode, slot_range: &SlotRange) {
    tracing::info!(
        "syncing slots gte {}, and lte {}, {} slots total",
        slot_range.greater_than_or_equal,
        slot_range.less_than_or_equal,
        slot_range.less_than_or_equal - slot_range.greater_than_or_equal + 1
    );

    let mut progress = pit_wall::Progress::new(
        "sync slots",
        (slot_range.less_than_or_equal - slot_range.greater_than_or_equal + 1).into(),
    );

    for slot in slot_range.greater_than_or_equal..=slot_range.less_than_or_equal {
        sync_slot(pool, beacon_node, &slot).await;

        progress.inc_work_done();
        if progress.work_done != 0 && progress.work_done % 30 == 0 {
            tracing::info!("{}", progress.get_progress_string());
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FinalizedCheckpointEvent {
    block: String,
    state: String,
    #[serde(deserialize_with = "from_u32_string")]
    epoch: u32,
}

fn stream_finalized_checkpoints() -> mpsc::UnboundedReceiver<FinalizedCheckpointEvent> {
    let (mut finalized_checkpoint_tx, finalized_checkpoint_rx) = mpsc::unbounded();

    let url_string = format!(
        "{}/eth/v1/events/?topics=finalized_checkpoint",
        config::get_beacon_url()
    );
    let url = reqwest::Url::parse(&url_string).unwrap();

    let client = eventsource::reqwest::Client::new(url);

    tokio::spawn(async move {
        for event in client {
            let event = event.unwrap();
            match event.event_type {
                Some(ref event_type) if event_type == "finalized_checkpoint" => {
                    let finalized_checkpoint =
                        serde_json::from_str::<FinalizedCheckpointEvent>(&event.data).unwrap();

                    tracing::debug!(
                        "received new finalized checkpoint {:#?}",
                        finalized_checkpoint
                    );

                    finalized_checkpoint_tx
                        .send(finalized_checkpoint)
                        .await
                        .unwrap();
                }
                Some(event) => {
                    tracing::warn!(
                        "received a server event that was not a finalized_checkpoint: {}",
                        event
                    );
                }
                None => {
                    tracing::debug!("received an empty server event");
                }
            }
        }
    });

    finalized_checkpoint_rx
}

async fn get_range_to_sync<'a>(
    executor: impl PgExecutor<'a>,
    last_finalized_block_on_start: &BeaconBlock,
) -> Option<SlotRange> {
    let last_synced_state = states::get_last_state(executor).await;

    tracing::debug!(
        "last synced state: {}",
        last_synced_state
            .as_ref()
            .map_or("?".to_string(), |state| state.slot.to_string())
    );

    match last_synced_state {
        Some(last_synced_state) => {
            let last_synced_cmp_last_finalized = last_synced_state
                .slot
                .cmp(&last_finalized_block_on_start.slot);

            match last_synced_cmp_last_finalized {
                Ordering::Less => Some(SlotRange {
                    greater_than_or_equal: last_synced_state.slot + 1,
                    less_than_or_equal: last_finalized_block_on_start.slot + 1,
                }),
                Ordering::Equal => None,
                Ordering::Greater => {
                    panic!(
                        "last synced is ahead of on-chain last finalized, rollback not implemented"
                    );
                }
            }
        }
        None => Some(SlotRange {
            greater_than_or_equal: 0,
            less_than_or_equal: last_finalized_block_on_start.slot,
        }),
    }
}

const SLOTS_PER_EPOCH: u32 = 32;

pub async fn sync_beacon_states() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing beacon states");

    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let beacon_node = BeaconNode::new();

    // If we're synced with the latest on-chain checkpoint, we can start listening for and syncing
    // to new checkpoints as they come in. If we're not synced, we'd like to start syncing right
    // away, to whatever the current last finalized checkpoint is, whilst concurrently listening
    // for and queueing new finalized checkpoints.

    // Get the block we need to be synced to before we can listen and process new finalized
    // checkpoints.
    let last_finalized_block_on_start = beacon_node
        .get_last_finalized_block()
        .await
        .expect("last finalized block to be available");

    tracing::debug!(
        "last finalized block on start: {}",
        last_finalized_block_on_start.slot
    );

    // Start listening for new finalized checkpoints.
    let mut finalized_checkpoint_stream = stream_finalized_checkpoints();

    // Sync to last finalized block on start.
    let range_to_sync_on_start = get_range_to_sync(&pool, &last_finalized_block_on_start).await;
    match range_to_sync_on_start {
        Some(slot_range) => {
            tracing::debug!("slots to sync on-start, syncing");
            sync_slots(&pool, &beacon_node, &slot_range).await;
        }
        None => {
            tracing::debug!("no slots to sync on-start, skipping sync");
        }
    }

    // Process queue of new finalized checkpoints.
    while let Some(checkpoint) = finalized_checkpoint_stream.next().await {
        tracing::debug!(
            "processing next finalized checkpoint, stream lower bound: {}",
            &finalized_checkpoint_stream.size_hint().0
        );

        let last_synced_state = states::get_last_state(&pool)
            .await
            .expect("synced slots after sync on-start");

        sync_slots(
            &pool,
            &beacon_node,
            &SlotRange {
                greater_than_or_equal: last_synced_state.slot + 1,
                less_than_or_equal: (checkpoint.epoch * SLOTS_PER_EPOCH),
            },
        )
        .await;
    }
}
