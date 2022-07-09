use futures::SinkExt;
use futures::{channel::mpsc, StreamExt};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use thiserror::Error;

use crate::{config, decoders::from_u32_string, eth_units::GweiAmount};

use super::{
    balances,
    beacon_time::FirstOfDaySlot,
    blocks, deposits, issuance,
    node::{self, BeaconHeaderSignedEnvelope},
    states,
};

pub struct SlotRange {
    pub greater_than_or_equal_to: u32,
    pub less_than: u32,
}

async fn store_state_with_block(
    pool: &PgPool,
    state_root: &str,
    slot: &u32,
    header: &BeaconHeaderSignedEnvelope,
    deposit_sum: &GweiAmount,
    deposit_sum_aggregated: &GweiAmount,
) {
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

async fn sync_slot(
    pool: &PgPool,
    node_client: &reqwest::Client,
    slot: &u32,
) -> Result<(), SyncError> {
    let state_root = node::get_state_root_by_slot(node_client, slot).await?;

    let header = node::get_header_by_slot(node_client, slot).await?;
    let block = match header {
        None => None,
        Some(ref header) => Some(node::get_block_by_root(node_client, &header.root).await?),
    };
    let deposit_sum_aggregated = match block {
        None => None,
        Some(ref block) => Some(deposits::get_deposit_sum_aggregated(pool, block).await?),
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
                "storing slot without header, slot: {:?}, state_root: {}",
                slot,
                state_root
            );
            states::store_state(pool, &state_root, slot).await?;
        }
    };

    if let Some(start_of_day_date_time) = FirstOfDaySlot::new(slot) {
        let validator_balances = node::get_validator_balances(node_client, &state_root).await?;
        let validator_balances_sum_gwei = balances::sum_validator_balances(validator_balances);
        balances::store_validator_sum_for_day(
            pool,
            &state_root,
            &start_of_day_date_time,
            &validator_balances_sum_gwei,
        )
        .await;

        if let Some(deposit_sum_aggregated) = deposit_sum_aggregated {
            issuance::store_issuance_for_day(
                pool,
                &state_root,
                &start_of_day_date_time,
                &issuance::calc_issuance(&validator_balances_sum_gwei, &deposit_sum_aggregated),
            )
            .await;
        }
    }

    Ok(())
}

async fn sync_slots(
    pool: &PgPool,
    node_client: &reqwest::Client,
    SlotRange {
        greater_than_or_equal_to: from,
        less_than: to,
    }: SlotRange,
) {
    tracing::info!(
        "syncing slots from {}, to {}, {} slots total",
        from,
        to,
        to - from
    );

    let mut progress = pit_wall::Progress::new("sync slots", (to - from).into());

    for slot in from..=to {
        sync_slot(pool, node_client, &slot).await.unwrap();

        progress.inc_work_done();
        if progress.work_done != 0 && progress.work_done % 1000 == 0 {
            tracing::info!("{}", progress.get_progress_string());
        }
    }
}

#[derive(Debug, Deserialize)]
struct FinalizedCheckpoint {
    block: String,
    state: String,
    #[serde(deserialize_with = "from_u32_string")]
    epoch: u32,
}

fn stream_finalized_checkpoints() -> mpsc::UnboundedReceiver<FinalizedCheckpoint> {
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
                        serde_json::from_str::<FinalizedCheckpoint>(&event.data).unwrap();

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

pub async fn sync_beacon_states() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing beacon states");

    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let node_client = reqwest::Client::new();

    let latest_finalized_block_on_start = super::get_last_finalized_block(&node_client)
        .await
        .expect("last finalized block to be available");

    tracing::debug!(
        "last finalized block on start: {}",
        latest_finalized_block_on_start.slot
    );

    let last_synced_state = states::get_last_state(&pool).await;

    tracing::debug!(
        "last synced state: {}",
        last_synced_state
            .as_ref()
            .map_or("?".to_string(), |state| state.slot.to_string())
    );

    let mut finalized_checkpoint_stream = stream_finalized_checkpoints();

    match last_synced_state {
        Some(last_synced_state) => {
            sync_slots(
                &pool,
                &node_client,
                SlotRange {
                    greater_than_or_equal_to: last_synced_state.slot as u32 + 1,
                    less_than: latest_finalized_block_on_start.slot,
                },
            )
            .await;
        }
        None => {
            sync_slots(
                &pool,
                &node_client,
                SlotRange {
                    greater_than_or_equal_to: 0,
                    less_than: latest_finalized_block_on_start.slot,
                },
            )
            .await;
        }
    }

    while let Some(checkpoint) = finalized_checkpoint_stream.next().await {
        dbg!(checkpoint);
    }
}
