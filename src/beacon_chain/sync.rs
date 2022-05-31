use reqwest::Client;
use sqlx::PgPool;
use thiserror::Error;
use tokio::join;

use crate::eth_units::GweiAmount;

use super::{
    balances,
    beacon_time::FirstOfDaySlot,
    blocks, deposits, issuance,
    node::{self, BeaconHeaderSignedEnvelope},
    states,
};

pub struct SlotRange {
    pub from: u32,
    pub to: u32,
}

async fn store_state_with_block(
    pool: &PgPool,
    state_root: &str,
    slot: &u32,
    header: &BeaconHeaderSignedEnvelope,
    deposit_sum: &GweiAmount,
    deposit_sum_aggregated: &GweiAmount,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    states::store_state(&mut tx, state_root, slot).await?;

    blocks::store_block(
        &mut tx,
        state_root,
        header,
        deposit_sum,
        deposit_sum_aggregated,
    )
    .await?;

    tx.commit().await
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
            .await?;
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
                start_of_day_date_time,
                issuance::calc_issuance(&validator_balances_sum_gwei, &deposit_sum_aggregated),
            )
            .await;
        }
    }

    Ok(())
}

async fn sync_slots(
    pool: &PgPool,
    node_client: &Client,
    SlotRange { from, to }: SlotRange,
) -> Result<(), SyncError> {
    tracing::info!("syncing slots from {:?}, to {:?}", from, to);

    let mut progress = pit_wall::Progress::new("sync slots", (to - from).into());

    for slot in from..=to {
        sync_slot(pool, node_client, &slot).await?;

        progress.inc_work_done();
        if progress.work_done != 0 && progress.work_done % 1000 == 0 {
            tracing::info!("{}", progress.get_progress_string());
        }
    }

    Ok(())
}

pub async fn sync_beacon_states(pool: &PgPool, node_client: &Client) -> Result<(), SyncError> {
    let (last_finalized_block, last_state) = join!(
        node::get_last_finalized_block(&node_client),
        states::get_last_state(&pool)
    );

    let last_finalized_block =
        last_finalized_block.expect("a last finalized block to always be available");

    match last_state? {
        Some(last_state) => {
            sync_slots(
                &pool,
                &node_client,
                SlotRange {
                    from: last_state.slot as u32 + 1,
                    to: last_finalized_block.slot,
                },
            )
            .await
        }
        None => {
            sync_slots(
                &pool,
                &node_client,
                SlotRange {
                    from: 0,
                    to: last_finalized_block.slot,
                },
            )
            .await
        }
    }
}
