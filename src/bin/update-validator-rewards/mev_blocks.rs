use anyhow::{Context, Result};
use eth_analysis::{
    beacon_chain::BeaconNode,
    mev_blocks::{MevBlocksStore, RelayApi, EARLIEST_AVAILABLE_SLOT},
    units::{EthNewtype, GweiImprecise, GweiNewtype, WeiNewtype},
};
use sqlx::PgExecutor;
use tracing::{debug, info};

use crate::{ValidatorReward, SLOTS_PER_YEAR};

pub async fn sync_mev_blocks(
    mev_blocks_store: &impl MevBlocksStore,
    beacon_node: &impl BeaconNode,
    relay_api: &impl RelayApi,
) {
    let last_header = beacon_node.get_last_header().await.unwrap();
    debug!(slot = last_header.slot().0, "last on-chain header");

    let last_synced_slot = mev_blocks_store.last_synced_slot().await;
    debug!("last synced slot: {:?}", last_synced_slot);

    let mut start_slot = last_synced_slot.map_or(EARLIEST_AVAILABLE_SLOT, |slot| slot.0 + 1);

    while start_slot < last_header.slot().0 {
        let end_slot = start_slot + 199;

        let blocks = relay_api.fetch_mev_blocks(start_slot, end_slot).await;

        debug!(start_slot, end_slot, "got {} blocks", blocks.len());

        mev_blocks_store.store_blocks(&blocks).await;

        start_slot = end_slot + 1;
    }

    info!(start_slot, "no more blocks to process");
}

pub async fn get_mev_reward(
    executor: impl PgExecutor<'_>,
    effective_balance_sum: GweiNewtype,
) -> Result<ValidatorReward> {
    let mev_per_slot: EthNewtype = sqlx::query!(
        "
        SELECT AVG(bid_wei)::TEXT
        FROM mev_blocks
        WHERE timestamp > NOW() - INTERVAL '6 months'
        "
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .avg
    .context("expect at least one block in mev_blocks table before computing MEV reward")?
    .parse::<WeiNewtype>()
    .context("failed to parse MEV per slot as Wei")?
    .into();

    let effective_balance_sum_eth: EthNewtype = effective_balance_sum.into();
    let active_validators: f64 = (effective_balance_sum_eth.0 / 32.0).floor();
    let annual_reward_eth = mev_per_slot.0 * SLOTS_PER_YEAR / active_validators;
    let annual_reward = EthNewtype(annual_reward_eth).into();
    let apr = annual_reward_eth / 32f64;

    debug!(
        "average MEV per slot in the last 6 months: {} ETH",
        mev_per_slot.0
    );
    debug!(
        "total effective balance: {} ETH",
        effective_balance_sum_eth.0
    );
    debug!("nr of active validators: {}", active_validators);
    debug!("MEV annual reward: {} ETH", annual_reward_eth);
    debug!("MEV APR: {:.2}%", apr * 100f64);

    Ok(ValidatorReward { annual_reward, apr })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eth_analysis::{
        beacon_chain::{BeaconHeaderSignedEnvelopeBuilder, MockBeaconNode, Slot},
        mev_blocks::{MevBlock, MockMevBlocksStore, MockRelayApi},
    };
    use mockall::predicate::*;

    #[tokio::test]
    async fn test_sync_mev_blocks() {
        let mut mock_store = MockMevBlocksStore::new();
        let mut mock_node = MockBeaconNode::new();
        let mut mock_relay = MockRelayApi::new();

        let header = BeaconHeaderSignedEnvelopeBuilder::new("test_sync_mev_blocks")
            .slot(&Slot(200))
            .build();
        mock_node
            .expect_get_last_header()
            .times(1)
            .return_once(|| Ok(header));

        mock_store
            .expect_last_synced_slot()
            .times(1)
            .returning(move || Some(Slot(0)));

        let mev_block = MevBlock {
            slot: 1,
            block_number: 1,
            block_hash: "0x000000".to_string(),
            bid: WeiNewtype::from(10),
        };

        let fetched_blocks = vec![mev_block.clone()];

        mock_relay
            .expect_fetch_mev_blocks()
            .with(eq(1), eq(1 + 199))
            .times(1)
            .return_once(move |_start, _end| fetched_blocks);

        mock_store
            .expect_store_blocks()
            .with(eq(vec![mev_block.clone()]))
            .times(1)
            .return_const(());

        sync_mev_blocks(&mock_store, &mock_node, &mock_relay).await;
    }
}
