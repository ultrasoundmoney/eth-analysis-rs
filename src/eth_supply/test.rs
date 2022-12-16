use anyhow::Result;
use chrono::{SubsecRound, Utc};
use sqlx::{Acquire, PgConnection};

use crate::beacon_chain::{self, BeaconBalancesSum, BeaconDepositsSum};
use crate::execution_chain::{BlockStore, ExecutionBalancesSum, ExecutionNodeBlock};
use crate::units::GweiNewtype;
use crate::{beacon_chain::Slot, units::EthF64};

// Replace with shared testing helper that helps easily build the right mock block.
pub fn make_test_block() -> ExecutionNodeBlock {
    ExecutionNodeBlock {
        base_fee_per_gas: 0,
        difficulty: 0,
        gas_used: 0,
        hash: "0xtest".to_string(),
        number: 0,
        parent_hash: "0xparent".to_string(),
        timestamp: Utc::now().trunc_subsecs(0),
        total_difficulty: 10,
    }
}

pub async fn store_test_eth_supply(
    executor: &mut PgConnection,
    slot: &Slot,
    eth_supply: EthF64,
) -> Result<()> {
    let mut block_store = BlockStore::new(executor);

    let test_block = make_test_block();
    let state_root = "0xstate_root";

    block_store.store_block(&test_block, 0.0).await;

    beacon_chain::store_state(executor.acquire().await.unwrap(), state_root, slot).await?;

    let execution_balances_sum = ExecutionBalancesSum {
        block_number: 0,
        balances_sum: GweiNewtype::from_eth_f64(eth_supply).wei().0,
    };
    let beacon_balances_sum = BeaconBalancesSum {
        balances_sum: GweiNewtype(0),
        slot: *slot,
    };
    let beacon_deposits_sum = BeaconDepositsSum {
        slot: *slot,
        deposits_sum: GweiNewtype(0),
    };

    super::store(
        executor,
        slot,
        &execution_balances_sum.block_number,
        &execution_balances_sum.balances_sum,
        &beacon_deposits_sum.deposits_sum,
        &beacon_balances_sum.balances_sum,
    )
    .await?;

    Ok(())
}
