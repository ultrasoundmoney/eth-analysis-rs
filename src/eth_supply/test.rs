use anyhow::Result;
use chrono::{SubsecRound, Utc};
use sqlx::{Acquire, PgConnection};

use crate::beacon_chain::Slot;
use crate::beacon_chain::{self, BeaconBalancesSum, BeaconDepositsSum};
use crate::execution_chain::{self, ExecutionBalancesSum, ExecutionNodeBlock};
use crate::units::{EthNewtype, GweiNewtype};

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
    connection: &mut PgConnection,
    slot: &Slot,
    eth_supply: EthNewtype,
) -> Result<()> {
    let test_block = make_test_block();
    let state_root = "0xstate_root";

    execution_chain::store_block(connection.acquire().await.unwrap(), &test_block, 0.0).await;

    beacon_chain::store_state(connection.acquire().await.unwrap(), state_root, slot).await;

    let execution_balances_sum = ExecutionBalancesSum {
        block_number: 0,
        balances_sum: eth_supply.into(),
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
        connection,
        slot,
        &execution_balances_sum.block_number,
        &execution_balances_sum.balances_sum,
        &beacon_deposits_sum.deposits_sum,
        &beacon_balances_sum.balances_sum,
    )
    .await?;

    Ok(())
}
