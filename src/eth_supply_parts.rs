use serde::Serialize;
use sqlx::{Acquire, PgConnection};

use crate::beacon_chain::{self, BeaconBalancesSum, BeaconDepositsSum};
use crate::caching;
use crate::execution_chain;
use crate::execution_chain::ExecutionBalancesSum;
use crate::key_value_store::{self, KeyValueStr};
use crate::performance::TimedExt;

const ETH_SUPPLY_PARTS_CACHE_KEY: &str = "eth-supply-parts";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EthSupply {
    beacon_balances_sum: BeaconBalancesSum,
    beacon_deposits_sum: BeaconDepositsSum,
    execution_balances_sum: ExecutionBalancesSum,
}

async fn get_eth_supply_parts(
    executor: &mut PgConnection,
    beacon_balances_sum: BeaconBalancesSum,
) -> EthSupply {
    let execution_balances_sum =
        execution_chain::get_balances_sum(executor.acquire().await.unwrap()).await;
    let beacon_deposits_sum = beacon_chain::get_deposits_sum(executor).await;

    EthSupply {
        execution_balances_sum,
        beacon_balances_sum,
        beacon_deposits_sum,
    }
}

pub async fn update(executor: &mut PgConnection, beacon_balances_sum: BeaconBalancesSum) {
    let eth_supply_parts = get_eth_supply_parts(executor, beacon_balances_sum)
        .timed("get eth supply parts")
        .await;

    key_value_store::set_value_str(
        executor.acquire().await.unwrap(),
        KeyValueStr {
            key: ETH_SUPPLY_PARTS_CACHE_KEY,
            // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
            // as string however.
            value_str: &serde_json::to_string(&eth_supply_parts).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(executor, ETH_SUPPLY_PARTS_CACHE_KEY).await;
}
