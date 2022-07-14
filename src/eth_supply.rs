use serde::Serialize;
use sqlx::PgPool;

use crate::beacon_chain::{self, BeaconBalancesSum, BeaconDepositsSum};
use crate::caching;
use crate::execution_chain;
use crate::execution_chain::ExecutionBalancesSum;
use crate::key_value_store::{self, KeyValueStr};
use crate::performance::LifetimeMeasure;

const ETH_SUPPLY_CACHE_KEY: &str = "eth-supply";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EthSupply {
    beacon_balances_sum: BeaconBalancesSum,
    beacon_deposits_sum: BeaconDepositsSum,
    execution_balances_sum: ExecutionBalancesSum,
}

async fn get_eth_supply<'a>(
    executor: &PgPool,
    beacon_balances_sum: BeaconBalancesSum,
) -> EthSupply {
    let _ = LifetimeMeasure::log_lifetime("get eth supply");
    let execution_balances_sum = execution_chain::get_balances_sum(executor).await;
    let beacon_deposits_sum = beacon_chain::get_deposits_sum(executor).await;

    EthSupply {
        execution_balances_sum,
        beacon_balances_sum,
        beacon_deposits_sum,
    }
}

pub async fn update(executor: &PgPool, beacon_balances_sum: BeaconBalancesSum) {
    let eth_supply = get_eth_supply(executor, beacon_balances_sum).await;

    key_value_store::set_value_str(
        executor,
        KeyValueStr {
            key: ETH_SUPPLY_CACHE_KEY,
            // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
            // as string however.
            value_str: &serde_json::to_string(&eth_supply).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(executor, ETH_SUPPLY_CACHE_KEY).await;
}
