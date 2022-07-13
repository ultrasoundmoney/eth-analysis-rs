use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::beacon_chain::{self, BeaconBalancesSum, BeaconDepositsSum};
use crate::caching;
use crate::execution_chain;
use crate::execution_chain::ExecutionBalancesSum;
use crate::key_value_store::{self, KeyValueStr};
use crate::performance::LifetimeMeasure;

const TOTAL_SUPPLY_CACHE_KEY: &str = "total-supply";

#[derive(Deserialize, Serialize)]
struct TotalSupply {
    execution_balances_sum: ExecutionBalancesSum,
    beacon_balances_sum: BeaconBalancesSum,
    beacon_deposits_sum: BeaconDepositsSum,
}

async fn get_total_supply<'a>(executor: &PgPool) -> TotalSupply {
    let _ = LifetimeMeasure::log_lifetime("get total supply");
    let execution_balances = execution_chain::get_balances_sum(executor).await;
    let beacon_balances = beacon_chain::get_balances_sum(executor).await;
    let beacon_deposits = beacon_chain::get_deposits_sum(executor).await;

    TotalSupply {
        execution_balances_sum: execution_balances,
        beacon_balances_sum: beacon_balances,
        beacon_deposits_sum: beacon_deposits,
    }
}

pub async fn update(executor: &PgPool) {
    let total_supply = get_total_supply(executor).await;

    // sqlx wants a Value, but serde_json does not support i128 in Value, it's happy to serialize
    // as string however.
    key_value_store::set_value_str(
        executor,
        KeyValueStr {
            key: TOTAL_SUPPLY_CACHE_KEY,
            value_str: &serde_json::to_string(&total_supply).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(executor, TOTAL_SUPPLY_CACHE_KEY).await;
}
