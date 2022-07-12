use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgConnection};

use crate::beacon_chain::{BeaconBalancesSum, BeaconDepositsSum};
use crate::config;
use crate::execution_chain::ExecutionBalancesSum;
use crate::key_value_store::{self, KeyValue};

const TOTAL_SUPPLY_CACHE_KEY: &str = "total-supply";

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct SupplyAtBlock {
    pub block_number: u32,
    pub total_supply: u64,
}

#[derive(Deserialize, Serialize)]
struct TotalSupply {
    execution_balances: ExecutionBalancesSum,
    beacon_balances: BeaconBalancesSum,
    beacon_deposits: BeaconDepositsSum,
}

async fn get_total_supply(executor: &mut PgConnection) -> TotalSupply {
    let mut tx = executor.begin().await.unwrap();

    let execution_balances = crate::execution_chain::get_balances_sum(&mut *tx).await;
    let beacon_balances = crate::beacon_chain::get_balances_sum(&mut *tx).await;
    let beacon_deposits = crate::beacon_chain::get_deposits_sum(&mut *tx).await;

    tx.commit().await.unwrap();

    TotalSupply {
        execution_balances,
        beacon_balances,
        beacon_deposits,
    }
}

pub async fn update() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating total supply");

    let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let total_supply = get_total_supply(&mut connection).await;

    key_value_store::set_value(
        &mut connection,
        KeyValue {
            key: TOTAL_SUPPLY_CACHE_KEY,
            value: serde_json::to_value(&total_supply).unwrap(),
        },
    )
    .await;
}
