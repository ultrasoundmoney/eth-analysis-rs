use serde::{Deserialize, Serialize};
use sqlx::PgConnection;

use crate::config;
use crate::key_value_store::{self, KeyValue};

const TOTAL_SUPPLY_CACHE_KEY: &str = "total-supply";

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct SupplyAtBlock {
    pub block_number: u32,
    pub total_supply: u64,
}

#[derive(Serialize)]
struct TotalSupply {
    execution_chain: SupplyAtBlock,
    beacon_chain: SupplyAtBlock,
}

pub async fn update() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating total supply");

    let mut connection: PgConnection = sqlx::Connection::connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    let execution_total_supply =
        crate::execution_supply_deltas::get_latest_total_supply(&mut connection).await;
    let beacon_total_supply = crate::beacon_chain::get_latest_total_supply(&mut connection).await;

    let total_supply = TotalSupply {
        execution_chain: execution_total_supply,
        beacon_chain: beacon_total_supply,
    };

    key_value_store::set_value(
        &mut connection,
        KeyValue {
            key: TOTAL_SUPPLY_CACHE_KEY,
            value: serde_json::to_value(&total_supply).unwrap(),
        },
    )
    .await;
}
