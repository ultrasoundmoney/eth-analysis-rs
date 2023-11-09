use std::fs::File;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use eth_analysis::{
    beacon_chain::{self, GweiInTime},
    caching::{self, CacheKey},
    db, eth_supply, log,
    units::GWEI_PER_ETH_F64,
    SupplyAtTime,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Decode;
use tracing::{debug, info};

lazy_static! {
    static ref SUPPLY_LOWER_LIMIT_DATE_TIME: DateTime<Utc> =
        ("2015-07-30T00:00:00Z").parse::<DateTime<Utc>>().unwrap();
}

#[derive(Decode)]
pub struct GweiInTimeRow {
    pub timestamp: DateTime<Utc>,
    pub gwei: i64,
}

impl From<&GweiInTimeRow> for GweiInTime {
    fn from(row: &GweiInTimeRow) -> Self {
        Self {
            t: row.timestamp.timestamp() as u64,
            v: row.gwei,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimestampValuePoint {
    pub t: u64,
    // fraction
    pub v: f64,
}

impl From<SupplyAtTime> for TimestampValuePoint {
    fn from(supply_at_time: SupplyAtTime) -> Self {
        TimestampValuePoint {
            t: supply_at_time.timestamp.timestamp() as u64,
            v: supply_at_time.supply.0,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SupplyProjectionInputs {
    supply_by_day: Vec<TimestampValuePoint>,
    in_contracts_by_day: Vec<TimestampValuePoint>,
    in_beacon_validators_by_day: Vec<TimestampValuePoint>,
}

#[tokio::main]
pub async fn main() {
    log::init_with_env();

    info!("updating supply projection inputs");

    let db_pool = db::get_db_pool("supply-projection-inputs", 3).await;

    sqlx::migrate!().run(&db_pool).await.unwrap();

    // We originally got this data from the Glassnode API when we paid for it. We don't pay
    // anymore, so no more new data.
    let in_contracts_by_day_file =
        File::open("src/bin/update-supply-projection-inputs/in_contracts_by_day.json")
            .map_err(|e| anyhow!("failed to open in_contracts_by_day.json: {}", e))
            .unwrap();
    let json_value: Value = serde_json::from_reader(in_contracts_by_day_file).unwrap();
    let in_contracts_by_day: Vec<TimestampValuePoint> = serde_json::from_value(json_value).unwrap();

    debug!(
        "got gwei in contracts by day, {} data points",
        in_contracts_by_day.len()
    );

    let in_beacon_validators_by_day =
        beacon_chain::get_validator_balances_by_start_of_day(&db_pool)
            .await
            .iter()
            .map(|point| TimestampValuePoint {
                t: point.t,
                v: point.v as f64 / GWEI_PER_ETH_F64,
            })
            .collect::<Vec<_>>();

    debug!(
        "got balances in beacon validators by day, {} data points",
        in_beacon_validators_by_day.len()
    );

    let supply_by_day: Vec<TimestampValuePoint> = eth_supply::get_daily_supply(&db_pool)
        .await
        .into_iter()
        .filter(|point| point.timestamp >= *SUPPLY_LOWER_LIMIT_DATE_TIME)
        .map(Into::into)
        .collect();

    debug!("got supply by day, {} data points", supply_by_day.len());

    // Deprecate supplyData, lockedData, stakedData after prod frontend has switched to new supply projection inputs.
    let supply_projetion_inputs = SupplyProjectionInputs {
        supply_by_day,
        in_contracts_by_day,
        in_beacon_validators_by_day,
    };

    caching::update_and_publish(
        &db_pool,
        &CacheKey::SupplyProjectionInputs,
        &supply_projetion_inputs,
    )
    .await;

    info!("done updating supply projection inputs");
}
