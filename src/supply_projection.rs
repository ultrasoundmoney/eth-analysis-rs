use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, Decode};

use crate::{
    beacon_chain, caching, config,
    eth_units::GWEI_PER_ETH_F64,
    glassnode::{self, GlassnodeDataPoint},
    key_value_store::{self, KeyValue},
};

const SUPPLY_PROJECTION_INPUTS_CACHE_KEY: &str = "supply-projection-inputs";

#[derive(Decode)]
pub struct GweiInTimeRow {
    pub timestamp: DateTime<Utc>,
    pub gwei: i64,
}

#[derive(Serialize)]
pub struct GweiInTime {
    pub t: u64,
    pub v: i64,
}

impl From<&GweiInTimeRow> for GweiInTime {
    fn from(row: &GweiInTimeRow) -> Self {
        Self {
            t: row.timestamp.timestamp() as u64,
            v: row.gwei,
        }
    }
}

fn add_beacon_issuance_to_supply(
    beacon_issuance_by_day: &[GlassnodeDataPoint],
    supply_data: &[GlassnodeDataPoint],
) -> Vec<GlassnodeDataPoint> {
    let issuance_by_day_map = beacon_issuance_by_day
        .iter()
        .map(|gwei_in_time| (gwei_in_time.t, gwei_in_time.v))
        .collect::<HashMap<u64, f64>>();

    supply_data
        .iter()
        .map(
            |supply_on_day| match issuance_by_day_map.get(&supply_on_day.t) {
                None => supply_on_day.clone(),
                Some(issuance_on_day) => GlassnodeDataPoint {
                    t: supply_on_day.t,
                    v: supply_on_day.v + (issuance_on_day),
                },
            },
        )
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SupplyProjectionInputs {
    supply_data: Vec<GlassnodeDataPoint>,
    supply_by_day: Vec<GlassnodeDataPoint>,
    locked_data: Vec<GlassnodeDataPoint>,
    in_contracts_by_day: Vec<GlassnodeDataPoint>,
    staked_data: Vec<GlassnodeDataPoint>,
    in_beacon_validators_by_day: Vec<GlassnodeDataPoint>,
}

pub async fn update_supply_projection_inputs() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating supply projection inputs");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let in_contracts_by_day = glassnode::get_locked_eth_data().await.unwrap();

    let staked_data = glassnode::get_staked_data().await.unwrap();

    let in_beacon_validators_by_day = beacon_chain::get_validator_balances_by_day(&pool)
        .await
        .unwrap()
        .iter()
        .map(|point| GlassnodeDataPoint {
            t: point.t,
            v: point.v as f64 / GWEI_PER_ETH_F64,
        })
        .collect();

    let beacon_issuance_by_day = beacon_chain::get_issuance_by_day(&pool)
        .await
        .unwrap()
        .iter()
        .map(|point| GlassnodeDataPoint {
            t: point.t,
            v: point.v as f64 / GWEI_PER_ETH_F64,
        })
        .collect::<Vec<GlassnodeDataPoint>>();

    let supply_data = glassnode::get_circulating_supply_data().await.unwrap();

    let supply_by_day = add_beacon_issuance_to_supply(&beacon_issuance_by_day, &supply_data);

    // Deprecate supplyData, lockedData, stakedData after prod frontend has switched to new supply projection inputs.
    let supply_projetion_inputs = SupplyProjectionInputs {
        supply_data: supply_by_day.clone(),
        supply_by_day,
        locked_data: in_contracts_by_day.clone(),
        in_contracts_by_day,
        staked_data,
        in_beacon_validators_by_day,
    };

    key_value_store::set_value(
        &pool,
        KeyValue {
            key: SUPPLY_PROJECTION_INPUTS_CACHE_KEY,
            value: serde_json::to_value(supply_projetion_inputs).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(&pool, SUPPLY_PROJECTION_INPUTS_CACHE_KEY).await;
}