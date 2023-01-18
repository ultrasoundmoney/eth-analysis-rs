use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, Decode};
use tracing::{debug, info};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    db,
    glassnode::{self, GlassnodeDataPoint},
    log,
    units::GWEI_PER_ETH_F64,
};

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

impl From<(DateTime<Utc>, i64)> for GweiInTime {
    fn from((dt, gwei): (DateTime<Utc>, i64)) -> Self {
        GweiInTime {
            t: dt.timestamp().try_into().unwrap(),
            v: gwei,
        }
    }
}

impl From<&GweiInTimeRow> for GweiInTime {
    fn from(row: &GweiInTimeRow) -> Self {
        Self {
            t: row.timestamp.timestamp() as u64,
            v: row.gwei,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SupplyProjectionInputs {
    supply_by_day: Vec<GlassnodeDataPoint>,
    in_contracts_by_day: Vec<GlassnodeDataPoint>,
    in_beacon_validators_by_day: Vec<GlassnodeDataPoint>,
}

pub async fn update_supply_projection_inputs() -> Result<()> {
    log::init_with_env();

    info!("updating supply projection inputs");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db::get_db_url_with_name("supply_projection_inputs"))
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let in_contracts_by_day = glassnode::get_locked_eth_data().await;

    debug!(
        "got gwei in contracts by day, {} data points",
        in_contracts_by_day.len()
    );

    let in_beacon_validators_by_day = beacon_chain::get_validator_balances_by_start_of_day(&pool)
        .await
        .iter()
        .map(|point| GlassnodeDataPoint {
            t: point.t,
            v: point.v as f64 / GWEI_PER_ETH_F64,
        })
        .collect::<Vec<_>>();

    debug!(
        "got balances in beacon validators by day, {} data points",
        in_beacon_validators_by_day.len()
    );

    let supply_data = glassnode::get_circulating_supply_data().await;

    debug!("got supply data by day, {} data points", supply_data.len());

    let supply_by_day = supply_data;

    debug!("got supply by day, {} data points", supply_by_day.len());

    // Deprecate supplyData, lockedData, stakedData after prod frontend has switched to new supply projection inputs.
    let supply_projetion_inputs = SupplyProjectionInputs {
        supply_by_day,
        in_contracts_by_day,
        in_beacon_validators_by_day,
    };

    caching::update_and_publish(
        &pool,
        &CacheKey::SupplyProjectionInputs,
        &supply_projetion_inputs,
    )
    .await
    .unwrap();

    info!("done updating supply projection inputs");

    Ok(())
}
