use anyhow::Result;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, Decode};
use tracing::{debug, info};

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    db, eth_supply,
    glassnode::{self, GlassnodeDataPoint},
    log,
    units::GWEI_PER_ETH_F64,
};

lazy_static! {
    static ref SUPPLY_LOWER_LIMIT_DATE_TIME: DateTime<Utc> =
        ("2015-07-30T00:00:00Z").parse::<DateTime<Utc>>().unwrap();
}

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

    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db::get_db_url_with_name("supply_projection_inputs"))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let in_contracts_by_day = glassnode::get_locked_eth_data().await;

    debug!(
        "got gwei in contracts by day, {} data points",
        in_contracts_by_day.len()
    );

    let in_beacon_validators_by_day =
        beacon_chain::get_validator_balances_by_start_of_day(&db_pool)
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

    let supply_by_day: Vec<GlassnodeDataPoint> = eth_supply::get_daily_supply(&db_pool)
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

    Ok(())
}
