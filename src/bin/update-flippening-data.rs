use std::io::{BufWriter, Write};
use std::{collections::HashMap, fs::File};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use eth_analysis::{
    beacon_chain::{self, GweiInTime},
    caching::{self, CacheKey},
    db,
    dune::get_flippening_data,
    eth_supply, log,
    units::GWEI_PER_ETH_F64,
    SupplyAtTime,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use sqlx::Decode;
use tracing::{debug, info, warn};

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
struct FlippeningDatapoint {
    pub t: u64,
    pub eth_price: Option<f64>,
    pub btc_price: Option<f64>,
    pub bitcoin_supply: Option<f64>,
    pub eth_supply: Option<f64>,
    pub is_presale_period: bool,
}

#[tokio::main]
pub async fn main() {
    log::init_with_env();

    info!("updating flippening data");

    let db_pool = db::get_db_pool("flippening-data", 3).await;

    // sqlx::migrate!().run(&db_pool).await.unwrap();

    let read_from_file = std::env::var("MOCK_DUNE_API").is_ok();
    let raw_dune_data = if read_from_file {
        warn!("Reading from file instead of DUNE API");
        let data = std::fs::read_to_string("raw_dune_values.json").expect("Unable to read file");
        serde_json::from_str(&data).expect("JSON does not have correct format.")
    } else {
        get_flippening_data().await.unwrap()
    };

    info!(
        "got flippening data from dune, {} data points",
        raw_dune_data.len()
    );

    let supply_by_day: Vec<TimestampValuePoint> = eth_supply::get_daily_supply(&db_pool)
        .await
        .into_iter()
        .filter(|point| point.timestamp >= *SUPPLY_LOWER_LIMIT_DATE_TIME)
        .map(Into::into)
        .collect();

    let first_supply_datapoint = supply_by_day[0].t;
    info!("got first supply data point at: {:}", supply_by_day[0].t);

    let eth_supply_map: HashMap<u64, f64> = supply_by_day
        .into_iter()
        .map(|point| (point.t, point.v))
        .collect();
    info!("got supply by day, {} data points", eth_supply_map.len());

    let flippening_data: Vec<FlippeningDatapoint> = raw_dune_data
        .into_iter()
        .map(|row| {
            let t = NaiveDateTime::parse_from_str(&row.time, "%Y-%m-%d %H:%M:%S%.f UTC")
                .unwrap()
                .timestamp() as u64;
            FlippeningDatapoint {
                t,
                eth_price: row.eth_price,
                btc_price: row.btc_price,
                bitcoin_supply: row.bitcoin_supply,
                eth_supply: eth_supply_map.get(&t).copied(),
                is_presale_period: t < first_supply_datapoint,
            }
        })
        .collect();

    // Write to Json
    let file = File::create("flippening_data_combined.json").unwrap();
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &flippening_data).unwrap();
    writer.flush().unwrap();

    // // Deprecate supplyData, lockedData, stakedData after prod frontend has switched to new flippening data.
    // let flippening_data = FlippeningData {
    //     supply_by_day,
    // };

    // caching::update_and_publish(
    //     &db_pool,
    //     &CacheKey::FlippeningData,
    //     &flippening_data,
    // )
    // .await;

    info!("done updating flippening data");
}
