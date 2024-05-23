use std::io::{BufWriter, Write};
use std::{collections::HashMap, fs::File};

use chrono::{DateTime, NaiveDateTime, Utc};
use eth_analysis::{
    beacon_chain::GweiInTime,
    caching::{self, CacheKey},
    db,
    dune::get_flippening_data,
    eth_supply, log,
    SupplyAtTime,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use sqlx::Decode;
use tracing::{info, warn};

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

fn forward_fill(data: &mut [FlippeningDatapointPartial]) {
    let mut last_eth_price: Option<f64> = None;
    let mut last_eth_supply: Option<f64> = None;
    let mut last_btc_price: Option<f64> = None;
    let mut last_bitcoin_supply: Option<f64> = None;

    for entry in data.iter_mut() {
        if entry.eth_price.is_none() {
            entry.eth_price = last_eth_price;
        } else {
            last_eth_price = entry.eth_price;
        }

        if entry.eth_supply.is_none() {
            entry.eth_supply = last_eth_supply;
        } else {
            last_eth_supply = entry.eth_supply;
        }

        if entry.btc_price.is_none() {
            entry.btc_price = last_btc_price;
        } else {
            last_btc_price = entry.btc_price;
        }

        if entry.bitcoin_supply.is_none() {
            entry.bitcoin_supply = last_bitcoin_supply;
        } else {
            last_bitcoin_supply = entry.bitcoin_supply;
        }

    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FlippeningDatapoint {
    pub t: u64,
    pub marketcap_ratio: Option<f64>,
    pub eth_price: Option<f64>,
    pub eth_marketcap: Option<f64>,
    pub btc_price: Option<f64>,
    pub btc_marketcap: Option<f64>,
    pub bitcoin_supply: Option<f64>,
    pub eth_supply: Option<f64>,
    pub is_presale_period: bool,
}

struct FlippeningDatapointPartial {
    pub t: u64,
    pub eth_price: Option<f64>,
    pub btc_price: Option<f64>,
    pub bitcoin_supply: Option<f64>,
    pub eth_supply: Option<f64>,
    pub is_presale_period: bool,
}

const OUTPUT_FILE_RAW_DATA: &str = "raw_dune_values.json";
const PRESALE_ETH_BTC_RATIO: f64 = 1337.0;
#[tokio::main]
pub async fn main() {
    log::init_with_env();

    info!("updating flippening data");

    let db_pool = db::get_db_pool("flippening-data", 3).await;

    // sqlx::migrate!().run(&db_pool).await.unwrap();

    let read_from_file = std::env::var("MOCK_DUNE_API").is_ok();
    let raw_dune_data = if read_from_file {
        warn!("Reading from file instead of DUNE API");
        let data = std::fs::read_to_string(OUTPUT_FILE_RAW_DATA).expect("Unable to read file");
        serde_json::from_str(&data).expect("JSON does not have correct format.")
    } else {
        get_flippening_data().await.unwrap()
    };

    let write_to_file = std::env::var("CACHE_DUNE_RESPONSE").is_ok();
    if write_to_file {
        // Write to json file
        let file = File::create(OUTPUT_FILE_RAW_DATA).unwrap();
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, &raw_dune_data).unwrap();
        writer.flush().unwrap();

    }

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

    let first_supply_datapoint = &supply_by_day[0];
    info!("got first supply data point at: {:}", first_supply_datapoint.t);

    let eth_supply_map: HashMap<u64, f64> = supply_by_day
        .iter()
        .map(|point| (point.t, point.v))
        .collect();
    info!("got supply by day, {} data points", eth_supply_map.len());

    let mut flippening_data: Vec<FlippeningDatapointPartial> = raw_dune_data
        .into_iter()
        .map(|row| {
            let t = NaiveDateTime::parse_from_str(&row.time, "%Y-%m-%d %H:%M:%S%.f UTC")
                .unwrap()
                .timestamp() as u64;
            let is_presale_period = t < first_supply_datapoint.t;
            let eth_supply = if is_presale_period { Some(first_supply_datapoint.v) } else { eth_supply_map.get(&t).copied() };
            let eth_price = if is_presale_period { row.btc_price.map(|btc_price| btc_price / PRESALE_ETH_BTC_RATIO)} else { row.eth_price };
            FlippeningDatapointPartial {
                t,
                eth_price,
                btc_price: row.btc_price,
                bitcoin_supply: row.bitcoin_supply,
                eth_supply,
                is_presale_period,
            }
        })
        .collect();
    flippening_data.sort_by_key(|row| row.t);
    forward_fill(&mut flippening_data);

    let flippening_data: Vec<FlippeningDatapoint> = flippening_data
        .into_iter()
        .map(|row| {
            let eth_marketcap = row.eth_price.zip(row.eth_supply).map(|(price, supply)| price*supply);
            let btc_marketcap = row.btc_price.zip(row.bitcoin_supply).map(|(price, supply)| price*supply);
            let marketcap_ratio = eth_marketcap.zip(btc_marketcap).map(|(eth, btc)| eth/btc);
            FlippeningDatapoint {
                t: row.t,
                marketcap_ratio,
                eth_price: row.eth_price,
                eth_marketcap,
                btc_price: row.btc_price,
                btc_marketcap,
                bitcoin_supply: row.bitcoin_supply,
                eth_supply: row.eth_supply,
                is_presale_period: row.is_presale_period,
            }
        })
        .collect();

    caching::update_and_publish(
        &db_pool,
        &CacheKey::FlippeningData,
        &flippening_data,
    )
    .await;

    info!("done updating flippening data");
}
