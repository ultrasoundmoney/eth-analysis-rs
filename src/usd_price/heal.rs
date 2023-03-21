use std::collections::HashSet;

use chrono::{Duration, DurationRound, TimeZone, Utc};
use sqlx::{Connection, PgConnection, Postgres};
use tracing::{debug, info};

use crate::{db, execution_chain, log};

use super::{bybit, store, EthPriceTimestamp};

pub async fn heal_eth_prices() {
    log::init_with_env();

    info!("healing missing eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    debug!("getting all eth prices");
    let mut connection = PgConnection::connect(&db::get_db_url_with_name("heal-eth-prices"))
        .await
        .unwrap();
    let eth_prices = sqlx::query_as::<Postgres, EthPriceTimestamp>(
        "
            SELECT
                timestamp
            FROM
                eth_prices
        ",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();

    if eth_prices.is_empty() {
        panic!("no eth prices found, are you running against a DB with prices?")
    }

    debug!("building set of known minutes");
    let mut known_minutes = HashSet::new();

    for eth_price in eth_prices.iter() {
        known_minutes.insert(eth_price.timestamp.timestamp());
    }

    debug!("walking through all minutes since London hardfork to look for missing minutes");

    let duration_since_london = Utc::now().duration_round(Duration::minutes(1)).unwrap()
        - *execution_chain::LONDON_HARD_FORK_TIMESTAMP;
    let minutes_since_london = duration_since_london.num_minutes();

    let london_minute_timestamp = execution_chain::LONDON_HARD_FORK_TIMESTAMP
        .duration_round(Duration::minutes(1))
        .unwrap()
        .timestamp();

    for minute_n in 0..minutes_since_london {
        let timestamp = london_minute_timestamp + minute_n * 60;
        if !known_minutes.contains(&timestamp) {
            let timestamp_date_time = Utc.timestamp_opt(timestamp, 0).unwrap();
            debug!(minute = timestamp_date_time.to_string(), "missing minute");
            let usd = bybit::get_closest_price_by_minute(
                timestamp_date_time,
                Duration::minutes(max_distance_in_minutes),
            )
            .await;
            match usd {
                None => {
                    debug!(
                        timestamp = timestamp_date_time.to_string(),
                        "no Bybit price available",
                    );
                }
                Some(usd) => {
                    debug!("found a price on Bybit, adding it to the DB");
                    store::store_price(&mut connection, timestamp_date_time, usd).await;
                }
            }
        }
    }

    info!("done healing eth prices");
}
