use std::collections::HashSet;

use chrono::{Duration, DurationRound, TimeZone, Utc};
use sqlx::Postgres;
use store::EthPriceStore;
use tracing::{debug, info};

use crate::{db, execution_chain, log};

use super::{bybit, store, EthPriceTimestamp};
use futures::stream::{self, StreamExt};
use tracing::warn;

pub async fn heal_eth_prices() {
    log::init_with_env();

    info!("healing missing eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    info!("getting all eth prices");
    let db_pool = db::get_db_pool("heal-eth-prices").await;

    let eth_price_store = store::EthPriceStorePostgres::new(db_pool.clone());

    let eth_prices = sqlx::query_as::<Postgres, EthPriceTimestamp>(
        "
            SELECT
                timestamp
            FROM
                eth_prices
        ",
    )
    .fetch_all(&db_pool)
    .await
    .unwrap();

    if eth_prices.is_empty() {
        warn!("no eth prices found, are you running against a DB with prices?")
    }

    info!("building set of known minutes");
    let mut known_minutes = HashSet::new();

    for eth_price in eth_prices.iter() {
        known_minutes.insert(eth_price.timestamp.timestamp());
    }

    info!("walking through all minutes since London hardfork to look for missing minutes");

    let duration_since_london = Utc::now().duration_round(Duration::minutes(1)).unwrap()
        - *execution_chain::LONDON_HARD_FORK_TIMESTAMP;
    let minutes_since_london = duration_since_london.num_minutes();

    let london_minute_timestamp = execution_chain::LONDON_HARD_FORK_TIMESTAMP
        .duration_round(Duration::minutes(1))
        .unwrap()
        .timestamp();

    let missing_minutes_timestamps = (0..minutes_since_london)
        .map(|minutes| london_minute_timestamp + minutes * 60)
        .filter(|timestamp| !known_minutes.contains(timestamp))
        .collect::<Vec<i64>>();

    let concurrent_requests = 8;
    info!("found {} missing minutes", missing_minutes_timestamps.len());
    let mut missing_minutes_stream = stream::iter(missing_minutes_timestamps)
        .map(|timestamp| async move {
            let timestamp_date_time = Utc.timestamp_opt(timestamp, 0).unwrap();
            debug!(minute = timestamp_date_time.to_string(), "missing minute");
            let usd = bybit::get_closest_price_by_minute(
                timestamp_date_time,
                Duration::minutes(max_distance_in_minutes),
            )
            .await;
            match usd {
                None => {
                    panic!(
                        "no Bybit price available for timestamp: {}",
                        timestamp_date_time,
                    );
                }
                Some(usd) => {
                    info!(
                        "found a price on Bybit for timestamp: {} - {}",
                        timestamp, usd
                    );
                }
            };
            (usd, timestamp_date_time)
        })
        .buffer_unordered(concurrent_requests);

    while let Some((usd, timestamp)) = missing_minutes_stream.next().await {
        if let Some(usd) = usd {
            debug!("Storing price for timestamp: {:?}", timestamp);
            eth_price_store.store_price(&timestamp, usd).await;
            debug!("Stored price for timestamp: {:?}", timestamp);
        }
    }

    info!("done healing eth prices");
}
