mod ftx;

use std::collections::HashSet;

use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use serde::Serialize;
use sqlx::{Connection, FromRow, PgConnection, Postgres};
use tokio::time::sleep;

use crate::{
    caching::{self, CacheKey},
    config,
    execution_chain::LONDON_HARDFORK_TIMESTAMP,
    key_value_store::{self, KeyValue},
};

#[derive(Debug, FromRow)]
struct EthPriceTimestamp {
    timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, FromRow, PartialEq, Serialize)]
pub struct EthPrice {
    timestamp: DateTime<Utc>,
    #[sqlx(rename = "ethusd")]
    usd: f64,
}

async fn get_most_recent_price(executor: &mut PgConnection) -> EthPrice {
    sqlx::query_as::<Postgres, EthPrice>(
        "
            SELECT
                timestamp, ethusd
            FROM
                eth_prices
            ORDER BY timestamp DESC
            LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap()
}

async fn store_price(executor: &mut PgConnection, timestamp: DateTime<Utc>, usd: f64) {
    sqlx::query(
        "
            INSERT INTO
                eth_prices (timestamp, ethusd)
            VALUES ($1, $2)
            ON CONFLICT (timestamp) DO UPDATE SET
                ethusd = excluded.ethusd
        ",
    )
    .bind(timestamp)
    .bind(usd)
    .execute(executor)
    .await
    .unwrap();
}

pub async fn record_eth_price() {
    tracing_subscriber::fmt::init();

    tracing::info!("recording eth prices");

    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();

    let mut last_price = get_most_recent_price(&mut connection).await;

    loop {
        let most_recent_price = ftx::get_most_recent_price().await;
        match most_recent_price {
            None => {
                tracing::debug!("no recent eth price found");
            }
            Some(most_recent_price) => {
                if last_price == most_recent_price {
                    tracing::debug!("most recent eth price is equal to last stored price: {}, minute: {}, skipping", last_price.timestamp, last_price.usd);
                }

                if last_price.timestamp == most_recent_price.timestamp {
                    tracing::debug!(
                        "most recent price is same minute as last: {}, but price: {} -> {}",
                        last_price.timestamp,
                        last_price.usd,
                        most_recent_price.usd
                    );
                } else {
                    tracing::debug!(
                        "storing new most recent eth price, minute: {}, price: {}",
                        most_recent_price.timestamp,
                        most_recent_price.usd
                    );
                }

                store_price(
                    &mut connection,
                    most_recent_price.timestamp,
                    most_recent_price.usd,
                )
                .await;
                last_price = most_recent_price.clone();

                key_value_store::set_value(
                    &mut connection,
                    KeyValue {
                        key: CacheKey::EthPrice.to_db_key(),
                        value: &serde_json::to_value(&most_recent_price).unwrap(),
                    },
                )
                .await;

                caching::publish_cache_update(&mut connection, CacheKey::EthPrice).await;
            }
        }
        sleep(std::time::Duration::from_secs(10)).await;
    }
}

pub async fn heal_eth_prices() {
    tracing_subscriber::fmt::init();

    tracing::info!("healing missing eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    tracing::debug!("getting all eth prices");
    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
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

    if eth_prices.len() == 0 {
        panic!("no eth prices found, are you running against a DB with prices?")
    }

    tracing::debug!("building set of known minutes");
    let mut known_minutes = HashSet::new();

    for eth_price in eth_prices.iter() {
        known_minutes.insert(eth_price.timestamp.timestamp());
    }

    tracing::debug!(
        "walking through all minutes since London hardfork to look for missing minutes"
    );

    let duration_since_london =
        Utc::now().duration_round(Duration::minutes(1)).unwrap() - *LONDON_HARDFORK_TIMESTAMP;
    let minutes_since_london = duration_since_london.num_minutes();

    let london_minute_timestamp = LONDON_HARDFORK_TIMESTAMP
        .duration_round(Duration::minutes(1))
        .unwrap()
        .timestamp();

    for minute_n in 0..minutes_since_london {
        let timestamp = london_minute_timestamp + minute_n * 60;
        if !known_minutes.contains(&timestamp) {
            let timestamp_date_time = Utc.timestamp(timestamp, 0);
            tracing::debug!("missing minute: {}", timestamp_date_time);
            let usd = ftx::get_closest_price_by_minute(
                timestamp_date_time,
                Duration::minutes(max_distance_in_minutes),
            )
            .await;
            match usd {
                None => {
                    tracing::debug!(
                        "FTX didn't have a price either for: {}",
                        timestamp_date_time
                    );
                }
                Some(usd) => {
                    tracing::debug!("found a price on FTX, adding it to the DB");
                    store_price(&mut connection, timestamp_date_time, usd).await;
                }
            }
        }
    }
}
