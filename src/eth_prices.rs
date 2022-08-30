mod ftx;

use std::collections::HashSet;

use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use sqlx::{Connection, FromRow, PgConnection, Postgres};

use crate::{config, execution_chain::LONDON_HARDFORK_TIMESTAMP};

#[derive(Debug, FromRow)]
struct EthPrice {
    timestamp: DateTime<Utc>,
}

async fn store_price(executor: &mut PgConnection, timestamp: DateTime<Utc>, usd: f64) {
    sqlx::query(
        "
            INSERT INTO
                eth_prices (timestamp, ethusd)
            VALUES ($1, $2)
        ",
    )
    .bind(timestamp)
    .bind(usd)
    .execute(executor)
    .await
    .unwrap();
}

pub async fn heal_eth_prices() {
    tracing_subscriber::fmt::init();

    tracing::debug!("healing missing eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    tracing::debug!("getting all eth prices");
    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
    let eth_prices = sqlx::query_as::<Postgres, EthPrice>(
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
            let price_usd = ftx::get_closest_price_by_minute(
                timestamp_date_time,
                Duration::minutes(max_distance_in_minutes),
            )
            .await;
            match price_usd {
                None => {
                    tracing::debug!(
                        "FTX didn't have a price either for: {}",
                        timestamp_date_time
                    );
                }
                Some(price_usd) => {
                    tracing::debug!("found a price on FTX, adding it to the DB");
                    store_price(&mut connection, timestamp_date_time, price_usd).await;
                }
            }
        }
    }
}
