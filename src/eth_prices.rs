mod ftx;

use std::collections::HashSet;

use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, Connection, FromRow, PgConnection, Postgres, Row};
use tokio::time::sleep;

use crate::{
    caching::{self, CacheKey},
    config,
    execution_chain::LONDON_HARDFORK_TIMESTAMP,
    key_value_store,
};

#[derive(Debug, FromRow)]
struct EthPriceTimestamp {
    timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, FromRow, PartialEq)]
pub struct EthPrice {
    timestamp: DateTime<Utc>,
    #[sqlx(rename = "ethusd")]
    usd: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EthPriceStats {
    timestamp: DateTime<Utc>,
    usd: f64,
    h24_change: f64,
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

#[allow(dead_code)]
async fn get_h24_average(executor: &mut PgConnection) -> f64 {
    sqlx::query(
        "
            SELECT
                AVG(ethusd) AS average
            FROM
                eth_prices
            WHERE timestamp >= NOW() - '24 hours'::INTERVAL
        ",
    )
    .map(|row: PgRow| row.get::<f64, _>("average"))
    .fetch_one(executor)
    .await
    .unwrap()
}

async fn get_price_h24_ago(executor: &mut PgConnection, age_limit: Duration) -> Option<EthPrice> {
    sqlx::query_as::<Postgres, EthPrice>(
        "
            WITH
              eth_price_distances AS (
                SELECT
                  ethusd,
                  timestamp,
                  ABS(
                    EXTRACT(
                      epoch
                      FROM
                        (timestamp - (NOW() - '24 hours':: INTERVAL))
                    )
                  ) AS distance_seconds
                FROM
                  eth_prices
                ORDER BY
                  distance_seconds ASC
              )
            SELECT ethusd, timestamp
            FROM eth_price_distances
            WHERE distance_seconds <= 600
            LIMIT 1
        ",
    )
    .bind(age_limit.num_seconds())
    .fetch_optional(executor)
    .await
    .unwrap()
}

fn calc_h24_change(current_price: &EthPrice, price_h24_ago: &EthPrice) -> f64 {
    (current_price.usd - price_h24_ago.usd) / price_h24_ago.usd
}

async fn update_eth_price_with_most_recent(
    connection: &mut PgConnection,
    last_price: &mut EthPrice,
) {
    let most_recent_price = ftx::get_most_recent_price().await;
    match most_recent_price {
        None => {
            tracing::debug!("no recent eth price found");
        }
        Some(most_recent_price) => {
            if last_price == &most_recent_price {
                tracing::debug!(
                    "most recent eth price is equal to last stored price: {}, minute: {}, skipping",
                    last_price.timestamp,
                    last_price.usd
                );
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
                connection,
                most_recent_price.timestamp,
                most_recent_price.usd,
            )
            .await;

            *last_price = most_recent_price;

            let price_h24_ago = get_price_h24_ago(connection, Duration::minutes(10))
                .await
                .expect("24h old price should be available within 10min of now - 24h");

            let eth_price_stats = EthPriceStats {
                timestamp: last_price.timestamp,
                usd: last_price.usd,
                h24_change: calc_h24_change(&last_price, &price_h24_ago),
            };

            key_value_store::set_value(
                sqlx::Acquire::acquire(&mut *connection).await.unwrap(),
                CacheKey::EthPrice.to_db_key(),
                &serde_json::to_value(&eth_price_stats).unwrap(),
            )
            .await;

            caching::publish_cache_update(connection, CacheKey::EthPrice).await;
        }
    }
}

pub async fn record_eth_price() {
    tracing_subscriber::fmt::init();

    tracing::info!("recording eth prices");

    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();

    let mut last_price = get_most_recent_price(&mut connection).await;

    loop {
        update_eth_price_with_most_recent(&mut connection, &mut last_price).await;
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

const RESYNC_ETH_PRICES_KEY: &str = "resync-eth-prices";

async fn get_last_synced_minute(executor: &mut PgConnection) -> Option<u32> {
    sqlx::query(
        "
            SELECT
                value
            FROM
                key_value_store
            WHERE
                key = $1
        ",
    )
    .bind(RESYNC_ETH_PRICES_KEY)
    .map(|row: PgRow| {
        let value: Value = row.get("value");
        serde_json::from_value::<u32>(value).unwrap()
    })
    .fetch_optional(executor)
    .await
    .unwrap()
}

async fn set_last_synced_minute(executor: &mut PgConnection, minute: u32) {
    sqlx::query(
        "
            INSERT INTO
                key_value_store (key, value)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value
        ",
    )
    .bind(RESYNC_ETH_PRICES_KEY)
    .bind(json!(minute))
    .execute(executor)
    .await
    .unwrap();
}

pub async fn resync_all() {
    tracing_subscriber::fmt::init();

    tracing::info!("resyncing all eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();

    tracing::debug!("walking through all minutes since London hardfork");

    let duration_since_london =
        Utc::now().duration_round(Duration::minutes(1)).unwrap() - *LONDON_HARDFORK_TIMESTAMP;
    let minutes_since_london: u32 = duration_since_london.num_minutes().try_into().unwrap();

    let london_minute_timestamp: u32 = LONDON_HARDFORK_TIMESTAMP
        .duration_round(Duration::minutes(1))
        .unwrap()
        .timestamp()
        .try_into()
        .unwrap();

    let start_minute = get_last_synced_minute(&mut connection)
        .await
        .map_or(0, |minute| minute + 1);

    tracing::debug!(
        "starting at {}",
        Utc.timestamp(london_minute_timestamp.into(), 0) + Duration::minutes((start_minute).into())
    );

    let mut progress = pit_wall::Progress::new(
        "resync eth prices",
        (minutes_since_london - start_minute).into(),
    );

    for minute_n in start_minute..minutes_since_london {
        let timestamp = london_minute_timestamp + minute_n * 60;
        let timestamp_date_time = Utc.timestamp(timestamp.into(), 0);

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
                store_price(&mut connection, timestamp_date_time, usd).await;
            }
        }

        progress.inc_work_done();

        // Every 100 minutes, store which minute we last resynced.
        if minute_n != 0 && minute_n % 100 == 0 {
            tracing::debug!("100 minutes synced, checkpointing at {timestamp_date_time}");
            set_last_synced_minute(&mut connection, minute_n).await;

            tracing::info!("{}", progress.get_progress_string());
        };
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;

    use crate::db_testing;

    use super::*;

    #[tokio::test]
    async fn store_price_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0),
            usd: 0.0,
        };

        store_price(&mut transaction, test_price.timestamp, test_price.usd).await;
        let eth_price = get_most_recent_price(&mut transaction).await;
        assert_eq!(eth_price, test_price);
    }

    #[tokio::test]
    async fn get_most_recent_price_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let test_price_1 = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::seconds(10),
            usd: 0.0,
        };
        let test_price_2 = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0),
            usd: 1.0,
        };

        store_price(&mut transaction, test_price_1.timestamp, test_price_1.usd).await;
        store_price(&mut transaction, test_price_2.timestamp, test_price_2.usd).await;
        let eth_price = get_most_recent_price(&mut transaction).await;
        assert_eq!(eth_price, test_price_2);
    }

    #[tokio::test]
    async fn update_eth_price_with_most_recent_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::minutes(10),
            usd: 0.0,
        };

        store_price(&mut transaction, Utc::now() - Duration::hours(24), 0.0).await;

        let mut last_price = test_price.clone();

        update_eth_price_with_most_recent(&mut transaction, &mut last_price).await;

        let eth_price = get_most_recent_price(&mut transaction).await;

        assert_ne!(eth_price, test_price);
    }

    #[tokio::test]
    async fn get_h24_average_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let test_price_1 = EthPrice {
            timestamp: Utc::now() - Duration::hours(23),
            usd: 10.0,
        };
        let test_price_2 = EthPrice {
            timestamp: Utc::now(),
            usd: 20.0,
        };

        store_price(&mut transaction, test_price_1.timestamp, test_price_1.usd).await;
        store_price(&mut transaction, test_price_2.timestamp, test_price_2.usd).await;

        let price_h24_average = get_h24_average(&mut transaction).await;
        assert_eq!(price_h24_average, 15.0);
    }

    #[tokio::test]
    async fn get_price_h24_ago_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(24),
            usd: 0.0,
        };

        store_price(&mut transaction, test_price.timestamp, test_price.usd).await;

        let price = get_price_h24_ago(&mut transaction, Duration::minutes(10)).await;
        assert_eq!(price, Some(test_price));
    }

    #[tokio::test]
    async fn get_price_h24_ago_limit_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(25),
            usd: 0.0,
        };

        store_price(&mut transaction, test_price.timestamp, test_price.usd).await;

        let price = get_price_h24_ago(&mut transaction, Duration::minutes(10)).await;
        assert_eq!(price, None);
    }

    #[tokio::test]
    async fn get_set_last_synced_minute_test() {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        set_last_synced_minute(&mut transaction, 1559).await;

        let minute = get_last_synced_minute(&mut transaction).await;
        assert_eq!(minute, Some(1559));
    }
}
