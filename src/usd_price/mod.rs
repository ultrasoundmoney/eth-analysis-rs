mod bybit;
mod heal;

pub use heal::heal_eth_prices;

use anyhow::Result;
use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, Connection, FromRow, PgConnection, Postgres, Row};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::{
    caching::{self, CacheKey},
    db,
    execution_chain::{ExecutionNodeBlock, LONDON_HARD_FORK_TIMESTAMP},
    key_value_store, log,
};

#[derive(Debug, FromRow)]
struct EthPriceTimestamp {
    timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, FromRow, PartialEq)]
pub struct EthPrice {
    pub timestamp: DateTime<Utc>,
    #[sqlx(rename = "ethusd")]
    pub usd: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EthPriceStats {
    timestamp: DateTime<Utc>,
    usd: f64,
    h24_change: f64,
}

async fn get_most_recent_price(executor: &mut PgConnection) -> sqlx::Result<EthPrice> {
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
}

async fn store_price(executor: &mut PgConnection, timestamp: DateTime<Utc>, usd: f64) {
    sqlx::query!(
        "
            INSERT INTO
                eth_prices (timestamp, ethusd)
            VALUES ($1, $2)
            ON CONFLICT (timestamp) DO UPDATE SET
                ethusd = excluded.ethusd
        ",
        timestamp,
        usd
    )
    .execute(executor)
    .await
    .unwrap();
}

#[allow(dead_code)]
async fn get_h24_average(executor: &mut PgConnection) -> f64 {
    sqlx::query!(
        r#"
            SELECT
                AVG(ethusd) AS "average!"
            FROM
                eth_prices
            WHERE timestamp >= NOW() - '24 hours'::INTERVAL
        "#,
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .average
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
) -> Result<()> {
    let most_recent_price = bybit::get_eth_price().await?;
    if last_price == &most_recent_price {
        debug!(
            price = last_price.usd,
            minute = last_price.timestamp.to_string(),
            "most recent eth price is equal to last stored price, skipping",
        );
    } else {
        if last_price.timestamp == most_recent_price.timestamp {
            debug!(
                minute = last_price.timestamp.to_string(),
                last_price = last_price.usd,
                most_recent_price = most_recent_price.usd,
                "found more recent price for existing minute",
            );
        } else {
            debug!(
                timestamp = most_recent_price.timestamp.to_string(),
                price = most_recent_price.usd,
                "new most recent price",
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
        .await?;

        caching::publish_cache_update(connection, &CacheKey::EthPrice).await?;
    }

    Ok(())
}

pub async fn record_eth_price() -> Result<()> {
    log::init_with_env();

    info!("recording eth prices");

    let mut connection = PgConnection::connect(&db::get_db_url_with_name("record-eth-price"))
        .await
        .unwrap();

    let mut last_price = get_most_recent_price(&mut connection).await?;

    loop {
        update_eth_price_with_most_recent(&mut connection, &mut last_price).await?;
        sleep(std::time::Duration::from_secs(10)).await;
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
    log::init_with_env();

    info!("resyncing all eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    let mut connection = PgConnection::connect(&db::get_db_url_with_name("resync-all-prices"))
        .await
        .unwrap();

    debug!("walking through all minutes since London hardfork");

    let duration_since_london =
        Utc::now().duration_round(Duration::minutes(1)).unwrap() - *LONDON_HARD_FORK_TIMESTAMP;
    let minutes_since_london: u32 = duration_since_london.num_minutes().try_into().unwrap();

    let london_minute_timestamp: u32 = LONDON_HARD_FORK_TIMESTAMP
        .duration_round(Duration::minutes(1))
        .unwrap()
        .timestamp()
        .try_into()
        .unwrap();

    let start_minute = get_last_synced_minute(&mut connection)
        .await
        .map_or(0, |minute| minute + 1);

    debug!(
        "starting at {}",
        Utc.timestamp_opt(london_minute_timestamp.into(), 0)
            .unwrap()
            + Duration::minutes((start_minute).into())
    );

    let mut progress = pit_wall::Progress::new(
        "resync eth prices",
        (minutes_since_london - start_minute).into(),
    );

    for minute_n in start_minute..minutes_since_london {
        let timestamp = london_minute_timestamp + minute_n * 60;
        let timestamp_date_time = Utc.timestamp_opt(timestamp.into(), 0).unwrap();

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
                store_price(&mut connection, timestamp_date_time, usd).await;
            }
        }

        progress.inc_work_done();

        // Every 100 minutes, store which minute we last resynced.
        if minute_n != 0 && minute_n % 100 == 0 {
            debug!(
                timestamp = timestamp_date_time.to_string(),
                "100 minutes synced, checkpointing"
            );
            set_last_synced_minute(&mut connection, minute_n).await;

            info!("{}", progress.get_progress_string());
        };
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetEthPriceError {
    #[error("closest price to given block was more than 5min away")]
    PriceTooOld,
}

pub async fn get_eth_price_by_block(
    executor: &mut PgConnection,
    block: &ExecutionNodeBlock,
) -> Result<f64, GetEthPriceError> {
    let row = sqlx::query!(
        r#"
            SELECT
              timestamp,
              ethusd AS "ethusd!"
            FROM eth_prices
            ORDER BY ABS(EXTRACT(epoch FROM (timestamp - $1)))
            LIMIT 1
        "#,
        block.timestamp,
    )
    .fetch_one(executor)
    .await
    .unwrap();

    if block.timestamp - row.timestamp <= Duration::minutes(20) {
        Ok(row.ethusd)
    } else {
        Err(GetEthPriceError::PriceTooOld)
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;

    use super::*;

    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 0,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now(),
            total_difficulty: 0,
        }
    }

    #[tokio::test]
    async fn store_price_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0),
            usd: 0.0,
        };

        store_price(&mut transaction, test_price.timestamp, test_price.usd).await;
        let eth_price = get_most_recent_price(&mut transaction).await.unwrap();
        assert_eq!(eth_price, test_price);
    }

    #[tokio::test]
    async fn get_most_recent_price_test() {
        let mut connection = db::get_test_db().await;
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
        let eth_price = get_most_recent_price(&mut transaction).await.unwrap();
        assert_eq!(eth_price, test_price_2);
    }

    #[tokio::test]
    async fn update_eth_price_with_most_recent_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::minutes(10),
            usd: 0.0,
        };

        store_price(&mut transaction, Utc::now() - Duration::hours(24), 0.0).await;

        let mut last_price = test_price.clone();

        update_eth_price_with_most_recent(&mut transaction, &mut last_price)
            .await
            .unwrap();

        let eth_price = get_most_recent_price(&mut transaction).await.unwrap();

        assert_ne!(eth_price, test_price);
    }

    #[tokio::test]
    async fn get_h24_average_test() {
        let mut connection = db::get_test_db().await;
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
        let mut connection = db::get_test_db().await;
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
        let mut connection = db::get_test_db().await;
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
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        set_last_synced_minute(&mut transaction, 1559).await;

        let minute = get_last_synced_minute(&mut transaction).await;
        assert_eq!(minute, Some(1559));
    }

    #[tokio::test]
    async fn insert_get_eth_price_test() {
        let mut db = db::get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_price(&mut *tx, Utc::now(), 5.2).await;
        let ethusd = get_eth_price_by_block(&mut *tx, &test_block).await.unwrap();

        assert_eq!(ethusd, 5.2);
    }

    #[tokio::test]
    async fn get_eth_price_too_old_test() {
        let mut db = db::get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_price(&mut *tx, Utc::now() - Duration::minutes(21), 5.2).await;
        let ethusd = get_eth_price_by_block(&mut *tx, &test_block).await;
        assert_eq!(ethusd, Err(GetEthPriceError::PriceTooOld));
    }

    #[tokio::test]
    async fn get_eth_price_old_block_test() {
        let mut db = db::get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_price(&mut *tx, Utc::now() - Duration::minutes(6), 4.0).await;
        let ethusd = get_eth_price_by_block(
            &mut *tx,
            &ExecutionNodeBlock {
                timestamp: Utc::now() - Duration::minutes(10),
                ..test_block
            },
        )
        .await
        .unwrap();
        assert_eq!(ethusd, 4.0);
    }
}
