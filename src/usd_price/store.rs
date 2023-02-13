use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::{PgExecutor, PgPool, Postgres};
use thiserror::Error;

use crate::execution_chain::ExecutionNodeBlock;

use super::EthPrice;

pub async fn get_most_recent_price(executor: impl PgExecutor<'_>) -> sqlx::Result<EthPrice> {
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

pub async fn store_price(executor: impl PgExecutor<'_>, timestamp: DateTime<Utc>, usd: f64) {
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
async fn get_h24_average(executor: impl PgExecutor<'_>) -> f64 {
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

pub async fn get_price_h24_ago(
    executor: impl PgExecutor<'_>,
    age_limit: Duration,
) -> Option<EthPrice> {
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

async fn get_eth_price_by_minute(
    executor: impl PgExecutor<'_>,
    timestamp: DateTime<Utc>,
) -> Option<f64> {
    sqlx::query!(
        r#"
        SELECT ethusd
        FROM eth_prices
        WHERE timestamp = $1
        "#,
        timestamp,
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| row.ethusd)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GetEthPriceError {
    #[error("closest price to given block was more than 5min away")]
    PriceTooOld,
}

async fn get_closest_price_by_block(
    executor: impl PgExecutor<'_>,
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

// We'll often have a price for the closest round minute. This is much faster to lookup. If we
// don't we can fall back to the slower to fetch closest price.
pub async fn get_eth_price_by_block(
    db_pool: &PgPool,
    block: &ExecutionNodeBlock,
) -> Result<f64, GetEthPriceError> {
    let price = get_eth_price_by_minute(
        db_pool,
        block
            .timestamp
            .duration_trunc(Duration::minutes(1))
            .unwrap(),
    )
    .await;

    match price {
        Some(price) => Ok(price),
        None => get_closest_price_by_block(db_pool, block).await,
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use sqlx::Acquire;

    use crate::db;

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
        let mut connection = db::tests::get_test_db_connection().await;
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
        let mut connection = db::tests::get_test_db_connection().await;
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
    async fn insert_get_eth_price_test() {
        let test_db = db::tests::TestDb::new().await;
        let test_block = make_test_block();

        store_price(&test_db.pool, Utc::now(), 5.2).await;
        let ethusd = get_closest_price_by_block(&test_db.pool, &test_block)
            .await
            .unwrap();

        assert_eq!(ethusd, 5.2);
    }

    #[tokio::test]
    async fn get_eth_price_too_old_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_price(&mut tx, Utc::now() - Duration::minutes(21), 5.2).await;
        let ethusd = get_closest_price_by_block(&mut tx, &test_block).await;
        assert_eq!(ethusd, Err(GetEthPriceError::PriceTooOld));
    }

    #[tokio::test]
    async fn get_eth_price_old_block_test() {
        let mut db = db::tests::get_test_db_connection().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        store_price(&mut tx, Utc::now() - Duration::minutes(6), 4.0).await;
        let ethusd = get_closest_price_by_block(
            &mut tx,
            &ExecutionNodeBlock {
                timestamp: Utc::now() - Duration::minutes(10),
                ..test_block
            },
        )
        .await
        .unwrap();
        assert_eq!(ethusd, 4.0);
    }

    #[tokio::test]
    async fn get_h24_average_test() {
        let mut connection = db::tests::get_test_db_connection().await;
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
        let mut connection = db::tests::get_test_db_connection().await;
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
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(25),
            usd: 0.0,
        };

        store_price(&mut transaction, test_price.timestamp, test_price.usd).await;

        let price = get_price_h24_ago(&mut transaction, Duration::minutes(10)).await;
        assert_eq!(price, None);
    }
}
