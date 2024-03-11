use async_trait::async_trait;
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::PgPool;
use thiserror::Error;

use crate::{execution_chain::ExecutionNodeBlock, time_frames::TimeFrame, units::UsdNewtype};

use super::EthPrice;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GetEthPriceError {
    #[error("closest price to given block was too old")]
    PriceTooOld,
}

#[async_trait]
pub trait EthPriceStore {
    async fn average_from_time_range(
        &self,
        start_timestamp: DateTime<Utc>,
        end_timestamp: DateTime<Utc>,
    ) -> UsdNewtype;
    async fn average_from_block_plus_time_range(
        &self,
        block: &ExecutionNodeBlock,
        time_frame: &TimeFrame,
    ) -> UsdNewtype;
    async fn get_most_recent_price(&self) -> sqlx::Result<EthPrice>;
    async fn store_price(&self, timestamp: &DateTime<Utc>, usd: f64);
    async fn get_h24_average(&self) -> f64;
    async fn get_price_h24_ago(&self, duration: &Duration) -> Option<EthPrice>;
    async fn get_eth_price_by_minute(&self, minute: DateTime<Utc>) -> Option<f64>;
    async fn get_closest_price_by_block(
        &self,
        block: &ExecutionNodeBlock,
    ) -> Result<f64, GetEthPriceError>;
    async fn get_eth_price_by_block(
        &self,
        block: &ExecutionNodeBlock,
    ) -> Result<f64, GetEthPriceError>;
}

pub struct EthPriceStorePostgres {
    db_pool: PgPool,
}

impl EthPriceStorePostgres {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl EthPriceStore for EthPriceStorePostgres {
    async fn average_from_time_range(
        &self,
        start_timestamp: DateTime<Utc>,
        end_timestamp: DateTime<Utc>,
    ) -> UsdNewtype {
        sqlx::query!(
            r#"
            SELECT AVG(ethusd) AS "avg!"
            FROM eth_prices
            WHERE timestamp >= $1
            AND timestamp <= $2
            "#,
            start_timestamp,
            end_timestamp,
        )
        .fetch_one(&self.db_pool)
        .await
        .unwrap()
        .avg
        .into()
    }

    async fn average_from_block_plus_time_range(
        &self,
        block: &ExecutionNodeBlock,
        time_frame: &TimeFrame,
    ) -> UsdNewtype {
        let start_timestamp = block.timestamp - time_frame.duration();
        let end_timestamp = block.timestamp;

        self.average_from_time_range(start_timestamp, end_timestamp)
            .await
    }

    async fn get_most_recent_price(&self) -> sqlx::Result<EthPrice> {
        sqlx::query_as!(
            EthPrice,
            "
            SELECT
                timestamp, ethusd AS usd
            FROM
                eth_prices
            ORDER BY timestamp DESC
            LIMIT 1
            ",
        )
        .fetch_one(&self.db_pool)
        .await
    }

    async fn store_price(&self, timestamp: &DateTime<Utc>, usd: f64) {
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
        .execute(&self.db_pool)
        .await
        .unwrap();
    }

    #[allow(dead_code)]
    async fn get_h24_average(&self) -> f64 {
        sqlx::query!(
            r#"
        SELECT
            AVG(ethusd) AS "average!"
        FROM
            eth_prices
        WHERE timestamp >= NOW() - '24 hours'::INTERVAL
        "#,
        )
        .fetch_one(&self.db_pool)
        .await
        .unwrap()
        .average
    }

    async fn get_price_h24_ago(&self, age_limit: &Duration) -> Option<EthPrice> {
        sqlx::query_as!(
            EthPrice,
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
            SELECT ethusd AS usd, timestamp
            FROM eth_price_distances
            WHERE distance_seconds <= $1
            LIMIT 1
            ",
            age_limit.num_seconds() as i32
        )
        .fetch_optional(&self.db_pool)
        .await
        .unwrap()
    }

    async fn get_eth_price_by_minute(&self, timestamp: DateTime<Utc>) -> Option<f64> {
        sqlx::query!(
            r#"
            SELECT ethusd
            FROM eth_prices
            WHERE timestamp = $1
            "#,
            timestamp,
        )
        .fetch_optional(&self.db_pool)
        .await
        .unwrap()
        .map(|row| row.ethusd)
    }

    async fn get_closest_price_by_block(
        &self,
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
        .fetch_one(&self.db_pool)
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
    async fn get_eth_price_by_block(
        &self,
        block: &ExecutionNodeBlock,
    ) -> Result<f64, GetEthPriceError> {
        let price = self
            .get_eth_price_by_minute(
                block
                    .timestamp
                    .duration_trunc(Duration::minutes(1))
                    .unwrap(),
            )
            .await;

        match price {
            Some(price) => Ok(price),
            None => self.get_closest_price_by_block(block).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use test_context::test_context;

    use crate::db::tests::TestDb;

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
            transactions: vec![],
            blob_gas_used: None,
            excess_blob_gas: None,
        }
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn store_price_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0),
            usd: 0.0,
        };

        eth_price_store
            .store_price(&test_price.timestamp, test_price.usd)
            .await;
        let eth_price = eth_price_store.get_most_recent_price().await.unwrap();
        assert_eq!(eth_price, test_price);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_most_recent_price_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_price_1 = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::seconds(10),
            usd: 0.0,
        };
        let test_price_2 = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0),
            usd: 1.0,
        };

        eth_price_store
            .store_price(&test_price_1.timestamp, test_price_1.usd)
            .await;
        eth_price_store
            .store_price(&test_price_2.timestamp, test_price_2.usd)
            .await;
        let eth_price = eth_price_store.get_most_recent_price().await.unwrap();
        assert_eq!(eth_price, test_price_2);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn insert_get_eth_price_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_block = make_test_block();

        eth_price_store.store_price(&Utc::now(), 5.2).await;
        let ethusd = eth_price_store
            .get_closest_price_by_block(&test_block)
            .await
            .unwrap();

        assert_eq!(ethusd, 5.2);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_eth_price_too_old_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_block = make_test_block();

        eth_price_store
            .store_price(&(Utc::now() - Duration::minutes(21)), 5.2)
            .await;
        let ethusd = eth_price_store
            .get_closest_price_by_block(&test_block)
            .await;
        assert_eq!(ethusd, Err(GetEthPriceError::PriceTooOld));
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_eth_price_old_block_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_block = make_test_block();

        eth_price_store
            .store_price(&(Utc::now() - Duration::minutes(6)), 4.0)
            .await;
        let ethusd = eth_price_store
            .get_closest_price_by_block(&ExecutionNodeBlock {
                timestamp: Utc::now() - Duration::minutes(10),
                ..test_block
            })
            .await
            .unwrap();
        assert_eq!(ethusd, 4.0);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_h24_average_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_price_1 = EthPrice {
            timestamp: Utc::now() - Duration::hours(23),
            usd: 10.0,
        };
        let test_price_2 = EthPrice {
            timestamp: Utc::now(),
            usd: 20.0,
        };

        eth_price_store
            .store_price(&test_price_1.timestamp, test_price_1.usd)
            .await;
        eth_price_store
            .store_price(&test_price_2.timestamp, test_price_2.usd)
            .await;

        let price_h24_average = eth_price_store.get_h24_average().await;
        assert_eq!(price_h24_average, 15.0);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_price_h24_ago_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());

        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(24),
            usd: 0.0,
        };

        eth_price_store
            .store_price(&test_price.timestamp, test_price.usd)
            .await;

        let price = eth_price_store
            .get_price_h24_ago(&Duration::minutes(10))
            .await;
        assert_eq!(price, Some(test_price));
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_price_h24_ago_limit_test(test_db: &TestDb) {
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());

        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::hours(25),
            usd: 0.0,
        };

        eth_price_store
            .store_price(&test_price.timestamp, test_price.usd)
            .await;

        let price = eth_price_store
            .get_price_h24_ago(&Duration::minutes(10))
            .await;
        assert_eq!(price, None);
    }
}
