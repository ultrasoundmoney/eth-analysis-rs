use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Row};
use thiserror::Error;

use super::node::ExecutionNodeBlock;

#[allow(dead_code)]
async fn insert_eth_price(executor: &mut PgConnection, timestamp: DateTime<Utc>, ethusd: f64) {
    sqlx::query(
        "
            INSERT INTO
                eth_prices (
                    timestamp,
                    ethusd
                )
            VALUES
                ($1, $2)
        ",
    )
    .bind(timestamp)
    .bind(ethusd)
    .execute(executor)
    .await
    .unwrap();
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
    let (timestamp, ethusd) = sqlx::query(
        "
            SELECT
              timestamp,
              ethusd
            FROM eth_prices
            ORDER BY ABS(EXTRACT(epoch FROM (timestamp - $1 )))
            LIMIT 1
        ",
    )
    .bind(block.timestamp)
    .map(|row: PgRow| {
        let timestamp = row.get::<DateTime<Utc>, _>("timestamp");
        let ethusd = row.get::<f64, _>("ethusd");
        (timestamp, ethusd)
    })
    .fetch_one(executor)
    .await
    .unwrap();

    if Utc::now() - timestamp <= Duration::minutes(5) {
        Ok(ethusd)
    } else {
        Err(GetEthPriceError::PriceTooOld)
    }
}

#[cfg(test)]
mod tests {
    use sqlx::Acquire;

    use super::*;
    use crate::db_testing::get_test_db;

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
    async fn insert_get_eth_price_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        insert_eth_price(&mut *tx, Utc::now(), 5.2).await;
        let ethusd = get_eth_price_by_block(&mut *tx, &test_block).await.unwrap();

        assert_eq!(ethusd, 5.2);
    }

    #[tokio::test]
    async fn get_eth_price_too_old_test() {
        let mut db = get_test_db().await;
        let mut tx = db.begin().await.unwrap();
        let test_block = make_test_block();

        insert_eth_price(&mut *tx, Utc::now() - Duration::minutes(6), 5.2).await;
        let ethusd = get_eth_price_by_block(&mut *tx, &test_block).await;
        assert_eq!(ethusd, Err(GetEthPriceError::PriceTooOld));
    }
}
