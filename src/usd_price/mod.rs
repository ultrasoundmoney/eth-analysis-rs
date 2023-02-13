mod average;
mod bybit;
mod heal;
mod resync;
mod store;

pub use average::average_from_time_range;
pub use heal::heal_eth_prices;
pub use resync::resync_all;
pub use store::get_eth_price_by_block;

use store::get_price_h24_ago;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use tokio::time::sleep;
use tracing::{debug, info};

use crate::{
    caching::{self, CacheKey},
    db, key_value_store, log,
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

fn calc_h24_change(current_price: &EthPrice, price_h24_ago: &EthPrice) -> f64 {
    (current_price.usd - price_h24_ago.usd) / price_h24_ago.usd
}

async fn update_eth_price_with_most_recent(
    db_pool: &PgPool,
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

        store::store_price(db_pool, most_recent_price.timestamp, most_recent_price.usd).await;

        *last_price = most_recent_price;

        let price_h24_ago = get_price_h24_ago(db_pool, Duration::minutes(10))
            .await
            .expect("24h old price should be available within 10min of now - 24h");

        let eth_price_stats = EthPriceStats {
            timestamp: last_price.timestamp,
            usd: last_price.usd,
            h24_change: calc_h24_change(last_price, &price_h24_ago),
        };

        key_value_store::set_value(
            db_pool,
            CacheKey::EthPrice.to_db_key(),
            &serde_json::to_value(&eth_price_stats).unwrap(),
        )
        .await?;

        caching::publish_cache_update(db_pool, &CacheKey::EthPrice).await?;
    }

    Ok(())
}

pub async fn record_eth_price() -> Result<()> {
    log::init_with_env();

    info!("recording eth prices");

    let db_pool = db::get_db_pool("record-eth-price").await;

    let mut last_price = store::get_most_recent_price(&db_pool).await?;

    loop {
        update_eth_price_with_most_recent(&db_pool, &mut last_price).await?;
        sleep(std::time::Duration::from_secs(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;

    use super::*;

    #[tokio::test]
    async fn update_eth_price_with_most_recent_test() {
        let test_db = db::tests::TestDb::new().await;
        let db_pool = test_db.pool.clone();
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::minutes(10),
            usd: 0.0,
        };

        store::store_price(&db_pool, Utc::now() - Duration::hours(24), 0.0).await;

        let mut last_price = test_price.clone();

        update_eth_price_with_most_recent(&db_pool, &mut last_price)
            .await
            .unwrap();

        let eth_price = store::get_most_recent_price(&db_pool).await.unwrap();

        assert_ne!(eth_price, test_price);

        test_db.cleanup().await;
    }
}
