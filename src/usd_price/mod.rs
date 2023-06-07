mod average;
mod bybit;
mod heal;
mod resync;
mod store;

pub use heal::heal_eth_prices;
pub use resync::resync_all;

pub use store::EthPriceStore;
pub use store::EthPriceStorePostgres;

pub use average::on_new_block;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool};
use tokio::time::sleep;
use tracing::{debug, info};

use crate::key_value_store::KeyValueStore;
use crate::key_value_store::KeyValueStorePostgres;
use crate::{
    caching::{self, CacheKey},
    db, log,
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
    key_value_store: &impl KeyValueStore,
    eth_price_store: &impl EthPriceStore,
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

        eth_price_store
            .store_price(&most_recent_price.timestamp, most_recent_price.usd)
            .await;

        *last_price = most_recent_price;

        let price_h24_ago = eth_price_store
            .get_price_h24_ago(&Duration::minutes(10))
            .await
            .expect("24h old price should be available within 10min of now - 24h");

        let eth_price_stats = EthPriceStats {
            timestamp: last_price.timestamp,
            usd: last_price.usd,
            h24_change: calc_h24_change(last_price, &price_h24_ago),
        };

        key_value_store
            .set_value(
                CacheKey::EthPrice.to_db_key(),
                &serde_json::to_value(&eth_price_stats).unwrap(),
            )
            .await;

        caching::publish_cache_update(db_pool, &CacheKey::EthPrice).await;
    }

    Ok(())
}

pub async fn record_eth_price() -> Result<()> {
    log::init_with_env();

    info!("recording eth prices");

    let db_pool = db::get_db_pool("record-eth-price").await;
    let key_value_store = KeyValueStorePostgres::new(db_pool.clone());
    let eth_price_store = EthPriceStorePostgres::new(db_pool.clone());

    let mut last_price = eth_price_store.get_most_recent_price().await?;

    loop {
        update_eth_price_with_most_recent(
            &db_pool,
            &key_value_store,
            &eth_price_store,
            &mut last_price,
        )
        .await?;
        sleep(std::time::Duration::from_secs(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use test_context::test_context;

    use crate::db::tests::TestDb;

    use super::*;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn update_eth_price_with_most_recent_test(test_db: &TestDb) {
        let key_value_store = KeyValueStorePostgres::new(test_db.pool.clone());
        let eth_price_store = EthPriceStorePostgres::new(test_db.pool.clone());
        let test_price = EthPrice {
            timestamp: Utc::now().trunc_subsecs(0) - Duration::minutes(10),
            usd: 0.0,
        };

        eth_price_store
            .store_price(&(Utc::now() - Duration::hours(24)), 0.0)
            .await;

        let mut last_price = test_price.clone();

        update_eth_price_with_most_recent(
            &test_db.pool,
            &key_value_store,
            &eth_price_store,
            &mut last_price,
        )
        .await
        .unwrap();

        let eth_price = eth_price_store.get_most_recent_price().await.unwrap();

        assert_ne!(eth_price, test_price);
    }
}
