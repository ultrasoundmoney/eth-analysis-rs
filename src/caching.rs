use std::{fmt::Display, str::FromStr};

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use sqlx::PgExecutor;
use thiserror::Error;
use tracing::debug;

use crate::key_value_store;

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum CacheKey {
    BaseFeeOverTime,
    BaseFeePerGas,
    BaseFeePerGasStats,
    BlockLag,
    EffectiveBalanceSum,
    EthPrice,
    EthSupplyParts,
    IssuanceBreakdown,
    SupplyDashboard,
    SupplyOverTime,
    SupplyProjectionInputs,
    SupplySinceMerge,
    TotalDifficultyProgress,
    ValidatorRewards,
}

impl Display for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_db_key())
    }
}

impl CacheKey {
    pub fn to_db_key(&self) -> &'_ str {
        match self {
            &Self::BaseFeeOverTime => "base-fee-over-time",
            &Self::BaseFeePerGas => "current-base-fee",
            &Self::BaseFeePerGasStats => "base-fee-per-gas-stats",
            &Self::BlockLag => "block-lag",
            &Self::EffectiveBalanceSum => "effective-balance-sum",
            &Self::EthPrice => "eth-price",
            &Self::EthSupplyParts => "eth-supply-parts",
            &Self::IssuanceBreakdown => "issuance-breakdown",
            &Self::SupplyDashboard => "supply-dashboard",
            &Self::SupplyOverTime => "supply-over-time",
            &Self::SupplyProjectionInputs => "supply-projection-inputs",
            &Self::SupplySinceMerge => "supply-since-merge",
            &Self::TotalDifficultyProgress => "total-difficulty-progress",
            &Self::ValidatorRewards => "validator-rewards",
        }
    }
}

#[derive(Debug, Error)]
pub enum ParseCacheKeyError {
    #[error("failed to parse cache key {0}")]
    UnknownCacheKey(String),
}

impl<'a> FromStr for CacheKey {
    type Err = ParseCacheKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "base-fee-over-time" => Ok(Self::BaseFeeOverTime),
            "current-base-fee" => Ok(Self::BaseFeePerGas),
            "base-fee-per-gas-stats" => Ok(Self::BaseFeePerGasStats),
            "block-lag" => Ok(Self::BlockLag),
            "effective-balance-sum" => Ok(Self::EffectiveBalanceSum),
            "eth-price" => Ok(Self::EthPrice),
            "eth-supply-parts" => Ok(Self::EthSupplyParts),
            "issuance-breakdown" => Ok(Self::IssuanceBreakdown),
            "supply-over-time" => Ok(Self::SupplyOverTime),
            "supply-projection-inputs" => Ok(Self::SupplyProjectionInputs),
            "supply-since-merge" => Ok(Self::SupplySinceMerge),
            "total-difficulty-progress" => Ok(Self::TotalDifficultyProgress),
            "validator-rewards" => Ok(Self::ValidatorRewards),
            unknown_key => Err(ParseCacheKeyError::UnknownCacheKey(unknown_key.to_string())),
        }
    }
}

pub async fn publish_cache_update<'a>(executor: impl PgExecutor<'a>, key: CacheKey) -> Result<()> {
    debug!(?key, "publishing cache update");

    sqlx::query!(
        "
            SELECT pg_notify('cache-update', $1)
        ",
        key.to_db_key()
    )
    .execute(executor)
    .await?;

    Ok(())
}

pub async fn get_serialized_caching_value(
    executor: impl PgExecutor<'_>,
    cache_key: &CacheKey,
) -> sqlx::Result<Option<Value>> {
    key_value_store::get_value(executor, cache_key.to_db_key()).await
}

pub async fn set_value<'a>(
    executor: impl PgExecutor<'_>,
    cache_key: &CacheKey,
    value: impl Serialize,
) -> Result<()> {
    key_value_store::set_value(
        executor,
        cache_key.to_db_key(),
        &serde_json::to_value(value)?,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use sqlx::Acquire;

    use crate::db;

    use super::*;

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct TestJson {
        name: String,
        age: i32,
    }

    // This test fails sometimes because when run against the actual dev DB many
    // notifications fire on the "cache-update" channel. Needs a test DB to work reliably.
    #[tokio::test]
    async fn test_publish_cache_update() {
        let mut listener = sqlx::postgres::PgListener::connect(&db::get_test_db_url())
            .await
            .unwrap();
        listener.listen("cache-update").await.unwrap();

        let notification_future = async { listener.recv().await };

        let mut connection = db::get_test_db().await;

        publish_cache_update(&mut connection, CacheKey::EffectiveBalanceSum)
            .await
            .unwrap();

        let notification = notification_future.await.unwrap();

        assert_eq!(
            notification.payload(),
            CacheKey::EffectiveBalanceSum.to_db_key()
        )
    }

    #[tokio::test]
    async fn get_raw_caching_value_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        set_value(
            &mut transaction,
            &CacheKey::BaseFeePerGasStats,
            &serde_json::to_value(&test_json).unwrap(),
        )
        .await?;

        let raw_value: Value =
            get_serialized_caching_value(&mut transaction, &CacheKey::BaseFeePerGasStats)
                .await?
                .unwrap();

        assert_eq!(raw_value, serde_json::to_value(test_json).unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn get_set_caching_value_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            age: 29,
            name: "alex".to_string(),
        };

        set_value(
            &mut transaction,
            &CacheKey::BaseFeePerGasStats,
            test_json.clone(),
        )
        .await?;

        let caching_value = key_value_store::get_deserializable_value::<TestJson>(
            &mut transaction,
            &CacheKey::BaseFeePerGasStats.to_db_key(),
        )
        .await?
        .unwrap();

        assert_eq!(caching_value, test_json);

        Ok(())
    }
}
