use std::{fmt::Display, str::FromStr};

use anyhow::Result;
use enum_iterator::Sequence;
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgExecutor, PgPool};
use thiserror::Error;
use tracing::debug;

use crate::{
    key_value_store::{self, KeyValueStore},
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Sequence)]
pub enum CacheKey {
    AverageEthPrice,
    BaseFeeOverTime,
    BaseFeePerGas,
    BaseFeePerGasBarrier,
    BaseFeePerGasStats,
    BaseFeePerGasStatsTimeFrame(TimeFrame),
    BlobFeePerGasStats,
    BlobFeePerGasStatsTimeFrame(TimeFrame),
    BlockLag,
    BurnRates,
    BurnSums,
    EffectiveBalanceSum,
    EthPrice,
    GaugeRates,
    SupplyParts,
    IssuanceBreakdown,
    IssuanceEstimate,
    SupplyChanges,
    SupplyDashboardAnalysis,
    SupplyOverTime,
    SupplyProjectionInputs,
    SupplySinceMerge,
    TotalDifficultyProgress,
    ValidatorRewards,
}

impl CacheKey {
    pub fn to_db_key(self) -> &'static str {
        use CacheKey::*;
        use GrowingTimeFrame::*;
        use LimitedTimeFrame::*;
        use TimeFrame::*;

        match self {
            AverageEthPrice => "average-eth-price",
            BaseFeeOverTime => "base-fee-over-time",
            BaseFeePerGas => "current-base-fee",
            BaseFeePerGasBarrier => "base-fee-per-gas-barrier",
            BaseFeePerGasStats => "base-fee-per-gas-stats",
            BaseFeePerGasStatsTimeFrame(time_frame) => match time_frame {
                Growing(SinceBurn) => "base-fee-per-gas-stats-since_burn",
                Growing(SinceMerge) => "base-fee-per-gas-stats-since_merge",
                Limited(Minute5) => "base-fee-per-gas-stats-m5",
                Limited(Hour1) => "base-fee-per-gas-stats-h1",
                Limited(Day1) => "base-fee-per-gas-stats-d1",
                Limited(Day7) => "base-fee-per-gas-stats-d7",
                Limited(Day30) => "base-fee-per-gas-stats-d30",
            },
            BlobFeePerGasStats => "blob-fee-per-gas-stats",
            BlobFeePerGasStatsTimeFrame(time_frame) => match time_frame {
                Growing(SinceBurn) => "blob-fee-per-gas-stats-since_burn",
                Growing(SinceMerge) => "blob-fee-per-gas-stats-since_merge",
                Limited(Minute5) => "blob-fee-per-gas-stats-m5",
                Limited(Hour1) => "blob-fee-per-gas-stats-h1",
                Limited(Day1) => "blob-fee-per-gas-stats-d1",
                Limited(Day7) => "blob-fee-per-gas-stats-d7",
                Limited(Day30) => "blob-fee-per-gas-stats-d30",
            },
            BlockLag => "block-lag",
            BurnRates => "burn-rates",
            BurnSums => "burn-sums",
            EffectiveBalanceSum => "effective-balance-sum",
            EthPrice => "eth-price",
            GaugeRates => "gauge-rates",
            IssuanceBreakdown => "issuance-breakdown",
            IssuanceEstimate => "issuance-estimate",
            SupplyChanges => "supply-changes",
            SupplyDashboardAnalysis => "supply-dashboard-analysis",
            SupplyOverTime => "supply-over-time",
            SupplyParts => "supply-parts",
            SupplyProjectionInputs => "supply-projection-inputs",
            SupplySinceMerge => "supply-since-merge",
            TotalDifficultyProgress => "total-difficulty-progress",
            ValidatorRewards => "validator-rewards",
        }
    }
}

impl Display for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_db_key())
    }
}

#[derive(Debug, Error)]
pub enum ParseCacheKeyError {
    #[error("failed to parse cache key {0}")]
    UnknownCacheKey(String),
}

impl FromStr for CacheKey {
    type Err = ParseCacheKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "average-eth-price" => Ok(Self::AverageEthPrice),
            "base-fee-over-time" => Ok(Self::BaseFeeOverTime),
            "current-base-fee" => Ok(Self::BaseFeePerGas),
            "base-fee-per-gas" => Ok(Self::BaseFeePerGas),
            "base-fee-per-gas-barrier" => Ok(Self::BaseFeePerGasBarrier),
            "base-fee-per-gas-stats" => Ok(Self::BaseFeePerGasStats),
            "block-lag" => Ok(Self::BlockLag),
            "burn-rates" => Ok(Self::BurnRates),
            "burn-sums" => Ok(Self::BurnSums),
            "effective-balance-sum" => Ok(Self::EffectiveBalanceSum),
            "eth-price" => Ok(Self::EthPrice),
            "gauge-rates" => Ok(Self::GaugeRates),
            "issuance-breakdown" => Ok(Self::IssuanceBreakdown),
            "issuance-estimate" => Ok(Self::IssuanceEstimate),
            "supply-changes" => Ok(Self::SupplyChanges),
            "supply-dashboard-analysis" => Ok(Self::SupplyDashboardAnalysis),
            "supply-over-time" => Ok(Self::SupplyOverTime),
            "supply-parts" => Ok(Self::SupplyParts),
            "supply-projection-inputs" => Ok(Self::SupplyProjectionInputs),
            "supply-since-merge" => Ok(Self::SupplySinceMerge),
            "total-difficulty-progress" => Ok(Self::TotalDifficultyProgress),
            "validator-rewards" => Ok(Self::ValidatorRewards),
            unknown_key if unknown_key.starts_with("base-fee-per-gas-stats-") => unknown_key
                .split('-')
                .nth(5)
                .expect(
                    "expect keys which start with 'base-fee-per-gas-stats-' to have a time frame",
                )
                .to_string()
                .parse::<TimeFrame>()
                .map_or(
                    Err(ParseCacheKeyError::UnknownCacheKey(unknown_key.to_string())),
                    |key| Ok(Self::BaseFeePerGasStatsTimeFrame(key)),
                ),
            unknown_key => Err(ParseCacheKeyError::UnknownCacheKey(unknown_key.to_string())),
        }
    }
}

pub async fn publish_cache_update<'a>(executor: impl PgExecutor<'a>, key: &CacheKey) {
    debug!(?key, "publishing cache update");

    sqlx::query!(
        "
            SELECT pg_notify('cache-update', $1)
        ",
        key.to_db_key()
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn get_serialized_caching_value(
    key_value_store: &impl KeyValueStore,
    cache_key: &CacheKey,
) -> Option<Value> {
    key_value_store.get_value(cache_key.to_db_key()).await
}

pub async fn set_value<'a>(
    executor: impl PgExecutor<'_>,
    cache_key: &CacheKey,
    value: impl Serialize,
) {
    key_value_store::set_value(
        executor,
        cache_key.to_db_key(),
        &serde_json::to_value(value).expect("expect value to be serializable"),
    )
    .await;
}

pub async fn update_and_publish(db_pool: &PgPool, cache_key: &CacheKey, value: impl Serialize) {
    set_value(db_pool, cache_key, value).await;
    publish_cache_update(db_pool, cache_key).await;
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::{db, env::ENV_CONFIG, key_value_store::KeyValueStorePostgres};

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
        let mut listener = sqlx::postgres::PgListener::connect(ENV_CONFIG.db_url.as_str())
            .await
            .unwrap();
        listener.listen("cache-update").await.unwrap();

        let notification_future = async { listener.recv().await };

        let mut connection = db::tests::get_test_db_connection().await;

        publish_cache_update(&mut connection, &CacheKey::EffectiveBalanceSum).await;

        let notification = notification_future.await.unwrap();

        assert_eq!(
            notification.payload(),
            CacheKey::EffectiveBalanceSum.to_db_key()
        )
    }

    #[tokio::test]
    async fn get_serialized_caching_value_test() {
        let test_db = db::tests::TestDb::new().await;
        let key_value_store = KeyValueStorePostgres::new(test_db.pool.clone());

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        key_value_store
            .set_serializable_value(CacheKey::BaseFeePerGasStats.to_db_key(), &test_json)
            .await;

        let raw_value: Value =
            get_serialized_caching_value(&key_value_store, &CacheKey::BaseFeePerGasStats)
                .await
                .unwrap();

        assert_eq!(raw_value, serde_json::to_value(test_json).unwrap());
    }

    #[tokio::test]
    async fn get_set_caching_value_test() -> Result<()> {
        let test_db = db::tests::TestDb::new().await;
        let key_value_store = KeyValueStorePostgres::new(test_db.pool.clone());

        let test_json = TestJson {
            age: 29,
            name: "alex".to_string(),
        };

        set_value(
            &test_db.pool,
            &CacheKey::BaseFeePerGasStats,
            test_json.clone(),
        )
        .await;

        let caching_value = key_value_store
            .get_deserializable_value::<TestJson>(CacheKey::BaseFeePerGasStats.to_db_key())
            .await
            .unwrap();

        assert_eq!(caching_value, test_json);

        Ok(())
    }

    #[test]
    fn parse_base_fees_time_frame_test() {
        assert_eq!(
            "base-fee-per-gas-stats-d1".parse::<CacheKey>().unwrap(),
            CacheKey::BaseFeePerGasStatsTimeFrame(TimeFrame::Limited(LimitedTimeFrame::Day1))
        );
        assert_eq!(
            "base-fee-per-gas-stats-since_merge"
                .parse::<CacheKey>()
                .unwrap(),
            CacheKey::BaseFeePerGasStatsTimeFrame(TimeFrame::Growing(GrowingTimeFrame::SinceMerge))
        );
    }
}
