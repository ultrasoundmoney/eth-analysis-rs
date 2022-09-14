use std::fmt::Display;

use sqlx::PgExecutor;

#[derive(Debug)]
pub enum CacheKey<'a> {
    BaseFeeOverTime,
    BaseFeePerGas,
    BaseFeePerGasStats,
    BlockLag,
    Custom(&'a str),
    EffectiveBalanceSum,
    EthPrice,
    EthSupplyParts,
    IssuanceBreakdown,
    MergeEstimate,
    MergeStatus,
    SupplyProjectionInputs,
    SupplySinceMerge,
    TotalDifficultyProgress,
    ValidatorRewards,
}

impl Display for CacheKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BaseFeeOverTime => write!(f, "base-fee-over-time"),
            Self::BaseFeePerGas => write!(f, "current-base-fee"),
            Self::BaseFeePerGasStats => write!(f, "base-fee-per-gas-stats"),
            Self::BlockLag => write!(f, "block-lag"),
            Self::Custom(key) => write!(f, "{key}"),
            Self::EffectiveBalanceSum => write!(f, "effective-balance-sum"),
            Self::EthPrice => write!(f, "eth-price"),
            Self::EthSupplyParts => write!(f, "eth-supply-parts"),
            Self::IssuanceBreakdown => write!(f, "issuance-breakdown"),
            Self::MergeEstimate => write!(f, "merge-estimate"),
            Self::MergeStatus => write!(f, "merge-status"),
            Self::SupplyProjectionInputs => write!(f, "supply-projection-inputs"),
            Self::SupplySinceMerge => write!(f, "supply-since-merge"),
            Self::TotalDifficultyProgress => write!(f, "total-difficulty-progress"),
            Self::ValidatorRewards => write!(f, "validator-rewards"),
        }
    }
}

impl<'a> CacheKey<'a> {
    pub fn to_db_key(&self) -> &'a str {
        match self {
            &Self::BaseFeeOverTime => "base-fee-over-time",
            &Self::BaseFeePerGas => "current-base-fee",
            &Self::BaseFeePerGasStats => "base-fee-per-gas-stats",
            &Self::BlockLag => "block-lag",
            &Self::Custom(key) => key,
            &Self::EffectiveBalanceSum => "effective-balance-sum",
            &Self::EthPrice => "eth-price",
            &Self::EthSupplyParts => "eth-supply-parts",
            &Self::IssuanceBreakdown => "issuance-breakdown",
            &Self::MergeEstimate => "merge-estimate",
            &Self::MergeStatus => "merge-status",
            &Self::SupplyProjectionInputs => "supply-projection-inputs",
            &Self::SupplySinceMerge => "supply-since-merge",
            &Self::TotalDifficultyProgress => "total-difficulty-progress",
            &Self::ValidatorRewards => "validator-rewards",
        }
    }
}

impl<'a> From<&'a str> for CacheKey<'a> {
    fn from(key: &'a str) -> Self {
        match key {
            "base-fee-over-time" => Self::BaseFeeOverTime,
            "current-base-fee" => Self::BaseFeePerGas,
            "base-fee-per-gas-stats" => Self::BaseFeePerGasStats,
            "block-lag" => Self::BlockLag,
            "effective-balance-sum" => Self::EffectiveBalanceSum,
            "eth-price" => Self::EthPrice,
            "eth-supply-parts" => Self::EthSupplyParts,
            "issuance-breakdown" => Self::IssuanceBreakdown,
            "merge-estimate" => Self::MergeEstimate,
            "merge-status" => Self::MergeStatus,
            "supply-projection-inputs" => Self::SupplyProjectionInputs,
            "supply-since-merge" => Self::SupplySinceMerge,
            "total-difficulty-progress" => Self::TotalDifficultyProgress,
            "validator-rewards" => Self::ValidatorRewards,
            key => Self::Custom(key),
        }
    }
}

pub async fn publish_cache_update<'a>(executor: impl PgExecutor<'a>, key: CacheKey<'_>) {
    tracing::debug!("publishing cache update: {}", key.to_db_key());

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;

    // This test fails sometimes because when run against the actual dev DB many
    // notifications fire on the "cache-update" channel. Needs a test DB to work reliably.
    #[tokio::test]
    async fn test_publish_cache_update() {
        let mut listener = sqlx::postgres::PgListener::connect(&config::get_db_url())
            .await
            .unwrap();
        listener.listen("cache-update").await.unwrap();

        let notification_future = async { listener.recv().await };

        let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();

        let test_key = "test-key";
        publish_cache_update(&pool, CacheKey::Custom("test-key")).await;

        let notification = notification_future.await.unwrap();

        assert_eq!(notification.payload(), test_key)
    }
}
