use std::fmt::Display;

use sqlx::PgExecutor;

#[derive(Debug)]
pub enum CacheKey<'a> {
    Custom(&'a str),
    EffectiveBalanceSum,
    EthPrice,
    EthSupplyParts,
    IssuanceBreakdown,
    MergeEstimate,
    SupplyProjectionInputs,
    TotalDifficultyProgress,
    ValidatorRewards,
}

impl Display for CacheKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Custom(key) => write!(f, "{key}"),
            Self::EffectiveBalanceSum => write!(f, "effective-balance-sum"),
            Self::EthPrice => write!(f, "eth-price"),
            Self::EthSupplyParts => write!(f, "eth-supply-parts"),
            Self::IssuanceBreakdown => write!(f, "issuance-breakdown"),
            Self::MergeEstimate => write!(f, "merge-estimate"),
            Self::SupplyProjectionInputs => write!(f, "supply-projection-inputs"),
            Self::TotalDifficultyProgress => write!(f, "total-difficulty-progress"),
            Self::ValidatorRewards => write!(f, "validator-rewards"),
        }
    }
}

impl<'a> CacheKey<'a> {
    pub fn to_db_key(&self) -> &'a str {
        match self {
            &Self::Custom(key) => key,
            &Self::EffectiveBalanceSum => "effective-balance-sum",
            &Self::EthPrice => "eth-price",
            &Self::EthSupplyParts => "eth-supply-parts",
            &Self::IssuanceBreakdown => "issuance-breakdown",
            &Self::MergeEstimate => "merge-estimate",
            &Self::SupplyProjectionInputs => "supply-projection-inputs",
            &Self::TotalDifficultyProgress => "total-difficulty-progress",
            &Self::ValidatorRewards => "validator-rewards",
        }
    }
}

impl<'a> From<&'a str> for CacheKey<'a> {
    fn from(key: &'a str) -> Self {
        match key {
            "effective-balance-sum" => Self::EffectiveBalanceSum,
            "eth-supply-parts" => Self::EthSupplyParts,
            "issuance-breakdown" => Self::IssuanceBreakdown,
            "merge-estimate" => Self::MergeEstimate,
            "supply-projection-inputs" => Self::SupplyProjectionInputs,
            "total-difficulty-progress" => Self::TotalDifficultyProgress,
            "validator-rewards" => Self::ValidatorRewards,
            key => Self::Custom(key),
        }
    }
}

pub async fn publish_cache_update<'a>(executor: impl PgExecutor<'a>, key: CacheKey<'_>) {
    tracing::debug!("publishing cache update: {}", key.to_string());

    sqlx::query!(
        "
            SELECT pg_notify('cache-update', $1)
        ",
        key.to_string()
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
