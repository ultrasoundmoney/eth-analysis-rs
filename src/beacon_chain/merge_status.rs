use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Acquire, PgConnection};
use tracing::error;

use crate::{
    caching::{self, CacheKey},
    eth_supply,
    eth_units::EthF64,
    execution_chain::{
        BlockNumber, ExecutionNodeBlock, TOTAL_TERMINAL_DIFFICULTY,
    },
    key_value_store,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MergeStats {
    timestamp: DateTime<Utc>,
    supply: f64,
    block_number: BlockNumber,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status")]
pub enum MergeStatus {
    #[serde(rename = "merged")]
    Merged(MergeStats),
    #[serde(rename = "pending")]
    Pending,
}

pub async fn update_merge_stats_by_hand<'a>(
    executor: &mut PgConnection,
    timestamp: DateTime<Utc>,
    supply: EthF64,
    block_number: BlockNumber,
) -> Result<()> {
    let merge_status = MergeStatus::Merged(MergeStats {
        timestamp,
        supply,
        block_number,
    });

    key_value_store::set_caching_value(
        executor.acquire().await.unwrap(),
        &CacheKey::MergeStatus,
        merge_status,
    )
    .await?;

    caching::publish_cache_update(executor, CacheKey::MergeStatus).await;

    Ok(())
}

pub async fn update_merge_stats_auto(
    executor: &mut PgConnection,
    block: &ExecutionNodeBlock,
) -> Result<()> {
    let merge_status =
        key_value_store::get_caching_value::<MergeStatus>(executor, &CacheKey::MergeStatus).await?;
    match merge_status {
        None => {
            error!("no merge status in DB, something is wrong, skipping auto detection");
        }
        Some(MergeStatus::Merged(_)) => (),
        Some(MergeStatus::Pending) => {
            // Did we merge?
            if block.total_difficulty >= TOTAL_TERMINAL_DIFFICULTY {
                // Yes! 🎊 Set merge state.
                let execution_supply = eth_supply::get_current_supply(executor).await?;
                let merge_stats = MergeStats {
                    timestamp: block.timestamp,
                    supply: execution_supply.supply,
                    block_number: block.number,
                };
                let merge_status = MergeStatus::Merged(merge_stats);

                key_value_store::set_caching_value(
                    executor.acquire().await.unwrap(),
                    &CacheKey::MergeStatus,
                    merge_status,
                )
                .await?;

                caching::publish_cache_update(executor, CacheKey::MergeStatus).await;
            }
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db_testing;

    use super::*;

    #[tokio::test]
    async fn update_merge_stats_by_hand_test() -> Result<()> {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let merge_stats = MergeStats {
            block_number: 0,
            supply: 10.0,
            timestamp: Utc::now(),
        };

        update_merge_stats_by_hand(
            &mut transaction,
            merge_stats.timestamp.clone(),
            merge_stats.supply.clone(),
            merge_stats.block_number.clone(),
        )
        .await?;

        let merge_status = key_value_store::get_caching_value::<MergeStatus>(
            &mut transaction,
            &CacheKey::MergeStatus,
        )
        .await?;

        assert_eq!(merge_status, Some(MergeStatus::Merged(merge_stats)));

        Ok(())
    }

    #[tokio::test]
    async fn overwrite_pending_test() -> Result<()> {
        let mut connection = db_testing::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        key_value_store::set_caching_value(
            &mut transaction,
            &CacheKey::MergeStatus,
            MergeStatus::Pending,
        )
        .await?;

        let test_merge_status_before = key_value_store::get_caching_value::<MergeStatus>(
            &mut transaction,
            &CacheKey::MergeStatus,
        )
        .await?;
        assert_eq!(test_merge_status_before, Some(MergeStatus::Pending));

        let merge_stats = MergeStats {
            block_number: 0,
            supply: 10.0,
            timestamp: Utc::now(),
        };

        update_merge_stats_by_hand(
            &mut transaction,
            merge_stats.timestamp.clone(),
            merge_stats.supply.clone(),
            merge_stats.block_number.clone(),
        )
        .await?;
        let test_merge_status_after =
            key_value_store::get_caching_value(&mut transaction, &CacheKey::MergeStatus).await?;

        assert_eq!(
            test_merge_status_after,
            Some(MergeStatus::Merged(merge_stats))
        );

        Ok(())
    }
}
