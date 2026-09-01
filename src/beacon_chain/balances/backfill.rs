use std::{cmp::min, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use futures::{stream, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tokio::time::{sleep, timeout};
use tracing::{info, warn};

use crate::{
    beacon_chain::{
        balances,
        node::{BeaconNodeHttp, ValidatorBalance},
        BeaconNode, Slot,
    },
    units::GweiNewtype,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(2 * 60);

#[derive(Debug)]
pub enum Granularity {
    Day,
    Epoch,
    Hour,
    Slot,
}

#[derive(Debug, Clone, Copy)]
pub struct BackfillBalancesConfig {
    pub concurrency: usize,
    pub execute: bool,
    pub max_attempts: usize,
}

impl BackfillBalancesConfig {
    fn validate(self) -> Result<Self> {
        if !(1..=8).contains(&self.concurrency) {
            bail!("concurrency must be between 1 and 8");
        }
        if !(1..=10).contains(&self.max_attempts) {
            bail!("max_attempts must be between 1 and 10");
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillBalancesReport {
    pub executed: bool,
    pub planned: u64,
    pub stored: u64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct SlotRow {
    slot: i32,
    state_root: String,
}

impl SlotRow {
    fn slot(&self) -> Slot {
        Slot(self.slot)
    }
}

fn matches_granularity(slot: Slot, granularity: &Granularity) -> bool {
    match granularity {
        Granularity::Slot => true,
        Granularity::Epoch => slot.is_first_of_epoch(),
        Granularity::Hour => slot.is_first_of_hour(),
        Granularity::Day => slot.is_first_of_day(),
    }
}

async fn fetch_missing_rows(
    db_pool: &PgPool,
    granularity: &Granularity,
    start_slot_opt: Option<Slot>,
    end_slot_opt: Option<Slot>,
) -> Result<Vec<SlotRow>> {
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        r#"
        SELECT
            beacon_blocks.slot,
            beacon_blocks.state_root
        FROM
            beacon_blocks
        LEFT JOIN beacon_validators_balance ON
            beacon_blocks.state_root = beacon_validators_balance.state_root
        WHERE
            beacon_validators_balance.state_root IS NULL
        "#,
    );

    let from_slot = start_slot_opt.unwrap_or(Slot(0));
    query_builder.push(" AND beacon_blocks.slot >= ");
    query_builder.push_bind(from_slot.0);

    if let Some(end_slot) = end_slot_opt {
        query_builder.push(" AND beacon_blocks.slot <= ");
        query_builder.push_bind(end_slot.0);
    }

    query_builder.push(" ORDER BY beacon_blocks.slot ASC");

    let rows = query_builder
        .build_query_as::<SlotRow>()
        .fetch_all(db_pool)
        .await
        .context("failed to load missing canonical beacon balance rows")?;

    Ok(rows
        .into_iter()
        .filter(|row| matches_granularity(row.slot(), granularity))
        .collect())
}

async fn fetch_validator_balances_with_retry<B: BeaconNode>(
    beacon_node: &B,
    row: &SlotRow,
    max_attempts: usize,
    request_timeout: Duration,
    initial_retry_delay: Duration,
) -> Result<Vec<ValidatorBalance>> {
    let mut retry_delay = initial_retry_delay;
    let mut last_error = None;

    for attempt in 1..=max_attempts {
        let result = timeout(
            request_timeout,
            beacon_node.get_validator_balances(&row.state_root),
        )
        .await;

        match result {
            Ok(Ok(Some(validator_balances))) => return Ok(validator_balances),
            Ok(Ok(None)) => {
                last_error = Some(anyhow!(
                    "beacon node returned no validator balances for canonical state root {}",
                    row.state_root
                ));
            }
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some(anyhow!(
                    "validator balances request exceeded {} seconds",
                    request_timeout.as_secs()
                ));
            }
        }

        let error = last_error
            .as_ref()
            .expect("failed request must set an error");
        warn!(
            slot = %row.slot(),
            state_root = %row.state_root,
            attempt,
            max_attempts,
            error = %error,
            "failed to fetch historical validator balances"
        );

        if attempt < max_attempts {
            sleep(retry_delay).await;
            retry_delay = min(retry_delay.saturating_mul(2), MAX_RETRY_DELAY);
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("validator balances request was not attempted")))
        .with_context(|| {
            format!(
                "failed to fetch validator balances for slot {} after {} attempts",
                row.slot(),
                max_attempts
            )
        })
}

async fn store_balance_sum(
    db_pool: &PgPool,
    row: &SlotRow,
    balances_sum: GweiNewtype,
) -> Result<bool> {
    let gwei: i64 = balances_sum.into();
    let result = sqlx::query(
        r#"
        INSERT INTO beacon_validators_balance (timestamp, state_root, gwei)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(row.slot().date_time())
    .bind(&row.state_root)
    .bind(gwei)
    .execute(db_pool)
    .await
    .with_context(|| {
        format!(
            "failed to store validator balance sum for slot {}",
            row.slot()
        )
    })?;

    Ok(result.rows_affected() == 1)
}

enum BackfillItemOutcome {
    StoreBalances(SlotRow, GweiNewtype),
    Failed(SlotRow, anyhow::Error),
}

pub async fn estimate_balances_backfill(
    db_pool: &PgPool,
    granularity: &Granularity,
    start_slot_opt: Option<Slot>,
    end_slot_opt: Option<Slot>,
) -> Result<u64> {
    Ok(
        fetch_missing_rows(db_pool, granularity, start_slot_opt, end_slot_opt)
            .await?
            .len() as u64,
    )
}

pub async fn backfill_balances(
    db_pool: &PgPool,
    granularity: &Granularity,
    start_slot_opt: Option<Slot>,
    end_slot_opt: Option<Slot>,
    config: BackfillBalancesConfig,
) -> Result<BackfillBalancesReport> {
    let config = config.validate()?;
    let rows = fetch_missing_rows(db_pool, granularity, start_slot_opt, end_slot_opt).await?;
    let work_todo = rows.len() as u64;

    info!(
        ?start_slot_opt,
        ?end_slot_opt,
        concurrency = config.concurrency,
        max_attempts = config.max_attempts,
        execute = config.execute,
        work_todo,
        "planned beacon balances backfill"
    );

    if !config.execute || rows.is_empty() {
        return Ok(BackfillBalancesReport {
            executed: false,
            planned: work_todo,
            stored: 0,
        });
    }

    let beacon_node = BeaconNodeHttp::new_from_env();
    let tasks = stream::iter(rows).map(move |row| {
        let beacon_node = beacon_node.clone();
        async move {
            match fetch_validator_balances_with_retry(
                &beacon_node,
                &row,
                config.max_attempts,
                REQUEST_TIMEOUT,
                INITIAL_RETRY_DELAY,
            )
            .await
            {
                Ok(validator_balances) => {
                    let balances_sum = balances::sum_validator_balances(&validator_balances);
                    BackfillItemOutcome::StoreBalances(row, balances_sum)
                }
                Err(error) => BackfillItemOutcome::Failed(row, error),
            }
        }
    });

    let mut outcomes = tasks.buffer_unordered(config.concurrency);
    let mut progress = Progress::new("backfill-beacon-balances", work_todo);
    let mut stored = 0u64;

    while let Some(outcome) = outcomes.next().await {
        match outcome {
            BackfillItemOutcome::StoreBalances(row, balances_sum) => {
                if store_balance_sum(db_pool, &row, balances_sum).await? {
                    stored += 1;
                    info!(slot = %row.slot(), state_root = %row.state_root, "stored validator balance sum");
                } else {
                    info!(slot = %row.slot(), state_root = %row.state_root, "validator balance sum was already stored");
                }
            }
            BackfillItemOutcome::Failed(row, error) => {
                warn!(slot = %row.slot(), state_root = %row.state_root, error = %error, "leaving validator balance row missing");
            }
        }

        progress.inc_work_done();
        if progress.work_done.is_multiple_of(25) || progress.work_done >= work_todo {
            info!("backfill progress: {}", progress.get_progress_string());
        }
    }

    let remaining = fetch_missing_rows(db_pool, granularity, start_slot_opt, end_slot_opt).await?;
    if !remaining.is_empty() {
        let sample = remaining
            .iter()
            .take(20)
            .map(|row| row.slot.to_string())
            .collect::<Vec<_>>()
            .join(",");
        bail!(
            "beacon balances backfill incomplete: {} canonical slots remain missing (first slots: {})",
            remaining.len(),
            sample
        );
    }

    info!(
        planned = work_todo,
        stored, "beacon balances backfill completed"
    );
    Ok(BackfillBalancesReport {
        executed: true,
        planned: work_todo,
        stored,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::beacon_chain::node::MockBeaconNode;

    #[tokio::test]
    async fn retry_exhaustion_returns_error_without_balance_data() {
        let mut beacon_node = MockBeaconNode::new();
        beacon_node
            .expect_get_validator_balances()
            .times(2)
            .returning(|_| Err(anyhow!("temporary failure")));
        let row = SlotRow {
            slot: 8_208_482,
            state_root: "0xstate-root".to_string(),
        };

        let result = fetch_validator_balances_with_retry(
            &beacon_node,
            &row,
            2,
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_uses_canonical_state_root() {
        let mut beacon_node = MockBeaconNode::new();
        beacon_node
            .expect_get_validator_balances()
            .withf(|state_root| state_root == "0xcanonical")
            .times(1)
            .returning(|_| {
                Ok(Some(vec![ValidatorBalance {
                    balance: GweiNewtype(42),
                }]))
            });
        let row = SlotRow {
            slot: 8_208_482,
            state_root: "0xcanonical".to_string(),
        };

        let balances = fetch_validator_balances_with_retry(
            &beacon_node,
            &row,
            1,
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .await
        .unwrap();

        assert_eq!(balances::sum_validator_balances(&balances), GweiNewtype(42));
    }
}
