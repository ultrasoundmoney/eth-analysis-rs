//! Tracks issuance on the beacon chain.
//!
//! As an experiment this module does not return Result wherever callers don't need it but simply
//! unwraps. If we don't want to recover from failure anyway, what is the point in communicating it
//! upwards.
//! As an experiment this module exposes a Store trait and a Postgres implementation of it. Using
//! this interface with transactions is an unsolved problem.
use std::ops::Sub;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::{PgExecutor, PgPool};
use tracing::info;

use crate::{
    burn_sums::EthUsdAmount,
    caching::{self, CacheKey},
    db,
    execution_chain::{ExecutionNodeBlock, LONDON_HARD_FORK_TIMESTAMP, MERGE_HARD_FORK_TIMESTAMP},
    time_frames::{GrowingTimeFrame, TimeFrame},
    units::{EthNewtype, GweiNewtype, UsdNewtype},
    usd_price,
};

use super::Slot;

pub async fn store_issuance(
    executor: impl PgExecutor<'_>,
    state_root: &str,
    slot: &Slot,
    gwei: &GweiNewtype,
) {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "INSERT INTO beacon_issuance (timestamp, state_root, gwei) VALUES ($1, $2, $3)",
        slot.date_time(),
        state_root,
        gwei,
    )
    .execute(executor)
    .await
    .unwrap();
}

pub fn calc_issuance(
    validator_balances_sum_gwei: &GweiNewtype,
    deposit_sum_aggregated: &GweiNewtype,
) -> GweiNewtype {
    *validator_balances_sum_gwei - *deposit_sum_aggregated
}

pub async fn get_current_issuance(executor: impl PgExecutor<'_>) -> GweiNewtype {
    sqlx::query!(
        "
        SELECT
            gwei
        FROM
            beacon_issuance
        ORDER BY
            timestamp DESC
        LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .map(|row| GweiNewtype(row.gwei))
    .unwrap()
}

pub async fn delete_issuances(connection: impl PgExecutor<'_>, greater_than_or_equal: &Slot) {
    sqlx::query!(
        "
        DELETE FROM beacon_issuance
        WHERE state_root IN (
            SELECT state_root FROM beacon_states
            WHERE slot >= $1
        )
        ",
        greater_than_or_equal.0
    )
    .execute(connection)
    .await
    .unwrap();
}

pub async fn delete_issuance(connection: impl PgExecutor<'_>, slot: &Slot) {
    sqlx::query!(
        "
        DELETE FROM beacon_issuance
        WHERE state_root IN (
            SELECT state_root FROM beacon_states
            WHERE slot = $1
        )
        ",
        slot.0
    )
    .execute(connection)
    .await
    .unwrap();
}

pub async fn get_day7_ago_issuance(executor: impl PgExecutor<'_>) -> GweiNewtype {
    sqlx::query!(
        "
        WITH
          issuance_distances AS (
            SELECT
              gwei,
              timestamp,
              ABS(
                EXTRACT(
                  epoch
                  FROM
                    (timestamp - (NOW() - '7 days':: INTERVAL))
                )
              ) AS distance_seconds
            FROM
              beacon_issuance
            ORDER BY
              distance_seconds ASC
          )
        SELECT gwei
        FROM issuance_distances 
        WHERE distance_seconds <= 86400
        LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .map(|row| GweiNewtype(row.gwei))
    .unwrap()
}

#[async_trait]
pub trait IssuanceStore {
    async fn current_issuance(&self) -> GweiNewtype;
    async fn day7_ago_issuance(&self) -> GweiNewtype;
}

pub struct IssuanceStorePostgres<'a> {
    db_pool: &'a PgPool,
}

impl<'a> IssuanceStorePostgres<'a> {
    pub fn new(db_pool: &'a PgPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl IssuanceStore for &IssuanceStorePostgres<'_> {
    async fn current_issuance(&self) -> GweiNewtype {
        get_current_issuance(self.db_pool).await
    }

    async fn day7_ago_issuance(&self) -> GweiNewtype {
        get_day7_ago_issuance(self.db_pool).await
    }
}

pub async fn get_last_week_issuance(issuance_store: impl IssuanceStore) -> GweiNewtype {
    let (current_issuance, day7_ago_issuance) = join!(
        issuance_store.current_issuance(),
        issuance_store.day7_ago_issuance()
    );
    current_issuance - day7_ago_issuance
}

const SLOTS_PER_MINUTE: u64 = 5;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;
const DAYS_PER_WEEK: u64 = 7;
const SLOTS_PER_WEEK: f64 =
    (SLOTS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY * DAYS_PER_WEEK) as f64;

#[derive(Debug, Serialize)]
struct IssuanceEstimate {
    slot: Slot,
    timestamp: DateTime<Utc>,
    issuance_per_slot_gwei: f64,
}

async fn get_issuance_per_slot_estimate(issuance_store: impl IssuanceStore) -> f64 {
    let last_week_issuance = get_last_week_issuance(issuance_store).await;
    last_week_issuance.0 as f64 / SLOTS_PER_WEEK
}

pub async fn update_issuance_estimate() {
    info!("updating issuance estimate");

    let db_pool = db::get_db_pool("update-issuance-estimate").await;
    let issuance_store = IssuanceStorePostgres::new(&db_pool);

    let issuance_per_slot_gwei = get_issuance_per_slot_estimate(&issuance_store).await;
    let slot = super::get_last_state(&db_pool)
        .await
        .expect("expect last state to exist in order to update issuance estimate")
        .slot;

    let timestamp = slot.date_time();
    let issuance_estimate = IssuanceEstimate {
        slot,
        timestamp,
        issuance_per_slot_gwei,
    };

    caching::update_and_publish(&db_pool, &CacheKey::IssuanceEstimate, issuance_estimate)
        .await
        .unwrap();

    info!("updated issuance estimate");
}

pub async fn estimated_issuance_from_time_frame(
    db_pool: &PgPool,
    time_frame: &TimeFrame,
    block: &ExecutionNodeBlock,
) -> EthUsdAmount {
    use GrowingTimeFrame::*;
    use TimeFrame::*;

    match time_frame {
        Growing(SinceBurn) => {
            // Since there is no issuance before the Merge we don't have data for it. We estimate.
            let days_since_merge = Utc::now().sub(*MERGE_HARD_FORK_TIMESTAMP).num_days();
            let days_since_burn = Utc::now().sub(*LONDON_HARD_FORK_TIMESTAMP).num_days();
            let extrapolation_factor = days_since_burn as f64 / days_since_merge as f64;

            let usd_price_average = usd_price::average_from_time_range(
                db_pool,
                time_frame.start_timestamp(block),
                block.timestamp,
            )
            .await;

            let row = sqlx::query!(
                r#"
                SELECT
                    SUM(gwei)::TEXT AS "gwei!",
                    SUM(gwei::FLOAT8 / 1e9 * $3) AS "usd!"
                FROM beacon_issuance
                WHERE timestamp >= $1
                AND timestamp <= $2
                "#,
                *MERGE_HARD_FORK_TIMESTAMP,
                block.timestamp,
                usd_price_average.0
            )
            .fetch_one(db_pool)
            .await
            .unwrap();

            let eth: EthNewtype = row.gwei.parse::<GweiNewtype>().unwrap().into();
            let usd = row.usd;
            let eth_estimated = EthNewtype(eth.0 as f64 * extrapolation_factor);
            let usd_estimated = UsdNewtype(row.usd * extrapolation_factor);

            EthUsdAmount {
                eth: eth_estimated,
                usd: usd_estimated,
            }
        }
        Growing(SinceMerge) => {
            let usd_price = 1000.0;

            let row = sqlx::query!(
                r#"
                SELECT
                    SUM(gwei)::TEXT AS "gwei!",
                    SUM(gwei::FLOAT8 / 1e9 * $3) AS "usd!"
                FROM beacon_issuance
                WHERE timestamp >= $1
                AND timestamp <= $2
                "#,
                *MERGE_HARD_FORK_TIMESTAMP,
                block.timestamp,
                usd_price
            )
            .fetch_one(db_pool)
            .await
            .unwrap();

            let eth: EthNewtype = row.gwei.parse::<GweiNewtype>().unwrap().into();
            let usd = UsdNewtype(row.usd);

            EthUsdAmount { eth, usd }
        }
        Limited(limited_time_frame) => {
            let usd_price = 1000.0;

            let row = sqlx::query!(
                r#"
                SELECT
                    SUM(gwei)::TEXT AS "gwei!",
                    SUM(gwei::FLOAT8 / 1e9 * $3) AS "usd!"
                FROM beacon_issuance
                WHERE timestamp >= NOW() - $1::INTERVAL
                AND timestamp <= $2
                "#,
                limited_time_frame.postgres_interval(),
                block.timestamp,
                usd_price
            )
            .fetch_one(db_pool)
            .await
            .unwrap();

            let eth: EthNewtype = row.gwei.parse::<GweiNewtype>().unwrap().into();
            let usd = UsdNewtype(row.usd);

            EthUsdAmount { eth, usd }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound, TimeZone, Utc};
    use sqlx::Acquire;

    use super::*;
    use crate::{
        beacon_chain::states::store_state, db, execution_chain::ExecutionNodeBlockBuilder,
        supply_projection::GweiInTime,
    };

    pub async fn get_issuance_by_start_of_day(pool: impl PgExecutor<'_>) -> Vec<GweiInTime> {
        sqlx::query!(
            r#"
            SELECT
                DISTINCT ON (DATE_TRUNC('day', timestamp)) DATE_TRUNC('day', timestamp) AS "day_timestamp!",
                gwei
            FROM
                beacon_issuance
            ORDER BY
                DATE_TRUNC('day', timestamp), timestamp ASC
            "#
            )
            .fetch_all(pool)
            .await
            .map(|rows|  {
                rows.iter()
                    .map(|row| {
                        GweiInTime {
                            t: row.day_timestamp.duration_trunc(Duration::days(1) ).unwrap().timestamp() as u64,
                            v: row.gwei
                        }
                    })
                .collect()
            })
        .unwrap()
    }

    #[test]
    fn calc_issuance_test() {
        let validator_balances_sum_gwei = GweiNewtype(100);
        let deposit_sum_aggregated = GweiNewtype(50);

        assert_eq!(
            calc_issuance(&validator_balances_sum_gwei, &deposit_sum_aggregated),
            GweiNewtype(50)
        )
    }

    #[tokio::test]
    async fn timestamp_is_start_of_day_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "timestamp_is_start_of_day";

        store_state(&mut transaction, test_id, &Slot(3599)).await;

        store_issuance(&mut transaction, test_id, &Slot(3599), &GweiNewtype(100)).await;

        let validator_balances_by_day = get_issuance_by_start_of_day(&mut transaction).await;

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc
            .timestamp_opt(unix_timestamp.try_into().unwrap(), 0)
            .unwrap();

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        assert_eq!(datetime, start_of_day_datetime);
    }

    #[tokio::test]
    async fn get_current_issuance_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_issuance_1", &Slot(3599)).await;

        store_state(&mut transaction, "0xtest_issuance_2", &Slot(10799)).await;

        store_issuance(
            &mut transaction,
            "0xtest_issuance_1",
            &Slot(3599),
            &GweiNewtype(100),
        )
        .await;

        store_issuance(
            &mut transaction,
            "0xtest_issuance_2",
            &Slot(10799),
            &GweiNewtype(110),
        )
        .await;

        let current_issuance = get_current_issuance(&mut transaction).await;

        assert_eq!(current_issuance, GweiNewtype(110));
    }

    #[tokio::test]
    async fn delete_issuance_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_issuance", &Slot(3599)).await;

        store_issuance(
            &mut transaction,
            "0xtest_issuance",
            &Slot(3599),
            &GweiNewtype(100),
        )
        .await;

        let issuance_by_day = get_issuance_by_start_of_day(&mut transaction).await;

        assert_eq!(issuance_by_day.len(), 1);

        delete_issuances(&mut transaction, &Slot(3599)).await;

        let issuance_by_day_after = get_issuance_by_start_of_day(&mut transaction).await;

        assert_eq!(issuance_by_day_after.len(), 0);
    }

    #[tokio::test]
    async fn get_day7_ago_issuance_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let now_min_seven_days_slot =
            Slot::from_date_time_rounded_down(&(Utc::now() - Duration::days(7)));
        let now_slot = Slot::from_date_time_rounded_down(&Utc::now());

        store_state(
            &mut transaction,
            "0xtest_issuance_1",
            &now_min_seven_days_slot,
        )
        .await;

        store_state(&mut transaction, "0xtest_issuance_2", &now_slot).await;

        store_issuance(
            &mut transaction,
            "0xtest_issuance_1",
            &now_min_seven_days_slot,
            &GweiNewtype(100),
        )
        .await;

        store_issuance(
            &mut transaction,
            "0xtest_issuance_2",
            &now_slot,
            &GweiNewtype(110),
        )
        .await;

        let day7_ago_issuance = get_day7_ago_issuance(&mut transaction).await;

        assert_eq!(day7_ago_issuance, GweiNewtype(100));
    }

    #[tokio::test]
    async fn get_last_week_issuance_test() {
        struct IssuanceStoreTest {}

        #[async_trait]
        impl IssuanceStore for IssuanceStoreTest {
            async fn current_issuance(&self) -> GweiNewtype {
                GweiNewtype(100)
            }

            async fn day7_ago_issuance(&self) -> GweiNewtype {
                GweiNewtype(50)
            }
        }

        let issuance_store = IssuanceStoreTest {};

        let issuance = get_last_week_issuance(issuance_store).await;

        assert_eq!(issuance, GweiNewtype(50));
    }

    #[tokio::test]
    async fn estimated_issuance_from_time_frame_test() {
        let test_db = db::tests::TestDb::new().await;
        let db_pool = test_db.pool();

        let test_id = "estimated_issuance_from_time_frame";

        let block = ExecutionNodeBlockBuilder::new(test_id).build();

        store_state(db_pool, test_id, &Slot(3599)).await;

        store_issuance(db_pool, test_id, &Slot(3599), &GweiNewtype(100)).await;

        let issuance = estimated_issuance_from_time_frame(
            db_pool,
            &TimeFrame::Growing(GrowingTimeFrame::SinceMerge),
            &block,
        )
        .await;

        assert_eq!(
            issuance,
            EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0)
            }
        );
    }
}
