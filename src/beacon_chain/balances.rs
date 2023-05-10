mod backfill;

use chrono::{Duration, DurationRound};
use serde::{Deserialize, Serialize};
use sqlx::PgExecutor;

use crate::supply_projection::GweiInTime;
use crate::units::GweiNewtype;

use super::node::{BeaconNode, ValidatorBalance};
use super::{get_last_state, Slot};

pub use backfill::backfill_balances_to_london;
pub use backfill::backfill_daily_balances_to_london;

pub fn sum_validator_balances(validator_balances: &[ValidatorBalance]) -> GweiNewtype {
    validator_balances
        .iter()
        .fold(GweiNewtype(0), |sum, validator_balance| {
            sum + validator_balance.balance
        })
}

pub async fn store_validators_balance(
    pool: impl PgExecutor<'_>,
    state_root: &str,
    slot: &Slot,
    gwei: &GweiNewtype,
) {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "
        INSERT INTO beacon_validators_balance (timestamp, state_root, gwei) VALUES ($1, $2, $3)
        ",
        slot.date_time(),
        state_root,
        gwei,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub async fn get_last_effective_balance_sum(
    executor: impl PgExecutor<'_>,
    beacon_node: &BeaconNode,
) -> GweiNewtype {
    let last_state_root = get_last_state(executor)
        .await
        .expect("can't calculate a last effective balance with an empty beacon_states table")
        .state_root;

    beacon_node
        .get_validators_by_state(&last_state_root)
        .await
        .map(|validators| {
            validators.iter().fold(GweiNewtype(0), |sum, validator| {
                sum + validator.effective_balance()
            })
        })
        .unwrap()
}

pub async fn get_validator_balances_by_start_of_day(
    executor: impl PgExecutor<'_>,
) -> Vec<GweiInTime> {
    sqlx::query!(
        r#"
        SELECT
            DISTINCT ON (DATE_TRUNC('day', timestamp)) DATE_TRUNC('day', timestamp) AS "day_timestamp!",
            gwei
        FROM
            beacon_validators_balance
        ORDER BY
            DATE_TRUNC('day', timestamp), timestamp ASC
        "#
    )
    .fetch_all(executor)
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
    }).unwrap()
}

pub async fn delete_validator_sums(executor: impl PgExecutor<'_>, greater_than_or_equal: &Slot) {
    sqlx::query!(
        "
        DELETE FROM beacon_validators_balance
        WHERE state_root IN (
            SELECT state_root FROM beacon_states
            WHERE slot >= $1
        )
        ",
        greater_than_or_equal.0
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn delete_validator_sum(executor: impl PgExecutor<'_>, slot: &Slot) {
    sqlx::query!(
        "
        DELETE FROM beacon_validators_balance
        WHERE state_root IN (
            SELECT state_root FROM beacon_states
            WHERE slot = $1
        )
        ",
        slot.0
    )
    .execute(executor)
    .await
    .unwrap();
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BeaconBalancesSum {
    pub slot: Slot,
    pub balances_sum: GweiNewtype,
}

pub async fn get_balances_by_state_root(
    executor: impl PgExecutor<'_>,
    state_root: &str,
) -> Option<GweiNewtype> {
    sqlx::query!(
        "
        SELECT
            gwei
        FROM
            beacon_validators_balance
        WHERE
            beacon_validators_balance.state_root = $1
        ",
        state_root
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .map(|row| {
        let gwei_i64: i64 = row.gwei;
        let gwei: GweiNewtype = gwei_i64.try_into().unwrap();
        gwei
    })
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound, TimeZone, Utc};
    use sqlx::Connection;

    use crate::{
        beacon_chain::{states::store_state, tests::store_test_block},
        db,
    };

    use super::*;

    #[tokio::test]
    async fn timestamp_is_start_of_day_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_balances", &Slot(17999)).await;

        store_validators_balance(
            &mut transaction,
            "0xtest_balances",
            &Slot(17999),
            &GweiNewtype(100),
        )
        .await;

        let validator_balances_by_day =
            get_validator_balances_by_start_of_day(&mut transaction).await;

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc
            .timestamp_opt(unix_timestamp.try_into().unwrap(), 0)
            .unwrap();

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        assert_eq!(datetime, start_of_day_datetime);
    }

    #[tokio::test]
    async fn delete_balance_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_balances", &Slot(17999)).await;

        store_validators_balance(
            &mut transaction,
            "0xtest_balances",
            &Slot(17999),
            &GweiNewtype(100),
        )
        .await;

        let balances = get_validator_balances_by_start_of_day(&mut transaction).await;
        assert_eq!(balances.len(), 1);

        delete_validator_sums(&mut transaction, &Slot(17999)).await;

        let balances = get_validator_balances_by_start_of_day(&mut transaction).await;
        assert_eq!(balances.len(), 0);
    }

    #[tokio::test]
    async fn get_balances_by_state_root_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_balances_by_state_root";
        let state_root = format!("0x{test_id}_state_root");

        store_test_block(&mut transaction, test_id).await;

        store_validators_balance(&mut transaction, &state_root, &Slot(0), &GweiNewtype(100)).await;

        let beacon_balances_sum = get_balances_by_state_root(&mut transaction, &state_root)
            .await
            .unwrap();

        assert_eq!(GweiNewtype(100), beacon_balances_sum);
    }
}
