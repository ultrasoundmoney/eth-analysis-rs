mod backfill;

use anyhow::Result;
use chrono::{Duration, DurationRound};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, PgExecutor, Row};

use crate::units::{GweiNewtype};
use crate::supply_projection::GweiInTime;

use super::node::{ValidatorBalance, BeaconNode};
use super::{states, Slot};

pub use backfill::backfill_balances_to_london;
pub use backfill::backfill_daily_balances_to_london;

pub fn sum_validator_balances(validator_balances: &Vec<ValidatorBalance>) -> GweiNewtype {
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
) -> Result<()>{
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
    .await?;

    Ok(())
}

pub async fn get_last_effective_balance_sum<'a>(
    executor: &mut PgConnection,
    beacon_node: &BeaconNode,
) -> anyhow::Result<GweiNewtype> {
    let last_state_root = states::get_last_state(executor)
        .await
        .expect("can't calculate a last effective balance with an empty beacon_states table")
        .state_root;

    beacon_node
        .get_validators_by_state(&last_state_root)
        .await
        .map(|validators| {
            validators.iter().fold(GweiNewtype(0), |sum, validator| {
                sum + validator.effective_balance
            })
        })
        .map_err(anyhow::Error::msg)
}

pub async fn get_validator_balances_by_start_of_day(
    executor: impl PgExecutor<'_>,
) -> sqlx::Result<Vec<GweiInTime>> {
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
         
    })
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BeaconBalancesSum {
    pub slot: Slot,
    #[serde(serialize_with = "to_gwei_string")]
    pub balances_sum: GweiNewtype,
}

pub async fn get_balances_by_state_root(executor: impl PgExecutor<'_>, state_root: &str) -> Result<Option<GweiNewtype>> {
    let row = sqlx::query(
        "
            SELECT
                gwei
            FROM
                beacon_validators_balance
            WHERE
                beacon_validators_balance.state_root = $1
        ",
    ).bind(state_root)
    .map(|row: PgRow| {
        let gwei: GweiNewtype = row.get::<i64, _>("gwei").try_into().unwrap();
        gwei
    })
    .fetch_optional(executor)
    .await?;

    Ok(row)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc, DurationRound};
    use sqlx::Connection;

    use crate::{beacon_chain::{states::store_state, tests::store_test_block}, db};

    use super::*;

    #[tokio::test]
    async fn timestamp_is_start_of_day_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_balances", &Slot(17999))
            .await
            .unwrap();

        store_validators_balance(
            &mut transaction,
            "0xtest_balances",
            &Slot(17999),
            &GweiNewtype(100),
        )
        .await.unwrap();

        let validator_balances_by_day =
            get_validator_balances_by_start_of_day(&mut transaction).await.unwrap();

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc.timestamp_opt(unix_timestamp.try_into().unwrap(), 0).unwrap();

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        assert_eq!(datetime, start_of_day_datetime);
    }

    #[tokio::test]
    async fn delete_balance_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_balances", &Slot(17999))
            .await
            .unwrap();

        store_validators_balance(
            &mut transaction,
            "0xtest_balances",
            &Slot(17999),
            &GweiNewtype(100),
        )
        .await.unwrap();

        let balances = get_validator_balances_by_start_of_day(&mut transaction).await.unwrap();
        assert_eq!(balances.len(), 1);

        delete_validator_sums(&mut transaction, &Slot(17999)).await;

        let balances = get_validator_balances_by_start_of_day(&mut transaction).await.unwrap();
        assert_eq!(balances.len(), 0);
    }

    #[tokio::test]
    async fn get_balances_by_state_root_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "get_balances_by_state_root";
        let state_root = format!("0x{test_id}_state_root");

        store_test_block(&mut transaction, test_id).await;

        store_validators_balance(
            &mut transaction,
            &state_root,
            &Slot(0),
            &GweiNewtype(100),
        )
        .await.unwrap();

        let beacon_balances_sum = get_balances_by_state_root(&mut transaction, &state_root).await.unwrap().unwrap();

        assert_eq!(GweiNewtype(100), beacon_balances_sum);
    }
}
