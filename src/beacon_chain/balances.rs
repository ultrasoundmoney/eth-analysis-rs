use chrono::{Duration, DurationRound};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgExecutor;

use crate::eth_units::{to_gwei_string, GweiAmount};
use crate::key_value_store;
use crate::supply_projection::{GweiInTime, GweiInTimeRow};

use super::beacon_time::FirstOfDaySlot;
use super::node::ValidatorBalance;
use super::{beacon_time, states, BeaconNode, Slot};

pub fn sum_validator_balances(validator_balances: Vec<ValidatorBalance>) -> GweiAmount {
    validator_balances
        .iter()
        .fold(GweiAmount(0), |sum, validator_balance| {
            sum + validator_balance.balance
        })
}

pub async fn store_validator_sum_for_day<'a>(
    pool: impl PgExecutor<'a>,
    state_root: &str,
    FirstOfDaySlot(slot): &FirstOfDaySlot,
    gwei: &GweiAmount,
) {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "
            INSERT INTO beacon_validators_balance (timestamp, state_root, gwei) VALUES ($1, $2, $3)
        ",
        beacon_time::get_date_time_from_slot(slot),
        state_root,
        gwei,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub async fn get_last_effective_balance_sum<'a>(
    executor: impl PgExecutor<'a>,
    beacon_node: &BeaconNode,
) -> anyhow::Result<GweiAmount> {
    let last_state_root = states::get_last_state(executor)
        .await
        .expect("can't calculate a last effective balance with an empty beacon_states table")
        .state_root;

    beacon_node
        .get_validators_by_state(&last_state_root)
        .await
        .map(|validators| {
            validators.iter().fold(GweiAmount(0), |sum, validator| {
                sum + validator.effective_balance
            })
        })
        .map_err(anyhow::Error::msg)
}

pub async fn get_validator_balances_by_start_of_day<'a>(
    pool: impl PgExecutor<'a>,
) -> sqlx::Result<Vec<GweiInTime>> {
    sqlx::query_as!(
        GweiInTimeRow,
        "
            SELECT timestamp, gwei FROM beacon_validators_balance
        "
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.iter()
            .map(|row| {
                (
                    row.timestamp.duration_trunc(Duration::days(1)).unwrap(),
                    row.gwei,
                )
            })
            .map(|row| row.into())
            .collect()
    })
}

pub const BEACON_BALANCES_SUM_CACHE_KEY: &str = "beacon-balances-sum";

pub async fn set_balances_sum<'a>(
    executor: impl PgExecutor<'a>,
    slot: &u32,
    balances_sum: GweiAmount,
) {
    let value = serde_json::to_value(json!({
        "slot": slot,
        "balances_sum": balances_sum
    }))
    .unwrap();

    key_value_store::set_value(
        executor,
        key_value_store::KeyValue {
            key: BEACON_BALANCES_SUM_CACHE_KEY,
            value,
        },
    )
    .await;
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BeaconBalancesSum {
    slot: Slot,
    #[serde(serialize_with = "to_gwei_string")]
    balances_sum: GweiAmount,
}

pub async fn get_balances_sum<'a>(executor: impl PgExecutor<'a>) -> BeaconBalancesSum {
    let value = key_value_store::get_value(executor, BEACON_BALANCES_SUM_CACHE_KEY)
        .await
        .unwrap();

    serde_json::from_value(value).unwrap()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use serial_test::serial;
    use sqlx::{Connection, PgConnection};

    use crate::{beacon_chain::states::store_state, config};

    use super::*;

    #[tokio::test]
    #[serial]
    async fn timestamp_is_start_of_day_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_balances", &17999)
            .await
            .unwrap();

        store_validator_sum_for_day(
            &mut transaction,
            "0xtest_balances",
            &FirstOfDaySlot::new(&17999).unwrap(),
            &GweiAmount(100),
        )
        .await;

        let validator_balances_by_day = get_validator_balances_by_start_of_day(&mut transaction)
            .await
            .unwrap();

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc.timestamp(unix_timestamp.try_into().unwrap(), 0);

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        assert_eq!(datetime, start_of_day_datetime);
    }

    #[tokio::test]
    #[serial]
    async fn get_balances_sum_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        set_balances_sum(&mut transaction, &0, GweiAmount(1)).await;

        let beacon_chain_balances = get_balances_sum(&mut transaction).await;

        assert_eq!(
            beacon_chain_balances,
            BeaconBalancesSum {
                slot: 0,
                balances_sum: GweiAmount(1)
            }
        )
    }

    #[tokio::test]
    #[serial]
    async fn set_balances_sum_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        set_balances_sum(&mut transaction, &0, GweiAmount(1)).await;

        set_balances_sum(&mut transaction, &1, GweiAmount(2)).await;

        let beacon_chain_balances = get_balances_sum(&mut transaction).await;

        assert_eq!(
            beacon_chain_balances,
            BeaconBalancesSum {
                slot: 1,
                balances_sum: GweiAmount(2)
            }
        )
    }
}
