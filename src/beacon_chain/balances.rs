use chrono::{Duration, DurationRound};
use reqwest::Client;
use sqlx::{PgExecutor, PgPool};

use crate::eth_units::GweiAmount;
use crate::supply_projection::{GweiInTime, GweiInTimeRow};

use super::beacon_time::FirstOfDaySlot;
use super::node::ValidatorBalance;
use super::{beacon_time, node, states};

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
        beacon_time::get_timestamp(slot),
        state_root,
        gwei,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub async fn get_last_effective_balance_sum(
    pool: &PgPool,
    client: &Client,
) -> anyhow::Result<GweiAmount> {
    let last_state_root = states::get_last_state(pool)
        .await?
        .expect("can't calculate a last effective balance with an empty beacon_states table")
        .state_root;

    node::get_validators_by_state(client, &last_state_root)
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

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use serial_test::serial;
    use sqlx::PgConnection;

    use crate::{beacon_chain::states::store_state, config};

    use super::*;

    async fn clean_tables<'a>(pg_exec: impl PgExecutor<'a>) {
        sqlx::query!("TRUNCATE TABLE beacon_states CASCADE")
            .execute(pg_exec)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_timestamp_is_start_of_day() {
        let mut conn: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut conn, "0xtest_balances", &17999)
            .await
            .unwrap();

        store_validator_sum_for_day(
            &mut conn,
            "0xtest_balances",
            &FirstOfDaySlot::new(&17999).unwrap(),
            &GweiAmount(100),
        )
        .await;

        let validator_balances_by_day = get_validator_balances_by_start_of_day(&mut conn)
            .await
            .unwrap();

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc.timestamp(unix_timestamp.try_into().unwrap(), 0);

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        clean_tables(&mut conn).await;

        assert_eq!(datetime, start_of_day_datetime);
    }
}
