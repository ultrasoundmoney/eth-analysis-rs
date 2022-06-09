use chrono::{Duration, DurationRound};
use sqlx::PgExecutor;

use crate::{
    eth_units::GweiAmount,
    supply_projection::{GweiInTime, GweiInTimeRow},
};

use super::{
    beacon_time::{self, FirstOfDaySlot},
    deposits,
};

pub async fn store_issuance_for_day<'a>(
    pool: impl PgExecutor<'a>,
    state_root: &str,
    FirstOfDaySlot(slot): &FirstOfDaySlot,
    gwei: &GweiAmount,
) {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "
            INSERT INTO beacon_issuance (timestamp, state_root, gwei) VALUES ($1, $2, $3)
        ",
        beacon_time::get_timestamp(&slot),
        state_root,
        gwei,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub fn calc_issuance(
    validator_balances_sum_gwei: &GweiAmount,
    deposit_sum_aggregated: &GweiAmount,
) -> GweiAmount {
    (*validator_balances_sum_gwei - *deposit_sum_aggregated) - deposits::INITIAL_DEPOSITS
}

pub async fn get_issuance_by_start_of_day<'a>(
    pool: impl PgExecutor<'a>,
) -> sqlx::Result<Vec<GweiInTime>> {
    sqlx::query_as!(
        GweiInTimeRow,
        "
            SELECT timestamp, gwei FROM beacon_issuance
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

pub async fn get_current_issuance<'a>(pool: impl PgExecutor<'a>) -> sqlx::Result<GweiAmount> {
    sqlx::query!(
        "
            SELECT gwei FROM beacon_issuance
            ORDER BY timestamp DESC
            LIMIT 1
        "
    )
    .fetch_one(pool)
    .await
    .map(|row| GweiAmount(row.gwei as u64))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serial_test::serial;
    use sqlx::{PgConnection, PgExecutor};

    use super::*;
    use crate::{beacon_chain::states::store_state, config};

    #[test]
    fn test_calc_issuance() {
        let validator_balances_sum_gwei = deposits::INITIAL_DEPOSITS + GweiAmount(100);
        let deposit_sum_aggregated = GweiAmount(50);

        assert_eq!(
            calc_issuance(&validator_balances_sum_gwei, &deposit_sum_aggregated),
            GweiAmount(50)
        )
    }

    async fn clean_tables<'a>(pg_exec: impl PgExecutor<'a>) {
        sqlx::query!("TRUNCATE beacon_states CASCADE")
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

        store_state(&mut conn, "0xtest_issuance", &3599)
            .await
            .unwrap();

        store_issuance_for_day(
            &mut conn,
            "0xtest_issuance",
            &FirstOfDaySlot::new(&3599).unwrap(),
            &GweiAmount(100),
        )
        .await;

        let validator_balances_by_day = get_issuance_by_start_of_day(&mut conn).await.unwrap();

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc.timestamp(unix_timestamp.try_into().unwrap(), 0);

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        clean_tables(&mut conn).await;

        assert_eq!(datetime, start_of_day_datetime);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_current_issuance() {
        let mut conn: PgConnection = sqlx::Connection::connect(&config::get_db_url())
            .await
            .unwrap();

        store_state(&mut conn, "0xtest_issuance_1", &3599)
            .await
            .unwrap();

        store_state(&mut conn, "0xtest_issuance_2", &10799)
            .await
            .unwrap();

        store_issuance_for_day(
            &mut conn,
            "0xtest_issuance_1",
            &FirstOfDaySlot::new(&3599).unwrap(),
            &GweiAmount(100),
        )
        .await;

        store_issuance_for_day(
            &mut conn,
            "0xtest_issuance_2",
            &FirstOfDaySlot::new(&10799).unwrap(),
            &GweiAmount(110),
        )
        .await;

        let current_issuance = get_current_issuance(&mut conn).await.unwrap();

        clean_tables(&mut conn).await;

        assert_eq!(current_issuance, GweiAmount(110));
    }
}
