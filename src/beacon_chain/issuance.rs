use anyhow::Result;
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::{postgres::PgRow, Acquire, PgConnection, PgExecutor, Row};

use crate::{
    eth_units::GweiNewtype,
    supply_projection::GweiInTime,
};

use super::{
    beacon_time,
    Slot,
};

pub async fn store_issuance(
    executor: impl PgExecutor<'_>,
    state_root: &str,
    slot: &Slot,
    gwei: &GweiNewtype,
) -> Result<()> {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "
            INSERT INTO beacon_issuance (timestamp, state_root, gwei) VALUES ($1, $2, $3)
        ",
        beacon_time::get_date_time_from_slot(&slot),
        state_root,
        gwei,
    )
    .execute(executor)
    .await?;

    Ok(())
}

pub fn calc_issuance(
    validator_balances_sum_gwei: &GweiNewtype,
    deposit_sum_aggregated: &GweiNewtype,
) -> GweiNewtype {
    *validator_balances_sum_gwei - *deposit_sum_aggregated
}

pub async fn get_issuance_by_start_of_day(
    pool: impl PgExecutor<'_>,
) -> sqlx::Result<Vec<GweiInTime>> {
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
}

pub struct BeaconIssuance {
    pub gwei: GweiNewtype,
    timestamp: DateTime<Utc>,
}

pub async fn get_current_issuance(executor: impl PgExecutor<'_>) -> BeaconIssuance {
    sqlx::query(
        "
            SELECT gwei, timestamp FROM beacon_issuance
            ORDER BY timestamp DESC
            LIMIT 1
        ",
    )
    .map(|row: PgRow| {
        let timestamp = row.get::<DateTime<Utc>, _>("timestamp");
        let gwei = row.get::<i64, _>("gwei") as u64;
        BeaconIssuance {
            timestamp,
            gwei: GweiNewtype(gwei),
        }
    })
    .fetch_one(executor)
    .await
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
        *greater_than_or_equal as i32
    )
    .execute(connection)
    .await
    .unwrap();
}

pub async fn delete_issuance(connection: impl PgExecutor<'_>, slot: &Slot) {
    sqlx::query(
        "
            DELETE FROM beacon_issuance
            WHERE state_root IN (
                SELECT state_root FROM beacon_states
                WHERE slot = $1
            )
        ",
    )
    .bind(*slot as i32)
    .execute(connection)
    .await
    .unwrap();
}

pub async fn get_day7_ago_issuance( executor: impl PgExecutor<'_>,) -> GweiNewtype {
    let row = sqlx::query!(
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
    .unwrap();

    GweiNewtype(row.gwei as u64)
}

pub async fn get_last_week_issuance(executor: &mut PgConnection) -> GweiNewtype {
    let current_issuance = get_current_issuance(executor.acquire().await.unwrap()).await;
    let day7_ago_issuance = get_day7_ago_issuance(executor).await;
    current_issuance.gwei - day7_ago_issuance
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::{beacon_chain::states::store_state, db};

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
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_id = "timestamp_is_start_of_day";

        store_state(&mut transaction, test_id, &3599, &test_id)
            .await
            .unwrap();

        store_issuance(
            &mut transaction,
            test_id,
            &3599,
            &GweiNewtype(100),
        )
        .await.unwrap();

        let validator_balances_by_day = get_issuance_by_start_of_day(&mut transaction)
            .await
            .unwrap();

        let unix_timestamp = validator_balances_by_day.first().unwrap().t;

        let datetime = Utc.timestamp(unix_timestamp.try_into().unwrap(), 0);

        let start_of_day_datetime = datetime.duration_trunc(Duration::days(1)).unwrap();

        assert_eq!(datetime, start_of_day_datetime);
    }

    #[tokio::test]
    async fn get_current_issuance_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_issuance_1", &3599, "")
            .await
            .unwrap();

        store_state(&mut transaction, "0xtest_issuance_2", &10799, "")
            .await
            .unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance_1",
            &3599,
            &GweiNewtype(100),
        )
        .await.unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance_2",
            &10799,
            &GweiNewtype(110),
        )
        .await.unwrap();

        let current_issuance = get_current_issuance(&mut transaction).await.gwei;

        assert_eq!(current_issuance, GweiNewtype(110));
    }

    #[tokio::test]
    async fn delete_issuance_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        store_state(&mut transaction, "0xtest_issuance", &3599, "")
            .await
            .unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance",
            &3599,
            &GweiNewtype(100),
        )
        .await.unwrap();

        let issuance_by_day = get_issuance_by_start_of_day(&mut transaction)
            .await
            .unwrap();

        assert_eq!(issuance_by_day.len(), 1);

        delete_issuances(&mut transaction, &3599).await;

        let issuance_by_day_after = get_issuance_by_start_of_day(&mut transaction)
            .await
            .unwrap();

        assert_eq!(issuance_by_day_after.len(), 0);
    }

    #[tokio::test]
    async fn get_day7_ago_issuance_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let now_min_seven_days_slot = beacon_time::get_slot_from_date_time(&(Utc::now() - Duration::days(7)));
        let now_slot = beacon_time::get_slot_from_date_time(&Utc::now());

        store_state(&mut transaction, "0xtest_issuance_1", &now_min_seven_days_slot, "")
            .await
            .unwrap();

        store_state(&mut transaction, "0xtest_issuance_2", &now_slot, "")
            .await
            .unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance_1",
            &now_min_seven_days_slot,
            &GweiNewtype(100),
        )
        .await.unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance_2",
            &now_slot,
            &GweiNewtype(110),
        )
        .await.unwrap();

        let current_issuance = get_current_issuance(&mut transaction).await;
        let day7_ago_issuance = get_day7_ago_issuance(&mut transaction).await;

        assert_eq!(day7_ago_issuance, GweiNewtype(100));
    }

    #[tokio::test]
    async fn get_last_week_issuance_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let now_min_seven_days_slot = beacon_time::get_slot_from_date_time(&(Utc::now() - Duration::days(7)));
        let now_slot = beacon_time::get_slot_from_date_time(&Utc::now());

        store_state(&mut transaction, "0xtest_issuance_1", &now_min_seven_days_slot, "")
            .await
            .unwrap();

        store_state(&mut transaction, "0xtest_issuance_2", &now_slot, "")
            .await
            .unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance_1",
            &now_min_seven_days_slot,
            &GweiNewtype(100),
        )
        .await.unwrap();

        store_issuance(
            &mut transaction,
            "0xtest_issuance_2",
            &now_slot,
            &GweiNewtype(110),
        )
        .await.unwrap();

        let issuance = get_last_week_issuance(&mut transaction).await;

        assert_eq!(issuance, GweiNewtype(10));
    }
}
