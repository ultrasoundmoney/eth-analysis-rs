use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, PgPool, Row};
use tracing::debug;

use crate::units::EthF64;
use crate::{
    beacon_chain::Slot,
    execution_chain::BlockNumber,
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
};

use GrowingTimeFrame::*;
use LimitedTimeFrame::*;

#[derive(Debug, Clone, PartialEq, Serialize)]
struct SupplyAtTime {
    slot: Option<Slot>,
    supply: EthF64,
    timestamp: DateTime<Utc>,
}

async fn get_supply_over_time_time_frame(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> Vec<SupplyAtTime> {
    match time_frame {
        TimeFrame::Growing(SinceBurn) => {
            sqlx::query(
                "
                    -- We select only one row per day, using ORDER BY to make sure it's the first.
                    -- The column we output is rounded to whole days for convenience.
                    SELECT
                        DISTINCT ON (DATE_TRUNC('day', timestamp)) DATE_TRUNC('day', timestamp) AS day_timestamp,
                        supply::FLOAT8 / 1e18 AS supply
                    FROM
                        eth_supply
                    ORDER BY
                        DATE_TRUNC('day', timestamp), timestamp ASC
                ",
            )
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("day_timestamp");
                let supply = row.get::<f64, _>("supply");
                SupplyAtTime {
                    slot: None,
                    timestamp,
                    supply,
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        },
        TimeFrame::Growing(SinceMerge) => {
            sqlx::query(
                "
                    SELECT
                        DISTINCT ON (DATE_TRUNC('day', timestamp)) DATE_TRUNC('day', timestamp) AS day_timestamp,
                        supply::FLOAT8 / 1e18 AS supply
                    FROM
                        eth_supply
                    WHERE
                        timestamp >= '2022-09-15T06:42:42Z'::TIMESTAMPTZ
                    ORDER BY
                        DATE_TRUNC('day', timestamp), timestamp ASC
                ",
            )
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("day_timestamp");
                let supply = row.get::<f64, _>("supply");
                SupplyAtTime {
                    slot: None,
                    timestamp,
                    supply,
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        }
        TimeFrame::Limited(ltf @ Minute5)
        | TimeFrame::Limited(ltf @ Hour1) => {
            sqlx::query(
                "
                    SELECT
                        timestamp,
                        supply::FLOAT8 / 1e18 AS supply,
                        balances_slot
                    FROM
                        eth_supply
                    WHERE
                        timestamp >= NOW() - $1
                    ORDER BY
                        timestamp ASC
                ",
            )
            .bind(ltf.postgres_interval())
            .map(|row: PgRow| {
                let slot: Slot = row
                    .get::<i32, _>("balances_slot")
                    .try_into().unwrap();
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("timestamp");
                let supply = row.get::<f64, _>("supply");
                SupplyAtTime {
                    slot: Some(slot),
                    timestamp,
                    supply,
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        }
        TimeFrame::Limited(ltf @ Day1) => {
            sqlx::query(
                "
                    SELECT
                        DISTINCT ON (DATE_TRUNC('minute', timestamp)) DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                        supply::FLOAT8 / 1e18 AS supply
                    FROM
                        eth_supply
                    WHERE
                        timestamp >= NOW() - $1
                    ORDER BY
                        DATE_TRUNC('minute', timestamp), timestamp ASC
                ",
            )
            .bind(ltf.postgres_interval())
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("minute_timestamp");
                let supply = row.get::<f64, _>("supply");
                SupplyAtTime {
                    slot: None,
                    timestamp,
                    supply,
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        }
        TimeFrame::Limited(ltf @ Day7) => {
            sqlx::query(
                "
                    SELECT
                        DISTINCT ON (DATE_BIN('5 minutes', timestamp, '2022-01-01')) DATE_BIN('5 minutes', timestamp, '2022-01-01') AS five_minute_timestamp,
                        supply::FLOAT8 / 1e18 AS supply
                    FROM
                        eth_supply
                    WHERE
                        timestamp >= NOW() - $1
                    ORDER BY
                        DATE_BIN('5 minutes', timestamp, '2022-01-01'), timestamp ASC
                ",
            )
            .bind(ltf.postgres_interval())
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("five_minute_timestamp");
                let supply = row.get::<f64, _>("supply");
                SupplyAtTime {
                    slot: None,
                    timestamp,
                    supply,
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        }
        TimeFrame::Limited(ltf @ Day30) => {
            sqlx::query(
                "
                    SELECT
                        DISTINCT ON (DATE_TRUNC('hour', timestamp)) DATE_TRUNC('hour', timestamp) AS hour_timestamp,
                        supply::FLOAT8 / 1e18 AS supply
                    FROM
                        eth_supply
                    WHERE
                        timestamp >= NOW() - $1
                    ORDER BY
                        DATE_TRUNC('hour', timestamp), timestamp ASC
                ",
            )
            .bind(ltf.postgres_interval())
            .map(|row: PgRow| {
                let timestamp: DateTime<Utc> = row.get::<DateTime<Utc>, _>("hour_timestamp");
                let supply = row.get::<f64, _>("supply");
                SupplyAtTime {
                    slot: None,
                    timestamp,
                    supply,
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        }
    }
}

#[derive(Clone, Serialize)]
pub struct SupplyOverTime {
    block_number: BlockNumber,
    d1: Vec<SupplyAtTime>,
    d30: Vec<SupplyAtTime>,
    d7: Vec<SupplyAtTime>,
    h1: Vec<SupplyAtTime>,
    m5: Vec<SupplyAtTime>,
    since_burn: Vec<SupplyAtTime>,
    since_merge: Vec<SupplyAtTime>,
    slot: Slot,
    timestamp: DateTime<Utc>,
}

pub async fn get_supply_over_time(
    executor: &PgPool,
    slot: &Slot,
    block_number: BlockNumber,
) -> Result<SupplyOverTime> {
    debug!("updating supply over time");

    let (d1, d30, d7, h1, m5, since_merge, since_burn) = join!(
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Day1)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Day30)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Day7)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Hour1)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Minute5)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Growing(SinceMerge)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Growing(SinceBurn))
    );

    let supply_over_time = SupplyOverTime {
        block_number,
        d1,
        d30,
        d7,
        h1,
        m5,
        since_burn,
        since_merge,
        slot: *slot,
        timestamp: slot.date_time(),
    };

    Ok(supply_over_time)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, SubsecRound};
    use sqlx::Acquire;

    use crate::{db, eth_supply::test::store_test_eth_supply, units::EthNewtype};

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn selects_within_time_frame_test() {}

    #[tokio::test]
    async fn supply_over_time_m5_test() {
        let mut connection = db::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_timestamp = Utc::now().trunc_subsecs(0) - Duration::minutes(2);
        // This step is inexact. We estimate the slot based on the timestamp. Then convert this
        // estimated slot to the actual timestamp.
        let test_slot: Slot = Slot::from_date_time_rounded_down(&test_timestamp);
        let reverse_timestamp = test_slot.date_time();

        let test_supply_at_time = SupplyAtTime {
            timestamp: reverse_timestamp,
            supply: 10.0,
            slot: Some(test_slot),
        };

        store_test_eth_supply(&mut transaction, &test_slot, EthNewtype(10.0))
            .await
            .unwrap();

        let since_merge =
            get_supply_over_time_time_frame(&mut transaction, &TimeFrame::Limited(Minute5)).await;

        assert_eq!(since_merge, vec![test_supply_at_time]);
    }
}
