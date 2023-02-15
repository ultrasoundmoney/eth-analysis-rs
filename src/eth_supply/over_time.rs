use anyhow::Result;
use cached::proc_macro::once;
use chrono::{DateTime, Duration, DurationRound, Utc};
use futures::join;
use lazy_static::lazy_static;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, PgPool, Row};
use tracing::debug;

use crate::units::EthNewtype;
use crate::{
    beacon_chain::Slot,
    execution_chain::BlockNumber,
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
};

use GrowingTimeFrame::*;
use LimitedTimeFrame::*;

lazy_static! {
    static ref ETH_SUPPLY_FIRST_TIMESTAMP_DAY: DateTime<Utc> = "2022-09-09T21:36:23Z"
        .parse::<DateTime<Utc>>()
        .unwrap()
        .duration_trunc(Duration::days(1))
        .unwrap();
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct SupplyAtTime {
    slot: Option<Slot>,
    supply: EthNewtype,
    timestamp: DateTime<Utc>,
}

async fn get_last_supply_point(executor: impl PgExecutor<'_>) -> SupplyAtTime {
    sqlx::query!(
        "
        SELECT
            timestamp,
            supply::FLOAT8 / 1e18 AS \"supply!\"
        FROM
            eth_supply
        ORDER BY timestamp DESC
        LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .map(|row| SupplyAtTime {
        slot: None,
        timestamp: row.timestamp,
        supply: EthNewtype(row.supply),
    })
    .unwrap()
}

#[once]
async fn get_daily_glassnode_supply(executor: impl PgExecutor<'_>) -> Vec<SupplyAtTime> {
    sqlx::query!(
        "
        SELECT
            timestamp,
            supply
        FROM daily_supply_glassnode
        WHERE timestamp < $1
        ORDER BY timestamp ASC
        ",
        *ETH_SUPPLY_FIRST_TIMESTAMP_DAY
    )
    .fetch_all(executor)
    .await
    .unwrap()
    .into_iter()
    .map(|row| SupplyAtTime {
        slot: None,
        timestamp: row.timestamp,
        supply: EthNewtype(row.supply),
    })
    .collect()
}

async fn from_time_frame(
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
                    supply: EthNewtype(supply),
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
                    supply: EthNewtype(supply),
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
                    supply: EthNewtype(supply),
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
                    supply: EthNewtype(supply),
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
                    supply: EthNewtype(supply),
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
                    supply: EthNewtype(supply),
                }
            })
            .fetch_all(executor)
            .await
            .unwrap()
        }
    }
}

async fn since_burn_combined(db_pool: &PgPool) -> Vec<SupplyAtTime> {
    // Glassnode data goes way back, but we only rely on it temporarily until our own table reaches
    // back to genesis. Currently, it reaches back to ETH_SUPPLY_FIRST_TIMESTAMP_DAY. Everything up
    // to there is from Glassnode.
    let eth_supply_glassnode = get_daily_glassnode_supply(db_pool).await;
    let eth_supply_ours =
        from_time_frame(db_pool, &TimeFrame::Growing(GrowingTimeFrame::SinceBurn)).await;

    eth_supply_glassnode
        .into_iter()
        .chain(eth_supply_ours.into_iter())
        .collect()
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

    let (d1, mut d30, mut d7, h1, m5, mut since_merge, mut since_burn, last_supply_point) = join!(
        from_time_frame(executor, &TimeFrame::Limited(Day1)),
        from_time_frame(executor, &TimeFrame::Limited(Day30)),
        from_time_frame(executor, &TimeFrame::Limited(Day7)),
        from_time_frame(executor, &TimeFrame::Limited(Hour1)),
        from_time_frame(executor, &TimeFrame::Limited(Minute5)),
        from_time_frame(executor, &TimeFrame::Growing(SinceMerge)),
        since_burn_combined(executor),
        get_last_supply_point(executor)
    );

    // We like showing the latest data point we have, regardless of the time frame. We add said
    // point to the time frames that use a higher than every-minute granularity.
    d7.push(last_supply_point.clone());
    d30.push(last_supply_point.clone());
    since_merge.push(last_supply_point.clone());
    since_burn.push(last_supply_point);

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
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_timestamp = Utc::now().trunc_subsecs(0) - Duration::minutes(2);
        // This step is inexact. We estimate the slot based on the timestamp. Then convert this
        // estimated slot to the actual timestamp.
        let test_slot: Slot = Slot::from_date_time_rounded_down(&test_timestamp);
        let reverse_timestamp = test_slot.date_time();

        let test_supply_at_time = SupplyAtTime {
            timestamp: reverse_timestamp,
            supply: EthNewtype(10.0),
            slot: Some(test_slot),
        };

        store_test_eth_supply(&mut transaction, &test_slot, EthNewtype(10.0))
            .await
            .unwrap();

        let since_merge = from_time_frame(&mut transaction, &TimeFrame::Limited(Minute5)).await;

        assert_eq!(since_merge, vec![test_supply_at_time]);
    }
}
