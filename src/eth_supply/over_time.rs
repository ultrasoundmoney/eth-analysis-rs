use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::join;
use serde::Serialize;
use sqlx::postgres::types::PgInterval;
use sqlx::{PgExecutor, PgPool};
use tracing::debug;
use tracing::warn;

use crate::{
    beacon_chain::Slot,
    eth_time,
    execution_chain::{self, BlockNumber},
    time_frames::{GrowingTimeFrame, LimitedTimeFrame, TimeFrame},
    units::EthNewtype,
};

use GrowingTimeFrame::*;
use LimitedTimeFrame::*;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SupplyAtTime {
    pub supply: EthNewtype,
    pub timestamp: DateTime<Utc>,
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
        supply: EthNewtype(row.supply),
        timestamp: row.timestamp,
    })
    .unwrap()
}

async fn get_our_first_timestamp(executor: impl PgExecutor<'_>) -> DateTime<Utc> {
    sqlx::query!(
        "
        SELECT
            timestamp
        FROM
            eth_supply
        ORDER BY timestamp ASC
        LIMIT 1
        ",
    )
    .fetch_one(executor)
    .await
    .map(|row| row.timestamp)
    .unwrap()
}

async fn get_early_supply_since_burn(
    executor: impl PgExecutor<'_>,
    our_first_timestamp: &DateTime<Utc>,
) -> Vec<SupplyAtTime> {
    sqlx::query!(
        "
        SELECT
            timestamp,
            supply
        FROM daily_supply_glassnode
        WHERE timestamp >= $1
        AND timestamp < $2
        ORDER BY timestamp ASC
        ",
        *execution_chain::LONDON_HARD_FORK_TIMESTAMP,
        our_first_timestamp,
    )
    .fetch_all(executor)
    .await
    .unwrap()
    .into_iter()
    .map(|row| SupplyAtTime {
        timestamp: row.timestamp,
        supply: EthNewtype(row.supply),
    })
    .collect()
}

pub async fn from_time_frame(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> Vec<SupplyAtTime> {
    match time_frame {
        TimeFrame::Growing(SinceBurn) => {
            sqlx::query!(
                "
                -- We select only one row per day, using ORDER BY to make sure it's the first.
                -- The column we output is rounded to whole days for convenience.
                SELECT
                    DISTINCT ON (DATE_TRUNC('day', timestamp)) DATE_TRUNC('day', timestamp) AS \"day_timestamp!\",
                    supply::FLOAT8 / 1e18 AS \"supply!\"
                FROM
                    eth_supply
                ORDER BY
                    DATE_TRUNC('day', timestamp) ASC
                ",
            )
            .fetch_all(executor)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                SupplyAtTime {
                    timestamp: row.day_timestamp,
                    supply: EthNewtype(row.supply),
                }
            })
            .collect()
        },
        TimeFrame::Growing(SinceMerge) => {
            sqlx::query!(
                "
                SELECT
                    DISTINCT ON (DATE_TRUNC('day', timestamp)) DATE_TRUNC('day', timestamp) AS \"day_timestamp!\",
                    supply::FLOAT8 / 1e18 AS \"supply!\"
                FROM
                    eth_supply
                WHERE
                    timestamp >= $1
                ORDER BY
                    DATE_TRUNC('day', timestamp) ASC
                ",
                *eth_time::MERGE_HARD_FORK_TIMESTAMP
            )
            .fetch_all(executor)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                SupplyAtTime {
                    timestamp: row.day_timestamp,
                    supply: EthNewtype(row.supply),
                }
            })
            .collect()
        }
        TimeFrame::Limited(ltf @ Minute5)
        | TimeFrame::Limited(ltf @ Hour1) => {
            sqlx::query!(
                "
                SELECT
                    timestamp,
                    supply::FLOAT8 / 1e18 AS \"supply!\"
                FROM
                    eth_supply
                WHERE
                    timestamp >= NOW() - $1::INTERVAL
                ORDER BY
                    timestamp ASC
                ",
                Into::<PgInterval>::into(ltf)
            )
            .fetch_all(executor)
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                SupplyAtTime {
                    timestamp: row.timestamp,
                    supply: EthNewtype(row.supply),
                }
            })
            .collect()
        }
        TimeFrame::Limited(ltf @ Day1) => {
            sqlx::query!(
                "
                SELECT
                    DISTINCT ON (date_bin('384 seconds', timestamp, '2022-1-1')) date_bin('384 seconds', timestamp, '2022-1-1') AS \"epoch_timestamp!\",
                    supply::FLOAT8 / 1e18 AS \"supply!\"
                FROM
                    eth_supply
                WHERE
                    timestamp >= NOW() - $1::INTERVAL
                ORDER BY
                    date_bin('384 seconds', timestamp, '2022-1-1') ASC
                ",
                Into::<PgInterval>::into(ltf)
            )
            .fetch_all(executor)
            .await
            .unwrap()
                .into_iter()
            .map(|row| {
                SupplyAtTime {
                    timestamp: row.epoch_timestamp,
                    supply: EthNewtype(row.supply),
                }
            })
            .collect()
        }
        TimeFrame::Limited(ltf @ Day7) => {
            sqlx::query!(
                "
                SELECT
                    DISTINCT ON (DATE_BIN('5 minutes', timestamp, '2022-01-01')) DATE_BIN('5 minutes', timestamp, '2022-01-01') AS \"five_minute_timestamp!\",
                    supply::FLOAT8 / 1e18 AS \"supply!\"
                FROM
                    eth_supply
                WHERE
                    timestamp >= NOW() - $1::INTERVAL
                ORDER BY
                    DATE_BIN('5 minutes', timestamp, '2022-01-01') ASC
                ",
                ltf.postgres_interval()
            )
            .fetch_all(executor)
            .await
            .unwrap()
                .into_iter()
                .map(|row| {
                    SupplyAtTime {
                        timestamp: row.five_minute_timestamp,
                        supply: EthNewtype(row.supply),
                    }
                })
                .collect()
        }
        TimeFrame::Limited(ltf @ Day30) => {
            sqlx::query!(
                "
                SELECT
                    DISTINCT ON (DATE_TRUNC('hour', timestamp)) DATE_TRUNC('hour', timestamp) AS \"hour_timestamp!\",
                    supply::FLOAT8 / 1e18 AS \"supply!\"
                FROM
                    eth_supply
                WHERE
                    timestamp >= NOW() - $1::INTERVAL
                ORDER BY
                    DATE_TRUNC('hour', timestamp) ASC
                ",
                Into::<PgInterval>::into(ltf)
            )
            .fetch_all(executor)
                .await
                .unwrap()
            .into_iter()
                .map(|row| {
                SupplyAtTime {
                    timestamp: row.hour_timestamp,
                    supply: EthNewtype(row.supply),
                }
            })
            .collect()
        }
    }
}

async fn get_supply_since_burn(db_pool: &PgPool) -> Vec<SupplyAtTime> {
    // We're in the process of collecting slot by slot ETH supply data. This is a slow process.
    // Until we are done, we use Glassnode's data to fill in the gap temporarily until our own
    // table reaches back to genesis. We query to find out when our table starts, and then query
    // everything up to there from Glassnode based table.
    let our_first_timestamp = get_our_first_timestamp(db_pool).await;
    let eth_supply_glassnode = get_early_supply_since_burn(db_pool, &our_first_timestamp).await;
    let eth_supply_ours =
        from_time_frame(db_pool, &TimeFrame::Growing(GrowingTimeFrame::SinceBurn)).await;

    let eth_supply: Vec<SupplyAtTime> = eth_supply_glassnode
        .into_iter()
        .chain(eth_supply_ours.into_iter())
        .collect();

    eth_supply
}

#[derive(Clone, Serialize)]
pub struct SupplyOverTime {
    block_number: BlockNumber,
    pub d1: Vec<SupplyAtTime>,
    pub d30: Vec<SupplyAtTime>,
    pub d7: Vec<SupplyAtTime>,
    pub h1: Vec<SupplyAtTime>,
    pub m5: Vec<SupplyAtTime>,
    pub since_burn: Vec<SupplyAtTime>,
    pub since_merge: Vec<SupplyAtTime>,
    pub slot: Slot,
    pub timestamp: DateTime<Utc>,
}

pub async fn get_supply_over_time(
    executor: &PgPool,
    slot: Slot,
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
        get_supply_since_burn(executor),
        get_last_supply_point(executor)
    );

    if d1.is_empty() {
        warn!("d1 supply over time vector is empty");
    }
    if d30.is_empty() {
        warn!("d30 supply over time vector is empty");
    }
    if d7.is_empty() {
        warn!("d7 supply over time vector is empty");
    }
    if h1.is_empty() {
        warn!("h1 supply over time vector is empty");
    }
    if m5.is_empty() {
        warn!("m5 supply over time vector is empty");
    }
    if since_merge.is_empty() {
        warn!("since_merge supply over time vector is empty");
    }
    if since_burn.is_empty() {
        warn!("since_burn supply over time vector is empty");
    }

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
        slot,
        timestamp: slot.date_time(),
    };

    Ok(supply_over_time)
}

pub async fn get_daily_supply(db_pool: &PgPool) -> Vec<SupplyAtTime> {
    let our_first_timestamp = get_our_first_timestamp(db_pool).await;

    let eth_supply_glassnode: Vec<_> = sqlx::query!(
        "
        SELECT
            timestamp,
            supply
        FROM daily_supply_glassnode
        WHERE timestamp < $1
        ORDER BY timestamp ASC
        ",
        our_first_timestamp,
    )
    .fetch_all(db_pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| SupplyAtTime {
        timestamp: row.timestamp,
        supply: EthNewtype(row.supply),
    })
    .collect();

    let eth_supply_ours =
        from_time_frame(db_pool, &TimeFrame::Growing(GrowingTimeFrame::SinceBurn)).await;

    let eth_supply: Vec<SupplyAtTime> = eth_supply_glassnode
        .into_iter()
        .chain(eth_supply_ours.into_iter())
        .collect();

    eth_supply
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound, SubsecRound};
    use sqlx::Acquire;

    use crate::{db, eth_supply::test::store_test_eth_supply, units::EthNewtype};

    use super::*;

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
        };

        store_test_eth_supply(&mut transaction, test_slot, EthNewtype(10.0))
            .await
            .unwrap();

        let since_merge = from_time_frame(&mut *transaction, &TimeFrame::Limited(Minute5)).await;

        assert_eq!(since_merge, vec![test_supply_at_time]);
    }

    #[tokio::test]
    async fn daily_supply_glassnode_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_supply_at_time = SupplyAtTime {
            timestamp: "2022-09-09T21:36:23Z"
                .parse::<DateTime<Utc>>()
                .unwrap()
                .duration_trunc(Duration::days(1))
                .unwrap()
                - Duration::days(1),
            supply: EthNewtype(10.0),
        };

        sqlx::query!(
            "INSERT INTO daily_supply_glassnode (timestamp, supply) VALUES ($1, $2)",
            test_supply_at_time.timestamp,
            test_supply_at_time.supply.0
        )
        .execute(&mut *transaction)
        .await
        .unwrap();

        let since_burn = get_early_supply_since_burn(
            &mut *transaction,
            &(test_supply_at_time.timestamp + Duration::seconds(1)),
        )
        .await;

        assert_eq!(since_burn, vec![test_supply_at_time]);
    }
}
