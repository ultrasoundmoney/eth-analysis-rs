use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::try_join;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, PgPool, Row};
use tracing::debug;

use crate::time_frames::LimitedTimeFrame::*;
use crate::{
    beacon_chain::Slot,
    caching::{self, CacheKey},
    execution_chain::BlockNumber,
    time_frames::TimeFrame,
};

use super::SupplyAtTime;

async fn get_supply_over_time_time_frame(
    executor: impl PgExecutor<'_>,
    time_frame: &TimeFrame,
) -> sqlx::Result<Vec<SupplyAtTime>> {
    match time_frame {
        TimeFrame::SinceBurn => {
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
        },
        TimeFrame::SinceMerge => {
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
            .bind(ltf.get_postgres_interval())
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
            .bind(ltf.get_postgres_interval())
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
            .bind(ltf.get_postgres_interval())
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
            .bind(ltf.get_postgres_interval())
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
        }
    }
}

#[derive(Serialize)]
struct SupplyOverTime {
    block_number: BlockNumber,
    d1: Vec<SupplyAtTime>,
    d30: Vec<SupplyAtTime>,
    d7: Vec<SupplyAtTime>,
    h1: Vec<SupplyAtTime>,
    m5: Vec<SupplyAtTime>,
    since_burn: Option<Vec<SupplyAtTime>>,
    since_merge: Vec<SupplyAtTime>,
    slot: Slot,
    timestamp: DateTime<Utc>,
}

pub async fn update_supply_over_time(
    executor: &PgPool,
    slot: Slot,
    block_number: BlockNumber,
    timestamp: DateTime<Utc>,
) -> Result<()> {
    debug!("updating supply over time");

    let (d1, d30, d7, h1, m5, since_merge, since_burn) = try_join!(
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Day1)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Day30)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Day7)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Hour1)),
        get_supply_over_time_time_frame(executor, &TimeFrame::Limited(Minute5)),
        get_supply_over_time_time_frame(executor, &TimeFrame::SinceMerge),
        get_supply_over_time_time_frame(executor, &TimeFrame::SinceBurn)
    )?;

    let supply_over_time = SupplyOverTime {
        block_number,
        d1,
        d30,
        d7,
        h1,
        m5,
        since_burn: Some(since_burn),
        since_merge,
        slot,
        timestamp,
    };

    caching::set_value(executor, &CacheKey::SupplyOverTime, supply_over_time).await?;

    caching::publish_cache_update(executor, CacheKey::SupplyOverTime).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, DurationRound, SubsecRound};
    use sqlx::{Acquire, PgConnection};

    use crate::{
        beacon_chain::{self, beacon_time, BeaconBalancesSum, BeaconDepositsSum},
        db,
        eth_supply::{self, EthSupplyParts},
        eth_units::{EthF64, GweiNewtype},
        execution_chain::{BlockStore, ExecutionBalancesSum, ExecutionNodeBlock},
    };

    use super::*;

    // Replace with shared testing helper that helps easily build the right mock block.
    fn make_test_block() -> ExecutionNodeBlock {
        ExecutionNodeBlock {
            base_fee_per_gas: 0,
            difficulty: 0,
            gas_used: 0,
            hash: "0xtest".to_string(),
            number: 0,
            parent_hash: "0xparent".to_string(),
            timestamp: Utc::now().trunc_subsecs(0),
            total_difficulty: 10,
        }
    }

    async fn store_test_eth_supply(
        executor: &mut PgConnection,
        slot: &Slot,
        eth_supply: EthF64,
    ) -> Result<()> {
        let mut block_store = BlockStore::new(executor);

        let test_block = make_test_block();

        block_store.store_block(&test_block, 0.0).await;

        beacon_chain::store_state(executor.acquire().await.unwrap(), "0xstate_root", slot).await?;

        let execution_balances_sum = ExecutionBalancesSum {
            block_number: 0,
            balances_sum: GweiNewtype::from_eth_f64(eth_supply).into_wei(),
        };
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: GweiNewtype(0),
            slot: *slot,
        };
        let beacon_deposits_sum = BeaconDepositsSum {
            slot: *slot,
            deposits_sum: GweiNewtype(0),
        };

        let eth_supply_parts = EthSupplyParts {
            beacon_balances_sum,
            beacon_deposits_sum,
            execution_balances_sum,
        };

        eth_supply::store(executor, &eth_supply_parts).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn selects_within_time_frame_test() {}

    #[ignore = "broken for unclear reasons, broken eth price?"]
    #[tokio::test]
    async fn m5_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_timestamp = Utc::now().trunc_subsecs(0) - Duration::minutes(2);
        let test_slot = beacon_time::get_slot_from_date_time(&test_timestamp);

        let test_supply_at_time = SupplyAtTime {
            timestamp: test_timestamp.duration_trunc(Duration::hours(1))?,
            supply: 10.0,
            slot: None,
        };

        store_test_eth_supply(&mut transaction, &test_slot, 10.0).await?;

        let since_merge =
            get_supply_over_time_time_frame(&mut transaction, &TimeFrame::SinceMerge).await?;

        assert_eq!(since_merge, vec![test_supply_at_time]);

        Ok(())
    }
}
