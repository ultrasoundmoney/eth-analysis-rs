//! # Burn Totals
//! This module sums total burn. The sums are of two kinds. One kind uses a limited time frame,
//! sliding through time, excluding and including blocks as the clock ticks. The other treats
//! growing time frames. Beginning at a past point in time and ending at the current time.
//!
//! ## Growing time frames
//! Get the last valid sum. If one exists, continue, otherwise, start from zero. Continue with
//! addition.
//!
//! ### GetLastValidSum
//! Check the hash of the last sum exists in the blocks table. If it does, return the sum, if it
//! does not, iterate backwards through the sums, dropping as we go, until one is found with a hash in the blocks
//! table. If none is found, report none exists.
//!
//! ### Addition
//! Gather the burn of each block after the last valid sum. Sum them iteratively and remember the
//! last one hundred sums calculated. Add them to the sums table.
//!
//! ## Limited time frames
//! Get the last valid sum. If one exists, continue, otherwise, start from zero. Continue with
//! expiration, then addition.
//!
//! ### GetLastValidSum
//! Check the hash of the last sum exists in the blocks table. If it does, return the sum, if it
//! does not, iterate backwards through the in-frame blocks, dropping as we go, whilst subtracting from the sum, until one is found with a hash in the blocks
//! table. If none is found, report none exists.
//!
//! ### Expiration
//! Iterate forward through the in-frame blocks, dropping as we go, whilst subtracting from the
//! sum, until one is found which is timestamped after NOW - TIME_FRAME_INTERVAL.
//!
//! ### Addition
//! Gather the burn of each block after the last valid sum. Remember each in-frame block, and
//! update the sum.
//!
//! ## Table schema
//! block_number,hash,block_burn,block_timestamp,sum,time_frame

use futures::join;
use sqlx::postgres::types::PgInterval;
use sqlx::PgExecutor;
use sqlx::PgPool;
use tracing::warn;

use crate::execution_chain::LONDON_HARD_FORK_BLOCK_NUMBER;
use crate::execution_chain::MERGE_BLOCK_NUMBER;
use crate::time_frames::GrowingTimeFrame;
use crate::time_frames::LimitedTimeFrame;
use crate::time_frames::TimeFrame;
use crate::units::EthNewtype;
use crate::units::WeiNewtype;

use GrowingTimeFrame::*;
use LimitedTimeFrame::*;
use TimeFrame::*;

#[derive(Debug, PartialEq)]
pub struct BurnTotals {
    pub since_merge: EthNewtype,
    pub since_burn: EthNewtype,
    pub d1: EthNewtype,
    pub d30: EthNewtype,
    pub d7: EthNewtype,
    pub h1: EthNewtype,
    pub m5: EthNewtype,
}

async fn get_burn_total_since_burn(executor: impl PgExecutor<'_>) -> WeiNewtype {
    warn!("getting burn total since burn is slow");
    sqlx::query!(
        "
            SELECT
                SUM(gas_used::NUMERIC(78) * base_fee_per_gas::NUMERIC(78))::TEXT AS wei_text
            FROM
                blocks
            WHERE
                number >= $1
        ",
        LONDON_HARD_FORK_BLOCK_NUMBER
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .wei_text
    .map_or_else(
        || {
            warn!("tried to sum burn since burn started but no blocks found");
            WeiNewtype(0)
        },
        |wei_text| wei_text.parse::<WeiNewtype>().unwrap(),
    )
}

async fn get_burn_total_since_merge(executor: impl PgExecutor<'_>) -> WeiNewtype {
    warn!("getting burn total since merge is slow");
    sqlx::query!(
        "
            SELECT
                SUM(gas_used::NUMERIC(78) * base_fee_per_gas::NUMERIC(78))::TEXT AS wei_text
            FROM
                blocks
            WHERE
                number >= $1
        ",
        MERGE_BLOCK_NUMBER
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .wei_text
    .map_or_else(
        || {
            warn!("tried to sum burn since merge started but no blocks found");
            WeiNewtype(0)
        },
        |wei_text| wei_text.parse::<WeiNewtype>().unwrap(),
    )
}

async fn get_burn_total_limited<'a>(
    executor: impl PgExecutor<'a>,
    limited_time_frame: &LimitedTimeFrame,
) -> WeiNewtype {
    sqlx::query!(
        "
            SELECT
                SUM(gas_used::NUMERIC(78) * base_fee_per_gas::NUMERIC(78))::TEXT AS wei_text
            FROM
                blocks
            WHERE
                mined_at >= NOW() - $1::INTERVAL
        ",
        Into::<PgInterval>::into(limited_time_frame)
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .wei_text
    .map_or_else(
        || {
            warn!(
                "tried to sum burn in limited time frame {limited_time_frame} but no blocks found",
            );
            WeiNewtype(0)
        },
        |wei_text| wei_text.parse::<WeiNewtype>().unwrap(),
    )
}

async fn get_burn_total(executor: impl PgExecutor<'_>, time_frame: &TimeFrame) -> EthNewtype {
    use TimeFrame::*;

    match time_frame {
        Limited(limited_time_frame) => get_burn_total_limited(executor, limited_time_frame).await,
        Growing(growing_time_frame) => match growing_time_frame {
            GrowingTimeFrame::SinceBurn => get_burn_total_since_burn(executor).await,
            GrowingTimeFrame::SinceMerge => get_burn_total_since_merge(executor).await,
        },
    }
    .into()
}

pub async fn get_burn_totals(db_pool: &PgPool) -> BurnTotals {
    let (m5, h1, d1, d7, d30, since_burn, since_merge) = join!(
        get_burn_total(db_pool, &Limited(Minute5)),
        get_burn_total(db_pool, &Limited(Hour1)),
        get_burn_total(db_pool, &Limited(Day1)),
        get_burn_total(db_pool, &Limited(Day7)),
        get_burn_total(db_pool, &Limited(Day30)),
        get_burn_total(db_pool, &Growing(SinceBurn)),
        get_burn_total(db_pool, &Growing(SinceMerge)),
    );

    BurnTotals {
        d1,
        d30,
        d7,
        h1,
        m5,
        since_burn,
        since_merge,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, Utc};
    use sqlx::Connection;

    use crate::db;

    use super::*;

    async fn insert_block<'a>(
        executor: impl PgExecutor<'a>,
        gas_used: i64,
        base_fee_per_gas: i64,
        number: i32,
        timestamp: DateTime<Utc>,
    ) {
        sqlx::query!(
            "
            INSERT INTO
                blocks (
                base_fee_per_gas,
                gas_used,
                hash,
                mined_at,
                number
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5
            )
        ",
            base_fee_per_gas,
            gas_used,
            "0xtest",
            timestamp,
            number,
        )
        .execute(executor)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn burn_total_since_burn_test() {
        let mut connection = db::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        insert_block(&mut transaction, 10, 10, 12_965_000, Utc::now()).await;

        let burn_total_since_burn = get_burn_total_since_burn(&mut transaction).await;

        assert_eq!(burn_total_since_burn, WeiNewtype(100));
    }

    #[tokio::test]
    async fn burn_total_since_merge_test() {
        let mut connection = db::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        insert_block(&mut transaction, 10, 10, MERGE_BLOCK_NUMBER, Utc::now()).await;

        let burn_total_since_merge = get_burn_total_since_merge(&mut transaction).await;

        assert_eq!(burn_total_since_merge, WeiNewtype(100));
    }

    #[tokio::test]
    async fn burn_total_limited_time_frame_test() {
        let mut connection = db::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        insert_block(
            &mut transaction,
            10,
            10,
            15_123_456,
            Utc::now() - Duration::minutes(4),
        )
        .await;

        let burn_total_m5 = get_burn_total_limited(&mut transaction, &Minute5).await;

        assert_eq!(burn_total_m5, WeiNewtype(100));
    }
}
