use crate::eth_units::Wei;
use crate::time_frames::LimitedTimeFrame;
use crate::time_frames::TimeFrame;
use chrono::Utc;
use futures::join;
use sqlx::postgres::types::PgInterval;
use sqlx::PgExecutor;
use sqlx::PgPool;

#[derive(Debug, PartialEq)]
pub struct BurnTotals {
    pub all: Wei,
    d1: Wei,
    d30: Wei,
    d7: Wei,
    h1: Wei,
    m5: Wei,
}

impl BurnTotals {
    pub fn get_by_limited_time_frame(&self, limited_time_frame: &LimitedTimeFrame) -> Wei {
        match limited_time_frame {
            LimitedTimeFrame::Day1 => self.d1,
            LimitedTimeFrame::Day7 => self.d7,
            LimitedTimeFrame::Day30 => self.d30,
            LimitedTimeFrame::Minute5 => self.m5,
            LimitedTimeFrame::Hour1 => self.h1,
        }
    }

    fn get_by_time_frame(&self, time_frame: &TimeFrame) -> Wei {
        match time_frame {
            TimeFrame::All => self.all,
            TimeFrame::LimitedTimeFrame(limited_time_frame) => {
                self.get_by_limited_time_frame(&limited_time_frame)
            }
        }
    }
}

async fn get_burn_total_all<'a>(executor: impl PgExecutor<'a>) -> Wei {
    sqlx::query!(
        "
            SELECT
                SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78))::TEXT AS wei
            FROM
                blocks
        "
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .wei
    .map_or(0, |wei| wei.parse::<i128>().unwrap())
}

async fn get_burn_total_limited_time_frame<'a>(
    executor: impl PgExecutor<'a>,
    limited_time_frame: LimitedTimeFrame,
) -> Result<Wei> {
    let row = sqlx::query!(
        "
            SELECT
                SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78))::TEXT AS wei
            FROM
                blocks
            WHERE
                mined_at >= NOW() - $1::interval
        ",
        Into::<PgInterval>::into(limited_time_frame)
    )
    .fetch_optional(executor)
    .await?;

    let wei = row.map_or(0, |row| row.wei.parse::<i128>().unwrap());

    Ok(wei)
}

pub async fn get_burn_totals(db_pool: &PgPool) -> BurnTotals {
    let (m5, h1, d1, d7, d30, all) = join!(
        get_burn_total_limited_time_frame(db_pool, LimitedTimeFrame::Minute5),
        get_burn_total_limited_time_frame(db_pool, LimitedTimeFrame::Hour1),
        get_burn_total_limited_time_frame(db_pool, LimitedTimeFrame::Day1),
        get_burn_total_limited_time_frame(db_pool, LimitedTimeFrame::Day7),
        get_burn_total_limited_time_frame(db_pool, LimitedTimeFrame::Day30),
        get_burn_total_all(db_pool)
    );

    BurnTotals {
        all,
        d1,
        d30,
        d7,
        h1,
        m5,
    }
}

/// Only used for testing.
#[allow(dead_code)]
async fn insert_block<'a>(executor: impl PgExecutor<'a>, gas_used: i64, base_fee_per_gas: Wei) {
    sqlx::query!(
        "
            INSERT INTO
                blocks
            (
                base_fee_per_gas,
                gas_used,
                hash,
                mined_at,
                number
            )
            VALUES
            (
                $1,
                $2,
                $3,
                $4,
                $5
            )
        ",
        base_fee_per_gas as i64,
        gas_used,
        "0xtest",
        Utc::now(),
        0,
    )
    .execute(executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use sqlx::Connection;
    use sqlx::PgConnection;

    use super::*;

    #[tokio::test]
    async fn burn_total_all_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        insert_block(&mut transaction, 10, 10).await;

        let burn_total_all = get_burn_total_all(&mut transaction).await;

        assert_eq!(burn_total_all, 100);
    }

    #[tokio::test]
    async fn burn_total_limited_time_frame_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        insert_block(&mut transaction, 10, 10).await;

        let burn_total_m5 =
            get_burn_total_limited_time_frame(&mut transaction, LimitedTimeFrame::Minute5).await;

        assert_eq!(burn_total_m5, 100);
    }
}
