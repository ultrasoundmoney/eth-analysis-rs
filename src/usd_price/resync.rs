use chrono::{Duration, DurationRound, TimeZone, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgExecutor, Row};
use store::EthPriceStore;
use tracing::{debug, info};

use crate::{db, execution_chain, log};

use super::{bybit, store};

const RESYNC_ETH_PRICES_KEY: &str = "resync-eth-prices";

async fn get_last_synced_minute(executor: impl PgExecutor<'_>) -> Option<u32> {
    sqlx::query(
        "
        SELECT
            value
        FROM
            key_value_store
        WHERE
            key = $1
        ",
    )
    .bind(RESYNC_ETH_PRICES_KEY)
    .map(|row: PgRow| {
        let value: Value = row.get("value");
        serde_json::from_value::<u32>(value).unwrap()
    })
    .fetch_optional(executor)
    .await
    .unwrap()
}

async fn set_last_synced_minute(executor: impl PgExecutor<'_>, minute: u32) {
    sqlx::query(
        "
        INSERT INTO
            key_value_store (key, value)
        VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET
            value = excluded.value
        ",
    )
    .bind(RESYNC_ETH_PRICES_KEY)
    .bind(json!(minute))
    .execute(executor)
    .await
    .unwrap();
}

pub async fn resync_all() {
    log::init();

    let client = reqwest::Client::new();

    info!("resyncing all eth prices");
    let max_distance_in_minutes: i64 = std::env::args()
        .collect::<Vec<String>>()
        .get(1)
        .and_then(|str| str.parse::<i64>().ok())
        .unwrap_or(10);

    let db_pool = db::get_db_pool("resync-all-prices", 3).await;
    let eth_price_store = store::EthPriceStorePostgres::new(db_pool.clone());

    debug!("walking through all minutes since London hardfork");

    let duration_since_london = Utc::now().duration_round(Duration::minutes(1)).unwrap()
        - *execution_chain::LONDON_HARD_FORK_TIMESTAMP;
    let minutes_since_london: u32 = duration_since_london.num_minutes().try_into().unwrap();

    let london_minute_timestamp: u32 = execution_chain::LONDON_HARD_FORK_TIMESTAMP
        .duration_round(Duration::minutes(1))
        .unwrap()
        .timestamp()
        .try_into()
        .unwrap();

    let start_minute = get_last_synced_minute(&db_pool)
        .await
        .map_or(0, |minute| minute + 1);

    debug!(
        "starting at {}",
        Utc.timestamp_opt(london_minute_timestamp.into(), 0)
            .unwrap()
            + Duration::minutes((start_minute).into())
    );

    let mut progress = pit_wall::Progress::new(
        "resync eth prices",
        (minutes_since_london - start_minute).into(),
    );

    for minute_n in start_minute..minutes_since_london {
        let timestamp = london_minute_timestamp + minute_n * 60;
        let timestamp_date_time = Utc.timestamp_opt(timestamp.into(), 0).unwrap();

        let usd = bybit::get_closest_price_by_minute(
            &client,
            timestamp_date_time,
            Duration::minutes(max_distance_in_minutes),
        )
        .await;

        match usd {
            None => {
                debug!(
                    timestamp = timestamp_date_time.to_string(),
                    "no Bybit price available",
                );
            }
            Some(usd) => {
                eth_price_store.store_price(&timestamp_date_time, usd).await;
            }
        }

        progress.inc_work_done();

        // Every 100 minutes, store which minute we last resynced.
        if minute_n != 0 && minute_n % 100 == 0 {
            debug!(
                timestamp = timestamp_date_time.to_string(),
                "100 minutes synced, checkpointing"
            );
            set_last_synced_minute(&db_pool, minute_n).await;

            info!("{}", progress.get_progress_string());
        };
    }
}

#[cfg(test)]
mod tests {
    use test_context::test_context;

    use crate::db::tests::TestDb;

    use super::*;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn get_set_last_synced_minute_test(test_db: &TestDb) {
        set_last_synced_minute(&test_db.pool, 1559).await;

        let minute = get_last_synced_minute(&test_db.pool).await;
        assert_eq!(minute, Some(1559));
    }
}
