use chrono::Utc;
use eth_analysis::{
    beacon_chain::{
        backfill::{backfill_balances, Granularity},
        FIRST_POST_LONDON_SLOT,
    },
    db,
    execution_chain::LONDON_HARD_FORK_TIMESTAMP,
    log,
};
use tracing::info;

#[tokio::main]
pub async fn main() {
    log::init_with_env();

    info!("backfilling hourly beacon balances");

    let db_pool = db::get_db_pool("backfill-hourly-balances").await;

    let days_since_merge = (Utc::now() - *LONDON_HARD_FORK_TIMESTAMP).num_days();

    backfill_balances(
        &db_pool,
        days_since_merge.try_into().unwrap(),
        Granularity::Day,
        &FIRST_POST_LONDON_SLOT,
    )
    .await
    .unwrap();

    info!("done backfilling hourly beacon balances");
}
