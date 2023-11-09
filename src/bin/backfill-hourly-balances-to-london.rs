use eth_analysis::{
    beacon_chain::{
        backfill::{backfill_balances, Granularity},
        FIRST_POST_LONDON_SLOT,
    },
    db, log,
};
use tracing::info;

#[tokio::main]
pub async fn main() {
    log::init_with_env();

    info!("backfilling hourly beacon balances");

    let db_pool = db::get_db_pool("backfill-hourly-balances-to-london", 3).await;

    backfill_balances(&db_pool, &Granularity::Hour, &FIRST_POST_LONDON_SLOT).await;

    info!("done backfilling hourly beacon balances to london");
}
