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

    info!("backfilling beacon balances to london");

    let db_pool = db::get_db_pool("backfill-balances-to-london", 3).await;

    backfill_balances(&db_pool, &Granularity::Slot, &FIRST_POST_LONDON_SLOT).await;

    info!("done backfilling beacon balances to london");
}
