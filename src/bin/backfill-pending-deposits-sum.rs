use eth_analysis::{beacon_chain::backfill_pending_deposits_sum, db, log};
use tracing::info;

#[tokio::main]
pub async fn main() {
    log::init();

    info!("starting backfill process for missing pending_deposits_sum_gwei");

    let db_pool = db::get_db_pool("backfill-pending-deposits-sum", 3).await;

    backfill_pending_deposits_sum(&db_pool).await;

    info!("backfill process for missing pending_deposits_sum_gwei finished");
}
