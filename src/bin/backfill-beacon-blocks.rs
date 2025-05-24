use eth_analysis::{
    beacon_chain::{self},
    db, log,
};
use tracing::info;

#[tokio::main]
pub async fn main() {
    log::init();

    info!("starting beacon block backfill process");

    let db_pool = db::get_db_pool("backfill-beacon-blocks", 3).await;

    beacon_chain::blocks::backfill::backfill_blocks(&db_pool).await;

    info!("beacon block backfill process finished");
}
