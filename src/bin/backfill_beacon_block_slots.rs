use anyhow::Result;
use eth_analysis::beacon_chain::blocks::backfill_beacon_block_slots;
use eth_analysis::{db, log};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting beacon_blocks.slot backfill script");

    let db_pool = db::get_db_pool("backfill-beacon-block-slots", 1).await;
    backfill_beacon_block_slots(&db_pool).await?;

    info!("finished beacon_blocks.slot backfill script");

    Ok(())
}