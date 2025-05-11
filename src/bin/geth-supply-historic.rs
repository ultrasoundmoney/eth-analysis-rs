use anyhow::Result;
use eth_analysis::{env::ENV_CONFIG, geth_supply::historic, log};
use std::path::PathBuf;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    let data_dir = PathBuf::from(
        ENV_CONFIG
            .data_dir_geth_supply
            .as_ref()
            .expect("DATA_DIR_GETH_SUPPLY is required"),
    );
    info!(
        "Starting historic supply delta backfill from {:?}",
        data_dir
    );

    historic::backfill_historic_deltas(&data_dir).await?;

    Ok(())
}
