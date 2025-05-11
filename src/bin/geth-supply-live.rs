use anyhow::Result;
use eth_analysis::{env::ENV_CONFIG, geth_supply::live, log};
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
    let port = 3000;
    info!(
        "starting live supply delta API on port {} reading from {:?}",
        port, data_dir
    );

    live::start_live_api(data_dir, port).await?;

    Ok(())
}
