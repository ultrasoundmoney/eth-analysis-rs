use eth_analysis::{analyze_states_loop, log};
use tracing::info;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    log::init_with_env();

    info!("analyzing on-chain ETH activity");

    analyze_states_loop().await
}
