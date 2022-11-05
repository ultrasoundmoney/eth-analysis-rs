#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::sync_eth_supply_gaps().await
}
