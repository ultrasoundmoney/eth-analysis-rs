#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::fill_eth_supply_gaps().await
}
