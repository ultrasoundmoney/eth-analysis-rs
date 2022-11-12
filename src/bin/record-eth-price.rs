#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::record_eth_price().await?;
    Ok(())
}
