#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::write_blocks_from_london().await?;
    Ok(())
}
