#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    eth_analysis::check_blocks_gaps().await?;
    Ok(())
}
