#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::update_issuance_breakdown().await?;
    Ok(())
}
