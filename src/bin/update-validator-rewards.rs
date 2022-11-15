#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::update_validator_rewards().await?;
    Ok(())
}
