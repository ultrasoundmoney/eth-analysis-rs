#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::update_effective_balance_sum().await?;
    Ok(())
}
