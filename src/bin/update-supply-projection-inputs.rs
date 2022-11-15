#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::update_supply_projection_inputs().await?;
    Ok(())
}
