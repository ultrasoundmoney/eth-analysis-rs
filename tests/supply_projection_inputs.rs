#[tokio::test]
async fn update_supply_projection_inputs() -> Result<(), anyhow::Error> {
    eth_analysis::update_supply_projection_inputs().await?;
    Ok(())
}
