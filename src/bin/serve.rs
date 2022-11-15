#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::start_server().await?;
    Ok(())
}
