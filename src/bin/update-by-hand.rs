use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    eth_analysis::update_by_hand().await
}
