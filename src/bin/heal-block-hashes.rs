#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::heal_block_hashes().await
}
