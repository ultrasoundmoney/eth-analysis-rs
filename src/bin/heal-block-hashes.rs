#[tokio::main]
pub async fn main() {
    eth_analysis::heal_block_hashes().await;
}
