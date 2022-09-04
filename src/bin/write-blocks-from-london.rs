#[tokio::main]
pub async fn main() {
    eth_analysis::write_blocks_from_london().await
}
