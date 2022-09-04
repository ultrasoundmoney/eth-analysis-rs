#[tokio::main]
pub async fn main() {
    eth_analysis::resync_all().await
}
