#[tokio::main]
pub async fn main() {
    eth_analysis::record_eth_price().await;
}
