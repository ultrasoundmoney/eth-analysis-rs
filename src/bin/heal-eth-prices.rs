#[tokio::main]
pub async fn main() {
    eth_analysis::heal_eth_prices().await
}
