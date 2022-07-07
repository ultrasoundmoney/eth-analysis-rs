#[tokio::main]
pub async fn main() {
    eth_analysis::update_total_supply().await;
}
