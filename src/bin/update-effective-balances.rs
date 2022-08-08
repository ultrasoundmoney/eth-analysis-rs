#[tokio::main]
pub async fn main() {
    eth_analysis::update_effective_balance_sum().await;
}
