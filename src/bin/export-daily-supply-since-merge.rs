#[tokio::main]
pub async fn main() {
    eth_analysis::export_daily_supply_since_merge().await;
}
