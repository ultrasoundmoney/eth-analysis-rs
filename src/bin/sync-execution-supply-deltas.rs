#[tokio::main]
pub async fn main() {
    eth_analysis::sync_execution_supply_deltas().await
}
