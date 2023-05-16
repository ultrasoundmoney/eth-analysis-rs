#[tokio::main]
pub async fn main() {
    eth_analysis::export_execution_supply_deltas().await
}
