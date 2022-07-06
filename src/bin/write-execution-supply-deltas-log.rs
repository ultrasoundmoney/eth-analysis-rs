#[tokio::main]
pub async fn main() {
    eth_analysis::write_execution_supply_deltas_log().await
}
