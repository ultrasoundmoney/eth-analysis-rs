#[tokio::main]
pub async fn main() {
    eth_analysis::write_execution_supply_delta_log().await
}
