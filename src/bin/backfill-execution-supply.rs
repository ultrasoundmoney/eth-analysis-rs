#[tokio::main]
pub async fn main() {
    eth_analysis::backfill_execution_supply().await;
}
