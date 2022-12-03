#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::backfill_balances_to_london().await
}
