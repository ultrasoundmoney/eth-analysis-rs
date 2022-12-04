#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::backfill_daily_balances_to_london().await
}
