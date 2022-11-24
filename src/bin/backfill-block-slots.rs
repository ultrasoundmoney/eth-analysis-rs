#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    eth_analysis::backfill_historic_slots().await
}
