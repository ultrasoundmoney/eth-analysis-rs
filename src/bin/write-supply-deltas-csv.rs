use eth_analysis::write_supply_deltas_csv;
use std::error::Error;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    write_supply_deltas_csv().await
}
