#[tokio::main]
pub async fn main() {
    eth_analysis::update_supply_projection_inputs().await;
}
