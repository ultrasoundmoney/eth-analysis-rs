#[tokio::main]
pub async fn main() {
    eth_analysis::heal_beacon_states().await;
}
