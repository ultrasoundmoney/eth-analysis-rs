#[tokio::main]
pub async fn main() {
    eth_analysis::check_beacon_state_gaps().await.unwrap();
}
