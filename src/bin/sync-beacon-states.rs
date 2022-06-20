#[tokio::main]
pub async fn main() {
    eth_analysis::sync_beacon_states().await;
}
