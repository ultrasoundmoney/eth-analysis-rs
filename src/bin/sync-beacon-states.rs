#[tokio::main]
pub async fn main() {
    eth_analysis::beacon_chain::sync_beacon_states().await;
}
