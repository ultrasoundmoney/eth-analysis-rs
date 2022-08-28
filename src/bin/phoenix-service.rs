#[tokio::main]
pub async fn main() {
    eth_analysis::monitor_critical_services().await;
}
