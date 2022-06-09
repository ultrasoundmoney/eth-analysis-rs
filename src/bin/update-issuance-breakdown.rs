#[tokio::main]
pub async fn main() {
    eth_analysis::issuance_breakdown::update_issuance_breakdown().await;
}
