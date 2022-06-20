#[tokio::main]
pub async fn main() {
    eth_analysis::update_validator_rewards().await;
}
