use tracing::trace;

#[tokio::main]
pub async fn main() {
    eth_analysis::beacon_chain::update_validator_rewards().await;
}
