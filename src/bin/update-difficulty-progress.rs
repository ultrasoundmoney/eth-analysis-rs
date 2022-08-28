#[tokio::main]
pub async fn main() {
    eth_analysis::update_difficulty_progress().await;
}
