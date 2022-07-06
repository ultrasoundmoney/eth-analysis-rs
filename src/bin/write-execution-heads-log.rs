#[tokio::main]
pub async fn main() {
    eth_analysis::write_execution_heads_log().await
}
