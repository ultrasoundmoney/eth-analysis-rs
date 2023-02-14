#[tokio::main]
pub async fn main() {
    eth_analysis::start_server().await.unwrap();
}
