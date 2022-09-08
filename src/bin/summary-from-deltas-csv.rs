#[tokio::main]
pub async fn main() {
    eth_analysis::summary_from_deltas_csv().await
}
