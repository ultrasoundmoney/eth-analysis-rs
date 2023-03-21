#[tokio::main]
pub async fn main() {
    eth_analysis::export_thousandth_epoch_supply().await
}
