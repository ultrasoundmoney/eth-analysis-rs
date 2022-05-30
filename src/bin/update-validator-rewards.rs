#[tokio::main]
pub async fn main() {
    env_logger::init();

    log::info!("updating validator rewards");

    eth_analysis::beacon_chain::update_validator_rewards()
        .await
        .map_or_else(
            |error| {
                log::error!("{}", error);
                log::error!("failed to update validator rewards");
            },
            |_| {
                log::info!("done updating validator rewards");
            },
        );
}
