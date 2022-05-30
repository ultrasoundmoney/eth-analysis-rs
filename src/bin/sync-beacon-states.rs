use eth_analysis::beacon_chain::SyncError;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    log::info!("syncing beacon states");

    eth_analysis::beacon_chain::sync_beacon_states()
        .await
        .map_or_else(
            |error| {
                match error {
                    SyncError::SqlxError(error) => {
                        log::error!("{}", error);
                    }
                    SyncError::ReqwestError(error) => {
                        log::error!("{}", error);
                    }
                };
                log::error!("failed to sync beacon states");
            },
            |_| {
                log::info!("done syncing beacon states");
            },
        );
}
