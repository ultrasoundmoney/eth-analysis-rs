use eth_analysis::{beacon_chain::SyncError, config};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    log::info!("syncing beacon states");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    eth_analysis::beacon_chain::sync(&pool).await.map_or_else(
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
