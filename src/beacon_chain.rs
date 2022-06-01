mod balances;
mod beacon_time;
mod blocks;
mod deposits;
mod issuance;
mod node;
mod rewards;
mod states;
mod sync;

use sqlx::postgres::PgPoolOptions;

use crate::config;

pub use self::balances::get_validator_balances_by_start_of_day;
pub use self::issuance::get_issuance_by_start_of_day;
pub use self::sync::SyncError;
pub use rewards::get_issuance_reward;

pub async fn sync_beacon_states() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing beacon states");

    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let node_client = reqwest::Client::new();

    sync::sync_beacon_states(&pool, &node_client)
        .await
        .map_or_else(
            |error| {
                tracing::error!("{}", error);
                tracing::error!("failed to sync beacon states");
            },
            |_| {
                tracing::info!("done syncing beacon states");
            },
        );
}

pub async fn update_validator_rewards() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating validator rewards");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let node_client = reqwest::Client::new();

    rewards::update_validator_rewards(&pool, &node_client)
        .await
        .map_or_else(
            |error| {
                tracing::error!("{}", error);
                tracing::error!("failed to update validator rewards");
            },
            |_| {
                tracing::info!("done updating validator rewards");
            },
        );
}
