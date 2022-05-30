mod balances;
mod beacon_time;
mod blocks;
mod deposits;
mod issuance;
pub mod node;
pub mod rewards;
mod states;
mod sync;

use sqlx::postgres::PgPoolOptions;

use crate::config;

pub use self::sync::SyncError;

pub async fn sync_beacon_states() -> Result<(), SyncError> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let node_client = reqwest::Client::new();

    sync::sync_beacon_states(&pool, &node_client).await
}

pub async fn update_validator_rewards() -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let node_client = reqwest::Client::new();

    rewards::update_validator_rewards(&pool, &node_client).await
}
