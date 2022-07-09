mod balances;
mod beacon_time;
mod blocks;
mod deposits;
mod issuance;
mod node;
mod rewards;
mod states;
mod sync;
mod total_supply;

use sqlx::postgres::PgPoolOptions;

pub use self::balances::get_validator_balances_by_start_of_day;
pub use self::issuance::get_current_issuance;
pub use self::issuance::get_issuance_by_start_of_day;
pub use self::sync::SyncError;
pub use self::rewards::update_validator_rewards;
pub use self::total_supply::get_latest_total_supply;

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

    sync::sync_beacon_states(&pool, &node_client).await.unwrap();
}
