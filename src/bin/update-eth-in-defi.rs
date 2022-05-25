use eth_analysis::update_eth_in_defi;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

#[tokio::main]
pub async fn main() {
    env_logger::init();

    log::info!("analyzing eth in defi");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(PgConnectOptions::new())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    update_eth_in_defi(&pool).await;

    log::info!("done analyzing eth in defi");
}
