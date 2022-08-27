use sqlx::postgres::PgConnectOptions;
use sqlx::ConnectOptions;
use sqlx::PgConnection;

pub async fn get_test_db() -> PgConnection {
    PgConnectOptions::new()
        .database("testdb")
        .connect()
        .await
        .unwrap()
}
