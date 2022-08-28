use sqlx::Connection;
use sqlx::PgConnection;

use crate::config;

pub async fn get_test_db() -> PgConnection {
    if !config::get_db_url().contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    Connection::connect(&config::get_db_url()).await.unwrap()
}
