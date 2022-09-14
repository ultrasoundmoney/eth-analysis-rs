use sqlx::Connection;
use sqlx::PgConnection;

use crate::config;

pub async fn get_test_db() -> PgConnection {
    let url = config::get_db_url();

    if !url.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    Connection::connect(&url).await.unwrap()
}

pub fn get_test_db_url() -> String {
    let url = config::get_db_url();
    if !url.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    url
}
