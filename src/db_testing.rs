use sqlx::Connection;
use sqlx::PgConnection;

use crate::db;

pub async fn get_test_db() -> PgConnection {
    let url = db::get_db_url_with_name("testing");

    if !url.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    Connection::connect(&url).await.unwrap()
}

pub fn get_test_db_url() -> String {
    let url = db::get_db_url_with_name("testing");
    if !url.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    url
}
