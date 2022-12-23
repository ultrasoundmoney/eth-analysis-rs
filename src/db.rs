use lazy_static::lazy_static;
use sqlx::PgPool;

use crate::env;

lazy_static! {
    pub static ref DB_URL: String = env::get_env_var_unsafe("DATABASE_URL");
}

pub fn get_db_url_with_name(name: &str) -> String {
    format!("{}?application_name={name}", *DB_URL)
}

pub async fn get_db_pool(name: &str) -> PgPool {
    PgPool::connect(&get_db_url_with_name(name))
        .await
        .expect("expect DB to be available to connect")
}

#[cfg(test)]
pub async fn get_test_db() -> sqlx::PgConnection {
    use sqlx::Connection;

    if !DB_URL.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    Connection::connect(&DB_URL).await.unwrap()
}

#[cfg(test)]
pub fn get_test_db_url() -> String {
    let url = get_db_url_with_name("testing");
    if !url.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    url
}
