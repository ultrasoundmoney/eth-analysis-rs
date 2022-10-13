use lazy_static::lazy_static;

use crate::env;

lazy_static! {
    pub static ref DB_URL: String = env::get_env_var_unsafe("DATABASE_URL");
}

pub fn get_db_url_with_name<'a>(name: &str) -> String {
    format!("{}?application_name={name}", *DB_URL)
}

#[cfg(test)]
pub async fn get_test_db() -> sqlx::PgConnection {
    use sqlx::Connection;

    if !DB_URL.contains("testdb") {
        panic!("tried to run tests against db that is not 'testdb'");
    }

    Connection::connect(&DB_URL).await.unwrap()
}
