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
pub mod tests {
    use std::sync::atomic::AtomicUsize;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    pub async fn get_test_db_connection() -> sqlx::PgConnection {
        use sqlx::Connection;

        if !DB_URL.contains("testdb") {
            panic!("tried to run tests against db that is not 'testdb'");
        }

        Connection::connect(&DB_URL).await.unwrap()
    }

    pub fn get_test_db_url() -> String {
        let url = get_db_url_with_name("testing");
        if !url.contains("testdb") {
            panic!("tried to run tests against db that is not 'testdb'");
        }

        url
    }

    pub struct TestDb {
        pub pool: PgPool,
        name: String,
    }

    static DB_ID_COUNTER: AtomicUsize = AtomicUsize::new(1);
    fn get_id() -> usize {
        DB_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    impl TestDb {
        pub async fn new() -> Self {
            let name = format!("testdb_{}", get_id());

            let mut connection = get_test_db_connection().await;
            sqlx::query(&format!("CREATE DATABASE {name}"))
                .execute(&mut connection)
                .await
                .unwrap();

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&DB_URL.to_string().replace("testdb", &name))
                .await
                .unwrap();

            sqlx::migrate!("./migrations").run(&pool).await.unwrap();

            Self { pool, name }
        }

        pub async fn cleanup(&self) {
            self.pool.close().await;
            let mut connection = get_test_db_connection().await;
            sqlx::query(&format!("DROP DATABASE {}", self.name))
                .execute(&mut connection)
                .await
                .unwrap();
        }
    }
}
