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
    use chrono::Utc;
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
        pool: PgPool,
        name: String,
    }

    impl TestDb {
        pub async fn new() -> Self {
            let name = format!("testdb_{}", Utc::now().timestamp_millis());

            let mut connection = get_test_db_connection().await;
            sqlx::query(&format!("CREATE DATABASE {}", name))
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

        pub fn pool(&self) -> &PgPool {
            &self.pool
        }
    }

    #[cfg(test)]
    impl Drop for TestDb {
        fn drop(&mut self) {
            let db_name = self.name.clone();
            tokio::spawn(async move {
                let mut db_connection = get_test_db_connection().await;
                sqlx::query(&format!("DROP DATABASE {}", db_name))
                    .execute(&mut db_connection)
                    .await
                    .unwrap();
            });
        }
    }
}
