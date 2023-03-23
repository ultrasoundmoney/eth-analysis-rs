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
    use async_trait::async_trait;
    use nanoid::nanoid;
    use sqlx::postgres::PgPoolOptions;
    use test_context::AsyncTestContext;

    use super::*;

    const ALPHABET: [char; 16] = [
        '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f',
    ];

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

    #[async_trait]
    impl AsyncTestContext for TestDb {
        async fn setup() -> TestDb {
            TestDb::new().await
        }

        async fn teardown(self) {
            self.pool.close().await;
            let mut connection = get_test_db_connection().await;
            sqlx::query(&format!("DROP DATABASE {}", self.name))
                .execute(&mut connection)
                .await
                .unwrap();
        }
    }

    impl TestDb {
        pub async fn new() -> Self {
            let name = format!("testdb_{}", nanoid!(10, &ALPHABET));

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
    }
}
