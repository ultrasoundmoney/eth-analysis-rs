use sqlx::{postgres::PgPoolOptions, Connection, Executor, PgConnection, PgPool};
use std::time::Duration;

use crate::env::ENV_CONFIG;

pub async fn get_db_pool(name: &str, max_connections: u32) -> PgPool {
    let name_query = format!("SET application_name = '{}';", name);
    PgPoolOptions::new()
        .after_connect(move |conn, _meta| {
            let name_query = name_query.clone();
            Box::pin(async move {
                conn.execute(name_query.as_ref()).await?;
                Ok(())
            })
        })
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&ENV_CONFIG.db_url)
        .await
        .expect("expect DB to be available to connect")
}

pub async fn get_db_connection(name: &str) -> sqlx::PgConnection {
    let mut conn = PgConnection::connect(ENV_CONFIG.db_url.as_str())
        .await
        .expect("expect DB to be available to connect");
    let query = format!("SET application_name = '{}'", name);
    sqlx::query(&query).execute(&mut conn).await.unwrap();
    conn
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
        if !ENV_CONFIG.db_url.contains("testdb") {
            panic!("tried to run tests against db that is not 'testdb'");
        }

        get_db_connection("testing").await
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
                .connect(&ENV_CONFIG.db_url.replace("testdb", &name))
                .await
                .unwrap();

            sqlx::migrate!("./migrations").run(&pool).await.unwrap();

            Self { pool, name }
        }
    }
}
