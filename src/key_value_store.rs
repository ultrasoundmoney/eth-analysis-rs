use std::fmt::Debug;

use serde::Serialize;
use serde_json::Value;
use sqlx::{PgExecutor, PgPool};

#[allow(dead_code)]
struct KeyValueFromDb {
    value: Option<Value>,
}

// Do we need a distinction between key/value pair isn't there and value is null?
#[allow(dead_code)]
pub async fn get_value(pool: &PgPool, key: &str) -> Option<Value> {
    sqlx::query_as!(
        KeyValueFromDb,
        r#"
            SELECT value FROM key_value_store
            WHERE key = $1
        "#,
        key
    )
    .fetch_one(pool)
    .await
    .unwrap()
    .value
}

#[derive(Debug, Serialize)]
pub struct KeyValue<'a> {
    pub key: &'a str,
    pub value: Value,
}

pub async fn set_value<'a>(pg_executor: impl PgExecutor<'a>, key_value: KeyValue<'_>) {
    tracing::debug!(
        "storing key: {}, value: {:?}",
        &key_value.key,
        &key_value.value
    );

    sqlx::query!(
        "
            INSERT INTO key_value_store (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value
        ",
        key_value.key,
        key_value.value
    )
    .execute(pg_executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::config;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct TestJson {
        name: String,
        age: i32,
    }

    #[tokio::test]
    async fn test_get_set_value() {
        let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();
        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        set_value(
            &pool,
            KeyValue {
                key: "test-key",
                value: serde_json::to_value(&test_json).unwrap(),
            },
        )
        .await;

        let test_json_from_db =
            serde_json::from_value::<TestJson>(get_value(&pool, "test-key").await.unwrap())
                .unwrap();

        assert_eq!(test_json_from_db, test_json)
    }
}
