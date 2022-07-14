use std::fmt::Debug;

use serde::Serialize;
use serde_json::Value;
use sqlx::PgExecutor;

#[allow(dead_code)]
struct KeyValueFromDb {
    value: Option<Value>,
}

// Do we need a distinction between key/value pair isn't there and value is null?
#[allow(dead_code)]
pub async fn get_value<'a>(executor: impl PgExecutor<'a>, key: &str) -> Option<Value> {
    tracing::debug!("getting key: {}", key);

    sqlx::query_as!(
        KeyValueFromDb,
        r#"
            SELECT value FROM key_value_store
            WHERE key = $1
        "#,
        key
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .and_then(|kv| kv.value)
}

#[derive(Debug, Serialize)]
pub struct KeyValue<'a> {
    pub key: &'a str,
    pub value: Value,
}

pub async fn set_value<'a>(executor: impl PgExecutor<'a>, key_value: KeyValue<'_>) {
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
    .execute(executor)
    .await
    .unwrap();
}

pub struct KeyValueStr<'a> {
    pub key: &'a str,
    pub value_str: &'a str,
}

pub async fn set_value_str<'a>(executor: impl PgExecutor<'a>, key_value: KeyValueStr<'_>) {
    tracing::debug!(
        "storing key: {}, value: {:?}",
        &key_value.key,
        &key_value.value_str
    );

    sqlx::query(
        "
            INSERT INTO key_value_store (key, value) VALUES ($1, $2::jsonb)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value
        ",
    )
    .bind(key_value.key)
    .bind(key_value.value_str)
    .execute(executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;
    use serial_test::serial;
    use sqlx::{Connection, PgConnection};

    use crate::config;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize)]
    struct TestJson {
        name: String,
        age: i32,
    }

    #[tokio::test]
    #[serial]
    async fn get_set_value_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        set_value(
            &mut transaction,
            KeyValue {
                key: "test-key",
                value: serde_json::to_value(&test_json).unwrap(),
            },
        )
        .await;

        let test_json_from_db = serde_json::from_value::<TestJson>(
            get_value(&mut transaction, "test-key").await.unwrap(),
        )
        .unwrap();

        assert_eq!(test_json_from_db, test_json)
    }

    #[tokio::test]
    #[serial]
    async fn set_value_str_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        let test_json_str = serde_json::to_string(&json!({
            "name": "alex",
            "age": 29
        }))
        .unwrap();

        set_value_str(
            &mut transaction,
            KeyValueStr {
                key: "test-key",
                value_str: &test_json_str,
            },
        )
        .await;

        let test_json_from_db = serde_json::from_value::<TestJson>(
            get_value(&mut transaction, "test-key").await.unwrap(),
        )
        .unwrap();

        assert_eq!(test_json_from_db, test_json)
    }
}
