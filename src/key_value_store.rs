use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, PgExecutor, Row};
use tracing::debug;

use crate::caching::CacheKey;

// Do we need a distinction between key/value pair isn't there and value is null?
pub async fn get_value<'a>(executor: impl PgExecutor<'a>, key: &str) -> Option<Value> {
    debug!(key = key, "getting key value pair");

    sqlx::query(
        "
            SELECT value FROM key_value_store
            WHERE key = $1
        ",
    )
    .bind(key)
    .map(|row: PgRow| row.get::<Option<Value>, _>("value"))
    .fetch_optional(executor)
    .await
    .unwrap()
    .flatten()
}

pub async fn get_raw_caching_value(
    executor: impl PgExecutor<'_>,
    cache_key: &CacheKey<'_>,
) -> Option<Value> {
    get_value(executor, cache_key.to_db_key()).await
}

#[allow(dead_code)]
pub async fn get_caching_value<T>(
    executor: impl PgExecutor<'_>,
    cache_key: &CacheKey<'_>,
) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    match get_value(executor, cache_key.to_db_key()).await {
        None => Ok(None),
        Some(value) => {
            let decoded_value: T = serde_json::from_value::<T>(value)?;
            Ok(Some(decoded_value))
        }
    }
}

pub async fn set_value<'a>(executor: impl PgExecutor<'a>, key: &str, value: &Value) {
    debug!("storing key: {}", &key,);

    sqlx::query(
        "
            INSERT INTO key_value_store (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value
        ",
    )
    .bind(key)
    .bind(value)
    .execute(executor)
    .await
    .unwrap();
}

pub async fn set_caching_value<'a>(
    executor: impl PgExecutor<'_>,
    cache_key: &CacheKey<'_>,
    value: impl Serialize,
) -> Result<()> {
    set_value(
        executor,
        cache_key.to_db_key(),
        &serde_json::to_value(value)?,
    )
    .await;
    Ok(())
}

pub async fn set_value_str<'a>(executor: impl PgExecutor<'a>, key: &str, value_str: &str) {
    debug!(key = key, "storing key value pair");

    sqlx::query(
        "
            INSERT INTO key_value_store (key, value) VALUES ($1, $2::jsonb)
            ON CONFLICT (key) DO UPDATE SET
                value = excluded.value
        ",
    )
    .bind(key)
    .bind(value_str)
    .execute(executor)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sqlx::Connection;

    use crate::db;

    use super::*;

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct TestJson {
        name: String,
        age: i32,
    }

    #[tokio::test]
    async fn get_set_value_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        set_value(
            &mut transaction,
            "test-key",
            &serde_json::to_value(&test_json).unwrap(),
        )
        .await;
        let value = get_value(&mut transaction, "test-key").await.unwrap();
        let test_json_from_db = serde_json::from_value::<TestJson>(value).unwrap();

        assert_eq!(test_json_from_db, test_json)
    }

    #[tokio::test]
    async fn get_null_value_test() {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        set_value(
            &mut transaction,
            "test-key",
            &serde_json::to_value(json!(None::<String>)).unwrap(),
        )
        .await;

        let test_json_from_db = serde_json::from_value::<Option<String>>(
            get_value(&mut transaction, "test-key").await.unwrap(),
        )
        .unwrap();

        assert_eq!(test_json_from_db, None)
    }

    #[tokio::test]
    async fn set_value_str_test() {
        let mut connection = db::get_test_db().await;
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

        set_value_str(&mut transaction, "test-key", &test_json_str).await;

        let test_json_from_db = serde_json::from_value::<TestJson>(
            get_value(&mut transaction, "test-key").await.unwrap(),
        )
        .unwrap();

        assert_eq!(test_json_from_db, test_json)
    }

    #[tokio::test]
    async fn get_raw_caching_value_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        set_caching_value(
            &mut transaction,
            &CacheKey::BaseFeePerGasStats,
            &serde_json::to_value(&test_json).unwrap(),
        )
        .await?;

        let raw_value: Value =
            get_raw_caching_value(&mut transaction, &CacheKey::BaseFeePerGasStats)
                .await
                .unwrap();

        assert_eq!(raw_value, serde_json::to_value(test_json).unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn get_set_caching_value_test() -> Result<()> {
        let mut connection = db::get_test_db().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            age: 29,
            name: "alex".to_string(),
        };

        set_caching_value(
            &mut transaction,
            &CacheKey::BaseFeePerGasStats,
            test_json.clone(),
        )
        .await?;

        let caching_value =
            get_caching_value::<TestJson>(&mut transaction, &CacheKey::BaseFeePerGasStats)
                .await?
                .unwrap();

        assert_eq!(caching_value, test_json);

        Ok(())
    }
}
