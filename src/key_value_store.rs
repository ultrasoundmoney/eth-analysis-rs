use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgExecutor, PgPool};
use tracing::trace;

pub async fn get_value(executor: impl PgExecutor<'_>, key: &str) -> Option<Value> {
    trace!(key = key, "getting key value pair");

    sqlx::query!(
        "
        SELECT value FROM key_value_store
        WHERE key = $1
        ",
        key,
    )
    .fetch_optional(executor)
    .await
    .unwrap()
    .and_then(|row| row.value)
}

pub async fn set_value(executor: impl PgExecutor<'_>, key: &str, value: &Value) {
    trace!("storing key: {}", &key,);

    sqlx::query!(
        "
        INSERT INTO key_value_store (key, value) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET
            value = excluded.value
        ",
        key,
        value
    )
    .execute(executor)
    .await
    .unwrap();
}

#[async_trait]
pub trait KeyValueStore {
    async fn get_value(&self, key: &str) -> Option<Value>;
    async fn set_value(&self, key: &str, value: &Value);
}

pub struct KeyValueStorePostgres {
    db_pool: PgPool,
}

impl KeyValueStorePostgres {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
    }

    pub async fn get_deserializable_value<D: for<'de> Deserialize<'de>>(
        &self,
        key: &str,
    ) -> Option<D> {
        get_value(&self.db_pool, key)
            .await
            .and_then(|value| serde_json::from_value(value).ok())
    }

    pub async fn set_serializable_value<S: Serialize>(&self, key: &str, value: S) {
        set_value(
            &self.db_pool,
            key,
            &serde_json::to_value(value).expect("expect value to be serializable"),
        )
        .await
    }
}

#[async_trait]
impl KeyValueStore for KeyValueStorePostgres {
    async fn get_value(&self, key: &str) -> Option<Value> {
        get_value(&self.db_pool, key).await
    }

    async fn set_value(&self, key: &str, value: &Value) {
        set_value(&self.db_pool, key, value).await
    }
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
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 29,
        };

        set_value(
            &mut *transaction,
            "test-key",
            &serde_json::to_value(&test_json).unwrap(),
        )
        .await;
        let value = get_value(&mut *transaction, "test-key").await.unwrap();
        let test_json_from_db = serde_json::from_value::<TestJson>(value).unwrap();

        assert_eq!(test_json_from_db, test_json)
    }

    #[tokio::test]
    async fn get_null_value_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        set_value(
            &mut *transaction,
            "test-key",
            &serde_json::to_value(json!(None::<String>)).unwrap(),
        )
        .await;

        let value = get_value(&mut *transaction, "test-key").await.unwrap();
        let test_json_from_db = serde_json::from_value::<Option<String>>(value).unwrap();

        assert_eq!(test_json_from_db, None)
    }

    #[tokio::test]
    async fn test_set_and_get_value() {
        let test_db = db::tests::TestDb::new().await;
        let store = KeyValueStorePostgres::new(test_db.pool.clone());

        // Set a value for a key
        let key = "test_key";
        let value = "test_value";
        store.set_serializable_value(key, value).await;

        // Get the value for the key and deserialize it
        let retrieved_value: Option<String> = store.get_deserializable_value(key).await;

        // Assert that the value retrieved is the same as the one set
        assert_eq!(retrieved_value, Some(value.to_owned()));
    }

    #[tokio::test]
    async fn test_get_nonexistent_value() {
        let test_db = db::tests::TestDb::new().await;
        let store = KeyValueStorePostgres::new(test_db.pool.clone());

        // Get the value for a nonexistent key
        let key = "nonexistent_key";
        let retrieved_value: Option<String> = store.get_deserializable_value(key).await;

        // Assert that the retrieved value is None
        assert_eq!(retrieved_value, None);
    }
}
