use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgExecutor;
use tracing::debug;

pub async fn get_value(executor: impl PgExecutor<'_>, key: &str) -> sqlx::Result<Option<Value>> {
    debug!(key = key, "getting key value pair");

    let row = sqlx::query!(
        "
            SELECT value FROM key_value_store
            WHERE key = $1
        ",
        key,
    )
    .fetch_optional(executor)
    .await?;
    // .map(|row: PgRow| row.get::<Option<Value>, _>("value"))

    Ok(row.and_then(|row| row.value))
}

pub async fn get_deserializable_value<T>(
    executor: impl PgExecutor<'_>,
    key: &str,
) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let value = get_value(executor, key).await?;
    match value {
        None => Ok(None),
        Some(value) => {
            let decoded_value: T = serde_json::from_value::<T>(value)?;
            Ok(Some(decoded_value))
        }
    }
}

pub async fn set_serializable_value(
    executor: impl PgExecutor<'_>,
    key: &str,
    value: impl Serialize,
) -> Result<()> {
    let serialized = serde_json::to_value(value)?;
    set_value(executor, key, &serialized).await?;
    Ok(())
}

pub async fn set_value<'a>(executor: impl PgExecutor<'a>, key: &str, value: &Value) -> Result<()> {
    debug!("storing key: {}", &key,);

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
    .await?;

    Ok(())
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
            &mut transaction,
            "test-key",
            &serde_json::to_value(&test_json).unwrap(),
        )
        .await
        .unwrap();
        let value = get_value(&mut transaction, "test-key")
            .await
            .unwrap()
            .unwrap();
        let test_json_from_db = serde_json::from_value::<TestJson>(value).unwrap();

        assert_eq!(test_json_from_db, test_json)
    }

    #[tokio::test]
    async fn get_null_value_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        set_value(
            &mut transaction,
            "test-key",
            &serde_json::to_value(json!(None::<String>)).unwrap(),
        )
        .await
        .unwrap();

        let value = get_value(&mut transaction, "test-key")
            .await
            .unwrap()
            .unwrap();
        let test_json_from_db = serde_json::from_value::<Option<String>>(value).unwrap();

        assert_eq!(test_json_from_db, None)
    }

    #[tokio::test]
    async fn get_set_serializable_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        let test_json = TestJson {
            name: "alex".to_string(),
            age: 30,
        };

        set_serializable_value(&mut transaction, "test-key", test_json.clone())
            .await
            .unwrap();

        let retrieved_test_json =
            get_deserializable_value::<TestJson>(&mut transaction, "test-key")
                .await
                .unwrap()
                .unwrap();

        assert_eq!(retrieved_test_json, test_json);
    }
}
