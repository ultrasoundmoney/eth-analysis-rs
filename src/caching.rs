use sqlx::PgExecutor;

pub async fn publish_cache_update<'a>(pool: impl PgExecutor<'a>, key: &str) {
    tracing::debug!("publishing cache update: {}", key);

    sqlx::query!(
        r#"
            SELECT pg_notify('cache-update', $1)
        "#,
        key
    )
    .execute(pool)
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;

    // This test fails sometimes because when run against the actual dev DB many
    // notifications fire on the "cache-update" channel. Needs a test DB to work reliably.
    #[tokio::test]
    async fn test_publish_cache_update() {
        let mut listener = sqlx::postgres::PgListener::connect(&config::get_db_url())
            .await
            .unwrap();
        listener.listen("cache-update").await.unwrap();

        let notification_future = async { listener.recv().await };

        let pool = sqlx::PgPool::connect(&config::get_db_url()).await.unwrap();

        let test_key = "test-key";
        publish_cache_update(&pool, test_key).await;

        let notification = notification_future.await.unwrap();

        assert_eq!(notification.payload(), test_key)
    }
}
