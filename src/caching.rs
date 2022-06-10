use sqlx::PgExecutor;

pub async fn publish_cache_update<'a>(pool: impl PgExecutor<'a>, key: &str) {
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
