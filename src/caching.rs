use sqlx::PgPool;

pub async fn publish_cache_update(pool: &PgPool, key: &str) {
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
