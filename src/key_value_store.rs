use serde::Serialize;
use sqlx::{types::Json, Encode, PgPool, Postgres, Type};

pub async fn store_value<'a, A>(pool: &PgPool, key: &'a str, value: A)
where
    A: 'a + Send + Encode<'a, Postgres> + Type<Postgres> + Serialize,
{
    sqlx::query(
        "
            INSERT INTO key_value_store (key, value) VALUES ($1, $2)
        ",
    )
    .bind(key)
    .bind(Json(value))
    .execute(pool)
    .await
    .unwrap();
}
