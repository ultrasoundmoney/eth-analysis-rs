use sqlx::{PgExecutor, Row};

#[allow(dead_code)]
pub async fn get_latest_block<'a>(executor: impl PgExecutor<'a>) -> Option<i32> {
    // sqlx::query!(
    //     "
    //         SELECT MAX(number) FROM execution_blocks
    //     "
    // )
    sqlx::query(
        "
            SELECT MAX(number) FROM execution_blocks
        ",
    )
    .fetch_one(executor)
    .await
    .unwrap()
    .get("max")
}
