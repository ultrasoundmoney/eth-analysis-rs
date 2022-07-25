use sqlx::{PgExecutor, Row};

// Execution chain blocks come in about once every 13s from genesis. With u32 our program
// would overflow when the block number passes 4_294_967_295. u32::MAX * 13 seconds = ~1769 years.
pub type BlockNumber = u32;

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
