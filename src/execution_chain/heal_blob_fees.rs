//! heals incorrectly calculated blob_base_fee values in blocks_next.
//!
//! the blob_base_fee calculation changed at the pectra fork (block 22,431,084) due to a new
//! blob_update_fraction value. blocks stored before the fix have incorrect blob_base_fee values.
//!
//! ## derivative data
//!
//! the following data is affected by bad blob_base_fee values:
//!
//! - `burn_sums` table: stores sums computed from `blob_base_fee * blob_gas_used`. this table
//!   auto-heals as entries older than ~100 blocks are deleted. the cached burn_sums are only used
//!   for rollbacks to a block number before the heal completed, which are exceedingly rare.
//!
//! - cached data (burn_rates, gauges, blob_stats, base_fee_over_time): computed at query time from
//!   blocks_next, will return correct values after this heal completes.

use anyhow::Result;
use chrono::{DateTime, Utc};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, trace};

use super::block_store::blob_base_fee_for_db;
use super::BlockNumber;

struct BlockRow {
    number: i32,
    timestamp: DateTime<Utc>,
    excess_blob_gas: i32,
    blob_base_fee: Option<i64>,
}

async fn get_blocks_with_blob_gas_from(
    pool: &PgPool,
    start_block: BlockNumber,
) -> Result<Vec<BlockRow>> {
    let rows = sqlx::query_as!(
        BlockRow,
        r#"
        SELECT
            number,
            timestamp,
            excess_blob_gas AS "excess_blob_gas!",
            blob_base_fee
        FROM
            blocks_next
        WHERE
            number >= $1
            AND excess_blob_gas IS NOT NULL
        ORDER BY
            number ASC
        "#,
        start_block
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

async fn update_blob_base_fee(
    pool: &PgPool,
    block_number: BlockNumber,
    blob_base_fee: i64,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE blocks_next
        SET blob_base_fee = $1
        WHERE number = $2
        "#,
        blob_base_fee,
        block_number
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// heals blob_base_fee values in blocks_next starting from the given block number.
///
/// this recalculates blob_base_fee for all blocks with excess_blob_gas >= start_block
/// using the corrected calculation that accounts for the pectra fork.
pub async fn heal_blob_fees(pool: &PgPool, start_block: BlockNumber) -> Result<()> {
    info!(%start_block, "starting blob fees healing");

    let blocks = get_blocks_with_blob_gas_from(pool, start_block).await?;

    if blocks.is_empty() {
        info!("no blocks with blob gas found from start_block, nothing to heal");
        return Ok(());
    }

    let total = blocks.len() as u64;
    let mut progress = Progress::new("heal-blob-fees", total);

    info!(total_blocks = %total, "found blocks with blob gas to heal");

    for block in blocks {
        let new_blob_base_fee: i64 =
            blob_base_fee_for_db(Some(block.excess_blob_gas), block.timestamp)
                .expect("excess_blob_gas is Some, so blob_base_fee should be Some");

        if block.blob_base_fee == Some(new_blob_base_fee) {
            trace!(
                block_number = %block.number,
                excess_blob_gas = %block.excess_blob_gas,
                current_blob_base_fee = ?block.blob_base_fee,
                "blob_base_fee already correct, skipping update"
            );
        } else {
            debug!(
                block_number = %block.number,
                excess_blob_gas = %block.excess_blob_gas,
                current_blob_base_fee = ?block.blob_base_fee,
                new_blob_base_fee = %new_blob_base_fee,
                "updating blob_base_fee"
            );

            update_blob_base_fee(pool, block.number, new_blob_base_fee).await?;
        }

        progress.inc_work_done();

        if progress.work_done.is_multiple_of(1000) || progress.work_done == total {
            info!("{}", progress.get_progress_string());
        }
    }

    info!("blob fees healing finished");
    Ok(())
}
