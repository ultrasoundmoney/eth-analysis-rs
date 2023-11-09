use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use pit_wall::Progress;
use sqlx::Row;
use tracing::{debug, error, info};

use crate::{db, execution_chain::ExecutionNode, log};

pub async fn check_blocks_gaps() -> Result<()> {
    log::init_with_env();

    info!("checking for gaps in blocks");

    let mut connection = db::get_db_connection("check-block-gaps").await;

    // Store blocks fetched, we run through them twice.
    let mut blocks = vec![];

    // Fast check by number.
    info!("fast checking stored block numbers for gaps");

    {
        let mut rows = sqlx::query(
            "
            SELECT number, hash FROM blocks
            ORDER BY number ASC
            ",
        )
        .fetch(&mut connection)
        .map(|row| {
            row.map(|row| {
                let number: i32 = row.get("number");
                let hash: String = row.get("hash");
                (number, hash)
            })
        });

        let mut last_number = None;
        while let Some((number, hash)) = rows.try_next().await? {
            blocks.push((number, hash));
            if let Some(last_number) = last_number {
                if last_number != number - 1 {
                    error!("last number: {last_number}, current number: {number}")
                }
            }

            last_number = Some(number);
        }

        info!("done checking blocks for gaps");
    }

    // Slow check on-chain by hash.
    info!("slow checking stored hashes against on-chain hashes");

    let mut progress = Progress::new("check on-chain hashes", blocks.len().try_into().unwrap());

    let execution_node = ExecutionNode::connect().await;

    for (number, stored_hash) in blocks {
        let on_chain = execution_node.get_block_by_number(&number).await.unwrap();

        if stored_hash != on_chain.hash {
            error!(stored_hash, on_chain.hash, "block mismatch",);
        }

        if progress.work_done % 100 == 0 && progress.work_done != 0 {
            debug!("{}", progress.get_progress_string());
        }

        progress.inc_work_done();
    }

    info!("done checking block hashes for mismatches");

    Ok(())
}
