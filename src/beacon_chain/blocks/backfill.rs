use std::collections::HashMap;

use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{debug, info};

use crate::{db, log};

struct BlockWithoutSlot {
    block_root: String,
    state_root: String,
}

async fn get_next_blocks_without_slots(db_pool: &PgPool) -> sqlx::Result<Vec<BlockWithoutSlot>> {
    sqlx::query_as!(
        BlockWithoutSlot,
        "
            SELECT
                block_root,
                state_root
            FROM
                beacon_blocks
            WHERE
                slot IS NULL
            LIMIT 1000
        "
    )
    .fetch_all(db_pool)
    .await
}

pub async fn backfill_historic_slots() -> Result<()> {
    log::init_with_env();

    info!("healing execution block hashes");

    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db::get_db_url_with_name("heal-beacon-states"))
        .await?;

    debug!("fetching blocks without slots");
    let mut blocks_without_slots = get_next_blocks_without_slots(&db_pool).await?;

    while blocks_without_slots.len() != 0 {
        let state_roots = blocks_without_slots
            .iter()
            .map(|row| row.state_root.clone())
            .collect::<Vec<String>>();

        debug!("fetching slots for fetched blocks");

        let state_root_slot_map = sqlx::query!(
            "
                SELECT
                    slot,
                    state_root
                FROM
                    beacon_states
                WHERE state_root = ANY($1)
            ",
            &state_roots[..]
        )
        .fetch_all(&db_pool)
        .await?
        .into_iter()
        .map(|row| (row.state_root, row.slot))
        .collect::<HashMap<String, i32>>();

        let block_root_slot_pairs = blocks_without_slots
            .iter()
            .map(|block| {
                (
                    block.block_root.clone(),
                    state_root_slot_map.get(&block.state_root).unwrap().clone(),
                )
            })
            .collect::<Vec<(String, i32)>>();

        let (block_roots, block_root_slots): (Vec<String>, Vec<i32>) =
            block_root_slot_pairs.iter().cloned().unzip();

        debug!("updating blocks with slots");

        sqlx::query(
            "
                UPDATE
                    beacon_blocks
                SET
                    slot = data_table.slot
                FROM (
                    SELECT
                        UNNEST($1::TEXT[]) AS block_root,
                        UNNEST($2::INT8[]) AS slot
                ) AS data_table
                WHERE
                    beacon_blocks.block_root = data_table.block_root
            ",
        )
        .bind(&block_roots[..])
        .bind(&block_root_slots[..])
        .execute(&db_pool)
        .await?;

        debug!("getting next set of blocks without slots");
        blocks_without_slots = get_next_blocks_without_slots(&db_pool).await?;
    }

    Ok(())
}
