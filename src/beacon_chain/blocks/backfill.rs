use std::collections::HashMap;

use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, query_builder, Postgres, QueryBuilder};
use tracing::{debug, info};

use crate::{beacon_chain::Slot, db, log};

pub async fn backfill_historic_slots() -> Result<()> {
    log::init_with_env();

    info!("healing execution block hashes");

    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db::get_db_url_with_name("heal-beacon-states"))
        .await?;

    struct BlockWithoutSlot {
        block_root: String,
        state_root: String,
    }

    debug!("fetching blocks without slots");

    let blocks_without_slots = sqlx::query_as!(
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
    .fetch_all(&db_pool)
    .await?;

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

    debug!("updating blocks with slots");

    let mut query_builder: QueryBuilder<Postgres> =
        QueryBuilder::new("INSERT INTO beacon_blocks (block_root, slot) ");

    query_builder.push_values(block_root_slot_pairs, |mut b, tuple| {
        b.push_bind(tuple.0).push_bind(tuple.1);
    });

    query_builder.push(" ON CONFLICT (block_root) DO UPDATE SET slot = excluded.slot");

    let query = query_builder.build();

    query.execute(&db_pool).await?;

    Ok(())
}
