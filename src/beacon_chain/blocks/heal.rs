use std::collections::HashMap;

use anyhow::Result;
use pit_wall::Progress;
use sqlx::{postgres::PgPoolOptions, PgExecutor};
use tracing::{debug, info};

use crate::{
    beacon_chain::{self, blocks, node::BlockRoot, BeaconNode, Slot, FIRST_POST_MERGE_SLOT},
    db, key_value_store, log,
};

const HEAL_BLOCK_HASHES_KEY: &str = "heal-beacon-states";

async fn store_last_checked(executor: impl PgExecutor<'_>, slot: &Slot) -> Result<()> {
    key_value_store::set_serializable_value(executor, HEAL_BLOCK_HASHES_KEY, slot).await?;
    Ok(())
}

async fn get_last_checked(executor: impl PgExecutor<'_>) -> Result<Option<Slot>> {
    let slot =
        key_value_store::get_deserializable_value::<Slot>(executor, HEAL_BLOCK_HASHES_KEY).await?;
    Ok(slot)
}

pub async fn heal_block_hashes() -> Result<()> {
    log::init_with_env();

    info!("healing execution block hashes");

    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db::get_db_url_with_name("heal-beacon-states"))
        .await?;

    let beacon_node = BeaconNode::new();
    let last_slot = beacon_chain::get_last_state(&db_pool)
        .await
        .expect("a beacon state should be stored before trying to heal any")
        .slot;
    let first_slot = get_last_checked(&db_pool)
        .await?
        .unwrap_or(FIRST_POST_MERGE_SLOT);

    let slot_block_root_map = sqlx::query!(
        "
            SELECT
                block_root,
                slot
            FROM
                beacon_blocks
            JOIN
                beacon_states
            ON
                beacon_blocks.state_root = beacon_states.state_root
        "
    )
    .fetch_all(&db_pool)
    .await?
    .into_iter()
    .map(|row| (row.slot as u32, row.block_root))
    .collect::<HashMap<Slot, BlockRoot>>();

    let mut progress = Progress::new("heal-block-hash", (last_slot - first_slot).into());

    for slot in first_slot..=last_slot {
        let block_root = slot_block_root_map.get(&slot);
        match block_root {
            None => (),
            Some(block_root) => {
                let block = beacon_node
                    .get_block_by_block_root(&block_root)
                    .await?
                    .expect("expect block to exist for historic block_root");

                let block_hash = block
                    .body
                    .execution_payload
                    .expect("expect execution payload to exist for post-merge block")
                    .block_hash;

                debug!(block_root, block_hash, "setting block hash");

                blocks::update_block_hash(&db_pool, block_root, &block_hash).await?;
            }
        }

        progress.inc_work_done();

        if slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
            store_last_checked(&db_pool, &slot).await?;
        }
    }

    info!("done healing execution block hashes");

    Ok(())
}
