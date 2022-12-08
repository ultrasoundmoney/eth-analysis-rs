use anyhow::Result;
use futures::TryStreamExt;
use pit_wall::Progress;
use sqlx::{postgres::PgPoolOptions, PgExecutor};
use tracing::{debug, info};

use crate::{
    beacon_chain::{blocks, BeaconNode, Slot, FIRST_POST_MERGE_SLOT},
    db, key_value_store, log,
};

const HEAL_BLOCK_HASHES_KEY: &str = "heal-block-hashes";

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
    let first_slot = get_last_checked(&db_pool)
        .await?
        .unwrap_or(FIRST_POST_MERGE_SLOT);

    let work_todo = sqlx::query!(
        r#"
            SELECT
                COUNT(*) AS "count!"
            FROM
                beacon_blocks
            JOIN beacon_states ON
                beacon_blocks.state_root = beacon_states.state_root
            WHERE
                slot >= $1
            AND
                block_hash IS NULL
        "#,
        first_slot.0
    )
    .fetch_one(&db_pool)
    .await?;

    struct BlockSlotRow {
        block_root: String,
        slot: i32,
    }

    let mut rows = sqlx::query_as!(
        BlockSlotRow,
        r#"
            SELECT
                block_root,
                slot AS "slot!"
            FROM
                beacon_blocks
            JOIN beacon_states ON
                beacon_blocks.state_root = beacon_states.state_root
            WHERE
                slot >= $1
        "#,
        first_slot.0
    )
    .fetch(&db_pool);

    let mut progress = Progress::new("heal-block-hashes", work_todo.count.try_into().unwrap());

    while let Some(row) = rows.try_next().await? {
        let block_root = row.block_root;
        let slot = row.slot;

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

        blocks::update_block_hash(&db_pool, &block_root, &block_hash).await?;

        progress.inc_work_done();

        if slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
            store_last_checked(&db_pool, &slot.into()).await?;
        }
    }

    info!("done healing beacon block hashes");

    Ok(())
}
