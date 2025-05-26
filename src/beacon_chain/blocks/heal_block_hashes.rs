use futures::TryStreamExt;
use pit_wall::Progress;
use tracing::{debug, info};

use crate::{
    beacon_chain::{blocks, node::BeaconNodeHttp, BeaconNode, FIRST_POST_MERGE_SLOT},
    db, job_progress, key_value_store, log,
};

const HEAL_BLOCK_HASHES_KEY: &str = "heal-block-hashes";

pub async fn heal_block_hashes() {
    log::init();

    info!("healing execution block hashes");

    let db_pool = db::get_db_pool("heal-beacon-states", 1).await;
    let key_value_store = key_value_store::KeyValueStorePostgres::new(db_pool.clone());
    let job_progress = job_progress::JobProgress::new(HEAL_BLOCK_HASHES_KEY, &key_value_store);

    let beacon_node = BeaconNodeHttp::new_from_env();
    let first_slot = job_progress.get().await.unwrap_or(FIRST_POST_MERGE_SLOT);

    let work_todo = sqlx::query!(
        r#"
        SELECT
            COUNT(*) AS "count!"
        FROM
            beacon_blocks
        JOIN beacon_states ON
            beacon_blocks.state_root = beacon_states.state_root
        WHERE
            beacon_states.slot >= $1
        AND
            block_hash IS NULL
        "#,
        first_slot.0
    )
    .fetch_one(&db_pool)
    .await
    .unwrap();

    struct BlockSlotRow {
        block_root: String,
        slot: i32,
    }

    let mut rows = sqlx::query_as!(
        BlockSlotRow,
        r#"
        SELECT
            block_root,
            beacon_states.slot AS "slot!"
        FROM
            beacon_blocks
        JOIN beacon_states ON
            beacon_blocks.state_root = beacon_states.state_root
        WHERE
            beacon_states.slot >= $1
        "#,
        first_slot.0
    )
    .fetch(&db_pool);

    let mut progress = Progress::new("heal-block-hashes", work_todo.count.try_into().unwrap());

    while let Some(row) = rows.try_next().await.unwrap() {
        let block_root = row.block_root;
        let slot = row.slot;

        let block = beacon_node
            .get_block_by_block_root(&block_root)
            .await
            .unwrap()
            .expect("expect block to exist for historic block_root");

        let block_hash = block
            .body
            .execution_payload
            .expect("expect execution payload to exist for post-merge block")
            .block_hash;

        debug!(block_root, block_hash, "setting block hash");

        blocks::update_block_hash(&db_pool, &block_root, &block_hash).await;

        progress.inc_work_done();

        if slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
            job_progress.set(&slot.into()).await;
        }
    }

    info!("done healing beacon block hashes");
}
