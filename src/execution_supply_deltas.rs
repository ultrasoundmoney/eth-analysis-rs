use futures::prelude::*;
use sqlx::postgres::PgPoolOptions;

use crate::{config, execution_node::ExecutionNode};

const SUPPLY_DELTA_BUFFER_SIZE: usize = 10_000;

pub async fn write_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("writing supply deltas CSV");

    let mut supply_deltas_rx =
        crate::execution_node::stream_supply_delta_chunks(0, SUPPLY_DELTA_BUFFER_SIZE);

    let mut progress = pit_wall::Progress::new("write supply deltas", 15_000_000);

    let timestamp = crate::time::get_timestamp();

    let file_path = format!("supply_deltas_{}.csv", timestamp);

    let mut csv_writer = csv::Writer::from_path(&file_path).unwrap();

    while let Some(supply_deltas) = supply_deltas_rx.next().await {
        for supply_delta in supply_deltas {
            csv_writer.serialize(supply_delta).unwrap();
        }

        progress.inc_work_done_by(SUPPLY_DELTA_BUFFER_SIZE.try_into().unwrap());
        tracing::debug!("{}", progress.get_progress_string());
    }

    // A CSV writer maintains an internal buffer, so it's important
    // to flush the buffer when you're done.
    csv_writer.flush().unwrap();
}

const BLOCK_SYNC_QUEUE: Vec<u32> = vec![];

pub async fn sync_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing supply deltas");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config::get_db_url())
        .await
        .unwrap();

    sqlx::migrate!().run(&pool).await.unwrap();

    let mut execution_node = ExecutionNode::connect().await;
    let latest_block = execution_node.get_latest_block().await;

    dbg!(latest_block);

    let mut new_heads_rx = crate::execution_node::stream_new_heads();

    while let Some(new_head) = new_heads_rx.next().await {
        // let _latest_stored_block = crate::execution_chain::get_latest_block(&pool).await;
        dbg!(new_head);
    }
}