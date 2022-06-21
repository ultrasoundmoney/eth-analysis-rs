use futures::channel;
use futures::prelude::*;

use crate::execution_node::ExecutionNode;

const SUPPLY_DELTA_BUFFER_SIZE: usize = 10_000;

pub async fn write_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("writing supply deltas CSV");

    let (supply_deltas_tx, mut supply_deltas_rx) = channel::mpsc::unbounded();

    tokio::spawn(async move {
        crate::execution_node::stream_supply_deltas_from(
            supply_deltas_tx,
            &0,
            SUPPLY_DELTA_BUFFER_SIZE,
        )
        .await;
    });

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

pub async fn sync_deltas() {
    tracing_subscriber::fmt::init();

    tracing::info!("syncing supply deltas");

    let execution_node = ExecutionNode::connect().await;
    execution_node.get_latest_block().await;
}
