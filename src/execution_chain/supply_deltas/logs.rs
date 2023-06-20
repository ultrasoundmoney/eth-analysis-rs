use crate::{
    execution_chain::{supply_deltas, BlockNumber, ExecutionNode},
    log,
};
use futures::StreamExt;
use serde::Serialize;
use std::collections::HashSet;
use tracing::{debug, info};

#[derive(Serialize)]
struct SupplyDeltaLog {
    block_number: BlockNumber,
    block_hash: String,
    is_duplicate_number: bool,
    is_jumping_ahead: bool,
    parent_hash: String,
    received_at: String,
}

pub async fn write_deltas_log() {
    log::init_with_env();

    let timestamp = chrono::offset::Utc::now().timestamp();

    info!("writing supply delta log {timestamp}");

    let execution_node = ExecutionNode::connect().await;
    let latest_block = execution_node.get_latest_block().await;

    let mut supply_deltas_stream = supply_deltas::stream_supply_deltas_from(latest_block.number);

    let file_path = format!("supply_deltas_log_{timestamp}.csv");

    let mut csv_writer = csv::Writer::from_path(&file_path).unwrap();

    let mut seen_block_heights = HashSet::<BlockNumber>::new();
    let mut seen_block_hashes = HashSet::<String>::new();

    while let Some(supply_delta) = supply_deltas_stream.next().await {
        let is_duplicate_number = seen_block_heights.contains(&supply_delta.block_number);
        let is_jumping_ahead =
            !seen_block_hashes.is_empty() && !seen_block_hashes.contains(&supply_delta.parent_hash);

        seen_block_heights.insert(supply_delta.block_number);
        seen_block_hashes.insert(supply_delta.block_hash.clone());

        let supply_delta_log = SupplyDeltaLog {
            block_number: supply_delta.block_number,
            block_hash: supply_delta.block_hash,
            parent_hash: supply_delta.parent_hash,
            is_jumping_ahead,
            is_duplicate_number,
            received_at: chrono::offset::Utc::now().to_rfc3339(),
        };

        csv_writer.serialize(supply_delta_log).unwrap();
        csv_writer.flush().unwrap();

        debug!("wrote supply delta log {}", supply_delta.block_number);
    }
}
