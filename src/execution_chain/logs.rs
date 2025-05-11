use std::{collections::HashSet, iter::Iterator};

use chrono::SubsecRound;
use futures::StreamExt;
use serde::Serialize;

use crate::execution_chain::{self, BlockNumber};
use crate::log;

#[derive(Serialize)]
struct HeadLog {
    block_number: BlockNumber,
    hash: String,
    is_duplicate_number: bool,
    is_jumping_ahead: bool,
    parent_hash: String,
    received_at: String,
    timestamp: String,
}

pub async fn write_heads_log() {
    log::init();

    let timestamp = chrono::offset::Utc::now().timestamp();

    tracing::info!("writing heads log {timestamp}");

    let mut heads_stream = execution_chain::stream_new_heads();

    let file_path = format!("heads_log_{timestamp}.csv");

    let mut csv_writer = csv::Writer::from_path(&file_path).unwrap();

    let mut seen_block_heights = HashSet::<BlockNumber>::new();
    let mut seen_block_hashes = HashSet::<String>::new();

    while let Some(head) = heads_stream.next().await {
        let is_duplicate_number = seen_block_heights.contains(&head.number);
        let is_jumping_ahead =
            !seen_block_hashes.is_empty() && !seen_block_hashes.contains(&head.parent_hash);

        seen_block_heights.insert(head.number);
        seen_block_hashes.insert(head.hash.clone());

        let head_log = HeadLog {
            block_number: head.number,
            hash: head.hash,
            is_duplicate_number,
            is_jumping_ahead,
            parent_hash: head.parent_hash,
            received_at: chrono::offset::Utc::now().trunc_subsecs(0).to_rfc3339(),
            timestamp: head.timestamp.to_rfc3339(),
        };

        csv_writer.serialize(head_log).unwrap();
        csv_writer.flush().unwrap();

        tracing::debug!("wrote head log {}", head.number);
    }
}
