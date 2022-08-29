use crate::execution_chain::node::BlockNumber;
use std::{collections::HashMap, time::SystemTime};

use chrono::{DateTime, Utc};
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::execution_chain::sync::EXECUTION_BLOCK_NUMBER_AUG_1ST;

use super::{
    node::{Difficulty, ExecutionNodeBlock, TotalDifficulty},
    sync::BlockRange,
    ExecutionNode,
};

fn get_historic_stream(block_range: &BlockRange) -> impl Stream<Item = ExecutionNodeBlock> {
    let (mut tx, rx) = futures::channel::mpsc::channel(10);

    let block_range_clone = block_range.clone();
    tokio::spawn(async move {
        let mut execution_node = ExecutionNode::connect().await;
        for block_number in block_range_clone.into_iter() {
            let block = execution_node
                .get_block_by_number(&block_number)
                .await
                .unwrap();
            tx.send(block.into()).await.unwrap();
        }
    });

    rx
}

#[derive(Deserialize)]
struct EthPrice {
    number: u32,
    eth_price: f64,
}

#[derive(Serialize)]
struct OutRow {
    // Highest gas price seen, ~4000 Gwei, if we want 10x- 100x future proof, we need to handle
    // 4000 * 100 * 1e9 (Gwei), which wouldn't fit in i32, but is <1% of i64.
    base_fee_per_gas: u64,
    difficulty: Difficulty,
    eth_price: f64,
    // Started at 8M, currently at 30M, seems to fit in 2^31 for the foreseeable future.
    gas_used: u32,
    hash: String,
    number: BlockNumber,
    parent_hash: String,
    timestamp: DateTime<Utc>,
    total_difficulty: TotalDifficulty,
}

pub async fn write_blocks_from_august() {
    tracing_subscriber::fmt::init();

    tracing::info!("writing blocks to CSV");

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    tracing::debug!("loading eth prices");

    let mut eth_prices_csv = csv::Reader::from_path("eth_prices.csv").unwrap();
    let mut iter = eth_prices_csv.deserialize();

    let mut eth_prices = HashMap::new();

    while let Some(record) = iter.next() {
        let record: EthPrice = record.unwrap();
        eth_prices.insert(record.number, record.eth_price);
    }

    tracing::debug!("done loading eth prices");

    let mut historic_stream = get_historic_stream(&BlockRange {
        greater_than_or_equal: EXECUTION_BLOCK_NUMBER_AUG_1ST,
        less_than_or_equal: 15429946,
    });

    let mut progress = pit_wall::Progress::new(
        "write blocks",
        (15429946 - EXECUTION_BLOCK_NUMBER_AUG_1ST).into(),
    );

    let file_path = format!("blocks_from_august_{}.csv", timestamp);

    let mut csv_writer = csv::Writer::from_path(&file_path).unwrap();

    while let Some(block) = historic_stream.next().await {
        let out = OutRow {
            base_fee_per_gas: block.base_fee_per_gas,
            difficulty: block.difficulty,
            eth_price: *eth_prices.get(&block.number).unwrap(),
            gas_used: block.gas_used,
            hash: block.hash,
            number: block.number,
            parent_hash: block.parent_hash,
            timestamp: block.timestamp,
            total_difficulty: block.total_difficulty,
        };
        csv_writer.serialize(out).unwrap();

        progress.inc_work_done();
        if block.number % 100 == 0 {
            tracing::debug!("{}", progress.get_progress_string());
        }
    }

    // A CSV writer maintains an internal buffer, so it's important
    // to flush the buffer when you're done.
    csv_writer.flush().unwrap();
}