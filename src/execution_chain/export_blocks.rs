use crate::execution_chain::node::BlockNumber;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader},
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::execution_chain::sync::EXECUTION_BLOCK_NUMBER_AUG_1ST;

use super::{
    node::{Difficulty, ExecutionNodeBlock, TotalDifficulty},
    sync::BlockRange,
    ExecutionNode,
};

const LONDON_HARDFORK_BLOCK_NUMBER: u32 = 12965000;

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

#[derive(Deserialize, Serialize)]
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

const BLOCKS_NEXT_MIN: u32 = 15253306;

fn get_last_written_number(path: &str) -> Option<u32> {
    let mut blocks_from_london_csv = csv::Reader::from_path(path).ok()?;
    let mut iter = blocks_from_london_csv.deserialize();
    let last_stored: OutRow = iter.next().unwrap().unwrap();
    Some(last_stored.number)
}

pub async fn write_blocks_from_london() {
    tracing_subscriber::fmt::init();

    tracing::info!("writing blocks to CSV");

    tracing::debug!("loading eth prices");

    let mut eth_prices_csv = csv::Reader::from_path("eth_prices.csv").unwrap();
    let mut iter = eth_prices_csv.deserialize();

    let mut eth_prices = HashMap::new();

    while let Some(record) = iter.next() {
        let record: EthPrice = record.unwrap();
        eth_prices.insert(record.number, record.eth_price);
    }

    tracing::debug!("done loading eth prices");

    let mut progress = pit_wall::Progress::new(
        "write blocks",
        Into::<u64>::into(BLOCKS_NEXT_MIN - LONDON_HARDFORK_BLOCK_NUMBER) + 1,
    );

    let file_path = "blocks_from_london.csv";

    let last_stored: Option<u32> = get_last_written_number(file_path);

    match last_stored {
        None => {
            tracing::info!("first run, starting at london hardfork");
        }
        Some(last_stored) => {
            tracing::info!("picking up from previous run, starting at block {last_stored}");
            // Because we interrupt the writing sometime the last row may be malformed, if a file
            // exists, drop the last line.
            let file = File::open(file_path).unwrap();
            let lines_text = BufReader::new(file)
                .lines()
                .map(|x| x.unwrap())
                .collect::<Vec<String>>();
            let strings = lines_text[0..(lines_text.len() - 1)].join("\n");
            fs::write(file_path, strings.as_bytes()).unwrap();
        }
    }

    let mut historic_stream = get_historic_stream(&BlockRange {
        greater_than_or_equal: last_stored.unwrap_or(LONDON_HARDFORK_BLOCK_NUMBER),
        less_than_or_equal: BLOCKS_NEXT_MIN - 1,
    });

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
