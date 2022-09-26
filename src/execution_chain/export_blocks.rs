use crate::execution_chain::node::BlockNumber;
use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{BufRead, BufReader},
    time::SystemTime,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace, warn};

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

#[derive(Debug, Deserialize)]
struct EthPriceRow {
    usd: f64,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct EthPrice {
    usd: f64,
    timestamp: DateTime<Utc>,
}

impl From<EthPriceRow> for EthPrice {
    fn from(row: EthPriceRow) -> Self {
        Self {
            timestamp: row.timestamp.parse::<DateTime<Utc>>().unwrap(),
            usd: row.usd,
        }
    }
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

// We have the one after this in our DB already.
const EARLIEST_STORED_DB_BLOCK_NUMBER: u32 = 15429946;

async fn write_blocks_from(gte_block_number: u32, to_path: &str) -> Result<()> {
    debug!("loading eth prices");

    let mut eth_prices_csv = csv::Reader::from_path("eth_prices.csv")
        .expect("'eth_prices.csv' file to exist in cwd to read prices from");

    let eth_prices = eth_prices_csv
        .deserialize::<EthPriceRow>()
        .map(|row| row.unwrap().into())
        .collect::<Vec<EthPrice>>();

    debug!("done loading eth prices");

    let mut historic_stream = get_historic_stream(&BlockRange {
        greater_than_or_equal: gte_block_number,
        less_than_or_equal: EARLIEST_STORED_DB_BLOCK_NUMBER,
    });

    let mut progress = pit_wall::Progress::new(
        "write blocks",
        (EARLIEST_STORED_DB_BLOCK_NUMBER - gte_block_number).into(),
    );

    let mut csv_writer = csv::Writer::from_path(&to_path)?;

    let mut closest_price_index = 0;
    let mut closest_price = eth_prices
        .get(closest_price_index)
        .expect("eth prices should have at least one price");

    while let Some(block) = historic_stream.next().await {
        debug!(block.number, "exporting block");

        // Calculate the distance between the current block and the closest price we had for the last block.
        let mut distance_closest = closest_price
            .timestamp
            .signed_duration_since(block.timestamp)
            .num_seconds()
            .abs();

        trace!(distance = distance_closest, "current closest distance");

        // Peek the next price and update the closest until we have the closest.
        loop {
            trace!(%block.timestamp, %closest_price.timestamp);

            let next_index = closest_price_index + 1;
            let peeked_price = eth_prices.get(next_index);

            match peeked_price {
                None => {
                    warn!(
                        ?closest_price,
                        "no next eth price to peak, using current closest"
                    );
                    break;
                }
                Some(peeked_price) => {
                    let distance_peeked = peeked_price
                        .timestamp
                        .signed_duration_since(block.timestamp)
                        .num_seconds()
                        .abs();

                    trace!(distance = distance_peeked, "candidate distance");

                    match distance_closest.cmp(&distance_peeked) {
                        Ordering::Greater | Ordering::Equal => {
                            trace!("found a closer price");
                            closest_price_index = next_index;
                            closest_price = peeked_price;
                            distance_closest = distance_peeked;
                        }
                        Ordering::Less => {
                            trace!("current already closest");
                            break;
                        }
                    }
                }
            }
        }

        let out = OutRow {
            base_fee_per_gas: block.base_fee_per_gas,
            difficulty: block.difficulty,
            eth_price: closest_price.usd,
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
            info!("{}", progress.get_progress_string());
        }
    }

    // A CSV writer maintains an internal buffer, so it's important
    // to flush the buffer when you're done.
    csv_writer.flush().unwrap();

    Ok(())
}

pub async fn write_blocks_from_august() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!(
        august_block_number = EXECUTION_BLOCK_NUMBER_AUG_1ST,
        "writing blocks from august to CSV"
    );

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    debug!("loading eth prices");

    let file_path = format!("blocks_from_august_{}.csv", timestamp);

    write_blocks_from(EXECUTION_BLOCK_NUMBER_AUG_1ST, &file_path).await?;

    Ok(())
}

pub async fn write_blocks_from_london() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!(
        earliest_stored = EARLIEST_STORED_DB_BLOCK_NUMBER,
        "writing blocks from london to earliest stored, to CSV"
    );

    let file_path = "blocks_from_london.csv";

    let file = File::open(file_path);
    match file {
        Err(_err) => {
            info!("first run, starting at london hardfork");
            write_blocks_from(LONDON_HARDFORK_BLOCK_NUMBER, &file_path).await?;
        }
        Ok(file) => {
            // Because we interrupt the writing sometimes the last row may be malformed, if a file
            // exists, drop the last line.
            let lines_text = BufReader::new(&file)
                .lines()
                .map(|x| x.unwrap())
                .collect::<Vec<String>>();
            let strings = lines_text[0..(lines_text.len() - 1)].join("\n");
            fs::write(file_path, strings.as_bytes())?;

            let mut blocks_from_london_csv = csv::Reader::from_path(file_path)?;
            let last_stored_block_number = blocks_from_london_csv
                .deserialize()
                .last()
                .map(|row: Result<OutRow, _>| row.unwrap().number)
                .unwrap_or(LONDON_HARDFORK_BLOCK_NUMBER);
            info!(last_stored_block_number, "picking up from previous run");
            write_blocks_from(last_stored_block_number + 1, &file_path).await?;
        }
    };

    Ok(())
}
