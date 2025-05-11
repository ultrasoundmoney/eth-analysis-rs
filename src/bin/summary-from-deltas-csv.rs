use futures::prelude::*;
use serde::Deserialize;
use tracing::{debug, info};

use eth_analysis::{execution_chain::supply_deltas, log};

const SUPPLY_DELTA_BUFFER_SIZE: usize = 10_000;

pub async fn export_deltas() {
    log::init();

    let timestamp = eth_analysis::time::get_timestamp();

    info!("writing supply deltas {timestamp}");

    let mut supply_deltas_rx =
        supply_deltas::stream_supply_delta_chunks(0, SUPPLY_DELTA_BUFFER_SIZE);

    let mut progress = pit_wall::Progress::new("write supply deltas", 15_000_000);

    let file_path = format!("supply_deltas_{timestamp}.csv");

    let mut csv_writer = csv::Writer::from_path(&file_path).unwrap();

    while let Some(supply_deltas) = supply_deltas_rx.next().await {
        let len = supply_deltas.len();

        for supply_delta in supply_deltas {
            csv_writer.serialize(supply_delta).unwrap();
        }

        progress.inc_work_done_by(len.try_into().unwrap());
        debug!("{}", progress.get_progress_string());
    }

    // A CSV writer maintains an internal buffer, so it's important
    // to flush the buffer when you're done.
    csv_writer.flush().unwrap();
}

const STOP_AT_BLOCK_NUMBER: u32 = 11214496;

#[derive(Deserialize)]
struct SupplyDeltaRowV1 {
    // block_hash: String,
    block_number: u32,
    // fee_burn: u128,
    // fixed_reward: u64,
    // parent_hash: String,
    self_destruct: i128,
    supply_delta: i128,
    // uncles_reward: u64,
}

pub const GENESIS_ETH_BALANCE: i128 = 72009990499480000000000000i128;

#[tokio::main]
pub async fn main() {
    log::init();

    info!("generating summary from deltas csv");

    let mut eth_prices_csv = csv::Reader::from_path("supply_deltas_1662636045.csv").unwrap();
    let iter = eth_prices_csv.deserialize::<SupplyDeltaRowV1>();

    let mut sum = GENESIS_ETH_BALANCE;
    let mut self_destruct_count = 0u32;
    let mut self_destruct_sum = 0i128;

    for row in iter {
        let row = row.unwrap();

        sum += row.supply_delta;
        self_destruct_sum += row.self_destruct;

        if row.self_destruct != 0 {
            self_destruct_count += 1;
        }

        if row.block_number == STOP_AT_BLOCK_NUMBER {
            break;
        }
    }

    info!("done!");
    info!("-- summary --");
    info!("sum: {sum}");
    info!("self destruct count: {self_destruct_count}");
    info!("self destruct sum:: {self_destruct_sum}");
}
