use chrono::NaiveDate;
use eth_analysis::dune::get_eth_in_contracts;
use serde::Deserialize;
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};

const OUTPUT_FILE_RAW_DATA: &str = "raw_dune_values.json";
const OUTPUT_FILE: &str = "formatted_dune_values.json";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TimestampValuePoint {
    pub t: u64,
    // fraction
    pub v: f64,
}

#[tokio::main]
pub async fn main() {
    let raw_data = get_eth_in_contracts().await.unwrap();

    // Write to Json
    let file = File::create(OUTPUT_FILE_RAW_DATA).unwrap();
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &raw_data).unwrap();
    writer.flush().unwrap();

    let timestamp_values = raw_data
        .into_iter()
        .map(|row| TimestampValuePoint {
            t: NaiveDate::parse_from_str(&row.block_date, "%Y-%m-%d")
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp() as u64,
            v: row.cumulative_sum,
        })
        .collect::<Vec<_>>();

    // Write to Json
    let file = File::create(OUTPUT_FILE).unwrap();
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &timestamp_values).unwrap();
    writer.flush().unwrap();
}
