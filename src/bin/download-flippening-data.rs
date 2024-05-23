use eth_analysis::dune::get_flippening_data;
use std::fs::File;
use std::io::{BufWriter, Write};

const OUTPUT_FILE_RAW_DATA: &str = "raw_dune_values.json";

#[tokio::main]
pub async fn main() {
    let raw_data = get_flippening_data().await.unwrap();

    // Write to Json
    let file = File::create(OUTPUT_FILE_RAW_DATA).unwrap();
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, &raw_data).unwrap();
    writer.flush().unwrap();
}
