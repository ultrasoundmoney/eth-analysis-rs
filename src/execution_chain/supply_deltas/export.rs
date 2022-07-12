use futures::prelude::*;

const SUPPLY_DELTA_BUFFER_SIZE: usize = 10_000;

pub async fn write_deltas() {
    tracing_subscriber::fmt::init();

    let timestamp = crate::time::get_timestamp();

    tracing::info!("writing supply deltas {timestamp}");

    let mut supply_deltas_rx =
        crate::execution_chain::stream_supply_delta_chunks(0, SUPPLY_DELTA_BUFFER_SIZE);

    let mut progress = pit_wall::Progress::new("write supply deltas", 15_000_000);

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
