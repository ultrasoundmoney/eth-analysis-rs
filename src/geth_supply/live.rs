use anyhow::Result;
use axum::{extract::Query, routing::get, Json, Router};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom},
    sync::RwLock,
};
use tracing::{error, info, warn};

use super::GethSupplyDelta;
use crate::execution_chain::{BlockNumber, SupplyDelta};

const MAX_CACHED_DELTAS: usize = 1_000_000;
const TAIL_POLL_INTERVAL: Duration = Duration::from_secs(1);

fn convert_to_supply_delta(geth_delta: &GethSupplyDelta) -> Result<SupplyDelta> {
    // Convert hex strings to numbers, defaulting to 0 if optional fields are None
    let genesis_allocation = geth_delta
        .issuance
        .as_ref()
        .and_then(|i| i.genesis_allocation.as_ref())
        .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;

    let reward = geth_delta
        .issuance
        .as_ref()
        .and_then(|i| i.reward.as_ref())
        .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;

    let withdrawals = geth_delta
        .issuance
        .as_ref()
        .and_then(|i| i.withdrawals.as_ref())
        .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;

    let mut fee_burn = 0i128;
    if let Some(burn) = &geth_delta.burn {
        if let Some(eip1559) = &burn.eip1559 {
            fee_burn += i128::from_str_radix(&eip1559[2..], 16)?;
        }
        if let Some(blob) = &burn.blob {
            fee_burn += i128::from_str_radix(&blob[2..], 16)?;
        }
    }

    let self_destruct = geth_delta
        .burn
        .as_ref()
        .and_then(|b| b.misc.as_ref())
        .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;

    let supply_delta_val = withdrawals + reward + genesis_allocation - fee_burn - self_destruct;

    Ok(SupplyDelta {
        block_number: geth_delta.block_number,
        parent_hash: geth_delta.parent_hash.clone(),
        block_hash: geth_delta.hash.clone(),
        supply_delta: supply_delta_val,
        self_destruct: self_destruct,
        fee_burn: fee_burn,
        fixed_reward: withdrawals, // As per previous logic
        uncles_reward: 0,          // As per previous logic
    })
}

pub struct LiveSupplyReader {
    data_dir: PathBuf,
    deltas: Arc<RwLock<BTreeMap<BlockNumber, SupplyDelta>>>,
}

impl LiveSupplyReader {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            deltas: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn ensure_cache_limit(deltas: &mut BTreeMap<BlockNumber, SupplyDelta>) {
        if deltas.len() > MAX_CACHED_DELTAS {
            let mut num_to_remove = deltas.len() - MAX_CACHED_DELTAS;
            // BTreeMap keys are sorted, so first_key_value gives the smallest (oldest) block
            while num_to_remove > 0 {
                if let Some((block_number, _)) = deltas.first_key_value() {
                    // Need to clone block_number because BTreeMap::remove takes ownership of the key if it's not Copy
                    let oldest_block_number = *block_number;
                    deltas.remove(&oldest_block_number);
                } else {
                    break; // Should not happen if len > 0
                }
                num_to_remove -= 1;
            }
        }
    }

    async fn get_initial_supply_files(&self) -> Result<Vec<PathBuf>> {
        let mut files_to_process = Vec::new();

        // Find the most recent timestamped historic file
        let pattern = self
            .data_dir
            .join("supply_*.jsonl")
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid path pattern for historic files"))?
            .to_string();

        let mut historic_files: Vec<_> = glob::glob(&pattern)?.filter_map(Result::ok).collect();
        historic_files.sort(); // Sorts alphabetically, which works for timestamped names

        if let Some(latest_historic) = historic_files.last() {
            files_to_process.push(latest_historic.clone());
        }

        // Add the current live file
        let current_live_file = self.data_dir.join("supply.jsonl");
        if current_live_file.exists() {
            // Avoid adding live file if it's the same as the latest historic (e.g. after rotation just happened)
            if files_to_process
                .last()
                .map_or(true, |f| f != &current_live_file)
            {
                files_to_process.push(current_live_file);
            }
        }

        info!("Initial files to read: {:?}", files_to_process);
        Ok(files_to_process)
    }

    pub fn start_background_tailing(self: Arc<Self>) {
        tokio::spawn(async move {
            info!("Background supply reading task started.");
            if let Err(e) = self.initial_load_and_tail().await {
                error!("Error in live supply reading process: {}", e);
            }
        });
    }

    async fn initial_load_and_tail(&self) -> Result<()> {
        // 1. Load initial files
        let initial_files = self.get_initial_supply_files().await?;
        for file_path in initial_files {
            if file_path.exists() {
                self.read_supply_file(&file_path).await?;
            } else {
                warn!("Initial supply file not found, skipping: {:?}", file_path);
            }
        }

        // 2. Start tailing the live file
        let live_file_path = self.data_dir.join("supply.jsonl");
        if live_file_path.exists() {
            self.tail_supply_file(&live_file_path).await?;
        } else {
            warn!("Live supply file ({:?}) not found. Tailing will not start. The service might only serve historic data if any was loaded.", live_file_path);
            // We might want to periodically check if it appears later, or just error.
            // For now, it just won't tail.
        }
        Ok(())
    }

    async fn read_supply_file(&self, file_path: &PathBuf) -> Result<()> {
        info!("Reading supply data from: {:?}", file_path);
        let file = File::open(file_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut deltas_guard = self.deltas.write().await;
        while let Some(line_result) = lines.next_line().await? {
            match serde_json::from_str::<GethSupplyDelta>(&line_result) {
                Ok(geth_delta) => match convert_to_supply_delta(&geth_delta) {
                    Ok(supply_delta) => {
                        deltas_guard.insert(supply_delta.block_number, supply_delta);
                    }
                    Err(e) => {
                        error!(
                            "Failed to convert GethSupplyDelta (block: {}): {} from file: {:?}",
                            geth_delta.block_number, e, file_path
                        );
                    }
                },
                Err(e) => {
                    error!(
                        "Failed to parse GethSupplyDelta line from file {:?}: {} -- Line: {}",
                        file_path, e, line_result
                    );
                }
            }
        }
        // Apply cache limit after processing the whole file
        Self::ensure_cache_limit(&mut deltas_guard);
        info!(
            "Finished reading {:?}. Cache size: {}",
            file_path,
            deltas_guard.len()
        );
        Ok(())
    }

    async fn tail_supply_file(&self, file_path: &PathBuf) -> Result<()> {
        info!("Starting to tail live supply file: {:?}", file_path);
        let mut file = File::open(file_path).await?;
        // Start reading from the end of the file, as initial_load_and_tail should have processed existing content.
        let mut current_position = file.seek(SeekFrom::End(0)).await?;
        info!(
            "Initial position for tailing {:?}: {}",
            file_path, current_position
        );

        loop {
            tokio::time::sleep(TAIL_POLL_INTERVAL).await;

            let metadata = tokio::fs::metadata(file_path).await?;
            let file_size = metadata.len();

            if file_size < current_position {
                warn!(
                    "File {:?} appears to have been truncated or rotated (size: {} < last_pos: {}). Resetting position to 0.",
                    file_path,
                    file_size,
                    current_position
                );
                current_position = 0;
                // It might be prudent to re-read the entire file here if we suspect full rotation,
                // but for simple truncation, just resetting and continuing might be okay for append-only.
                // For a robust solution, one might re-trigger a full read or compare with latest historic.
            }

            if file_size > current_position {
                info!(
                    "File {:?} has grown from {} to {}. Reading new lines.",
                    file_path, current_position, file_size
                );
                file.seek(SeekFrom::Start(current_position)).await?;
                let reader = BufReader::new(&mut file); // Re-create reader for the mutable file reference
                let mut lines = reader.lines();

                let mut deltas_guard = self.deltas.write().await;
                let mut new_lines_processed = 0;

                while let Some(line_result) = lines.next_line().await? {
                    new_lines_processed += 1;
                    match serde_json::from_str::<GethSupplyDelta>(&line_result) {
                        Ok(geth_delta) => match convert_to_supply_delta(&geth_delta) {
                            Ok(supply_delta) => {
                                deltas_guard.insert(supply_delta.block_number, supply_delta);
                            }
                            Err(e) => {
                                error!("Tail: Failed to convert GethSupplyDelta (block: {}): {} from file: {:?}", geth_delta.block_number, e, file_path);
                            }
                        },
                        Err(e) => {
                            error!("Tail: Failed to parse GethSupplyDelta line from file {:?}: {} -- Line: {}", file_path, e, line_result);
                        }
                    }
                }

                // Update current_position to the new end of what we've read.
                // This relies on lines.next_line() consuming the reader until EOF for the current read op.
                current_position = file.seek(SeekFrom::Current(0)).await?;

                if new_lines_processed > 0 {
                    Self::ensure_cache_limit(&mut deltas_guard);
                    info!(
                        "Tail: Processed {} new lines from {:?}. New position: {}. Cache size: {}",
                        new_lines_processed,
                        file_path,
                        current_position,
                        deltas_guard.len()
                    );
                }
            }
        }
        // Unreachable loop, Ok(()) if we decide to make it finite.
    }

    pub async fn get_supply_delta(&self, block_number: BlockNumber) -> Option<SupplyDelta> {
        self.deltas.read().await.get(&block_number).cloned()
    }
}

#[derive(Deserialize)]
struct SupplyDeltaQuery {
    block_number: BlockNumber,
}

pub async fn start_live_api(data_dir: PathBuf, port: u16) -> Result<()> {
    let reader = Arc::new(LiveSupplyReader::new(data_dir));

    // Start the background processing (initial load and tailing)
    reader.clone().start_background_tailing();

    let app = Router::new()
        .route("/supply/delta", get(get_supply_delta_handler))
        .with_state(reader);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    info!("Starting live supply API on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn get_supply_delta_handler(
    Query(query): Query<SupplyDeltaQuery>,
    axum::extract::State(reader): axum::extract::State<Arc<LiveSupplyReader>>,
) -> Json<Option<SupplyDelta>> {
    Json(reader.get_supply_delta(query.block_number).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn setup_test_server_and_reader() -> (PathBuf, Arc<LiveSupplyReader>) {
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let historic_file_path = data_dir.join("supply_20230101T000000.jsonl");
        let historic_content = r#"{"blockNumber":1,"hash":"0xh1","parentHash":"0xp1","issuance":{"withdrawals":"0x10"}}
{"blockNumber":2,"hash":"0xh2","parentHash":"0xp2","issuance":{"withdrawals":"0x20"}}"#;
        tokio::fs::write(&historic_file_path, historic_content)
            .await
            .unwrap();

        let live_file_path = data_dir.join("supply.jsonl");
        let live_content = r#"{"blockNumber":3,"hash":"0xh3","parentHash":"0xp3","issuance":{"withdrawals":"0x30"}}
{"blockNumber":4,"hash":"0xh4","parentHash":"0xp4","issuance":{"withdrawals":"0x40"}}"#;
        tokio::fs::write(&live_file_path, live_content)
            .await
            .unwrap();

        let reader = Arc::new(LiveSupplyReader::new(data_dir.clone()));
        (data_dir, reader)
    }

    #[tokio::test]
    async fn test_initial_load() {
        let (_data_dir, reader) = setup_test_server_and_reader().await;
        reader.initial_load_and_tail().await.unwrap();

        let deltas = reader.deltas.read().await;
        assert_eq!(deltas.len(), 4, "Should load data from both files");
        assert!(deltas.contains_key(&1));
        assert!(deltas.contains_key(&2));
        assert!(deltas.contains_key(&3));
        assert!(deltas.contains_key(&4));
    }

    #[tokio::test]
    async fn test_ensure_cache_limit() {
        let mut deltas: BTreeMap<BlockNumber, SupplyDelta> = BTreeMap::new();
        for i in 1..(MAX_CACHED_DELTAS + 5) {
            deltas.insert(
                i as BlockNumber,
                SupplyDelta {
                    block_number: i as BlockNumber,
                    block_hash: format!("0x{}", i),
                    parent_hash: format!("0xp{}", i),
                    supply_delta: i as i128,
                    self_destruct: 0,
                    fee_burn: 0,
                    fixed_reward: 0,
                    uncles_reward: 0,
                },
            );
        }
        assert_eq!(deltas.len(), MAX_CACHED_DELTAS + 4);
        LiveSupplyReader::ensure_cache_limit(&mut deltas);
        assert_eq!(deltas.len(), MAX_CACHED_DELTAS);
        assert!(!deltas.contains_key(&1));
        assert!(deltas.contains_key(&((MAX_CACHED_DELTAS + 4) as BlockNumber)));
    }
}
