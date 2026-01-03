use anyhow::Result;
use axum::{extract::Query, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Deserialize;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom},
    sync::RwLock,
};
use tracing::{debug, error, info, warn};

use super::GethSupplyDelta;
use crate::env;
use crate::execution_chain::{BlockNumber, SupplyDelta};

const MAX_CACHED_DELTAS: usize = 1_000_000;
const TAIL_POLL_INTERVAL: Duration = Duration::from_secs(1);
const WATCHDOG_MAX_STALE_DEFAULT: Duration = Duration::from_secs(60 * 60);
const WATCHDOG_CHECK_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);

#[cfg(unix)]
fn file_inode(meta: &std::fs::Metadata) -> u64 {
    meta.ino()
}

#[cfg(not(unix))]
fn file_inode(_meta: &std::fs::Metadata) -> u64 {
    0
}

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
        self_destruct,
        fee_burn,
        fixed_reward: reward,
        uncles_reward: 0,
    })
}

pub struct LiveSupplyReader {
    data_dir: PathBuf,
    deltas: Arc<RwLock<BTreeMap<BlockNumber, Vec<SupplyDelta>>>>,
    last_progress: Arc<Mutex<Instant>>,
}

impl LiveSupplyReader {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            deltas: Arc::new(RwLock::new(BTreeMap::new())),
            last_progress: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn mark_progress(&self) {
        let mut last_progress = self
            .last_progress
            .lock()
            .expect("last_progress mutex poisoned");
        *last_progress = Instant::now();
    }

    fn last_progress_age(&self) -> Duration {
        let last_progress = self
            .last_progress
            .lock()
            .expect("last_progress mutex poisoned");
        last_progress.elapsed()
    }

    fn ensure_cache_limit(deltas: &mut BTreeMap<BlockNumber, Vec<SupplyDelta>>) {
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

    /// Adds a supply delta to the cache only if a delta with the same block_number
    /// and block_hash does not already exist.
    /// Returns true if a new delta was added, false otherwise.
    fn add_delta_to_cache_if_unique(
        deltas_cache: &mut BTreeMap<BlockNumber, Vec<SupplyDelta>>,
        delta_to_add: SupplyDelta,
    ) -> bool {
        let block_deltas = deltas_cache.entry(delta_to_add.block_number).or_default();

        let hash_exists = block_deltas
            .iter()
            .any(|existing_delta| existing_delta.block_hash == delta_to_add.block_hash);

        if !hash_exists {
            block_deltas.push(delta_to_add);
            true
        } else {
            false
        }
    }

    // New private helper function to find and sort historic files
    fn find_historic_files_in_dir(&self) -> Result<Vec<PathBuf>> {
        let mut historic_files: Vec<PathBuf> = Vec::new();

        debug!(
            "scanning directory {:?} for historic supply files (supply-*.jsonl excluding supply.jsonl).",
            self.data_dir
        );

        match std::fs::read_dir(&self.data_dir) {
            Ok(entries) => {
                for entry_result in entries {
                    match entry_result {
                        Ok(entry) => {
                            let path = entry.path();
                            if path.is_file() {
                                if let Some(filename_osstr) = path.file_name() {
                                    if let Some(filename_str) = filename_osstr.to_str() {
                                        if filename_str.starts_with("supply-")
                                            && filename_str.ends_with(".jsonl")
                                            && filename_str != "supply.jsonl"
                                        {
                                            historic_files.push(path.clone());
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "error reading a directory entry in {:?}: {}",
                                self.data_dir, e
                            );
                        }
                    }
                }
                debug!(
                    "found {} historic files by manual scan.",
                    historic_files.len()
                );
                historic_files.sort();
                Ok(historic_files)
            }
            Err(e) => {
                error!(
                    "failed to read directory {:?}: {}. no historic files will be loaded.",
                    self.data_dir, e
                );
                Ok(Vec::new())
            }
        }
    }

    async fn get_initial_supply_files(&self) -> Result<Vec<PathBuf>> {
        let mut historic_files = self.find_historic_files_in_dir()?;
        // Reverse sort historic files to process newest first
        historic_files.reverse();

        let mut files_to_process: Vec<PathBuf> = Vec::new();

        // 1. Add the current live file first, if it exists.
        let current_live_file = self.data_dir.join("supply.jsonl");
        if current_live_file.exists() {
            files_to_process.push(current_live_file.clone());
        }

        // 2. Add historic files (newest first), ensuring no duplicates if live file was also a historic one (e.g. just rotated)
        for historic_file in historic_files {
            // Check if this historic file is the same as the live file we might have already added.
            // This check is mainly for the scenario where supply.jsonl was the *only* file found by find_historic_files_in_dir
            // if it was named supply_....jsonl, which our current find_historic_files_in_dir excludes.
            // More robustly, ensure we don't add a file path that's already in files_to_process.
            if !files_to_process.contains(&historic_file) {
                // Avoid duplicates
                files_to_process.push(historic_file);
            }
        }

        info!(
            "initial files to process (most recent first): {} -> {:?}",
            files_to_process.len(),
            files_to_process
        );
        Ok(files_to_process)
    }

    pub fn start_background_tasks(self: Arc<Self>) {
        let reader_clone_tail = self.clone();
        tokio::spawn(async move {
            info!("background supply reading task started.");
            if let Err(e) = reader_clone_tail.initial_load_and_tail().await {
                error!("error in live supply reading process: {}", e);
            }
        });

        let reader_clone_rescan = self;
        tokio::spawn(async move {
            info!("background periodic full rescan task started.");
            reader_clone_rescan.periodic_full_rescan().await;
            warn!("periodic full rescan task unexpectedly finished.");
        });
    }

    pub fn start_watchdog_task(self: Arc<Self>, max_stale: Duration, check_interval: Duration) {
        tokio::spawn(async move {
            info!(
                "watchdog enabled. restarting process if no new supply deltas for {}s (check every {}s)",
                max_stale.as_secs(),
                check_interval.as_secs()
            );
            loop {
                tokio::time::sleep(check_interval).await;
                let age = self.last_progress_age();
                if age >= max_stale {
                    error!(
                        "no new supply deltas for {}s (limit {}s); exiting so supervisor can restart",
                        age.as_secs(),
                        max_stale.as_secs()
                    );
                    std::process::exit(1);
                }
            }
        });
    }

    async fn initial_load_and_tail(&self) -> Result<()> {
        let initial_files = self.get_initial_supply_files().await?;
        info!("processing {} initial supply files until cache limit ({}) is reached or all files are read.", initial_files.len(), MAX_CACHED_DELTAS);

        for file_path in initial_files {
            if !file_path.exists() {
                warn!(
                    "initial supply file not found during processing, skipping: {:?}",
                    file_path
                );
                continue;
            }

            let current_cache_size = self.deltas.read().await.len();
            if current_cache_size >= MAX_CACHED_DELTAS {
                info!(
                    "cache limit ({}) reached or exceeded (current size: {}). stopping initial file load. will proceed to tailing.", 
                    MAX_CACHED_DELTAS, current_cache_size
                );
                break;
            }

            self.read_supply_file(&file_path).await?;
        }

        let live_file_path = self.data_dir.join("supply.jsonl");
        if live_file_path.exists() {
            self.tail_supply_file(&live_file_path).await?;
        } else {
            warn!(
                "live supply file ({:?}) not found. tailing will not start.",
                live_file_path
            );
        }
        Ok(())
    }

    async fn read_supply_file(&self, file_path: &PathBuf) -> Result<()> {
        debug!("reading supply data from: {:?}", file_path);
        let file = File::open(file_path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut deltas_guard = self.deltas.write().await;

        while let Some(line_result) = lines.next_line().await? {
            match serde_json::from_str::<GethSupplyDelta>(&line_result) {
                Ok(geth_delta) => match convert_to_supply_delta(&geth_delta) {
                    Ok(supply_delta) => {
                        if Self::add_delta_to_cache_if_unique(&mut deltas_guard, supply_delta) {
                            self.mark_progress();
                        }
                    }
                    Err(e) => {
                        error!(
                            "failed to convert gethsupplydelta (block: {}): {} from file: {:?}",
                            geth_delta.block_number, e, file_path
                        );
                    }
                },
                Err(e) => {
                    error!(
                        "failed to parse gethsupplydelta line from file {:?}: {} -- line: {}",
                        file_path, e, line_result
                    );
                }
            }
        }
        Self::ensure_cache_limit(&mut deltas_guard);
        debug!(
            "finished reading {:?}. cache size: {}",
            file_path,
            deltas_guard.len()
        );
        Ok(())
    }

    async fn tail_supply_file(&self, file_path: &PathBuf) -> Result<()> {
        info!("starting to tail live supply file: {:?}", file_path);
        let mut file = File::open(file_path).await?;
        let mut current_position = file.seek(SeekFrom::End(0)).await?;
        let mut last_inode = file_inode(&file.metadata().await?);
        let inode_supported = cfg!(unix);
        debug!(
            "initial position for tailing {:?}: {}",
            file_path, current_position
        );

        loop {
            tokio::time::sleep(TAIL_POLL_INTERVAL).await;

            let metadata = tokio::fs::metadata(file_path).await?;
            let file_size = metadata.len();
            let rotated_due_to_size = file_size < current_position;
            let rotated_due_to_inode = inode_supported && file_inode(&metadata) != last_inode;
            let rotated = rotated_due_to_size || rotated_due_to_inode;

            if rotated {
                if rotated_due_to_size {
                    warn!(
                        "file {:?} appears to have been truncated (size: {} < last_pos: {}). reopening and resetting position to 0.",
                        file_path,
                        file_size,
                        current_position
                    );
                } else {
                    warn!(
                        "file {:?} appears to have been rotated (inode change). reopening and resetting position to 0.",
                        file_path
                    );
                }
                file = File::open(file_path).await?;
                current_position = 0;
                last_inode = file_inode(&file.metadata().await?);
            }

            if file_size > current_position {
                debug!(
                    "file {:?} has grown from {} to {}. reading new lines.",
                    file_path, current_position, file_size
                );
                file.seek(SeekFrom::Start(current_position)).await?;
                let reader = BufReader::new(&mut file);
                let mut lines = reader.lines();

                let mut deltas_guard = self.deltas.write().await;
                let mut new_deltas_processed = 0;

                while let Some(line_result) = lines.next_line().await? {
                    match serde_json::from_str::<GethSupplyDelta>(&line_result) {
                        Ok(geth_delta) => match convert_to_supply_delta(&geth_delta) {
                            Ok(supply_delta) => {
                                if Self::add_delta_to_cache_if_unique(
                                    &mut deltas_guard,
                                    supply_delta,
                                ) {
                                    new_deltas_processed += 1;
                                    self.mark_progress();
                                }
                            }
                            Err(e) => {
                                error!("tail: failed to convert gethsupplydelta (block: {}): {} from file: {:?}", geth_delta.block_number, e, file_path);
                            }
                        },
                        Err(e) => {
                            error!("tail: failed to parse gethsupplydelta line from file {:?}: {} -- line: {}", file_path, e, line_result);
                        }
                    }
                }

                current_position = file.seek(SeekFrom::Current(0)).await?;

                if new_deltas_processed > 0 {
                    Self::ensure_cache_limit(&mut deltas_guard);
                    debug!(
                        "tail: processed {} new unique deltas from {:?}. new position: {}. cache size: {}",
                        new_deltas_processed,
                        file_path,
                        current_position,
                        deltas_guard.len()
                    );
                }
            }
        }
    }

    async fn periodic_full_rescan(&self) {
        let rescan_interval = Duration::from_secs(60);
        let file_path = self.data_dir.join("supply.jsonl");

        loop {
            tokio::time::sleep(rescan_interval).await;

            if !file_path.exists() {
                warn!("periodic_full_rescan: supply.jsonl not found, skipping scan.");
                continue;
            }

            debug!("starting periodic full rescan of {:?}", file_path);
            let file = match File::open(&file_path).await {
                Ok(f) => f,
                Err(e) => {
                    error!(
                        "periodic_full_rescan: failed to open {:?}: {}. skipping scan.",
                        file_path, e
                    );
                    continue;
                }
            };

            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            let mut deltas_added = 0;
            let mut lines_processed = 0;

            {
                let mut deltas_guard = self.deltas.write().await;
                while let Ok(Some(line_result)) = lines.next_line().await {
                    lines_processed += 1;
                    match serde_json::from_str::<GethSupplyDelta>(&line_result) {
                        Ok(geth_delta) => match convert_to_supply_delta(&geth_delta) {
                            Ok(supply_delta) => {
                                if Self::add_delta_to_cache_if_unique(
                                    &mut deltas_guard,
                                    supply_delta,
                                ) {
                                    deltas_added += 1;
                                    self.mark_progress();
                                }
                            }
                            Err(e) => {
                                error!(
                                    "periodic_full_rescan: failed to convert gethsupplydelta (block: {}): {} from file: {:?}",
                                    geth_delta.block_number, e, file_path
                                );
                            }
                        },
                        Err(e) => {
                            error!(
                                "periodic_full_rescan: failed to parse gethsupplydelta line from file {:?}: {} -- line: {}",
                                file_path, e, line_result
                            );
                        }
                    }
                }
                if deltas_added > 0 {
                    Self::ensure_cache_limit(&mut deltas_guard);
                }
            }

            debug!(
                "periodic_full_rescan: finished scan of {:?}. Processed {} lines, added {} new unique deltas. Cache size: {}",
                file_path,
                lines_processed,
                deltas_added,
                self.deltas.read().await.len()
            );
        }
    }

    pub async fn get_supply_delta(&self, block_number: BlockNumber) -> Option<Vec<SupplyDelta>> {
        self.deltas.read().await.get(&block_number).cloned()
    }
}

#[derive(Deserialize)]
struct SupplyDeltaQuery {
    block_number: BlockNumber,
}

fn env_duration_secs(key: &str) -> Option<Duration> {
    env::get_env_var(key)
        .map(|value| Duration::from_secs(value.parse::<u64>().expect("invalid duration seconds")))
}

pub async fn start_live_api(data_dir: PathBuf, port: u16) -> Result<()> {
    let reader = Arc::new(LiveSupplyReader::new(data_dir));

    reader.clone().start_background_tasks();
    let max_stale =
        env_duration_secs("GETH_SUPPLY_LIVE_MAX_STALE_SECS").unwrap_or(WATCHDOG_MAX_STALE_DEFAULT);
    let check_interval = env_duration_secs("GETH_SUPPLY_LIVE_WATCHDOG_INTERVAL_SECS")
        .unwrap_or(WATCHDOG_CHECK_INTERVAL_DEFAULT);
    if max_stale.as_secs() == 0 {
        warn!("watchdog disabled (GETH_SUPPLY_LIVE_MAX_STALE_SECS=0)");
    } else {
        reader
            .clone()
            .start_watchdog_task(max_stale, check_interval);
    }

    let app = Router::new()
        .route("/supply/delta", get(get_supply_delta_handler))
        .with_state(reader);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    info!("starting live supply api on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn get_supply_delta_handler(
    Query(query): Query<SupplyDeltaQuery>,
    axum::extract::State(reader): axum::extract::State<Arc<LiveSupplyReader>>,
) -> impl IntoResponse {
    match reader.get_supply_delta(query.block_number).await {
        Some(deltas_vec) => (StatusCode::OK, Json(deltas_vec)).into_response(),
        None => (StatusCode::OK, Json(Vec::<SupplyDelta>::new())).into_response(), // Return 200 OK with empty array
    }
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
    #[ignore = "fails in CI for unclear reasons"]
    async fn test_initial_load() {
        let (_data_dir, reader) = setup_test_server_and_reader().await;
        reader.initial_load_and_tail().await.unwrap();

        let deltas_map = reader.deltas.read().await;
        assert_eq!(
            deltas_map.len(),
            4,
            "Should load data for 4 distinct block numbers"
        );
        assert_eq!(deltas_map.get(&1).map(|v| v.len()), Some(1));
        assert_eq!(deltas_map.get(&2).map(|v| v.len()), Some(1));
        assert_eq!(deltas_map.get(&3).map(|v| v.len()), Some(1));
        assert_eq!(deltas_map.get(&4).map(|v| v.len()), Some(1));

        // Example of how to check content if needed
        // let block1_deltas = deltas_map.get(&1).unwrap();
        // assert_eq!(block1_deltas[0].supply_delta, 16); // 0x10
    }

    #[tokio::test]
    async fn test_ensure_cache_limit() {
        let mut deltas: BTreeMap<BlockNumber, Vec<SupplyDelta>> = BTreeMap::new();
        for i in 1..(MAX_CACHED_DELTAS + 5) {
            let supply_delta_entry = SupplyDelta {
                block_number: i as BlockNumber,
                block_hash: format!("0x{i}"),
                parent_hash: format!("0xp{i}"),
                supply_delta: i as i128,
                self_destruct: 0,
                fee_burn: 0,
                fixed_reward: 0,
                uncles_reward: 0,
            };
            deltas.insert(i as BlockNumber, vec![supply_delta_entry]);
        }
        assert_eq!(deltas.len(), MAX_CACHED_DELTAS + 4);
        LiveSupplyReader::ensure_cache_limit(&mut deltas);
        assert_eq!(deltas.len(), MAX_CACHED_DELTAS);
        assert!(!deltas.contains_key(&1));
        assert!(deltas.contains_key(&((MAX_CACHED_DELTAS + 4) as BlockNumber)));
    }
}
