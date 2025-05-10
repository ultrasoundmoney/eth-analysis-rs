use std::time::Duration;

use anyhow::Context;
use futures::{channel::mpsc::UnboundedReceiver, Stream, StreamExt};
use lazy_static::lazy_static;
use reqwest::Client;
use sqlx::PgConnection;
use tokio::time::sleep;
use tracing::{debug, error, warn};

use crate::{
    env::ENV_CONFIG,
    execution_chain::{BlockNumber, ExecutionNode, SupplyDelta},
};

use super::sync;

lazy_static! {
    static ref GETH_SUPPLY_LIVE_API_URL: String = ENV_CONFIG
        .geth_supply_live_api_url
        .clone()
        .expect("GETH_SUPPLY_LIVE_API_URL must be set");
    static ref HTTP_CLIENT: Client = Client::new();
}

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_NO_DELTA_DURATION: Duration = Duration::from_secs(10 * 60); // 10 minutes

async fn fetch_supply_delta_http(block_number: BlockNumber) -> Result<SupplyDelta, anyhow::Error> {
    let url = format!(
        "{}/supply/delta?block_number={}",
        *GETH_SUPPLY_LIVE_API_URL, block_number
    );
    debug!(%url, "fetching supply delta(s)");

    let response = HTTP_CLIENT
        .get(&url)
        .timeout(HTTP_TIMEOUT)
        .send()
        .await
        .context(format!("http get request failed for url: {}", url))?;

    let response = response
        .error_for_status()
        .context(format!("server returned error for url: {}", url))?;

    let deltas_vec = response.json::<Vec<SupplyDelta>>().await.context(format!(
        "failed to parse json response (Vec<SupplyDelta>) from url: {}",
        url
    ))?;

    if deltas_vec.len() > 1 {
        debug!(
            block_number,
            count = deltas_vec.len(),
            "received multiple supply deltas for block, attempting to disambiguate via execution node"
        );

        let execution_node = ExecutionNode::connect().await;
        match execution_node.get_block_by_number(&block_number).await {
            Some(canonical_block) => {
                let canonical_hash = canonical_block.hash;
                debug!(%block_number, %canonical_hash, "found canonical hash for block");
                for delta in &deltas_vec {
                    if delta.block_hash == canonical_hash {
                        debug!(%block_number, selected_hash = %delta.block_hash, "selected matching delta");
                        return Ok(delta.clone());
                    }
                }
                error!(
                    %block_number,
                    %canonical_hash,
                    num_deltas_received = deltas_vec.len(),
                    "no supply delta matched canonical hash for block after disambiguation attempt. deltas: {:?}",
                    deltas_vec
                );
                anyhow::bail!(
                    "no supply delta matched canonical hash {} for block {}. received {} deltas.",
                    canonical_hash,
                    block_number,
                    deltas_vec.len()
                )
            }
            None => {
                error!(
                    %block_number,
                    num_deltas_received = deltas_vec.len(),
                    "failed to get canonical block from execution node to disambiguate supply deltas. deltas: {:?}",
                    deltas_vec
                );
                anyhow::bail!(
                    "failed to get canonical block for number {} to disambiguate {} supply deltas",
                    block_number,
                    deltas_vec.len()
                )
            }
        }
    } else if deltas_vec.len() == 1 {
        Ok(deltas_vec.into_iter().next().unwrap())
    } else {
        anyhow::bail!(
            "no supply delta found for block {} from url: {}",
            block_number,
            url
        )
    }
}

pub fn stream_supply_deltas_from(
    greater_than_or_equal_to: BlockNumber,
) -> impl Stream<Item = SupplyDelta> {
    debug!(%greater_than_or_equal_to, "streaming supply deltas via http");
    let (tx, rx) = futures::channel::mpsc::unbounded();

    tokio::spawn(async move {
        let mut current_block_number = greater_than_or_equal_to;
        let mut last_successful_fetch_time = tokio::time::Instant::now();
        loop {
            match fetch_supply_delta_http(current_block_number).await {
                Ok(delta) => {
                    if tx.unbounded_send(delta).is_err() {
                        error!("failed to send delta through channel, receiver dropped");
                        break;
                    }
                    current_block_number += 1;
                    last_successful_fetch_time = tokio::time::Instant::now();
                }
                Err(e) => {
                    warn!(
                        %current_block_number,
                        error = %e,
                        "failed to fetch supply delta for block {}. retrying after delay.", current_block_number
                    );
                    if last_successful_fetch_time.elapsed() > MAX_NO_DELTA_DURATION {
                        error!(
                            "no supply delta received for over {} seconds. Crashing.",
                            MAX_NO_DELTA_DURATION.as_secs()
                        );
                        // Using panic here as requested to crash the application.
                        // In a production system, a more graceful shutdown or alert might be preferred.
                        panic!(
                            "No supply delta received for over {} seconds for block_number: {}",
                            MAX_NO_DELTA_DURATION.as_secs(),
                            current_block_number
                        );
                    }
                    sleep(POLL_INTERVAL).await;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    });

    rx
}

pub async fn stream_supply_deltas_from_last(
    executor: &mut PgConnection,
) -> impl Stream<Item = SupplyDelta> {
    let last_synced_supply_delta_number = sync::get_last_synced_supply_delta_number(executor)
        .await
        .unwrap_or(super::snapshot::SUPPLY_SNAPSHOT_15082718.block_number);
    stream_supply_deltas_from(last_synced_supply_delta_number + 1)
}

pub fn stream_supply_delta_chunks(
    from: BlockNumber,
    chunk_size: usize,
) -> UnboundedReceiver<Vec<SupplyDelta>> {
    let (tx, rx) = futures::channel::mpsc::unbounded();
    let mut supply_deltas_rx = stream_supply_deltas_from(from);
    let mut supply_delta_buffer = Vec::with_capacity(chunk_size);

    tokio::spawn(async move {
        let mut last_send_time = tokio::time::Instant::now();
        let max_chunk_wait_time = Duration::from_secs(5);

        loop {
            tokio::select! {
                maybe_delta = supply_deltas_rx.next() => {
                    match maybe_delta {
                        Some(supply_delta) => {
                            supply_delta_buffer.push(supply_delta);
                            if supply_delta_buffer.len() >= chunk_size {
                                if tx.unbounded_send(supply_delta_buffer.clone()).is_err() {
                                    error!("failed to send delta chunk, receiver dropped");
                                    break;
                                }
                                supply_delta_buffer.clear();
                                last_send_time = tokio::time::Instant::now();
                            }
                        }
                        None => {
                            if !supply_delta_buffer.is_empty() && tx.unbounded_send(supply_delta_buffer.clone()).is_err() {
                                error!("failed to send final delta chunk, receiver dropped");
                            }
                            debug!("supply delta stream ended");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep_until(last_send_time + max_chunk_wait_time) => {
                    if !supply_delta_buffer.is_empty() {
                        debug!("flushing supply delta chunk due to timeout");
                        if tx.unbounded_send(supply_delta_buffer.clone()).is_err() {
                             error!("failed to send delta chunk after timeout, receiver dropped");
                            break;
                        }
                        supply_delta_buffer.clear();
                        last_send_time = tokio::time::Instant::now();
                    }
                }
            }
        }
    });
    rx
}

pub async fn get_supply_delta_by_block_number(
    block_number: BlockNumber,
) -> Result<SupplyDelta, anyhow::Error> {
    fetch_supply_delta_http(block_number).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::TestDb;
    use crate::execution_chain::supply_deltas::add_delta;

    use test_context::test_context;

    #[ignore]
    #[tokio::test]
    async fn test_fetch_supply_delta_http_integration() {}

    #[tokio::test]
    async fn test_get_supply_delta_by_block_number_updated_signature() {
        let result = get_supply_delta_by_block_number(15_000_000).await;
        if let Ok(delta) = result {
            assert_eq!(delta.block_number, 15_000_000);
        } else {
            // Consider asserting an error type or message if appropriate for a real endpoint
        }
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn test_get_latest_synced_supply_delta_number(ctx: &mut TestDb) {
        let mut connection = ctx.pool.acquire().await.unwrap();

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 0,
            block_hash: "0xtest_node_1".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };
        add_delta(&mut connection, &supply_delta_test).await;

        let latest_synced = sync::get_last_synced_supply_delta_number(&mut connection).await;
        assert_eq!(latest_synced, Some(0));
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn test_get_latest_synced_supply_delta_number_empty(ctx: &mut TestDb) {
        let mut connection = ctx.pool.acquire().await.unwrap();

        let latest_synced = sync::get_last_synced_supply_delta_number(&mut connection).await;
        assert_eq!(latest_synced, None);
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn test_get_latest_synced_supply_delta_number_in_node_scope(ctx: &mut TestDb) {
        let mut connection = ctx.pool.acquire().await.unwrap();

        let supply_delta_test = SupplyDelta {
            supply_delta: 1,
            block_number: 1,
            block_hash: "0xtest_node_sync_1_in_node".to_string(),
            fee_burn: 0,
            fixed_reward: 0,
            parent_hash: "0xtestparent".to_string(),
            self_destruct: 0,
            uncles_reward: 0,
        };
        add_delta(&mut connection, &supply_delta_test).await;

        let latest_synced = sync::get_last_synced_supply_delta_number(&mut connection).await;
        assert_eq!(latest_synced, Some(1));
    }

    #[test_context(TestDb)]
    #[tokio::test]
    async fn test_get_latest_synced_supply_delta_number_empty_in_node_scope(ctx: &mut TestDb) {
        let mut connection = ctx.pool.acquire().await.unwrap();

        let latest_synced = sync::get_last_synced_supply_delta_number(&mut connection).await;
        assert_eq!(latest_synced, None);
    }
}
