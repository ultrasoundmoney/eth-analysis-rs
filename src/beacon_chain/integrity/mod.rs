use anyhow::{bail, Context, Result};
use sqlx::PgExecutor;
use tracing::{debug, info, warn};

use crate::beacon_chain::{BeaconNode, Slot, GENESIS_PARENT_ROOT};

#[derive(Debug, Clone)]
struct BlockLink {
    slot: Slot,
    root: String,
    parent_root: String,
}

/// Checks the integrity of the beacon_blocks chain by verifying parent-child relationships.
/// Can start from a specific slot if its parent root is known.
/// Missed slots (gaps in slot numbers) are considered normal.
pub async fn check_beacon_block_chain_integrity<BN: BeaconNode + Send + Sync + ?Sized>(
    executor: impl PgExecutor<'_> + Copy,
    beacon_node: &BN,
    start_from_slot_opt: Option<Slot>,
) -> Result<()> {
    let start_slot = start_from_slot_opt.unwrap_or(Slot(0));

    info!(
        start_slot = %start_slot,
        "starting beacon block chain integrity check (v14: unified chunk query)"
    );

    let mut expected_block_parent_root: String;
    let mut prev_block_slot_for_log: Option<Slot>;

    if start_slot == Slot(0) {
        expected_block_parent_root = GENESIS_PARENT_ROOT.to_string();
        prev_block_slot_for_log = None;
    } else {
        info!(%start_slot, "fetching header from beacon node to determine initial expected parent root");
        match beacon_node.get_header_by_slot(start_slot).await? {
            Some(header_of_start_slot) => {
                expected_block_parent_root = header_of_start_slot.parent_root();
                prev_block_slot_for_log = Some(Slot(start_slot.0 - 1));
                info!(%expected_block_parent_root, slot = %start_slot, "determined initial expected parent root from node header");
            }
            None => bail!(
                "cannot determine starting conditions: beacon node has no header for start_slot {}",
                start_slot
            ),
        }
    }

    // Determine upfront whether the canonical chain actually contains a block at `start_slot`.
    // If it does and our database is missing it, we fail fast instead of continuing the scan.
    let node_has_block_at_start_slot = beacon_node.get_block_by_slot(start_slot).await?.is_some();

    if node_has_block_at_start_slot {
        let db_has_block: Option<bool> = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM beacon_blocks WHERE slot = $1)"#,
            start_slot.0
        )
        .fetch_one(executor)
        .await
        .context(format!(
            "failed to check for presence of start_slot {} in db",
            start_slot
        ))?;

        if db_has_block != Some(true) {
            bail!(
                "db missing block at start_slot {} while beacon node reports a block present",
                start_slot
            );
        }
    }

    let chunk_size: i64 = 1000;
    let mut offset: i64 = 0;
    // Counter used purely for logging purposes.
    let mut total_blocks_processed = 0;

    loop {
        debug!(%start_slot, %offset, "fetching block chunk");
        let chunk = sqlx::query_as!(
            BlockLink,
            r#"
            SELECT slot AS "slot!: _", block_root AS root, parent_root
            FROM beacon_blocks
            WHERE slot IS NOT NULL AND slot >= $1
            ORDER BY slot ASC
            LIMIT $2 OFFSET $3
            "#,
            start_slot.0,
            chunk_size,
            offset
        )
        .fetch_all(executor)
        .await
        .context(format!(
            "failed to fetch block chunk with start_slot_filter {} and offset {}",
            start_slot, offset
        ))?;

        if chunk.is_empty() {
            if total_blocks_processed == 0 && offset == 0 {
                if start_from_slot_opt.is_some() {
                    bail!("no blocks found at or after the user-specified start_slot: {:?}. DB might be empty in this range or start slot too high.", start_from_slot_opt.unwrap());
                } else {
                    info!("no blocks found (database is empty from slot 0). integrity check passed (empty case).");
                }
            }
            debug!("finished processing all chunks or no more matching blocks found.");
            break;
        }

        for current_block in chunk {
            total_blocks_processed += 1;

            if current_block.parent_root != expected_block_parent_root {
                warn!(current_slot = %current_block.slot, current_root = %current_block.root, actual_parent = %current_block.parent_root, expected_parent = %expected_block_parent_root, prev_block_context_slot = ?prev_block_slot_for_log, "on-chain verification needed: parent mismatch");
                bail!(
                    "integrity failure: parent mismatch at slot {}. current block's parent_root ({}) does not match expected parent_root ({}). context: expected parent was from block at/before slot ~{:?}.",
                    current_block.slot, current_block.parent_root, expected_block_parent_root, prev_block_slot_for_log.unwrap_or(start_slot)
                );
            }

            expected_block_parent_root = current_block.root.clone();
            prev_block_slot_for_log = Some(current_block.slot);
        }
        offset += chunk_size;
    }

    if total_blocks_processed == 0 && start_from_slot_opt.is_none() {
        // This case is already handled by the info log inside the loop when chunk is empty for offset 0.
        // If start_from_slot_opt was Some and no blocks, it would have bailed.
    } else if total_blocks_processed > 0 {
        info!(
            total_blocks_processed,
            "beacon block chain integrity check (v14) passed successfully"
        );
    }
    Ok(())
}
