use std::cmp::Ordering;

use anyhow::Result;
use sqlx::{Acquire, PgConnection};
use tracing::{debug, error, warn};

use crate::beacon_chain::{Slot, FIRST_POST_MERGE_SLOT};

use super::store;

/// Stores an eth supply for a given slot.
pub async fn sync_eth_supply(executor: &mut PgConnection, slot: &Slot) -> Result<()> {
    let last_stored_execution_balances_slot =
        store::get_last_stored_balances_slot(executor.acquire().await?).await?;

    let slots_to_store = match last_stored_execution_balances_slot {
        None => {
            warn!("no execution balances have ever been stored, skipping store eth supply");
            vec![]
        }
        Some(last_stored_execution_balances_slot) => {
            let last_stored_supply_slot =
                store::get_last_stored_supply_slot(executor.acquire().await?).await?;

            // Don't exceed currently syncing slot. We wouldn't have the balances.
            let sync_limit = last_stored_execution_balances_slot.min(*slot);

            match last_stored_supply_slot {
                None => {
                    debug!(
                        %FIRST_POST_MERGE_SLOT,
                        "eth supply has never been stored, starting from FIRST_POST_MERGE_SLOT"
                    );
                    let range = FIRST_POST_MERGE_SLOT.0..=sync_limit.0;
                    range.collect()
                }
                Some(last_stored_supply_slot) => match last_stored_supply_slot.cmp(&sync_limit) {
                    Ordering::Less => {
                        debug!("execution balances have updated, storing eth supply for new slots");
                        let first = last_stored_supply_slot + 1;
                        let range = first.0..=last_stored_execution_balances_slot.0;
                        range.collect()
                    }
                    Ordering::Equal => {
                        debug!("no new execution balances stored since last slot sync, skipping store eth supply");
                        vec![]
                    }
                    Ordering::Greater => {
                        error!("eth supply table is ahead of execution supply, did we miss a rollback? skipping store eth supply");
                        vec![]
                    }
                },
            }
        }
    };

    for slot in slots_to_store {
        debug!(
            slot,
            "storing eth supply for newly available execution balance slot"
        );
        store::store_supply_for_slot(executor, &Slot(slot)).await?;
    }

    Ok(())
}
