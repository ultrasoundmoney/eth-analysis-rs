//! This module helps sync gaps in our eth supply data.
//! Eth supply is calculated using an execution block, beacon deposits and beacon balances. Beacon
//! chain is leading, and beacon deposits we only calculate for slots with blocks, whereas balances
//! we can get every slot. Although at the time of writing not yet done, in the future we we should
//! be able to request an execution block for every beacon slot.

use anyhow::Result;
use pit_wall::Progress;
use tracing::warn;
use tracing::{debug, info};

use crate::beacon_chain::{BeaconStore, BeaconStorePostgres};
use crate::{
    beacon_chain::Slot,
    db,
    eth_supply::{self, SupplyPartsError, SupplyPartsStore},
    log,
};

// The first slot we have stored.
const FIRST_STORED_ETH_SUPPLY_SLOT: Slot = Slot(4697813);

pub async fn fill_gaps() -> Result<()> {
    log::init_with_env();

    info!("syncing gaps in eth supply");

    let db_pool = db::get_db_pool("sync-eth-supply-gaps", 3).await;
    let beacon_store = BeaconStorePostgres::new(db_pool.clone());

    let last_slot = beacon_store
        .get_last_state()
        .await
        .expect("a beacon state should be stored before trying to fill any gaps")
        .slot;

    debug!(
        %FIRST_STORED_ETH_SUPPLY_SLOT,
        %last_slot,
        "checking first stored slot to last slot for gaps"
    );

    let work_todo = last_slot.0 - FIRST_STORED_ETH_SUPPLY_SLOT.0;
    let mut progress = Progress::new("sync-eth-supply-gas", work_todo.try_into().unwrap());

    let supply_parts_store = SupplyPartsStore::new(&db_pool);

    for slot in (FIRST_STORED_ETH_SUPPLY_SLOT.0..=last_slot.0).map(Slot) {
        let stored_eth_supply = eth_supply::get_supply_exists_by_slot(&db_pool, slot).await?;
        if !stored_eth_supply {
            info!(%slot, "missing eth_supply, filling gap");

            let supply_parts = supply_parts_store.get(slot).await;

            match supply_parts {
                Err(SupplyPartsError::NoValidatorBalancesAvailable(_)) => {
                    warn!(%slot, "eth supply parts unavailable for slot");
                }
                Ok(supply_parts) => {
                    eth_supply::store(
                        &db_pool,
                        slot,
                        &supply_parts.block_number(),
                        &supply_parts.execution_balances_sum,
                        &supply_parts.beacon_balances_sum,
                        &supply_parts.beacon_deposits_sum,
                    )
                    .await?;
                }
            }

            info!("{}", progress.get_progress_string());
        } else {
            debug!(%slot, "slot looks fine");
        }

        progress.inc_work_done();
        if slot.0 % 100 == 0 {
            info!("{}", progress.get_progress_string());
        }
    }

    info!("done syncing gaps in eth supply");

    Ok(())
}
