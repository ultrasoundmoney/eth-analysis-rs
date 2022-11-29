//! This module helps sync gaps in our eth supply data.
//! Eth supply is calculated using an execution block, beacon deposits and beacon balances. Beacon
//! chain is leading, and beacon deposits we only calculate for slots with blocks, whereas balances
//! we can get every slot. Although at the time of writing not yet done, in the future we we should
//! be able to request an execution block for every beacon slot.

use anyhow::Result;
use pit_wall::Progress;
use sqlx::{Connection, PgConnection};
use tracing::warn;
use tracing::{debug, info};

use crate::eth_supply;
use crate::{
    beacon_chain::{self, Slot},
    db, log,
};

// The first slot we have stored.
const FIRST_STORED_ETH_SUPPLY_SLOT: Slot = 4697813;

pub async fn fill_gaps() -> Result<()> {
    log::init_with_env();

    info!("syncing gaps in eth supply");

    let mut db_connection =
        PgConnection::connect(&db::get_db_url_with_name("sync-eth-supply-gaps")).await?;

    let last_slot = beacon_chain::get_last_state(&mut db_connection)
        .await
        .expect("a beacon state should be stored before trying to fill any gaps")
        .slot;

    debug!(
        FIRST_STORED_ETH_SUPPLY_SLOT,
        last_slot, "checking first stored slot to last slot for gaps"
    );

    let mut progress = Progress::new(
        "sync-eth-supply-gas",
        (last_slot - FIRST_STORED_ETH_SUPPLY_SLOT)
            .try_into()
            .unwrap(),
    );

    for slot in FIRST_STORED_ETH_SUPPLY_SLOT..=last_slot {
        let stored_eth_supply =
            eth_supply::get_supply_exists_by_slot(&mut db_connection, &slot).await?;
        if !stored_eth_supply {
            info!(slot, "missing eth_supply, filling gap");

            let supply_parts = eth_supply::get_supply_parts(&mut db_connection, &slot).await?;

            match supply_parts {
                None => {
                    warn!(slot, "eth supply parts unavailable for slot");
                }
                Some(supply_parts) => {
                    eth_supply::store(
                        &mut db_connection,
                        &slot,
                        &supply_parts.block_number(),
                        &supply_parts.execution_balances_sum_next,
                        &supply_parts.beacon_balances_sum_next,
                        &supply_parts.beacon_deposits_sum_next,
                    )
                    .await?;
                }
            }

            info!("{}", progress.get_progress_string());
        } else {
            debug!(slot, "slot looks fine");
        }

        progress.inc_work_done();
        if slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
        }
    }

    info!("done syncing gaps in eth supply");

    Ok(())
}
