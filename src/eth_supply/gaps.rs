//! This module helps sync gaps in our eth supply data.
//! Eth supply is calculated using an execution block, beacon deposits and beacon balances. Beacon
//! chain is leading, and beacon deposits we only calculate for slots with blocks, whereas balances
//! we can get every slot. Although at the time of writing not yet done, in the future we we should
//! be able to request an execution block for every beacon slot.

use crate::eth_supply::get_supply_parts;
use crate::eth_supply::BeaconBalancesSum;
use anyhow::Result;
use pit_wall::Progress;
use sqlx::{Connection, PgConnection};
use tracing::{debug, info};

use crate::{
    beacon_chain::{self, Slot},
    db, eth_supply, log,
};

// The first slot we have stored.
const FIRST_STORED_ETH_SUPPLY_SLOT: Slot = 4697813;

pub async fn sync_gaps() -> Result<()> {
    log::init_with_env();

    info!("syncing gaps in eth supply");

    let mut db_connection =
        PgConnection::connect(&db::get_db_url_with_name("sync-eth-supply-gaps")).await?;

    let beacon_node = beacon_chain::BeaconNode::new();
    let last_slot = beacon_chain::get_last_state(&mut db_connection)
        .await
        .expect("a beacon state should be stored before trying to fill any gaps")
        .slot;

    debug!(
        FIRST_STORED_ETH_SUPPLY_SLOT,
        last_slot, "checking first stored to last for gaps"
    );

    let mut progress = Progress::new(
        "sync-eth-supply-gas",
        (last_slot - FIRST_STORED_ETH_SUPPLY_SLOT).into(),
    );
    for slot in FIRST_STORED_ETH_SUPPLY_SLOT..=last_slot {
        if slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
        }

        let stored_eth_supply =
            eth_supply::get_supply_exists_by_slot(&mut db_connection, &slot).await?;
        if stored_eth_supply {
            progress.inc_work_done();
            debug!(slot, "slot looks fine");
            continue;
        }

        let state_root = beacon_chain::get_state_root_by_slot(&mut db_connection, &slot).await?;
        let validator_balances = beacon_node
            .get_validator_balances(&state_root)
            .await
            .unwrap();
        let validator_balances_sum = beacon_chain::sum_validator_balances(validator_balances);
        let beacon_balances_sum = BeaconBalancesSum {
            balances_sum: validator_balances_sum,
            slot: slot.clone(),
        };
        let eth_supply_parts = get_supply_parts(&mut db_connection, beacon_balances_sum).await?;

        eth_supply::store(&mut db_connection, &eth_supply_parts).await?;

        progress.inc_work_done();
        info!(slot, "filled gap at slot");
        info!("{}", progress.get_progress_string());
    }

    info!("done syncing gaps in eth supply");

    Ok(())
}
