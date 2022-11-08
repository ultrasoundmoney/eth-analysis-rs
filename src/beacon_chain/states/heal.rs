use crate::beacon_chain::{sync, Slot};
use anyhow::Result;
use pit_wall::Progress;
use sqlx::postgres::PgPoolOptions;
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{self, BeaconNode},
    db, log,
};

// The first slot we have stored.
const FIRST_STORED_ETH_SUPPLY_SLOT: Slot = 4697813;

pub async fn heal_state_roots() -> Result<()> {
    log::init_with_env();

    info!("healing reorged states");

    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db::get_db_url_with_name("heal-beacon-states"))
        .await?;

    let beacon_node = BeaconNode::new();
    let last_slot = beacon_chain::get_last_state(&db_pool)
        .await
        .expect("a beacon state should be stored before trying to heal any")
        .slot;

    debug!(
        FIRST_STORED_ETH_SUPPLY_SLOT,
        last_slot, "checking first stored slot to last slot for gaps"
    );

    let mut progress = Progress::new(
        "heal-beacon-states",
        (last_slot - FIRST_STORED_ETH_SUPPLY_SLOT).into(),
    );
    for slot in FIRST_STORED_ETH_SUPPLY_SLOT..=last_slot {
        if slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
        }

        let stored_state_root = beacon_chain::get_state_root_by_slot(&db_pool, &slot).await?;
        let envelope = beacon_node
            .get_header_by_slot(&slot)
            .await?
            .expect("expect historic slots to exist");
        let state_root = envelope.header.message.state_root;

        if stored_state_root != state_root {
            warn!(
                stored_state_root,
                state_root, "found bad stored state_root, healing"
            );

            sync::rollback_slots(&mut *db_pool.acquire().await?, &slot).await?;

            sync::sync_slot(&db_pool, &beacon_node, &slot).await?;
        }

        progress.inc_work_done();
        info!(slot, "healed state at slot");
        info!("{}", progress.get_progress_string());
    }

    info!("done syncing gaps in eth supply");
    Ok(())
}
