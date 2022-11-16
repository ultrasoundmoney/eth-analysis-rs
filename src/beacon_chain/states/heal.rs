use crate::{
    beacon_chain::{sync, Slot},
    key_value_store,
};
use anyhow::Result;
use pit_wall::Progress;
use sqlx::{postgres::PgPoolOptions, PgExecutor};
use tracing::{debug, info, warn};

use crate::{
    beacon_chain::{self, BeaconNode},
    db, log,
};

// The first slot we have stored.
const FIRST_STORED_ETH_SUPPLY_SLOT: Slot = 4697813;

const HEAL_BEACON_STATES_KEY: &str = "heal-beacon-states";

async fn store_last_checked(executor: impl PgExecutor<'_>, slot: &Slot) -> Result<()> {
    key_value_store::set_serializable_value(executor, HEAL_BEACON_STATES_KEY, slot).await?;
    Ok(())
}

async fn get_last_checked(executor: impl PgExecutor<'_>) -> Result<Option<Slot>> {
    let slot =
        key_value_store::get_deserializable_value::<Slot>(executor, HEAL_BEACON_STATES_KEY).await?;
    Ok(slot)
}

pub async fn heal_beacon_states() -> Result<()> {
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
    let last_checked = get_last_checked(&db_pool).await?;
    let earliest = last_checked.unwrap_or(FIRST_STORED_ETH_SUPPLY_SLOT);

    debug!(
        earliest,
        last_slot, "checking first stored slot to last slot for gaps"
    );

    let mut progress = Progress::new("heal-beacon-states", (last_slot - earliest).into());

    for slot in earliest..=last_slot {
        if slot % 100 == 0 {
            store_last_checked(&db_pool, &slot).await?;
            info!("{}", progress.get_progress_string());
        }

        let stored_state_root = beacon_chain::get_state_root_by_slot(&db_pool, &slot)
            .await?
            .expect("expect some historic state_root to have been stored");
        let state_root = beacon_node
            .get_state_root_by_slot(&slot)
            .await?
            .expect("expect state_root to exist for historic slots");

        if stored_state_root != state_root {
            warn!("state roon mismatch, rolling back stored and resyncing");
            sync::rollback_slot(&mut *db_pool.acquire().await?, &slot).await?;
            sync::sync_state_root(&db_pool, &beacon_node, &state_root, &slot).await?;
            info!(slot, "healed state at slot");
        }

        progress.inc_work_done();
    }

    info!("done syncing gaps in eth supply");
    Ok(())
}
