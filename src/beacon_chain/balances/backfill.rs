use futures::{pin_mut, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::{balances, node::BeaconNodeHttp, BeaconNode, Slot};

const SLOTS_PER_EPOCH: i64 = 32;

pub enum Granularity {
    Day,
    Epoch,
    Hour,
    Slot,
}

async fn estimate_work_todo(db_pool: &PgPool, granularity: &Granularity, from: &Slot) -> u64 {
    let slots_count = sqlx::query!(
        "
        SELECT
            COUNT(beacon_states.slot) as \"count!\"
        FROM
            beacon_states
        LEFT JOIN beacon_validators_balance ON
            beacon_states.state_root = beacon_validators_balance.state_root
        WHERE
            slot >= $1
        AND
            beacon_validators_balance.state_root IS NULL
        ",
        from.0,
    )
    .fetch_one(db_pool)
    .await
    .unwrap()
    .count;

    match granularity {
        Granularity::Slot => slots_count,
        Granularity::Epoch => slots_count * SLOTS_PER_EPOCH,
        Granularity::Hour => slots_count / 300,
        Granularity::Day => slots_count / 7200,
    }
    .try_into()
    .unwrap()
}

pub async fn backfill_balances(db_pool: &PgPool, granularity: &Granularity, from: &Slot) {
    let beacon_node = BeaconNodeHttp::new();

    debug!("estimating work to be done");
    let work_todo = estimate_work_todo(db_pool, granularity, from).await;
    debug!("estimated work to be done: {} slots", work_todo);
    let mut progress = Progress::new("backfill-beacon-balances", work_todo);

    let rows = sqlx::query!(
        "
        SELECT
            beacon_states.state_root,
            beacon_states.slot
        FROM
            beacon_states
        LEFT JOIN beacon_validators_balance ON
            beacon_states.state_root = beacon_validators_balance.state_root
        WHERE
            slot >= $1
        AND
            beacon_validators_balance.state_root IS NULL
        ORDER BY slot DESC
        ",
        from.0,
    )
    .fetch(db_pool);

    let rows_filtered = rows.filter_map(|row| async move {
        if let Ok(row) = row {
            match granularity {
                Granularity::Slot => Some(row),
                Granularity::Epoch => {
                    if Slot(row.slot).is_first_of_epoch() {
                        Some(row)
                    } else {
                        None
                    }
                }
                Granularity::Hour => {
                    if Slot(row.slot).is_first_of_hour() {
                        Some(row)
                    } else {
                        None
                    }
                }
                Granularity::Day => {
                    if Slot(row.slot).is_first_of_day() {
                        Some(row)
                    } else {
                        None
                    }
                }
            }
        } else {
            None
        }
    });

    let tasks = rows_filtered.map(|row| {
        let beacon_node_clone = beacon_node.clone();
        async move {
            let validator_balances = beacon_node_clone
                .get_validator_balances(&row.state_root)
                .await
                .unwrap();
            (row.state_root, row.slot, validator_balances)
        }
    });

    let buffered_tasks = tasks.buffered(32); // Run at most eight in parallel.

    pin_mut!(buffered_tasks);

    while let Some((state_root, slot, balances_result)) = buffered_tasks.next().await {
        debug!(slot, "fetching validator balances");

        let validator_balances = {
            match balances_result {
                Some(validator_balances) => validator_balances,
                None => {
                    warn!(state_root, slot, "state_root without validator balances",);
                    progress.inc_work_done();
                    continue;
                }
            }
        };

        let balances_sum = balances::sum_validator_balances(&validator_balances);

        balances::store_validators_balance(db_pool, &state_root, &slot.into(), &balances_sum).await;

        progress.inc_work_done();

        info!("{}", progress.get_progress_string());
    }
}
