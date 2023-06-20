use futures::TryStreamExt;
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info};

use crate::beacon_chain::{balances, BeaconNode, Slot};

pub enum Granularity {
    Slot,
    Hour,
    Day,
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
        Granularity::Hour => slots_count / 300,
        Granularity::Day => slots_count / 7200,
    }
    .try_into()
    .unwrap()
}

pub async fn backfill_balances(db_pool: &PgPool, granularity: &Granularity, from: &Slot) {
    let beacon_node = BeaconNode::new();

    debug!("estimating work to be done");
    let work_todo = estimate_work_todo(db_pool, granularity, from).await;
    debug!("estimated work to be done: {} slots", work_todo);
    let mut progress = Progress::new("backfill-beacon-balances", work_todo);

    let mut rows = sqlx::query!(
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
    .fetch(db_pool)
    .try_filter(|row| match granularity {
        Granularity::Slot => futures::future::ready(true),
        Granularity::Hour => futures::future::ready(Slot(row.slot).is_first_of_hour()),
        Granularity::Day => futures::future::ready(Slot(row.slot).is_first_of_day()),
    });

    while let Some(row) = rows.try_next().await.unwrap() {
        debug!(row.slot, "getting validator balances");

        let validator_balances = beacon_node
            .get_validator_balances(&row.state_root)
            .await
            .unwrap()
            .expect("expect balances to exist for every state root");
        let balances_sum = balances::sum_validator_balances(&validator_balances);

        balances::store_validators_balance(
            db_pool,
            &row.state_root,
            &row.slot.into(),
            &balances_sum,
        )
        .await;

        progress.inc_work_done();

        info!("{}", progress.get_progress_string());
    }
}
