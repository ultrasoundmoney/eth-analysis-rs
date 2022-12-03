use anyhow::Result;
use futures::TryStreamExt;
use pit_wall::Progress;
use sqlx::postgres::PgPoolOptions;
use tracing::{debug, info};

use crate::{
    beacon_chain::{balances, BeaconNode, FIRST_POST_LONDON_SLOT},
    db, log,
};

pub async fn backfill_balances_to_london() -> Result<()> {
    log::init_with_env();

    info!("backfilling beacon balances");

    let db_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db::get_db_url_with_name("backfill-beacon-balances"))
        .await?;

    let beacon_node = BeaconNode::new();
    let first_slot = FIRST_POST_LONDON_SLOT;

    let work_todo = sqlx::query!(
        r#"
            SELECT
                COUNT(*) AS "count!"
            FROM
                beacon_states
            LEFT JOIN beacon_validators_balance ON
                beacon_states.state_root = beacon_validators_balance.state_root
            WHERE
                slot >= $1
            AND
                beacon_validators_balance.state_root IS NULL
        "#,
        first_slot
    )
    .fetch_one(&db_pool)
    .await?;

    debug!(work_todo.count, "work todo");

    let mut rows = sqlx::query!(
        r#"
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
        "#,
        first_slot,
    )
    .fetch(&db_pool);

    let mut progress = Progress::new(
        "backfill-beacon-balances",
        work_todo.count.try_into().unwrap(),
    );

    while let Some(row) = rows.try_next().await? {
        debug!(row.slot, "getting validator balances");
        let validator_balances = beacon_node
            .get_validator_balances(&row.state_root)
            .await?
            .expect("expect block to exist for historic block_root");
        let balances_sum = balances::sum_validator_balances(&validator_balances);

        balances::store_validators_balance(&db_pool, &row.state_root, &row.slot, &balances_sum)
            .await?;

        progress.inc_work_done();

        if row.slot % 100 == 0 {
            info!("{}", progress.get_progress_string());
        }
    }

    info!("done healing beacon block hashes");

    Ok(())
}
