use anyhow::Result;
use chrono::Utc;
use futures::TryStreamExt;
use pit_wall::Progress;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{debug, info};

use crate::{
    beacon_chain::{balances, BeaconNode, Slot, FIRST_POST_LONDON_SLOT},
    db,
    execution_chain::LONDON_HARD_FORK_TIMESTAMP,
    log,
};

async fn backfill_balances(db_pool: &PgPool, work_todo: u64, daily_only: bool) -> Result<()> {
    let beacon_node = BeaconNode::new();

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
        FIRST_POST_LONDON_SLOT.0,
    )
    .fetch(db_pool)
    .try_filter(|row| {
        if daily_only {
            let is_first_of_day_slot = Slot(row.slot).is_first_of_day();
            futures::future::ready(is_first_of_day_slot)
        } else {
            futures::future::ready(true)
        }
    });

    let mut progress = Progress::new("backfill-beacon-balances", work_todo);

    while let Some(row) = rows.try_next().await? {
        debug!(row.slot, "getting validator balances");

        let validator_balances = beacon_node
            .get_validator_balances(&row.state_root)
            .await?
            .expect("expect block to exist for historic block_root");
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

    Ok(())
}

pub async fn backfill_balances_to_london() -> Result<()> {
    log::init_with_env();

    info!("backfilling beacon balances");

    let db_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db::get_db_url_with_name("backfill-beacon-balances"))
        .await?;

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
        FIRST_POST_LONDON_SLOT.0
    )
    .fetch_one(&db_pool)
    .await?;

    debug!(work_todo.count, "work todo");

    backfill_balances(&db_pool, work_todo.count.try_into().unwrap(), false).await?;

    info!("done backfilling beacon balances");

    Ok(())
}

pub async fn backfill_daily_balances_to_london() -> Result<()> {
    log::init_with_env();

    info!("backfilling daily beacon balances");

    let db_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db::get_db_url_with_name("backfill-beacon-balances"))
        .await?;

    let days_since_merge = (Utc::now() - *LONDON_HARD_FORK_TIMESTAMP).num_days();

    backfill_balances(&db_pool, days_since_merge.try_into().unwrap(), true).await?;

    info!("done backfilling daily beacon block hashes");

    Ok(())
}
