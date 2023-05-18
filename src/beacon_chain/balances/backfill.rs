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

enum Granularity {
    Slot,
    Hour,
    Day,
}

async fn backfill_balances(
    db_pool: &PgPool,
    work_todo: u64,
    granularity: Granularity,
) -> Result<()> {
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
    .try_filter(|row| match granularity {
        Granularity::Slot => futures::future::ready(true),
        Granularity::Hour => futures::future::ready(Slot(row.slot).is_first_of_hour()),
        Granularity::Day => futures::future::ready(Slot(row.slot).is_first_of_day()),
    });

    let mut progress = Progress::new("backfill-beacon-balances", work_todo);

    while let Some(row) = rows.try_next().await? {
        debug!(row.slot, "getting validator balances");

        let validator_balances = beacon_node
            .get_validator_balances(&row.state_root)
            .await?
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

    Ok(())
}

async fn get_slots_since_merge(db_pool: &PgPool) -> Result<u64> {
    let row = sqlx::query!(
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
    .fetch_one(db_pool)
    .await?;

    Ok(row.count.try_into().unwrap())
}

pub async fn backfill_balances_to_london() -> Result<()> {
    log::init_with_env();

    info!("backfilling beacon balances");

    let db_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db::get_db_url_with_name("backfill-beacon-balances"))
        .await?;

    let slot_count = get_slots_since_merge(&db_pool).await?;
    debug!(slot_count, "work todo");

    backfill_balances(&db_pool, slot_count, Granularity::Slot).await?;

    info!("done backfilling beacon balances");

    Ok(())
}

pub async fn backfill_hourly_balances_to_london() -> Result<()> {
    log::init_with_env();

    info!("backfilling hourly beacon balances");

    let db_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db::get_db_url_with_name("backfill-beacon-balances"))
        .await?;

    let hours_since_merge = (Utc::now() - *LONDON_HARD_FORK_TIMESTAMP).num_hours();

    backfill_balances(
        &db_pool,
        hours_since_merge.try_into().unwrap(),
        Granularity::Hour,
    )
    .await?;

    info!("done backfilling hourly beacon block hashes");

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

    backfill_balances(
        &db_pool,
        days_since_merge.try_into().unwrap(),
        Granularity::Day,
    )
    .await?;

    info!("done backfilling daily beacon block hashes");

    Ok(())
}
