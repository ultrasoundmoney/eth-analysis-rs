use futures::{pin_mut, StreamExt};
use pit_wall::Progress;
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::beacon_chain::{
    balances,
    node::{BeaconNodeHttp, ValidatorBalance},
    BeaconNode, Slot,
};

const GET_BALANCES_CONCURRENCY_LIMIT: usize = 8;

#[derive(Debug)]
pub enum Granularity {
    Day,
    Epoch,
    Hour,
    Slot,
}

#[derive(sqlx::FromRow, Debug)]
struct SlotRow {
    slot: i32,
}

async fn estimate_work_todo(
    db_pool: &PgPool,
    granularity: &Granularity,
    start_slot_opt: Option<Slot>,
    end_slot_opt: Option<Slot>,
) -> u64 {
    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        r#"
        SELECT
            beacon_states.slot
        FROM
            beacon_states
        LEFT JOIN beacon_validators_balance ON
            beacon_states.state_root = beacon_validators_balance.state_root
        WHERE
            beacon_validators_balance.state_root IS NULL
        "#,
    );

    let from_slot = start_slot_opt.unwrap_or(Slot(0));
    query_builder.push(" AND slot >= ");
    query_builder.push_bind(from_slot.0);

    if let Some(end_slot) = end_slot_opt {
        query_builder.push(" AND slot <= ");
        query_builder.push_bind(end_slot.0);
    }

    let rows: Vec<i32> = query_builder
        .build_query_scalar()
        .fetch_all(db_pool)
        .await
        .unwrap_or_else(|e| {
            warn!("failed to fetch rows for work estimation: {:?}", e);
            Vec::new()
        });

    let count = match granularity {
        Granularity::Slot => rows.len(),
        Granularity::Epoch => rows
            .iter()
            .filter(|&&s| Slot(s).is_first_of_epoch())
            .count(),
        Granularity::Hour => rows.iter().filter(|&&s| Slot(s).is_first_of_hour()).count(),
        Granularity::Day => rows.iter().filter(|&&s| Slot(s).is_first_of_day()).count(),
    };

    count as u64
}

// Define an outcome enum for processing each item
enum BackfillItemOutcome {
    StoreBalances(String, i32, Vec<ValidatorBalance>), // state_root_from_header, slot, balances
    HeaderExistsNoBalances(String, i32),               // state_root_from_header, slot
    SkippedMissedSlot(i32),                            // slot
    SkippedError(i32, String),                         // slot, error details
}

pub async fn backfill_balances(
    db_pool: &PgPool,
    granularity: &Granularity,
    start_slot_opt: Option<Slot>,
    end_slot_opt: Option<Slot>,
) {
    let beacon_node = BeaconNodeHttp::new_from_env();
    let from_slot = start_slot_opt.unwrap_or(Slot(0));

    debug!("estimating work to be done for backfill");
    let work_todo = estimate_work_todo(db_pool, granularity, start_slot_opt, end_slot_opt).await;
    debug!(
        ?start_slot_opt,
        ?end_slot_opt,
        "estimated work to be done for backfill: {} items matching granularity",
        work_todo
    );
    let mut progress = Progress::new("backfill-beacon-balances", work_todo);

    let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        r#"
        SELECT
            beacon_blocks.slot
        FROM
            beacon_blocks
        LEFT JOIN beacon_validators_balance ON
            beacon_blocks.state_root = beacon_validators_balance.state_root
        WHERE
            beacon_validators_balance.state_root IS NULL
        "#,
    );

    query_builder.push(" AND slot >= ");
    query_builder.push_bind(from_slot.0);

    if let Some(end_slot) = end_slot_opt {
        query_builder.push(" AND slot <= ");
        query_builder.push_bind(end_slot.0);
    }

    query_builder.push(" ORDER BY slot DESC");

    let rows = query_builder.build_query_as::<SlotRow>().fetch(db_pool);

    // filter_map closure must return a Future<Output = Option<Item>>
    let rows_filtered = rows.filter_map(|row_result| async move {
        match row_result {
            Ok(row) => {
                let slot_for_filter = Slot(row.slot);
                match granularity {
                    Granularity::Slot => Some(row),
                    Granularity::Epoch => {
                        if slot_for_filter.is_first_of_epoch() {
                            Some(row)
                        } else {
                            None
                        }
                    }
                    Granularity::Hour => {
                        if slot_for_filter.is_first_of_hour() {
                            Some(row)
                        } else {
                            None
                        }
                    }
                    Granularity::Day => {
                        if slot_for_filter.is_first_of_day() {
                            Some(row)
                        } else {
                            None
                        }
                    }
                }
            }
            Err(e) => {
                warn!("error fetching row for backfill: {:?}", e);
                None
            }
        }
    });

    // rows_filtered is now Stream<Item = Record>
    // .map closure takes Record and returns Future<Output = BackfillItemOutcome>
    let tasks = rows_filtered.map(move |row| {
        // row is { state_root: String (original from DB), slot: i64 }
        let beacon_node_clone = beacon_node.clone();
        async move {
            let current_slot_val_i32 = row.slot;
            let slot_obj = Slot(current_slot_val_i32);
            match beacon_node_clone.get_header_by_slot(slot_obj).await {
                Ok(Some(header_envelope)) => {
                    debug!(slot = %slot_obj, "backfill: header found.");
                    let state_root_from_header = header_envelope.state_root();
                    match beacon_node_clone.get_validator_balances_by_slot(slot_obj).await {
                        Ok(Some(validator_balances)) => {
                            debug!(slot = %slot_obj, state_root = %state_root_from_header, "backfill: validator balances successfully fetched.");
                            BackfillItemOutcome::StoreBalances(state_root_from_header.clone(), current_slot_val_i32, validator_balances)
                        }
                        Ok(None) => {
                            warn!(slot = %slot_obj, state_root = %state_root_from_header, "backfill: beacon node reported no validator balances for slot (using slot-based fetch).");
                            BackfillItemOutcome::HeaderExistsNoBalances(state_root_from_header.clone(), current_slot_val_i32)
                        }
                        Err(e) => {
                            warn!(slot = %slot_obj, state_root = %state_root_from_header, "backfill: failed to get validator balances by slot: {}", e.to_string());
                            BackfillItemOutcome::SkippedError(current_slot_val_i32, format!("getting balances for slot {slot_obj}: {e}"))
                        }
                    }
                }
                Ok(None) => {
                    debug!(slot = %slot_obj, "backfill: slot missed (no header found), skipping.");
                    BackfillItemOutcome::SkippedMissedSlot(current_slot_val_i32)
                }
                Err(e) => {
                    warn!(slot = %slot_obj, "backfill: failed to get header: {}. skipping slot.", e.to_string());
                    BackfillItemOutcome::SkippedError(current_slot_val_i32, format!("getting header for slot {slot_obj}: {e}"))
                }
            }
        }
    });

    let buffered_tasks = tasks.buffered(GET_BALANCES_CONCURRENCY_LIMIT);

    pin_mut!(buffered_tasks);

    while let Some(outcome) = buffered_tasks.next().await {
        match outcome {
            BackfillItemOutcome::StoreBalances(
                state_root_to_store,
                slot_val,
                validator_balances,
            ) => {
                let slot_obj = Slot(slot_val);
                debug!(slot = %slot_obj, state_root = %state_root_to_store, "backfill: attempting to store balances");
                let balances_sum = balances::sum_validator_balances(&validator_balances);
                balances::store_validators_balance(
                    db_pool,
                    &state_root_to_store,
                    slot_obj,
                    &balances_sum,
                )
                .await;
                info!(slot = %slot_obj, state_root = %state_root_to_store, "backfill: successfully stored validator balances");
            }
            BackfillItemOutcome::HeaderExistsNoBalances(state_root_ref, slot_val) => {
                let slot_obj = Slot(slot_val);
                info!(slot = %slot_obj, state_root = %state_root_ref, "backfill: header existed but no balances found; skipped storage.");
            }
            BackfillItemOutcome::SkippedMissedSlot(slot_val) => {
                let slot_obj = Slot(slot_val);
                info!(slot = %slot_obj, "backfill: slot was missed on-chain; skipped.");
            }
            BackfillItemOutcome::SkippedError(slot_val, err_msg) => {
                let slot_obj = Slot(slot_val);
                warn!(slot = %slot_obj, error = %err_msg, "backfill: slot skipped due to error.");
            }
        }

        progress.inc_work_done();
        // Log progress periodically or when done.
        if progress.work_done % 100 == 0 || progress.work_done >= work_todo {
            info!("backfill progress: {}", progress.get_progress_string());
        }
    }
    info!(
        "beacon balances backfill process finished. Final progress: {}",
        progress.get_progress_string()
    );
}
