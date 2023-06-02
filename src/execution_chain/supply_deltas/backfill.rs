use serde_json::{json, Value};
use sqlx::{postgres::PgRow, PgExecutor, Row};
use tracing::{debug, info};

use crate::{
    db,
    execution_chain::{BlockNumber, BlockRange, GENESIS_SUPPLY},
    log,
    units::Wei,
};

const BACKFILL_EXECUTION_SUPPLY_KEY: &str = "backfill-execution-supply";

async fn get_last_synced_block(executor: impl PgExecutor<'_>) -> Option<BlockNumber> {
    sqlx::query(
        "
        SELECT
            value
        FROM
            key_value_store
        WHERE
            key = $1
        ",
    )
    .bind(BACKFILL_EXECUTION_SUPPLY_KEY)
    .map(|row: PgRow| {
        let value: Value = row.get("value");
        serde_json::from_value::<BlockNumber>(value).unwrap()
    })
    .fetch_optional(executor)
    .await
    .unwrap()
}

async fn set_last_synced_block(executor: impl PgExecutor<'_>, block: BlockNumber) {
    sqlx::query(
        "
        INSERT INTO
            key_value_store (key, value)
        VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET
            value = excluded.value
        ",
    )
    .bind(BACKFILL_EXECUTION_SUPPLY_KEY)
    .bind(json!(block))
    .execute(executor)
    .await
    .unwrap();
}

const BLOCK_NUMBER_MIN_BEFORE_BACKFILL: BlockNumber = 15082719;

pub async fn backfill_execution_supply() {
    log::init_with_env();

    let db_pool = db::get_db_pool("backfill-execution-supply").await;

    let last_synced_block = get_last_synced_block(&db_pool).await;

    let mut last_supply: (String, BlockNumber, Wei) = match last_synced_block {
        None => {
            // Store genensis supply and return it as the last synced block.
            sqlx::query!(
                "
                INSERT INTO execution_supply (block_hash, block_number, balances_sum) VALUES (
                    '0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3',
                    0,
                    $1::NUMERIC
                )
                ",
                GENESIS_SUPPLY.0.to_string() as String,
            )
            .execute(&db_pool)
            .await
            .unwrap();

            set_last_synced_block(&db_pool, 0).await;

            (
                "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3".to_string(),
                0,
                GENESIS_SUPPLY.0,
            )
        }
        Some(last_synced_block) => {
            // Get the supply of the last synced block from the db.
            let row = sqlx::query!(
                "
                SELECT
                    block_hash,
                    block_number,
                    balances_sum::TEXT
                FROM
                    execution_supply
                WHERE
                    block_number = $1
                ",
                last_synced_block
            )
            .fetch_one(&db_pool)
            .await
            .unwrap();
            let balances_sum = row.balances_sum.unwrap().parse::<Wei>().unwrap();
            (row.block_hash, row.block_number, balances_sum)
        }
    };

    let work_todo = BLOCK_NUMBER_MIN_BEFORE_BACKFILL - last_synced_block.unwrap_or(0);
    let mut progress =
        pit_wall::Progress::new("backfill-execution-supply", work_todo.try_into().unwrap());
    info!(?work_todo, "backfilling execution supply");

    debug!(
        balances_sum = last_supply.2,
        block_number = last_supply.1,
        "found earliest stored supply"
    );

    const BULK_INSERT_SIZE: i32 = 10000;

    while last_supply.1 < BLOCK_NUMBER_MIN_BEFORE_BACKFILL {
        // To get execution supply n, we add execution supply n - 1 to the delta for block n.
        let next_range = BlockRange::new(
            last_supply.1 + 1,
            (last_supply.1 + BULK_INSERT_SIZE).min(BLOCK_NUMBER_MIN_BEFORE_BACKFILL - 1),
        );

        debug!("storing execution supplies for {:?}", next_range);

        let supply_deltas = sqlx::query!(
            "
            SELECT block_number, block_hash, parent_hash, supply_delta::TEXT FROM execution_supply_deltas
            WHERE block_number >= $1 AND block_number <= $2
            ORDER BY block_number ASC
            ",
            next_range.start,
            next_range.end,
        )
        .fetch_all(&db_pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            let supply_delta = row.supply_delta.unwrap().parse::<Wei>().unwrap();
            (
                row.block_number,
                row.block_hash,
                row.parent_hash,
                supply_delta,
            )
        })
        .collect::<Vec<_>>();

        let new_execution_supplies: Vec<(String, BlockNumber, Wei)> = supply_deltas
            .iter()
            .map(|row| {
                // We calculate the next supply by taking the last supply we synced,
                // then take the delta for that block, and adding the delta we get the
                // next execution supply.
                let block_number = row.0;
                let block_hash = row.1.clone();
                let balances_sum = last_supply.2 + row.3;

                last_supply = (block_hash.clone(), block_number, balances_sum);

                (block_hash, block_number, balances_sum)
            })
            .collect();

        let mut query_string =
            "INSERT INTO execution_supply (block_hash, block_number, balances_sum) VALUES"
                .to_string();
        let mut values = Vec::new();

        for (i, (block_hash, block_number, balances_sum)) in
            new_execution_supplies.into_iter().enumerate()
        {
            query_string.push_str(&format!(
                " (${}, ${}, ${}::NUMERIC)",
                3 * i + 1,
                3 * i + 2,
                3 * i + 3
            ));

            if i != supply_deltas.len() - 1 {
                query_string.push(',');
            }

            values.push(block_hash);
            values.push(block_number.to_string());
            values.push(balances_sum.to_string());
        }

        let mut query = sqlx::query(&query_string);

        for value in values {
            query = query.bind(value);
        }

        query.execute(&db_pool).await.unwrap();
        set_last_synced_block(&db_pool, last_supply.1).await;

        progress.inc_work_done_by(supply_deltas.len().try_into().unwrap());
        debug!(?last_supply, "stored execution supplies");
        debug!("{}", progress.get_progress_string());
    }
}
