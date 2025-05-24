use sqlx::PgPool;
use tracing::{debug, info};

use crate::{
    execution_chain::{BlockNumber, BlockRange, GENESIS_SUPPLY},
    job_progress::JobProgress,
    key_value_store::KeyValueStorePostgres,
    units::Wei,
};

const BACKFILL_EXECUTION_SUPPLY_KEY: &str = "backfill-execution-supply";

const BLOCK_NUMBER_MIN_BEFORE_BACKFILL: BlockNumber = 15082719;

async fn bulk_insert_execution_supplies(
    pool: &PgPool,
    execution_supplies: &[(String, BlockNumber, i128)],
) {
    let block_hashes = execution_supplies
        .iter()
        .map(|(block_hash, _, _)| block_hash.as_str())
        .collect::<Vec<_>>();
    let block_numbers = execution_supplies
        .iter()
        .map(|(_, block_number, _)| *block_number)
        .collect::<Vec<_>>();
    let balances_sums = execution_supplies
        .iter()
        .map(|(_, _, balances_sum)| balances_sum.to_string())
        .collect::<Vec<_>>();

    sqlx::query!(
        "
        INSERT INTO execution_supply (block_hash, block_number, balances_sum)
        SELECT * FROM UNNEST($1::text[], $2::int4[], $3::numeric[])
        ON CONFLICT (block_hash) DO UPDATE SET
            balances_sum = excluded.balances_sum,
            block_number = excluded.block_number
        ",
        &block_hashes[..] as &[&str],
        &block_numbers[..],
        &balances_sums as &Vec<String>,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub async fn backfill_execution_supply(pool: &PgPool) {
    info!("initiating execution supply backfill");

    let key_value_store = KeyValueStorePostgres::new(pool.clone());
    let job_progress = JobProgress::new(BACKFILL_EXECUTION_SUPPLY_KEY, &key_value_store);

    let last_synced_block: Option<BlockNumber> = job_progress.get().await;

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
                ON CONFLICT (block_hash) DO NOTHING
                ",
                GENESIS_SUPPLY.0.to_string() as String,
            )
            .execute(pool)
            .await
            .unwrap();

            job_progress.set(&0).await;

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
            .fetch_one(pool)
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

    while last_supply.1 < BLOCK_NUMBER_MIN_BEFORE_BACKFILL - 1 {
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
        .fetch_all(pool)
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

        if supply_deltas.is_empty() {
            info!("no more supply deltas found, stopping execution supply backfill");
            break;
        }

        let new_execution_supplies: Vec<(String, i32, Wei)> = supply_deltas
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

        bulk_insert_execution_supplies(pool, &new_execution_supplies).await;

        job_progress.set(&last_supply.1).await;

        progress.inc_work_done_by(supply_deltas.len().try_into().unwrap());
        debug!(?last_supply, "stored execution supplies");
        debug!("{}", progress.get_progress_string());
    }
    info!("done backfilling execution supply");
}
