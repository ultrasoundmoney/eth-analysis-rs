use anyhow::Result;
use std::path::Path;
use tokio::fs;
use tracing::{debug, error, warn};

use crate::db::get_db_pool;
use crate::execution_chain::SupplyDelta;

use super::GethSupplyDelta;

// New function similar to the one in live.rs
fn find_historic_supply_files_in_dir(data_dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut historic_files: Vec<std::path::PathBuf> = Vec::new();

    debug!(
        "scanning directory {:?} for historic supply files (supply_*.jsonl).",
        data_dir
    );

    match std::fs::read_dir(data_dir) {
        Ok(entries) => {
            for entry_result in entries {
                match entry_result {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.is_file() {
                            if let Some(filename_osstr) = path.file_name() {
                                if let Some(filename_str) = filename_osstr.to_str() {
                                    // In historic.rs, we are looking for "supply_*.jsonl"
                                    // and NOT excluding "supply.jsonl" as it won't match "supply_*"
                                    if filename_str.starts_with("supply_")
                                        && filename_str.ends_with(".jsonl")
                                    {
                                        historic_files.push(path.clone());
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Using warn! as in live.rs, though error! could also be justified
                        // if we want to be stricter about directory reading issues.
                        warn!("error reading a directory entry in {:?}: {}", data_dir, e);
                    }
                }
            }
            debug!(
                "found {} historic files by manual scan.",
                historic_files.len()
            );
            historic_files.sort(); // Sorting is important for processing order
            Ok(historic_files)
        }
        Err(e) => {
            error!(
                "failed to read directory {:?}: {}. no historic files will be loaded.",
                data_dir, e
            );
            Ok(Vec::new()) // Return an empty vec on error, consistent with original glob behavior
        }
    }
}

pub async fn backfill_historic_deltas(data_dir: &Path) -> Result<()> {
    let db_pool = get_db_pool("backfill-historic-supply", 3).await;

    // Replace glob with the new function
    let files = find_historic_supply_files_in_dir(data_dir)?;

    // The original glob pattern was "supply_*.jsonl",
    // our new function find_historic_supply_files_in_dir already implements this logic.
    // Sorting is also handled inside the new function.

    debug!(
        "found {} historic supply files to process.", // Simplified log
        files.len()
    );

    for file_path in files {
        // Renamed 'file' to 'file_path' for clarity
        debug!("processing historic file: {:?}", file_path);
        process_supply_file(&db_pool, &file_path).await?;
    }

    Ok(())
}

async fn process_supply_file(db_pool: &sqlx::PgPool, file: &Path) -> Result<()> {
    let content = fs::read_to_string(file).await?;
    let mut transaction = db_pool.begin().await?;

    for line in content.lines() {
        match serde_json::from_str::<GethSupplyDelta>(line) {
            Ok(delta) => {
                let withdrawals = delta
                    .issuance
                    .as_ref()
                    .and_then(|i| i.withdrawals.as_ref())
                    .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;
                let reward = delta
                    .issuance
                    .as_ref()
                    .and_then(|i| i.reward.as_ref())
                    .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;
                let genesis_allocation = delta
                    .issuance
                    .as_ref()
                    .and_then(|i| i.genesis_allocation.as_ref())
                    .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;

                let mut fee_burn_val = 0i128;
                if let Some(burn) = &delta.burn {
                    if let Some(eip1559) = &burn.eip1559 {
                        fee_burn_val += i128::from_str_radix(&eip1559[2..], 16)?;
                    }
                    if let Some(blob) = &burn.blob {
                        fee_burn_val += i128::from_str_radix(&blob[2..], 16)?;
                    }
                }
                let self_destruct_val = delta
                    .burn
                    .as_ref()
                    .and_then(|b| b.misc.as_ref())
                    .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;

                let supply_delta_val =
                    withdrawals + reward + genesis_allocation - fee_burn_val - self_destruct_val;

                let supply_delta_to_store = SupplyDelta {
                    block_number: delta.block_number,
                    parent_hash: delta.parent_hash.clone(),
                    block_hash: delta.hash.clone(),
                    supply_delta: supply_delta_val,
                    self_destruct: self_destruct_val,
                    fee_burn: fee_burn_val,
                    fixed_reward: reward,
                    uncles_reward: 0,
                };
                store_supply_delta(&mut transaction, &supply_delta_to_store).await?;
            }
            Err(e) => {
                error!(
                    "failed to parse supply delta line in file {:?}: {}",
                    file, e
                );
            }
        }
    }

    transaction.commit().await?;
    Ok(())
}

async fn store_supply_delta(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    delta: &SupplyDelta,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO execution_supply_deltas (
            block_number,
            block_hash,
            parent_hash,
            supply_delta,
            self_destruct,
            fee_burn,
            fixed_reward,
            uncles_reward
        )
        VALUES ($1, $2, $3, $4::NUMERIC, $5::NUMERIC, $6::NUMERIC, $7::NUMERIC, $8::NUMERIC)
        ON CONFLICT (block_number) DO NOTHING
        "#,
    )
    .bind(delta.block_number)
    .bind(&delta.block_hash)
    .bind(&delta.parent_hash)
    .bind(delta.supply_delta.to_string())
    .bind(delta.self_destruct.to_string())
    .bind(delta.fee_burn.to_string())
    .bind(delta.fixed_reward.to_string())
    .bind(delta.uncles_reward.to_string())
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::tests::TestDb;

    use super::*;
    use tempfile::tempdir;
    use test_context::test_context;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn test_process_supply_file(test_db: &TestDb) {
        let temp_dir = tempdir().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        // Create a test supply file
        let supply_file = data_dir.join("supply_20240301.jsonl");
        let content = r#"{"issuance":{"withdrawals":"0x3b7cc750fca7400"},"burn":{"1559":"0x3182cbb2c587217"},"blockNumber":17675197,"hash":"0x361dbd6690e2e8d2ae019ecb294999c52d63fcb6229e4f82b5d20af48606986d","parentHash":"0xbca3b0c59b8a1a9fc640078d784ee2dc9d8b5878eed0077c24dad1e1cfe2dc6f"}
{"issuance":{"withdrawals":"0x3b40e345e025000"},"blockNumber":17675198,"hash":"0xbbc135ee5d160eaa2f551b65f136c22304cd26afe0d5ef1162d58e04852b21f4","parentHash":"0x361dbd6690e2e8d2ae019ecb294999c52d63fcb6229e4f82b5d20af48606986d"}"#;

        tokio::fs::write(&supply_file, content).await.unwrap();

        // Process the file
        process_supply_file(&test_db.pool, &supply_file)
            .await
            .unwrap();

        // Verify the data was stored
        let deltas = sqlx::query!(
            r#"
            SELECT block_number, supply_delta::TEXT, fee_burn::TEXT
            FROM execution_supply_deltas
            ORDER BY block_number
            "#
        )
        .fetch_all(&test_db.pool)
        .await
        .unwrap();

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].block_number, 17675197);
        assert_eq!(
            deltas[0]
                .supply_delta
                .as_ref()
                .unwrap()
                .parse::<i128>()
                .unwrap(),
            0x3b7cc750fca7400
        );
        assert_eq!(
            deltas[0]
                .fee_burn
                .as_ref()
                .unwrap()
                .parse::<i128>()
                .unwrap(),
            0x3182cbb2c587217
        );
    }
}
