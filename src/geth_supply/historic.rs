use anyhow::Result;
use glob::glob;
use std::path::Path;
use tokio::fs;
use tracing::{error, info};

use crate::db::get_db_pool;
use crate::execution_chain::SupplyDelta;

use super::GethSupplyDelta;

pub async fn backfill_historic_deltas(data_dir: &Path) -> Result<()> {
    let db_pool = get_db_pool("backfill-historic-supply", 3).await;

    // Find all timestamped supply files
    let pattern = data_dir
        .join("supply_*.jsonl")
        .to_str()
        .unwrap()
        .to_string();
    let mut files: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();

    // Sort by name (which includes timestamp)
    files.sort();

    info!("Found {} historic supply files", files.len());

    for file in files {
        info!("Processing historic file: {:?}", file);
        process_supply_file(&db_pool, &file).await?;
    }

    Ok(())
}

async fn process_supply_file(db_pool: &sqlx::PgPool, file: &Path) -> Result<()> {
    let content = fs::read_to_string(file).await?;
    let mut transaction = db_pool.begin().await?;

    for line in content.lines() {
        match serde_json::from_str::<GethSupplyDelta>(line) {
            Ok(delta) => {
                // Convert hex strings to numbers
                let withdrawals = delta
                    .issuance
                    .as_ref()
                    .and_then(|i| i.withdrawals.as_ref())
                    .map_or(Ok(0i128), |s| i128::from_str_radix(&s[2..], 16))?;
                let fee_burn = delta
                    .burn
                    .as_ref()
                    .and_then(|b| b.eip1559.as_ref())
                    .map(|h| i128::from_str_radix(&h[2..], 16))
                    .transpose()?
                    .unwrap_or(0);
                let misc = delta
                    .burn
                    .as_ref()
                    .and_then(|b| b.misc.as_ref())
                    .map(|h| i128::from_str_radix(&h[2..], 16))
                    .transpose()?
                    .unwrap_or(0);

                let supply_delta = SupplyDelta {
                    block_number: delta.block_number,
                    parent_hash: delta.parent_hash,
                    block_hash: delta.hash,
                    supply_delta: withdrawals,
                    self_destruct: misc,
                    fee_burn,
                    fixed_reward: 0,  // Not present in new schema
                    uncles_reward: 0, // Not present in new schema
                };

                store_supply_delta(&mut transaction, &supply_delta).await?;
            }
            Err(e) => {
                error!("Failed to parse supply delta line: {}", e);
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

    // dbg!("store delta", delta); // Keep or remove dbg! as preferred

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
