/// Binary to detect and fix corrupted blob data in the database
///
/// This tool:
/// 1. Scans the database for blocks with suspiciously high blob_base_fee or excess_blob_gas
/// 2. Shows detailed corruption report
/// 3. Deletes corrupted blocks (when confirmed)
/// 4. Application re-sync will fetch correct data and recalculate burn_sums
///
/// Usage:
///   cargo run --bin fix-blob-corruption -- --mode=diagnose
///   cargo run --bin fix-blob-corruption -- --mode=fix --confirm
///
/// See README.md in this directory for full documentation.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::env;
use tracing::{info, error};

// Type alias for block number
type BlockNumber = i32;

#[derive(Debug, Clone)]
struct CorruptBlock {
    number: BlockNumber,
    timestamp: DateTime<Utc>,
    hash: String,
    blob_base_fee: Option<i64>,
    excess_blob_gas: Option<i32>,
    blob_gas_used: Option<i32>,
}

// Thresholds for detecting corruption
const MAX_REASONABLE_BLOB_FEE_GWEI: i64 = 10_000; // 10,000 gwei
const MAX_REASONABLE_BLOB_FEE_WEI: i64 = MAX_REASONABLE_BLOB_FEE_GWEI * 1_000_000_000;
const MAX_REASONABLE_EXCESS_BLOB_GAS: i32 = 100_000_000; // ~127 blobs worth

/// Calculate blob base fee from excess blob gas using the correct EIP-4844 formula
#[allow(dead_code)]
fn calc_blob_base_fee(excess_blob_gas: Option<i32>) -> Option<i64> {
    let excess_blob_gas: u128 = excess_blob_gas?.try_into().ok()?;

    const MIN_BLOB_BASE_FEE: u128 = 1;
    const BLOB_BASE_FEE_UPDATE_FRACTION: u128 = 3338477;

    let blob_base_fee = fake_exponential(
        MIN_BLOB_BASE_FEE,
        excess_blob_gas,
        BLOB_BASE_FEE_UPDATE_FRACTION,
    );

    blob_base_fee.try_into().ok()
}

#[allow(dead_code)]
fn fake_exponential(factor: u128, numerator: u128, denominator: u128) -> u128 {
    let mut i: u128 = 1;
    let mut output: u128 = 0;
    let mut numerator_accum: u128 = factor * denominator;
    while numerator_accum > 0 {
        output += numerator_accum;
        numerator_accum = (numerator_accum * numerator) / (denominator * i);
        i += 1;
    }
    output / denominator
}

async fn find_corrupted_blocks(pool: &PgPool) -> Result<Vec<CorruptBlock>> {
    info!("Scanning database for corrupted blocks...");

    let rows = sqlx::query_as::<_, (i32, DateTime<Utc>, String, Option<i64>, Option<i32>, Option<i32>)>(
        r#"
        SELECT
            number,
            timestamp,
            hash,
            blob_base_fee,
            excess_blob_gas,
            blob_gas_used
        FROM blocks_next
        WHERE (blob_base_fee > $1 OR excess_blob_gas > $2)
          AND blob_base_fee IS NOT NULL
        ORDER BY number ASC
        "#
    )
    .bind(MAX_REASONABLE_BLOB_FEE_WEI)
    .bind(MAX_REASONABLE_EXCESS_BLOB_GAS)
    .fetch_all(pool)
    .await
    .context("Failed to query corrupted blocks")?;

    let corrupted: Vec<CorruptBlock> = rows
        .into_iter()
        .map(|(number, timestamp, hash, blob_base_fee, excess_blob_gas, blob_gas_used)| CorruptBlock {
            number,
            timestamp,
            hash,
            blob_base_fee,
            excess_blob_gas,
            blob_gas_used,
        })
        .collect();

    info!("Found {} corrupted blocks", corrupted.len());

    Ok(corrupted)
}

async fn diagnose(pool: &PgPool) -> Result<()> {
    println!("\n=== BLOB DATA CORRUPTION DIAGNOSTIC ===\n");

    let corrupted = find_corrupted_blocks(pool).await?;

    if corrupted.is_empty() {
        println!("✅ No corrupted blocks found!");
        println!("   All blob_base_fee values are < {} gwei", MAX_REASONABLE_BLOB_FEE_GWEI);
        println!("   All excess_blob_gas values are < {}", MAX_REASONABLE_EXCESS_BLOB_GAS);
        return Ok(());
    }

    println!("❌ Found {} corrupted blocks\n", corrupted.len());

    // Show range
    let first = corrupted.first().unwrap();
    let last = corrupted.last().unwrap();
    println!("Block range:");
    println!("  First: {} at {}", first.number, first.timestamp);
    println!("  Last:  {} at {}", last.number, last.timestamp);
    println!();

    // Calculate total bad burn
    let total_bad_burn: f64 = corrupted.iter()
        .filter_map(|b| {
            let fee = b.blob_base_fee? as f64;
            let gas = b.blob_gas_used? as f64;
            Some((fee * gas) / 1e18)
        })
        .sum();

    println!("Total bogus blob burn: {:.2} ETH", total_bad_burn);
    println!();

    // Show top 10 worst offenders
    println!("Top 10 most corrupted blocks:");
    println!("{:<10} {:<25} {:<15} {:<15} {:<15}",
             "Block", "Timestamp", "Blob Fee (gwei)", "Excess Gas", "Burn (ETH)");
    println!("{:-<90}", "");

    let mut sorted = corrupted.clone();
    sorted.sort_by_key(|b| std::cmp::Reverse(b.blob_base_fee));

    for block in sorted.iter().take(10) {
        let fee_gwei = block.blob_base_fee.unwrap_or(0) as f64 / 1e9;
        let excess = block.excess_blob_gas.unwrap_or(0);
        let burn_eth = if let (Some(fee), Some(gas)) = (block.blob_base_fee, block.blob_gas_used) {
            (fee as f64 * gas as f64) / 1e18
        } else {
            0.0
        };

        println!("{:<10} {:<25} {:<15.2} {:<15} {:<15.4}",
                 block.number,
                 block.timestamp.format("%Y-%m-%d %H:%M:%S"),
                 fee_gwei,
                 excess,
                 burn_eth);
    }
    println!();

    // Check burn_sums impact
    let burn_sums = sqlx::query_as::<_, (String, String, f64)>(
        r#"
        SELECT
            time_frame,
            sum_wei::TEXT,
            sum_usd
        FROM burn_sums
        WHERE time_frame = 'd7'
        ORDER BY last_included_block_number DESC
        LIMIT 1
        "#
    )
    .fetch_optional(pool)
    .await?;

    if let Some((time_frame, sum_wei_str, sum_usd)) = burn_sums {
        let sum_wei: u128 = sum_wei_str.parse().unwrap_or(0);
        let sum_eth = sum_wei as f64 / 1e18;
        println!("Current 7-day burn sum: {:.2} ETH (${:.2})", sum_eth, sum_usd);
        println!();
    }

    println!("To fix this corruption, run:");
    println!("  cargo run --bin fix-blob-corruption -- --mode=fix --confirm");
    println!();

    Ok(())
}

async fn fix_corruption(pool: &PgPool, confirmed: bool) -> Result<()> {
    if !confirmed {
        error!("Fix mode requires --confirm flag to prevent accidental data deletion");
        println!("\nTo confirm deletion and trigger re-sync, run:");
        println!("  cargo run --bin fix-blob-corruption -- --mode=fix --confirm");
        return Ok(());
    }

    println!("\n=== FIXING BLOB DATA CORRUPTION ===\n");

    let corrupted = find_corrupted_blocks(pool).await?;

    if corrupted.is_empty() {
        println!("✅ No corrupted blocks to fix!");
        return Ok(());
    }

    println!("Will delete {} corrupted blocks", corrupted.len());
    println!("This will cascade delete related burn_sums records");
    println!("The application will re-sync these blocks on next startup");
    println!();

    // Delete corrupted blocks in a transaction
    let mut tx = pool.begin().await?;

    info!("Deleting corrupted blocks...");

    let deleted = sqlx::query(
        r#"
        DELETE FROM blocks_next
        WHERE blob_base_fee > $1 OR excess_blob_gas > $2
        "#
    )
    .bind(MAX_REASONABLE_BLOB_FEE_WEI)
    .bind(MAX_REASONABLE_EXCESS_BLOB_GAS)
    .execute(&mut *tx)
    .await?;

    println!("Deleted {} blocks from blocks_next", deleted.rows_affected());

    // Commit the transaction
    tx.commit().await?;

    println!("\n✅ Corruption fixed!");
    println!();
    println!("Next steps:");
    println!("  1. Restart the eth-analysis-rs application");
    println!("  2. It will detect missing blocks and re-sync from the execution node");
    println!("  3. Burn sums will be recalculated automatically");
    println!();
    println!("To verify the fix after restart:");
    println!("  cargo run --bin fix-blob-corruption -- --mode=diagnose");
    println!();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    let mode = args
        .iter()
        .find(|arg| arg.starts_with("--mode="))
        .and_then(|arg| arg.strip_prefix("--mode="))
        .unwrap_or("diagnose");

    let confirmed = args.iter().any(|arg| arg == "--confirm");

    // Get database URL from environment
    let database_url = env::var("DATABASE_URL")
        .context("DATABASE_URL must be set")?;

    // Connect to database
    let pool = PgPool::connect(&database_url)
        .await
        .context("Failed to connect to database")?;

    match mode {
        "diagnose" => diagnose(&pool).await?,
        "fix" => fix_corruption(&pool, confirmed).await?,
        _ => {
            error!("Unknown mode: {}. Use --mode=diagnose or --mode=fix", mode);
            println!("\nUsage:");
            println!("  cargo run --bin fix-blob-corruption -- --mode=diagnose");
            println!("  cargo run --bin fix-blob-corruption -- --mode=fix --confirm");
        }
    }

    pool.close().await;

    Ok(())
}
