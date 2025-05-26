use clap::{Parser, Subcommand};
use csv::WriterBuilder;
use sqlx::{PgPool, Row};
use std::io::{self, Write};
use tracing::{info, warn};

use eth_analysis::{
    beacon_chain::{BeaconNode, BeaconNodeHttp, Slot},
    db, log,
    units::GweiNewtype,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Export pre-pectra eth1 deposits (gwei) for a slot range to CSV.
    ExportEth1Deposits {
        /// Inclusive start slot.
        #[clap(long)]
        start_slot: i32,
        /// Inclusive end slot.
        #[clap(long)]
        end_slot: i32,
        /// Optional output file path. Defaults to stdout.
        #[clap(long)]
        output: Option<String>,
    },
    /// Export eth supply components for a slot range to CSV.
    ExportEthSupplyComponents {
        /// Inclusive start slot.
        #[clap(long)]
        start_slot: i32,
        /// Inclusive end slot.
        #[clap(long)]
        end_slot: i32,
        /// Optional output file path. Defaults to stdout.
        #[clap(long)]
        output: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ExportEth1Deposits {
            start_slot,
            end_slot,
            output,
        } => {
            export_eth1_deposits(start_slot, end_slot, output).await?;
        }
        Commands::ExportEthSupplyComponents {
            start_slot,
            end_slot,
            output,
        } => {
            export_supply_components(start_slot, end_slot, output).await?;
        }
    }

    Ok(())
}

async fn export_eth1_deposits(
    start_slot: i32,
    end_slot: i32,
    output: Option<String>,
) -> anyhow::Result<()> {
    if start_slot > end_slot {
        anyhow::bail!("start_slot must be <= end_slot");
    }

    let mut writer = csv_writer(&output)?;

    writer.write_record(["slot", "eth1_deposits_sum_gwei"])?;

    let beacon_node = BeaconNodeHttp::new_from_env();

    for slot_i32 in start_slot..=end_slot {
        let slot = Slot(slot_i32);
        match beacon_node.get_block_by_slot(slot).await {
            Ok(Some(block)) => {
                let sum: GweiNewtype = block
                    .deposits()
                    .iter()
                    .fold(GweiNewtype(0), |acc, d| acc + d.amount);
                let record = [slot_i32.to_string(), i64::from(sum).to_string()];
                writer.write_record(&record)?;
            }
            Ok(None) => {
                warn!(slot = %slot, "no block found for slot – skipping");
            }
            Err(e) => {
                warn!(slot = %slot, error = %e, "failed to fetch block – skipping");
            }
        }
    }

    writer.flush()?;
    info!("export eth1 deposits completed");
    Ok(())
}

async fn export_supply_components(
    start_slot: i32,
    end_slot: i32,
    output: Option<String>,
) -> anyhow::Result<()> {
    if start_slot > end_slot {
        anyhow::bail!("start_slot must be <= end_slot");
    }

    let mut writer = csv_writer(&output)?;

    writer.write_record([
        "slot",
        "execution_supply_wei",
        "deposit_sum_gwei",
        "beacon_balances_gwei",
        "pending_deposits_sum_gwei",
    ])?;

    // Reuse existing db pool helper
    let db_pool: PgPool = db::get_db_pool("export-cli", 5).await;

    let rows = sqlx::query(
        r#"
        SELECT
            bs.slot,
            e.balances_sum::TEXT                  AS execution_supply_text,
            bb.deposit_sum                        AS deposit_sum,
            bb.pending_deposits_sum_gwei          AS pending_deposits_sum_gwei,
            bv.gwei                               AS beacon_balances_gwei
        FROM beacon_states bs
        JOIN beacon_blocks bb ON bs.state_root = bb.state_root
        LEFT JOIN execution_supply e ON bb.block_hash = e.block_hash
        LEFT JOIN beacon_validators_balance bv ON bb.state_root = bv.state_root
        WHERE bs.slot >= $1 AND bs.slot <= $2
        ORDER BY bs.slot ASC
        "#,
    )
    .bind(start_slot)
    .bind(end_slot)
    .fetch_all(&db_pool)
    .await?;

    for row in rows {
        let slot: i32 = row.get("slot");
        let exec_supply_text: Option<String> = row.get("execution_supply_text");
        let deposit_sum_opt: Option<i64> = row.get("deposit_sum");
        let pending_deposits_opt: Option<i64> = row.get("pending_deposits_sum_gwei");
        let beacon_balances_opt: Option<i64> = row.get("beacon_balances_gwei");

        let record = [
            slot.to_string(),
            exec_supply_text.unwrap_or_default(),
            deposit_sum_opt.map(|v| v.to_string()).unwrap_or_default(),
            beacon_balances_opt
                .map(|v| v.to_string())
                .unwrap_or_default(),
            pending_deposits_opt
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ];
        writer.write_record(&record)?;
    }

    writer.flush()?;
    info!("export eth supply components completed");
    Ok(())
}

fn csv_writer(path_opt: &Option<String>) -> anyhow::Result<csv::Writer<Box<dyn Write>>> {
    let boxed_writer: Box<dyn Write> = match path_opt {
        Some(p) => Box::new(std::fs::File::create(p)?),
        None => Box::new(io::stdout()),
    };
    Ok(WriterBuilder::new()
        .has_headers(true)
        .from_writer(boxed_writer))
}
