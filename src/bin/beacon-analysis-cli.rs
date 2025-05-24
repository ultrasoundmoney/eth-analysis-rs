use clap::Parser;
use sqlx::PgPool;
use tracing::info;

use eth_analysis::{
    beacon_chain::{
        backfill::{backfill_balances, Granularity},
        backfill_pending_deposits_sum,
        blocks,
        issuance::backfill::{backfill_missing_issuance, backfill_slot_range_issuance},
        Slot, // Assuming Slot can be created from i32
        FIRST_POST_LONDON_SLOT,
        PECTRA_SLOT,
    },
    db,
    execution_chain::supply_deltas::backfill_execution_supply,
    log,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum GranularityArgs {
    Slot,
    Hour,
    Day,
    Epoch,
}

impl From<GranularityArgs> for Granularity {
    fn from(granularity_arg: GranularityArgs) -> Self {
        match granularity_arg {
            GranularityArgs::Slot => Granularity::Slot,
            GranularityArgs::Hour => Granularity::Hour,
            GranularityArgs::Day => Granularity::Day,
            GranularityArgs::Epoch => Granularity::Epoch,
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Parser, Debug)]
enum Commands {
    /// Backfills beacon chain issuance data.
    BackfillIssuance {
        /// Optional: Specify a start slot for ranged backfill.
        /// If provided, --end-slot must also be provided.
        /// This will overwrite existing issuance data for any hour touched by the slot range.
        #[clap(long)]
        start_slot: Option<i32>,

        /// Optional: Specify an end slot for ranged backfill.
        /// If provided, --start-slot must also be provided.
        #[clap(long)]
        end_slot: Option<i32>,
    },
    /// Backfills beacon chain balances to London fork.
    BackfillBalancesToLondon {
        #[clap(subcommand)]
        granularity: GranularityArgs,
    },
    /// Backfills beacon chain balances to Pectra fork.
    BackfillBalancesToPectra {
        #[clap(subcommand)]
        granularity: GranularityArgs,
    },
    /// Backfills beacon chain block slots.
    BackfillBeaconBlockSlots,
    /// Backfills pending deposits sum.
    BackfillPendingDepositsSum,
    /// Backfills execution supply.
    BackfillExecutionSupply,
    /// Backfills all hourly balances.
    BackfillHourlyBalances,
}

async fn run_cli(pool: PgPool, commands: Commands) {
    match commands {
        Commands::BackfillIssuance {
            start_slot,
            end_slot,
        } => match (start_slot, end_slot) {
            (Some(start), Some(end)) => {
                if start >= end {
                    eprintln!("error: start_slot must be less than end_slot for ranged backfill.");
                    return;
                }
                info!(
                    start_slot = start,
                    end_slot = end,
                    "initiating ranged beacon issuance backfill (one per hour, overwriting)."
                );
                backfill_slot_range_issuance(&pool, Slot(start), Slot(end)).await;
            }
            (None, None) => {
                info!("initiating missing beacon issuance backfill (one per hour).");
                backfill_missing_issuance(&pool).await;
            }
            _ => {
                eprintln!("error: for ranged backfill, both --start-slot and --end-slot must be provided.");
            }
        },
        Commands::BackfillBalancesToLondon { granularity } => {
            info!(
                granularity = ?granularity,
                "initiating beacon balances backfill to london"
            );
            let gran: Granularity = granularity.into();
            backfill_balances(&pool, &gran, FIRST_POST_LONDON_SLOT).await;
            info!("done backfilling beacon balances to london for specified granularity");
        }
        Commands::BackfillBalancesToPectra { granularity } => {
            info!(
                granularity = ?granularity,
                "initiating beacon balances backfill to pectra"
            );
            let gran: Granularity = granularity.into();
            backfill_balances(&pool, &gran, *PECTRA_SLOT + 1).await;
            info!("done backfilling beacon balances to pectra for specified granularity");
        }
        Commands::BackfillBeaconBlockSlots => {
            info!("initiating beacon block slots backfill");
            blocks::backfill::backfill_beacon_block_slots(&pool).await;
            info!("done backfilling beacon block slots");
        }
        Commands::BackfillPendingDepositsSum => {
            info!("initiating pending deposits sum backfill");
            backfill_pending_deposits_sum(&pool).await;
            info!("done backfilling pending deposits sum");
        }
        Commands::BackfillExecutionSupply => {
            backfill_execution_supply(&pool).await;
        }
        Commands::BackfillHourlyBalances => {
            info!("backfilling hourly beacon balances");
            backfill_balances(&pool, &Granularity::Hour, Slot(0)).await;
            info!("done backfilling hourly beacon balances");
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init();

    let cli = Cli::parse();

    info!("connecting to db");
    let db_pool = db::get_db_pool("beacon-analysis-cli", 5).await;
    info!("connected to db");

    run_cli(db_pool, cli.command).await;

    Ok(())
}
