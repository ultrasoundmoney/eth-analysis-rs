use clap::Parser;
use sqlx::PgPool;
use tracing::info;

use eth_analysis::{
    beacon_chain::{
        backfill::{backfill_balances, Granularity},
        backfill_pending_deposits_sum, blocks,
        blocks::backfill::backfill_missing_beacon_blocks,
        integrity::check_beacon_block_chain_integrity,
        issuance::backfill::{backfill_missing_issuance, backfill_slot_range_issuance},
        BeaconNodeHttp, Slot, FIRST_POST_LONDON_SLOT, PECTRA_SLOT,
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

#[derive(Parser, Debug)]
enum HardforkArgs {
    Genesis,
    London,
    Pectra,
}

impl From<HardforkArgs> for Slot {
    fn from(arg: HardforkArgs) -> Self {
        match arg {
            HardforkArgs::Genesis => Slot(0),
            HardforkArgs::London => FIRST_POST_LONDON_SLOT,
            HardforkArgs::Pectra => *PECTRA_SLOT,
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
    /// Checks the integrity of the beacon block chain.
    CheckBeaconBlockChainIntegrity {
        /// Optional: Specify a slot to start the integrity check from.
        #[clap(long)]
        start_slot: Option<i32>,
    },
    /// Backfills missing beacon_blocks between a hardfork (inclusive) and the DB tip.
    BackfillMissingBeaconBlocks {
        /// The hardfork boundary to start the backfill from (defaults to GENESIS).
        #[clap(subcommand)]
        hardfork: Option<HardforkArgs>,
    },
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
        Commands::CheckBeaconBlockChainIntegrity { start_slot } => {
            let start_slot_opt = start_slot.map(Slot);
            info!(
                ?start_slot_opt,
                "initiating beacon block chain integrity check"
            );

            let beacon_node = BeaconNodeHttp::new();

            match check_beacon_block_chain_integrity(&pool, &beacon_node, start_slot_opt).await {
                Ok(()) => info!("beacon block chain integrity check successful"),
                Err(e) => eprintln!("error during beacon block chain integrity check: {}", e),
            }
        }
        Commands::BackfillMissingBeaconBlocks { hardfork } => {
            let start_slot = hardfork.map(|hf| hf.into()).unwrap_or(Slot(0));
            info!(%start_slot, "initiating missing beacon_block backfill");
            backfill_missing_beacon_blocks(&pool, start_slot).await;
            info!("done backfilling missing beacon_blocks");
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
