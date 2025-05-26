use clap::Parser;
use sqlx::PgPool;
use std::str::FromStr;
use tracing::{error, info};

use eth_analysis::{
    beacon_chain::{
        backfill::{backfill_balances, Granularity},
        backfill_pending_deposits_sum, blocks,
        blocks::backfill::backfill_missing_beacon_blocks,
        deposits::heal::heal_deposit_sums,
        integrity::check_beacon_block_chain_integrity,
        issuance::backfill::{backfill_missing_issuance, backfill_slot_range_issuance},
        BeaconNode, BeaconNodeHttp, Slot, FIRST_POST_LONDON_SLOT, PECTRA_SLOT,
    },
    db,
    eth_supply::backfill::backfill_eth_supply,
    execution_chain::supply_deltas::backfill_execution_supply,
    log,
    units::GweiNewtype,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug, Clone, clap::ValueEnum)]
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

#[derive(Parser, Debug, Clone, clap::ValueEnum)]
enum HardforkArgs {
    Genesis,
    London,
    Capella,
    Deneb,
    Pectra,
}

impl From<HardforkArgs> for Slot {
    fn from(arg: HardforkArgs) -> Self {
        match arg {
            HardforkArgs::Genesis => Slot(0),
            HardforkArgs::London => FIRST_POST_LONDON_SLOT,
            HardforkArgs::Capella => Slot(6_209_536),
            HardforkArgs::Deneb => Slot(8_626_176),
            HardforkArgs::Pectra => *PECTRA_SLOT,
        }
    }
}

#[derive(Debug, Clone)]
struct SlotRange {
    start: Slot,
    end: Slot,
}

impl FromStr for SlotRange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return Err("slot range must be in the format start,end".to_string());
        }
        let start = parts[0]
            .parse::<i32>()
            .map_err(|e| format!("invalid start slot: {}", e))?;
        let end = parts[1]
            .parse::<i32>()
            .map_err(|e| format!("invalid end slot: {}", e))?;
        if start >= end {
            return Err("start slot must be less than end slot".to_string());
        }
        Ok(SlotRange {
            start: Slot(start),
            end: Slot(end),
        })
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Parser, Debug)]
enum Commands {
    /// Backfills beacon chain issuance data.
    BackfillIssuance {
        /// Optional: Specify a start hardfork for ranged backfill.
        /// If not provided, backfills missing issuance post-Pectra.
        /// If provided, this will overwrite existing issuance data for any hour
        /// from the start_hardfork up to the latest slot in the database.
        #[clap(
            long,
            help = "Optional: Specify a start hardfork for ranged backfill. Overwrites existing hourly issuance from this hardfork to the DB tip."
        )]
        start_hardfork: Option<HardforkArgs>,
    },
    /// Backfills beacon chain balances.
    BackfillBalances {
        /// The granularity for the balances backfill (slot, hour, day, epoch).
        granularity: GranularityArgs,
        /// Optional: The hardfork boundary to start the backfill from.
        /// If slot_range is provided, this is ignored.
        #[clap(long)]
        hardfork: Option<HardforkArgs>,
        /// Optional: Specify a slot range for backfill, e.g., "1000,2000".
        /// This will backfill balances for slots within this range (inclusive).
        /// Overrides 'hardfork' if provided.
        #[clap(long)]
        slot_range: Option<SlotRange>,
    },
    /// Backfills beacon block slots between a hardfork (inclusive) and the DB tip.
    BackfillMissingBeaconBlockSlots {
        /// The hardfork boundary to start the backfill from.
        #[clap(subcommand)]
        hardfork: HardforkArgs,
    },
    /// Backfills pending deposits sum.
    BackfillPendingDepositsSum {
        /// The granularity for the pending deposits sum backfill (slot, hour, day, epoch).
        granularity: GranularityArgs,
        /// Optional: Specify a slot range for backfill, e.g., "1000,2000".
        /// This will backfill pending deposits sum for slots within this range (inclusive),
        /// after Pectra fork.
        #[clap(long)]
        slot_range: Option<SlotRange>,
    },
    /// Backfills execution supply.
    BackfillExecutionSupply,
    /// Backfills eth supply table for slots with missing data but available prerequisites.
    BackfillEthSupply {
        /// Optional: Specify a hardfork to start the backfill from.
        /// Defaults to the Merge if not provided and slot_range is not used.
        /// If slot_range is provided, this is ignored for the start slot.
        #[clap(long)]
        hardfork: Option<HardforkArgs>,
        /// Optional: Specify a slot range for backfill, e.g., "1000,2000".
        /// This will backfill eth supply for slots within this range (inclusive).
        /// Overrides 'hardfork' for the start slot if provided.
        #[clap(long)]
        slot_range: Option<SlotRange>,
    },
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
    /// Fetches and computes the pending deposit sum for a given slot.
    GetPendingDepositsSum {
        /// The slot to fetch the pending deposit sum for.
        #[clap(long)]
        slot: i32,
    },
    /// Fetches and computes the pending deposit sum for a given slot and the aggregated sum up to that slot.
    GetPendingDepositsSumAggregated {
        /// The slot to fetch the pending deposit sum and aggregated sum for.
        #[clap(long)]
        slot: i32,
    },
    /// Heals deposit sums in the beacon_blocks table.
    HealDepositSums {
        /// The hardfork to start healing deposit sums from.
        #[clap(long)]
        hardfork: HardforkArgs,
    },
}

async fn run_cli(pool: PgPool, commands: Commands) {
    match commands {
        Commands::BackfillIssuance { start_hardfork } => match start_hardfork {
            Some(start_hf_arg) => {
                let start_slot: Slot = start_hf_arg.clone().into();
                info!(
                    start_hardfork = ?start_hf_arg,
                    derived_start_slot = %start_slot,
                    "initiating ranged beacon issuance backfill (one per hour, overwriting from start to DB tip)."
                );
                let result = backfill_slot_range_issuance(&pool, start_slot).await;
                match result {
                    Ok(()) => info!("done backfilling beacon issuance for specified slot range"),
                    Err(e) => error!("error during beacon issuance backfill: {:?}", e),
                }
            }
            None => {
                info!("initiating missing beacon issuance backfill (one per hour, typically post-pectra unless db is older).");
                let result = backfill_missing_issuance(&pool).await;
                match result {
                    Ok(()) => info!("done backfilling beacon issuance"),
                    Err(e) => error!("error during beacon issuance backfill: {:?}", e),
                }
            }
        },
        Commands::BackfillBalances {
            granularity,
            hardfork,
            slot_range,
        } => {
            let gran: Granularity = granularity.into();
            match slot_range {
                Some(range) => {
                    info!(granularity = ?gran, start_slot = %range.start, end_slot = %range.end, "initiating beacon balances backfill for slot range");
                    backfill_balances(&pool, &gran, Some(range.start), Some(range.end)).await;
                    info!("done backfilling beacon balances for specified slot range");
                }
                None => {
                    let start_slot: Slot = hardfork.unwrap_or(HardforkArgs::Genesis).into();
                    info!(granularity = ?gran, %start_slot, "initiating beacon balances backfill from hardfork to db tip");
                    backfill_balances(&pool, &gran, Some(start_slot), None).await;
                    info!("done backfilling beacon balances for specified hardfork");
                }
            }
        }
        Commands::BackfillMissingBeaconBlockSlots { hardfork } => {
            let start_slot: Slot = hardfork.into();
            info!(%start_slot, "initiating missing beacon_block slot backfill");
            blocks::backfill::backfill_beacon_block_slots(&pool, start_slot).await;
            info!("done backfilling beacon_block slots");
        }
        Commands::BackfillPendingDepositsSum {
            granularity,
            slot_range,
        } => {
            let gran: Granularity = granularity.into();
            match slot_range {
                Some(range) => {
                    info!(granularity = ?gran, start_slot = %range.start, end_slot = %range.end, "initiating pending deposits sum backfill for slot range");
                    backfill_pending_deposits_sum(&pool, &gran, Some(range.start), Some(range.end))
                        .await;
                    info!("done backfilling pending deposits sum for specified slot range");
                }
                None => {
                    info!(granularity = ?gran, "initiating pending deposits sum backfill from Pectra to db tip");
                    backfill_pending_deposits_sum(&pool, &gran, None, None).await;
                    info!("done backfilling pending deposits sum");
                }
            }
        }
        Commands::BackfillExecutionSupply => {
            backfill_execution_supply(&pool).await;
        }
        Commands::BackfillEthSupply {
            hardfork,
            slot_range,
        } => {
            match slot_range {
                Some(range) => {
                    info!(start_slot = %range.start, end_slot = %range.end, "initiating eth supply backfill for slot range");
                    backfill_eth_supply(&pool, Some(range.start), Some(range.end)).await;
                }
                None => {
                    let start_slot_opt: Option<Slot> = hardfork.map(|hf_arg| hf_arg.into());
                    info!(?start_slot_opt, "initiating eth supply backfill from specified start (or Merge) to latest available prerequisites");
                    backfill_eth_supply(&pool, start_slot_opt, None).await;
                }
            }
            info!("done backfilling eth supply");
        }
        Commands::CheckBeaconBlockChainIntegrity { start_slot } => {
            let start_slot_opt = start_slot.map(Slot);
            info!(
                ?start_slot_opt,
                "initiating beacon block chain integrity check"
            );

            let beacon_node = BeaconNodeHttp::new_from_env();

            match check_beacon_block_chain_integrity(&pool, &beacon_node, start_slot_opt).await {
                Ok(()) => info!("beacon block chain integrity check successful"),
                Err(e) => eprintln!("error during beacon block chain integrity check: {}", e),
            }
        }
        Commands::BackfillMissingBeaconBlocks { hardfork } => {
            let start_slot = hardfork.map(|hf| hf.into()).unwrap_or(Slot(0));
            info!(%start_slot, "initiating missing beacon_block backfill");
            match backfill_missing_beacon_blocks(&pool, start_slot).await {
                Ok(()) => info!("done backfilling missing beacon_blocks"),
                Err(e) => error!("error during missing beacon_block backfill: {:?}", e),
            }
        }
        Commands::GetPendingDepositsSum { slot } => {
            info!(%slot, "fetching pending deposits sum for slot");
            let beacon_node = BeaconNodeHttp::new_from_env();
            let slot_obj = Slot(slot);

            match beacon_node.get_header_by_slot(slot_obj).await {
                Ok(Some(block_header)) => {
                    let state_root = block_header.state_root();
                    info!(%slot, %state_root, "found state root for slot");
                    match beacon_node.get_pending_deposits_sum(&state_root).await {
                        Ok(Some(sum)) => {
                            info!(%slot, pending_deposits_sum_gwei = %sum, "successfully fetched pending deposits sum");
                        }
                        Ok(None) => {
                            eprintln!("error: beacon node reported no pending deposits sum for state_root {} (slot {})", state_root, slot);
                        }
                        Err(e) => {
                            eprintln!("error: failed to get pending deposits sum for state_root {} (slot {}): {}", state_root, slot, e);
                        }
                    }
                }
                Ok(None) => {
                    eprintln!("error: block header not found for slot {}", slot);
                }
                Err(e) => {
                    eprintln!("error: failed to get block header for slot {}: {}", slot, e);
                }
            }
        }
        Commands::GetPendingDepositsSumAggregated { slot } => {
            info!(%slot, "fetching pending deposits sum and aggregated sum for slot");
            let beacon_node = BeaconNodeHttp::new_from_env();
            let slot_obj = Slot(slot);

            // Fetch individual pending deposits sum for the slot
            let individual_sum: Option<GweiNewtype> = match beacon_node
                .get_header_by_slot(slot_obj)
                .await
            {
                Ok(Some(block_header)) => {
                    let state_root = block_header.state_root();
                    info!(%slot, %state_root, "found state root for slot");
                    match beacon_node.get_pending_deposits_sum(&state_root).await {
                        Ok(Some(sum)) => {
                            info!(%slot, pending_deposits_sum_gwei = %sum, "successfully fetched pending deposits sum for slot");
                            Some(sum)
                        }
                        Ok(None) => {
                            eprintln!("error: beacon node reported no pending deposits sum for state_root {} (slot {})", state_root, slot);
                            None
                        }
                        Err(e) => {
                            eprintln!("error: failed to get pending deposits sum for state_root {} (slot {}): {}", state_root, slot, e);
                            None
                        }
                    }
                }
                Ok(None) => {
                    eprintln!("error: block header not found for slot {}", slot);
                    None
                }
                Err(e) => {
                    eprintln!("error: failed to get block header for slot {}: {}", slot, e);
                    None
                }
            };

            // Fetch aggregated pending deposits sum from the database
            if slot_obj < *PECTRA_SLOT {
                info!(%slot, pectra_slot = %*PECTRA_SLOT, "slot is before pectra, aggregated sum is not applicable / will be zero.");
                if let Some(ind_sum) = individual_sum {
                    info!(%slot, pending_deposits_sum_gwei = %ind_sum, aggregated_pending_deposits_sum_gwei = 0, "successfully fetched sums");
                } else {
                    info!(%slot, "individual sum could not be fetched, cannot report aggregated sum.");
                }
                return;
            }

            match sqlx::query_scalar!(
                r#"
                SELECT SUM(pending_deposits_sum_gwei)::BIGINT
                FROM beacon_blocks
                WHERE slot <= $1 AND slot >= $2 AND pending_deposits_sum_gwei IS NOT NULL
                "#,
                slot,
                PECTRA_SLOT.0
            )
            .fetch_optional(&pool)
            .await
            {
                Ok(Some(Some(aggregated_sum_db))) => {
                    let aggregated_sum_gwei = GweiNewtype(aggregated_sum_db);
                    if let Some(ind_sum) = individual_sum {
                        info!(%slot, pending_deposits_sum_gwei = %ind_sum, %aggregated_sum_gwei, "successfully fetched sums");
                    } else {
                        info!(%slot, %aggregated_sum_gwei, "individual sum could not be fetched, but aggregated sum is available");
                    }
                }
                Ok(Some(None)) => {
                    // SUM was NULL (no rows matched or all matching rows had NULL pending_deposits_sum_gwei)
                    if let Some(ind_sum) = individual_sum {
                        info!(%slot, pending_deposits_sum_gwei = %ind_sum, aggregated_pending_deposits_sum_gwei = 0, "successfully fetched sums (no prior sums for aggregation)");
                    } else {
                        info!(%slot, aggregated_pending_deposits_sum_gwei = 0, "individual sum could not be fetched, and no prior sums for aggregation");
                    }
                }
                Ok(None) => {
                    // Should not happen with SUM if table exists, but handle defensively
                    if let Some(ind_sum) = individual_sum {
                        info!(%slot, pending_deposits_sum_gwei = %ind_sum, aggregated_pending_deposits_sum_gwei = 0, "no aggregated data found (unexpected query result)");
                    } else {
                        info!(%slot, aggregated_pending_deposits_sum_gwei = 0, "individual sum could not be fetched, and no aggregated data found (unexpected query result)");
                    }
                }
                Err(e) => {
                    eprintln!(
                        "error: failed to fetch aggregated pending deposits sum for slot {}: {}",
                        slot, e
                    );
                }
            }
        }
        Commands::HealDepositSums { hardfork } => {
            let start_slot: Slot = hardfork.into();
            info!(%start_slot, "initiating deposit sum healing");
            match heal_deposit_sums(&pool, start_slot).await {
                Ok(()) => info!("done healing deposit sums"),
                Err(e) => error!("error during deposit sum healing: {:?}", e),
            }
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
