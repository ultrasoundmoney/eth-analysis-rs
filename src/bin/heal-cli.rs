use clap::Parser;
use tracing::{error, info};

use eth_analysis::{
    beacon_chain::{deposits::heal::heal_deposit_sums, Slot, FIRST_POST_LONDON_SLOT, PECTRA_SLOT},
    db, eth_supply, log,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
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

#[derive(Parser, Debug)]
enum Commands {
    /// Heals beacon states.
    BeaconStates,
    /// Heals ETH prices.
    EthPrices,
    /// Heals block hashes.
    BlockHashes,
    /// Heals deposit sums in the beacon_blocks table.
    DepositSums {
        /// The hardfork to start healing deposit sums from.
        #[clap(long)]
        hardfork: HardforkArgs,
    },
    /// Heals eth supply table entries.
    EthSupply {
        /// The hardfork to start healing eth supply from.
        #[clap(long)]
        hardfork: HardforkArgs,
    },
}

async fn run_cli(pool: sqlx::PgPool, command: Commands) {
    match command {
        Commands::BeaconStates => {
            info!("initiating beacon states healing");
            eth_analysis::heal_beacon_states().await;
            info!("done healing beacon states");
        }
        Commands::EthPrices => {
            info!("initiating eth prices healing");
            eth_analysis::heal_eth_prices().await;
            info!("done healing eth prices");
        }
        Commands::BlockHashes => {
            info!("initiating block hashes healing");
            eth_analysis::heal_block_hashes().await;
            info!("done healing block hashes");
        }
        Commands::DepositSums { hardfork } => {
            let start_slot: Slot = hardfork.into();
            info!(%start_slot, "initiating deposit sum healing");
            match heal_deposit_sums(&pool, start_slot).await {
                Ok(()) => info!("done healing deposit sums"),
                Err(e) => error!("error during deposit sum healing: {:?}", e),
            }
        }
        Commands::EthSupply { hardfork } => {
            let start_slot: Slot = hardfork.into();
            info!(%start_slot, "initiating eth supply healing");
            match eth_supply::heal_eth_supply(&pool, start_slot).await {
                Ok(()) => info!("done healing eth supply"),
                Err(e) => error!("error during eth supply healing: {:?}", e),
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log::init();

    let cli = Cli::parse();

    info!("connecting to db");
    let db_pool = db::get_db_pool("heal-cli", 5).await;
    info!("connected to db");

    run_cli(db_pool, cli.command).await;

    Ok(())
}
