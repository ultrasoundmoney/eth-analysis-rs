use anyhow::{Context, Result};
use clap::Parser;
use eth_analysis::beacon_chain::{balances::sum_validator_balances, BeaconNodeHttp, Slot};
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    slot: u32,
}

fn init_tracing_subscriber() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing_subscriber();

    let args = Args::parse();
    let slot = Slot(args.slot as i32);

    info!("fetching validator balances for slot: {}", slot);

    let beacon_node = BeaconNodeHttp::new();

    match beacon_node.get_validator_balances_by_slot(slot).await {
        Ok(Some(validator_balances)) => {
            let total_balance = sum_validator_balances(&validator_balances);
            info!(
                "total validator balance for slot {}: {}",
                slot, total_balance
            );
            Ok(())
        }
        Ok(None) => {
            info!("no validator balances found for slot {}", slot);
            Ok(())
        }
        Err(e) => {
            error!(
                "failed to get validator balances for slot {}: {:?}",
                slot, e
            );
            Err(e).context("failed to get validator balances")
        }
    }
}
