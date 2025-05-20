use anyhow::Result;
use clap::Parser;
use eth_analysis::beacon_chain::{balances::sum_validator_balances, BeaconNodeHttp, Slot};
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    start_slot: u32,
    #[arg(long)]
    end_slot: u32,
    #[arg(long, default_value = "beacon_balances.csv")]
    output_file: String,
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

    if args.start_slot > args.end_slot {
        anyhow::bail!("start_slot cannot be greater than end_slot");
    }

    let output_file_name = if args.output_file == "beacon_balances.csv" {
        format!("beacon_balances_{}_{}.csv", args.start_slot, args.end_slot)
    } else {
        args.output_file.clone()
    };

    info!(
        "fetching validator balances from slot {} to {} into {}",
        args.start_slot, args.end_slot, output_file_name
    );

    let beacon_node = BeaconNodeHttp::new();
    let mut writer = csv::Writer::from_path(output_file_name.clone())?;
    writer.write_record(["slot", "total_balance_gwei"])?;

    for current_slot_val in args.start_slot..=args.end_slot {
        let slot = Slot(current_slot_val as i32);
        info!("fetching validator balances for slot: {}", slot);

        match beacon_node.get_validator_balances_by_slot(slot).await {
            Ok(Some(validator_balances)) => {
                let total_balance = sum_validator_balances(&validator_balances);
                info!(
                    "total validator balance for slot {}: {}",
                    slot, total_balance
                );
                writer.write_record(&[slot.to_string(), total_balance.to_string()])?;
            }
            Ok(None) => {
                info!("no validator balances found for slot {}", slot);
                writer.write_record(&[slot.to_string(), "0".to_string()])?;
            }
            Err(e) => {
                error!(
                    "failed to get validator balances for slot {}: {:?}",
                    slot, e
                );
                // Optionally, decide if you want to skip this slot or stop the process
                // For now, we'll write 0 and continue.
                writer.write_record(&[slot.to_string(), "0".to_string()])?;
            }
        }
    }

    writer.flush()?;
    info!("finished writing balances to {}", output_file_name);
    Ok(())
}
