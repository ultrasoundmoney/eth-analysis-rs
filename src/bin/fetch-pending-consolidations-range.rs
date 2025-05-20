use anyhow::Result;
use clap::Parser;
use reqwest::Client; // For making HTTP requests
use serde::Deserialize; // For deserializing JSON
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

// Data structures for API response
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct PendingBalanceConsolidation {
    source_index: String,
    target_index: String,
    #[serde(deserialize_with = "deserialize_gwei_string")]
    amount: u64, // Gwei
}

// Custom deserializer for string to u64
fn deserialize_gwei_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<u64>().map_err(serde::de::Error::custom)
}

#[derive(Deserialize, Debug)]
struct ApiPendingConsolidationsResponse {
    data: Vec<PendingBalanceConsolidation>,
    // Optional metadata fields if ever needed
    // version: Option<String>,
    // execution_optimistic: Option<bool>,
    // finalized: Option<bool>,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[clap(name = "fetch-pending-consolidations-range")]
struct Args {
    #[arg(long)]
    start_slot: u32,
    #[arg(long)]
    end_slot: u32,
    #[arg(long, default_value = "pending_consolidations.csv")]
    output_file: String,
    #[arg(
        long,
        help = "Base URL for the Beacon Node API, e.g., http://localhost:5052"
    )]
    beacon_node_url: String,
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

    let output_file_name = if args.output_file == "pending_consolidations.csv" {
        format!(
            "pending_consolidations_{}_{}.csv",
            args.start_slot, args.end_slot
        )
    } else {
        args.output_file.clone()
    };

    info!(
        "fetching pending consolidations from slot {} to {} into {} using beacon node: {}",
        args.start_slot, args.end_slot, output_file_name, args.beacon_node_url
    );

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(60)) // Increased timeout
        .build()?;

    let mut writer = csv::Writer::from_path(output_file_name.clone())?;
    writer.write_record(&["slot", "total_pending_consolidation_gwei"])?;

    for current_slot_val in args.start_slot..=args.end_slot {
        info!(
            "fetching pending consolidations for slot: {}",
            current_slot_val
        );

        let request_url = format!(
            "{}/eth/v1/beacon/states/{}/pending_consolidations",
            args.beacon_node_url.trim_end_matches('/'), // Ensure no double slash
            current_slot_val
        );

        info!("requesting url: {}", request_url);

        match client.get(&request_url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<ApiPendingConsolidationsResponse>().await {
                        Ok(pending_consolidations_resp) => {
                            let total_pending_gwei: u64 = pending_consolidations_resp
                                .data
                                .iter()
                                .map(|c| c.amount)
                                .sum();
                            info!(
                                "total pending consolidation gwei for slot {}: {}",
                                current_slot_val, total_pending_gwei
                            );
                            writer.write_record(&[
                                current_slot_val.to_string(),
                                total_pending_gwei.to_string(),
                            ])?;
                        }
                        Err(e) => {
                            error!(
                                "failed to parse json for pending consolidations for slot {}: {:?}. url: {}",
                                current_slot_val, e, request_url
                            );
                            writer.write_record(&[
                                current_slot_val.to_string(),
                                "parse_error".to_string(),
                            ])?;
                        }
                    }
                } else {
                    let status = response.status();
                    let error_body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read error body".to_string());
                    error!(
                        "api request failed for slot {} with status: {}. url: {}. body: {}",
                        current_slot_val, status, request_url, error_body
                    );
                    writer.write_record(&[
                        current_slot_val.to_string(),
                        format!("http_error_{}", status).to_string(),
                    ])?;
                }
            }
            Err(e) => {
                error!(
                    "failed to fetch pending consolidations for slot {}: {:?}. url: {}",
                    current_slot_val, e, request_url
                );
                writer.write_record(&[current_slot_val.to_string(), "fetch_error".to_string()])?;
            }
        }
    }

    writer.flush()?;
    info!(
        "finished writing pending consolidations to {}",
        output_file_name
    );
    Ok(())
}
