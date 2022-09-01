use std::time::{Duration, SystemTime};

use chrono::{DateTime, TimeZone, Utc};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;

use crate::config;

#[derive(Deserialize)]
struct OpsGenieError {
    message: String,
}

async fn fire_alarm(name: &str) {
    let client = reqwest::Client::new();

    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("GenieKey {}", config::get_opsgenie_api_key())).unwrap(),
    );

    let res = client
        .post("https://api.opsgenie.com/v2/alerts")
        .headers(headers)
        .json(&json!({
            "message":
                format!(
                    "no {} for more than {} minutes!",
                    name,
                    PHOENIX_MAX_LIFESPAN.as_secs() / 60
                )
        }))
        .send()
        .await
        .unwrap();

    if res.status() != 202 {
        match res.json::<OpsGenieError>().await {
            Err(_) => {
                panic!("failed to create alarm with OpsGenie")
            }
            Ok(body) => {
                panic!(
                    "failed to create alarm with OpsGenie, message: {}",
                    body.message
                )
            }
        }
    }
}

#[derive(Deserialize)]
struct ExecutionResponse {
    number: u32,
}

async fn get_current_execution_block_number() -> u32 {
    reqwest::get("https://ultrasound.money/api/fees/grouped-analysis-1")
        .await
        .unwrap()
        .json::<ExecutionResponse>()
        .await
        .unwrap()
        .number
}

#[derive(Deserialize)]
struct BeaconBalancesSum {
    slot: u32,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionBalancesSum {
    block_number: u32,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthSupplyResponse {
    beacon_balances_sum: BeaconBalancesSum,
    execution_balances_sum: ExecutionBalancesSum,
}

async fn get_current_beacon_slot() -> u32 {
    reqwest::get("https://ultrasound.money/api/fees/eth-supply")
        .await
        .unwrap()
        .json::<EthSupplyResponse>()
        .await
        .unwrap()
        .beacon_balances_sum
        .slot
}

async fn get_current_execution_delta_block_number() -> u32 {
    reqwest::get("https://ultrasound.money/api/fees/eth-supply")
        .await
        .unwrap()
        .json::<EthSupplyResponse>()
        .await
        .unwrap()
        .execution_balances_sum
        .block_number
}

fn date_time_from_system_time(system_time: SystemTime) -> DateTime<Utc> {
    chrono::Utc.timestamp(
        system_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap(),
        0,
    )
}

const PHOENIX_MAX_LIFESPAN: Duration = Duration::from_secs(60 * 6);
const MIN_ALARM_WAIT: Duration = Duration::from_secs(60 * 4);

pub async fn monitor_critical_services() {
    tracing_subscriber::fmt::init();
    tracing::info!(
        "releasing phoenix, dies after {} seconds",
        PHOENIX_MAX_LIFESPAN.as_secs()
    );

    let mut execution_phoenix_birth = SystemTime::now();
    let mut execution_delta_phoenix_birth = SystemTime::now();
    let mut beacon_slot_phoenix_birth = SystemTime::now();

    let mut last_alarm_fire = None;
    let mut last_seen_execution_block_number = get_current_execution_block_number().await;
    let mut last_seen_execution_delta_block_number =
        get_current_execution_delta_block_number().await;
    let mut last_seen_beacon_slot = get_current_beacon_slot().await;

    loop {
        let current_execution_block_number = get_current_execution_block_number().await;
        if last_seen_execution_block_number != current_execution_block_number {
            execution_phoenix_birth = SystemTime::now();
            tracing::debug!( "last seen execution block number: {last_seen_execution_block_number}, current block number: {current_execution_block_number}, rebirth phoenix at {:?}",  date_time_from_system_time(execution_phoenix_birth));
            last_seen_execution_block_number = current_execution_block_number;
        }

        let current_execution_delta_block_number = get_current_execution_delta_block_number().await;
        if last_seen_execution_delta_block_number != current_execution_delta_block_number {
            execution_delta_phoenix_birth = SystemTime::now();
            tracing::debug!( "last seen execution delta block number: {last_seen_execution_delta_block_number}, current block number: {current_execution_delta_block_number}, rebirth phoenix at {:?}", date_time_from_system_time(execution_delta_phoenix_birth));
            last_seen_execution_delta_block_number = current_execution_delta_block_number;
        }

        let current_beacon_slot = get_current_beacon_slot().await;
        if last_seen_beacon_slot != current_beacon_slot {
            beacon_slot_phoenix_birth = SystemTime::now();
            tracing::debug!( "last seen beacon slot: {last_seen_beacon_slot}, current slot: {current_beacon_slot}, rebirth phoenix at {:?}", date_time_from_system_time(beacon_slot_phoenix_birth));
            last_seen_beacon_slot = current_beacon_slot;
        }

        let execution_phoenix_lifespan = SystemTime::now()
            .duration_since(execution_phoenix_birth)
            .unwrap();

        let execution_delta_phoenix_lifespan = SystemTime::now()
            .duration_since(execution_delta_phoenix_birth)
            .unwrap();

        let beacon_slot_phoenix_lifespan = SystemTime::now()
            .duration_since(beacon_slot_phoenix_birth)
            .unwrap();

        let is_alarm_throttled = last_alarm_fire.map_or(false, |last_alarm_fire| {
            SystemTime::now().duration_since(last_alarm_fire).unwrap() < MIN_ALARM_WAIT
        });

        if execution_phoenix_lifespan > PHOENIX_MAX_LIFESPAN && !is_alarm_throttled {
            tracing::error!(
                "execution block phoenix died, no block for more than {:?}",
                execution_phoenix_lifespan
            );
            last_alarm_fire = Some(SystemTime::now());
            fire_alarm("execution block").await
        }

        if execution_delta_phoenix_lifespan > PHOENIX_MAX_LIFESPAN && !is_alarm_throttled {
            tracing::error!(
                "execution delta phoenix died, no execution delta block for more than {:?}",
                execution_delta_phoenix_lifespan
            );
            last_alarm_fire = Some(SystemTime::now());
            fire_alarm("execution delta block").await
        }

        if beacon_slot_phoenix_lifespan > PHOENIX_MAX_LIFESPAN && !is_alarm_throttled {
            tracing::error!(
                "beacon slot phoenix died, no beacon slot for more than {:?}",
                beacon_slot_phoenix_lifespan
            );
            last_alarm_fire = Some(SystemTime::now());
            fire_alarm("beacon slot").await
        }

        sleep(Duration::from_secs(10)).await
    }
}
