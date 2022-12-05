mod grouped_analysis_1;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};

use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::{
    beacon_chain::{beacon_time, Slot},
    env, log,
};

use self::grouped_analysis_1::GroupedAnalysis1;

lazy_static! {
    static ref OPSGENIE_AUTH_HEADER: String = {
        let opsgenie_api_key = env::get_env_var_unsafe("OPSGENIE_API_KEY");
        format!("GenieKey {}", opsgenie_api_key)
    };
    static ref MIN_ALARM_WAIT: Duration = Duration::minutes(4);
}

#[derive(Deserialize)]
struct OpsGenieError {
    message: String,
}

struct Alarm {
    client: reqwest::Client,
    last_fired: Option<DateTime<Utc>>,
}

impl Alarm {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            last_fired: None,
        }
    }

    fn is_throttled(&self) -> bool {
        self.last_fired.map_or(false, |last_fired| {
            Utc::now() - last_fired < *MIN_ALARM_WAIT
        })
    }

    async fn fire(&mut self, message: &str) {
        if self.is_throttled() {
            warn!("alarm is throttled, ignoring request to fire alarm");
            return ();
        }

        error!(message, "firing alarm");

        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static(&*OPSGENIE_AUTH_HEADER),
        );

        let res = self
            .client
            .post("https://api.opsgenie.com/v2/alerts")
            .headers(headers)
            .json(&json!({ "message": message }))
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

        self.last_fired = Some(Utc::now());
    }

    async fn fire_dashboard_stalled(&mut self, phoenix: &Phoenix) {
        let message = format!(
            "{} hasn't updated for more than {} seconds!",
            phoenix.name,
            PHOENIX_MAX_LIFESPAN.num_seconds()
        );

        self.fire(&message).await
    }
}

lazy_static! {
    static ref PHOENIX_MAX_LIFESPAN: Duration = Duration::minutes(6);
}

#[derive(Debug, Clone)]
enum Ordinal {
    #[allow(dead_code)]
    Slot(Slot),
    Timestamp(DateTime<Utc>),
}

struct Phoenix {
    name: &'static str,
    last_seen: Ordinal,
}

impl Phoenix {
    fn is_slot_age_over_limit(&self) -> bool {
        match self.last_seen {
            Ordinal::Slot(slot) => {
                let slot_timestamp = beacon_time::get_date_time_from_slot(&slot);
                let age = Utc::now() - slot_timestamp;
                debug!(
                    name = self.name,
                    slot,
                    age = age.num_seconds(),
                    limit = PHOENIX_MAX_LIFESPAN.num_seconds(),
                    "checking slot age"
                );
                age >= *PHOENIX_MAX_LIFESPAN
            }
            Ordinal::Timestamp(timestamp) => {
                let age = Utc::now() - timestamp;
                debug!(
                    name = self.name,
                    %timestamp,
                    age = age.num_seconds(),
                    limit = PHOENIX_MAX_LIFESPAN.num_seconds(),
                    "checking block age"
                );
                age >= *PHOENIX_MAX_LIFESPAN
            }
        }
    }

    fn set_last_seen(&mut self, ordinal: Ordinal) {
        debug!(
            name = self.name,
            ?ordinal,
            "setting last seen to passed ordinal"
        );
        self.last_seen = ordinal;
    }
}

#[derive(Debug)]
struct MissingBlockFeesError;

impl TryFrom<GroupedAnalysis1> for Phoenix {
    type Error = anyhow::Error;

    fn try_from(value: GroupedAnalysis1) -> Result<Self, Self::Error> {
        match value.first_block_fee() {
            None => Err(anyhow!("empty list of block fees on grouped analysis 1")),
            Some(block_fee) => Ok(Self {
                name: "grouped-analysis-1",
                last_seen: Ordinal::Timestamp(block_fee.mined_at),
            }),
        }
    }
}

pub async fn monitor_critical_services() {
    log::init_with_env();

    info!(
        "releasing phoenix, dies after {} seconds",
        PHOENIX_MAX_LIFESPAN.num_seconds()
    );

    let mut alarm = Alarm::new();

    let initial_grouped_analysis_1 = {
        let grouped_analysis_1 = GroupedAnalysis1::get_current().await;
        if grouped_analysis_1.is_err() {
            let message =
                "failed to fetch initial grouped-analysis-1 dashboard, impossible to calculate age";
            error!(message);
            alarm.fire(message).await;
            panic!("{}", message);
        }
        grouped_analysis_1.expect("initial grouped-analysis-1 or panic")
    };

    let mut grouped_analysis_1_phoenix: Phoenix = {
        let phoenix = initial_grouped_analysis_1.try_into();

        if phoenix.is_err() {
            let message = "failed to set up initial grouped-analysis-1 phoenix, missing block fees";
            error!(message);
            alarm.fire(message).await;
            panic!("{}", message);
        }

        phoenix.expect("expect initial grouped analysis 1 phoenix or panic")
    };

    loop {
        if grouped_analysis_1_phoenix.is_slot_age_over_limit() {
            alarm
                .fire_dashboard_stalled(&grouped_analysis_1_phoenix)
                .await;
        }

        let grouped_analysis_1_timestamp = GroupedAnalysis1::get_current()
            .await
            .map(|grouped_analysis_1| grouped_analysis_1.timestamp());
        if let Ok(Some(timestamp)) = grouped_analysis_1_timestamp {
            grouped_analysis_1_phoenix.set_last_seen(Ordinal::Timestamp(timestamp));
        }

        sleep(Duration::seconds(10).to_std().unwrap()).await;
    }
}
