mod grouped_analysis_1;
mod supply_over_time;
mod supply_parts;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::{
    env, log,
    phoenix::{
        grouped_analysis_1::GroupedAnalysis1Monitor, supply_over_time::SupplyOverTimeMonitor,
        supply_parts::SupplyPartsMonitor,
    },
};

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

    async fn fire_dashboard_stalled(&mut self, name: &str) {
        let message = format!(
            "{} hasn't updated for more than {} seconds!",
            name,
            PHOENIX_MAX_LIFESPAN.num_seconds()
        );

        self.fire(&message).await
    }
}

lazy_static! {
    static ref PHOENIX_MAX_LIFESPAN: Duration = Duration::minutes(6);
}

struct Phoenix {
    name: &'static str,
    last_seen: DateTime<Utc>,
    monitor: Box<dyn PhoenixMonitor>,
}

impl Phoenix {
    fn is_age_over_limit(&self) -> bool {
        let age = Utc::now() - self.last_seen;
        debug!(
            name = self.name,
            age = age.num_seconds(),
            limit = PHOENIX_MAX_LIFESPAN.num_seconds(),
            "checking age"
        );
        age >= *PHOENIX_MAX_LIFESPAN
    }

    fn set_last_seen(&mut self, last_seen: DateTime<Utc>) {
        debug!(name = self.name, ?last_seen, "setting last seen");
        self.last_seen = last_seen;
    }
}

#[async_trait]
trait PhoenixMonitor {
    async fn refresh(&mut self) -> Result<DateTime<Utc>>;
}

pub async fn monitor_critical_services() {
    log::init_with_env();

    info!(
        "releasing phoenix, dies after {} seconds",
        PHOENIX_MAX_LIFESPAN.num_seconds()
    );

    let mut alarm = Alarm::new();

    let mut phoenixes = vec![
        Phoenix {
            last_seen: Utc::now(),
            name: "grouped-analysis-1",
            monitor: Box::new(GroupedAnalysis1Monitor::new()),
        },
        Phoenix {
            last_seen: Utc::now(),
            name: "supply-over-time",
            monitor: Box::new(SupplyOverTimeMonitor::new()),
        },
        Phoenix {
            last_seen: Utc::now(),
            name: "supply-parts",
            monitor: Box::new(SupplyPartsMonitor::new()),
        },
    ];

    loop {
        for phoenix in phoenixes.iter_mut() {
            if phoenix.is_age_over_limit() {
                alarm.fire_dashboard_stalled(&phoenix.name).await;
            }

            let current = phoenix.monitor.refresh().await;
            match current {
                Ok(current) => phoenix.set_last_seen(current),
                Err(err) => {
                    error!(
                        name = phoenix.name,
                        ?err,
                        "failed to refresh phoenix monitor"
                    );
                }
            }
        }

        sleep(Duration::seconds(10).to_std().unwrap()).await;
    }
}
