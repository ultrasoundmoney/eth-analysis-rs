mod grouped_analysis_1;
mod price_stats;
mod supply_changes;
mod supply_over_time;
mod supply_parts;

use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use axum::{http::StatusCode, routing::get, Router, Server};
use chrono::{DateTime, Duration, Utc};

use futures::try_join;
use lazy_static::lazy_static;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::{
    env, log,
    phoenix::{
        grouped_analysis_1::GroupedAnalysis1Monitor, price_stats::EthPriceStatsMonitor,
        supply_changes::SupplyChangesMonitor, supply_over_time::SupplyOverTimeMonitor,
        supply_parts::SupplyPartsMonitor,
    },
};

lazy_static! {
    static ref OPSGENIE_AUTH_HEADER: String = {
        let opsgenie_api_key = env::get_env_var("OPSGENIE_API_KEY").unwrap();
        format!("GenieKey {opsgenie_api_key}")
    };
    static ref MIN_ALARM_WAIT: Duration = Duration::minutes(30);
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
        self.last_fired
            .is_some_and(|last_fired| Utc::now() - last_fired < *MIN_ALARM_WAIT)
    }

    async fn fire(&mut self, message: &str) {
        if self.is_throttled() {
            warn!("alarm is throttled, ignoring request to fire alarm");
            return;
        }

        error!(message, "firing alarm");

        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static(&OPSGENIE_AUTH_HEADER),
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
            "{} hasn't updated for more than {} minutes!",
            name,
            MAX_WAIT.num_minutes()
        );

        self.fire(&message).await
    }
}

lazy_static! {
    static ref MAX_WAIT: Duration = Duration::minutes(16);
}

struct Phoenix {
    name: &'static str,
    last_seen: DateTime<Utc>,
    monitor: Box<dyn PhoenixMonitor + Send + Sync>,
}

impl Phoenix {
    fn is_age_over_limit(&self) -> bool {
        let age = Utc::now() - self.last_seen;
        debug!(
            name = self.name,
            age = age.num_seconds(),
            limit = MAX_WAIT.num_seconds(),
            "checking age"
        );
        age >= *MAX_WAIT
    }

    fn set_last_seen(&mut self, last_seen: DateTime<Utc>) {
        debug!(name = self.name, ?last_seen, "setting last seen");
        self.last_seen = last_seen;
    }
}

#[async_trait]
trait PhoenixMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>>;
}

async fn run_health_check_server(last_checked: Arc<Mutex<DateTime<Utc>>>) {
    let app = Router::new().route(
        "/healthz",
        get(|| async move {
            let last_checked = last_checked.lock().unwrap();
            let age = Utc::now() - *last_checked;
            if age > Duration::seconds(30) {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                StatusCode::OK
            }
        }),
    );

    let port = env::get_env_var("PORT").unwrap_or_else(|| "8080".to_string());
    let socket_addr = format!("0.0.0.0:{port}").parse().unwrap();
    info!(%socket_addr, "starting health check server");
    Server::bind(&socket_addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn run_alarm_loop(last_checked: Arc<Mutex<DateTime<Utc>>>) {
    info!(
        "releasing phoenix, dies after {} seconds",
        MAX_WAIT.num_seconds()
    );

    let mut alarm = Alarm::new();

    let mut phoenixes = vec![
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(EthPriceStatsMonitor::new()),
            name: "eth-price-stats",
        },
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
        Phoenix {
            last_seen: Utc::now(),
            name: "supply-changes",
            monitor: Box::new(SupplyChangesMonitor::new()),
        },
    ];

    loop {
        for phoenix in phoenixes.iter_mut() {
            if phoenix.is_age_over_limit() {
                alarm.fire_dashboard_stalled(phoenix.name).await;
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

        // Update the last checked time.
        {
            let mut last_checked = last_checked.lock().unwrap();
            *last_checked = Utc::now();
        }

        sleep(Duration::seconds(10).to_std().unwrap()).await;
    }
}

pub async fn monitor_critical_services() {
    log::init_with_env();

    // Used to share when we last checked with the health check endpoint.
    let last_checked = Arc::new(Mutex::new(Utc::now()));

    let last_checked_ref = last_checked.clone();

    let alarm_thread = tokio::spawn(async move {
        run_alarm_loop(last_checked).await;
    });

    let server_thread = tokio::spawn(async move {
        run_health_check_server(last_checked_ref).await;
    });

    try_join!(server_thread, alarm_thread).unwrap();
}
