use chrono::{DateTime, Utc};
use format_url::FormatUrl;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::{env, eth_supply::SupplyAtTime};

const GLASSNODE_API: &str = "https://api.glassnode.com";

lazy_static! {
    static ref GLASSNODE_API_KEY: String = env::get_env_var_unsafe("GLASSNODE_API_KEY");
}

#[derive(Debug, Deserialize)]
struct GlassnodeDataPointF {
    t: u64,
    v: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GlassnodeDataPoint {
    pub t: u64,
    pub v: f64,
}

impl From<SupplyAtTime> for GlassnodeDataPoint {
    fn from(supply_at_time: SupplyAtTime) -> Self {
        GlassnodeDataPoint {
            t: supply_at_time.timestamp.timestamp() as u64,
            v: supply_at_time.supply.0,
        }
    }
}

pub async fn get_locked_eth_data() -> Vec<GlassnodeDataPoint> {
    let since = ("2015-08-07T00:00:00Z")
        .parse::<DateTime<Utc>>()
        .unwrap()
        .timestamp()
        .to_string();
    let until = chrono::Utc::now().timestamp().to_string();
    let url = FormatUrl::new(GLASSNODE_API)
        .with_path_template("/v1/metrics/distribution/supply_contracts")
        .with_query_params(vec![
            ("a", "ETH"),
            ("api_key", &GLASSNODE_API_KEY),
            ("f", "JSON"),
            ("i", "24h"),
            ("s", &since),
            ("u", &until),
        ])
        .format_url();

    reqwest::get(url)
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Vec<GlassnodeDataPointF>>()
        .await
        .map(|data_points| {
            data_points
                .iter()
                .filter(|data_point| data_point.v.is_some())
                .map(|data_point| GlassnodeDataPoint {
                    t: data_point.t,
                    v: data_point
                        .v
                        .expect("expect data point mapping only for data points with a v"),
                })
                .collect()
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_locked_eth_data() {
        get_locked_eth_data().await;
    }

    #[tokio::test]
    async fn test_get_circulating_supply_data() {
        get_circulating_supply_data().await;
    }
}
