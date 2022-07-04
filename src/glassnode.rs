use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config;

const GLASSNODE_API: &str = "https://api.glassnode.com";

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

#[derive(Serialize)]
struct StakedDataParams<'a> {
    a: &'static str,
    api_key: &'a str,
    c: &'static str,
    f: &'static str,
    i: &'static str,
    s: i64,
    u: i64,
}

#[derive(Serialize)]
struct CirculatingSupplyDataParams<'a> {
    a: &'static str,
    api_key: &'a str,
    c: &'static str,
    f: &'static str,
    i: &'static str,
    s: i64,
    u: i64,
}

fn make_circulating_supply_data_url() -> String {
    let params = CirculatingSupplyDataParams {
        a: "ETH",
        api_key: &config::get_glassnode_api_key(),
        c: "NATIVE",
        f: "JSON",
        i: "24h",
        s: ("2015-07-30T00:00:00Z")
            .parse::<DateTime<Utc>>()
            .unwrap()
            .timestamp(),
        u: chrono::Utc::now().timestamp(),
    };

    format!(
        "{GLASSNODE_API}/v1/metrics/supply/current?{}",
        serde_qs::to_string(&params).unwrap()
    )
}

pub async fn get_circulating_supply_data() -> reqwest::Result<Vec<GlassnodeDataPoint>> {
    reqwest::get(make_circulating_supply_data_url())
        .await?
        .error_for_status()?
        .json::<Vec<GlassnodeDataPoint>>()
        .await
}

#[derive(Serialize)]
struct EthInSmartContractsDataParams<'a> {
    a: &'static str,
    api_key: &'a str,
    f: &'static str,
    i: &'static str,
    s: i64,
    u: i64,
}

fn make_eth_in_smart_contracts_data_url() -> String {
    let params = EthInSmartContractsDataParams {
        a: "ETH",
        api_key: &config::get_glassnode_api_key(),
        f: "JSON",
        i: "24h",
        s: ("2015-08-07T00:00:00Z")
            .parse::<DateTime<Utc>>()
            .unwrap()
            .timestamp(),
        u: chrono::Utc::now().timestamp(),
    };

    format!(
        "{GLASSNODE_API}/v1/metrics/distribution/supply_contracts?{}",
        serde_qs::to_string(&params).unwrap()
    )
}

pub async fn get_locked_eth_data() -> reqwest::Result<Vec<GlassnodeDataPoint>> {
    reqwest::get(make_eth_in_smart_contracts_data_url())
        .await?
        .error_for_status()?
        .json::<Vec<GlassnodeDataPointF>>()
        .await
        .map(|data_points| {
            data_points
                .iter()
                .filter_map(|data_point| match data_point.v {
                    None => None,
                    Some(v) => Some(GlassnodeDataPoint { t: data_point.t, v }),
                })
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_locked_eth_data() {
        get_locked_eth_data().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_staked_data() {
        get_staked_data().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_circulating_supply_data() {
        get_circulating_supply_data().await.unwrap();
    }
}
