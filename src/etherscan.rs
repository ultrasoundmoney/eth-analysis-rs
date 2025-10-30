use chrono::{TimeZone, Utc};
use format_url::FormatUrl;
use serde::Deserialize;

use crate::{env::ENV_CONFIG, units::WeiNewtype, usd_price::EthPrice};

const ETHERSCAN_API: &str = "https://api.etherscan.io/v2/api";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EthSupply2 {
    /// eth_supply is calculated before adding ETH minted as Eth2Staking rewards and subtracting BurntFees from EIP-1559.
    #[serde(rename = "EthSupply")]
    pub eth_supply_min_beacon_issuance_plus_burn: WeiNewtype,
    #[allow(dead_code)]
    pub eth2_staking: WeiNewtype,
    #[allow(dead_code)]
    pub burnt_fees: WeiNewtype,
}

#[derive(Deserialize)]
struct EthSupply2Response {
    result: EthSupply2,
}

pub async fn get_eth_supply_2() -> reqwest::Result<EthSupply2> {
    let etherscan_api_key = ENV_CONFIG
        .etherscan_api_key
        .as_ref()
        .expect("expect ETHERSCAN_API_KEY in env in order to fetch eth supply 2");
    let url = FormatUrl::new(ETHERSCAN_API)
        .with_query_params(vec![
            ("chainid", "1"),
            ("module", "stats"),
            ("action", "ethsupply2"),
            ("apikey", etherscan_api_key),
        ])
        .format_url();

    reqwest::get(url)
        .await?
        .error_for_status()?
        .json::<EthSupply2Response>()
        .await
        .map(|body| body.result)
}

#[derive(Debug, Deserialize)]
struct EtherscanEthPrice {
    ethusd: String,
    ethusd_timestamp: String,
}

#[derive(Debug, Deserialize)]
struct EthPriceEnvelope {
    result: EtherscanEthPrice,
}

#[allow(dead_code)]
pub async fn get_eth_price() -> reqwest::Result<EthPrice> {
    let etherscan_api_key = ENV_CONFIG
        .etherscan_api_key
        .as_ref()
        .expect("expect ETHERSCAN_API_KEY in env in order to fetch eth price");
    reqwest::get(format!(
        "https://api.etherscan.io/v2/api?chainid=1&module=stats&action=ethprice&apikey={etherscan_api_key}"
    ))
    .await?
    .error_for_status()?
    .json::<EthPriceEnvelope>()
    .await
    .map(|body| EthPrice {
        timestamp: Utc
            .timestamp_opt(body.result.ethusd_timestamp.parse::<i64>().unwrap(), 0)
            .unwrap(),
        usd: body.result.ethusd.parse::<f64>().unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_eth_supply_2_test() {
        get_eth_supply_2().await.unwrap();
    }

    #[tokio::test]
    async fn get_eth_price_test() {
        get_eth_price().await.unwrap();
    }
}
