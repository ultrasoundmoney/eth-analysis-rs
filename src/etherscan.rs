use chrono::{TimeZone, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::{env, eth_prices::EthPrice, eth_units::WeiString};

lazy_static! {
    static ref ETHERSCAN_API_KEY: String = env::get_env_var_unsafe("ETHERSCAN_API_KEY");
}

const ETHERSCAN_API: &str = "https://api.etherscan.io/api";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EthSupply2Params<'a> {
    module: &'static str,
    action: &'static str,
    api_key: &'a str,
}

fn make_eth_supply_2_url() -> String {
    let params = EthSupply2Params {
        module: "stats",
        action: "ethsupply2",
        api_key: &*ETHERSCAN_API_KEY,
    };

    format!("{ETHERSCAN_API}?{}", serde_qs::to_string(&params).unwrap())
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EthSupply2 {
    /// eth_supply is calculated before adding ETH minted as Eth2Staking rewards and subtracting BurntFees from EIP-1559.
    pub eth_supply: WeiString,
    pub eth2_staking: WeiString,
    pub burnt_fees: WeiString,
}

#[derive(Deserialize)]
struct EthSupply2Response {
    result: EthSupply2,
}

pub async fn get_eth_supply_2() -> reqwest::Result<EthSupply2> {
    reqwest::get(make_eth_supply_2_url())
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
    reqwest::get(format!(
        "https://api.etherscan.io/api?module=stats&action=ethprice&apikey={}",
        *ETHERSCAN_API_KEY
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
        let res = get_eth_price().await.unwrap();
    }
}
