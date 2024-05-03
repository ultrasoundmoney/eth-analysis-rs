mod types;


// use std::fs::File;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use format_url::FormatUrl;
use eth_analysis::env::ENV_CONFIG;

lazy_static! {
    static ref SUPPLY_LOWER_LIMIT_DATE_TIME: DateTime<Utc> =
        ("2015-07-30T00:00:00Z").parse::<DateTime<Utc>>().unwrap();
}

const DUNE_API: &str = "https://api.dune.com/api/v1/query/3686915/results";


#[tokio::main]
pub async fn main() {
   let dune_api_key = ENV_CONFIG
        .dune_api_key
        .as_ref()
        .expect("expect DUNE_API_KEY in env in order to fetch eth in smart contracts");
    let url = FormatUrl::new(DUNE_API)
        .format_url();

    let client = reqwest::Client::new();
    let data = client.get(url)
        .header("X-Dune-API-Key", dune_api_key)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<types::DuneEthInContractsResponse>()
        .await
        .map(|body| body.result);

    
}
