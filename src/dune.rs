// Generated from: https://transform.tools/json-to-rust-serde
use crate::env::ENV_CONFIG;
use anyhow::Result;
use format_url::FormatUrl;
use serde::Deserialize;
use serde::Serialize;

const DUNE_ETH_IN_CONTRACTS_QUERY_URL: &str = "https://api.dune.com/api/v1/query/3751774/results";
const DUNE_FLIPPENING_DATA_QUERY_URL: &str = "https://api.dune.com/api/v1/query/3758140/results";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DuneResponse<Row> {
    pub execution_id: String,
    pub query_id: i64,
    pub is_execution_finished: bool,
    pub state: String,
    pub submitted_at: String,
    pub expires_at: String,
    pub execution_started_at: String,
    pub execution_ended_at: String,
    pub result: DuneResult<Row>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DuneResult<Row> {
    pub rows: Vec<Row>,
    pub metadata: Metadata,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EthInContractsRow {
    pub block_date: String,
    pub cumulative_sum: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlippeningDataRow {
    pub time: String,
    pub eth_price: f64,
    pub btc_price: f64,
    pub bitcoin_supply: f64,
}


#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub column_names: Vec<String>,
    pub column_types: Vec<String>,
    pub row_count: i64,
    pub result_set_bytes: i64,
    pub total_row_count: i64,
    pub total_result_set_bytes: i64,
    pub datapoint_count: i64,
    pub pending_time_millis: i64,
    pub execution_time_millis: i64,
}

pub async fn get_eth_in_contracts() -> Result<Vec<EthInContractsRow>> {
    get_dune_data(DUNE_ETH_IN_CONTRACTS_QUERY_URL).await
}

pub async fn get_flippening_data() -> Result<Vec<FlippeningDataRow>> {
    get_dune_data(DUNE_FLIPPENING_DATA_QUERY_URL).await
}

async fn get_dune_data<Row>(url: &str) -> Result<Vec<Row>> 
where
    Row: for<'a> Deserialize<'a>,
{
    let dune_api_key = ENV_CONFIG
        .dune_api_key
        .as_ref()
        .expect("expect DUNE_API_KEY in env in order to fetch eth in smart contracts");
    let url = FormatUrl::new(url).format_url();

    let client = reqwest::Client::new();
    Ok(client
        .get(url)
        .header("X-Dune-API-Key", dune_api_key)
        .send()
        .await?
        .error_for_status()?
        .json::<DuneResponse<Row>>()
        .await
        .map(|body| body.result.rows)?)
}

