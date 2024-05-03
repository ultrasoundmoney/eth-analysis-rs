// Generated from: https://transform.tools/json-to-rust-serde
use serde::Deserialize;
use serde::Serialize;
use crate::env::ENV_CONFIG;
use format_url::FormatUrl;
use anyhow::Result;

const DUNE_API: &str = "https://api.dune.com/api/v1/query/3686915/results";

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DuneResponse<Row> {
    #[serde(rename = "execution_id")]
    pub execution_id: String,
    #[serde(rename = "query_id")]
    pub query_id: i64,
    #[serde(rename = "is_execution_finished")]
    pub is_execution_finished: bool,
    pub state: String,
    #[serde(rename = "submitted_at")]
    pub submitted_at: String,
    #[serde(rename = "expires_at")]
    pub expires_at: String,
    #[serde(rename = "execution_started_at")]
    pub execution_started_at: String,
    #[serde(rename = "execution_ended_at")]
    pub execution_ended_at: String,
    pub result: DuneResult<Row>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DuneResult<Row> {
    pub rows: Vec<Row>,
    pub metadata: Metadata,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthInContractsRow {
    #[serde(rename = "block_date")]
    pub block_date: String,
    #[serde(rename = "cumulative_sum")]
    pub cumulative_sum: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    #[serde(rename = "column_names")]
    pub column_names: Vec<String>,
    #[serde(rename = "column_types")]
    pub column_types: Vec<String>,
    #[serde(rename = "row_count")]
    pub row_count: i64,
    #[serde(rename = "result_set_bytes")]
    pub result_set_bytes: i64,
    #[serde(rename = "total_row_count")]
    pub total_row_count: i64,
    #[serde(rename = "total_result_set_bytes")]
    pub total_result_set_bytes: i64,
    #[serde(rename = "datapoint_count")]
    pub datapoint_count: i64,
    #[serde(rename = "pending_time_millis")]
    pub pending_time_millis: i64,
    #[serde(rename = "execution_time_millis")]
    pub execution_time_millis: i64,
}


pub async fn get_eth_in_contracts() -> Result<Vec<EthInContractsRow>> {
    let dune_api_key = ENV_CONFIG
        .dune_api_key
        .as_ref()
        .expect("expect DUNE_API_KEY in env in order to fetch eth in smart contracts");
    let url = FormatUrl::new(DUNE_API)
        .format_url();

    let client = reqwest::Client::new();
    Ok(client.get(url)
        .header("X-Dune-API-Key", dune_api_key)
        .send()
        .await?
        .error_for_status()?
        .json::<DuneResponse<EthInContractsRow>>()
        .await
        .map(|body| body.result.rows)?)
}
