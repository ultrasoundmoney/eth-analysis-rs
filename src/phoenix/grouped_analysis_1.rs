use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockFee {
    pub mined_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupedAnalysis1 {
    latest_block_fees: Vec<BlockFee>,
}

impl GroupedAnalysis1 {
    pub fn first_block_fee(&self) -> Option<&BlockFee> {
        self.latest_block_fees.first()
    }

    pub async fn get_current() -> reqwest::Result<GroupedAnalysis1> {
        reqwest::get("https://ultrasound.money/api/fees/grouped-analysis-1")
            .await?
            .error_for_status()?
            .json::<GroupedAnalysis1>()
            .await
    }

    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.latest_block_fees
            .first()
            .map(|block_fee| block_fee.mined_at)
    }
}
