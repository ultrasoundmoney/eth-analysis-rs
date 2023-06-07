use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tracing::debug;

use crate::burn_sums::{BurnSum, BurnSums};
use crate::caching::{self, CacheKey};
use crate::execution_chain::BlockNumber;
use crate::time_frames::TimeFrame;

type EthPerMinute = f64;
type UsdPerMinute = f64;

#[derive(Debug, Serialize)]
pub struct EthUsdRate {
    eth_per_minute: EthPerMinute,
    usd_per_minute: UsdPerMinute,
}

#[derive(Debug, Serialize)]
pub struct BurnRate {
    block_number: BlockNumber,
    rate: EthUsdRate,
    timestamp: DateTime<Utc>,
}

pub type BurnRates = HashMap<TimeFrame, BurnRate>;

impl From<(&TimeFrame, &BurnSum)> for BurnRate {
    fn from((time_frame, burn_sum): (&TimeFrame, &BurnSum)) -> Self {
        let minutes = time_frame.duration().num_minutes() as f64;
        let eth_per_minute = burn_sum.sum.eth.0 / minutes;
        let usd_per_minute = burn_sum.sum.usd.0 / minutes;
        BurnRate {
            block_number: burn_sum.block_number,
            rate: EthUsdRate {
                eth_per_minute,
                usd_per_minute,
            },
            timestamp: burn_sum.timestamp,
        }
    }
}

pub async fn on_new_block(db_pool: &PgPool, burn_sums: &BurnSums) {
    debug!("calculating new burn rates");

    let burn_rates: BurnRates = burn_sums
        .iter()
        .map(|(tf, bs)| -> (TimeFrame, BurnRate) { (*tf, (tf, bs).into()) })
        .collect();

    caching::update_and_publish(db_pool, &CacheKey::BurnRates, burn_rates).await;
}
