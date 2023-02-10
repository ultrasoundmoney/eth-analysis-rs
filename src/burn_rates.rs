use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::burn_sums::BurnSumsEnvelope;
use crate::execution_chain::BlockNumber;
use crate::time_frames::TimeFrame;

type EthPerMinute = f64;
type UsdPerMinute = f64;

#[derive(Debug, Serialize)]
pub struct EthUsdRate {
    eth_per_minute: EthPerMinute,
    usd_per_minute: UsdPerMinute,
}

pub type BurnRates = HashMap<TimeFrame, EthUsdRate>;

#[derive(Debug, Serialize)]
pub struct BurnRatesEnvelope {
    block_number: BlockNumber,
    burn_rates: BurnRates,
    timestamp: DateTime<Utc>,
}

impl From<&BurnSumsEnvelope> for BurnRatesEnvelope {
    fn from(burn_sums_envelope: &BurnSumsEnvelope) -> Self {
        let BurnSumsEnvelope {
            block_number,
            burn_sums,
            timestamp,
        } = burn_sums_envelope;

        let burn_rates = burn_sums
            .iter()
            .map(|(time_frame, eth_usd_amount)| {
                let minutes = time_frame.duration().num_minutes() as f64;
                let eth_per_minute = eth_usd_amount.eth.0 / minutes;
                let usd_per_minute = eth_usd_amount.usd.0 / minutes;
                (
                    *time_frame,
                    EthUsdRate {
                        eth_per_minute,
                        usd_per_minute,
                    },
                )
            })
            .collect();

        BurnRatesEnvelope {
            block_number: *block_number,
            burn_rates,
            timestamp: *timestamp,
        }
    }
}
