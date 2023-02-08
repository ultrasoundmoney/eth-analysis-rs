use std::collections::HashMap;
use std::ops::Deref;

use enum_iterator::all;
use serde::Serialize;

use crate::burn_sums::BurnSums;
use crate::time_frames::TimeFrame;

type EthPerMinute = f64;
type UsdPerMinute = f64;

#[derive(Debug, Serialize)]
pub struct EthUsdRate {
    eth_per_minute: EthPerMinute,
    usd_per_minute: UsdPerMinute,
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct BurnRates(HashMap<TimeFrame, EthUsdRate>);

impl From<HashMap<TimeFrame, EthUsdRate>> for BurnRates {
    fn from(burn_rates: HashMap<TimeFrame, EthUsdRate>) -> Self {
        Self(burn_rates)
    }
}

impl From<&BurnSums> for BurnRates {
    fn from(burn_totals: &BurnSums) -> Self {
        all::<TimeFrame>()
            .map(|time_frame| {
                let minutes = (time_frame).duration().num_minutes() as f64;
                let eth_per_minute = burn_totals[time_frame].eth.0 / minutes;
                let usd_per_minute = burn_totals[time_frame].usd.0 / minutes;
                (
                    time_frame,
                    EthUsdRate {
                        eth_per_minute,
                        usd_per_minute,
                    },
                )
            })
            .collect::<HashMap<TimeFrame, EthUsdRate>>()
            .into()
    }
}

impl Deref for BurnRates {
    type Target = HashMap<TimeFrame, EthUsdRate>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        burn_sums::EthUsdAmount,
        time_frames::{GrowingTimeFrame, LimitedTimeFrame},
        units::{EthNewtype, UsdNewtype},
    };

    use super::*;

    #[tokio::test]
    async fn burn_rates_test() {
        let burn_totals = BurnSums {
            d1: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
            d7: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
            d30: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
            h1: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
            m5: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
            since_burn: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
            since_merge: EthUsdAmount {
                eth: EthNewtype(100.0),
                usd: UsdNewtype(100.0),
            },
        };

        let burn_rates: BurnRates = (&burn_totals).into();

        let burn_rate_since_burn = burn_rates
            .get(&TimeFrame::Growing(GrowingTimeFrame::SinceBurn))
            .unwrap();
        let burn_rate_since_merge = burn_rates
            .get(&TimeFrame::Growing(GrowingTimeFrame::SinceMerge))
            .unwrap();
        let burn_rate_m5 = burn_rates
            .get(&TimeFrame::Limited(LimitedTimeFrame::Minute5))
            .unwrap();

        assert!(
            burn_rate_since_burn.eth_per_minute > 0.0 && burn_rate_since_burn.eth_per_minute < 1.0
        );
        assert!(
            burn_rate_since_merge.eth_per_minute > 0.0
                && burn_rate_since_merge.eth_per_minute < 1.0
        );
        assert_eq!(burn_rate_m5.eth_per_minute, 20.0);
    }
}
