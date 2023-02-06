use serde::Serialize;

use crate::burn_sums::BurnSums;
use crate::time_frames::GrowingTimeFrame;
use crate::time_frames::LimitedTimeFrame;

type EthPerMinute = f64;

#[derive(Debug, Serialize)]
pub struct BurnRates {
    d1: EthPerMinute,
    d30: EthPerMinute,
    d7: EthPerMinute,
    h1: EthPerMinute,
    m5: EthPerMinute,
    since_burn: EthPerMinute,
    since_merge: EthPerMinute,
}

impl From<&BurnSums> for BurnRates {
    fn from(burn_totals: &BurnSums) -> Self {
        let BurnSums {
            d1,
            d30,
            d7,
            h1,
            m5,
            since_burn,
            since_merge,
        } = burn_totals;

        let d1 = d1.0 / LimitedTimeFrame::Day1.duration().num_minutes() as f64;
        let d30 = d30.0 / LimitedTimeFrame::Day30.duration().num_minutes() as f64;
        let d7 = d7.0 / LimitedTimeFrame::Day7.duration().num_minutes() as f64;
        let h1 = h1.0 / LimitedTimeFrame::Hour1.duration().num_minutes() as f64;
        let m5 = m5.0 / LimitedTimeFrame::Minute5.duration().num_minutes() as f64;
        let since_burn =
            since_burn.0 / (GrowingTimeFrame::SinceBurn.duration()).num_minutes() as f64;
        let since_merge =
            since_merge.0 / (GrowingTimeFrame::SinceMerge.duration()).num_minutes() as f64;

        BurnRates {
            d1,
            d30,
            d7,
            h1,
            m5,
            since_burn,
            since_merge,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::units::EthNewtype;

    use super::*;

    #[tokio::test]
    async fn burn_rates_test() {
        let burn_totals = BurnSums {
            d1: EthNewtype(100.0),
            d30: EthNewtype(100.0),
            d7: EthNewtype(100.0),
            h1: EthNewtype(100.0),
            m5: EthNewtype(100.0),
            since_burn: EthNewtype(100.0),
            since_merge: EthNewtype(100.0),
        };

        let burn_rates: BurnRates = (&burn_totals).into();

        assert!(burn_rates.since_burn > 0.0 && burn_rates.since_burn < 1.0);
        assert!(burn_rates.since_merge > 0.0 && burn_rates.since_merge < 1.0);
        assert_eq!(burn_rates.m5, 20.0);
    }
}
