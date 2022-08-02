use crate::burn_totals::BurnTotals;
use crate::execution_chain::LONDON_HARDFORK_TIMESTAMP;
use crate::time_frames::LimitedTimeFrame;
use chrono::Duration;
use chrono::Utc;

type EthPerMinute = f64;

pub struct BurnRates {
    all: EthPerMinute,
    d1: EthPerMinute,
    d30: EthPerMinute,
    d7: EthPerMinute,
    h1: EthPerMinute,
    m5: EthPerMinute,
}

fn get_burn_rate_limited_time_frame(
    burn_totals: &BurnTotals,
    limited_time_frame: LimitedTimeFrame,
) -> f64 {
    burn_totals.get_by_limited_time_frame(&limited_time_frame) as f64
        / Into::<Duration>::into(limited_time_frame).num_minutes() as f64
}

fn get_burn_rate_all(burn_totals: &BurnTotals) -> f64 {
    let duration_since_london = Utc::now() - *LONDON_HARDFORK_TIMESTAMP;
    burn_totals.all as f64 / duration_since_london.num_minutes() as f64
}

pub fn get_burn_rates(burn_totals: BurnTotals) -> BurnRates {
    BurnRates {
        all: get_burn_rate_all(&burn_totals),
        m5: get_burn_rate_limited_time_frame(&burn_totals, LimitedTimeFrame::Minute5),
        h1: get_burn_rate_limited_time_frame(&burn_totals, LimitedTimeFrame::Hour1),
        d1: get_burn_rate_limited_time_frame(&burn_totals, LimitedTimeFrame::Day1),
        d7: get_burn_rate_limited_time_frame(&burn_totals, LimitedTimeFrame::Day7),
        d30: get_burn_rate_limited_time_frame(&burn_totals, LimitedTimeFrame::Day30),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn burn_rate_all_test() {
        let burn_totals = BurnTotals {
            all: 100,
            d1: 100,
            d30: 100,
            d7: 100,
            h1: 100,
            m5: 100,
        };
        let burn_rate_all = get_burn_rate_all(&burn_totals);

        assert!(burn_rate_all > 0.0 && burn_rate_all < 1.0);
    }

    #[tokio::test]
    async fn burn_rate_limited_time_frame_test() {
        let burn_totals = BurnTotals {
            all: 100,
            d1: 100,
            d30: 100,
            d7: 100,
            h1: 100,
            m5: 100,
        };
        let burn_rate_m5 =
            get_burn_rate_limited_time_frame(&burn_totals, LimitedTimeFrame::Minute5);

        assert!(burn_rate_m5 > 0.0 && burn_rate_m5 <= 20.0);
    }
}
