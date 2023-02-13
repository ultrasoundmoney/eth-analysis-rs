/// Calculates the various rates displayed in the gauge charts.
///
/// For since burn issuance we consider beacon chain issuance only giving an idea of the rate of
/// issuance on the beacon chain based on data since the burn, not the execution chain, which has
/// issued many ETH since then too. To get a feel for this scenario we offer the "simulate pow"
/// toggle which sets the rate to an estimate of only the pow issuance.
use std::collections::HashMap;

use enum_iterator::all;
use futures::join;
use serde::Serialize;
use sqlx::PgPool;

use crate::{
    beacon_chain::IssuanceStore,
    burn_sums::{BurnSumsEnvelope, EthUsdAmount},
    caching::{self, CacheKey},
    execution_chain::ExecutionNodeBlock,
    performance::TimedExt,
    time_frames::TimeFrame,
    units::{EthNewtype, UsdNewtype},
    usd_price,
};

const PROOF_OF_WORK_DAILY_ISSUANCE_ESTIMATE: f64 = 13500.0;
const DAYS_PER_YEAR: f64 = 365.25;
const PROOF_OF_WORK_YEARLY_ISSUANCE_ESTIMATE: f64 =
    PROOF_OF_WORK_DAILY_ISSUANCE_ESTIMATE * DAYS_PER_YEAR;
const HOURS_PER_DAY: f64 = 24.0;
const MINUTES_PER_HOUR: f64 = 60.0;
const MINUTES_PER_YEAR: f64 = MINUTES_PER_HOUR * HOURS_PER_DAY * DAYS_PER_YEAR;

#[derive(Debug, Serialize)]
pub struct GaugeRatesTimeFrame {
    burn_rate_yearly: EthUsdAmount,
    issuance_rate_yearly: EthUsdAmount,
    issuance_rate_yearly_pow: EthUsdAmount,
    supply_growth_rate_yearly: f64,
    supply_growth_rate_yearly_pow: f64,
}

pub type GaugeRates = HashMap<TimeFrame, GaugeRatesTimeFrame>;

pub async fn on_new_block(
    db_pool: &PgPool,
    issuance_store: impl IssuanceStore + Copy,
    block: &ExecutionNodeBlock,
    burn_sums_envelope: &BurnSumsEnvelope,
    eth_supply: &EthNewtype,
) {
    let mut gauge_rates: GaugeRates = HashMap::new();

    for time_frame in all::<TimeFrame>() {
        let burn_rate_yearly = burn_sums_envelope
            .burn_sums
            .get(&time_frame)
            .unwrap()
            .yearly_rate_from_time_frame(time_frame);

        let (issuance_time_frame, usd_price_average) = join!(
            issuance_store
                .issuance_from_time_frame(block, &time_frame)
                .timed(&format!("issuance_from_time_frame_{time_frame}")),
            usd_price::average_from_time_range(
                db_pool,
                time_frame.start_timestamp(block),
                block.timestamp,
            )
            .timed(&format!("usd_price::average_from_time_range_{time_frame}"))
        );

        let issuance_time_frame_eth: EthNewtype = issuance_time_frame.into();
        let year_time_frame_fraction =
            MINUTES_PER_YEAR / time_frame.duration().num_minutes() as f64;
        let issuance_rate_yearly_eth =
            EthNewtype(issuance_time_frame_eth.0 as f64 * year_time_frame_fraction);
        let issuance_rate_yearly = EthUsdAmount {
            eth: issuance_rate_yearly_eth,
            // It'd be nice to have a precise estimate of the USD issuance, but we don't have usd prices per
            // slot yet. We use an average usd price over the time frame instead.
            usd: UsdNewtype(issuance_rate_yearly_eth.0 * usd_price_average.0),
        };

        let issuance_rate_yearly_pow = EthUsdAmount {
            eth: EthNewtype(PROOF_OF_WORK_YEARLY_ISSUANCE_ESTIMATE),
            usd: UsdNewtype(PROOF_OF_WORK_YEARLY_ISSUANCE_ESTIMATE * usd_price_average.0),
        };

        let supply_growth_rate_yearly = {
            let eth_burn = burn_rate_yearly.eth;
            let eth_issuance = issuance_rate_yearly.eth;
            let yearly_delta = eth_issuance - eth_burn;
            yearly_delta.0 / eth_supply.0
        };

        let supply_growth_rate_yearly_pow = {
            let eth_burn = burn_rate_yearly.eth;
            let eth_issuance = issuance_rate_yearly_pow.eth;
            let yearly_delta = eth_issuance - eth_burn;
            yearly_delta.0 / eth_supply.0
        };

        gauge_rates.insert(
            time_frame,
            GaugeRatesTimeFrame {
                burn_rate_yearly,
                issuance_rate_yearly,
                issuance_rate_yearly_pow,
                supply_growth_rate_yearly,
                supply_growth_rate_yearly_pow,
            },
        );
    }

    caching::update_and_publish(db_pool, &CacheKey::GaugeRates, gauge_rates)
        .await
        .unwrap();
}
