use std::collections::HashMap;

use enum_iterator::all;
use serde::Serialize;
use sqlx::PgPool;

use crate::{
    beacon_chain,
    burn_sums::{BurnSumsEnvelope, EthUsdAmount},
    caching::{self, CacheKey},
    execution_chain::ExecutionNodeBlock,
    time_frames::TimeFrame,
    units::EthNewtype,
};

#[derive(Debug, Serialize)]
pub struct GaugeRatesTimeFrame {
    burn_rate_yearly: EthUsdAmount,
    issuance_rate_yearly: EthUsdAmount,
    issuance_rate_yearly_pow: EthUsdAmount,
    supply_growth_rate_yearly: f64,
    supply_growth_rate_yearly_pow: f64,
}

pub type GaugeRates = HashMap<TimeFrame, GaugeRatesTimeFrame>;

// Collect the burn rates.
// Collect the issuance rates.
// Collect the supply growth rates.
// Build the proof of work simulations based on the issuance rate.
// Build the proof of work simulations based on the supply growth rate.
pub async fn on_new_block(
    db_pool: &PgPool,
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
        let issuance_rate_yearly =
            beacon_chain::estimated_issuance_from_time_frame(db_pool, &time_frame, block).await;
        let supply_growth_rate = {
            let eth_burn = burn_rate_yearly.eth;
            let eth_issuance = issuance_rate_yearly.eth;
            let yearly_delta = eth_issuance - eth_burn;
            yearly_delta.0 / eth_supply.0
        };

        gauge_rates.insert(
            time_frame,
            GaugeRatesTimeFrame {
                burn_rate_yearly,
                issuance_rate_yearly,
                issuance_rate_yearly_pow: issuance_rate_yearly,
                supply_growth_rate_yearly: supply_growth_rate,
                supply_growth_rate_yearly_pow: supply_growth_rate,
            },
        );
    }

    caching::update_and_publish(db_pool, &CacheKey::GaugeRates, gauge_rates)
        .await
        .unwrap();
}
