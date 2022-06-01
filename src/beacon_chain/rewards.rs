use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use sqlx::{PgPool, Row};

use crate::{
    caching, eth_time,
    eth_units::{self, GweiAmount, GWEI_PER_ETH_F64},
    key_value_store::{self, KeyValue},
};

use super::balances;

pub const VALIDATOR_REWARDS_CACHE_KEY: &str = "validator-rewards";

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorReward {
    annual_reward: GweiAmount,
    apr: f64,
}

fn get_days_since_london() -> i64 {
    (Utc::now() - *eth_time::LONDON_HARDFORK_TIMESTAMP).num_days()
}

async fn get_tips_since_london(pool: &PgPool) -> sqlx::Result<GweiAmount> {
    let tips_since_genesis: f64 = sqlx::query(
        "
            SELECT SUM(tips) / 1e9 AS tips_since_genesis FROM blocks
        ",
    )
    .fetch_one(pool)
    .await?
    .get("tips_since_genesis");

    Ok(GweiAmount(tips_since_genesis as u64))
}

async fn get_tips_reward(
    pool: &PgPool,
    effective_balance_sum: GweiAmount,
) -> sqlx::Result<ValidatorReward> {
    let GweiAmount(tips_since_london) = get_tips_since_london(pool).await?;
    let tips_per_year = tips_since_london as f64 / get_days_since_london() as f64 * 365.25;
    let single_validator_share =
        (32_f64 * eth_units::GWEI_PER_ETH as f64) / effective_balance_sum.0 as f64;
    let tips_earned_per_year = tips_per_year * single_validator_share;
    let apr = tips_earned_per_year / (32 * eth_units::GWEI_PER_ETH) as f64;

    Ok(ValidatorReward {
        annual_reward: GweiAmount(tips_earned_per_year as u64),
        apr,
    })
}

const MAX_EFFECTIVE_BALANCE: f64 = 32f64 * GWEI_PER_ETH_F64;
const SECONDS_PER_SLOT: u8 = 12;
const SLOTS_PER_EPOCH: u8 = 32;
const EPOCHS_PER_DAY: f64 =
    (24 * 60 * 60) as f64 / SLOTS_PER_EPOCH as f64 / SECONDS_PER_SLOT as f64;
const EPOCHS_PER_YEAR: f64 = 365.25 * EPOCHS_PER_DAY;

const BASE_REWARD_FACTOR: u8 = 64;

// Consider staying in Gwei until the last moment instead of converting early.
pub fn get_issuance_reward(GweiAmount(effective_balance_sum): GweiAmount) -> ValidatorReward {
    let total_effective_balance = effective_balance_sum as f64;

    let active_validators = total_effective_balance / GWEI_PER_ETH_F64 / 32f64;

    // Balance at stake (Gwei)
    let max_balance_at_stake = active_validators * MAX_EFFECTIVE_BALANCE;

    let max_issuance_per_epoch = ((BASE_REWARD_FACTOR as f64 * max_balance_at_stake)
        / max_balance_at_stake.sqrt().floor())
    .trunc();
    let max_issuance_per_year = max_issuance_per_epoch * EPOCHS_PER_YEAR;

    let annual_reward = max_issuance_per_year / active_validators;
    let apr = max_issuance_per_year / total_effective_balance;

    tracing::debug!(
        "total effective balance: {} ETH",
        total_effective_balance / GWEI_PER_ETH_F64
    );
    tracing::debug!("nr of active validators: {}", active_validators);
    tracing::debug!(
        "max issuance per epoch: {} ETH",
        max_issuance_per_epoch / GWEI_PER_ETH_F64
    );
    tracing::debug!(
        "max issuance per year: {} ETH",
        max_issuance_per_year / GWEI_PER_ETH_F64
    );
    tracing::debug!("APR: {:.4}%", apr);

    ValidatorReward {
        annual_reward: GweiAmount(annual_reward as u64),
        apr,
    }
}

#[derive(Debug, Serialize)]
struct ValidatorRewards {
    issuance: ValidatorReward,
    tips: ValidatorReward,
    mev: ValidatorReward,
}

async fn get_validator_rewards(pool: &PgPool, client: &Client) -> anyhow::Result<ValidatorRewards> {
    let last_effective_balance_sum = balances::get_last_effective_balance_sum(pool, client).await?;
    let issuance_reward = get_issuance_reward(last_effective_balance_sum);
    let tips_reward = get_tips_reward(pool, last_effective_balance_sum).await?;

    Ok(ValidatorRewards {
        issuance: issuance_reward,
        tips: tips_reward,
        mev: ValidatorReward {
            annual_reward: GweiAmount((0.3 * GWEI_PER_ETH_F64) as u64),
            apr: 0.01,
        },
    })
}

pub async fn update_validator_rewards(pool: &PgPool, node_client: &Client) -> anyhow::Result<()> {
    let validator_rewards = get_validator_rewards(&pool, &node_client).await?;
    tracing::debug!("validator rewards: {:?}", validator_rewards);

    key_value_store::set_value(
        &pool,
        KeyValue {
            key: VALIDATOR_REWARDS_CACHE_KEY,
            value: serde_json::to_value(validator_rewards).unwrap(),
        },
    )
    .await;

    caching::publish_cache_update(&pool, VALIDATOR_REWARDS_CACHE_KEY).await;

    Ok(())
}
