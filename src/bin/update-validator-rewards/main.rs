mod mev_blocks;

use crate::mev_blocks::sync_mev_blocks;
use chrono::Utc;
use eth_analysis::{
    beacon_chain::{balances, BeaconNodeHttp},
    caching::{self, CacheKey},
    db,
    execution_chain::LONDON_HARD_FORK_TIMESTAMP,
    log,
    mev_blocks::{MevBlocksStorePostgres, RelayApiHttp},
    units::{EthNewtype, GweiImprecise, GweiNewtype, GWEI_PER_ETH_F64},
};
use serde::Serialize;
use sqlx::{Decode, PgExecutor, PgPool};
use tracing::{debug, info};

#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorReward {
    annual_reward: GweiImprecise,
    apr: f64,
}

fn get_days_since_london() -> i64 {
    (Utc::now() - *LONDON_HARD_FORK_TIMESTAMP).num_days()
}

#[derive(Decode)]
struct TipsSinceLondonRow {
    tips_since_london: f64,
}

async fn get_tips_since_london<'a>(pool: impl PgExecutor<'a>) -> sqlx::Result<GweiNewtype> {
    sqlx::query_as!(
        TipsSinceLondonRow,
        r#"
            SELECT SUM(tips) / 1e9 AS "tips_since_london!" FROM blocks
        "#,
    )
    .fetch_one(pool)
    .await
    .map(|row| GweiNewtype(row.tips_since_london.round() as i64))
}

// Tips and MEV rewards overlap. Whenever a validator is paid MEV rewards they are not paid
// tips, or rather they are included in the MEV bid already. We estimate roughly 90% of slots are
// handled by mev blocks. Therefore we estimate only 10% of tips paid on-chain are tips earned by
// validators.
const TIPS_REWARD_FACTOR: f64 = 0.1;

async fn calc_tips_reward<'a>(
    executor: impl PgExecutor<'a>,
    effective_balance_sum: GweiNewtype,
) -> sqlx::Result<ValidatorReward> {
    let GweiNewtype(tips_since_london) = get_tips_since_london(executor).await?;
    debug!("tips since london {}", tips_since_london);

    let tips_per_year =
        tips_since_london as f64 / get_days_since_london() as f64 * 365.25 * TIPS_REWARD_FACTOR;
    let single_validator_share =
        (32_f64 * EthNewtype::GWEI_PER_ETH as f64) / effective_balance_sum.0 as f64;
    debug!("single validator share {}", tips_since_london);

    let tips_earned_per_year_per_validator = tips_per_year * single_validator_share;
    debug!(
        "tips earned per year per validator {}",
        tips_earned_per_year_per_validator
    );

    let apr = tips_earned_per_year_per_validator / (32 * EthNewtype::GWEI_PER_ETH) as f64;
    debug!("tips APR {}", apr);

    Ok(ValidatorReward {
        annual_reward: GweiImprecise(tips_earned_per_year_per_validator),
        apr,
    })
}

const MAX_EFFECTIVE_BALANCE: f64 = 32f64 * GWEI_PER_ETH_F64;
const SECONDS_PER_SLOT: u8 = 12;
const SLOTS_PER_EPOCH: u8 = 32;
const SLOTS_PER_YEAR: f64 = 365.25 * 24.0 * 60.0 * 60.0 / SECONDS_PER_SLOT as f64;
const EPOCHS_PER_DAY: f64 =
    (24 * 60 * 60) as f64 / SLOTS_PER_EPOCH as f64 / SECONDS_PER_SLOT as f64;
const EPOCHS_PER_YEAR: f64 = 365.25 * EPOCHS_PER_DAY;

const BASE_REWARD_FACTOR: u8 = 64;

// Consider staying in Gwei until the last moment instead of converting early.
pub fn calc_issuance_reward(GweiNewtype(effective_balance_sum): GweiNewtype) -> ValidatorReward {
    let active_validators = effective_balance_sum as f64 / GWEI_PER_ETH_F64 / 32f64;

    // Balance at stake (Gwei)
    let max_balance_at_stake = active_validators * MAX_EFFECTIVE_BALANCE;

    let max_issuance_per_epoch = ((BASE_REWARD_FACTOR as f64 * max_balance_at_stake)
        / max_balance_at_stake.sqrt().floor())
    .trunc();
    let max_issuance_per_year = max_issuance_per_epoch * EPOCHS_PER_YEAR;

    let annual_reward = max_issuance_per_year / active_validators;
    let apr = max_issuance_per_year / effective_balance_sum as f64;

    debug!(
        "total effective balance: {} ETH",
        effective_balance_sum as f64 / GWEI_PER_ETH_F64
    );
    debug!("nr of active validators: {}", active_validators);
    debug!(
        "max issuance per epoch: {} ETH",
        max_issuance_per_epoch / GWEI_PER_ETH_F64
    );
    debug!(
        "max issuance per year: {} ETH",
        max_issuance_per_year / GWEI_PER_ETH_F64
    );
    debug!("APR: {:.2}%", apr * 100f64);

    ValidatorReward {
        annual_reward: GweiImprecise(annual_reward),
        apr,
    }
}

#[derive(Debug, Serialize)]
struct ValidatorRewards {
    issuance: ValidatorReward,
    tips: ValidatorReward,
    mev: Option<ValidatorReward>,
}

async fn get_validator_rewards(db_pool: &PgPool, beacon_node: &BeaconNodeHttp) -> ValidatorRewards {
    let last_effective_balance_sum =
        balances::get_last_effective_balance_sum(db_pool, beacon_node).await;
    let issuance_reward = calc_issuance_reward(last_effective_balance_sum);
    let tips_reward = calc_tips_reward(db_pool, last_effective_balance_sum)
        .await
        .unwrap();
    let mev = mev_blocks::calc_mev_reward(db_pool, last_effective_balance_sum)
        .await
        .unwrap();

    ValidatorRewards {
        issuance: issuance_reward,
        mev: Some(mev),
        tips: tips_reward,
    }
}

#[tokio::main]
pub async fn main() {
    log::init();

    info!("updating validator rewards");

    let db_pool = db::get_db_pool("update-validator-rewards", 3).await;

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNodeHttp::new();
    let relay_api = RelayApiHttp::new();
    let mev_blocks_store = MevBlocksStorePostgres::new(db_pool.clone());

    sync_mev_blocks(&mev_blocks_store, &beacon_node, &relay_api).await;

    let validator_rewards = get_validator_rewards(&db_pool, &beacon_node).await;
    debug!("validator rewards: {:?}", validator_rewards);

    caching::update_and_publish(&db_pool, &CacheKey::ValidatorRewards, validator_rewards).await;

    info!("done updating validator rewards");
}
