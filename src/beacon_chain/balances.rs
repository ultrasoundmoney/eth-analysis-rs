use reqwest::Client;
use sqlx::PgPool;

use crate::eth_units::GweiAmount;
use crate::supply_projection::{GweiInTime, GweiInTimeRow};

use super::beacon_time::FirstOfDaySlot;
use super::node::ValidatorBalance;
use super::{beacon_time, node, states};

pub fn sum_validator_balances(validator_balances: Vec<ValidatorBalance>) -> GweiAmount {
    validator_balances
        .iter()
        .fold(GweiAmount(0), |sum, validator_balance| {
            sum + validator_balance.balance
        })
}

pub async fn store_validator_sum_for_day(
    pool: &PgPool,
    state_root: &str,
    FirstOfDaySlot(slot): &FirstOfDaySlot,
    gwei: &GweiAmount,
) {
    let gwei: i64 = gwei.to_owned().into();

    sqlx::query!(
        "
            INSERT INTO beacon_validators_balance (timestamp, state_root, gwei) VALUES ($1, $2, $3)
        ",
        beacon_time::get_timestamp(slot),
        state_root,
        gwei,
    )
    .execute(pool)
    .await
    .unwrap();
}

pub async fn get_last_effective_balance_sum(
    pool: &PgPool,
    client: &Client,
) -> anyhow::Result<GweiAmount> {
    let last_state_root = states::get_last_state(pool)
        .await?
        .expect("can't calculate a last effective balance with an empty beacon_states table")
        .state_root;

    node::get_validators_by_state(client, &last_state_root)
        .await
        .map(|validators| {
            validators.iter().fold(GweiAmount(0), |sum, validator| {
                sum + validator.effective_balance
            })
        })
        .map_err(anyhow::Error::msg)
}

pub async fn get_validator_balances_by_day(pool: &PgPool) -> sqlx::Result<Vec<GweiInTime>> {
    sqlx::query_as!(
        GweiInTimeRow,
        "
            SELECT timestamp, gwei FROM beacon_validators_balance
        "
    )
    .fetch_all(pool)
    .await
    .map(|rows| rows.iter().map(|row| row.into()).collect())
}
