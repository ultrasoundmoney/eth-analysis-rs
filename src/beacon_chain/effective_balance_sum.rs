use anyhow::Result;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgExecutor, Row,
};
use tracing::info;

use crate::{
    beacon_chain,
    caching::{self, CacheKey},
    db, key_value_store, log,
    units::GweiNewtype,
};

use super::{node::StateRoot, BeaconNode};

async fn get_effective_balance_sum(
    beacon_node: &BeaconNode,
    state_root: &StateRoot,
) -> GweiNewtype {
    beacon_node
        .get_validators_by_state(state_root)
        .await
        .unwrap()
        .iter()
        .filter(|validator_envelope| validator_envelope.is_active())
        // Sum the effective balance of all validators in the state.
        .fold(GweiNewtype(0), |sum, validator_envelope| {
            sum + validator_envelope.effective_balance()
        })
}

#[allow(dead_code)]
pub async fn get_stored_effective_balance_sum(
    executor: impl PgExecutor<'_>,
    state_root: &StateRoot,
) -> Option<GweiNewtype> {
    // Although we expect not to have an effective_balance_sum for many state_roots we do expect
    // all requested state_roots to be in the DB.
    let row = sqlx::query!(
        "
            SELECT
                effective_balance_sum
            FROM
                beacon_states
            WHERE
                state_root = $1
        ",
        state_root
    )
    .fetch_one(executor)
    .await
    .unwrap();

    row.effective_balance_sum.map(GweiNewtype)
}

#[allow(dead_code)]
pub async fn get_last_stored_effective_balance_sum(executor: impl PgExecutor<'_>) -> GweiNewtype {
    sqlx::query(
        "
            SELECT
                effective_balance_sum
            FROM
                beacon_states
            WHERE
                effective_balance_sum IS NOT NULL
            ORDER BY slot DESC
            LIMIT 1
        ",
    )
    .map(|row: PgRow| {
        let effective_balance_sum_i64 = row.get::<i64, _>("effective_balance_sum");
        GweiNewtype(effective_balance_sum_i64)
    })
    .fetch_one(executor)
    .await
    .expect("should have at least one effective balance sum stored before getting the last")
}

async fn store_effective_balance_sum<'a>(
    executor: impl PgExecutor<'a>,
    effective_balance_sum: GweiNewtype,
    state_root: &StateRoot,
) {
    let gwei_i64: i64 = effective_balance_sum
        .try_into()
        .expect("GweiAmount to fit in i64 when encoding for storage in Postgres");
    sqlx::query!(
        "
            UPDATE
                beacon_states
            SET
                effective_balance_sum = $1
            WHERE
                state_root = $2
        ",
        gwei_i64,
        state_root
    )
    .execute(executor)
    .await
    .unwrap();
}

pub async fn update_effective_balance_sum() -> Result<()> {
    log::init_with_env();

    info!("updating effective balance sum");

    let db_pool = PgPoolOptions::new()
        .connect(&db::get_db_url_with_name("update-effective-balance-sum"))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNode::new();
    let last_state = beacon_chain::get_last_state(&db_pool)
        .await
        .expect("at least one beacon slot to be synced before updating effective balances");

    let effective_balance_sum =
        get_effective_balance_sum(&beacon_node, &last_state.state_root).await;

    store_effective_balance_sum(&db_pool, effective_balance_sum, &last_state.state_root).await;

    let effective_balance_sum_f64: f64 = effective_balance_sum.into();

    key_value_store::set_value(
        &db_pool,
        CacheKey::EffectiveBalanceSum.to_db_key(),
        &effective_balance_sum_f64.into(),
    )
    .await?;

    caching::publish_cache_update(&db_pool, &CacheKey::EffectiveBalanceSum).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::Connection;

    use crate::beacon_chain::states;
    use crate::beacon_chain::BeaconNode;
    use crate::beacon_chain::Slot;

    use super::*;

    const SLOT_0_STATE_ROOT: &str =
        "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b";

    const SLOT_0_EFFECTIVE_BALANCES_SUM: GweiNewtype = GweiNewtype(674016000000000);

    #[tokio::test]
    async fn get_effective_balance_sum_test() {
        let beacon_node = BeaconNode::new();
        let effective_balance_sum =
            get_effective_balance_sum(&beacon_node, &SLOT_0_STATE_ROOT.to_string()).await;

        assert_eq!(effective_balance_sum, SLOT_0_EFFECTIVE_BALANCES_SUM);
    }

    #[tokio::test]
    async fn get_store_effective_balance_sum_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        states::store_state(&mut transaction, "0xstate_root", &Slot(0)).await;

        store_effective_balance_sum(
            &mut transaction,
            GweiNewtype(10),
            &"0xstate_root".to_string(),
        )
        .await;

        let stored_effective_balance_sum =
            get_stored_effective_balance_sum(&mut transaction, &"0xstate_root".to_string()).await;

        assert_eq!(stored_effective_balance_sum, Some(GweiNewtype(10)))
    }

    #[tokio::test]
    async fn get_not_stored_effective_balance_sum_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        states::store_state(&mut transaction, "0xstate_root", &Slot(0)).await;

        let stored_effective_balance_sum =
            get_stored_effective_balance_sum(&mut transaction, &"0xstate_root".to_string()).await;

        assert_eq!(stored_effective_balance_sum, None)
    }

    #[tokio::test]
    async fn get_last_stored_effective_balance_sum_test() {
        let mut connection = db::tests::get_test_db_connection().await;
        let mut transaction = connection.begin().await.unwrap();

        states::store_state(&mut transaction, "0xtest_root", &Slot(0)).await;
        store_effective_balance_sum(
            &mut transaction,
            GweiNewtype(10),
            &"0xtest_root".to_string(),
        )
        .await;

        let last_stored_effective_balance_sum =
            get_last_stored_effective_balance_sum(&mut transaction).await;

        assert_eq!(last_stored_effective_balance_sum, GweiNewtype(10));
    }
}
