use sqlx::{postgres::PgPoolOptions, PgExecutor};

use crate::{beacon_chain::states::get_last_state, caching, config, eth_units::GweiAmount};

use super::{node::StateRoot, BeaconNode};

async fn get_effective_balance_sum(beacon_node: &BeaconNode, state_root: &StateRoot) -> GweiAmount {
    beacon_node
        .get_validators_by_state(state_root)
        .await
        .map(|validators| {
            validators.iter().fold(GweiAmount(0), |sum, validator| {
                sum + validator.effective_balance
            })
        })
        .unwrap()
}

#[allow(dead_code)]
pub async fn get_stored_effective_balance_sum(
    executor: impl PgExecutor<'_>,
    state_root: &StateRoot,
) -> Option<GweiAmount> {
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

    row.effective_balance_sum
        .map(|effective_balance_sum| GweiAmount(effective_balance_sum as u64))
}

async fn store_effective_balance_sum<'a>(
    executor: impl PgExecutor<'a>,
    effective_balance_sum: GweiAmount,
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

const EFFECTIVE_BALANCE_SUM_CACHE_KEY: &str = "effective-balance-sum";

pub async fn update_effective_balance_sum() {
    tracing_subscriber::fmt::init();

    tracing::info!("updating validator rewards");

    let db_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&config::get_db_url_with_name(
            "update-effective-balance-sum",
        ))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNode::new();
    let last_state = get_last_state(&db_pool)
        .await
        .expect("at least one beacon slot to be synced before updating effective balances");

    let effective_balance_sum =
        get_effective_balance_sum(&beacon_node, &last_state.state_root).await;

    store_effective_balance_sum(&db_pool, effective_balance_sum, &last_state.state_root).await;

    caching::publish_cache_update(&db_pool, EFFECTIVE_BALANCE_SUM_CACHE_KEY).await;
}

#[cfg(test)]
mod tests {
    use sqlx::{Connection, PgConnection};

    use crate::beacon_chain::states;
    use crate::beacon_chain::BeaconNode;
    use crate::config;

    use super::*;

    const SLOT_0_STATE_ROOT: &str =
        "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b";

    const SLOT_0_EFFECTIVE_BALANCES_SUM: GweiAmount = GweiAmount(674016000000000);

    #[tokio::test]
    async fn get_effective_balance_sum_test() {
        let beacon_node = BeaconNode::new();
        let effective_balance_sum =
            get_effective_balance_sum(&beacon_node, &SLOT_0_STATE_ROOT.to_string()).await;

        assert_eq!(effective_balance_sum, SLOT_0_EFFECTIVE_BALANCES_SUM);
    }

    #[tokio::test]
    async fn get_store_effective_balance_sum_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        states::store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        store_effective_balance_sum(
            &mut transaction,
            GweiAmount(10),
            &"0xstate_root".to_string(),
        )
        .await;

        let stored_effective_balance_sum =
            get_stored_effective_balance_sum(&mut transaction, &"0xstate_root".to_string()).await;

        assert_eq!(stored_effective_balance_sum, Some(GweiAmount(10)))
    }

    #[tokio::test]
    async fn get_not_stored_effective_balance_sum_test() {
        let mut connection = PgConnection::connect(&config::get_db_url()).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();

        states::store_state(&mut transaction, "0xstate_root", &0)
            .await
            .unwrap();

        let stored_effective_balance_sum =
            get_stored_effective_balance_sum(&mut transaction, &"0xstate_root".to_string()).await;

        assert_eq!(stored_effective_balance_sum, None)
    }
}
