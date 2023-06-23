use tracing::{debug, info};

use eth_analysis::{
    beacon_chain::{self, BeaconNodeHttp},
    caching::{self, CacheKey},
    db,
    effective_balance_sums::{self, EffectiveBalanceSum},
    log,
};

#[tokio::main]
pub async fn main() {
    log::init_with_env();

    info!("updating effective balance sum");

    let db_pool = db::get_db_pool("update-effective-balance-sum").await;

    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNodeHttp::new();
    let last_state = beacon_chain::get_last_state(&db_pool)
        .await
        .expect("expect at least one beacon slot to be synced before updating effective balances");

    let sum =
        effective_balance_sums::get_effective_balance_sum(&beacon_node, &last_state.state_root)
            .await;

    effective_balance_sums::store_effective_balance_sum(&db_pool, &last_state.state_root, &sum)
        .await;

    let effective_balance_sum = EffectiveBalanceSum::new(&last_state.slot, sum);

    debug!("effective balance sum updated {:?}", effective_balance_sum);

    caching::update_and_publish(
        &db_pool,
        &CacheKey::EffectiveBalanceSum,
        effective_balance_sum,
    )
    .await;
}
