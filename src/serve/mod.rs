mod caching;
mod etag_middleware;
mod health;

use axum::response::IntoResponse;
pub use caching::cached_get;
pub use caching::cached_get_with_custom_duration;

use axum::{middleware, routing::get, Extension, Router};
use chrono::Duration;
use futures::{try_join, TryFutureExt};
use lazy_static::lazy_static;
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::{debug, error, info};

use crate::health::HealthCheckable;
use crate::key_value_store::KeyValueStorePostgres;
use crate::serve::health::ServeHealth;
use crate::{caching::CacheKey, db, env, execution_chain, log};

use self::caching::Cache;

lazy_static! {
    static ref FOUR_SECONDS: Duration = Duration::seconds(4);
    static ref ONE_DAY: Duration = Duration::days(1);
    static ref ONE_MINUTE: Duration = Duration::minutes(1);
}

pub type StateExtension = Extension<Arc<State>>;

pub struct State {
    pub cache: Cache,
    pub db_pool: PgPool,
    pub health: ServeHealth,
}

pub async fn start_server() {
    log::init_with_env();

    let started_on = chrono::Utc::now();

    let db_pool = PgPool::connect(&db::get_db_url_with_name("eth-analysis-serve"))
        .await
        .unwrap();

    let key_value_store = KeyValueStorePostgres::new(db_pool.clone());

    sqlx::migrate!().run(&db_pool).await.unwrap();

    debug!("warming cache");

    let cache = Cache::new(&key_value_store).await;

    info!("cache ready");

    let health = ServeHealth::new(started_on);

    let shared_state = Arc::new(State {
        cache,
        db_pool,
        health,
    });

    let update_cache_thread =
        caching::update_cache_from_notifications(shared_state.clone(), &shared_state.db_pool).await;

    let app = Router::new()
        .route(
            "/api/v2/fees/average-eth-price",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::AverageEthPrice).await
            }),
        )
        .route(
            "/api/v2/fees/base-fee-over-time",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::BaseFeeOverTime).await
            }),
        )
        .route(
            "/api/v2/fees/base-fee-per-gas",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::BaseFeePerGas).await
            }),
        )
        .route("/api/v2/fees/base-fee-per-gas-barrier", 
            get(|state: StateExtension| async move {
                cached_get_with_custom_duration(state, &CacheKey::BaseFeePerGasBarrier, &ONE_MINUTE, &ONE_DAY).await
            }),
        )
        .route(
            "/api/v2/fees/base-fee-per-gas-stats",
            get(execution_chain::routes::base_fee_per_gas_stats),
        )
        .route(
            "/api/v2/fees/block-lag",
            get(
                |state: StateExtension| async move { cached_get(state, &CacheKey::BlockLag).await },
            ),
        )
        .route(
            "/api/v2/fees/burn-sums",
            get(
                |state: StateExtension| async move { cached_get(state, &CacheKey::BurnSums).await },
            ),
        )
        .route(
            "/api/v2/fees/burn-rates",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::BurnRates).await
            }),
        )
        .route(
            "/api/v2/fees/gauge-rates",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::GaugeRates).await
            }),
        )
        .route(
            "/api/v2/fees/healthz",
            get(|state: StateExtension| async move {
                state.health.health_status().into_response()
            }),
        )
        .route(
            "/api/v2/fees/effective-balance-sum",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::EffectiveBalanceSum).await
            }),
        )
        .route(
            "/api/v2/fees/eth-price-stats",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::EthPrice).await
            }),
        )
        // Deprecated, remove after frontend switches over.
        .route(
            "/api/v2/fees/eth-supply-parts",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplyParts).await
            }),
        )
        .route(
            "/api/v2/fees/issuance-estimate",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::IssuanceEstimate).await
            }),
        )
        .route(
            "/api/v2/fees/supply-changes",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplyChanges).await
            }),
        )
        .route(
            "/api/v2/fees/supply-dashboard-analysis",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplyDashboardAnalysis).await
            }),
        )
        .route(
            "/api/v2/fees/supply-over-time",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplyOverTime).await
            }),
        )
        .route(
            "/api/v2/fees/supply-parts",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplyParts).await
            }),
        )
        .route(
            "/api/v2/fees/supply-projection-inputs",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplyProjectionInputs).await
            }),
        )
        .route(
            "/api/v2/fees/supply-since-merge",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::SupplySinceMerge).await
            }),
        )
        .route(
            "/api/v2/fees/total-difficulty-progress",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::TotalDifficultyProgress).await
            }),
        )
        .route(
            "/api/v2/fees/validator-rewards",
            get(|state: StateExtension| async move {
                cached_get(state, &CacheKey::ValidatorRewards).await
            }),
        )
        .route(
            "/healthz",
            get(|state: StateExtension| async move {
                state.health.health_status().into_response()
            }),
        )
        .layer(
            ServiceBuilder::new()
                .layer(middleware::from_fn(etag_middleware::middleware_fn))
                .layer(CompressionLayer::new())
                .layer(Extension(shared_state)),
        );

    let port = env::get_env_var("PORT").unwrap_or_else(|| "3002".to_string());

    info!(port, "server listening");
    let socket_addr = format!("0.0.0.0:{port}").parse().unwrap();
    let server_thread = axum::Server::bind(&socket_addr).serve(app.into_make_service());

    try_join!(
        update_cache_thread.map_err(|err| error!("{}", err)),
        server_thread.map_err(|err| error!("{}", err))
    )
    .unwrap();
}
