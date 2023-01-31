use anyhow::Result;
use axum::{
    body::HttpBody,
    http::{header, HeaderMap, HeaderValue, Request},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use bytes::BufMut;
use etag::EntityTag;
use futures::{try_join, TryFutureExt, TryStreamExt};
use reqwest::StatusCode;
use serde_json::Value;
use sqlx::{Connection, PgExecutor, PgPool};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::{debug, error, info, trace};

use crate::{
    caching::{self, CacheKey, ParseCacheKeyError},
    db, env, log,
};

type StateExtension = Extension<Arc<State>>;

type CachedValue = RwLock<Option<Value>>;

#[derive(Debug)]
struct Cache {
    base_fee_over_time: CachedValue,
    base_fee_per_gas: CachedValue,
    base_fee_per_gas_stats: CachedValue,
    block_lag: CachedValue,
    burn_rates: CachedValue,
    burn_sums: CachedValue,
    effective_balance_sum: CachedValue,
    eth_price_stats: CachedValue,
    supply_parts: CachedValue,
    issuance_breakdown: CachedValue,
    issuance_estimate: CachedValue,
    supply_changes: CachedValue,
    supply_dashboard_analysis: CachedValue,
    supply_over_time: CachedValue,
    supply_projection_inputs: CachedValue,
    supply_since_merge: CachedValue,
    total_difficulty_progress: CachedValue,
    validator_rewards: CachedValue,
}

pub struct State {
    cache: Arc<Cache>,
    db_pool: PgPool,
}

async fn get_value_hash_lock(
    connection: impl PgExecutor<'_>,
    key: &CacheKey,
) -> Result<CachedValue> {
    let value = caching::get_serialized_caching_value(connection, key).await?;
    Ok(RwLock::new(value))
}

async fn etag_middleware<B: std::fmt::Debug>(
    req: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    let if_none_match_header = req.headers().get(header::IF_NONE_MATCH).cloned();
    let path = req.uri().path().to_owned();
    let res = next.run(req).await;
    let (mut parts, mut body) = res.into_parts();

    let bytes = {
        let mut body_bytes = vec![];

        while let Some(inner) = body.data().await {
            let bytes = inner.unwrap();
            body_bytes.put(bytes);
        }

        body_bytes
    };

    match bytes.is_empty() {
        true => {
            trace!(path, "response without body, skipping etag");
            Ok(parts.into_response())
        }
        false => match if_none_match_header {
            None => {
                let etag = EntityTag::from_data(&bytes);

                parts.headers.insert(
                    header::ETAG,
                    HeaderValue::from_str(&etag.to_string()).unwrap(),
                );

                trace!(path, %etag, "no if-none-match header");

                Ok((parts, bytes).into_response())
            }
            Some(if_none_match) => {
                let if_none_match_etag = if_none_match.to_str().unwrap().parse::<EntityTag>();
                match if_none_match_etag {
                    Err(ref err) => {
                        error!("{} - {:?}", err, &if_none_match_etag);
                        let etag = EntityTag::from_data(&bytes);
                        parts.headers.insert(
                            header::ETAG,
                            HeaderValue::from_str(&etag.to_string()).unwrap(),
                        );
                        Ok((parts, bytes).into_response())
                    }
                    Ok(if_none_match_etag) => {
                        let etag = EntityTag::from_data(&bytes);

                        parts.headers.insert(
                            header::ETAG,
                            HeaderValue::from_str(&etag.to_string()).unwrap(),
                        );

                        let some_match = etag.strong_eq(&if_none_match_etag);

                        trace!(
                            path,
                            %etag,
                            some_match,
                            "if-none-match" = %if_none_match_etag
                        );

                        if some_match {
                            Ok((StatusCode::NOT_MODIFIED, parts).into_response())
                        } else {
                            Ok((parts, bytes).into_response())
                        }
                    }
                }
            }
        },
    }
}

async fn get_cached_with_cache_duration(
    cached_value: &CachedValue,
    max_age: Option<u32>,
    // s_max_age: Option<u32>,
    stale_while_revalidate: Option<u32>,
) -> impl IntoResponse {
    let cached_value_inner = cached_value.read().unwrap();
    match &*cached_value_inner {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(cached_value) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_str(&format!(
                    // "public, max-age={}, s-maxage={}, stale-while-revalidate={}",
                    "public, max-age={}, stale-while-revalidate={}",
                    max_age.unwrap_or(6),
                    // s_max_age.unwrap_or(4),
                    stale_while_revalidate.unwrap_or(120)
                ))
                .unwrap(),
            );

            (headers, Json(cached_value).into_response()).into_response()
        }
    }
}

async fn get_cached(cached_value: &CachedValue) -> impl IntoResponse {
    get_cached_with_cache_duration(cached_value, None, None).await
}

fn get_cache_value_by_key<'a>(cache: &'a Arc<Cache>, key: &'a CacheKey) -> &'a CachedValue {
    match key {
        CacheKey::BaseFeeOverTime => &cache.base_fee_over_time,
        CacheKey::BaseFeePerGas => &cache.base_fee_per_gas,
        CacheKey::BaseFeePerGasStats => &cache.base_fee_per_gas_stats,
        CacheKey::BlockLag => &cache.block_lag,
        CacheKey::BurnRates => &cache.burn_rates,
        CacheKey::BurnSums => &cache.burn_sums,
        CacheKey::EffectiveBalanceSum => &cache.effective_balance_sum,
        CacheKey::EthPrice => &cache.eth_price_stats,
        CacheKey::SupplyParts => &cache.supply_parts,
        CacheKey::IssuanceBreakdown => &cache.issuance_breakdown,
        CacheKey::IssuanceEstimate => &cache.issuance_estimate,
        CacheKey::SupplyChanges => &cache.supply_changes,
        CacheKey::SupplyDashboardAnalysis => &cache.supply_dashboard_analysis,
        CacheKey::SupplyOverTime => &cache.supply_over_time,
        CacheKey::SupplyProjectionInputs => &cache.supply_projection_inputs,
        CacheKey::SupplySinceMerge => &cache.supply_since_merge,
        CacheKey::TotalDifficultyProgress => &cache.total_difficulty_progress,
        CacheKey::ValidatorRewards => &cache.validator_rewards,
    }
}

async fn update_cache_from_key(
    connection: impl PgExecutor<'_>,
    cached_value: &CachedValue,
    cache_key: &CacheKey,
) -> Result<()> {
    debug!(%cache_key, "cache update",);
    let value = caching::get_serialized_caching_value(connection, cache_key).await?;
    let mut cache_wlock = cached_value.write().unwrap();
    *cache_wlock = value;
    Ok(())
}

async fn update_cache_from_notifications(state: Arc<State>, db_pool: &PgPool) -> JoinHandle<()> {
    let mut listener =
        sqlx::postgres::PgListener::connect(&db::get_db_url_with_name("serve-rs-cache-update"))
            .await
            .unwrap();
    listener.listen("cache-update").await.unwrap();
    debug!("listening for cache updates");

    let mut notification_stream = listener.into_stream();
    let mut connection = db_pool.acquire().await.unwrap();

    tokio::spawn(async move {
        while let Some(notification) = notification_stream.try_next().await.unwrap() {
            let payload = notification.payload();
            let cache_key = payload.parse::<CacheKey>();

            match cache_key {
                Err(ParseCacheKeyError::UnknownCacheKey(cache_key)) => {
                    trace!(
                        %cache_key,
                        "unspported cache update, skipping"
                    );
                }
                Ok(cache_key) => {
                    let cache_value = get_cache_value_by_key(&state.cache, &cache_key);
                    update_cache_from_key(&mut connection, cache_value, &cache_key)
                        .await
                        .unwrap();
                }
            }
        }
    })
}

pub async fn start_server() -> Result<()> {
    log::init_with_env();

    let db_pool = PgPool::connect(&db::get_db_url_with_name("eth-analysis-serve")).await?;

    sqlx::migrate!().run(&db_pool).await?;

    debug!("warming cache");

    let (
        base_fee_over_time,
        base_fee_per_gas,
        base_fee_per_gas_stats,
        block_lag,
        burn_rates,
        burn_sums,
        effective_balance_sum,
        eth_price_stats,
        supply_parts,
        issuance_breakdown,
        issuance_estimate,
        supply_changes,
        supply_dashboard_analysis,
        supply_over_time,
        supply_projection_inputs,
        supply_since_merge,
        total_difficulty_progress,
        validator_rewards,
    ) = try_join!(
        get_value_hash_lock(&db_pool, &CacheKey::BaseFeeOverTime),
        get_value_hash_lock(&db_pool, &CacheKey::BaseFeePerGas),
        get_value_hash_lock(&db_pool, &CacheKey::BaseFeePerGasStats),
        get_value_hash_lock(&db_pool, &CacheKey::BlockLag),
        get_value_hash_lock(&db_pool, &CacheKey::BurnRates),
        get_value_hash_lock(&db_pool, &CacheKey::BurnSums),
        get_value_hash_lock(&db_pool, &CacheKey::EffectiveBalanceSum),
        get_value_hash_lock(&db_pool, &CacheKey::EthPrice),
        get_value_hash_lock(&db_pool, &CacheKey::SupplyParts),
        get_value_hash_lock(&db_pool, &CacheKey::IssuanceBreakdown),
        get_value_hash_lock(&db_pool, &CacheKey::IssuanceEstimate),
        get_value_hash_lock(&db_pool, &CacheKey::SupplyChanges),
        get_value_hash_lock(&db_pool, &CacheKey::SupplyDashboardAnalysis),
        get_value_hash_lock(&db_pool, &CacheKey::SupplyOverTime),
        get_value_hash_lock(&db_pool, &CacheKey::SupplyProjectionInputs),
        get_value_hash_lock(&db_pool, &CacheKey::SupplySinceMerge),
        get_value_hash_lock(&db_pool, &CacheKey::TotalDifficultyProgress),
        get_value_hash_lock(&db_pool, &CacheKey::ValidatorRewards),
    )?;

    let cache = Arc::new(Cache {
        base_fee_per_gas,
        base_fee_over_time,
        base_fee_per_gas_stats,
        block_lag,
        burn_rates,
        burn_sums,
        effective_balance_sum,
        eth_price_stats,
        supply_parts,
        issuance_breakdown,
        issuance_estimate,
        supply_changes,
        supply_dashboard_analysis,
        supply_over_time,
        supply_projection_inputs,
        supply_since_merge,
        total_difficulty_progress,
        validator_rewards,
    });

    info!("cache ready");

    let shared_state = Arc::new(State { cache, db_pool });

    let update_cache_thread =
        update_cache_from_notifications(shared_state.clone(), &shared_state.db_pool).await;

    let app =
        Router::new()
            .route(
                "/api/v2/fees/base-fee-over-time",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.base_fee_over_time)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/base-fee-per-gas",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.base_fee_per_gas)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/base-fee-per-gas-stats",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.base_fee_per_gas_stats)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/block-lag",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.block_lag).await
                }),
            )
            .route(
                "/api/v2/fees/healthz",
                get(|state: StateExtension| async move {
                    let _ = &state.db_pool.acquire().await.unwrap().ping().await.unwrap();
                    StatusCode::OK
                }),
            )
            .route(
                "/api/v2/fees/effective-balance-sum",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.effective_balance_sum)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/eth-price-stats",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.eth_price_stats)
                        .await
                        .into_response()
                }),
            )
            // Deprecated, remove after frontend switches over.
            .route(
                "/api/v2/fees/eth-supply-parts",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_parts)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/issuance-estimate",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.issuance_estimate)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/supply-changes",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_changes)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/supply-dashboard-analysis",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_dashboard_analysis)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/supply-over-time",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_over_time).await
                }),
            )
            .route(
                "/api/v2/fees/supply-parts",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_parts)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/supply-projection-inputs",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_projection_inputs).await
                }),
            )
            .route(
                "/api/v2/fees/supply-since-merge",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.supply_since_merge)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/total-difficulty-progress",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.total_difficulty_progress)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/validator-rewards",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.validator_rewards)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/healthz",
                get(|state: StateExtension| async move {
                    let _ = &state.db_pool.acquire().await.unwrap().ping().await.unwrap();
                    StatusCode::OK
                }),
            )
            .layer(
                ServiceBuilder::new()
                    .layer(middleware::from_fn(etag_middleware))
                    .layer(CompressionLayer::new())
                    .layer(Extension(shared_state)),
            );

    let port = env::get_env_var("PORT").unwrap_or_else(|| "3002".to_string());

    info!(port, "server listening");
    let socket_addr = format!("0.0.0.0:{port}").parse()?;
    let server_thread = axum::Server::bind(&socket_addr).serve(app.into_make_service());

    try_join!(
        update_cache_thread.map_err(|err| error!("{}", err)),
        server_thread.map_err(|err| error!("{}", err))
    )
    .unwrap();

    Ok(())
}
