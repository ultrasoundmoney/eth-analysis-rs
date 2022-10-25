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
use futures::TryStreamExt;
use reqwest::StatusCode;
use serde_json::Value;
use sqlx::{Connection, PgExecutor, PgPool};
use std::sync::{Arc, RwLock};
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::{error, event, span, trace, Instrument, Level};

use crate::{caching::CacheKey, db, env, key_value_store, log};

type StateExtension = Extension<Arc<State>>;

type CachedValue = RwLock<Option<Value>>;

#[derive(Debug)]
struct Cache {
    base_fee_over_time: CachedValue,
    base_fee_per_gas: CachedValue,
    base_fee_per_gas_stats: CachedValue,
    block_lag: CachedValue,
    eth_price_stats: CachedValue,
    eth_supply_parts: CachedValue,
    merge_estimate: CachedValue,
    merge_status: CachedValue,
    supply_since_merge: CachedValue,
    total_difficulty_progress: CachedValue,
}

pub struct State {
    cache: Arc<Cache>,
    db_pool: PgPool,
}

async fn get_value_hash_lock(connection: impl PgExecutor<'_>, key: &CacheKey<'_>) -> CachedValue {
    let value = key_value_store::get_raw_caching_value(connection, key).await;
    RwLock::new(value)
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

    match bytes.len() == 0 {
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
        Some(merge_estimate) => {
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

            (headers, Json(merge_estimate).into_response()).into_response()
        }
    }
}

async fn get_cached(cached_value: &CachedValue) -> impl IntoResponse {
    get_cached_with_cache_duration(cached_value, None, None).await
}

async fn update_cache_from_key(
    connection: impl PgExecutor<'_>,
    cached_value: &CachedValue,
    cache_key: &CacheKey<'_>,
) {
    event!(
        Level::DEBUG,
        cache_key = cache_key.to_string(),
        "cache update",
    );
    let value = key_value_store::get_raw_caching_value(connection, cache_key).await;
    let mut cache_wlock = cached_value.write().unwrap();
    *cache_wlock = value;
}

async fn update_cache_from_notifications(state: Arc<State>, db_pool: &PgPool) {
    let mut listener =
        sqlx::postgres::PgListener::connect(&db::get_db_url_with_name("serve-rs-cache-update"))
            .await
            .unwrap();
    listener.listen("cache-update").await.unwrap();
    event!(Level::DEBUG, "listening for cache updates");

    let mut notification_stream = listener.into_stream();
    let mut connection = db_pool.acquire().await.unwrap();

    tokio::spawn(async move {
        while let Some(notification) = notification_stream
            .try_next()
            .instrument(span!(Level::INFO, "cache update"))
            .await
            .unwrap()
        {
            let payload = notification.payload();
            let payload_cache_key = CacheKey::from(payload);
            match payload_cache_key {
                key @ CacheKey::BaseFeeOverTime => {
                    update_cache_from_key(&mut connection, &state.cache.base_fee_over_time, &key)
                        .await
                }
                key @ CacheKey::BaseFeePerGas => {
                    update_cache_from_key(&mut connection, &state.cache.base_fee_per_gas, &key)
                        .await
                }
                key @ CacheKey::BaseFeePerGasStats => {
                    update_cache_from_key(
                        &mut connection,
                        &state.cache.base_fee_per_gas_stats,
                        &key,
                    )
                    .await
                }
                key @ CacheKey::BlockLag => {
                    update_cache_from_key(&mut connection, &state.cache.block_lag, &key).await
                }
                key @ CacheKey::EthPrice => {
                    update_cache_from_key(&mut connection, &state.cache.eth_price_stats, &key).await
                }
                key @ CacheKey::EthSupplyParts => {
                    update_cache_from_key(&mut connection, &state.cache.eth_supply_parts, &key)
                        .await
                }
                key @ CacheKey::MergeEstimate => {
                    update_cache_from_key(&mut connection, &state.cache.merge_estimate, &key).await
                }
                key @ CacheKey::MergeStatus => {
                    update_cache_from_key(&mut connection, &state.cache.merge_status, &key).await
                }
                key @ CacheKey::SupplySinceMerge => {
                    update_cache_from_key(&mut connection, &state.cache.supply_since_merge, &key)
                        .await
                }
                key @ CacheKey::TotalDifficultyProgress => {
                    update_cache_from_key(
                        &mut connection,
                        &state.cache.total_difficulty_progress,
                        &key,
                    )
                    .await
                }
                key => {
                    event!(
                        Level::DEBUG,
                        "cache_key" = key.to_string(),
                        "unspported cache update, skipping"
                    );
                }
            }
        }
    });
}

pub async fn start_server() {
    log::init_with_env();

    let db_pool = PgPool::connect(&db::get_db_url_with_name("eth-analysis-serve"))
        .await
        .unwrap();

    sqlx::migrate!().run(&db_pool).await.unwrap();

    event!(Level::DEBUG, "warming cache");

    let base_fee_per_gas = get_value_hash_lock(&db_pool, &CacheKey::BaseFeePerGas).await;
    let base_fee_over_time = get_value_hash_lock(&db_pool, &CacheKey::BaseFeeOverTime).await;
    let base_fee_per_gas_stats = get_value_hash_lock(&db_pool, &CacheKey::BaseFeePerGasStats).await;
    let block_lag = get_value_hash_lock(&db_pool, &CacheKey::BlockLag).await;
    let eth_price_stats = get_value_hash_lock(&db_pool, &CacheKey::EthPrice).await;
    let eth_supply_parts = get_value_hash_lock(&db_pool, &CacheKey::EthSupplyParts).await;
    let merge_estimate = get_value_hash_lock(&db_pool, &CacheKey::MergeEstimate).await;
    let merge_status = get_value_hash_lock(&db_pool, &CacheKey::MergeStatus).await;
    let supply_since_merge = get_value_hash_lock(&db_pool, &CacheKey::SupplySinceMerge).await;
    let total_difficulty_progress =
        get_value_hash_lock(&db_pool, &CacheKey::TotalDifficultyProgress).await;

    let cache = Arc::new(Cache {
        base_fee_per_gas,
        base_fee_over_time,
        base_fee_per_gas_stats,
        block_lag,
        eth_price_stats,
        eth_supply_parts,
        merge_estimate,
        merge_status,
        supply_since_merge,
        total_difficulty_progress,
    });

    event!(Level::INFO, "cache ready");

    let shared_state = Arc::new(State { cache, db_pool });

    update_cache_from_notifications(shared_state.clone(), &shared_state.db_pool).await;

    let app =
        Router::new()
            .route(
                "/api/v2/fees/base-fee-per-gas",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.base_fee_per_gas)
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
                "/api/v2/fees/eth-price-stats",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.eth_price_stats)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/eth-supply-parts",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.eth_supply_parts)
                        .await
                        .into_response()
                }),
            )
            .route(
                "/api/v2/fees/merge-estimate",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.merge_estimate)
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
                "/api/v2/fees/base-fee-over-time",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.base_fee_over_time)
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
                "/healthz",
                get(|state: StateExtension| async move {
                    let _ = &state.db_pool.acquire().await.unwrap().ping().await.unwrap();
                    StatusCode::OK
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
                "/api/v2/fees/merge-status",
                get(|state: StateExtension| async move {
                    get_cached(&state.clone().cache.merge_status)
                        .await
                        .into_response()
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
            .layer(
                ServiceBuilder::new()
                    .layer(middleware::from_fn(etag_middleware))
                    .layer(CompressionLayer::new())
                    .layer(Extension(shared_state)),
            );

    let port = env::get_env_var("PORT").unwrap_or("3002".to_string());

    event!(Level::INFO, port, "server listening");
    axum::Server::bind(&format!("0.0.0.0:{port}").parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
