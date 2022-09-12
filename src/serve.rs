use axum::body::HttpBody;
use axum::http::header;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Request;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::Extension;
use axum::Json;
use axum::Router;
use etag::EntityTag;
use futures::TryStreamExt;
use reqwest::StatusCode;
use serde_json::Value;
use sqlx::Connection;
use sqlx::PgExecutor;
use sqlx::PgPool;
use std::borrow::BorrowMut;
use std::sync::Arc;
use std::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::event;
use tracing::span;
use tracing::Instrument;
use tracing::Level;

use crate::caching::CacheKey;
use crate::config;
use crate::key_value_store;

type StateExtension = Extension<Arc<State>>;

type CachedValue = RwLock<Option<Value>>;

#[derive(Debug)]
struct Cache {
    base_fee_per_gas: CachedValue,
    base_fee_per_gas_stats: CachedValue,
    base_fee_over_time: CachedValue,
    block_lag: CachedValue,
    eth_price_stats: CachedValue,
    eth_supply_parts: CachedValue,
    merge_estimate: CachedValue,
    total_difficulty_progress: CachedValue,
}

pub struct State {
    cache: Arc<Cache>,
    db_pool: PgPool,
}

async fn get_value_hash_lock(connection: impl PgExecutor<'_>, key: &CacheKey<'_>) -> CachedValue {
    let value = key_value_store::get_value(connection, &key.to_db_key()).await;
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
    let body = body.borrow_mut().data().await;

    match body {
        None => {
            event!(Level::TRACE, path, "response without body, skipping etag");
            Ok(parts.into_response())
        }
        Some(body) => {
            let body = body.unwrap();

            match if_none_match_header {
                None => {
                    let etag = EntityTag::from_data(&body);

                    parts.headers.insert(
                        header::ETAG,
                        HeaderValue::from_str(&etag.to_string()).unwrap(),
                    );

                    event!(
                        Level::TRACE,
                        path,
                        etag = etag.to_string(),
                        "no if-none-match header"
                    );

                    Ok((parts, body).into_response())
                }
                Some(if_none_match) => {
                    let if_none_match_etag = if_none_match
                        .to_str()
                        .unwrap()
                        .parse::<EntityTag>()
                        .unwrap();
                    dbg!(&body);
                    let etag = EntityTag::from_data(&body);

                    parts.headers.insert(
                        header::ETAG,
                        HeaderValue::from_str(&etag.to_string()).unwrap(),
                    );

                    let some_match = etag.strong_eq(&if_none_match_etag);

                    event!(
                        Level::TRACE,
                        path,
                        etag = etag.to_string(),
                        some_match,
                        "if-none-match" = if_none_match_etag.to_string()
                    );

                    if some_match {
                        Ok((StatusCode::NOT_MODIFIED, parts).into_response())
                    } else {
                        Ok((parts, body).into_response())
                    }
                }
            }
        }
    }
}

async fn get_cached<'a>(cached_value: &CachedValue) -> impl IntoResponse {
    let cached_value_inner = cached_value.read().unwrap();
    match &*cached_value_inner {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(merge_estimate) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_str("public, max-age=4, s-maxage=1, stale-while-revalidate=60")
                    .unwrap(),
            );

            (headers, Json(merge_estimate).into_response()).into_response()
        }
    }
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
    let value = key_value_store::get_value(connection, &cache_key.to_db_key()).await;
    let mut cache_wlock = cached_value.write().unwrap();
    *cache_wlock = value;
}

async fn update_cache_from_notifications(state: Arc<State>, db_pool: &PgPool) {
    let mut listener = sqlx::postgres::PgListener::connect(&config::get_db_url())
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
                key @ CacheKey::BaseFeePerGas => {
                    update_cache_from_key(&mut connection, &state.cache.base_fee_per_gas, &key)
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
                key @ CacheKey::TotalDifficultyProgress => {
                    update_cache_from_key(
                        &mut connection,
                        &state.cache.total_difficulty_progress,
                        &key,
                    )
                    .await
                }
                key @ CacheKey::BaseFeeOverTime => {
                    update_cache_from_key(&mut connection, &state.cache.base_fee_over_time, &key)
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
    tracing_subscriber::fmt::init();

    let db_pool = PgPool::connect(&config::get_db_url_with_name("eth-analysis-serve"))
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
            .layer(
                ServiceBuilder::new()
                    .layer(Extension(shared_state))
                    .layer(CompressionLayer::new())
                    .layer(middleware::from_fn(etag_middleware)),
            );

    let port = config::get_env_var("PORT").unwrap_or("3002".to_string());

    event!(Level::INFO, port, "server listening");
    axum::Server::bind(&format!("0.0.0.0:{port}").parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
