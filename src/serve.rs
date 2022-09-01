use axum::http::header;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Extension;
use axum::Json;
use axum::Router;
use etag::EntityTag;
use futures::TryStreamExt;
use reqwest::StatusCode;
use serde_json::Value;
use sha1::Digest;
use sha1::Sha1;
use sqlx::Connection;
use sqlx::PgConnection;
use std::sync::Arc;
use std::sync::RwLock;

use crate::caching::CacheKey;
use crate::config;
use crate::key_value_store;

type StateExtension = Extension<Arc<State>>;

type Hash = String;
type CachedValue = RwLock<Option<(Value, Hash)>>;

#[derive(Debug)]
struct Cache {
    total_difficulty_progress: CachedValue,
    eth_supply_parts: CachedValue,
    merge_estimate: CachedValue,
}

pub struct State {
    cache: Arc<Cache>,
}

fn hash_from_u8(text: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(text);
    let hash = hasher.finalize();
    base64::encode(hash)
}

fn hash_from_json(v: &Value) -> String {
    let v_bytes = serde_json::to_vec(v).unwrap();
    hash_from_u8(&v_bytes)
}

async fn get_value_etag_pair(
    connection: &mut PgConnection,
    key: &CacheKey<'_>,
) -> Option<(Value, String)> {
    let value = key_value_store::get_value(connection, &key.to_db_key()).await?;
    let hash = hash_from_json(&value);
    Some((value, hash))
}

async fn get_value_hash_lock(connection: &mut PgConnection, key: &CacheKey<'_>) -> CachedValue {
    let pair = get_value_etag_pair(connection, key).await;
    RwLock::new(pair)
}

async fn get_eth_supply(state: StateExtension) -> impl IntoResponse {
    let eth_supply = state.cache.eth_supply_parts.read().unwrap();
    match &*eth_supply {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some((eth_supply, hash)) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("max-age=4, stale-while-revalidate=60"),
            );

            let etag = EntityTag::strong(&hash);
            headers.insert(
                header::ETAG,
                HeaderValue::from_str(&etag.to_string()).unwrap(),
            );

            (headers, Json(eth_supply).into_response()).into_response()
        }
    }
}

async fn get_total_difficulty_progress(state: StateExtension) -> impl IntoResponse {
    let difficulty_by_day = state.cache.total_difficulty_progress.read().unwrap();
    match &*difficulty_by_day {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some((difficulty_by_day, hash)) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("max-age=600, stale-while-revalidate=86400"),
            );

            let etag = EntityTag::strong(&hash);
            headers.insert(
                header::ETAG,
                HeaderValue::from_str(&etag.to_string()).unwrap(),
            );

            (headers, Json(difficulty_by_day).into_response()).into_response()
        }
    }
}

async fn get_merge_estimate(state: StateExtension) -> impl IntoResponse {
    let merge_estimate = state.cache.merge_estimate.read().unwrap();
    match &*merge_estimate {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some((merge_estimate, hash)) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("max-age=4, stale-while-revalidate=14400"),
            );

            let etag = EntityTag::strong(&hash);
            headers.insert(
                header::ETAG,
                HeaderValue::from_str(&etag.to_string()).unwrap(),
            );

            (headers, Json(merge_estimate).into_response()).into_response()
        }
    }
}

pub async fn start_server() {
    tracing_subscriber::fmt::init();

    let mut connection = PgConnection::connect(&config::get_db_url_with_name("eth-analysis-serve"))
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    tracing::debug!("warming up total difficulty progress cache");
    let total_difficulty_progress =
        get_value_hash_lock(&mut connection, &CacheKey::TotalDifficultyProgress).await;
    let merge_estimate = get_value_hash_lock(&mut connection, &CacheKey::MergeEstimate).await;
    let eth_supply_parts = get_value_hash_lock(&mut connection, &CacheKey::EthSupplyParts).await;
    let cache = Arc::new(Cache {
        total_difficulty_progress,
        eth_supply_parts,
        merge_estimate,
    });

    tracing::debug!("cache warming done");

    let shared_state = Arc::new(State { cache });

    tracing::debug!("setting up listening for cache updates");
    let mut listener = sqlx::postgres::PgListener::connect(&config::get_db_url())
        .await
        .unwrap();
    listener.listen("cache-update").await.unwrap();
    let mut notification_stream = listener.into_stream();

    let shared_state_cache_update_clone = shared_state.clone();
    tokio::spawn(async move {
        while let Some(notification) = notification_stream.try_next().await.unwrap() {
            let payload = notification.payload();
            let payload_cache_key = CacheKey::from(payload);
            match payload_cache_key {
                CacheKey::TotalDifficultyProgress => {
                    tracing::debug!("total difficulty progress cache update");
                    let pair =
                        get_value_etag_pair(&mut connection, &CacheKey::TotalDifficultyProgress)
                            .await;

                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .total_difficulty_progress
                        .write()
                        .unwrap();

                    *cache_wlock = pair;
                }
                CacheKey::MergeEstimate => {
                    tracing::debug!("merge estimate cache update");
                    let pair = get_value_etag_pair(&mut connection, &CacheKey::MergeEstimate).await;

                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .merge_estimate
                        .write()
                        .unwrap();

                    *cache_wlock = pair;
                }
                CacheKey::EthSupplyParts => {
                    tracing::debug!("eth supply cache update");
                    let pair =
                        get_value_etag_pair(&mut connection, &CacheKey::EthSupplyParts).await;

                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .eth_supply_parts
                        .write()
                        .unwrap();

                    *cache_wlock = pair;
                }
                key => {
                    tracing::debug!("received unsupported cache key: {key:?}, skipping");
                }
            }
        }
    });

    let app = Router::new()
        .route("/api/v2/fees/eth-supply", get(get_eth_supply))
        .route("/api/v2/fees/merge-estimate", get(get_merge_estimate))
        .route(
            "/api/v2/fees/total-difficulty-progress",
            get(get_total_difficulty_progress),
        )
        .route("/healthz", get(|| async { StatusCode::OK }))
        .layer(Extension(shared_state));

    let port = config::get_env_var("PORT").unwrap_or("3002".to_string());

    tracing::info!("listening on {port}");
    axum::Server::bind(&format!("0.0.0.0:{port}").parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
