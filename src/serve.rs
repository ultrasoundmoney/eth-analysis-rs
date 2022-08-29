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

use crate::config;
use crate::eth_supply_parts::ETH_SUPPLY_PARTS_CACHE_KEY;
use crate::execution_chain::MERGE_ESTIMATE_CACHE_KEY;
use crate::execution_chain::TOTAL_DIFFICULTY_PROGRESS_CACHE_KEY;
use crate::key_value_store;

type StateExtension = Extension<Arc<State>>;

#[derive(Debug)]
struct Cache {
    difficulty_by_day: RwLock<Option<Value>>,
    eth_supply: RwLock<Option<Value>>,
    merge_estimate: RwLock<Option<Value>>,
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

async fn get_eth_supply(state: StateExtension) -> impl IntoResponse {
    let eth_supply = state.cache.eth_supply.read().unwrap();
    match &*eth_supply {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(eth_supply) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("max-age=4, stale-while-revalidate=60"),
            );

            let hash = hash_from_json(&eth_supply);
            let etag = EntityTag::strong(&hash);
            headers.insert(header::ETAG, HeaderValue::from_str(etag.tag()).unwrap());

            (headers, Json(eth_supply).into_response()).into_response()
        }
    }
}

async fn get_total_difficulty_progress(state: StateExtension) -> impl IntoResponse {
    let difficulty_by_day = state.cache.difficulty_by_day.read().unwrap();
    match &*difficulty_by_day {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(difficulty_by_day) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("max-age=600, stale-while-revalidate=86400"),
            );

            let hash = hash_from_json(&difficulty_by_day);
            let etag = EntityTag::strong(&hash);
            headers.insert(header::ETAG, HeaderValue::from_str(etag.tag()).unwrap());

            (headers, Json(difficulty_by_day).into_response()).into_response()
        }
    }
}

async fn get_merge_estimate(state: StateExtension) -> impl IntoResponse {
    let merge_estimate = state.cache.merge_estimate.read().unwrap();
    match &*merge_estimate {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(merge_estimate) => {
            let mut headers = HeaderMap::new();

            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("max-age=4, stale-while-revalidate=14400"),
            );

            let hash = hash_from_json(&merge_estimate);
            let etag = EntityTag::strong(&hash);
            headers.insert(header::ETAG, HeaderValue::from_str(etag.tag()).unwrap());

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
    let difficulty_by_day = {
        let difficulty_by_day =
            key_value_store::get_value(&mut connection, TOTAL_DIFFICULTY_PROGRESS_CACHE_KEY).await;
        RwLock::new(difficulty_by_day)
    };
    let merge_estimate = {
        let merge_estimate =
            key_value_store::get_value(&mut connection, MERGE_ESTIMATE_CACHE_KEY).await;
        RwLock::new(merge_estimate)
    };
    let eth_supply = {
        let eth_supply =
            key_value_store::get_value(&mut connection, ETH_SUPPLY_PARTS_CACHE_KEY).await;
        RwLock::new(eth_supply)
    };
    let cache = Arc::new(Cache {
        difficulty_by_day,
        eth_supply,
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
            match payload {
                TOTAL_DIFFICULTY_PROGRESS_CACHE_KEY => {
                    tracing::debug!("total difficulty progress cache update");
                    let difficulty_by_day = key_value_store::get_value(
                        &mut connection,
                        TOTAL_DIFFICULTY_PROGRESS_CACHE_KEY,
                    )
                    .await;

                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .difficulty_by_day
                        .write()
                        .unwrap();
                    *cache_wlock = difficulty_by_day;
                }
                MERGE_ESTIMATE_CACHE_KEY => {
                    tracing::debug!("merge estimate cache update");
                    let merge_estimate =
                        key_value_store::get_value(&mut connection, MERGE_ESTIMATE_CACHE_KEY).await;

                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .merge_estimate
                        .write()
                        .unwrap();
                    *cache_wlock = merge_estimate;
                }
                ETH_SUPPLY_PARTS_CACHE_KEY => {
                    tracing::debug!("eth supply cache update");
                    let eth_supply =
                        key_value_store::get_value(&mut connection, MERGE_ESTIMATE_CACHE_KEY).await;

                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .eth_supply
                        .write()
                        .unwrap();
                    *cache_wlock = eth_supply;
                }
                _ => (),
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
