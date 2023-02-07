use axum::{
    http::{HeaderMap, HeaderValue},
    response::IntoResponse,
    Extension, Json,
};
use enum_iterator::all;
use futures::{Stream, TryStreamExt};
use reqwest::{header, StatusCode};
use serde_json::Value;
use sqlx::PgPool;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use crate::{
    caching::{self, CacheKey, ParseCacheKeyError},
    db,
};

use super::{State, StateExtension};

#[derive(Debug)]
pub struct Cache(RwLock<HashMap<CacheKey, Value>>);

impl Cache {
    pub async fn new(db_pool: &PgPool) -> Self {
        let map = RwLock::new(HashMap::new());

        // Tries to fetch a value from the key value store for every cached analysis value.
        for key in all::<CacheKey>().collect::<Vec<_>>() {
            let value = caching::get_serialized_caching_value(db_pool, &key)
                .await
                .unwrap();
            if let Some(value) = value {
                map.write().unwrap().insert(key, value);
            }
        }

        Self(map)
    }
}

async fn cached_get_with_cache_duration(
    Extension(state): StateExtension,
    analysis_cache_key: &CacheKey,
    max_age: Option<u32>,
    // s_max_age: Option<u32>,
    stale_while_revalidate: Option<u32>,
) -> impl IntoResponse {
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

    match state.cache.0.read().unwrap().get(analysis_cache_key) {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(cached_value) => (headers, Json(cached_value).into_response()).into_response(),
    }
}

pub async fn cached_get(state: StateExtension, analysis_cache_key: &CacheKey) -> impl IntoResponse {
    cached_get_with_cache_duration(state, analysis_cache_key, None, None).await
}

async fn process_notifications(
    mut notification_stream: impl Stream<Item = Result<sqlx::postgres::PgNotification, sqlx::Error>>
        + Unpin,
    state: Arc<State>,
    db_pool: &PgPool,
) {
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
                debug!(%cache_key, "cache update");
                let value = caching::get_serialized_caching_value(db_pool, &cache_key)
                    .await
                    .unwrap();
                if let Some(value) = value {
                    state.cache.0.write().unwrap().insert(cache_key, value);
                } else {
                    warn!(
                        %cache_key,
                        "got a message to update our served cache, but DB had no value to give"
                    );
                }
            }
        }
    }
}

pub async fn update_cache_from_notifications(
    state: Arc<State>,
    db_pool: &PgPool,
) -> JoinHandle<()> {
    let mut listener =
        sqlx::postgres::PgListener::connect(&db::get_db_url_with_name("serve-rs-cache-update"))
            .await
            .unwrap();
    listener.listen("cache-update").await.unwrap();
    debug!("listening for cache updates");

    let notification_stream = listener.into_stream();
    let db_pool_cloned = db_pool.clone();

    tokio::spawn(async move {
        process_notifications(notification_stream, state, &db_pool_cloned).await;
    })
}
