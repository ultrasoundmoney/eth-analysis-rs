use axum::response::IntoResponse;
use axum::routing::get;
use axum::Extension;
use axum::Json;
use axum::Router;
use futures::TryStreamExt;
use reqwest::StatusCode;

use serde_json::Value;
use sqlx::Connection;
use sqlx::PgConnection;
use std::sync::Arc;
use std::sync::RwLock;

use crate::config;
use crate::execution_chain::DIFFICULTY_PROGRESS_CACHE_KEY;
use crate::key_value_store;

type StateExtension = Extension<Arc<State>>;

#[derive(Debug)]
struct Cache {
    difficulty_by_day: RwLock<Option<Value>>,
}

pub struct State {
    cache: Arc<Cache>,
}

async fn get_total_difficulty_progress(state: StateExtension) -> impl IntoResponse {
    let difficulty_by_day = state.cache.difficulty_by_day.read().unwrap();
    match &*difficulty_by_day {
        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some(difficulty_by_day) => Json(difficulty_by_day).into_response(),
    }
}

pub async fn start_server() {
    tracing_subscriber::fmt::init();

    let mut connection = PgConnection::connect(&config::get_db_url_with_name("eth-analysis-serve"))
        .await
        .unwrap();

    sqlx::migrate!().run(&mut connection).await.unwrap();

    tracing::debug!("warming up total difficulty progress cache");
    let difficulty_by_day = RwLock::new(
        key_value_store::get_value(&mut connection, DIFFICULTY_PROGRESS_CACHE_KEY).await,
    );
    let cache = Arc::new(Cache { difficulty_by_day });

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
                DIFFICULTY_PROGRESS_CACHE_KEY => {
                    tracing::debug!("total difficulty progress cache update");
                    let next_difficulty_by_day =
                        key_value_store::get_value(&mut connection, DIFFICULTY_PROGRESS_CACHE_KEY)
                            .await;
                    let mut cache_wlock = shared_state_cache_update_clone
                        .cache
                        .difficulty_by_day
                        .write()
                        .unwrap();
                    *cache_wlock = next_difficulty_by_day;
                }
                _ => (),
            }
        }
    });

    let app = Router::new()
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
