use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;
use tracing::info;

use eth_analysis::{db, execution_chain::ExecutionNode};

#[derive(Serialize)]
struct HealthReport {
    db_ok: bool,
    beacon_node_ok: bool,
    last_header_synced_at: Option<DateTime<Utc>>,
    last_header_sync_age_secs: Option<i64>,
    sync_ok: bool,
}

async fn get_health_status(
    db_pool: &PgPool,
    execution_node: &ExecutionNode,
    last_synced: SystemTime,
) -> (StatusCode, axum::Json<HealthReport>) {
    let db_ok = sqlx::query("SELECT 1").execute(db_pool).await.is_ok();
    let beacon_node_ok = execution_node.get_latest_block().await.is_ok();

    let now = SystemTime::now();
    let duration_since_last_sync = now.duration_since(last_synced).unwrap_or_default();
    let sync_ok = duration_since_last_sync < std::time::Duration::from_secs(60);

    let last_header_synced_at = DateTime::<Utc>::from(last_synced);
    let last_header_sync_age_secs = Utc::now()
        .signed_duration_since(last_header_synced_at)
        .num_seconds();

    let report = HealthReport {
        db_ok,
        beacon_node_ok,
        last_header_synced_at: Some(last_header_synced_at),
        last_header_sync_age_secs: Some(last_header_sync_age_secs),
        sync_ok,
    };

    let status_code = if db_ok && beacon_node_ok && sync_ok {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (status_code, axum::Json(report))
}

struct HealthCheckState {
    db_pool: PgPool,
    execution_node: ExecutionNode,
    last_synced: Arc<Mutex<SystemTime>>,
}

async fn health_check_handler(
    axum::extract::State(state): axum::extract::State<Arc<HealthCheckState>>,
) -> impl IntoResponse {
    let last_synced = *state.last_synced.lock().unwrap();
    get_health_status(&state.db_pool, &state.execution_node, last_synced).await
}

async fn serve_health_check(
    db_pool: PgPool,
    execution_node: ExecutionNode,
    last_synced: Arc<Mutex<SystemTime>>,
) {
    let shared_state = Arc::new(HealthCheckState {
        db_pool,
        execution_node,
        last_synced,
    });

    let app = Router::new()
        .route("/healthz", get(health_check_handler))
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    info!("serving health check for sync-execution-blocks on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    eth_analysis::log::init();

    let db_pool = db::get_db_pool("sync-execution-blocks", 6).await;
    sqlx::migrate!().run(&db_pool).await.unwrap();

    let execution_node = ExecutionNode::connect().await;
    let last_synced = Arc::new(Mutex::new(SystemTime::now()));

    let health_db_pool = db_pool.clone();
    let health_execution_node = execution_node.clone();
    let health_last_synced = last_synced.clone();

    tokio::spawn(serve_health_check(
        health_db_pool,
        health_execution_node,
        health_last_synced,
    ));

    eth_analysis::sync_execution_blocks(&db_pool, &execution_node, &last_synced)
        .await
        .unwrap();
}
