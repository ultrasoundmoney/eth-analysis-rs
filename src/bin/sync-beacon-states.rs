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

use eth_analysis::{
    beacon_chain::{BeaconNode, BeaconNodeHttp},
    db,
};

#[derive(Serialize)]
struct HealthReport {
    db_ok: bool,
    beacon_node_ok: bool,
    slot_sync_ok: bool,
    last_slot_synced_at: Option<DateTime<Utc>>,
    last_slot_sync_age_secs: Option<i64>,
}

async fn get_health_status(
    db_pool: &PgPool,
    beacon_node: &BeaconNodeHttp,
    last_synced: SystemTime,
) -> (StatusCode, axum::Json<HealthReport>) {
    let db_ok = sqlx::query("SELECT 1").execute(db_pool).await.is_ok();
    let beacon_node_ok = beacon_node.get_last_header().await.is_ok();

    let now = SystemTime::now();
    let duration_since_last_sync = now.duration_since(last_synced).unwrap_or_default();
    let slot_sync_ok = duration_since_last_sync < std::time::Duration::from_secs(60);

    let last_slot_synced_at = DateTime::<Utc>::from(last_synced);
    let last_slot_sync_age_secs = Utc::now()
        .signed_duration_since(last_slot_synced_at)
        .num_seconds();

    let report = HealthReport {
        db_ok,
        beacon_node_ok,
        slot_sync_ok,
        last_slot_synced_at: Some(last_slot_synced_at),
        last_slot_sync_age_secs: Some(last_slot_sync_age_secs),
    };

    let status_code = if db_ok && beacon_node_ok && slot_sync_ok {
        StatusCode::OK
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (status_code, axum::Json(report))
}

struct HealthCheckState {
    db_pool: PgPool,
    beacon_node: BeaconNodeHttp,
    last_synced: Arc<Mutex<SystemTime>>,
}

async fn health_check_handler(
    axum::extract::State(state): axum::extract::State<Arc<HealthCheckState>>,
) -> impl IntoResponse {
    let last_synced = *state.last_synced.lock().unwrap();
    get_health_status(&state.db_pool, &state.beacon_node, last_synced).await
}

async fn serve_health_check(
    db_pool: PgPool,
    beacon_node: BeaconNodeHttp,
    last_synced: Arc<Mutex<SystemTime>>,
) {
    let shared_state = Arc::new(HealthCheckState {
        db_pool,
        beacon_node,
        last_synced,
    });

    let app = Router::new()
        .route("/healthz", get(health_check_handler))
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("serving health check for sync-beacon-states on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[tokio::main]
pub async fn main() {
    eth_analysis::log::init();

    let db_pool = db::get_db_pool("sync-beacon-states", 5).await;
    sqlx::migrate!().run(&db_pool).await.unwrap();

    let beacon_node = BeaconNodeHttp::new_from_env();
    let last_synced = Arc::new(Mutex::new(SystemTime::now()));

    let health_db_pool = db_pool.clone();
    let health_beacon_node = beacon_node.clone();
    let health_last_synced = last_synced.clone();

    tokio::spawn(serve_health_check(
        health_db_pool,
        health_beacon_node,
        health_last_synced,
    ));

    eth_analysis::sync_beacon_states(&db_pool, &beacon_node, &last_synced)
        .await
        .unwrap();
}
