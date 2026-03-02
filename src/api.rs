use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::db::SqliteStore;
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::stats::PoolStats;
use crate::validation::ValidationEngine;

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<SqliteStore>,
    pub stats: Arc<PoolStats>,
    pub jobs: Arc<JobManager>,
    pub validation: Arc<ValidationEngine>,
}

pub async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/api/stats", get(handle_stats))
        .route("/api/miners", get(handle_miners))
        .route("/api/miner/:address", get(handle_miner))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts", get(handle_payouts))
        .with_state(state);

    tracing::info!(addr = %addr, "api listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Serialize)]
struct StatsResponse {
    pool: PoolSummary,
    chain: ChainSummary,
    validation: ValidationSummary,
}

#[derive(Serialize)]
struct PoolSummary {
    miners: usize,
    workers: usize,
    hashrate: f64,
    shares_accepted: u64,
    shares_rejected: u64,
    blocks_found: u64,
    total_shares: u64,
    total_blocks: u64,
}

#[derive(Serialize)]
struct ChainSummary {
    current_job_height: Option<u64>,
}

#[derive(Serialize)]
struct ValidationSummary {
    in_flight: i64,
    candidate_queue_depth: usize,
    regular_queue_depth: usize,
    tracked_addresses: usize,
    forced_verify_addresses: usize,
    total_shares: u64,
    sampled_shares: u64,
    invalid_samples: u64,
    pending_provisional: u64,
    fraud_detections: u64,
}

async fn handle_stats(State(state): State<ApiState>) -> impl IntoResponse {
    let snap = state.stats.snapshot();
    let validation = state.validation.snapshot();

    let total_shares = state.store.get_total_share_count().unwrap_or(0);
    let total_blocks = state.store.get_block_count().unwrap_or(0);
    let current_job_height = state.jobs.current_job().map(|j| j.height);

    let response = StatsResponse {
        pool: PoolSummary {
            miners: snap.connected_miners,
            workers: snap.connected_workers,
            hashrate: snap.estimated_hashrate,
            shares_accepted: snap.total_shares_accepted,
            shares_rejected: snap.total_shares_rejected,
            blocks_found: snap.total_blocks_found,
            total_shares,
            total_blocks,
        },
        chain: ChainSummary { current_job_height },
        validation: ValidationSummary {
            in_flight: validation.in_flight,
            candidate_queue_depth: validation.candidate_queue_depth,
            regular_queue_depth: validation.regular_queue_depth,
            tracked_addresses: validation.tracked_addresses,
            forced_verify_addresses: validation.forced_verify_addresses,
            total_shares: validation.total_shares,
            sampled_shares: validation.sampled_shares,
            invalid_samples: validation.invalid_samples,
            pending_provisional: validation.pending_provisional,
            fraud_detections: validation.fraud_detections,
        },
    };

    Json(response)
}

async fn handle_miners(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.stats.all_miner_stats())
}

async fn handle_miner(
    Path(address): Path<String>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let stats = state.stats.get_miner_stats(&address);
    let shares = state
        .store
        .get_shares_for_miner(&address, 100)
        .unwrap_or_default();

    match stats {
        Some(miner_stats) => Json(serde_json::json!({
            "stats": miner_stats,
            "shares": shares,
            "hashrate": state.stats.estimate_miner_hashrate(&address),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"miner not found"})),
        )
            .into_response(),
    }
}

async fn handle_blocks(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.store.get_recent_blocks(100).unwrap_or_default())
}

async fn handle_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.store.get_recent_payouts(100).unwrap_or_default())
}
