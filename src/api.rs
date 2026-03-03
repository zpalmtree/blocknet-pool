use axum::body::Body;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::Serialize;

use crate::db::PoolFeeEvent;
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::stats::PoolStats;
use crate::store::PoolStore;
use crate::validation::ValidationEngine;

const DB_TOTALS_CACHE_TTL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<PoolStore>,
    pub stats: Arc<PoolStats>,
    pub jobs: Arc<JobManager>,
    pub validation: Arc<ValidationEngine>,
    pub db_totals_cache: Arc<Mutex<DbTotalsCache>>,
    pub api_key: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct DbTotals {
    total_shares: u64,
    total_blocks: u64,
    pool_fees_collected: u64,
}

#[derive(Debug, Default)]
pub struct DbTotalsCache {
    updated_at: Option<Instant>,
    totals: DbTotals,
}

pub async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    let app_state = state.clone();
    let app = Router::new()
        .route("/api/stats", get(handle_stats))
        .route("/api/miners", get(handle_miners))
        .route("/api/miner/:address", get(handle_miner))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts", get(handle_payouts))
        .route("/api/fees", get(handle_fees))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            require_api_key,
        ))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "api listening");
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
    pool_fees_collected: u64,
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

#[derive(Serialize)]
struct FeesResponse {
    total_collected: u64,
    recent: Vec<PoolFeeEvent>,
}

async fn handle_stats(State(state): State<ApiState>) -> impl IntoResponse {
    let snap = state.stats.snapshot();
    let validation = state.validation.snapshot();
    let totals = match state.db_totals().await {
        Ok(v) => v,
        Err(err) => return internal_error("failed loading pool stats", err).into_response(),
    };
    let current_job_height = state.jobs.current_job().map(|j| j.height);

    let response = StatsResponse {
        pool: PoolSummary {
            miners: snap.connected_miners,
            workers: snap.connected_workers,
            hashrate: snap.estimated_hashrate,
            shares_accepted: snap.total_shares_accepted,
            shares_rejected: snap.total_shares_rejected,
            blocks_found: snap.total_blocks_found,
            total_shares: totals.total_shares,
            total_blocks: totals.total_blocks,
            pool_fees_collected: totals.pool_fees_collected,
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

    Json(response).into_response()
}

async fn handle_miners(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.stats.all_miner_stats())
}

async fn handle_miner(
    Path(address): Path<String>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let stats = state.stats.get_miner_stats(&address);
    let store = Arc::clone(&state.store);
    let address_for_query = address.clone();
    let shares = match tokio::task::spawn_blocking(move || {
        store.get_shares_for_miner(&address_for_query, 100)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading miner shares", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading miner shares",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };

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
    let store = Arc::clone(&state.store);
    let blocks = match tokio::task::spawn_blocking(move || store.get_recent_blocks(100)).await {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading blocks", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading blocks",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    Json(blocks).into_response()
}

async fn handle_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let payouts = match tokio::task::spawn_blocking(move || store.get_recent_payouts(100)).await {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading payouts", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading payouts",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    Json(payouts).into_response()
}

async fn handle_fees(State(state): State<ApiState>) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let (total_collected, recent) =
        match tokio::task::spawn_blocking(move || -> anyhow::Result<(u64, Vec<PoolFeeEvent>)> {
            Ok((
                store.get_total_pool_fees()?,
                store.get_recent_pool_fees(100)?,
            ))
        })
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => return internal_error("failed loading fees", err).into_response(),
            Err(err) => {
                return internal_error("failed loading fees", anyhow::anyhow!("join error: {err}"))
                    .into_response()
            }
        };

    Json(FeesResponse {
        total_collected,
        recent,
    })
    .into_response()
}

impl ApiState {
    async fn db_totals(&self) -> anyhow::Result<DbTotals> {
        {
            let cache = self.db_totals_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < DB_TOTALS_CACHE_TTL)
            {
                return Ok(cache.totals);
            }
        }

        let store = Arc::clone(&self.store);
        let totals = tokio::task::spawn_blocking(move || -> anyhow::Result<DbTotals> {
            Ok(DbTotals {
                total_shares: store.get_total_share_count()?,
                total_blocks: store.get_block_count()?,
                pool_fees_collected: store.get_total_pool_fees()?,
            })
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        let mut cache = self.db_totals_cache.lock();
        cache.totals = totals;
        cache.updated_at = Some(Instant::now());
        Ok(totals)
    }
}

async fn require_api_key(
    State(state): State<ApiState>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let expected = state.api_key.trim();
    if expected.is_empty() {
        return next.run(req).await.into_response();
    }

    let api_key = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let bearer = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty());

    if api_key == Some(expected) || bearer == Some(expected) {
        return next.run(req).await.into_response();
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"error":"unauthorized"})),
    )
        .into_response()
}

fn internal_error(msg: &str, err: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    tracing::warn!(error = %err, "{msg}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": msg})),
    )
}
