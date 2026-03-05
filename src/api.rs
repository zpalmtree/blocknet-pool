use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::db::{Payout, PoolFeeEvent};
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::node::NodeClient;
use crate::stats::PoolStats;
use crate::store::PoolStore;
use crate::validation::ValidationEngine;

const DB_TOTALS_CACHE_TTL: Duration = Duration::from_secs(2);
const DAEMON_HEALTH_CACHE_TTL: Duration = Duration::from_secs(5);
const EXPLORER_HASHRATE_SAMPLE_COUNT: usize = 10;
const NETWORK_HASHRATE_CACHE_RETRY_TTL: Duration = Duration::from_secs(5);
const LIST_SCAN_LIMIT: i64 = 5_000;
const DEFAULT_PAGE_LIMIT: usize = 25;
const MAX_PAGE_LIMIT: usize = 200;

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<PoolStore>,
    pub stats: Arc<PoolStats>,
    pub jobs: Arc<JobManager>,
    pub node: Arc<NodeClient>,
    pub validation: Arc<ValidationEngine>,
    pub db_totals_cache: Arc<Mutex<DbTotalsCache>>,
    pub daemon_health_cache: Arc<Mutex<DaemonHealthCache>>,
    pub network_hashrate_cache: Arc<Mutex<NetworkHashrateCache>>,
    pub api_key: String,
    pub pool_name: String,
    pub pool_url: String,
    pub stratum_port: u16,
    pub started_at: Instant,
    pub started_at_system: SystemTime,
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct DaemonHealth {
    reachable: bool,
    chain_height: Option<u64>,
    peers: Option<i64>,
    syncing: Option<bool>,
    mempool_size: Option<i64>,
    best_hash: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Default)]
pub struct DaemonHealthCache {
    updated_at: Option<Instant>,
    value: Option<DaemonHealth>,
}

#[derive(Debug, Default)]
pub struct NetworkHashrateCache {
    updated_at: Option<Instant>,
    chain_height: Option<u64>,
    difficulty: Option<u64>,
    hashrate_hps: Option<f64>,
}

pub async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    let app_state = state.clone();
    let protected = Router::new()
        .route("/api/miners", get(handle_miners))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts", get(handle_payouts))
        .route("/api/fees", get(handle_fees))
        .route("/api/health", get(handle_health))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            require_api_key,
        ));

    let app = Router::new()
        .route("/", get(handle_ui))
        .route("/ui", get(handle_ui))
        .route("/api/info", get(handle_info))
        .route("/api/stats", get(handle_stats))
        .route("/api/miner/:address", get(handle_miner))
        .merge(protected)
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

const UI_INDEX_HTML: &str = include_str!("ui/index.html");

async fn handle_ui() -> Html<&'static str> {
    Html(UI_INDEX_HTML)
}

#[derive(Serialize)]
struct PoolInfoResponse {
    pool_name: String,
    pool_url: String,
    stratum_port: u16,
    api_auth_configured: bool,
    started_at_unix_secs: u64,
    version: &'static str,
    public_endpoints: Vec<&'static str>,
    protected_endpoints: Vec<&'static str>,
}

async fn handle_info(State(state): State<ApiState>) -> impl IntoResponse {
    Json(PoolInfoResponse {
        pool_name: state.pool_name.clone(),
        pool_url: state.pool_url.clone(),
        stratum_port: state.stratum_port,
        api_auth_configured: !state.api_key.trim().is_empty(),
        started_at_unix_secs: system_time_to_unix_secs(state.started_at_system),
        version: env!("CARGO_PKG_VERSION"),
        public_endpoints: vec!["/api/info", "/api/stats", "/api/miner/{address}"],
        protected_endpoints: vec![
            "/api/miners",
            "/api/blocks",
            "/api/payouts",
            "/api/fees",
            "/api/health",
        ],
    })
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
    network_hashrate: Option<f64>,
    daemon_chain_height: u64,
    daemon_syncing: bool,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Serialize)]
struct HealthResponse {
    uptime_seconds: u64,
    api_key_configured: bool,
    daemon: DaemonHealth,
    job: JobHealth,
    payouts: PayoutHealth,
    validation: ValidationSummary,
}

#[derive(Serialize)]
struct JobHealth {
    current_height: Option<u64>,
    current_difficulty: Option<u64>,
    template_id: Option<String>,
    template_age_seconds: Option<u64>,
    last_refresh_millis: Option<u64>,
    tracked_templates: usize,
    active_assignments: usize,
}

#[derive(Serialize)]
struct PayoutHealth {
    pending_count: usize,
    last_payout: Option<Payout>,
}

#[derive(Debug, Deserialize, Default)]
struct MinerDetailQuery {
    share_limit: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
struct MinersQuery {
    paged: Option<bool>,
    limit: Option<usize>,
    offset: Option<usize>,
    search: Option<String>,
    sort: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct BlocksQuery {
    paged: Option<bool>,
    limit: Option<usize>,
    offset: Option<usize>,
    finder: Option<String>,
    status: Option<String>,
    sort: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct PayoutsQuery {
    paged: Option<bool>,
    limit: Option<usize>,
    offset: Option<usize>,
    address: Option<String>,
    tx_hash: Option<String>,
    sort: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct FeesQuery {
    paged: Option<bool>,
    limit: Option<usize>,
    offset: Option<usize>,
    fee_address: Option<String>,
    sort: Option<String>,
}

#[derive(Debug, Serialize)]
struct PageMeta {
    limit: usize,
    offset: usize,
    returned: usize,
    total: usize,
}

#[derive(Debug, Serialize)]
struct PagedResponse<T> {
    items: Vec<T>,
    page: PageMeta,
}

#[derive(Debug, Serialize)]
struct MinerListItem {
    address: String,
    worker_count: usize,
    workers: Vec<String>,
    shares_accepted: u64,
    shares_rejected: u64,
    blocks_found: u64,
    hashrate: f64,
    last_share_at: Option<SystemTime>,
}

async fn handle_stats(State(state): State<ApiState>) -> impl IntoResponse {
    let snap = state.stats.snapshot();
    let validation = state.validation.snapshot();
    let totals = match state.db_totals().await {
        Ok(v) => v,
        Err(err) => return internal_error("failed loading pool stats", err).into_response(),
    };
    let current_job = state.jobs.current_job();
    let current_job_height = current_job.as_ref().map(|j| j.height);
    let network_hashrate = state.network_hashrate_for_job(current_job.as_ref()).await;

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
        chain: ChainSummary {
            current_job_height,
            network_hashrate,
            daemon_chain_height: state.node.chain_height(),
            daemon_syncing: state.node.syncing(),
        },
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

async fn handle_health(State(state): State<ApiState>) -> impl IntoResponse {
    let daemon = state.daemon_health().await;
    let validation = state.validation.snapshot();

    let current_job = state.jobs.current_job();
    let template_age_seconds = state.jobs.current_job_age().map(|age| age.as_secs());
    let last_refresh_millis = state
        .jobs
        .last_refresh_elapsed()
        .map(|age| age.as_millis() as u64);

    let store = Arc::clone(&state.store);
    let payout_health =
        match tokio::task::spawn_blocking(move || -> anyhow::Result<PayoutHealth> {
            let pending_count = store.get_pending_payouts()?.len();
            let last_payout = store.get_recent_payouts(1)?.into_iter().next();
            Ok(PayoutHealth {
                pending_count,
                last_payout,
            })
        })
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => {
                return internal_error("failed loading payout health", err).into_response()
            }
            Err(err) => {
                return internal_error(
                    "failed loading payout health",
                    anyhow::anyhow!("join error: {err}"),
                )
                .into_response()
            }
        };

    let response = HealthResponse {
        uptime_seconds: state.started_at.elapsed().as_secs(),
        api_key_configured: !state.api_key.trim().is_empty(),
        daemon,
        job: JobHealth {
            current_height: current_job.as_ref().map(|job| job.height),
            current_difficulty: current_job.as_ref().map(|job| job.network_difficulty),
            template_id: current_job.and_then(|job| job.template_id),
            template_age_seconds,
            last_refresh_millis,
            tracked_templates: state.jobs.tracked_job_count(),
            active_assignments: state.jobs.active_assignment_count(),
        },
        payouts: payout_health,
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

async fn handle_miners(
    Query(query): Query<MinersQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    if !query.paged.unwrap_or(false) {
        return Json(state.stats.all_miner_stats()).into_response();
    }

    let mut items = state
        .stats
        .all_miner_stats()
        .into_values()
        .map(|stats| {
            let mut workers = stats.workers.iter().cloned().collect::<Vec<String>>();
            workers.sort();
            MinerListItem {
                address: stats.address.clone(),
                worker_count: workers.len(),
                workers,
                shares_accepted: stats.shares_accepted,
                shares_rejected: stats.shares_rejected,
                blocks_found: stats.blocks_found,
                hashrate: state.stats.estimate_miner_hashrate(&stats.address),
                last_share_at: stats.last_share_at,
            }
        })
        .collect::<Vec<MinerListItem>>();

    if let Some(search) = non_empty(&query.search) {
        items.retain(|item| contains_ci(&item.address, search));
    }

    match query
        .sort
        .as_deref()
        .map(str::trim)
        .unwrap_or("hashrate_desc")
    {
        "address_asc" => items.sort_by(|a, b| a.address.cmp(&b.address)),
        "accepted_desc" => items.sort_by(|a, b| b.shares_accepted.cmp(&a.shares_accepted)),
        "rejected_desc" => items.sort_by(|a, b| b.shares_rejected.cmp(&a.shares_rejected)),
        "last_share_desc" => items.sort_by(|a, b| b.last_share_at.cmp(&a.last_share_at)),
        _ => items.sort_by(|a, b| {
            b.hashrate
                .partial_cmp(&a.hashrate)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    }

    let total = items.len();
    let (limit, offset) = page_bounds(query.limit, query.offset);
    let page_items = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let returned = page_items.len();

    Json(PagedResponse {
        items: page_items,
        page: PageMeta {
            limit,
            offset,
            returned,
            total,
        },
    })
    .into_response()
}

async fn handle_miner(
    Path(address): Path<String>,
    Query(query): Query<MinerDetailQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let stats = state.stats.get_miner_stats(&address);
    let store = Arc::clone(&state.store);
    let address_for_query = address.clone();
    let share_limit = share_limit(query.share_limit);

    let (shares, balance, pending_payout) = match tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((
            store.get_shares_for_miner(&address_for_query, share_limit)?,
            store.get_balance(&address_for_query)?,
            store.get_pending_payout(&address_for_query)?,
        ))
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

    let hashrate = state.stats.estimate_miner_hashrate(&address);
    let balance_json = serde_json::json!({
        "pending": balance.pending,
        "paid": balance.paid,
    });
    let has_activity = miner_has_activity(
        shares.len(),
        balance.pending,
        balance.paid,
        pending_payout.is_some(),
    );

    match stats {
        Some(miner_stats) => Json(serde_json::json!({
            "stats": miner_stats,
            "shares": shares,
            "hashrate": hashrate,
            "balance": balance_json,
            "pending_payout": pending_payout,
        }))
        .into_response(),
        None if has_activity => Json(serde_json::json!({
            "stats": serde_json::Value::Null,
            "shares": shares,
            "hashrate": hashrate,
            "balance": balance_json,
            "pending_payout": pending_payout,
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error":"miner not found",
                "hashrate": hashrate,
                "balance": balance_json,
                "pending_payout": pending_payout,
            })),
        )
            .into_response(),
    }
}

async fn handle_blocks(
    Query(query): Query<BlocksQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);

    if query.legacy_mode() {
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
        return Json(blocks).into_response();
    }

    let store = Arc::clone(&state.store);
    let mut blocks =
        match tokio::task::spawn_blocking(move || store.get_recent_blocks(LIST_SCAN_LIMIT)).await {
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

    if let Some(finder) = non_empty(&query.finder) {
        blocks.retain(|block| contains_ci(&block.finder, finder));
    }

    if let Some(status) = non_empty(&query.status) {
        match status.to_ascii_lowercase().as_str() {
            "confirmed" => blocks.retain(|block| block.confirmed && !block.orphaned),
            "orphaned" => blocks.retain(|block| block.orphaned),
            "pending" => blocks.retain(|block| !block.confirmed && !block.orphaned),
            "paid" => blocks.retain(|block| block.paid_out),
            "unpaid" => blocks.retain(|block| !block.paid_out),
            _ => {}
        }
    }

    match query
        .sort
        .as_deref()
        .map(str::trim)
        .unwrap_or("height_desc")
    {
        "height_asc" => blocks.sort_by(|a, b| a.height.cmp(&b.height)),
        "reward_desc" => blocks.sort_by(|a, b| b.reward.cmp(&a.reward)),
        "reward_asc" => blocks.sort_by(|a, b| a.reward.cmp(&b.reward)),
        "time_asc" => blocks.sort_by(|a, b| a.timestamp.cmp(&b.timestamp)),
        _ => blocks.sort_by(|a, b| b.height.cmp(&a.height)),
    }

    Json(paginate_response(blocks, query.limit, query.offset)).into_response()
}

async fn handle_payouts(
    Query(query): Query<PayoutsQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);

    if query.legacy_mode() {
        let payouts = match tokio::task::spawn_blocking(move || store.get_recent_payouts(100)).await
        {
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
        return Json(payouts).into_response();
    }

    let store = Arc::clone(&state.store);
    let mut payouts = match tokio::task::spawn_blocking(move || {
        store.get_recent_payouts(LIST_SCAN_LIMIT)
    })
    .await
    {
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

    if let Some(address) = non_empty(&query.address) {
        payouts.retain(|payout| contains_ci(&payout.address, address));
    }

    if let Some(tx_hash) = non_empty(&query.tx_hash) {
        payouts.retain(|payout| contains_ci(&payout.tx_hash, tx_hash));
    }

    match query.sort.as_deref().map(str::trim).unwrap_or("time_desc") {
        "time_asc" => payouts.sort_by(|a, b| a.timestamp.cmp(&b.timestamp)),
        "amount_desc" => payouts.sort_by(|a, b| b.amount.cmp(&a.amount)),
        "amount_asc" => payouts.sort_by(|a, b| a.amount.cmp(&b.amount)),
        _ => payouts.sort_by(|a, b| b.timestamp.cmp(&a.timestamp)),
    }

    Json(paginate_response(payouts, query.limit, query.offset)).into_response()
}

async fn handle_fees(
    Query(query): Query<FeesQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);

    if query.legacy_mode() {
        let (total_collected, recent) = match tokio::task::spawn_blocking(
            move || -> anyhow::Result<(u64, Vec<PoolFeeEvent>)> {
                Ok((
                    store.get_total_pool_fees()?,
                    store.get_recent_pool_fees(100)?,
                ))
            },
        )
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => return internal_error("failed loading fees", err).into_response(),
            Err(err) => {
                return internal_error("failed loading fees", anyhow::anyhow!("join error: {err}"))
                    .into_response()
            }
        };

        return Json(FeesResponse {
            total_collected,
            recent,
        })
        .into_response();
    }

    let store = Arc::clone(&state.store);
    let (total_collected, mut fees) =
        match tokio::task::spawn_blocking(move || -> anyhow::Result<(u64, Vec<PoolFeeEvent>)> {
            Ok((
                store.get_total_pool_fees()?,
                store.get_recent_pool_fees(LIST_SCAN_LIMIT)?,
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

    if let Some(fee_address) = non_empty(&query.fee_address) {
        fees.retain(|event| contains_ci(&event.fee_address, fee_address));
    }

    match query.sort.as_deref().map(str::trim).unwrap_or("time_desc") {
        "time_asc" => fees.sort_by(|a, b| a.timestamp.cmp(&b.timestamp)),
        "amount_desc" => fees.sort_by(|a, b| b.amount.cmp(&a.amount)),
        "amount_asc" => fees.sort_by(|a, b| a.amount.cmp(&b.amount)),
        "height_asc" => fees.sort_by(|a, b| a.block_height.cmp(&b.block_height)),
        "height_desc" => fees.sort_by(|a, b| b.block_height.cmp(&a.block_height)),
        _ => fees.sort_by(|a, b| b.timestamp.cmp(&a.timestamp)),
    }

    Json(serde_json::json!({
        "total_collected": total_collected,
        "recent": paginate_response(fees, query.limit, query.offset),
    }))
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

    async fn daemon_health(&self) -> DaemonHealth {
        {
            let cache = self.daemon_health_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < DAEMON_HEALTH_CACHE_TTL)
            {
                if let Some(cached) = cache.value.clone() {
                    return cached;
                }
            }
        }

        let node = Arc::clone(&self.node);
        let sampled = tokio::task::spawn_blocking(move || node.get_status()).await;

        let value = match sampled {
            Ok(Ok(status)) => DaemonHealth {
                reachable: true,
                chain_height: Some(status.chain_height),
                peers: Some(status.peers),
                syncing: Some(status.syncing),
                mempool_size: Some(status.mempool_size),
                best_hash: Some(status.best_hash),
                error: None,
            },
            Ok(Err(err)) => DaemonHealth {
                reachable: false,
                chain_height: None,
                peers: None,
                syncing: None,
                mempool_size: None,
                best_hash: None,
                error: Some(err.to_string()),
            },
            Err(err) => DaemonHealth {
                reachable: false,
                chain_height: None,
                peers: None,
                syncing: None,
                mempool_size: None,
                best_hash: None,
                error: Some(format!("join error: {err}")),
            },
        };

        let mut cache = self.daemon_health_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.value = Some(value.clone());
        value
    }

    async fn network_hashrate_for_job(&self, job: Option<&crate::engine::Job>) -> Option<f64> {
        let job = job?;
        let chain_height = job.height.checked_sub(1)?;
        let difficulty = job.network_difficulty.max(1);

        {
            let cache = self.network_hashrate_cache.lock();
            let same_key =
                cache.chain_height == Some(chain_height) && cache.difficulty == Some(difficulty);
            if same_key {
                if let Some(value) = cache.hashrate_hps {
                    return Some(value);
                }
                if cache
                    .updated_at
                    .is_some_and(|updated| updated.elapsed() < NETWORK_HASHRATE_CACHE_RETRY_TTL)
                {
                    return None;
                }
            }
        }

        let node = Arc::clone(&self.node);
        let sampled = tokio::task::spawn_blocking(move || {
            estimate_explorer_network_hashrate_hps(node.as_ref(), chain_height, difficulty)
        })
        .await
        .ok()
        .and_then(Result::ok)
        .filter(|value| value.is_finite() && *value >= 0.0);

        let mut cache = self.network_hashrate_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.chain_height = Some(chain_height);
        cache.difficulty = Some(difficulty);
        cache.hashrate_hps = sampled;
        sampled
    }
}

fn estimate_explorer_network_hashrate_hps(
    node: &NodeClient,
    chain_height: u64,
    difficulty: u64,
) -> anyhow::Result<f64> {
    // Match explorer.go: hashrate = NextDifficulty / avg(last 10 positive block-time deltas).
    if chain_height < 2 {
        return Ok(0.0);
    }

    let mut total_time = 0i64;
    let mut count = 0usize;
    let mut current_ts = node
        .get_block_by_height_optional(chain_height)?
        .map(|block| block.timestamp);
    let mut height = chain_height;

    while height > 0 && count < EXPLORER_HASHRATE_SAMPLE_COUNT {
        let prev_ts = node
            .get_block_by_height_optional(height - 1)?
            .map(|block| block.timestamp);
        if let (Some(block_ts), Some(prev_block_ts)) = (current_ts, prev_ts) {
            let block_time = block_ts - prev_block_ts;
            if block_time > 0 {
                total_time += block_time;
                count += 1;
            }
        }
        current_ts = prev_ts;
        height -= 1;
    }

    if count > 0 && total_time > 0 {
        let avg_block_time = total_time as f64 / count as f64;
        return Ok(difficulty as f64 / avg_block_time);
    }

    Ok(0.0)
}

async fn require_api_key(
    State(state): State<ApiState>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let expected = state.api_key.trim();
    if expected.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"api key not configured"})),
        )
            .into_response();
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

fn contains_ci(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn non_empty(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|v| !v.is_empty())
}

fn page_bounds(limit: Option<usize>, offset: Option<usize>) -> (usize, usize) {
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
    let offset = offset.unwrap_or(0).min(1_000_000);
    (limit, offset)
}

fn share_limit(value: Option<i64>) -> i64 {
    let raw = value.unwrap_or(100);
    raw.clamp(1, 500)
}

fn paginate_response<T: Serialize + Clone>(
    all_items: Vec<T>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> PagedResponse<T> {
    let total = all_items.len();
    let (limit, offset) = page_bounds(limit, offset);
    let items = all_items
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let returned = items.len();

    PagedResponse {
        items,
        page: PageMeta {
            limit,
            offset,
            returned,
            total,
        },
    }
}

fn system_time_to_unix_secs(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl BlocksQuery {
    fn legacy_mode(&self) -> bool {
        !self.paged.unwrap_or(false)
            && self.limit.is_none()
            && self.offset.is_none()
            && non_empty(&self.finder).is_none()
            && non_empty(&self.status).is_none()
            && non_empty(&self.sort).is_none()
    }
}

impl PayoutsQuery {
    fn legacy_mode(&self) -> bool {
        !self.paged.unwrap_or(false)
            && self.limit.is_none()
            && self.offset.is_none()
            && non_empty(&self.address).is_none()
            && non_empty(&self.tx_hash).is_none()
            && non_empty(&self.sort).is_none()
    }
}

impl FeesQuery {
    fn legacy_mode(&self) -> bool {
        !self.paged.unwrap_or(false)
            && self.limit.is_none()
            && self.offset.is_none()
            && non_empty(&self.fee_address).is_none()
            && non_empty(&self.sort).is_none()
    }
}

fn miner_has_activity(
    shares_len: usize,
    balance_pending: u64,
    balance_paid: u64,
    has_pending_payout: bool,
) -> bool {
    shares_len > 0 || balance_pending > 0 || balance_paid > 0 || has_pending_payout
}

fn internal_error(msg: &str, err: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    tracing::warn!(error = %err, "{msg}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": msg})),
    )
}

#[cfg(test)]
mod tests {
    use super::{contains_ci, miner_has_activity, page_bounds, share_limit};

    #[test]
    fn miner_activity_detects_share_history() {
        assert!(miner_has_activity(1, 0, 0, false));
    }

    #[test]
    fn miner_activity_detects_balance_and_pending() {
        assert!(miner_has_activity(0, 1, 0, false));
        assert!(miner_has_activity(0, 0, 1, false));
        assert!(miner_has_activity(0, 0, 0, true));
        assert!(!miner_has_activity(0, 0, 0, false));
    }

    #[test]
    fn page_bounds_clamps_limits_and_offsets() {
        assert_eq!(page_bounds(None, None), (25, 0));
        assert_eq!(page_bounds(Some(0), Some(2)), (1, 2));
        assert_eq!(page_bounds(Some(5_000), Some(2_000_000)), (200, 1_000_000));
    }

    #[test]
    fn case_insensitive_contains_matches() {
        assert!(contains_ci("AlphaMiner", "alpha"));
        assert!(contains_ci("AlphaMiner", "MINER"));
        assert!(!contains_ci("AlphaMiner", "beta"));
    }

    #[test]
    fn share_limit_clamps() {
        assert_eq!(share_limit(None), 100);
        assert_eq!(share_limit(Some(0)), 1);
        assert_eq!(share_limit(Some(9999)), 500);
    }
}
