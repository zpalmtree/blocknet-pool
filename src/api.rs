use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use crate::db::{DbBlock, Payout, PoolFeeEvent};
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::node::NodeClient;
use crate::stats::{PoolStats, RejectionAnalyticsSnapshot};
use crate::store::PoolStore;
use crate::validation::ValidationEngine;

const DB_TOTALS_CACHE_TTL: Duration = Duration::from_secs(2);
const DAEMON_HEALTH_CACHE_TTL: Duration = Duration::from_secs(5);
const EXPLORER_HASHRATE_SAMPLE_COUNT: usize = 10;
const NETWORK_HASHRATE_CACHE_RETRY_TTL: Duration = Duration::from_secs(5);
const DEFAULT_PAGE_LIMIT: usize = 25;
const MAX_PAGE_LIMIT: usize = 200;
const HASHRATE_WINDOW: Duration = Duration::from_secs(60 * 60);
const HASHRATE_WARMUP_WINDOW: Duration = Duration::from_secs(5 * 60);
const INITIAL_REWARD: u64 = 72_325_093_035;
const TAIL_EMISSION: u64 = 200_000_000;
const MONTHS_TO_TAIL: u64 = 48;
const DECAY_RATE: f64 = 0.75;
const BLOCK_INTERVAL_SECS: u64 = 5 * 60;
const BLOCKS_PER_MONTH: u64 = (30 * 24 * 60 * 60) / BLOCK_INTERVAL_SECS;
const ROUND_TARGET_SECONDS: f64 = 300.0;
const INSIGHTS_CACHE_TTL: Duration = Duration::from_secs(10);
const STATUS_SAMPLES_RETENTION: Duration = Duration::from_secs(8 * 24 * 60 * 60);
const STATUS_MAX_INCIDENTS: usize = 256;

fn db_miner_hashrate(store: &PoolStore, address: &str) -> f64 {
    let since = SystemTime::now()
        .checked_sub(HASHRATE_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_for_miner(address, since)
    else {
        return 0.0;
    };
    hashrate_from_stats_with_warmup(
        total_diff,
        count,
        oldest,
        newest,
        HASHRATE_WINDOW,
        HASHRATE_WARMUP_WINDOW,
    )
}

fn db_pool_hashrate(store: &PoolStore) -> f64 {
    let since = SystemTime::now()
        .checked_sub(HASHRATE_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_pool(since) else {
        return 0.0;
    };
    hashrate_from_stats_with_warmup(
        total_diff,
        count,
        oldest,
        newest,
        HASHRATE_WINDOW,
        HASHRATE_WARMUP_WINDOW,
    )
}

fn estimated_block_reward(height: u64) -> u64 {
    let month = height / BLOCKS_PER_MONTH.max(1);
    if month >= MONTHS_TO_TAIL {
        return TAIL_EMISSION;
    }
    let years = month as f64 / 12.0;
    let decay = (-DECAY_RATE * years).exp();
    let reward =
        (INITIAL_REWARD.saturating_sub(TAIL_EMISSION)) as f64 * decay + TAIL_EMISSION as f64;
    if reward < TAIL_EMISSION as f64 {
        TAIL_EMISSION
    } else {
        reward as u64
    }
}

fn hydrate_provisional_block_reward(block: &mut DbBlock) {
    if !block.confirmed && !block.orphaned && block.reward == 0 {
        block.reward = estimated_block_reward(block.height);
    }
}

fn hashrate_from_stats(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
) -> f64 {
    if count < 2 {
        return 0.0;
    }
    let (Some(oldest), Some(newest)) = (oldest, newest) else {
        return 0.0;
    };
    let Ok(window) = newest.duration_since(oldest) else {
        return 0.0;
    };
    if window.as_secs_f64() < 1.0 {
        return 0.0;
    }
    total_diff as f64 / window.as_secs_f64()
}

fn hashrate_from_stats_or_window_floor(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
    floor_window: Duration,
) -> f64 {
    let from_stats = hashrate_from_stats(total_diff, count, oldest, newest);
    if from_stats > 0.0 {
        return from_stats;
    }
    if total_diff == 0 {
        return 0.0;
    }
    let secs = floor_window.as_secs_f64().max(1.0);
    total_diff as f64 / secs
}

fn hashrate_from_stats_with_warmup(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
    smoothing_window: Duration,
    warmup_window: Duration,
) -> f64 {
    if total_diff == 0 {
        return 0.0;
    }

    let smoothing_secs = smoothing_window.as_secs_f64().max(1.0);
    let warmup_secs = warmup_window.as_secs_f64().clamp(1.0, smoothing_secs);
    let observed_secs = if count < 2 {
        0.0
    } else {
        let (Some(oldest), Some(newest)) = (oldest, newest) else {
            return total_diff as f64 / warmup_secs;
        };
        let Ok(window) = newest.duration_since(oldest) else {
            return total_diff as f64 / warmup_secs;
        };
        window.as_secs_f64()
    };

    let denominator = if observed_secs >= 1.0 {
        observed_secs.clamp(warmup_secs, smoothing_secs)
    } else {
        warmup_secs
    };
    total_diff as f64 / denominator
}

fn worker_hashrate_by_name(
    miner_hashrate: f64,
    worker_hashrate_raw: Vec<(String, u64, u64, Option<SystemTime>, Option<SystemTime>)>,
) -> HashMap<String, f64> {
    let total_worker_diff_window: u64 = worker_hashrate_raw
        .iter()
        .map(|(_, total_diff, _, _, _)| *total_diff)
        .sum();
    let can_scale_to_miner_hashrate = miner_hashrate > 0.0 && total_worker_diff_window > 0;
    worker_hashrate_raw
        .into_iter()
        .map(|(worker, total_diff, accepted_count, oldest, newest)| {
            let hr = if can_scale_to_miner_hashrate {
                miner_hashrate * (total_diff as f64 / total_worker_diff_window as f64)
            } else {
                hashrate_from_stats_or_window_floor(
                    total_diff,
                    accepted_count,
                    oldest,
                    newest,
                    HASHRATE_WINDOW,
                )
            };
            (worker, hr)
        })
        .collect()
}

fn sort_workers_for_miner(
    mut workers: Vec<(String, u64, u64, u64, i64)>,
    hashrate_by_name: &HashMap<String, f64>,
    now: SystemTime,
    active_cutoff: Duration,
) -> Vec<(String, u64, u64, u64, i64)> {
    let now_unix = i64::try_from(system_time_to_unix_secs(now)).unwrap_or(i64::MAX);
    let cutoff_secs = i64::try_from(active_cutoff.as_secs()).unwrap_or(i64::MAX);
    let active_cutoff_unix = now_unix.saturating_sub(cutoff_secs);

    workers.sort_by(|a, b| {
        let a_active = a.4 >= active_cutoff_unix;
        let b_active = b.4 >= active_cutoff_unix;
        if a_active != b_active {
            return b_active.cmp(&a_active);
        }

        let a_hashrate = hashrate_by_name.get(&a.0).copied().unwrap_or(0.0);
        let b_hashrate = hashrate_by_name.get(&b.0).copied().unwrap_or(0.0);
        let hr_desc = b_hashrate
            .partial_cmp(&a_hashrate)
            .unwrap_or(std::cmp::Ordering::Equal);

        if a_active {
            if hr_desc != std::cmp::Ordering::Equal {
                return hr_desc;
            }
            let last_share_desc = b.4.cmp(&a.4);
            if last_share_desc != std::cmp::Ordering::Equal {
                return last_share_desc;
            }
        } else {
            let last_share_desc = b.4.cmp(&a.4);
            if last_share_desc != std::cmp::Ordering::Equal {
                return last_share_desc;
            }
            if hr_desc != std::cmp::Ordering::Equal {
                return hr_desc;
            }
        }

        a.0.cmp(&b.0)
    });

    workers
}

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
    pub insights_cache: Arc<Mutex<InsightsCache>>,
    pub status_history: Arc<Mutex<StatusHistory>>,
    pub api_key: String,
    pub pool_name: String,
    pub pool_url: String,
    pub stratum_port: u16,
    pub pool_fee_pct: f64,
    pub pool_fee_flat: f64,
    pub min_payout_amount: f64,
    pub blocks_before_payout: i32,
    pub payout_scheme: String,
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

#[derive(Debug, Clone, Default)]
pub struct InsightsCache {
    updated_at: Option<Instant>,
    value: Option<StatsInsightsResponse>,
}

#[derive(Debug, Clone)]
struct StatusSample {
    timestamp: SystemTime,
    daemon_reachable: bool,
}

#[derive(Debug, Clone)]
struct OpenIncident {
    id: u64,
    kind: &'static str,
    severity: &'static str,
    started_at: SystemTime,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusIncident {
    id: u64,
    kind: String,
    severity: String,
    started_at: SystemTime,
    ended_at: Option<SystemTime>,
    duration_seconds: Option<u64>,
    message: String,
    ongoing: bool,
}

#[derive(Debug, Clone, Default)]
pub struct StatusHistory {
    samples: VecDeque<StatusSample>,
    incidents: VecDeque<StatusIncident>,
    open_daemon_down: Option<OpenIncident>,
    open_daemon_syncing: Option<OpenIncident>,
    next_incident_id: u64,
}

#[derive(Debug, Clone, Serialize)]
struct UptimeWindow {
    label: String,
    window_seconds: u64,
    sample_count: usize,
    up_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct StatusPageResponse {
    checked_at: SystemTime,
    pool_uptime_seconds: u64,
    daemon: DaemonHealth,
    uptime: Vec<UptimeWindow>,
    incidents: Vec<StatusIncident>,
}

#[derive(Debug, Clone, Serialize)]
struct EffortBand {
    label: &'static str,
    tone: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct RoundProgressResponse {
    round_start: Option<SystemTime>,
    elapsed_seconds: u64,
    round_work: u64,
    expected_work: Option<u64>,
    effort_pct: Option<f64>,
    expected_block_seconds: Option<f64>,
    timer_effort_pct: Option<f64>,
    effort_band: EffortBand,
    timer_band: EffortBand,
    target_block_seconds: f64,
}

#[derive(Debug, Clone, Serialize)]
struct PayoutEtaResponse {
    last_payout_at: Option<SystemTime>,
    estimated_next_payout_at: Option<SystemTime>,
    eta_seconds: Option<u64>,
    typical_interval_seconds: Option<u64>,
    pending_count: usize,
    pending_total_amount: u64,
}

#[derive(Debug, Clone, Serialize)]
struct LuckRoundResponse {
    block_height: u64,
    block_hash: String,
    timestamp: SystemTime,
    difficulty: u64,
    round_work: u64,
    effort_pct: f64,
    duration_seconds: u64,
    timer_effort_pct: f64,
    effort_band: EffortBand,
    orphaned: bool,
    confirmed: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RejectionAnalyticsResponse {
    window: RejectionAnalyticsSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct StatsInsightsResponse {
    round: RoundProgressResponse,
    payout_eta: PayoutEtaResponse,
    luck_history: Vec<LuckRoundResponse>,
    rejections: RejectionAnalyticsResponse,
}

pub async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    let app_state = state.clone();
    let protected = Router::new()
        .route("/api/miners", get(handle_miners))
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
        .route("/ui-assets/app.js", get(handle_ui_asset_app_js))
        .route("/ui-assets/app.css", get(handle_ui_asset_app_css))
        .route(
            "/ui-assets/pool-entered.png",
            get(handle_ui_asset_pool_entered),
        )
        .route("/ui-assets/mining-tui.png", get(handle_ui_asset_mining_tui))
        .route("/api/info", get(handle_info))
        .route("/api/stats", get(handle_stats))
        .route("/api/stats/history", get(handle_stats_history))
        .route("/api/stats/insights", get(handle_stats_insights))
        .route("/api/status", get(handle_status))
        .route("/api/events", get(handle_events))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts/recent", get(handle_public_payouts))
        .route("/api/miner/:address", get(handle_miner))
        .route("/api/miner/:address/hashrate", get(handle_miner_hashrate))
        .merge(protected)
        .fallback(get(handle_ui))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

const UI_INDEX_HTML: &str = include_str!("ui/dist/index.html");
const UI_ASSET_APP_JS: &str = include_str!("ui/dist/app.js");
const UI_ASSET_APP_CSS: &str = include_str!("ui/dist/app.css");
const UI_ASSET_POOL_ENTERED_PNG: &[u8] = include_bytes!("ui/assets/pool-entered.png");
const UI_ASSET_MINING_TUI_PNG: &[u8] = include_bytes!("ui/assets/mining-tui.png");

async fn handle_ui() -> Html<&'static str> {
    Html(UI_INDEX_HTML)
}

async fn handle_ui_asset_app_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        UI_ASSET_APP_JS,
    )
}

async fn handle_ui_asset_app_css() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        UI_ASSET_APP_CSS,
    )
}

async fn handle_ui_asset_pool_entered() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        UI_ASSET_POOL_ENTERED_PNG,
    )
}

async fn handle_ui_asset_mining_tui() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        UI_ASSET_MINING_TUI_PNG,
    )
}

#[derive(Serialize)]
struct PoolInfoResponse {
    pool_name: String,
    pool_url: String,
    stratum_port: u16,
    api_auth_configured: bool,
    started_at_unix_secs: u64,
    version: &'static str,
    pool_fee_pct: f64,
    pool_fee_flat: f64,
    min_payout_amount: f64,
    blocks_before_payout: i32,
    payout_scheme: String,
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
        pool_fee_pct: state.pool_fee_pct,
        pool_fee_flat: state.pool_fee_flat,
        min_payout_amount: state.min_payout_amount,
        blocks_before_payout: state.blocks_before_payout,
        payout_scheme: state.payout_scheme.clone(),
        public_endpoints: vec![
            "/api/info",
            "/api/stats",
            "/api/stats/history",
            "/api/stats/insights",
            "/api/status",
            "/api/events",
            "/api/blocks",
            "/api/payouts/recent",
            "/api/miner/{address}",
        ],
        protected_endpoints: vec!["/api/miners", "/api/payouts", "/api/fees", "/api/health"],
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
struct StatsHistoryQuery {
    range: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MinerDetailQuery {
    share_limit: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
struct MinerHashrateQuery {
    range: Option<String>,
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

    let store = Arc::clone(&state.store);
    let pool_hashrate = tokio::task::spawn_blocking(move || db_pool_hashrate(&store))
        .await
        .unwrap_or(0.0);

    let response = StatsResponse {
        pool: PoolSummary {
            miners: snap.connected_miners,
            workers: snap.connected_workers,
            hashrate: pool_hashrate,
            shares_accepted: snap.total_shares_accepted,
            shares_rejected: snap.total_shares_rejected,
            blocks_found: totals.total_blocks,
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

async fn handle_stats_history(
    Query(query): Query<StatsHistoryQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let range_secs: u64 = match query.range.as_deref().unwrap_or("24h") {
        "1h" => 3600,
        "7d" => 7 * 86400,
        "30d" => 30 * 86400,
        _ => 86400, // default 24h
    };
    let since = SystemTime::now()
        .checked_sub(Duration::from_secs(range_secs))
        .unwrap_or(UNIX_EPOCH);

    let store = Arc::clone(&state.store);
    match tokio::task::spawn_blocking(move || store.get_stat_snapshots(since)).await {
        Ok(Ok(snapshots)) => Json(snapshots).into_response(),
        Ok(Err(err)) => internal_error("failed loading stat history", err).into_response(),
        Err(err) => internal_error(
            "failed loading stat history",
            anyhow::anyhow!("join error: {err}"),
        )
        .into_response(),
    }
}

async fn handle_stats_insights(State(state): State<ApiState>) -> impl IntoResponse {
    match state.stats_insights().await {
        Ok(v) => Json(v).into_response(),
        Err(err) => internal_error("failed loading stats insights", err).into_response(),
    }
}

async fn handle_status(State(state): State<ApiState>) -> impl IntoResponse {
    let daemon = state.daemon_health().await;
    Json(state.build_status_response(daemon)).into_response()
}

async fn handle_events(
    State(state): State<ApiState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream =
        IntervalStream::new(tokio::time::interval(Duration::from_secs(5))).then(move |_| {
            let state = state.clone();
            async move {
                let snap = state.stats.snapshot();
                let daemon = state.daemon_health().await;
                let payload = serde_json::json!({
                    "ts": system_time_to_unix_secs(SystemTime::now()),
                    "pool": {
                        "miners": snap.connected_miners,
                        "workers": snap.connected_workers,
                        "hashrate": snap.estimated_hashrate,
                        "accepted": snap.total_shares_accepted,
                        "rejected": snap.total_shares_rejected,
                        "blocks_found": snap.total_blocks_found,
                    },
                    "daemon": {
                        "reachable": daemon.reachable,
                        "syncing": daemon.syncing,
                        "chain_height": daemon.chain_height,
                    }
                });
                Ok::<Event, Infallible>(Event::default().event("tick").data(payload.to_string()))
            }
        });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
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

    let all_stats = state.stats.all_miner_stats();
    let fallback_hashrates = state.stats.estimate_all_miner_hashrates();
    let addresses = all_stats.keys().cloned().collect::<Vec<_>>();
    let store = Arc::clone(&state.store);
    let hashrates = match tokio::task::spawn_blocking(move || {
        let mut map = HashMap::with_capacity(addresses.len());
        for address in addresses {
            map.insert(address.clone(), db_miner_hashrate(&store, &address));
        }
        Ok::<_, anyhow::Error>(map)
    })
    .await
    {
        Ok(Ok(map)) => map,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "failed loading miner hashrates from db; using in-memory estimates");
            fallback_hashrates
        }
        Err(err) => {
            tracing::warn!(error = %err, "failed joining miner hashrate db task; using in-memory estimates");
            fallback_hashrates
        }
    };
    let mut items = all_stats
        .into_values()
        .map(|stats| {
            let mut workers = stats.workers.iter().cloned().collect::<Vec<String>>();
            workers.sort();
            let hashrate = hashrates.get(&stats.address).copied().unwrap_or(0.0);
            MinerListItem {
                address: stats.address.clone(),
                worker_count: workers.len(),
                workers,
                shares_accepted: stats.shares_accepted,
                shares_rejected: stats.shares_rejected,
                blocks_found: stats.blocks_found,
                hashrate,
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
    let addr = address.clone();
    let share_limit = share_limit(query.share_limit);

    let db_result = match tokio::task::spawn_blocking(move || {
        let shares = store.get_shares_for_miner(&addr, share_limit)?;
        let balance = store.get_balance(&addr)?;
        let pending_payout = store.get_pending_payout(&addr)?;
        let hr = db_miner_hashrate(&store, &addr);
        let since_hr_window = SystemTime::now()
            .checked_sub(HASHRATE_WINDOW)
            .unwrap_or(UNIX_EPOCH);
        let since_24h = SystemTime::now()
            .checked_sub(Duration::from_secs(86400))
            .unwrap_or(UNIX_EPOCH);
        let workers_raw = store.worker_stats_for_miner(&addr, since_24h)?;
        let worker_hashrate_raw = store.worker_hashrate_stats_for_miner(&addr, since_hr_window)?;
        let miner_blocks = store.get_blocks_for_miner(&addr)?;
        Ok::<_, anyhow::Error>((
            shares,
            balance,
            pending_payout,
            hr,
            workers_raw,
            worker_hashrate_raw,
            miner_blocks,
        ))
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading miner data", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading miner data",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    let (
        shares,
        balance,
        pending_payout,
        hashrate,
        workers_raw,
        worker_hashrate_raw,
        mut miner_blocks,
    ) = db_result;
    for block in &mut miner_blocks {
        hydrate_provisional_block_reward(block);
    }

    let balance_json = serde_json::json!({
        "pending": balance.pending,
        "paid": balance.paid,
    });

    let worker_hashrate_by_name = worker_hashrate_by_name(hashrate, worker_hashrate_raw);
    let workers_sorted = sort_workers_for_miner(
        workers_raw,
        &worker_hashrate_by_name,
        SystemTime::now(),
        HASHRATE_WINDOW,
    );

    let workers_json: Vec<serde_json::Value> = workers_sorted
        .iter()
        .map(|(worker, accepted, rejected, _total_diff, last_share_ts)| {
            let worker_hr = worker_hashrate_by_name.get(worker).copied().unwrap_or(0.0);
            serde_json::json!({
                "worker": worker,
                "hashrate": worker_hr,
                "accepted": accepted,
                "rejected": rejected,
                "last_share_at": last_share_ts,
            })
        })
        .collect();

    let total_accepted: u64 = workers_sorted.iter().map(|(_, a, _, _, _)| a).sum();
    let total_rejected: u64 = workers_sorted.iter().map(|(_, _, r, _, _)| r).sum();

    let has_activity = miner_has_activity(
        shares.len(),
        balance.pending,
        balance.paid,
        pending_payout.is_some(),
    );

    let base = serde_json::json!({
        "shares": shares,
        "hashrate": hashrate,
        "balance": balance_json,
        "pending_payout": pending_payout,
        "workers": workers_json,
        "blocks_found": miner_blocks,
        "total_accepted": total_accepted,
        "total_rejected": total_rejected,
    });

    match stats {
        Some(miner_stats) => {
            let mut obj = base;
            obj["stats"] = serde_json::to_value(&miner_stats).unwrap_or_default();
            Json(obj).into_response()
        }
        None if has_activity => {
            let mut obj = base;
            obj["stats"] = serde_json::Value::Null;
            Json(obj).into_response()
        }
        None => {
            let mut obj = base;
            obj["error"] = serde_json::Value::String("miner not found".to_string());
            (StatusCode::NOT_FOUND, Json(obj)).into_response()
        }
    }
}

async fn handle_miner_hashrate(
    Path(address): Path<String>,
    Query(query): Query<MinerHashrateQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let (range_secs, bucket_secs): (u64, i64) = match query.range.as_deref().unwrap_or("24h") {
        "1h" => (3600, 120),
        "7d" => (7 * 86400, 3600),
        "30d" => (30 * 86400, 14400),
        _ => (86400, 600), // default 24h
    };
    let now = SystemTime::now();
    let since = now
        .checked_sub(Duration::from_secs(range_secs))
        .unwrap_or(UNIX_EPOCH);

    let store = Arc::clone(&state.store);
    match tokio::task::spawn_blocking(move || {
        store.hashrate_history_for_miner(&address, since, bucket_secs)
    })
    .await
    {
        Ok(Ok(buckets)) => {
            // Zero-fill missing buckets to avoid visual inflation from sparse submissions,
            // then smooth with EWMA so the curve is less jumpy.
            let step = bucket_secs.max(1);
            let mut by_bucket = HashMap::<i64, f64>::with_capacity(buckets.len());
            for (ts, total_diff, _count) in buckets {
                let hr = total_diff as f64 / step as f64;
                by_bucket.insert(ts, hr);
            }

            let since_unix = match since.duration_since(UNIX_EPOCH) {
                Ok(v) => i64::try_from(v.as_secs()).unwrap_or(i64::MAX),
                Err(_) => 0,
            };
            let now_unix = match now.duration_since(UNIX_EPOCH) {
                Ok(v) => i64::try_from(v.as_secs()).unwrap_or(i64::MAX),
                Err(_) => 0,
            };
            let start_bucket = since_unix.div_euclid(step) * step;
            let end_bucket = now_unix.div_euclid(step) * step;

            let mut points = Vec::<serde_json::Value>::new();
            if end_bucket >= start_bucket {
                let alpha = 0.35_f64;
                let mut smoothed_prev = 0.0_f64;
                let mut first = true;
                let mut ts = start_bucket;
                while ts <= end_bucket {
                    let raw = by_bucket.get(&ts).copied().unwrap_or(0.0);
                    let smoothed = if first {
                        first = false;
                        raw
                    } else {
                        alpha * raw + (1.0 - alpha) * smoothed_prev
                    };
                    smoothed_prev = smoothed;
                    points.push(serde_json::json!({"timestamp": ts, "hashrate": smoothed}));
                    ts = ts.saturating_add(step);
                }
            }

            Json(points).into_response()
        }
        Ok(Err(err)) => {
            internal_error("failed loading miner hashrate history", err).into_response()
        }
        Err(err) => internal_error(
            "failed loading miner hashrate history",
            anyhow::anyhow!("join error: {err}"),
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
        let mut blocks = match tokio::task::spawn_blocking(move || store.get_recent_blocks(100))
            .await
        {
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
        for block in &mut blocks {
            hydrate_provisional_block_reward(block);
        }
        return Json(blocks).into_response();
    }

    let store = Arc::clone(&state.store);
    let mut blocks = match tokio::task::spawn_blocking(move || store.get_all_blocks()).await {
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
    for block in &mut blocks {
        hydrate_provisional_block_reward(block);
    }

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

#[derive(Debug, Clone, Serialize)]
struct PublicPayout {
    total_amount: u64,
    total_fee: u64,
    recipient_count: usize,
    tx_hashes: Vec<String>,
    timestamp: SystemTime,
}

/// Group payouts into batches by timestamp proximity (5 min window).
fn batch_payouts(payouts: &[Payout]) -> Vec<PublicPayout> {
    if payouts.is_empty() {
        return Vec::new();
    }
    let batch_window = Duration::from_secs(5 * 60);
    let mut batches: Vec<PublicPayout> = Vec::new();
    for p in payouts {
        let merged = batches.last_mut().and_then(|b| {
            let diff = b
                .timestamp
                .duration_since(p.timestamp)
                .or_else(|_| p.timestamp.duration_since(b.timestamp))
                .unwrap_or(Duration::ZERO);
            if diff <= batch_window {
                Some(b)
            } else {
                None
            }
        });
        if let Some(batch) = merged {
            batch.total_amount += p.amount;
            batch.total_fee += p.fee;
            batch.recipient_count += 1;
            if !p.tx_hash.is_empty() {
                batch.tx_hashes.push(p.tx_hash.clone());
            }
        } else {
            batches.push(PublicPayout {
                total_amount: p.amount,
                total_fee: p.fee,
                recipient_count: 1,
                tx_hashes: if p.tx_hash.is_empty() {
                    Vec::new()
                } else {
                    vec![p.tx_hash.clone()]
                },
                timestamp: p.timestamp,
            });
        }
    }
    batches
}

fn compute_payout_eta(store: &PoolStore) -> anyhow::Result<PayoutEtaResponse> {
    let mut payouts = store.get_recent_payouts(300)?;
    payouts.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    let batches = batch_payouts(&payouts);
    let last_payout_at = batches.first().map(|b| b.timestamp);

    let mut interval_samples = batches
        .windows(2)
        .filter_map(|pair| pair[0].timestamp.duration_since(pair[1].timestamp).ok())
        .map(|d| d.as_secs())
        .filter(|secs| *secs > 0)
        .collect::<Vec<_>>();
    interval_samples.sort_unstable();
    let typical_interval_seconds = if interval_samples.is_empty() {
        None
    } else {
        Some(interval_samples[interval_samples.len() / 2])
    };

    let estimated_next_payout_at = match (last_payout_at, typical_interval_seconds) {
        (Some(last), Some(interval_secs)) => last.checked_add(Duration::from_secs(interval_secs)),
        _ => None,
    };
    let eta_seconds = estimated_next_payout_at.and_then(|eta| {
        eta.duration_since(SystemTime::now())
            .ok()
            .map(|d| d.as_secs())
    });

    let pending = store.get_pending_payouts()?;
    let pending_total_amount = pending
        .iter()
        .fold(0u64, |acc, payout| acc.saturating_add(payout.amount));

    Ok(PayoutEtaResponse {
        last_payout_at,
        estimated_next_payout_at,
        eta_seconds,
        typical_interval_seconds,
        pending_count: pending.len(),
        pending_total_amount,
    })
}

fn compute_luck_history(
    store: &PoolStore,
    mut blocks: Vec<crate::db::DbBlock>,
) -> anyhow::Result<Vec<LuckRoundResponse>> {
    if blocks.len() < 2 {
        return Ok(Vec::new());
    }

    blocks.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut rounds = Vec::<LuckRoundResponse>::new();
    for pair in blocks.windows(2) {
        let prev = &pair[0];
        let current = &pair[1];
        let shares = store.get_shares_between(prev.timestamp, current.timestamp)?;
        let round_work = shares
            .into_iter()
            .filter(|share| share.status == "verified" || share.status == "provisional")
            .fold(0u64, |acc, share| acc.saturating_add(share.difficulty));

        let duration_seconds = current
            .timestamp
            .duration_since(prev.timestamp)
            .unwrap_or_default()
            .as_secs();
        let effort_pct = if current.difficulty > 0 {
            (round_work as f64 / current.difficulty as f64) * 100.0
        } else {
            0.0
        };
        let timer_effort_pct = if ROUND_TARGET_SECONDS > 0.0 {
            (duration_seconds as f64 / ROUND_TARGET_SECONDS) * 100.0
        } else {
            0.0
        };

        rounds.push(LuckRoundResponse {
            block_height: current.height,
            block_hash: current.hash.clone(),
            timestamp: current.timestamp,
            difficulty: current.difficulty,
            round_work,
            effort_pct,
            duration_seconds,
            timer_effort_pct,
            effort_band: classify_effort(effort_pct),
            orphaned: current.orphaned,
            confirmed: current.confirmed,
        });
    }

    rounds.sort_by(|a, b| b.block_height.cmp(&a.block_height));
    rounds.truncate(16);
    Ok(rounds)
}

async fn handle_public_payouts(
    Query(query): Query<PayoutsQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let mut payouts = match tokio::task::spawn_blocking(move || store.get_all_payouts()).await {
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

    // Always sort by time desc for batching, then re-sort result if needed.
    payouts.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    let mut public = batch_payouts(&payouts);

    match query.sort.as_deref().map(str::trim).unwrap_or("time_desc") {
        "time_asc" => public.sort_by(|a, b| a.timestamp.cmp(&b.timestamp)),
        "amount_desc" => public.sort_by(|a, b| b.total_amount.cmp(&a.total_amount)),
        "amount_asc" => public.sort_by(|a, b| a.total_amount.cmp(&b.total_amount)),
        _ => {} // already time_desc
    }

    Json(paginate_response(public, query.limit, query.offset)).into_response()
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
    let mut payouts = match tokio::task::spawn_blocking(move || store.get_all_payouts()).await {
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
            Ok((store.get_total_pool_fees()?, store.get_all_pool_fees()?))
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
    pub async fn sample_status(&self) {
        let _ = self.daemon_health().await;
    }

    async fn stats_insights(&self) -> anyhow::Result<StatsInsightsResponse> {
        {
            let cache = self.insights_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < INSIGHTS_CACHE_TTL)
            {
                if let Some(value) = cache.value.clone() {
                    return Ok(value);
                }
            }
        }

        let current_job = self.jobs.current_job();
        let current_difficulty = current_job
            .as_ref()
            .map(|job| job.network_difficulty.max(1));
        let network_hashrate = self.network_hashrate_for_job(current_job.as_ref()).await;

        let store = Arc::clone(&self.store);
        let now = SystemTime::now();
        let (pool_hashrate, round_start, round_work, payout_eta, luck_history) =
            tokio::task::spawn_blocking(move || {
                let pool_hashrate = db_pool_hashrate(&store);

                let blocks = store.get_recent_blocks(64)?;
                let round_start = blocks
                    .iter()
                    .max_by(|a, b| a.timestamp.cmp(&b.timestamp))
                    .map(|b| b.timestamp)
                    .or_else(|| now.checked_sub(Duration::from_secs(3600)));

                let round_work = if let Some(start) = round_start {
                    let (total_diff, _count, _oldest, _newest) =
                        store.hashrate_stats_pool(start)?;
                    total_diff
                } else {
                    0
                };

                let payout_eta = compute_payout_eta(&store)?;
                let luck_history = compute_luck_history(&store, blocks)?;

                Ok::<_, anyhow::Error>((
                    pool_hashrate,
                    round_start,
                    round_work,
                    payout_eta,
                    luck_history,
                ))
            })
            .await
            .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        let elapsed_seconds = round_start
            .and_then(|start| now.duration_since(start).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let effort_pct = current_difficulty.and_then(|expected_work| {
            if expected_work == 0 {
                None
            } else {
                Some((round_work as f64 / expected_work as f64) * 100.0)
            }
        });

        let expected_block_seconds = match (pool_hashrate, network_hashrate) {
            (pool, Some(network)) if pool > 0.0 && network > 0.0 => {
                Some(ROUND_TARGET_SECONDS * (network / pool))
            }
            _ => None,
        };
        let timer_effort_pct = expected_block_seconds.and_then(|expected| {
            if expected <= 0.0 {
                None
            } else {
                Some((elapsed_seconds as f64 / expected) * 100.0)
            }
        });

        let rejection_window = self.stats.rejection_analytics(Duration::from_secs(3600));
        let response = StatsInsightsResponse {
            round: RoundProgressResponse {
                round_start,
                elapsed_seconds,
                round_work,
                expected_work: current_difficulty,
                effort_pct,
                expected_block_seconds,
                timer_effort_pct,
                effort_band: classify_effort(effort_pct.unwrap_or(0.0)),
                timer_band: classify_effort(timer_effort_pct.unwrap_or(0.0)),
                target_block_seconds: ROUND_TARGET_SECONDS,
            },
            payout_eta,
            luck_history,
            rejections: RejectionAnalyticsResponse {
                window: rejection_window,
            },
        };

        let mut cache = self.insights_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.value = Some(response.clone());
        Ok(response)
    }

    fn build_status_response(&self, daemon: DaemonHealth) -> StatusPageResponse {
        let now = SystemTime::now();
        let pool_uptime_seconds = self.started_at.elapsed().as_secs();
        let history = self.status_history.lock();

        let uptime = vec![
            UptimeWindow {
                label: "1h".to_string(),
                window_seconds: 3600,
                sample_count: history.sample_count_within(Duration::from_secs(3600), now),
                up_pct: history.uptime_pct(Duration::from_secs(3600), now),
            },
            UptimeWindow {
                label: "24h".to_string(),
                window_seconds: 24 * 3600,
                sample_count: history.sample_count_within(Duration::from_secs(24 * 3600), now),
                up_pct: history.uptime_pct(Duration::from_secs(24 * 3600), now),
            },
            UptimeWindow {
                label: "7d".to_string(),
                window_seconds: 7 * 24 * 3600,
                sample_count: history.sample_count_within(Duration::from_secs(7 * 24 * 3600), now),
                up_pct: history.uptime_pct(Duration::from_secs(7 * 24 * 3600), now),
            },
        ];

        StatusPageResponse {
            checked_at: now,
            pool_uptime_seconds,
            daemon,
            uptime,
            incidents: history.incidents_for_api(now),
        }
    }

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
        self.status_history.lock().record_sample(
            SystemTime::now(),
            value.reachable,
            value.syncing,
            value.error.as_deref(),
        );
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

fn classify_effort(value_pct: f64) -> EffortBand {
    if value_pct >= 200.0 {
        EffortBand {
            label: "very overdue",
            tone: "critical",
        }
    } else if value_pct >= 100.0 {
        EffortBand {
            label: "overdue",
            tone: "warn",
        }
    } else {
        EffortBand {
            label: "on pace",
            tone: "ok",
        }
    }
}

impl StatusHistory {
    fn record_sample(
        &mut self,
        now: SystemTime,
        daemon_reachable: bool,
        daemon_syncing: Option<bool>,
        daemon_error: Option<&str>,
    ) {
        self.samples.push_back(StatusSample {
            timestamp: now,
            daemon_reachable,
        });
        let cutoff = now
            .checked_sub(STATUS_SAMPLES_RETENTION)
            .unwrap_or(UNIX_EPOCH);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.timestamp < cutoff)
        {
            self.samples.pop_front();
        }

        if !daemon_reachable {
            if self.open_daemon_down.is_none() {
                let id = self.next_incident_id;
                self.next_incident_id = self.next_incident_id.saturating_add(1);
                self.open_daemon_down = Some(OpenIncident {
                    id,
                    kind: "daemon_down",
                    severity: "critical",
                    started_at: now,
                    message: daemon_error.unwrap_or("daemon unreachable").to_string(),
                });
            }
        } else if let Some(open) = self.open_daemon_down.take() {
            self.incidents.push_front(StatusIncident {
                id: open.id,
                kind: open.kind.to_string(),
                severity: open.severity.to_string(),
                started_at: open.started_at,
                ended_at: Some(now),
                duration_seconds: now
                    .duration_since(open.started_at)
                    .ok()
                    .map(|d| d.as_secs()),
                message: open.message,
                ongoing: false,
            });
        }

        let syncing = daemon_reachable && daemon_syncing.unwrap_or(false);
        if syncing {
            if self.open_daemon_syncing.is_none() {
                let id = self.next_incident_id;
                self.next_incident_id = self.next_incident_id.saturating_add(1);
                self.open_daemon_syncing = Some(OpenIncident {
                    id,
                    kind: "daemon_syncing",
                    severity: "warn",
                    started_at: now,
                    message: "daemon reported syncing".to_string(),
                });
            }
        } else if let Some(open) = self.open_daemon_syncing.take() {
            self.incidents.push_front(StatusIncident {
                id: open.id,
                kind: open.kind.to_string(),
                severity: open.severity.to_string(),
                started_at: open.started_at,
                ended_at: Some(now),
                duration_seconds: now
                    .duration_since(open.started_at)
                    .ok()
                    .map(|d| d.as_secs()),
                message: open.message,
                ongoing: false,
            });
        }

        while self.incidents.len() > STATUS_MAX_INCIDENTS {
            self.incidents.pop_back();
        }
    }

    fn incidents_for_api(&self, now: SystemTime) -> Vec<StatusIncident> {
        let mut out = self.incidents.iter().cloned().collect::<Vec<_>>();
        if let Some(open) = self.open_daemon_down.clone() {
            out.insert(
                0,
                StatusIncident {
                    id: open.id,
                    kind: open.kind.to_string(),
                    severity: open.severity.to_string(),
                    started_at: open.started_at,
                    ended_at: None,
                    duration_seconds: now
                        .duration_since(open.started_at)
                        .ok()
                        .map(|d| d.as_secs()),
                    message: open.message,
                    ongoing: true,
                },
            );
        }
        if let Some(open) = self.open_daemon_syncing.clone() {
            out.insert(
                0,
                StatusIncident {
                    id: open.id,
                    kind: open.kind.to_string(),
                    severity: open.severity.to_string(),
                    started_at: open.started_at,
                    ended_at: None,
                    duration_seconds: now
                        .duration_since(open.started_at)
                        .ok()
                        .map(|d| d.as_secs()),
                    message: open.message,
                    ongoing: true,
                },
            );
        }
        out
    }

    fn uptime_pct(&self, window: Duration, now: SystemTime) -> Option<f64> {
        let cutoff = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
        let mut total = 0usize;
        let mut up = 0usize;
        for sample in self
            .samples
            .iter()
            .filter(|sample| sample.timestamp >= cutoff)
        {
            total += 1;
            if sample.daemon_reachable {
                up += 1;
            }
        }
        if total == 0 {
            None
        } else {
            Some((up as f64 / total as f64) * 100.0)
        }
    }

    fn sample_count_within(&self, window: Duration, now: SystemTime) -> usize {
        let cutoff = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
        self.samples
            .iter()
            .filter(|sample| sample.timestamp >= cutoff)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use crate::db::DbBlock;

    use super::{
        contains_ci, estimated_block_reward, hashrate_from_stats_with_warmup,
        hydrate_provisional_block_reward, miner_has_activity, page_bounds, share_limit,
        sort_workers_for_miner, worker_hashrate_by_name, HASHRATE_WARMUP_WINDOW, HASHRATE_WINDOW,
    };

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

    #[test]
    fn worker_hashrate_scales_to_miner_hashrate() {
        let now = SystemTime::now();
        let map = worker_hashrate_by_name(
            2.59,
            vec![
                (
                    "w1".to_string(),
                    240,
                    5,
                    Some(now),
                    Some(now + Duration::from_secs(30)),
                ),
                (
                    "w2".to_string(),
                    120,
                    4,
                    Some(now),
                    Some(now + Duration::from_secs(30)),
                ),
            ],
        );
        let w1 = map.get("w1").copied().unwrap_or_default();
        let w2 = map.get("w2").copied().unwrap_or_default();
        let total = w1 + w2;
        assert!((total - 2.59).abs() < 1e-9);
        assert!(w1 > w2);
    }

    #[test]
    fn worker_hashrate_falls_back_when_miner_hashrate_unavailable() {
        let t0 = UNIX_EPOCH + Duration::from_secs(10);
        let t1 = t0 + Duration::from_secs(10);
        let map =
            worker_hashrate_by_name(0.0, vec![("w1".to_string(), 200, 2, Some(t0), Some(t1))]);
        let w1 = map.get("w1").copied().unwrap_or_default();
        assert!((w1 - 20.0).abs() < 1e-9);
    }

    #[test]
    fn hashrate_warmup_uses_bootstrap_window_before_two_samples() {
        let hr = hashrate_from_stats_with_warmup(
            600,
            1,
            None,
            None,
            HASHRATE_WINDOW,
            HASHRATE_WARMUP_WINDOW,
        );
        assert!((hr - 2.0).abs() < 1e-9);
    }

    #[test]
    fn hashrate_warmup_clamps_short_observed_windows() {
        let t0 = UNIX_EPOCH + Duration::from_secs(10);
        let t1 = t0 + Duration::from_secs(60);
        let hr = hashrate_from_stats_with_warmup(
            600,
            2,
            Some(t0),
            Some(t1),
            HASHRATE_WINDOW,
            HASHRATE_WARMUP_WINDOW,
        );
        assert!((hr - 2.0).abs() < 1e-9);
    }

    #[test]
    fn hashrate_warmup_uses_observed_window_after_bootstrap() {
        let t0 = UNIX_EPOCH + Duration::from_secs(10);
        let t1 = t0 + Duration::from_secs(900);
        let hr = hashrate_from_stats_with_warmup(
            1800,
            8,
            Some(t0),
            Some(t1),
            HASHRATE_WINDOW,
            HASHRATE_WARMUP_WINDOW,
        );
        assert!((hr - 2.0).abs() < 1e-9);
    }

    #[test]
    fn sort_workers_prioritizes_active_then_hashrate_then_recency() {
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let mut hashrate_by_name = HashMap::new();
        hashrate_by_name.insert("active-high".to_string(), 9.0);
        hashrate_by_name.insert("active-low".to_string(), 1.0);
        hashrate_by_name.insert("stale-recent".to_string(), 100.0);
        hashrate_by_name.insert("stale-old".to_string(), 200.0);

        let workers = vec![
            ("stale-old".to_string(), 1, 0, 0, 200),
            ("active-low".to_string(), 1, 0, 0, 9_990),
            ("stale-recent".to_string(), 1, 0, 0, 6_350),
            ("active-high".to_string(), 1, 0, 0, 9_950),
        ];

        let sorted = sort_workers_for_miner(workers, &hashrate_by_name, now, HASHRATE_WINDOW);
        let names: Vec<String> = sorted.into_iter().map(|(name, _, _, _, _)| name).collect();

        assert_eq!(
            names,
            vec!["active-high", "active-low", "stale-recent", "stale-old"]
        );
    }

    #[test]
    fn sort_workers_uses_last_share_as_tie_breaker_for_active_hashrate() {
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let mut hashrate_by_name = HashMap::new();
        hashrate_by_name.insert("active-older".to_string(), 5.0);
        hashrate_by_name.insert("active-newer".to_string(), 5.0);

        let workers = vec![
            ("active-older".to_string(), 1, 0, 0, 9_800),
            ("active-newer".to_string(), 1, 0, 0, 9_990),
        ];

        let sorted = sort_workers_for_miner(workers, &hashrate_by_name, now, HASHRATE_WINDOW);
        let names: Vec<String> = sorted.into_iter().map(|(name, _, _, _, _)| name).collect();

        assert_eq!(names, vec!["active-newer", "active-older"]);
    }

    #[test]
    fn hydrate_provisional_reward_fills_pending_zero_reward() {
        let mut block = DbBlock {
            height: 3707,
            hash: "abc".to_string(),
            difficulty: 1,
            finder: "addr".to_string(),
            finder_worker: "rig".to_string(),
            reward: 0,
            timestamp: SystemTime::now(),
            confirmed: false,
            orphaned: false,
            paid_out: false,
        };
        hydrate_provisional_block_reward(&mut block);
        assert_eq!(block.reward, estimated_block_reward(3707));
    }

    #[test]
    fn hydrate_provisional_reward_does_not_change_confirmed_blocks() {
        let mut block = DbBlock {
            height: 3707,
            hash: "abc".to_string(),
            difficulty: 1,
            finder: "addr".to_string(),
            finder_worker: "rig".to_string(),
            reward: 123,
            timestamp: SystemTime::now(),
            confirmed: true,
            orphaned: false,
            paid_out: false,
        };
        hydrate_provisional_block_reward(&mut block);
        assert_eq!(block.reward, 123);
    }
}
