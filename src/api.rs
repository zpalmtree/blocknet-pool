use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{HeaderName, HeaderValue, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_stream::StreamExt;

use crate::config::Config;
use crate::db::{DbBlock, Payout, PoolFeeEvent, PublicPayoutBatch};
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::node::NodeClient;
use crate::payout::{weight_shares, PayoutTrustPolicy};
use crate::stats::{PoolStats, RejectionAnalyticsSnapshot, RejectionReasonCount};
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
const HASHRATE_BRAND_NEW_MIN_WINDOW: Duration = Duration::from_secs(60);
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
const MINER_PAYOUT_HISTORY_LIMIT: i64 = 50;
const PROPORTIONAL_WINDOW: Duration = Duration::from_secs(60 * 60);
const MAX_MINER_HASHRATE_DB_LOOKUPS: usize = 4096;
pub const DEFAULT_MAX_SSE_SUBSCRIBERS: usize = 256;
const DEFAULT_DAEMON_LOG_TAIL: usize = 200;
const MAX_DAEMON_LOG_TAIL: usize = 2000;
const DAEMON_LOG_LINE_LIMIT: usize = 8192;
const MINER_PENDING_ESTIMATE_CACHE_TTL: Duration = Duration::from_secs(10);
const MINER_PENDING_ESTIMATE_CACHE_MAX_ENTRIES: usize = 4096;

fn db_miner_hashrate(store: &PoolStore, address: &str) -> f64 {
    let since = SystemTime::now()
        .checked_sub(HASHRATE_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_for_miner(address, since)
    else {
        return 0.0;
    };
    hashrate_from_stats_with_miner_ramp(
        total_diff,
        count,
        oldest,
        newest,
        HASHRATE_WINDOW,
        HASHRATE_WARMUP_WINDOW,
        HASHRATE_BRAND_NEW_MIN_WINDOW,
        SystemTime::now(),
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

fn hashrate_from_stats_with_miner_ramp(
    total_diff: u64,
    count: u64,
    oldest: Option<SystemTime>,
    newest: Option<SystemTime>,
    smoothing_window: Duration,
    warmup_window: Duration,
    brand_new_min_window: Duration,
    now: SystemTime,
) -> f64 {
    if total_diff == 0 {
        return 0.0;
    }

    let smoothing_secs = smoothing_window.as_secs_f64().max(1.0);
    let warmup_secs = warmup_window.as_secs_f64().clamp(1.0, smoothing_secs);
    let brand_new_min_secs = brand_new_min_window.as_secs_f64().clamp(1.0, warmup_secs);

    let span_with_idle_secs = match (oldest, newest) {
        (Some(oldest), Some(newest)) => {
            let newest_age_secs = now
                .duration_since(newest)
                .ok()
                .map(|age| age.as_secs_f64())
                .unwrap_or(0.0);
            let observed_secs = if count < 2 {
                0.0
            } else {
                newest
                    .duration_since(oldest)
                    .ok()
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0)
            };
            if observed_secs >= 1.0 {
                observed_secs + newest_age_secs
            } else {
                newest_age_secs
            }
        }
        _ => 0.0,
    };

    let is_brand_new = oldest
        .and_then(|first| now.duration_since(first).ok())
        .is_some_and(|age| age.as_secs_f64() <= warmup_secs);

    let min_denominator_secs = if is_brand_new {
        brand_new_min_secs
    } else {
        warmup_secs
    };
    let denominator = if span_with_idle_secs >= 1.0 {
        span_with_idle_secs.clamp(min_denominator_secs, smoothing_secs)
    } else {
        min_denominator_secs
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
    pub config: Config,
    pub store: Arc<PoolStore>,
    pub stats: Arc<PoolStats>,
    pub jobs: Arc<JobManager>,
    pub node: Arc<NodeClient>,
    pub validation: Arc<ValidationEngine>,
    pub db_totals_cache: Arc<Mutex<DbTotalsCache>>,
    pub daemon_health_cache: Arc<Mutex<DaemonHealthCache>>,
    pub network_hashrate_cache: Arc<Mutex<NetworkHashrateCache>>,
    pub insights_cache: Arc<Mutex<InsightsCache>>,
    pub miner_pending_estimate_cache: Arc<Mutex<HashMap<String, MinerPendingEstimateCache>>>,
    pub status_history: Arc<Mutex<StatusHistory>>,
    pub sse_subscriber_limiter: Arc<Semaphore>,
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
    confirmed_blocks: u64,
    orphaned_blocks: u64,
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
pub struct MinerPendingEstimateCache {
    updated_at: Instant,
    chain_height: u64,
    value: MinerPendingEstimate,
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
struct BlockPageItemResponse {
    height: u64,
    hash: String,
    difficulty: u64,
    finder: String,
    finder_worker: String,
    reward: u64,
    timestamp: SystemTime,
    confirmed: bool,
    orphaned: bool,
    paid_out: bool,
    effort_pct: Option<f64>,
    duration_seconds: Option<u64>,
    timer_effort_pct: Option<f64>,
    effort_band: Option<EffortBand>,
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
        .route("/api/daemon/logs/stream", get(handle_daemon_logs_stream))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            require_api_key,
        ));

    let app = Router::new()
        .route("/", get(handle_ui))
        .route("/ui", get(handle_ui))
        .route("/robots.txt", get(handle_robots_txt))
        .route("/sitemap.xml", get(handle_sitemap_xml))
        .route("/favicon.svg", get(handle_favicon_svg))
        .route("/og-image.svg", get(handle_og_image_svg))
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
        .route("/api/luck", get(handle_luck_history))
        .route("/api/status", get(handle_status))
        .route("/api/events", get(handle_events))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts/recent", get(handle_public_payouts))
        .route("/api/miner/:address/balance", get(handle_miner_balance))
        .route("/api/miner/:address", get(handle_miner))
        .route("/api/miner/:address/hashrate", get(handle_miner_hashrate))
        .merge(protected)
        .fallback(get(handle_ui))
        .with_state(app_state);

    if state.config.has_api_tls() {
        let cert_path = state.config.api_tls_cert_path.trim();
        let key_path = state.config.api_tls_key_path.trim();
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path).await?;
        tracing::info!(
            addr = %addr,
            cert_path = cert_path,
            key_path = key_path,
            "api listening with tls"
        );
        axum_server::bind_rustls(addr, tls)
            .serve(app.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(addr = %addr, "api listening");
        axum::serve(listener, app).await?;
    }
    Ok(())
}

const UI_INDEX_HTML: &str = include_str!("ui/dist/index.html");
const UI_ASSET_APP_JS: &str = include_str!("ui/dist/app.js");
const UI_ASSET_APP_CSS: &str = include_str!("ui/dist/app.css");
const UI_ASSET_POOL_ENTERED_PNG: &[u8] = include_bytes!("ui/assets/pool-entered.png");
const UI_ASSET_MINING_TUI_PNG: &[u8] = include_bytes!("ui/assets/mining-tui.png");
const UI_FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="Blocknet Pool"><defs><linearGradient id="pool-grad" x1="0" x2="1" y1="0" y2="1"><stop offset="0%" stop-color="#57d78c"/><stop offset="100%" stop-color="#16a34a"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#071114"/><rect x="10" y="10" width="44" height="44" rx="12" fill="url(#pool-grad)"/><rect x="20" y="20" width="24" height="24" rx="6" fill="#f6f8f2" opacity="0.96"/><rect x="27" y="27" width="10" height="10" rx="3" fill="#071114"/></svg>"##;
const INDEXABLE_ROBOTS_TAG: &str = "index, follow, max-image-preview:large";
const PRIVATE_ROBOTS_TAG: &str = "noindex, nofollow, noarchive";

#[derive(Clone, Copy)]
enum UiRoute {
    Dashboard,
    Start,
    Luck,
    Blocks,
    Payouts,
    Stats,
    Admin,
    Status,
}

impl UiRoute {
    fn from_path(path: &str) -> Self {
        let normalized = if path == "/" {
            "/"
        } else {
            path.trim_end_matches('/')
        };
        match normalized {
            "/" | "/ui" => Self::Dashboard,
            "/start" => Self::Start,
            "/luck" => Self::Luck,
            "/blocks" => Self::Blocks,
            "/payouts" => Self::Payouts,
            "/stats" => Self::Stats,
            "/admin" => Self::Admin,
            "/status" => Self::Status,
            _ => Self::Dashboard,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Dashboard => "/",
            Self::Start => "/start",
            Self::Luck => "/luck",
            Self::Blocks => "/blocks",
            Self::Payouts => "/payouts",
            Self::Stats => "/stats",
            Self::Admin => "/admin",
            Self::Status => "/status",
        }
    }

    fn slug(self) -> &'static str {
        match self {
            Self::Dashboard => "dashboard",
            Self::Start => "start",
            Self::Luck => "luck",
            Self::Blocks => "blocks",
            Self::Payouts => "payouts",
            Self::Stats => "stats",
            Self::Admin => "admin",
            Self::Status => "status",
        }
    }

    fn schema_type(self) -> &'static str {
        match self {
            Self::Start => "HowTo",
            Self::Luck | Self::Blocks | Self::Payouts => "CollectionPage",
            _ => "WebPage",
        }
    }

    fn visible_title(self) -> &'static str {
        match self {
            Self::Dashboard => "Live Blocknet pool dashboard",
            Self::Start => "How to start mining Blocknet",
            Self::Luck => "Blocknet pool luck history",
            Self::Blocks => "Recently found Blocknet blocks",
            Self::Payouts => "Recent Blocknet pool payouts",
            Self::Stats => "Miner stats lookup",
            Self::Admin => "Admin dashboard",
            Self::Status => "Blocknet pool status",
        }
    }

    fn kicker(self) -> &'static str {
        match self {
            Self::Dashboard => "Blocknet Mining Pool",
            Self::Start => "Blocknet Mining Guide",
            Self::Luck => "Round History",
            Self::Blocks => "Block Discovery",
            Self::Payouts => "Payout Transparency",
            Self::Stats => "Miner Lookup",
            Self::Admin => "Operations",
            Self::Status => "Pool Monitoring",
        }
    }

    fn indexable(self) -> bool {
        matches!(
            self,
            Self::Dashboard | Self::Start | Self::Luck | Self::Blocks | Self::Payouts | Self::Status
        )
    }

    fn robots(self) -> &'static str {
        if self.indexable() {
            INDEXABLE_ROBOTS_TAG
        } else {
            PRIVATE_ROBOTS_TAG
        }
    }
}

struct UiSeoPage {
    title: String,
    description: String,
    canonical_url: String,
    site_name: String,
    robots: &'static str,
    og_image_url: String,
    og_image_alt: String,
    json_ld: String,
    content_html: String,
}

fn format_decimal(value: f64) -> String {
    let mut out = format!("{value:.2}");
    while out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.pop();
    }
    out
}

fn pool_base_url(state: &ApiState) -> String {
    let trimmed = state.pool_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "https://bntpool.com".to_string()
    } else {
        trimmed.to_string()
    }
}

fn pool_host(state: &ApiState) -> String {
    let raw = state.pool_url.trim();
    let without_scheme = raw.split("://").nth(1).unwrap_or(raw);
    let host_port = without_scheme.split('/').next().unwrap_or("bntpool.com");
    let host = host_port.split('@').next_back().unwrap_or(host_port);
    let normalized = if host.starts_with('[') {
        host
    } else {
        host.split(':').next().unwrap_or(host)
    };
    let trimmed = normalized.trim();
    if trimmed.is_empty() {
        "bntpool.com".to_string()
    } else {
        trimmed.to_string()
    }
}

fn stratum_endpoint(state: &ApiState) -> String {
    format!("stratum+tcp://{}:{}", pool_host(state), state.stratum_port)
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn json_for_script(value: &serde_json::Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "[]".to_string())
        .replace("</", "<\\/")
}

fn seo_title(route: UiRoute, state: &ApiState) -> String {
    match route {
        UiRoute::Dashboard => format!("Blocknet Mining Pool Dashboard | {}", state.pool_name),
        UiRoute::Start => format!("How To Start Mining Blocknet | {}", state.pool_name),
        UiRoute::Luck => format!("Pool Luck History | {}", state.pool_name),
        UiRoute::Blocks => format!("Recent Blocknet Blocks | {}", state.pool_name),
        UiRoute::Payouts => format!("Recent Pool Payouts | {}", state.pool_name),
        UiRoute::Stats => format!("Miner Stats Lookup | {}", state.pool_name),
        UiRoute::Admin => format!("Admin Dashboard | {}", state.pool_name),
        UiRoute::Status => format!("Pool Status | {}", state.pool_name),
    }
}

fn seo_description(route: UiRoute, state: &ApiState) -> String {
    match route {
        UiRoute::Dashboard => format!(
            "Mine Blocknet with {}. Track pool hashrate, round luck, recent blocks, payouts, and pool health from one public dashboard.",
            state.pool_name
        ),
        UiRoute::Start => {
            let fee = if state.pool_fee_pct > 0.0 {
                format!("{}% fee", format_decimal(state.pool_fee_pct))
            } else {
                "transparent fee model".to_string()
            };
            format!(
                "Learn how to mine Blocknet with {}, connect to {}, and understand {} payouts and {}.",
                state.pool_name,
                stratum_endpoint(state),
                state.payout_scheme.to_uppercase(),
                fee
            )
        }
        UiRoute::Luck => format!(
            "Track {} luck history with round effort, round duration, and confirmation status for recent Blocknet blocks.",
            state.pool_name
        ),
        UiRoute::Blocks => format!(
            "Browse recently found Blocknet blocks from {}, including confirmed, pending, and orphaned rounds with effort and timing data.",
            state.pool_name
        ),
        UiRoute::Payouts => format!(
            "Review recent Blocknet mining pool payouts from {}, including payout totals, recipient counts, explorer transaction links, and payout timing.",
            state.pool_name
        ),
        UiRoute::Stats => format!(
            "Look up a Blocknet wallet address on {} to inspect hashrate, balances, worker activity, recent payouts, and share history.",
            state.pool_name
        ),
        UiRoute::Admin => format!(
            "Administrative dashboard for {} with miners, payouts, fees, health checks, and daemon log streaming.",
            state.pool_name
        ),
        UiRoute::Status => format!(
            "Monitor {} uptime, daemon reachability, sync status, and incident history from the public Blocknet pool status page.",
            state.pool_name
        ),
    }
}

fn fallback_page_markup(route: UiRoute, intro: &str, body: &str) -> String {
    format!(
        r#"<div class="container"><main class="page active seo-fallback-page" id="seo-fallback-{slug}"><div class="page-header"><span class="page-kicker">{kicker}</span><h1>{title}</h1><p class="page-intro">{intro}</p></div>{body}</main></div>"#,
        slug = route.slug(),
        kicker = escape_html(route.kicker()),
        title = escape_html(route.visible_title()),
        intro = escape_html(intro),
        body = body,
    )
}

fn fallback_content(route: UiRoute, state: &ApiState) -> String {
    let stratum = escape_html(&stratum_endpoint(state));
    match route {
        UiRoute::Dashboard => fallback_page_markup(
            route,
            "Track pool hashrate, round luck, recent blocks, payout timing, and current chain conditions from the public dashboard.",
            &format!(
                r#"<div class="stratum-bar"><span style="font-size:14px;font-weight:600;color:var(--muted)">Stratum</span><span class="endpoint">{stratum}</span></div><div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Live pool metrics</h3><p>Watch connected miners, pool hashrate, network hashrate, and current job height from one public dashboard.</p></div><div class="card seo-copy-card"><h3>Transparent operations</h3><p>Review recently found blocks, round effort, payout timing, and service status before you mine.</p></div><div class="card seo-copy-card"><h3>Fast onboarding</h3><p>Use the getting started guide to connect Seine, set your Blocknet address, and begin mining.</p></div></div>"#
            ),
        ),
        UiRoute::Start => fallback_page_markup(
            route,
            "Download Seine, connect to the pool stratum endpoint, and monitor your Blocknet hashrate and payouts from the public dashboard.",
            &format!(
                r#"<div class="card section"><h3>Quick start</h3><p class="section-lead">Connect your Blocknet wallet address to the pool and start mining with Seine.</p><pre class="config-block">./seine --pool-url {stratum} --address YOUR_BLOCKNET_ADDRESS</pre></div><div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Transparent pool data</h3><p>Review blocks, payout batches, and status pages before you point any hashpower at the pool.</p></div><div class="card seo-copy-card"><h3>Simple setup</h3><p>You only need your Blocknet wallet address and the pool URL to start mining with Seine.</p></div><div class="card seo-copy-card"><h3>Operator visibility</h3><p>Dashboard metrics, historical luck, and uptime tracking make it easier to compare pool performance over time.</p></div></div>"#
            ),
        ),
        UiRoute::Luck => fallback_page_markup(
            route,
            "Compare round effort and duration over time to understand how actual block discovery compares with expected pool luck.",
            r#"<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Round effort</h3><p>Measure how much work each round required compared with statistical expectation.</p></div><div class="card seo-copy-card"><h3>Round duration</h3><p>Inspect how long each round lasted before a block was found or orphaned.</p></div><div class="card seo-copy-card"><h3>Historical transparency</h3><p>Use recent round history to compare pool variance across multiple blocks.</p></div></div>"#,
        ),
        UiRoute::Blocks => fallback_page_markup(
            route,
            "Browse confirmed, pending, and orphaned pool blocks with reward, round effort, and elapsed round time for each Blocknet block.",
            r#"<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Confirmed blocks</h3><p>Review completed rounds with explorer links for direct on-chain verification.</p></div><div class="card seo-copy-card"><h3>Pending rounds</h3><p>Track newly found blocks while they move through the required confirmations window.</p></div><div class="card seo-copy-card"><h3>Round diagnostics</h3><p>Compare effort percentage and round time to understand variance across recent blocks.</p></div></div>"#,
        ),
        UiRoute::Payouts => fallback_page_markup(
            route,
            "Review payout totals, recipient counts, network fees, and explorer transaction links for recent pool payout batches.",
            r#"<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Verified payouts</h3><p>Transaction links point to the public Blocknet explorer so payouts can be checked independently.</p></div><div class="card seo-copy-card"><h3>Batch visibility</h3><p>Inspect how many miners were paid in each payout batch and how much value moved on-chain.</p></div><div class="card seo-copy-card"><h3>Fee transparency</h3><p>Review recent network fees alongside payout timestamps to understand payout cadence.</p></div></div>"#,
        ),
        UiRoute::Stats => fallback_page_markup(
            route,
            "Look up a Blocknet wallet address to inspect hashrate, balances, workers, shares, and payout history.",
            r#"<div class="card section"><h3>Private miner lookup</h3><p class="section-lead">The stats page is intended for individual miner lookups and is excluded from search indexing.</p></div>"#,
        ),
        UiRoute::Admin => fallback_page_markup(
            route,
            "Administrative view for miners, payouts, fees, health checks, and daemon logs.",
            r#"<div class="card section"><h3>Operator tools</h3><p class="section-lead">The admin dashboard requires an API key and is excluded from search indexing.</p></div>"#,
        ),
        UiRoute::Status => fallback_page_markup(
            route,
            "Monitor uptime, daemon reachability, sync state, and recent incident history from the public status page.",
            r#"<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Daemon reachability</h3><p>Check whether the Blocknet daemon is online and ready to serve work.</p></div><div class="card seo-copy-card"><h3>Historical uptime</h3><p>Review uptime windows over time to understand how stable the pool has been.</p></div><div class="card seo-copy-card"><h3>Incident tracking</h3><p>Scan recent incidents and severity to catch operational issues quickly.</p></div></div>"#,
        ),
    }
}

fn structured_data(route: UiRoute, state: &ApiState, canonical_url: &str, title: &str, description: &str) -> serde_json::Value {
    let base_url = pool_base_url(state);
    let mut items = vec![
        serde_json::json!({
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": state.pool_name,
            "url": base_url,
            "description": seo_description(UiRoute::Dashboard, state),
        }),
        serde_json::json!({
            "@context": "https://schema.org",
            "@type": route.schema_type(),
            "name": title,
            "url": canonical_url,
            "description": description,
            "isPartOf": {
                "@type": "WebSite",
                "name": state.pool_name,
                "url": base_url,
            }
        }),
    ];

    if matches!(route, UiRoute::Start) {
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "HowTo",
            "name": "How to start mining Blocknet with Seine",
            "url": canonical_url,
            "description": description,
            "step": [
                {
                    "@type": "HowToStep",
                    "name": "Download Seine",
                    "text": "Download the latest Seine release for your platform.",
                },
                {
                    "@type": "HowToStep",
                    "name": "Enter your Blocknet wallet address",
                    "text": "Launch Seine and provide your Blocknet payout address.",
                },
                {
                    "@type": "HowToStep",
                    "name": "Connect to the pool stratum endpoint",
                    "text": format!("Use {} as the pool URL.", stratum_endpoint(state)),
                },
                {
                    "@type": "HowToStep",
                    "name": "Start mining and monitor payouts",
                    "text": "Run the miner, then use the pool dashboard and stats page to track hashrate and balances.",
                }
            ]
        }));
    }

    if !matches!(route, UiRoute::Dashboard) {
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {
                    "@type": "ListItem",
                    "position": 1,
                    "name": state.pool_name,
                    "item": base_url,
                },
                {
                    "@type": "ListItem",
                    "position": 2,
                    "name": route.visible_title(),
                    "item": canonical_url,
                }
            ]
        }));
    }

    serde_json::Value::Array(items)
}

fn build_ui_seo_page(route: UiRoute, state: &ApiState) -> UiSeoPage {
    let site_name = state.pool_name.clone();
    let canonical_url = format!("{}{}", pool_base_url(state), route.path());
    let title = seo_title(route, state);
    let description = seo_description(route, state);
    let og_image_url = format!("{}/og-image.svg", pool_base_url(state));
    let og_image_alt = format!("{} Blocknet mining pool overview", state.pool_name);
    let json_ld = json_for_script(&structured_data(
        route,
        state,
        &canonical_url,
        &title,
        &description,
    ));
    let content_html = fallback_content(route, state);

    UiSeoPage {
        title,
        description,
        canonical_url,
        site_name,
        robots: route.robots(),
        og_image_url,
        og_image_alt,
        json_ld,
        content_html,
    }
}

fn render_ui_html(route: UiRoute, state: &ApiState) -> String {
    let page = build_ui_seo_page(route, state);
    UI_INDEX_HTML
        .replace("__SEO_TITLE__", &escape_html(&page.title))
        .replace("__SEO_DESCRIPTION__", &escape_html(&page.description))
        .replace("__SEO_ROBOTS__", page.robots)
        .replace("__SEO_CANONICAL__", &escape_html(&page.canonical_url))
        .replace("__SEO_SITE_NAME__", &escape_html(&page.site_name))
        .replace("__SEO_OG_IMAGE__", &escape_html(&page.og_image_url))
        .replace("__SEO_OG_IMAGE_ALT__", &escape_html(&page.og_image_alt))
        .replace("__SEO_JSON_LD__", &page.json_ld)
        .replace("__SEO_CONTENT__", &page.content_html)
}

fn render_og_image_svg(state: &ApiState) -> String {
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" role="img" aria-labelledby="title desc"><title id="title">{title}</title><desc id="desc">{desc}</desc><defs><linearGradient id="bg-grad" x1="0" x2="1" y1="0" y2="1"><stop offset="0%" stop-color="#071114"/><stop offset="100%" stop-color="#102a1a"/></linearGradient><linearGradient id="panel-grad" x1="0" x2="1" y1="0" y2="1"><stop offset="0%" stop-color="#57d78c"/><stop offset="100%" stop-color="#16a34a"/></linearGradient></defs><rect width="1200" height="630" fill="url(#bg-grad)"/><circle cx="1080" cy="120" r="180" fill="#57d78c" opacity="0.08"/><circle cx="180" cy="560" r="220" fill="#f7b44b" opacity="0.08"/><rect x="72" y="72" width="1056" height="486" rx="36" fill="#0d181d" stroke="#1f3121" stroke-width="2"/><rect x="96" y="96" width="84" height="84" rx="24" fill="url(#panel-grad)"/><rect x="118" y="118" width="40" height="40" rx="10" fill="#f6f8f2" opacity="0.96"/><rect x="130" y="130" width="16" height="16" rx="4" fill="#071114"/><text x="214" y="128" fill="#57d78c" font-family="Manrope, Arial, sans-serif" font-size="28" font-weight="700">Blocknet Mining Pool</text><text x="96" y="246" fill="#ecf5f0" font-family="Manrope, Arial, sans-serif" font-size="76" font-weight="700">{pool_name}</text><text x="96" y="314" fill="#9cb0a8" font-family="Manrope, Arial, sans-serif" font-size="32">Live hashrate, block, payout, and status monitoring</text><rect x="96" y="380" width="496" height="68" rx="24" fill="#132129" stroke="#1f3121" stroke-width="2"/><text x="128" y="422" fill="#57d78c" font-family="JetBrains Mono, monospace" font-size="26">{stratum}</text><rect x="96" y="476" width="220" height="38" rx="19" fill="#57d78c" opacity="0.12"/><text x="126" y="501" fill="#81dfaf" font-family="Manrope, Arial, sans-serif" font-size="20" font-weight="700">Public blocks and payouts</text><rect x="334" y="476" width="182" height="38" rx="19" fill="#f7b44b" opacity="0.12"/><text x="364" y="501" fill="#f7b44b" font-family="Manrope, Arial, sans-serif" font-size="20" font-weight="700">Live status page</text><rect x="534" y="476" width="194" height="38" rx="19" fill="#57d78c" opacity="0.12"/><text x="564" y="501" fill="#81dfaf" font-family="Manrope, Arial, sans-serif" font-size="20" font-weight="700">Seine onboarding</text></svg>"##,
        title = escape_html(&state.pool_name),
        desc = escape_html("Blocknet mining pool social card"),
        pool_name = escape_html(&state.pool_name),
        stratum = escape_html(&stratum_endpoint(state)),
    )
}

async fn handle_ui(uri: Uri, State(state): State<ApiState>) -> Response {
    let route = UiRoute::from_path(uri.path());
    let html = render_ui_html(route, &state);
    let mut response = Html(html).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if let Ok(value) = HeaderValue::from_str(route.robots()) {
        response.headers_mut().insert(
            HeaderName::from_static("x-robots-tag"),
            value,
        );
    }
    response
}

async fn handle_robots_txt(State(state): State<ApiState>) -> impl IntoResponse {
    let body = format!(
        "User-agent: *\nAllow: /\nDisallow: /admin\nDisallow: /stats\nDisallow: /ui\nSitemap: {}/sitemap.xml\n",
        pool_base_url(&state)
    );
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
}

async fn handle_sitemap_xml(State(state): State<ApiState>) -> impl IntoResponse {
    let base = pool_base_url(&state);
    let entries = [
        ("/", "weekly", "1.0"),
        ("/start", "monthly", "0.95"),
        ("/blocks", "hourly", "0.9"),
        ("/payouts", "hourly", "0.85"),
        ("/luck", "daily", "0.75"),
        ("/status", "hourly", "0.8"),
    ];
    let urls = entries
        .iter()
        .map(|(path, changefreq, priority)| {
            format!(
                "<url><loc>{}</loc><changefreq>{}</changefreq><priority>{}</priority></url>",
                escape_html(&format!("{base}{path}")),
                changefreq,
                priority
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>"#
    );
    (
        [
            (header::CONTENT_TYPE, "application/xml; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
}

async fn handle_favicon_svg() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        UI_FAVICON_SVG,
    )
}

async fn handle_og_image_svg(State(state): State<ApiState>) -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        render_og_image_svg(&state),
    )
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
            "/api/luck",
            "/api/status",
            "/api/events",
            "/api/blocks",
            "/api/payouts/recent",
            "/api/miner/{address}",
            "/api/miner/{address}/balance",
        ],
        protected_endpoints: vec![
            "/api/miners",
            "/api/payouts",
            "/api/fees",
            "/api/health",
            "/api/daemon/logs/stream",
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
    orphaned_blocks: u64,
    orphan_rate_pct: f64,
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
struct StatsInsightsQuery {
    rejection_window: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct LuckHistoryQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct MinerDetailQuery {
    share_limit: Option<i64>,
    include_pending_estimate: Option<bool>,
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

#[derive(Debug, Deserialize, Default)]
struct DaemonLogsQuery {
    tail: Option<usize>,
    follow: Option<bool>,
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
            orphaned_blocks: totals.orphaned_blocks,
            orphan_rate_pct: {
                let resolved = totals
                    .confirmed_blocks
                    .saturating_add(totals.orphaned_blocks);
                if resolved == 0 {
                    0.0
                } else {
                    (totals.orphaned_blocks as f64 / resolved as f64) * 100.0
                }
            },
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

async fn handle_stats_insights(
    Query(query): Query<StatsInsightsQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let rejection_window = rejection_window_duration(query.rejection_window.as_deref());
    match state.stats_insights().await {
        Ok(mut v) => {
            match state.rejection_analytics_snapshot(rejection_window).await {
                Ok(snapshot) => {
                    v.rejections.window = snapshot;
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "failed to load persisted rejection analytics; using in-memory fallback"
                    );
                    v.rejections.window = state.stats.rejection_analytics(rejection_window);
                }
            }
            Json(v).into_response()
        }
        Err(err) => internal_error("failed loading stats insights", err).into_response(),
    }
}

fn rejection_window_duration(input: Option<&str>) -> Duration {
    let label = input.map(str::trim).unwrap_or("1h");
    if label.eq_ignore_ascii_case("24h") {
        Duration::from_secs(24 * 3600)
    } else if label.eq_ignore_ascii_case("7d") {
        Duration::from_secs(7 * 24 * 3600)
    } else {
        Duration::from_secs(3600)
    }
}

async fn handle_status(State(state): State<ApiState>) -> impl IntoResponse {
    let daemon = state.daemon_health().await;
    Json(state.build_status_response(daemon)).into_response()
}

async fn handle_events(State(state): State<ApiState>) -> Response {
    let permit = match Arc::clone(&state.sse_subscriber_limiter).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "5")],
                Json(serde_json::json!({"error":"too many active event subscribers"})),
            )
                .into_response();
        }
    };

    let stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(5))).then({
        let state = state.clone();
        move |_| {
            // Keep the semaphore permit held for the full stream lifetime.
            let _permit_held = &permit;
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
        }
    });

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
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

#[derive(Debug, Clone)]
struct DaemonLogCommand {
    source: &'static str,
    program: &'static str,
    args: Vec<String>,
}

async fn handle_daemon_logs_stream(
    Query(query): Query<DaemonLogsQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let tail = query
        .tail
        .unwrap_or(DEFAULT_DAEMON_LOG_TAIL)
        .clamp(1, MAX_DAEMON_LOG_TAIL);
    let follow = query.follow.unwrap_or(true);
    let config = state.config.clone();

    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, Infallible>>(128);
    tokio::spawn(async move {
        stream_daemon_logs(config, tail, follow, tx).await;
    });

    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from_stream(ReceiverStream::new(rx)),
    )
        .into_response()
}

async fn stream_daemon_logs(
    config: Config,
    tail: usize,
    follow: bool,
    tx: mpsc::Sender<Result<Vec<u8>, Infallible>>,
) {
    let mut errors = Vec::<String>::new();
    for command in daemon_log_commands(&config, tail, follow) {
        if !send_log_line(
            &tx,
            &format!(
                "[daemon-logs] source={} command={} {}",
                command.source,
                command.program,
                command.args.join(" ")
            ),
        )
        .await
        {
            return;
        }

        match stream_daemon_logs_with_command(&command, &tx).await {
            Ok(()) => return,
            Err(err) => {
                errors.push(format!("{} failed: {}", command.source, err));
            }
        }
    }

    let reason = if errors.is_empty() {
        "no daemon log source available".to_string()
    } else {
        errors.join("; ")
    };
    let _ = send_log_line(&tx, &format!("[daemon-logs] stream ended: {reason}")).await;
}

fn daemon_log_commands(config: &Config, tail: usize, follow: bool) -> Vec<DaemonLogCommand> {
    let mut journal_args = vec![
        "-u".to_string(),
        "blocknetd.service".to_string(),
        "-q".to_string(),
        "-a".to_string(),
        "-n".to_string(),
        tail.to_string(),
        "-o".to_string(),
        "short-iso".to_string(),
    ];
    if follow {
        journal_args.push("-f".to_string());
    }

    let mut tail_args = vec!["-n".to_string(), tail.to_string()];
    if follow {
        tail_args.push("-F".to_string());
    }
    tail_args.push(
        daemon_debug_log_path(config)
            .to_string_lossy()
            .trim()
            .to_string(),
    );

    vec![
        DaemonLogCommand {
            source: "journald",
            program: "journalctl",
            args: journal_args,
        },
        DaemonLogCommand {
            source: "debug-log",
            program: "tail",
            args: tail_args,
        },
    ]
}

fn daemon_debug_log_path(config: &Config) -> PathBuf {
    let data_dir = config.daemon_data_dir.trim();
    if data_dir.is_empty() {
        return PathBuf::from("data").join("debug.log");
    }
    PathBuf::from(data_dir).join("debug.log")
}

async fn stream_daemon_logs_with_command(
    command: &DaemonLogCommand,
    tx: &mpsc::Sender<Result<Vec<u8>, Infallible>>,
) -> anyhow::Result<()> {
    let mut child = Command::new(command.program)
        .args(&command.args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| anyhow::anyhow!("spawn failed: {err}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing stdout pipe"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing stderr pipe"))?;
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_open = true;
    let mut stderr_open = true;

    while stdout_open || stderr_open {
        tokio::select! {
            result = stdout_lines.next_line(), if stdout_open => {
                match result {
                    Ok(Some(line)) => {
                        if !send_log_line(tx, &trim_log_line(&line)).await {
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        stdout_open = false;
                    }
                    Err(err) => {
                        if !send_log_line(tx, &format!("[daemon-logs] stdout read error: {err}")).await {
                            return Ok(());
                        }
                        stdout_open = false;
                    }
                }
            }
            result = stderr_lines.next_line(), if stderr_open => {
                match result {
                    Ok(Some(line)) => {
                        if !send_log_line(tx, &format!("[stderr] {}", trim_log_line(&line))).await {
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        stderr_open = false;
                    }
                    Err(err) => {
                        if !send_log_line(tx, &format!("[daemon-logs] stderr read error: {err}")).await {
                            return Ok(());
                        }
                        stderr_open = false;
                    }
                }
            }
        }
    }

    let status = child
        .wait()
        .await
        .map_err(|err| anyhow::anyhow!("wait failed: {err}"))?;
    if !status.success() {
        anyhow::bail!("exited with status {status}");
    }
    Ok(())
}

async fn send_log_line(tx: &mpsc::Sender<Result<Vec<u8>, Infallible>>, line: &str) -> bool {
    let mut payload = line.as_bytes().to_vec();
    payload.push(b'\n');
    tx.send(Ok(payload)).await.is_ok()
}

fn trim_log_line(line: &str) -> String {
    if line.len() <= DAEMON_LOG_LINE_LIMIT {
        return line.to_string();
    }
    let mut boundary = DAEMON_LOG_LINE_LIMIT;
    while boundary > 0 && !line.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{} ...[truncated]", &line[..boundary])
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

    // Fetch DB-backed hashrates and lifetime per-miner counts in one blocking task.
    let (hashrates, lifetime_counts) = if addresses.len() > MAX_MINER_HASHRATE_DB_LOOKUPS {
        tracing::warn!(
            miner_count = addresses.len(),
            lookup_cap = MAX_MINER_HASHRATE_DB_LOOKUPS,
            "miner hashrate DB lookup skipped for large miner set; using in-memory estimates"
        );
        (fallback_hashrates, HashMap::new())
    } else {
        let store = Arc::clone(&state.store);
        match tokio::task::spawn_blocking(move || {
            let mut hr_map = HashMap::with_capacity(addresses.len());
            for address in &addresses {
                hr_map.insert(address.clone(), db_miner_hashrate(&store, address));
            }
            let lifetime = store.miner_lifetime_counts().unwrap_or_default();
            Ok::<_, anyhow::Error>((hr_map, lifetime))
        })
        .await
        {
            Ok(Ok(pair)) => pair,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "failed loading miner data from db; using in-memory estimates");
                (fallback_hashrates, HashMap::new())
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed joining miner db task; using in-memory estimates");
                (fallback_hashrates, HashMap::new())
            }
        }
    };
    let mut items = all_stats
        .into_values()
        .map(|stats| {
            let mut workers = stats.workers.iter().cloned().collect::<Vec<String>>();
            workers.sort();
            let hashrate = hashrates.get(&stats.address).copied().unwrap_or(0.0);
            // Use DB lifetime counts when available, fall back to in-memory.
            let (accepted, rejected, blocks, db_last_share) = lifetime_counts
                .get(&stats.address)
                .copied()
                .unwrap_or((stats.shares_accepted, stats.shares_rejected, stats.blocks_found, None));
            let last_share_at = db_last_share
                .map(|ts| std::time::UNIX_EPOCH + std::time::Duration::from_secs(ts.max(0) as u64))
                .or(stats.last_share_at);
            MinerListItem {
                address: stats.address.clone(),
                worker_count: workers.len(),
                workers,
                shares_accepted: accepted,
                shares_rejected: rejected,
                blocks_found: blocks,
                hashrate,
                last_share_at,
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

async fn handle_miner_balance(
    Path(address): Path<String>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let addr = address.clone();
    let store = Arc::clone(&state.store);
    let balance = match tokio::task::spawn_blocking(move || store.get_balance(&addr)).await {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading miner balance", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading miner balance",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response();
        }
    };

    Json(serde_json::json!({
        "address": address,
        "balance": {
            "pending": balance.pending,
            "pending_total": balance.pending,
            "pending_confirmed": balance.pending,
            "pending_estimated": 0u64,
            "paid": balance.paid,
        }
    }))
    .into_response()
}

async fn handle_miner(
    Path(address): Path<String>,
    Query(query): Query<MinerDetailQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let stats = state.stats.get_miner_stats(&address);
    let store = Arc::clone(&state.store);
    let chain_height = state.node.chain_height();
    let addr = address.clone();
    let share_limit = share_limit(query.share_limit);
    let include_pending_estimate = query.include_pending_estimate.unwrap_or(true);

    let db_result = match tokio::task::spawn_blocking(move || {
        let shares = store.get_shares_for_miner(&addr, share_limit)?;
        let balance = store.get_balance(&addr)?;
        let pending_payout = store.get_pending_payout(&addr)?;
        let payouts = store.get_recent_payouts_for_address(&addr, MINER_PAYOUT_HISTORY_LIMIT)?;
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
            payouts,
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
        payouts,
        hashrate,
        workers_raw,
        worker_hashrate_raw,
        mut miner_blocks,
    ) = db_result;
    for block in &mut miner_blocks {
        hydrate_provisional_block_reward(block);
    }

    let pending_estimate = if include_pending_estimate {
        match state
            .cached_pending_estimate_for_miner(&address, chain_height)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    address = %address,
                    error = %err,
                    "failed loading pending estimate for miner detail"
                );
                MinerPendingEstimate::default()
            }
        }
    } else {
        MinerPendingEstimate::default()
    };

    let pending_confirmed = balance.pending;
    let pending_estimated = pending_estimate.estimated_pending;
    let pending_total = pending_confirmed.saturating_add(pending_estimated);

    let balance_json = serde_json::json!({
        "pending": pending_total,
        "pending_total": pending_total,
        "pending_confirmed": pending_confirmed,
        "pending_estimated": pending_estimated,
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
    let pending_note = if include_pending_estimate {
        pending_balance_note(
            hashrate,
            total_accepted,
            pending_total,
            pending_estimate.blocks.len(),
        )
    } else {
        None
    };

    let has_activity = miner_has_activity(
        shares.len(),
        pending_total,
        balance.paid,
        pending_payout.is_some(),
        payouts.len(),
    );

    let base = serde_json::json!({
        "shares": shares,
        "hashrate": hashrate,
        "balance": balance_json,
        "pending_estimate": pending_estimate,
        "pending_note": pending_note,
        "pending_payout": pending_payout,
        "payouts": payouts,
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

    let (limit, offset) = page_bounds(query.limit, query.offset);
    let sort = match query.sort.as_deref().map(str::trim) {
        Some("height_asc") => "height_asc",
        Some("reward_desc") => "reward_desc",
        Some("reward_asc") => "reward_asc",
        Some("time_asc") => "time_asc",
        _ => "height_desc",
    };
    let finder = non_empty(&query.finder).map(str::to_string);
    let status = non_empty(&query.status)
        .filter(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "confirmed" | "orphaned" | "pending" | "paid" | "unpaid"
            )
        })
        .map(str::to_string);

    let store = Arc::clone(&state.store);
    let (mut blocks, total) = match tokio::task::spawn_blocking(move || {
        store.get_blocks_page(
            finder.as_deref(),
            status.as_deref(),
            sort,
            limit as i64,
            offset as i64,
        )
    })
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
    let returned = blocks.len();
    let target_hashes = blocks
        .iter()
        .map(|block| block.hash.clone())
        .collect::<HashSet<_>>();
    let store = Arc::clone(&state.store);
    let luck_by_hash = match tokio::task::spawn_blocking(move || {
        let all_blocks = store.get_all_blocks()?;
        compute_luck_details_for_hashes(&store, all_blocks, &target_hashes)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => {
            return internal_error("failed loading block luck details", err).into_response()
        }
        Err(err) => {
            return internal_error(
                "failed loading block luck details",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    let items = blocks
        .into_iter()
        .map(|block| {
            let block_hash = block.hash.clone();
            block_page_item_response(block, luck_by_hash.get(block_hash.as_str()))
        })
        .collect::<Vec<_>>();

    Json(PagedResponse {
        items,
        page: PageMeta {
            limit,
            offset,
            returned,
            total: total as usize,
        },
    })
    .into_response()
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

#[derive(Debug, Clone, Serialize)]
struct MinerPendingBlockEstimate {
    height: u64,
    hash: String,
    reward: u64,
    estimated_credit: u64,
    confirmations_remaining: u64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone, Serialize, Default)]
struct MinerPendingEstimate {
    estimated_pending: u64,
    blocks: Vec<MinerPendingBlockEstimate>,
}

fn estimate_unconfirmed_pending_for_miner(
    store: &PoolStore,
    address: &str,
    config: &Config,
    now: SystemTime,
    chain_height: u64,
) -> anyhow::Result<MinerPendingEstimate> {
    let unconfirmed_blocks = store.get_unconfirmed_blocks()?;
    let scheme_is_pplns = config.payout_scheme.trim().eq_ignore_ascii_case("pplns");
    let pplns_duration = config.pplns_window_duration_duration();
    let pplns_window = i64::from(config.pplns_window.max(1));
    let provisional_delay = config.provisional_share_delay_duration();
    let required_confirmations = config.blocks_before_payout.max(0) as u64;
    let trust_policy = PayoutTrustPolicy {
        min_verified_shares: config.payout_min_verified_shares.max(0) as u64,
        min_verified_ratio: config.payout_min_verified_ratio.clamp(0.0, 1.0),
        provisional_cap_multiplier: config.payout_provisional_cap_multiplier.max(0.0),
    };

    let mut risk_cache = HashMap::<String, bool>::new();
    let mut estimate = MinerPendingEstimate::default();

    for mut block in unconfirmed_blocks {
        if block.orphaned {
            continue;
        }
        hydrate_provisional_block_reward(&mut block);
        if block.reward == 0 {
            continue;
        }

        let mut distributable = block.reward.saturating_sub(config.pool_fee(block.reward));
        let shares = if scheme_is_pplns {
            if pplns_duration.is_zero() {
                store.get_last_n_shares_before(block.timestamp, pplns_window)?
            } else {
                let since = block
                    .timestamp
                    .checked_sub(pplns_duration)
                    .unwrap_or(UNIX_EPOCH);
                store.get_shares_between(since, block.timestamp)?
            }
        } else {
            let since = block
                .timestamp
                .checked_sub(PROPORTIONAL_WINDOW)
                .unwrap_or(UNIX_EPOCH);
            store.get_shares_between(since, block.timestamp)?
        };

        let mut credits = HashMap::<String, u64>::new();
        if shares.is_empty() {
            credit_address(&mut credits, &block.finder, distributable)?;
        } else {
            let (weights, total_weight) = weight_shares(
                &shares,
                now,
                provisional_delay,
                trust_policy,
                |candidate| {
                    if let Some(risky) = risk_cache.get(candidate) {
                        return *risky;
                    }
                    let risky = match store.should_force_verify_address(candidate) {
                        Ok((force_verify, _)) => force_verify,
                        Err(err) => {
                            tracing::warn!(
                                address = %candidate,
                                error = %err,
                                "failed risk check during pending estimate; treating address as risky"
                            );
                            true
                        }
                    };
                    risk_cache.insert(candidate.to_string(), risky);
                    risky
                },
            );

            if total_weight == 0 {
                credit_address(&mut credits, &block.finder, distributable)?;
            } else {
                if config.block_finder_bonus && config.block_finder_bonus_pct > 0.0 {
                    let bonus =
                        (distributable as f64 * config.block_finder_bonus_pct / 100.0) as u64;
                    credit_address(&mut credits, &block.finder, bonus)?;
                    distributable = distributable.saturating_sub(bonus);
                }
                allocate_weighted_credits(&mut credits, weights, total_weight, distributable)?;
            }
        }

        let estimated_credit = credits.get(address).copied().unwrap_or(0);
        if estimated_credit == 0 {
            continue;
        }

        estimate.estimated_pending = estimate.estimated_pending.saturating_add(estimated_credit);
        let confirmations = chain_height.saturating_sub(block.height);
        estimate.blocks.push(MinerPendingBlockEstimate {
            height: block.height,
            hash: block.hash.clone(),
            reward: block.reward,
            estimated_credit,
            confirmations_remaining: required_confirmations.saturating_sub(confirmations),
            timestamp: block.timestamp,
        });
    }

    estimate.blocks.sort_by(|a, b| b.height.cmp(&a.height));
    Ok(estimate)
}

fn allocate_weighted_credits(
    credits: &mut HashMap<String, u64>,
    weights: HashMap<String, u64>,
    total_weight: u64,
    amount: u64,
) -> anyhow::Result<()> {
    if total_weight == 0 || amount == 0 {
        return Ok(());
    }

    let mut weighted = weights.into_iter().collect::<Vec<(String, u64)>>();
    weighted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut distributed = 0u64;
    for (destination, weight) in &weighted {
        let share = ((amount as u128) * (*weight as u128) / (total_weight as u128)) as u64;
        if share == 0 {
            continue;
        }
        credit_address(credits, destination, share)?;
        distributed = distributed.saturating_add(share);
    }

    let remainder = amount.saturating_sub(distributed);
    if remainder > 0 {
        if let Some((destination, _)) = weighted.first() {
            credit_address(credits, destination, remainder)?;
        }
    }

    Ok(())
}

fn credit_address(
    credits: &mut HashMap<String, u64>,
    address: &str,
    amount: u64,
) -> anyhow::Result<()> {
    if amount == 0 {
        return Ok(());
    }
    let destination = address.trim();
    if destination.is_empty() {
        return Ok(());
    }
    let entry = credits.entry(destination.to_string()).or_default();
    *entry = entry
        .checked_add(amount)
        .ok_or_else(|| anyhow::anyhow!("credit overflow"))?;
    Ok(())
}

fn pending_balance_note(
    hashrate: f64,
    total_accepted: u64,
    pending_total: u64,
    estimated_blocks: usize,
) -> Option<String> {
    if pending_total > 0 {
        return None;
    }
    if hashrate <= 0.0 && total_accepted == 0 {
        return None;
    }
    if estimated_blocks > 0 {
        return Some(
            "Estimated pending from recent blocks rounds to 0 for this address right now; this can change as more shares and blocks arrive."
                .to_string(),
        );
    }
    Some(
        "Pending stays at 0 until the pool finds and credits blocks. Your accepted shares still count toward future block rewards while they remain in the payout window."
            .to_string(),
    )
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
    max_items: Option<usize>,
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
    if let Some(max_items) = max_items {
        rounds.truncate(max_items);
    }
    Ok(rounds)
}

fn compute_luck_details_for_hashes(
    store: &PoolStore,
    mut blocks: Vec<crate::db::DbBlock>,
    target_hashes: &HashSet<String>,
) -> anyhow::Result<HashMap<String, LuckRoundResponse>> {
    if target_hashes.is_empty() || blocks.len() < 2 {
        return Ok(HashMap::new());
    }

    blocks.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut rounds = HashMap::<String, LuckRoundResponse>::with_capacity(target_hashes.len());
    for pair in blocks.windows(2) {
        let prev = &pair[0];
        let current = &pair[1];
        if !target_hashes.contains(current.hash.as_str()) {
            continue;
        }

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

        rounds.insert(
            current.hash.clone(),
            LuckRoundResponse {
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
            },
        );

        if rounds.len() == target_hashes.len() {
            break;
        }
    }

    Ok(rounds)
}

fn block_page_item_response(
    block: DbBlock,
    luck: Option<&LuckRoundResponse>,
) -> BlockPageItemResponse {
    BlockPageItemResponse {
        height: block.height,
        hash: block.hash,
        difficulty: block.difficulty,
        finder: block.finder,
        finder_worker: block.finder_worker,
        reward: block.reward,
        timestamp: block.timestamp,
        confirmed: block.confirmed,
        orphaned: block.orphaned,
        paid_out: block.paid_out,
        effort_pct: luck.map(|row| row.effort_pct),
        duration_seconds: luck.map(|row| row.duration_seconds),
        timer_effort_pct: luck.map(|row| row.timer_effort_pct),
        effort_band: luck.map(|row| row.effort_band.clone()),
    }
}

async fn handle_luck_history(
    Query(query): Query<LuckHistoryQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let (limit, offset) = page_bounds(query.limit, query.offset);
    let store = Arc::clone(&state.store);

    let (items, total) = match tokio::task::spawn_blocking(move || {
        let blocks = store.get_all_blocks()?;
        let rounds = compute_luck_history(&store, blocks, None)?;
        let total = rounds.len();
        let items = rounds
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
        Ok::<_, anyhow::Error>((items, total))
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading luck history", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading luck history",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    let returned = items.len();

    Json(PagedResponse {
        items,
        page: PageMeta {
            limit,
            offset,
            returned,
            total,
        },
    })
    .into_response()
}

async fn handle_public_payouts(
    Query(query): Query<PayoutsQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let (limit, offset) = page_bounds(query.limit, query.offset);
    let sort = match query.sort.as_deref().map(str::trim) {
        Some("time_asc") => "time_asc",
        Some("amount_desc") => "amount_desc",
        Some("amount_asc") => "amount_asc",
        _ => "time_desc",
    };

    let store = Arc::clone(&state.store);
    let (batches, total) = match tokio::task::spawn_blocking(move || {
        store.get_public_payout_batches_page(sort, limit as i64, offset as i64)
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

    let items = batches
        .into_iter()
        .map(|batch: PublicPayoutBatch| PublicPayout {
            total_amount: batch.total_amount,
            total_fee: batch.total_fee,
            recipient_count: batch.recipient_count,
            tx_hashes: batch.tx_hashes,
            timestamp: batch.timestamp,
        })
        .collect::<Vec<_>>();
    let returned = items.len();

    Json(PagedResponse {
        items,
        page: PageMeta {
            limit,
            offset,
            returned,
            total: total as usize,
        },
    })
    .into_response()
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

    let (limit, offset) = page_bounds(query.limit, query.offset);
    let sort = match query.sort.as_deref().map(str::trim) {
        Some("time_asc") => "time_asc",
        Some("amount_desc") => "amount_desc",
        Some("amount_asc") => "amount_asc",
        _ => "time_desc",
    };
    let address = non_empty(&query.address).map(str::to_string);
    let tx_hash = non_empty(&query.tx_hash).map(str::to_string);

    let store = Arc::clone(&state.store);
    let (items, total) = match tokio::task::spawn_blocking(move || {
        store.get_payouts_page(
            address.as_deref(),
            tx_hash.as_deref(),
            sort,
            limit as i64,
            offset as i64,
        )
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
    let returned = items.len();

    Json(PagedResponse {
        items,
        page: PageMeta {
            limit,
            offset,
            returned,
            total: total as usize,
        },
    })
    .into_response()
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

    let (limit, offset) = page_bounds(query.limit, query.offset);
    let sort = match query.sort.as_deref().map(str::trim) {
        Some("time_asc") => "time_asc",
        Some("amount_desc") => "amount_desc",
        Some("amount_asc") => "amount_asc",
        Some("height_asc") => "height_asc",
        Some("height_desc") => "height_desc",
        _ => "time_desc",
    };
    let fee_address = non_empty(&query.fee_address).map(str::to_string);

    let store = Arc::clone(&state.store);
    let (total_collected, fees, total) = match tokio::task::spawn_blocking(
        move || -> anyhow::Result<(u64, Vec<PoolFeeEvent>, u64)> {
            let total_collected = store.get_total_pool_fees()?;
            let (fees, total) = store.get_pool_fees_page(
                fee_address.as_deref(),
                sort,
                limit as i64,
                offset as i64,
            )?;
            Ok((total_collected, fees, total))
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
    let returned = fees.len();

    Json(serde_json::json!({
        "total_collected": total_collected,
        "recent": PagedResponse {
            items: fees,
            page: PageMeta {
                limit,
                offset,
                returned,
                total: total as usize,
            },
        },
    }))
    .into_response()
}

impl ApiState {
    pub async fn sample_status(&self) {
        let _ = self.daemon_health().await;
    }

    async fn cached_pending_estimate_for_miner(
        &self,
        address: &str,
        chain_height: u64,
    ) -> anyhow::Result<MinerPendingEstimate> {
        {
            let cache = self.miner_pending_estimate_cache.lock();
            if let Some(entry) = cache.get(address) {
                if entry.chain_height == chain_height
                    && entry.updated_at.elapsed() < MINER_PENDING_ESTIMATE_CACHE_TTL
                {
                    return Ok(entry.value.clone());
                }
            }
        }

        let store = Arc::clone(&self.store);
        let cfg = self.config.clone();
        let addr = address.to_string();
        let now = SystemTime::now();
        let estimate = tokio::task::spawn_blocking(move || {
            estimate_unconfirmed_pending_for_miner(&store, &addr, &cfg, now, chain_height)
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        let mut cache = self.miner_pending_estimate_cache.lock();
        if cache.len() >= MINER_PENDING_ESTIMATE_CACHE_MAX_ENTRIES {
            cache.retain(|_, entry| entry.updated_at.elapsed() < MINER_PENDING_ESTIMATE_CACHE_TTL);
            if cache.len() >= MINER_PENDING_ESTIMATE_CACHE_MAX_ENTRIES {
                cache.clear();
            }
        }
        cache.insert(
            address.to_string(),
            MinerPendingEstimateCache {
                updated_at: Instant::now(),
                chain_height,
                value: estimate.clone(),
            },
        );

        Ok(estimate)
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
                let luck_history = compute_luck_history(&store, blocks, Some(16))?;

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

    async fn rejection_analytics_snapshot(
        &self,
        window: Duration,
    ) -> anyhow::Result<RejectionAnalyticsSnapshot> {
        let mut snapshot = self.stats.rejection_analytics(window);
        let since = SystemTime::now().checked_sub(window).unwrap_or(UNIX_EPOCH);
        let store = Arc::clone(&self.store);
        let (accepted, rejected, total_rejected, by_reason, mut totals_by_reason) =
            tokio::task::spawn_blocking(move || {
                let (accepted, rejected) = store.share_outcome_counts_since(since)?;
                let total_rejected = store.total_rejected_share_count()?;
                let by_reason = store.rejection_reason_counts_since(since)?;
                let totals_by_reason = store.total_rejection_reason_counts()?;
                Ok::<_, anyhow::Error>((
                    accepted,
                    rejected,
                    total_rejected,
                    by_reason,
                    totals_by_reason,
                ))
            })
            .await
            .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        snapshot.accepted = accepted;
        snapshot.rejected = rejected;
        snapshot.total_rejected = total_rejected;
        snapshot.by_reason = by_reason;
        let classified_total: u64 = totals_by_reason.iter().map(|item| item.count).sum();
        if total_rejected > classified_total {
            let missing = total_rejected - classified_total;
            if let Some(item) = totals_by_reason
                .iter_mut()
                .find(|item| item.reason == "legacy / unknown")
            {
                item.count = item.count.saturating_add(missing);
            } else {
                totals_by_reason.push(RejectionReasonCount {
                    reason: "legacy / unknown".to_string(),
                    count: missing,
                });
            }
            totals_by_reason
                .sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
        }
        snapshot.totals_by_reason = totals_by_reason;
        let denom = accepted.saturating_add(rejected);
        snapshot.rejection_rate_pct = if denom == 0 {
            0.0
        } else {
            (rejected as f64 / denom as f64) * 100.0
        };

        Ok(snapshot)
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
            let (confirmed_blocks, orphaned_blocks, _pending_blocks) =
                store.get_block_status_counts()?;
            Ok(DbTotals {
                total_shares: store.get_total_share_count()?,
                total_blocks: store.get_block_count()?,
                confirmed_blocks,
                orphaned_blocks,
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
    payouts_len: usize,
) -> bool {
    shares_len > 0
        || balance_pending > 0
        || balance_paid > 0
        || has_pending_payout
        || payouts_len > 0
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
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use crate::config::Config;
    use crate::db::DbBlock;
    use crate::engine::{ShareRecord, ShareStore};
    use crate::store::PoolStore;

    use super::{
        block_page_item_response, compute_luck_details_for_hashes, compute_luck_history,
        contains_ci, daemon_debug_log_path, daemon_log_commands,
        estimate_unconfirmed_pending_for_miner, estimated_block_reward,
        hashrate_from_stats_with_miner_ramp, hashrate_from_stats_with_warmup,
        hydrate_provisional_block_reward, miner_has_activity, page_bounds, pending_balance_note,
        rejection_window_duration, share_limit, sort_workers_for_miner, trim_log_line,
        worker_hashrate_by_name, DAEMON_LOG_LINE_LIMIT, HASHRATE_BRAND_NEW_MIN_WINDOW,
        HASHRATE_WARMUP_WINDOW, HASHRATE_WINDOW,
    };

    fn test_store() -> Arc<PoolStore> {
        let path = std::env::temp_dir().join(format!(
            "blocknet-pool-api-test-{}.sqlite",
            rand::random::<u64>()
        ));
        PoolStore::open_sqlite(path.to_str().expect("path")).expect("open sqlite store")
    }

    #[test]
    fn miner_activity_detects_share_history() {
        assert!(miner_has_activity(1, 0, 0, false, 0));
    }

    #[test]
    fn miner_activity_detects_balance_and_pending() {
        assert!(miner_has_activity(0, 1, 0, false, 0));
        assert!(miner_has_activity(0, 0, 1, false, 0));
        assert!(miner_has_activity(0, 0, 0, true, 0));
        assert!(miner_has_activity(0, 0, 0, false, 1));
        assert!(!miner_has_activity(0, 0, 0, false, 0));
    }

    #[test]
    fn pending_balance_note_explains_zero_while_hashing() {
        let note = pending_balance_note(150.0, 20, 0, 0);
        assert!(note.is_some());
        assert!(note
            .expect("note")
            .contains("Pending stays at 0 until the pool finds and credits blocks"));
        assert!(pending_balance_note(0.0, 0, 0, 0).is_none());
        assert!(pending_balance_note(150.0, 20, 10, 1).is_none());
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_matches_weighted_split() {
        let store = test_store();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;

        let base = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let block_ts = base + Duration::from_secs(120);
        store
            .add_share(ShareRecord {
                job_id: "j-1".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                reject_reason: None,
                created_at: base,
            })
            .expect("add share a");
        store
            .add_share(ShareRecord {
                job_id: "j-1".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(10),
            })
            .expect("add share b");
        store
            .add_block(&DbBlock {
                height: 99,
                hash: "blk-99".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
            })
            .expect("add unconfirmed block");

        let estimate = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            block_ts + Duration::from_secs(1),
            100,
        )
        .expect("estimate");
        assert_eq!(estimate.estimated_pending, 500);
        assert_eq!(estimate.blocks.len(), 1);
        assert_eq!(estimate.blocks[0].height, 99);
        assert_eq!(estimate.blocks[0].estimated_credit, 500);
        assert_eq!(estimate.blocks[0].confirmations_remaining, 59);
    }

    #[test]
    fn page_bounds_clamps_limits_and_offsets() {
        assert_eq!(page_bounds(None, None), (25, 0));
        assert_eq!(page_bounds(Some(0), Some(2)), (1, 2));
        assert_eq!(page_bounds(Some(5_000), Some(2_000_000)), (200, 1_000_000));
    }

    #[test]
    fn compute_luck_history_returns_all_rounds_and_can_truncate() {
        let store = test_store();
        let base = UNIX_EPOCH + Duration::from_secs(2_000_000);

        for (job_id, created_at, difficulty) in [
            ("job-1", base + Duration::from_secs(10), 40_u64),
            ("job-1", base + Duration::from_secs(20), 60_u64),
            ("job-2", base + Duration::from_secs(70), 100_u64),
        ] {
            store
                .add_share(ShareRecord {
                    job_id: job_id.to_string(),
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty,
                    nonce: difficulty,
                    status: "verified",
                    was_sampled: true,
                    block_hash: None,
                    reject_reason: None,
                    created_at,
                })
                .expect("add share");
        }

        for (height, hash, timestamp) in [
            (100_u64, "blk-100", base),
            (101_u64, "blk-101", base + Duration::from_secs(60)),
            (102_u64, "blk-102", base + Duration::from_secs(120)),
        ] {
            store
                .add_block(&DbBlock {
                    height,
                    hash: hash.to_string(),
                    difficulty: 100,
                    finder: "miner-a".to_string(),
                    finder_worker: "wa".to_string(),
                    reward: 1_000,
                    timestamp,
                    confirmed: true,
                    orphaned: false,
                    paid_out: false,
                })
                .expect("add block");
        }

        let blocks = store.get_all_blocks().expect("load blocks");
        let full = compute_luck_history(&store, blocks.clone(), None).expect("full luck history");
        assert_eq!(full.len(), 2);
        assert_eq!(full[0].block_height, 102);
        assert_eq!(full[0].round_work, 100);
        assert!((full[0].effort_pct - 100.0).abs() < f64::EPSILON);
        assert_eq!(full[1].block_height, 101);
        assert_eq!(full[1].round_work, 100);

        let truncated =
            compute_luck_history(&store, blocks, Some(1)).expect("truncated luck history");
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].block_height, 102);
    }

    #[test]
    fn compute_luck_details_for_hashes_returns_only_requested_rows() {
        let store = test_store();
        let base = UNIX_EPOCH + Duration::from_secs(3_000_000);

        for (job_id, created_at, difficulty) in [
            ("job-1", base + Duration::from_secs(10), 40_u64),
            ("job-1", base + Duration::from_secs(20), 60_u64),
            ("job-2", base + Duration::from_secs(70), 100_u64),
        ] {
            store
                .add_share(ShareRecord {
                    job_id: job_id.to_string(),
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty,
                    nonce: difficulty,
                    status: "verified",
                    was_sampled: true,
                    block_hash: None,
                    reject_reason: None,
                    created_at,
                })
                .expect("add share");
        }

        for (height, hash, timestamp) in [
            (100_u64, "blk-100", base),
            (101_u64, "blk-101", base + Duration::from_secs(60)),
            (102_u64, "blk-102", base + Duration::from_secs(120)),
        ] {
            store
                .add_block(&DbBlock {
                    height,
                    hash: hash.to_string(),
                    difficulty: 100,
                    finder: "miner-a".to_string(),
                    finder_worker: "wa".to_string(),
                    reward: 1_000,
                    timestamp,
                    confirmed: true,
                    orphaned: false,
                    paid_out: false,
                })
                .expect("add block");
        }

        let details = compute_luck_details_for_hashes(
            &store,
            store.get_all_blocks().expect("blocks"),
            &HashSet::from([String::from("blk-102")]),
        )
        .expect("details");

        assert_eq!(details.len(), 1);
        let row = details.get("blk-102").expect("row");
        assert_eq!(row.block_height, 102);
        assert_eq!(row.round_work, 100);

        let response = block_page_item_response(
            store
                .get_block(102)
                .expect("get block")
                .expect("block exists"),
            Some(row),
        );
        assert_eq!(response.effort_pct, Some(100.0));
        assert_eq!(response.duration_seconds, Some(60));
        assert!(response.effort_band.is_some());
    }

    #[test]
    fn daemon_debug_log_path_uses_daemon_data_dir() {
        let cfg = Config {
            daemon_data_dir: "/var/lib/blocknet/data".to_string(),
            ..Config::default()
        };
        assert_eq!(
            daemon_debug_log_path(&cfg).to_string_lossy(),
            "/var/lib/blocknet/data/debug.log"
        );
    }

    #[test]
    fn daemon_log_commands_include_journal_and_tail() {
        let cfg = Config {
            daemon_data_dir: "/var/lib/blocknet/data".to_string(),
            ..Config::default()
        };
        let commands = daemon_log_commands(&cfg, 200, true);
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].program, "journalctl");
        assert!(commands[0].args.iter().any(|a| a == "-q"));
        assert!(commands[0].args.iter().any(|a| a == "-a"));
        assert!(commands[0].args.iter().any(|a| a == "-f"));
        assert_eq!(commands[1].program, "tail");
        assert!(commands[1].args.iter().any(|a| a == "-F"));
        assert!(commands[1]
            .args
            .iter()
            .any(|a| a == "/var/lib/blocknet/data/debug.log"));
    }

    #[test]
    fn trim_log_line_caps_size() {
        let input = "x".repeat(DAEMON_LOG_LINE_LIMIT + 100);
        let trimmed = trim_log_line(&input);
        assert!(trimmed.len() < input.len());
        assert!(trimmed.contains("...[truncated]"));
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
    fn rejection_window_duration_parses_supported_ranges() {
        assert_eq!(rejection_window_duration(None).as_secs(), 3600);
        assert_eq!(rejection_window_duration(Some("1h")).as_secs(), 3600);
        assert_eq!(rejection_window_duration(Some("24h")).as_secs(), 24 * 3600);
        assert_eq!(
            rejection_window_duration(Some("7d")).as_secs(),
            7 * 24 * 3600
        );
        assert_eq!(
            rejection_window_duration(Some(" 24h ")).as_secs(),
            24 * 3600
        );
        assert_eq!(rejection_window_duration(Some("bad")).as_secs(), 3600);
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
    fn miner_hashrate_brand_new_uses_shorter_floor() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let first = now - Duration::from_secs(10);
        let hr = hashrate_from_stats_with_miner_ramp(
            600,
            1,
            Some(first),
            Some(first),
            HASHRATE_WINDOW,
            HASHRATE_WARMUP_WINDOW,
            HASHRATE_BRAND_NEW_MIN_WINDOW,
            now,
        );
        assert!((hr - 10.0).abs() < 1e-9);
    }

    #[test]
    fn miner_hashrate_stale_single_share_uses_share_age() {
        let now = UNIX_EPOCH + Duration::from_secs(2_000);
        let last_share = now - Duration::from_secs(1_200);
        let hr = hashrate_from_stats_with_miner_ramp(
            600,
            1,
            Some(last_share),
            Some(last_share),
            HASHRATE_WINDOW,
            HASHRATE_WARMUP_WINDOW,
            HASHRATE_BRAND_NEW_MIN_WINDOW,
            now,
        );
        assert!((hr - 0.5).abs() < 1e-9);
    }

    #[test]
    fn miner_hashrate_brand_new_uses_observed_window_with_two_shares() {
        let now = UNIX_EPOCH + Duration::from_secs(2_000);
        let first = now - Duration::from_secs(60);
        let hr = hashrate_from_stats_with_miner_ramp(
            600,
            2,
            Some(first),
            Some(now),
            HASHRATE_WINDOW,
            HASHRATE_WARMUP_WINDOW,
            HASHRATE_BRAND_NEW_MIN_WINDOW,
            now,
        );
        assert!((hr - 10.0).abs() < 1e-9);
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
