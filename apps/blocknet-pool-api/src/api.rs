use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{ConnectInfo, FromRequestParts, Path, Query, State};
use axum::http::header;
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::background_read::{BackgroundReads, ReadResult};
use crate::config::Config;
use crate::protect::{client_key, CacheLookup, ClientIpPolicy, PublicReadCache, WindowedCounter};
use crate::ui::{
    apply_security_headers, handle_app_fallback, handle_favicon_svg, handle_robots_txt,
    handle_sitemap_xml, handle_ui, handle_ui_asset,
};
use pool_common::db::{
    allocate_proportional_fees, ActiveVerificationHold, AddressRiskState, Balance, DbBlock,
    DbLuckRound, DbShare, MonitorHeartbeat, MonitorIncident, PendingPayout,
};
use pool_recovery::{RecoveryAgentClient, RecoveryInstanceId, RecoveryOperation, RecoveryStatus};
use pool_runtime::hashrate::{
    from_stats_or_window_floor as hashrate_from_stats_or_window_floor,
    from_stats_with_miner_ramp as hashrate_from_stats_with_miner_ramp,
    from_stats_with_warmup as hashrate_from_stats_with_warmup, HashrateStatsInput,
    MinerHashrateRamp,
};
use pool_runtime::jobs::JobManager;
use pool_runtime::node::{NodeClient, WalletBalance};
use pool_runtime::payout::{
    is_share_payout_eligible, recover_share_window_by_replay, reward_window_end, PayoutTrustPolicy,
};
use pool_runtime::pgdb::{
    with_statement_timeout, BalanceSourceSummary, ConfirmedPayoutImportRecipient,
    ConfirmedPayoutImportTx, ManualCompletedPayoutResolutionKind, MonitorUptimeSummary,
    ShareWindowAddressPreview, UnreconciledCompletedPayoutRow, WorkerHashrateStats,
};
use pool_runtime::rewards::estimated_block_reward;
use pool_runtime::service_state::{
    PersistedRuntimeSnapshot, SubmitRuntimeSnapshot, LIVE_RUNTIME_SNAPSHOT_META_KEY,
};
use pool_runtime::stats::{RejectionAnalyticsSnapshot, RejectionReasonCount};
use pool_runtime::store::PoolStore;
use pool_runtime::telemetry::{
    ApiPerformanceSnapshot, NamedCacheCounterTracker, NamedTimedOperationTracker,
};
use pool_runtime::validation::{is_verified_share_status, ValidationSnapshot};
use pool_runtime::wallet_send_journal::{
    aggregate_wallet_send_recipients, decode_wallet_send_body, WalletSendIdempotencyJournal,
};

const DB_TOTALS_CACHE_TTL: Duration = Duration::from_secs(15);
const LIVE_RUNTIME_SNAPSHOT_CACHE_TTL: Duration = Duration::from_secs(5);
const EXPLORER_HASHRATE_SAMPLE_COUNT: usize = 10;
const NETWORK_HASHRATE_CACHE_RETRY_TTL: Duration = Duration::from_secs(5);
const DEFAULT_PAGE_LIMIT: usize = 25;
const MAX_PAGE_LIMIT: usize = 200;
const HASHRATE_WINDOW: Duration = Duration::from_secs(60 * 60);
const HASHRATE_WARMUP_WINDOW: Duration = Duration::from_secs(5 * 60);
const HASHRATE_BRAND_NEW_MIN_WINDOW: Duration = Duration::from_secs(60);
const ROUND_TARGET_SECONDS: f64 = 300.0;
const INSIGHTS_CACHE_TTL: Duration = Duration::from_secs(30);
const REJECTION_ANALYTICS_CACHE_TTL: Duration = Duration::from_secs(30);
const STATS_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(15);
const CHAIN_AWARE_ORPHAN_LOOKBACK_BLOCKS: i64 = 1024;
const MAX_CONCURRENT_CHAIN_READS: usize = 16;
const MINER_BALANCE_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(5);
const MINER_DETAIL_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(10);
const MINER_BALANCE_RESPONSE_CACHE_MAX_ENTRIES: usize = 2048;
const MINER_DETAIL_RESPONSE_CACHE_MAX_ENTRIES: usize = 1024;
// Reserve database capacity for balances, stats, health, and background work.
const MAX_CONCURRENT_MINER_DETAIL_LOADS: usize = 2;
// Absorb short cold-cache bursts without admitting more simultaneous DB work.
const MINER_DETAIL_ADMISSION_WAIT: Duration = Duration::from_millis(50);
const PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(5);
const PUBLIC_TELEMETRY_STATS_RATE_LIMIT: u32 = 12;
const PUBLIC_TELEMETRY_MINER_RATE_LIMIT: u32 = 12;
const PUBLIC_TELEMETRY_RATE_LIMIT_RETRY_AFTER_SECS: u64 = 5;
// Expensive public reads (history, status, blocks, luck, payouts) are served
// from a shared response cache. Only requests that would reach the database
// count against this per-client budget; cache hits are never limited.
const PUBLIC_READ_MISS_RATE_LIMIT: u32 = 20;
const PUBLIC_READ_CACHE_MAX_ENTRIES: usize = 8192;
const PUBLIC_READ_CACHE_MAX_BYTES: usize = 32 << 20;
// Past their TTL, entries remain available as a fallback when a client is
// rate limited, the read budget is saturated, or the database read fails.
const PUBLIC_READ_STALE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_CONCURRENT_PUBLIC_READ_LOADS: usize = 2;
const PUBLIC_READ_ADMISSION_WAIT: Duration = Duration::from_secs(2);
const PUBLIC_READ_COALESCE_WAIT: Duration = Duration::from_secs(20);
const PUBLIC_STATUS_CACHE_TTL: Duration = Duration::from_secs(15);
const PUBLIC_BLOCKS_CACHE_TTL: Duration = Duration::from_secs(15);
const PUBLIC_LUCK_CACHE_TTL: Duration = Duration::from_secs(15);
const PUBLIC_PAYOUTS_CACHE_TTL: Duration = Duration::from_secs(15);
const PUBLIC_STATS_HISTORY_CACHE_TTL: Duration = Duration::from_secs(30);
// Wrong admin keys per client before protected routes answer 429 until the
// window ends. Requests without a key do not count, and a correct key clears
// the client's failures, so the operator is never locked out for long.
const ADMIN_AUTH_FAILURE_LIMIT: u32 = 20;
const ADMIN_AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(5 * 60);
const ADMIN_AUTH_FAILURE_MAX_CLIENTS: usize = 16_384;
// Signed monitor events must carry a `checked_at` within this distance of our
// clock and newer than the last accepted event, so captured requests cannot
// be replayed.
const MONITOR_INGEST_MAX_CLOCK_SKEW_SECS: u64 = 5 * 60;
const PUBLIC_TELEMETRY_RATE_LIMIT_MAX_BUCKETS: usize = 16_384;
const MINER_DETAIL_SHARE_LIMIT: i64 = 20;
const MINER_PAYOUT_HISTORY_LIMIT: i64 = 50;
const DEFAULT_DAEMON_LOG_TAIL: usize = 200;
const MAX_DAEMON_LOG_TAIL: usize = 2000;
const DAEMON_LOG_LINE_LIMIT: usize = 8192;
const DAEMON_LOG_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const LOCAL_MONITOR_HEALTH_TTL: Duration = Duration::from_secs(30);
const PUBLIC_MONITOR_HEALTH_TTL: Duration = Duration::from_secs(10 * 60);
const MINER_PENDING_ESTIMATE_REFRESH_AFTER: Duration = Duration::from_secs(15);
const MINER_PENDING_ESTIMATE_STALE_TTL: Duration = Duration::from_secs(60);
const MINER_PENDING_ESTIMATE_HOT_WINDOW: Duration = Duration::from_secs(60);
const PENDING_ESTIMATE_SNAPSHOT_WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const PENDING_ESTIMATE_SNAPSHOT_MAX_WAIT_ATTEMPTS: u32 = 3;
const LOCAL_MONITOR_SOURCE: &str = "local";
const CLOUDFLARE_MONITOR_SOURCE: &str = "cloudflare";
const PERF_SUCCESS_LOG_SAMPLE_RATE: u64 = 128;
const CHECKPOINTS_FILENAME: &str = "checkpoints.dat";
const MAX_CHECKPOINTS_FILE_BYTES: u64 = 32 << 20;
const CHECKPOINTS_CACHE_CONTROL: &str = "public, max-age=60";
const POOL_YIELD_WINDOW: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MIN_POOL_YIELD_SAMPLE_SIZE: u64 = 30;

fn db_miner_hashrate(store: &PoolStore, address: &str) -> f64 {
    let since = SystemTime::now()
        .checked_sub(HASHRATE_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_for_miner(address, since)
    else {
        return 0.0;
    };
    hashrate_from_stats_with_miner_ramp(
        HashrateStatsInput {
            total_diff,
            count,
            oldest,
            newest,
        },
        MinerHashrateRamp {
            smoothing_window: HASHRATE_WINDOW,
            warmup_window: HASHRATE_WARMUP_WINDOW,
            brand_new_min_window: HASHRATE_BRAND_NEW_MIN_WINDOW,
            now: SystemTime::now(),
        },
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

fn hydrate_provisional_block_reward(block: &mut DbBlock) {
    if !block.confirmed && !block.orphaned && block.reward == 0 {
        block.reward = estimated_block_reward(block.height);
    }
}

fn worker_hashrate_by_name(
    miner_hashrate: f64,
    worker_hashrate_raw: WorkerHashrateStats,
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

fn active_worker_cutoff_unix(now: SystemTime, active_cutoff: Duration) -> i64 {
    let now_unix = i64::try_from(system_time_to_unix_secs(now)).unwrap_or(i64::MAX);
    let cutoff_secs = i64::try_from(active_cutoff.as_secs()).unwrap_or(i64::MAX);
    now_unix.saturating_sub(cutoff_secs)
}

fn filter_active_workers_for_miner(
    workers: Vec<(String, u64, u64, u64, i64)>,
    now: SystemTime,
    active_cutoff: Duration,
) -> Vec<(String, u64, u64, u64, i64)> {
    let active_cutoff_unix = active_worker_cutoff_unix(now, active_cutoff);
    workers
        .into_iter()
        .filter(|(_, _, _, _, last_share_ts)| *last_share_ts >= active_cutoff_unix)
        .collect()
}

const WORKER_VERSION_CURRENT: &str = "current";
const WORKER_VERSION_OUTDATED: &str = "outdated";
const WORKER_VERSION_UNKNOWN: &str = "unknown";

fn parse_version_triple(raw: &str) -> Option<(u64, u64, u64)> {
    let trimmed = raw.trim().trim_start_matches('v');
    let core = trimmed
        .split_once(['-', '+'])
        .map(|(core, _)| core)
        .unwrap_or(trimmed);
    let mut parts = core.split('.');
    let major = parts.next()?.trim().parse().ok()?;
    let minor = parts.next()?.trim().parse().ok()?;
    let patch = parts.next()?.trim().parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

/// Derives a worker's miner-version status against the configured latest
/// release:
/// - no reported version → "unknown" (seine older than 0.2.14 or a non-seine
///   client; the two are indistinguishable, so never claim a specific client)
/// - reported but unparseable → "outdated" (something answered, but not with a
///   version we can compare, so still worth a nudge)
/// - reported and >= latest → "current"
/// - if the configured latest version itself does not parse, everything is
///   "unknown" rather than nudging against a broken baseline
fn worker_version_status(reported: Option<&str>, latest: &str) -> &'static str {
    let Some(latest) = parse_version_triple(latest) else {
        return WORKER_VERSION_UNKNOWN;
    };
    match reported {
        None => WORKER_VERSION_UNKNOWN,
        Some(raw) => match parse_version_triple(raw) {
            Some(version) if version >= latest => WORKER_VERSION_CURRENT,
            _ => WORKER_VERSION_OUTDATED,
        },
    }
}

fn sort_workers_for_miner(
    mut workers: Vec<(String, u64, u64, u64, i64)>,
    hashrate_by_name: &HashMap<String, f64>,
    now: SystemTime,
    active_cutoff: Duration,
) -> Vec<(String, u64, u64, u64, i64)> {
    let active_cutoff_unix = active_worker_cutoff_unix(now, active_cutoff);

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
pub(crate) struct ApiState {
    miner_release: crate::miner_release::MinerRelease,
    site_metrics: Arc<crate::site_metrics::SiteMetrics>,
    config: Config,
    store: Arc<PoolStore>,
    jobs: Arc<JobManager>,
    node: Arc<NodeClient>,
    db_totals_cache: Arc<Mutex<DbTotalsCache>>,
    network_hashrate_cache: Arc<Mutex<NetworkHashrateCache>>,
    insights_cache: Arc<Mutex<InsightsCache>>,
    rejection_analytics_cache: Arc<Mutex<RejectionAnalyticsCache>>,
    stats_response_cache: Arc<Mutex<StatsResponseCache>>,
    cache_refresh_locks: Arc<CacheRefreshLocks>,
    pending_estimate_snapshot_cache: Arc<Mutex<PendingEstimateSnapshotCache>>,
    pending_estimate_snapshot_notify: Arc<Notify>,
    miner_balance_response_cache: Arc<Mutex<MinerBalanceResponseCache>>,
    miner_detail_response_cache: Arc<Mutex<MinerDetailResponseCache>>,
    miner_detail_loads: Arc<Semaphore>,
    reward_breakdown_reads: Arc<BackgroundReads>,
    public_telemetry_rate_limiter: Arc<Mutex<PublicTelemetryRateLimiter>>,
    client_ip_policy: Arc<ClientIpPolicy>,
    public_read_cache: Arc<Mutex<PublicReadCache>>,
    public_read_loads: Arc<Semaphore>,
    checkpoint_file_cache: Arc<CheckpointFileCache>,
    admin_auth_failures: Arc<Mutex<WindowedCounter>>,
    /// Newest accepted Cloudflare `checked_at` (only the `public_http` probe
    /// exists). `None` until seeded from the latest stored heartbeat.
    monitor_ingest_last_checked_at: Arc<tokio::sync::Mutex<Option<u64>>>,
    performance: Arc<ApiPerformanceTracker>,
    recovery: Arc<RecoveryAgentClient>,
    live_runtime_snapshot_cache: Arc<Mutex<LiveRuntimeSnapshotCache>>,
    started_at: Instant,
}

#[derive(Clone, Copy, Default)]
struct DbTotals {
    total_blocks: u64,
    confirmed_blocks: u64,
    orphaned_blocks: u64,
    blocks_30d: u64,
    orphaned_blocks_30d: u64,
    paid_to_miners_total: u64,
}

#[derive(Default)]
struct DbTotalsCache {
    updated_at: Option<Instant>,
    chain_height: Option<u64>,
    totals: DbTotals,
}

#[derive(Serialize)]
struct DaemonHealth {
    reachable: bool,
    chain_height: Option<u64>,
    syncing: Option<bool>,
}

#[derive(Default)]
struct NetworkHashrateCache {
    updated_at: Option<Instant>,
    chain_height: Option<u64>,
    difficulty: Option<u64>,
    hashrate_hps: Option<f64>,
}

type InsightsCache = TimedValueCache<StatsInsightsResponse>;

#[derive(Default)]
struct RejectionAnalyticsCache {
    entries: HashMap<u64, TimedCacheEntry<RejectionAnalyticsSnapshot>>,
}

type StatsResponseCache = TimedValueCache<StatsResponse>;

#[derive(Default)]
struct PendingEstimateSnapshotCache {
    updated_at: Option<Instant>,
    last_requested_at: Option<Instant>,
    chain_height: Option<u64>,
    values: HashMap<String, MinerPendingEstimate>,
    refresh_in_flight: bool,
}

struct TimedCacheEntry<T> {
    updated_at: Instant,
    value: T,
}

struct TimedValueCache<T> {
    entry: Option<TimedCacheEntry<T>>,
}

impl<T: Clone> TimedValueCache<T> {
    fn new() -> Self {
        Self { entry: None }
    }

    fn get(&self, ttl: Duration) -> Option<T> {
        self.entry
            .as_ref()
            .filter(|entry| entry.updated_at.elapsed() < ttl)
            .map(|entry| entry.value.clone())
    }

    fn set(&mut self, value: T) {
        self.entry = Some(TimedCacheEntry {
            updated_at: Instant::now(),
            value,
        });
    }
}

#[derive(Clone, Serialize)]
struct MinerBalancePayload {
    address: String,
    balance: MinerBalanceResponse,
    pending_estimate: MinerPendingEstimate,
}

type MinerBalanceResponseCache = TimedMapCache<MinerBalancePayload>;

#[derive(Clone)]
struct MinerDetailPayload {
    found: bool,
    body: MinerDetailResponse,
}

type MinerDetailResponseCache = TimedMapCache<MinerDetailPayload>;

// Serialize cache checks and refreshes per key. Waiting requests recheck the
// cache after the first loader completes, instead of queuing duplicate database
// work on the blocking executor. Different miners remain independent.
#[derive(Default)]
struct CacheRefreshLocks {
    entries: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

impl CacheRefreshLocks {
    async fn lock(&self, key: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut entries = self.entries.lock();
            if let Some(lock) = entries.get(key).and_then(Weak::upgrade) {
                lock
            } else {
                // Completed/canceled requests must not leave an ever-growing
                // map of miner addresses. Live waiters retain their lock.
                entries.retain(|_, lock| lock.strong_count() > 0);
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entries.insert(key.to_string(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    /// Like [`Self::lock`] but returns `None` instead of waiting when another
    /// request is already refreshing `key`.
    fn try_lock(&self, key: &str) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let lock = {
            let mut entries = self.entries.lock();
            if let Some(lock) = entries.get(key).and_then(Weak::upgrade) {
                lock
            } else {
                entries.retain(|_, lock| lock.strong_count() > 0);
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                entries.insert(key.to_string(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.try_lock_owned().ok()
    }
}

struct TimedMapCache<T> {
    entries: HashMap<String, TimedCacheEntry<T>>,
    last_cleanup_at: Option<Instant>,
}

impl<T: Clone> TimedMapCache<T> {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            last_cleanup_at: None,
        }
    }

    fn get(&mut self, key: &str, ttl: Duration, max_entries: usize) -> Option<T> {
        let now = Instant::now();
        if self
            .last_cleanup_at
            .is_none_or(|last| now.duration_since(last) >= ttl)
        {
            prune_timed_cache_entries(&mut self.entries, ttl, max_entries, now);
            self.last_cleanup_at = Some(now);
        }

        self.entries
            .get(key)
            .filter(|entry| now.duration_since(entry.updated_at) < ttl)
            .map(|entry| entry.value.clone())
    }

    fn insert(&mut self, key: String, value: T, ttl: Duration, max_entries: usize) {
        let now = Instant::now();
        prune_timed_cache_entries(&mut self.entries, ttl, max_entries.saturating_sub(1), now);
        self.entries.insert(
            key,
            TimedCacheEntry {
                updated_at: now,
                value,
            },
        );
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.last_cleanup_at = None;
    }
}

fn prune_timed_cache_entries<T>(
    entries: &mut HashMap<String, TimedCacheEntry<T>>,
    ttl: Duration,
    max_entries: usize,
    now: Instant,
) {
    entries.retain(|_, entry| now.duration_since(entry.updated_at) < ttl);
    if entries.len() <= max_entries {
        return;
    }

    let mut oldest = entries
        .iter()
        .map(|(key, entry)| (key.clone(), entry.updated_at))
        .collect::<Vec<_>>();
    oldest.sort_by_key(|(_, updated_at)| *updated_at);
    let remove_count = oldest.len().saturating_sub(max_entries);
    for (key, _) in oldest.into_iter().take(remove_count) {
        entries.remove(&key);
    }
}

#[derive(Clone, Copy)]
enum PublicTelemetryRouteKind {
    Stats,
    Miner,
    /// Cache misses on expensive public reads.
    PublicRead,
}

impl PublicTelemetryRouteKind {
    fn limit(self) -> u32 {
        match self {
            Self::Stats => PUBLIC_TELEMETRY_STATS_RATE_LIMIT,
            Self::Miner => PUBLIC_TELEMETRY_MINER_RATE_LIMIT,
            Self::PublicRead => PUBLIC_READ_MISS_RATE_LIMIT,
        }
    }
}

impl PublicTelemetryRouteKind {
    fn key(self) -> &'static str {
        match self {
            Self::Stats => "stats",
            Self::Miner => "miner",
            Self::PublicRead => "read",
        }
    }
}

/// Per-client fixed-window limiter (see [`WindowedCounter`]): O(1) per
/// request, with the table dropped once per window.
struct PublicTelemetryRateLimiter {
    counter: WindowedCounter,
}

impl Default for PublicTelemetryRateLimiter {
    fn default() -> Self {
        Self {
            counter: WindowedCounter::new(
                PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW,
                PUBLIC_TELEMETRY_RATE_LIMIT_MAX_BUCKETS,
            ),
        }
    }
}

impl PublicTelemetryRateLimiter {
    fn allow(&mut self, client_key: &str, route: PublicTelemetryRouteKind, now: Instant) -> bool {
        let key = format!("{}:{client_key}", route.key());
        self.counter.try_acquire(&key, route.limit(), now)
    }
}

/// Rate-limit key of the requesting client, resolved by [`ClientIpPolicy`].
/// `None` when the client cannot be identified (trusted proxy without
/// forwarding headers); per-client limits are then skipped.
#[derive(Clone, Debug, Default)]
pub(crate) struct ClientIp(Option<String>);

impl ClientIp {
    fn key(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

#[axum::async_trait]
impl FromRequestParts<ApiState> for ClientIp {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &ApiState,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.client_ip(&parts.headers, &parts.extensions))
    }
}

#[derive(Default)]
struct ApiPerformanceTracker {
    routes: NamedTimedOperationTracker,
    operations: NamedTimedOperationTracker,
    tasks: NamedTimedOperationTracker,
    caches: NamedCacheCounterTracker,
    log_sample_counter: AtomicU64,
}

impl ApiPerformanceTracker {
    fn snapshot(&self) -> ApiPerformanceSnapshot {
        ApiPerformanceSnapshot {
            sampled_at: Some(SystemTime::now()),
            routes: self.routes.snapshot(),
            operations: self.operations.snapshot(),
            tasks: self.tasks.snapshot(),
            caches: self.caches.snapshot(),
            database_reads: Default::default(),
        }
    }

    fn should_sample_success(&self) -> bool {
        self.log_sample_counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(PERF_SUCCESS_LOG_SAMPLE_RATE)
    }
}

type LiveRuntimeSnapshotCache = TimedValueCache<Option<PersistedRuntimeSnapshot>>;

#[derive(Serialize)]
struct StatusIncident {
    id: u64,
    kind: String,
    severity: String,
    started_at: SystemTime,
    duration_seconds: Option<u64>,
    message: String,
    ongoing: bool,
}

#[derive(Serialize)]
struct ServiceHealth {
    observed: bool,
    healthy: bool,
}

#[derive(Serialize)]
struct StatusServices {
    public_http: ServiceHealth,
    api: ServiceHealth,
    stratum: ServiceHealth,
    database: ServiceHealth,
    daemon: ServiceHealth,
}

#[derive(Serialize)]
struct TemplateHealth {
    observed: bool,
    fresh: bool,
    age_seconds: Option<u64>,
    last_refresh_millis: Option<u64>,
}

const TEMPLATE_REFRESH_WARN_AFTER_MILLIS: u64 = 45_000;

#[derive(Serialize)]
struct UptimeWindow {
    label: String,
    sample_count: usize,
    external_sample_count: usize,
    api_up_pct: Option<f64>,
    stratum_up_pct: Option<f64>,
    pool_up_pct: Option<f64>,
    daemon_up_pct: Option<f64>,
    database_up_pct: Option<f64>,
    public_http_up_pct: Option<f64>,
}

#[derive(Serialize)]
struct StatusPageResponse {
    healthy: bool,
    pool_uptime_seconds: u64,
    services: StatusServices,
    daemon: DaemonHealth,
    template: TemplateHealth,
    uptime: Vec<UptimeWindow>,
    incidents: Vec<StatusIncident>,
}

#[derive(Clone, Serialize)]
struct RoundProgressResponse {
    elapsed_seconds: u64,
    effort_pct: Option<f64>,
    expected_block_seconds: Option<f64>,
    timer_effort_pct: Option<f64>,
}

#[derive(Clone, Serialize)]
struct PayoutEtaResponse {
    next_sweep_at: Option<SystemTime>,
    pending_total_amount: u64,
    wallet_spendable: Option<u64>,
    wallet_pending: Option<u64>,
}

#[derive(Serialize)]
struct PoolHashratePointResponse {
    timestamp: SystemTime,
    hashrate: f64,
}

#[derive(Serialize)]
struct MinerHashratePointResponse {
    timestamp: i64,
    hashrate: f64,
}

#[derive(Clone, Serialize)]
struct MinerBalanceResponse {
    pending_confirmed: u64,
    pending_queued: u64,
    paid: u64,
}

#[derive(Clone, Serialize)]
struct MinerPayoutResponse {
    amount: u64,
    fee: u64,
    tx_hash: String,
    timestamp: SystemTime,
    confirmed: bool,
}

#[derive(Clone, Serialize)]
struct MinerShareResponse {
    job_id: String,
    worker: String,
    difficulty: u64,
    status: String,
    created_at: SystemTime,
}

#[derive(Clone, Serialize)]
struct MinerWorkerResponse {
    worker: String,
    hashrate: f64,
    accepted: u64,
    rejected: u64,
    last_share_at: i64,
    miner_version: Option<String>,
    backend: Option<String>,
    version_status: &'static str,
}

#[derive(Clone, Serialize)]
struct MinerDetailResponse {
    shares: Vec<MinerShareResponse>,
    mining_since: Option<SystemTime>,
    hashrate: f64,
    verification_hold: Option<MinerVerificationHold>,
    payouts: Vec<MinerPayoutResponse>,
    workers: Vec<MinerWorkerResponse>,
    blocks_found: u64,
    total_accepted: u64,
    total_rejected: u64,
    latest_miner_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Clone, Serialize)]
struct LuckRoundResponse {
    block_height: u64,
    block_hash: String,
    timestamp: SystemTime,
    effort_pct: f64,
    duration_seconds: u64,
    orphaned: bool,
    confirmed: bool,
}

#[derive(Serialize)]
struct BlockPageItemResponse {
    height: u64,
    hash: String,
    reward: u64,
    timestamp: SystemTime,
    confirmed: bool,
    orphaned: bool,
    effort_pct: Option<f64>,
    duration_seconds: Option<u64>,
}

#[derive(Clone, Serialize)]
struct StatsInsightsResponse {
    round: RoundProgressResponse,
    payout_eta: PayoutEtaResponse,
    avg_effort_pct: Option<f64>,
    luck_history: Vec<LuckRoundResponse>,
    rejections: RejectionAnalyticsSnapshot,
}

#[derive(Clone, Serialize)]
struct CheckpointsMetadataResponse {
    available: bool,
    url: String,
    entries: usize,
    latest_height: Option<u64>,
    latest_hash: Option<String>,
    bytes: Option<u64>,
    sha256: Option<String>,
    updated_at: Option<u64>,
}

struct CheckpointFileSnapshot {
    body: Bytes,
    metadata: CheckpointsMetadataResponse,
}

#[derive(Debug)]
enum CheckpointFileError {
    NotFound,
    TooLarge(u64),
    Invalid(String),
    Io(std::io::Error),
    Internal(String),
}

async fn spawn_blocking_result<F, R>(operation: F) -> anyhow::Result<R>
where
    F: FnOnce() -> anyhow::Result<R> + Send + 'static,
    R: Send + 'static,
{
    join_result(tokio::task::spawn_blocking(operation).await)
}

#[derive(Debug)]
struct TelemetryOverloaded;

impl std::fmt::Display for TelemetryOverloaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("miner telemetry is busy; retry shortly")
    }
}

impl std::error::Error for TelemetryOverloaded {}

async fn limited_blocking_result<F, R>(budget: Arc<Semaphore>, operation: F) -> anyhow::Result<R>
where
    F: FnOnce() -> anyhow::Result<R> + Send + 'static,
    R: Send + 'static,
{
    let permit = tokio::time::timeout(MINER_DETAIL_ADMISSION_WAIT, budget.acquire_owned())
        .await
        .map_err(|_| TelemetryOverloaded)?
        .map_err(|_| TelemetryOverloaded)?;
    spawn_blocking_result(move || {
        // A canceled HTTP request cannot cancel synchronous Postgres work.
        // Keep its slot occupied until the blocking work actually finishes.
        let _permit = permit;
        operation()
    })
    .await
}

fn join_result<R>(result: Result<anyhow::Result<R>, tokio::task::JoinError>) -> anyhow::Result<R> {
    result.map_err(|err| anyhow::anyhow!("join error: {err}"))?
}

pub(crate) async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    state.site_metrics.start(Arc::clone(&state.store));
    let hosted_wallet = crate::hosted_wallet::router(state.config.hosted_wallet.as_ref())?;
    let app_state = state.clone();
    let protected = Router::new()
        .route("/api/miners", get(handle_miners))
        .route("/api/admin/perf", get(handle_admin_perf))
        .route("/api/admin/site-metrics", get(handle_site_metrics))
        .route("/api/admin/balances", get(handle_admin_balances))
        .route(
            "/api/admin/balance-overview",
            get(handle_admin_balance_overview),
        )
        .route(
            "/api/admin/reconciliation/issues",
            get(handle_admin_reconciliation_issues),
        )
        .route(
            "/api/admin/reconciliation/payouts/resolve",
            post(handle_admin_reconciliation_payout_resolution),
        )
        .route(
            "/api/admin/reconciliation/payouts/import-confirmed",
            post(handle_admin_reconciliation_payout_import),
        )
        .route(
            "/api/admin/reconciliation/manual-offsets/apply-live-pending",
            post(handle_admin_reconciliation_manual_offset_apply),
        )
        .route(
            "/api/admin/reconciliation/orphan-blocks/retry-cleanup",
            post(handle_admin_orphaned_block_cleanup_retry),
        )
        .route("/api/admin/shares", get(handle_admin_share_diagnostics))
        .route(
            "/api/admin/blocks/:height/reward-breakdown",
            get(handle_admin_block_reward_breakdown),
        )
        .route("/api/health", get(handle_health))
        .route("/api/admin/recovery/status", get(handle_recovery_status))
        .route(
            "/api/admin/recovery/payouts/pause",
            post(handle_recovery_pause_payouts),
        )
        .route(
            "/api/admin/recovery/payouts/resume",
            post(handle_recovery_resume_payouts),
        )
        .route(
            "/api/admin/recovery/inactive/start-sync",
            post(handle_recovery_start_inactive_sync),
        )
        .route(
            "/api/admin/recovery/inactive/rebuild-wallet",
            post(handle_recovery_rebuild_inactive_wallet),
        )
        .route("/api/admin/recovery/cutover", post(handle_recovery_cutover))
        .route(
            "/api/admin/recovery/inactive/purge-resync",
            post(handle_recovery_purge_inactive_daemon),
        )
        .route(
            "/api/admin/addresses/clear-risk-history",
            post(handle_admin_clear_address_risk_history),
        )
        .route("/api/daemon/logs/stream", get(handle_daemon_logs_stream))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            require_api_key,
        ));

    let public_telemetry = Router::new()
        .route("/api/stats", get(handle_stats))
        .route("/api/miner/:address/balance", get(handle_miner_balance))
        .route("/api/miner/:address", get(handle_miner))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            limit_public_telemetry_requests,
        ));

    let app = Router::new()
        .route("/", get(handle_ui))
        .route("/checkpoints.dat", get(handle_checkpoints_file))
        .route("/favicon.svg", get(handle_favicon_svg))
        .route("/robots.txt", get(handle_robots_txt))
        .route("/blog", get(crate::blog::handle_blog_root))
        .route("/blog/", get(crate::blog::handle_blog))
        .route("/blog/*path", get(crate::blog::handle_blog))
        .route("/sitemap.xml", get(handle_sitemap_xml))
        .route("/ui-assets/*name", get(handle_ui_asset))
        .route("/api/info", get(handle_info))
        .route(
            "/api/site-events",
            post(handle_site_event).layer(axum::extract::DefaultBodyLimit::max(1024)),
        )
        .route("/api/checkpoints", get(handle_checkpoints_metadata))
        .route("/api/stats/history", get(handle_stats_history))
        .route("/api/stats/insights", get(handle_stats_insights))
        .route("/api/luck", get(handle_luck_history))
        .route("/api/status", get(handle_status))
        .route("/api/monitor/public", get(handle_monitor_public))
        .route(
            "/api/monitor/ingest/cloudflare",
            post(handle_monitor_ingest_cloudflare),
        )
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts/recent", get(handle_public_payouts))
        .route("/api/miner/:address/hashrate", get(handle_miner_hashrate))
        .merge(hosted_wallet)
        .merge(public_telemetry)
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            observe_api_request_performance,
        ))
        .fallback(handle_app_fallback)
        .layer(middleware::from_fn(apply_security_headers))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "api listening");
    let count_state = state.clone();
    let _share_count_compaction = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let store = Arc::clone(&count_state.store);
            let started = Instant::now();
            let result = spawn_blocking_result(move || {
                store.compact_miner_share_count_deltas()?;
                store.compact_share_read_minutes()
            })
            .await;
            count_state.performance.tasks.record(
                "share_count_compaction",
                started.elapsed(),
                result.is_err(),
                started.elapsed() >= Duration::from_millis(200),
            );
            if let Err(error) = result {
                tracing::warn!(%error, "miner share count compaction failed");
            }
        }
    });
    let backfill_store = Arc::clone(&state.store);
    let _block_effort_backfill = tokio::task::spawn_blocking(move || {
        let started_at = Instant::now();
        match backfill_block_effort(&backfill_store) {
            Ok(updated) => tracing::info!(
                updated,
                duration_ms = started_at.elapsed().as_millis() as u64,
                "block effort backfill completed"
            ),
            Err(err) => tracing::warn!(
                error = %err,
                duration_ms = started_at.elapsed().as_millis() as u64,
                "block effort backfill failed"
            ),
        }
    });
    // Peer addresses let the client-IP policy distinguish our proxy from
    // direct callers whose forwarding headers must not be trusted.
    tokio::select! {
        result = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()) => { result?; }
        _ = state.miner_release.refresh_forever() => {}
    }
    Ok(())
}

#[derive(Serialize)]
struct PoolInfoResponse {
    pool_name: String,
    pool_url: String,
    stratum_port: u16,
    pool_fee_pct: f64,
    min_payout_amount: f64,
    blocks_before_payout: i32,
    pplns_window_duration: String,
}

async fn handle_info(State(state): State<ApiState>) -> impl IntoResponse {
    Json(PoolInfoResponse {
        pool_name: state.config.runtime.pool_name.clone(),
        pool_url: state.config.pool_url.clone(),
        stratum_port: state.config.runtime.stratum_port,
        pool_fee_pct: state.config.runtime.pool_fee_pct,
        min_payout_amount: state.config.runtime.min_payout_amount,
        blocks_before_payout: state.config.runtime.blocks_before_payout,
        pplns_window_duration: state.config.runtime.pplns_window_duration.clone(),
    })
}

async fn handle_checkpoints_metadata(State(state): State<ApiState>) -> Response {
    match load_checkpoint_file_snapshot(&state).await {
        Ok(snapshot) => Json(snapshot.metadata.clone()).into_response(),
        Err(CheckpointFileError::NotFound) => {
            Json(unavailable_checkpoints_metadata(&state.config)).into_response()
        }
        Err(err) => checkpoint_file_error_response(err),
    }
}

async fn handle_checkpoints_file(headers: HeaderMap, State(state): State<ApiState>) -> Response {
    let snapshot = match load_checkpoint_file_snapshot(&state).await {
        Ok(snapshot) => snapshot,
        Err(err) => return checkpoint_file_error_response(err),
    };

    let etag = snapshot
        .metadata
        .sha256
        .as_deref()
        .map(checkpoint_etag)
        .unwrap_or_default();
    if !etag.is_empty() && if_none_match_contains(headers.get(header::IF_NONE_MATCH), &etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        apply_checkpoint_response_headers(response.headers_mut(), &snapshot.metadata, &etag);
        return response;
    }

    let mut response = Body::from(snapshot.body.clone()).into_response();
    apply_checkpoint_response_headers(response.headers_mut(), &snapshot.metadata, &etag);
    response
}

#[derive(Clone, Serialize)]
struct StatsResponse {
    pool: PoolSummary,
    chain: ChainSummary,
}

#[derive(Clone, Serialize)]
struct PoolSummary {
    miners: usize,
    /// Credited share work per second over the live hashrate window.
    hashrate: f64,
    raw_hashrate: f64,
    effective_hashrate: Option<f64>,
    block_yield_efficiency_pct: Option<f64>,
    block_yield_sample_size: u64,
    blocks_found: u64,
    orphaned_blocks: u64,
    orphan_rate_pct: f64,
    orphan_rate_30d_pct: f64,
    paid_to_miners_total: u64,
}

#[derive(Clone, Serialize)]
struct ChainSummary {
    current_job_height: Option<u64>,
    network_hashrate: Option<f64>,
}

#[derive(Serialize)]
struct BlockRewardBreakdownResponse {
    block: BlockRewardBlockResponse,
    share_window: RewardWindowSummary,
    fee_amount: u64,
    distributable_reward: u64,
    preview_total_weight: u64,
    payout_total_weight: u64,
    actual_credit_total: u64,
    actual_fee_amount: Option<u64>,
    participants: Vec<BlockRewardParticipantResponse>,
}

#[derive(Serialize)]
struct BlockRewardBlockResponse {
    height: u64,
    reward: u64,
    timestamp: SystemTime,
    orphaned: bool,
    paid_out: bool,
}

#[derive(Serialize)]
struct RewardWindowSummary {
    label: String,
    share_count: usize,
    participant_count: usize,
}

#[derive(Serialize)]
struct BlockRewardParticipantResponse {
    address: String,
    finder: bool,
    risky: bool,
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_shares_eligible: u64,
    provisional_difficulty_eligible: u64,
    preview_weight: u64,
    preview_share_pct: f64,
    preview_credit: u64,
    preview_status: RewardParticipantStatus,
    payout_weight: u64,
    payout_share_pct: f64,
    payout_credit: u64,
    payout_status: RewardParticipantStatus,
    actual_credit: Option<u64>,
    delta_vs_payout: Option<i64>,
}

#[derive(Serialize)]
struct HealthResponse {
    pool_activity: PoolActivityHealth,
    active_verification_holds: Vec<ActiveVerificationHold>,
}

#[derive(Serialize)]
struct PoolActivityHealth {
    connected_miners: u64,
    estimated_hashrate: f64,
}

#[derive(Serialize)]
struct AdminShareDiagnosticsResponse {
    windows: Vec<AdminShareWindowResponse>,
    submit: SubmitRuntimeSnapshot,
    validation: ValidationSnapshot,
}

#[derive(Serialize)]
struct AdminShareWindowResponse {
    label: String,
    accepted: u64,
    rejected: u64,
    by_reason: Vec<RejectionReasonCount>,
}

#[derive(Serialize)]
struct AdminBalanceOverviewResponse {
    wallet: AdminBalanceOverviewWallet,
    payouts: AdminBalanceOverviewPayouts,
    ledger: AdminBalanceOverviewLedger,
}

#[derive(Serialize)]
struct AdminBalanceOverviewWallet {
    spendable: u64,
    pending: u64,
    total: u64,
}

#[derive(Serialize)]
struct AdminBalanceOverviewPayouts {
    clean_unpaid_count: usize,
    queued_count: usize,
    queued_amount: u64,
    next_sweep_at: Option<SystemTime>,
}

#[derive(Serialize)]
struct AdminBalanceOverviewLedger {
    miner_paid_total: u64,
    miner_unpaid_total: u64,
    miner_clean_unpaid_total: u64,
    miner_orphan_backed_unpaid_total: u64,
    miner_balance_source_drift_total: u64,
    net_block_reward_total: u64,
    pool_fee_total: u64,
    pool_fee_clean_unpaid_total: u64,
    pool_fee_orphan_backed_unpaid_total: u64,
    pool_fee_balance_source_drift_total: u64,
    pool_fee_balance_total: u64,
}

#[derive(Serialize)]
struct AdminReconciliationIssuesResponse {
    generated_at: SystemTime,
    missing_payouts: Vec<AdminMissingCompletedPayoutIssueResponse>,
    orphaned_blocks: Vec<AdminOrphanedBlockIssueResponse>,
}

#[derive(Serialize)]
struct AdminMissingCompletedPayoutIssueResponse {
    tx_hash: String,
    payout_row_count: usize,
    total_amount: u64,
    total_fee: u64,
    latest_timestamp: SystemTime,
    addresses: Vec<String>,
    live_linked_amount: u64,
    orphaned_linked_amount: u64,
    unlinked_amount: u64,
}

#[derive(Serialize)]
struct AdminOrphanedBlockIssueResponse {
    height: u64,
    hash: String,
    credit_event_count: u64,
    credited_address_count: u64,
    remaining_credit_amount: u64,
    paid_credit_amount: u64,
    remaining_fee_amount: u64,
    paid_fee_amount: u64,
    pending_payout_count: u64,
    broadcast_pending_payout_count: u64,
}

#[derive(Serialize)]
struct AdminReconciliationPayoutImportResponse {
    imported_tx_count: u64,
    imported_payout_rows: u64,
    imported_amount: u64,
    imported_fee: u64,
    canceled_pending_payouts: u64,
    recorded_manual_offset_amount: u64,
    imported_txs: Vec<AdminReconciliationImportedPayoutTxResponse>,
}

#[derive(Serialize)]
struct AdminReconciliationImportedPayoutTxResponse {
    tx_hash: String,
    payout_row_count: usize,
    total_amount: u64,
    total_fee: u64,
    timestamp: SystemTime,
    addresses: Vec<String>,
}

#[derive(Serialize)]
struct AdminReconciliationManualOffsetApplyResponse {
    scanned_offset_addresses: u64,
    offset_amount_before: u64,
    applied_address_count: u64,
    applied_amount: u64,
    remaining_offset_amount: u64,
    applications: Vec<AdminReconciliationManualOffsetApplicationResponse>,
}

#[derive(Serialize)]
struct AdminReconciliationManualOffsetApplicationResponse {
    address: String,
    applied_amount: u64,
    remaining_offset_amount: u64,
    remaining_balance_pending: u64,
    remaining_canonical_pending: u64,
}

#[derive(Deserialize)]
struct RangeQuery {
    range: Option<String>,
}

#[derive(Clone, Default)]
struct RewardWindowAddressStats {
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_shares_eligible: u64,
    provisional_difficulty_eligible: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RewardParticipantStatus {
    Included,
    CappedProvisional,
    AwaitingVerifiedShares,
    NoEligibleShares,
    RecordedOnly,
}

#[derive(Default)]
struct RewardModeComputation {
    weights: HashMap<String, u64>,
    credits: HashMap<String, u64>,
    statuses: HashMap<String, RewardParticipantStatus>,
    total_weight: u64,
}

struct RewardModeContext<'a> {
    distributable_reward: u64,
    risky_by_address: &'a HashMap<String, bool>,
}

#[derive(Deserialize)]
struct StatsInsightsQuery {
    rejection_window: Option<String>,
}

#[derive(Deserialize)]
struct PageQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Deserialize)]
struct MinerBalanceQuery {
    include_pending_estimate: Option<bool>,
}

#[derive(Deserialize, Default)]
struct SearchPageQuery {
    search: Option<String>,
    sort: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Deserialize)]
struct BlocksQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
}

#[derive(Deserialize)]
struct DaemonLogsQuery {
    tail: Option<usize>,
}

#[derive(Serialize)]
struct PagedResponse<T> {
    items: Vec<T>,
    total: usize,
}

impl<T> PagedResponse<T> {
    fn new(items: Vec<T>, total: usize) -> Self {
        Self { items, total }
    }

    fn from_unpaged(items: Vec<T>, limit: usize, offset: usize) -> Self {
        let total = items.len();
        Self::new(items.into_iter().skip(offset).take(limit).collect(), total)
    }
}

#[derive(Serialize)]
struct AdminBalanceItem {
    address: String,
    clean_payable: u64,
    queued_payout: u64,
    orphan_backed: u64,
    pending: u64,
    paid: u64,
}

#[derive(Serialize)]
struct MinerListItem {
    address: String,
    worker_count: usize,
    shares_accepted: u64,
    shares_rejected: u64,
    blocks_found: u64,
    hashrate: f64,
    last_share_at: Option<SystemTime>,
}

async fn handle_stats(State(state): State<ApiState>) -> impl IntoResponse {
    json_result(
        state.cached_stats_response().await,
        "failed loading pool stats",
    )
}

async fn handle_stats_history(
    Query(query): Query<RangeQuery>,
    client: ClientIp,
    State(state): State<ApiState>,
) -> Response {
    let label = normalized_range_label(query.range.as_deref());
    let task_state = state.clone();
    state
        .serve_public_read(
            &client,
            "stats_history",
            format!("stats_history:{label}"),
            PUBLIC_STATS_HISTORY_CACHE_TTL,
            "failed loading stat history",
            async move {
                let since = SystemTime::now()
                    .checked_sub(history_range_duration(Some(label)))
                    .unwrap_or(UNIX_EPOCH);
                let store = Arc::clone(&task_state.store);
                let snapshots = task_state
                    .public_blocking(move || store.get_stat_snapshots(since))
                    .await?;
                json_bytes(
                    &snapshots
                        .into_iter()
                        .map(|snapshot| PoolHashratePointResponse {
                            timestamp: snapshot.timestamp,
                            hashrate: snapshot.hashrate,
                        })
                        .collect::<Vec<_>>(),
                )
            },
        )
        .await
}

async fn handle_stats_insights(
    Query(query): Query<StatsInsightsQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let rejection_window = rejection_window_duration(query.rejection_window.as_deref());
    let mut response = match state.stats_insights().await {
        Ok(value) => value,
        Err(err) => return internal_error("failed loading stats insights", err),
    };
    match state.rejection_analytics_snapshot(rejection_window).await {
        Ok(snapshot) => {
            response.rejections = snapshot;
            Json(response).into_response()
        }
        Err(err) => internal_error("failed loading rejection analytics", err),
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

/// Canonical range label shared by the history endpoints (unknown -> 24h), so
/// cache keys cannot be multiplied with arbitrary query strings.
fn normalized_range_label(input: Option<&str>) -> &'static str {
    match input.map(str::trim).unwrap_or("24h") {
        "1h" => "1h",
        "7d" => "7d",
        "30d" => "30d",
        _ => "24h",
    }
}

fn miner_hashrate_cache_ttl(label: &str) -> Duration {
    match label {
        "1h" => Duration::from_secs(10),
        "24h" => Duration::from_secs(30),
        _ => Duration::from_secs(60),
    }
}

fn history_range_duration(input: Option<&str>) -> Duration {
    match input.map(str::trim).unwrap_or("24h") {
        "1h" => Duration::from_secs(3600),
        "7d" => Duration::from_secs(7 * 86400),
        "30d" => Duration::from_secs(30 * 86400),
        _ => Duration::from_secs(86400),
    }
}

fn miner_hashrate_range(input: Option<&str>) -> (Duration, i64) {
    match input.map(str::trim).unwrap_or("24h") {
        "1h" => (Duration::from_secs(3600), 120),
        "7d" => (Duration::from_secs(7 * 86400), 3600),
        "30d" => (Duration::from_secs(30 * 86400), 14400),
        _ => (Duration::from_secs(86400), 600),
    }
}

fn collect_admin_share_windows(
    store: &PoolStore,
    now: SystemTime,
) -> anyhow::Result<Vec<AdminShareWindowResponse>> {
    let windows = [
        ("5m", Duration::from_secs(5 * 60)),
        ("15m", Duration::from_secs(15 * 60)),
        ("1h", Duration::from_secs(60 * 60)),
        ("6h", Duration::from_secs(6 * 60 * 60)),
        ("24h", Duration::from_secs(24 * 60 * 60)),
    ];
    let mut rows = Vec::with_capacity(windows.len());
    for (label, window) in windows {
        let since = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
        let (accepted, rejected) = store.share_outcome_counts_since(since)?;
        let by_reason = store.rejection_reason_counts_since(since)?;
        rows.push(AdminShareWindowResponse {
            label: label.to_string(),
            accepted,
            rejected,
            by_reason,
        });
    }
    Ok(rows)
}

fn service_health_from_heartbeat(
    latest: Option<&MonitorHeartbeat>,
    fresh_for: Duration,
    healthy_fn: impl Fn(&MonitorHeartbeat) -> Option<bool>,
) -> ServiceHealth {
    let Some(row) = latest else {
        return ServiceHealth {
            observed: false,
            healthy: false,
        };
    };
    let fresh = SystemTime::now()
        .duration_since(row.sampled_at)
        .unwrap_or_default()
        <= fresh_for;
    let healthy = healthy_fn(row).unwrap_or(false) && fresh;
    ServiceHealth {
        observed: fresh,
        healthy,
    }
}

fn daemon_health_from_heartbeat(latest: Option<&MonitorHeartbeat>) -> DaemonHealth {
    DaemonHealth {
        reachable: latest.and_then(|row| row.daemon_up).unwrap_or(false),
        chain_height: latest.and_then(|row| row.chain_height),
        syncing: latest.and_then(|row| row.daemon_syncing),
    }
}

fn build_monitor_uptime_window(
    label: &str,
    local: &MonitorUptimeSummary,
    external: &MonitorUptimeSummary,
) -> UptimeWindow {
    UptimeWindow {
        label: label.to_string(),
        sample_count: local.sample_count as usize,
        external_sample_count: external.sample_count as usize,
        api_up_pct: uptime_pct(local.api_up, local.api_total),
        stratum_up_pct: uptime_pct(local.stratum_up, local.stratum_total),
        pool_up_pct: uptime_pct(local.pool_up, local.pool_total),
        daemon_up_pct: uptime_pct(local.daemon_up, local.daemon_total),
        database_up_pct: uptime_pct(local.database_up, local.database_total),
        public_http_up_pct: uptime_pct(external.public_http_up, external.public_http_total),
    }
}

fn uptime_pct(up: u64, total: u64) -> Option<f64> {
    if total == 0 {
        None
    } else {
        Some((up as f64 / total as f64) * 100.0)
    }
}

fn status_incident_from_monitor(incident: MonitorIncident, now: SystemTime) -> StatusIncident {
    let ended_at = incident.ended_at;
    StatusIncident {
        id: incident.id as u64,
        kind: incident.kind,
        severity: incident.severity,
        started_at: incident.started_at,
        duration_seconds: ended_at
            .unwrap_or(now)
            .duration_since(incident.started_at)
            .ok()
            .map(|elapsed| elapsed.as_secs()),
        message: incident.summary,
        ongoing: ended_at.is_none(),
    }
}

fn cloudflare_heartbeat(
    sampled_at: SystemTime,
    public_http_up: bool,
    synthetic: bool,
    detail: Option<String>,
) -> pool_common::db::MonitorHeartbeatUpsert {
    pool_common::db::MonitorHeartbeatUpsert {
        sampled_at,
        source: CLOUDFLARE_MONITOR_SOURCE.to_string(),
        synthetic,
        api_up: None,
        stratum_up: None,
        db_up: true,
        daemon_up: None,
        public_http_up: Some(public_http_up),
        daemon_syncing: None,
        chain_height: None,
        template_age_seconds: None,
        last_refresh_millis: None,
        stratum_snapshot_age_seconds: None,
        connected_miners: None,
        connected_workers: None,
        estimated_hashrate: None,
        wallet_up: None,
        last_accepted_share_at: None,
        last_accepted_share_age_seconds: None,
        payout_pending_count: None,
        payout_pending_amount: None,
        oldest_pending_payout_at: None,
        oldest_pending_payout_age_seconds: None,
        oldest_pending_send_started_at: None,
        oldest_pending_send_age_seconds: None,
        validation_candidate_queue_depth: None,
        validation_regular_queue_depth: None,
        summary_state: if public_http_up {
            "healthy".to_string()
        } else {
            "down".to_string()
        },
        details_json: detail.map(|detail| json!({ "detail": detail }).to_string()),
    }
}

fn verify_monitor_signature(secret: &str, provided: &str, body: &[u8]) -> bool {
    let Some(signature) = provided.strip_prefix("sha256=") else {
        return false;
    };
    let expected = hmac_sha256_hex(secret.as_bytes(), body);
    constant_time_eq(signature.as_bytes(), expected.as_bytes())
}

fn hmac_sha256_hex(secret: &[u8], body: &[u8]) -> String {
    const BLOCK_SIZE: usize = 64;
    let mut key_block = [0u8; BLOCK_SIZE];
    if secret.len() > BLOCK_SIZE {
        let digest = Sha256::digest(secret);
        key_block[..digest.len()].copy_from_slice(&digest);
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }

    let mut inner = [0u8; BLOCK_SIZE];
    let mut outer = [0u8; BLOCK_SIZE];
    for (idx, value) in key_block.iter().enumerate() {
        inner[idx] = value ^ 0x36;
        outer[idx] = value ^ 0x5c;
    }

    let mut inner_hasher = Sha256::new();
    inner_hasher.update(inner);
    inner_hasher.update(body);
    let inner_digest = inner_hasher.finalize();

    let mut outer_hasher = Sha256::new();
    outer_hasher.update(outer);
    outer_hasher.update(inner_digest);
    let digest = outer_hasher.finalize();

    hex::encode(digest)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (lhs, rhs) in a.iter().zip(b.iter()) {
        diff |= lhs ^ rhs;
    }
    diff == 0
}

async fn handle_status(client: ClientIp, State(state): State<ApiState>) -> Response {
    let task_state = state.clone();
    state
        .serve_public_read(
            &client,
            "status",
            "status".to_string(),
            PUBLIC_STATUS_CACHE_TTL,
            "failed loading status page",
            async move { json_bytes(&task_state.build_status_response().await?) },
        )
        .await
}

async fn handle_monitor_public(State(state): State<ApiState>) -> impl IntoResponse {
    let latest = {
        let store = Arc::clone(&state.store);
        spawn_blocking_result(move || {
            store.get_latest_monitor_heartbeat(Some(LOCAL_MONITOR_SOURCE))
        })
        .await
    };

    let local = match latest {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "failed loading public monitor heartbeat");
            None
        }
    };

    Json(serde_json::json!({
        "ok": true,
        "summary_state": local.as_ref().map(|row| row.summary_state.clone()).unwrap_or_else(|| "unknown".to_string()),
    }))
}

#[derive(Deserialize)]
struct CloudflareIngestEvent {
    service: String,
    status: String,
    started_at: Option<u64>,
    ended_at: Option<u64>,
    checked_at: Option<u64>,
    summary: Option<String>,
    detail: Option<String>,
}

/// Replay protection for signed monitor events: `checked_at` (and `ended_at`
/// when present) must be within [`MONITOR_INGEST_MAX_CLOCK_SKEW_SECS`] of now,
/// and `checked_at` must be newer than the last accepted event.
fn check_monitor_event_freshness(
    event: &CloudflareIngestEvent,
    now_secs: u64,
    last_accepted: u64,
) -> Result<u64, (StatusCode, &'static str)> {
    let Some(checked_at) = event.checked_at else {
        return Err((StatusCode::BAD_REQUEST, "monitor event missing checked_at"));
    };
    let outside_window =
        |timestamp: u64| timestamp.abs_diff(now_secs) > MONITOR_INGEST_MAX_CLOCK_SKEW_SECS;
    if outside_window(checked_at) || event.ended_at.is_some_and(outside_window) {
        return Err((
            StatusCode::BAD_REQUEST,
            "monitor event timestamp outside the accepted window",
        ));
    }
    if checked_at <= last_accepted {
        return Err((
            StatusCode::CONFLICT,
            "monitor event is not newer than the last accepted event",
        ));
    }
    Ok(checked_at)
}

async fn handle_monitor_ingest_cloudflare(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let secret = state.config.monitor_ingest_secret.trim();
    let provided = headers
        .get("x-monitor-signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !verify_monitor_signature(secret, provided, &body) {
        return error_response(StatusCode::UNAUTHORIZED, "invalid monitor signature");
    }

    let event: CloudflareIngestEvent = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON payload: {err}"),
            );
        }
    };

    if !event.service.trim().eq_ignore_ascii_case("public_http") {
        return error_response(StatusCode::BAD_REQUEST, "unsupported monitor service");
    }

    // Held until the event is stored so concurrent replays cannot both pass.
    let mut last_checked_at = state.monitor_ingest_last_checked_at.lock().await;
    let last = match *last_checked_at {
        Some(value) => value,
        None => {
            let store = Arc::clone(&state.store);
            match spawn_blocking_result(move || {
                store.get_latest_monitor_heartbeat(Some(CLOUDFLARE_MONITOR_SOURCE))
            })
            .await
            {
                Ok(latest) => latest
                    .map(|row| system_time_to_unix_secs(row.sampled_at))
                    .unwrap_or(0),
                Err(err) => {
                    return internal_error("failed loading last cloudflare monitor event", err)
                }
            }
        }
    };
    let now_secs = system_time_to_unix_secs(SystemTime::now());
    match check_monitor_event_freshness(&event, now_secs, last) {
        Ok(checked_at) => *last_checked_at = Some(checked_at),
        Err((status, message)) => {
            *last_checked_at = Some(last);
            tracing::warn!(
                checked_at = ?event.checked_at,
                last_accepted = last,
                now = now_secs,
                "rejected cloudflare monitor event: {message}"
            );
            return error_response(status, message);
        }
    }

    let store = Arc::clone(&state.store);
    let action = spawn_blocking_result(move || -> anyhow::Result<()> {
        let ended_at = UNIX_EPOCH
            + Duration::from_secs(
                event
                    .ended_at
                    .or(event.checked_at)
                    .unwrap_or_else(|| system_time_to_unix_secs(SystemTime::now())),
            );
        let started_at = event
            .started_at
            .map(|timestamp| UNIX_EPOCH + Duration::from_secs(timestamp));
        let status = event.status.trim().to_ascii_lowercase();
        let summary = event
            .summary
            .clone()
            .unwrap_or_else(|| "public HTTP probe changed state".to_string());
        let detail = event.detail.clone();

        if status == "down" {
            store.upsert_monitor_heartbeat(&cloudflare_heartbeat(
                ended_at,
                false,
                false,
                detail.clone(),
            ))?;
            store.upsert_monitor_incident(&pool_common::db::MonitorIncidentUpsert {
                dedupe_key: "cloudflare_public_http_down".to_string(),
                kind: "public_http_down".to_string(),
                severity: "critical".to_string(),
                visibility: "public".to_string(),
                source: CLOUDFLARE_MONITOR_SOURCE.to_string(),
                summary,
                detail,
                started_at: started_at.unwrap_or(ended_at),
                updated_at: ended_at,
            })?;
            return Ok(());
        }

        if let Some(started) = started_at {
            let mut ts = started;
            while ts < ended_at {
                store.upsert_monitor_heartbeat(&cloudflare_heartbeat(
                    ts,
                    false,
                    true,
                    detail.clone(),
                ))?;
                ts = ts.checked_add(Duration::from_secs(60)).unwrap_or(ended_at);
            }
            store.upsert_monitor_incident(&pool_common::db::MonitorIncidentUpsert {
                dedupe_key: "cloudflare_public_http_down".to_string(),
                kind: "public_http_down".to_string(),
                severity: "critical".to_string(),
                visibility: "public".to_string(),
                source: CLOUDFLARE_MONITOR_SOURCE.to_string(),
                summary,
                detail: detail.clone(),
                started_at: started,
                updated_at: ended_at,
            })?;
            store.resolve_monitor_incident("cloudflare_public_http_down", ended_at)?;
        } else {
            store.resolve_monitor_incident("cloudflare_public_http_down", ended_at)?;
        }

        store.upsert_monitor_heartbeat(&cloudflare_heartbeat(ended_at, true, false, detail))?;
        Ok(())
    })
    .await;

    match action {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(err) => internal_error("failed storing cloudflare monitor event", err),
    }
}

async fn handle_admin_perf(State(state): State<ApiState>) -> impl IntoResponse {
    let mut snapshot = state.performance.snapshot();
    snapshot.database_reads = state.store.read_performance();
    Json(snapshot).into_response()
}

async fn handle_admin_balances(
    Query(query): Query<SearchPageQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let search = query.search.clone().unwrap_or_default();
    let sort = query
        .sort
        .clone()
        .unwrap_or_else(|| "pending_desc".to_string());
    let (limit, offset) = page_bounds(query.limit, query.offset);

    let result =
        spawn_blocking_result(move || -> anyhow::Result<PagedResponse<AdminBalanceItem>> {
            let all = store.get_all_balances()?;
            let source_by_address = store
                .list_pending_balance_source_summaries()?
                .into_iter()
                .map(|source| (source.address.clone(), source))
                .collect::<HashMap<_, BalanceSourceSummary>>();
            let queued_by_address = store
                .get_pending_payouts()?
                .into_iter()
                .map(|payout| (payout.address, payout.amount))
                .collect::<HashMap<_, _>>();
            let mut filtered: Vec<_> = if search.is_empty() {
                all
            } else {
                let needle = search.to_lowercase();
                all.into_iter()
                    .filter(|b| b.address.to_lowercase().contains(&needle))
                    .collect()
            };

            match sort.as_str() {
                "pending_asc" => filtered.sort_by_key(|a| a.pending),
                "paid_desc" => filtered.sort_by_key(|b| std::cmp::Reverse(b.paid)),
                "paid_asc" => filtered.sort_by_key(|a| a.paid),
                "address_asc" => filtered.sort_by(|a, b| a.address.cmp(&b.address)),
                "address_desc" => filtered.sort_by(|a, b| b.address.cmp(&a.address)),
                _ => filtered.sort_by_key(|b| std::cmp::Reverse(b.pending)), // pending_desc default
            }

            let total = filtered.len();
            let items: Vec<AdminBalanceItem> = filtered
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|b| {
                    let source = source_by_address
                        .get(&b.address)
                        .cloned()
                        .unwrap_or_default();
                    let queued_payout = queued_by_address
                        .get(&b.address)
                        .copied()
                        .unwrap_or_default();
                    AdminBalanceItem {
                        address: b.address,
                        clean_payable: source.canonical_pending,
                        queued_payout,
                        orphan_backed: source.orphan_pending,
                        pending: b.pending,
                        paid: b.paid,
                    }
                })
                .collect();
            Ok(PagedResponse::new(items, total))
        })
        .await;
    json_result(result, "failed loading balances")
}

async fn handle_admin_balance_overview(State(state): State<ApiState>) -> impl IntoResponse {
    json_result(
        state.admin_balance_overview().await,
        "failed loading balance overview",
    )
}

async fn handle_admin_reconciliation_issues(State(state): State<ApiState>) -> impl IntoResponse {
    json_result(
        state.admin_reconciliation_issues().await,
        "failed loading reconciliation issues",
    )
}

async fn handle_admin_reconciliation_payout_resolution(
    State(state): State<ApiState>,
    Json(request): Json<AdminReconciliationPayoutResolutionRequest>,
) -> impl IntoResponse {
    let tx_hash = request.tx_hash.trim();
    if tx_hash.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "tx_hash is required");
    }

    no_content_result(
        state
            .resolve_missing_completed_payout_issue(tx_hash, request.action)
            .await,
        "failed resolving reconciliation payout issue",
    )
}

async fn handle_admin_reconciliation_payout_import(
    State(state): State<ApiState>,
    Json(request): Json<AdminReconciliationPayoutImportRequest>,
) -> impl IntoResponse {
    let tx_hashes = match normalize_reconciliation_import_tx_hashes(&request.tx_hashes) {
        Ok(tx_hashes) => tx_hashes,
        Err(err) => {
            return error_response(StatusCode::BAD_REQUEST, err.to_string());
        }
    };

    json_result(
        state.import_confirmed_wallet_payouts(tx_hashes).await,
        "failed importing confirmed payout txs into ledger",
    )
}

async fn handle_admin_reconciliation_manual_offset_apply(
    State(state): State<ApiState>,
) -> impl IntoResponse {
    json_result(
        state.apply_live_manual_payout_offsets().await,
        "failed applying manual payout offsets",
    )
}

async fn handle_admin_orphaned_block_cleanup_retry(
    State(state): State<ApiState>,
    Json(request): Json<AdminOrphanedBlockCleanupRequest>,
) -> impl IntoResponse {
    if request.block_height == 0 {
        return error_response(StatusCode::BAD_REQUEST, "block_height must be positive");
    }

    let store = Arc::clone(&state.store);
    let result = spawn_blocking_result(move || {
        store.reconcile_existing_orphaned_block_credits(request.block_height)
    })
    .await;
    no_content_result(result, "failed retrying orphaned block cleanup")
}

async fn handle_health(State(state): State<ApiState>) -> impl IntoResponse {
    let provisional_cutoff = SystemTime::now()
        .checked_sub(state.config.runtime.provisional_share_delay_duration())
        .unwrap_or(UNIX_EPOCH);
    let persisted_runtime = state.persisted_runtime_snapshot().await;
    let pool_activity = PoolActivityHealth {
        connected_miners: persisted_runtime
            .as_ref()
            .map(|snapshot| snapshot.connected_miners as u64)
            .unwrap_or_default(),
        estimated_hashrate: persisted_runtime
            .as_ref()
            .map(|snapshot| snapshot.estimated_hashrate)
            .unwrap_or_default(),
    };

    let store = Arc::clone(&state.store);
    let active_verification_holds = match spawn_blocking_result(move || {
        store.list_active_verification_holds(provisional_cutoff)
    })
    .await
    {
        Ok(v) => v,
        Err(err) => {
            return internal_error("failed loading active verification holds", err);
        }
    };

    let response = HealthResponse {
        pool_activity,
        active_verification_holds,
    };

    Json(response).into_response()
}

async fn handle_admin_share_diagnostics(State(state): State<ApiState>) -> impl IntoResponse {
    json_result(
        state.admin_share_diagnostics().await,
        "failed loading admin share diagnostics",
    )
}

#[derive(Deserialize)]
struct RecoveryCutoverRequest {
    target: RecoveryInstanceId,
}

#[derive(Deserialize)]
struct AdminReconciliationPayoutResolutionRequest {
    tx_hash: String,
    action: ManualCompletedPayoutResolutionKind,
}

#[derive(Deserialize)]
struct AdminReconciliationPayoutImportRequest {
    tx_hashes: Vec<String>,
}

#[derive(Deserialize)]
struct AdminOrphanedBlockCleanupRequest {
    block_height: u64,
}

#[derive(Deserialize)]
struct ClearAddressRiskHistoryRequest {
    address: String,
}

async fn handle_recovery_status(State(state): State<ApiState>) -> impl IntoResponse {
    if !state.config.recovery.enabled {
        return Json(RecoveryStatus::disabled(
            &state.config.runtime.payout_pause_file,
        ))
        .into_response();
    }
    json_result(
        state.recovery.status().await,
        "failed loading recovery status",
    )
}

async fn handle_recovery_pause_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    recovery_operation_response(&state, state.recovery.pause_payouts().await)
}

async fn handle_recovery_resume_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    recovery_operation_response(&state, state.recovery.resume_payouts().await)
}

async fn handle_recovery_start_inactive_sync(State(state): State<ApiState>) -> impl IntoResponse {
    recovery_operation_response(&state, state.recovery.start_inactive_sync().await)
}

async fn handle_recovery_rebuild_inactive_wallet(
    State(state): State<ApiState>,
) -> impl IntoResponse {
    recovery_operation_response(&state, state.recovery.rebuild_inactive_wallet().await)
}

async fn handle_recovery_cutover(
    State(state): State<ApiState>,
    Json(request): Json<RecoveryCutoverRequest>,
) -> impl IntoResponse {
    recovery_operation_response(&state, state.recovery.cutover(request.target).await)
}

async fn handle_recovery_purge_inactive_daemon(State(state): State<ApiState>) -> impl IntoResponse {
    recovery_operation_response(&state, state.recovery.purge_inactive_daemon().await)
}

async fn handle_admin_clear_address_risk_history(
    State(state): State<ApiState>,
    Json(request): Json<ClearAddressRiskHistoryRequest>,
) -> impl IntoResponse {
    let address = request.address.trim();
    if address.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "address is required");
    }

    let address = address.to_string();
    let store = Arc::clone(&state.store);
    let result = spawn_blocking_result(move || store.clear_address_risk_history(&address)).await;
    no_content_result(result, "failed clearing address risk history")
}

fn recovery_operation_response(
    state: &ApiState,
    result: anyhow::Result<RecoveryOperation>,
) -> Response {
    if !state.config.recovery.enabled {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "recovery controls are disabled",
        );
    }
    match result {
        Ok(operation) => Json(operation).into_response(),
        Err(err) => {
            let message = err.to_string();
            let status = if message.contains("already running")
                || message.contains("pause payouts before")
                || message.contains("still syncing")
                || message.contains("not loaded")
                || message.contains("not reachable")
                || message.contains("already active")
            {
                StatusCode::CONFLICT
            } else if message.contains("disabled") {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            if status == StatusCode::INTERNAL_SERVER_ERROR {
                internal_error("failed starting recovery operation", err)
            } else {
                error_response(status, message)
            }
        }
    }
}

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
    let config = state.config.clone();

    let (tx, rx) = mpsc::channel::<Result<Vec<u8>, Infallible>>(128);
    tokio::spawn(async move {
        stream_daemon_logs(config, tail, tx).await;
    });

    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache, no-transform"),
            (header::HeaderName::from_static("x-accel-buffering"), "no"),
        ],
        Body::from_stream(ReceiverStream::new(rx)),
    )
        .into_response()
}

async fn stream_daemon_logs(
    config: Config,
    tail: usize,
    tx: mpsc::Sender<Result<Vec<u8>, Infallible>>,
) {
    let mut errors = Vec::<String>::new();
    for command in daemon_log_commands(&config, tail) {
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

fn daemon_log_commands(config: &Config, tail: usize) -> Vec<DaemonLogCommand> {
    let mut tail_args = vec!["-n".to_string(), tail.to_string(), "-F".to_string()];
    tail_args.push(
        daemon_debug_log_path(config)
            .to_string_lossy()
            .trim()
            .to_string(),
    );

    let mut commands = Vec::new();
    for unit in daemon_log_units(config) {
        let journal_args = vec![
            "-u".to_string(),
            unit,
            "-q".to_string(),
            "-a".to_string(),
            "-n".to_string(),
            tail.to_string(),
            "-o".to_string(),
            "short-iso".to_string(),
            "-f".to_string(),
        ];
        commands.push(DaemonLogCommand {
            source: "journald",
            program: "journalctl",
            args: journal_args,
        });
    }
    commands.push(DaemonLogCommand {
        source: "debug-log",
        program: "tail",
        args: tail_args,
    });
    commands
}

fn normalize_reconciliation_import_tx_hashes(tx_hashes: &[String]) -> anyhow::Result<Vec<String>> {
    let mut normalized = Vec::<String>::new();
    let mut seen = HashSet::<String>::new();
    for tx_hash in tx_hashes {
        let tx_hash = tx_hash.trim();
        if tx_hash.is_empty() {
            continue;
        }
        if !seen.insert(tx_hash.to_string()) {
            return Err(anyhow::anyhow!("duplicate tx hash {}", tx_hash));
        }
        normalized.push(tx_hash.to_string());
    }
    if normalized.is_empty() {
        return Err(anyhow::anyhow!("at least one tx hash is required"));
    }
    Ok(normalized)
}

fn daemon_send_idempotency_path(config: &Config) -> PathBuf {
    daemon_data_path(config, "send-idempotency.json")
}

fn daemon_data_path(config: &Config, file_name: &str) -> PathBuf {
    if let Some(instance) = active_recovery_instance(config) {
        let data_dir = config.recovery.instance(instance).data_dir.trim();
        if !data_dir.is_empty() {
            return PathBuf::from(data_dir).join(file_name);
        }
    }
    let data_dir = config.runtime.daemon_data_dir.trim();
    if data_dir.is_empty() {
        return PathBuf::from("data").join(file_name);
    }
    PathBuf::from(data_dir).join(file_name)
}

fn load_confirmed_payout_import_txs(
    path: &FsPath,
    tx_hashes: &[String],
) -> anyhow::Result<Vec<ConfirmedPayoutImportTx>> {
    let raw = fs::read(path)
        .map_err(|err| anyhow::anyhow!("failed reading {}: {err}", path.display()))?;
    let journal: WalletSendIdempotencyJournal = serde_json::from_slice(&raw)
        .map_err(|err| anyhow::anyhow!("failed parsing {}: {err}", path.display()))?;
    let requested = tx_hashes.iter().cloned().collect::<HashSet<_>>();
    let mut manifests = HashMap::<String, ConfirmedPayoutImportTx>::new();

    for entry in journal.entries.into_values() {
        if entry.status != 200 {
            continue;
        }
        let Some(body) = decode_wallet_send_body(&entry).map_err(|err| {
            anyhow::anyhow!(
                "failed decoding wallet send entry from {}: {err}",
                path.display()
            )
        })?
        else {
            continue;
        };
        if body.dry_run {
            continue;
        }
        let tx_hash = body.txid.trim();
        if tx_hash.is_empty() || !requested.contains(tx_hash) {
            continue;
        }

        let recipients = aggregate_wallet_send_recipients(&body.recipients);
        if recipients.is_empty() {
            return Err(anyhow::anyhow!(
                "wallet send tx {} in {} has no recipients",
                tx_hash,
                path.display()
            ));
        }

        let manifest = ConfirmedPayoutImportTx {
            tx_hash: tx_hash.to_string(),
            timestamp: if entry.created_at_unix_nano <= 0 {
                UNIX_EPOCH
            } else {
                UNIX_EPOCH + Duration::from_nanos(entry.created_at_unix_nano as u64)
            },
            recipients: recipients
                .iter()
                .zip(allocate_proportional_fees(
                    recipients.iter().map(|(_, amount)| *amount),
                    body.fee,
                ))
                .map(|((address, amount), fee)| ConfirmedPayoutImportRecipient {
                    address: address.clone(),
                    amount: *amount,
                    fee,
                })
                .collect(),
        };
        match manifests.get(tx_hash) {
            Some(existing) if existing.timestamp >= manifest.timestamp => {}
            _ => {
                manifests.insert(tx_hash.to_string(), manifest);
            }
        }
    }

    let mut ordered = Vec::with_capacity(tx_hashes.len());
    for tx_hash in tx_hashes {
        let manifest = manifests.remove(tx_hash).ok_or_else(|| {
            anyhow::anyhow!(
                "successful live wallet send {} not found in {}",
                tx_hash,
                path.display()
            )
        })?;
        ordered.push(manifest);
    }
    Ok(ordered)
}

fn daemon_debug_log_path(config: &Config) -> PathBuf {
    daemon_data_path(config, "debug.log")
}

fn daemon_log_units(config: &Config) -> Vec<String> {
    let mut units = Vec::new();
    if let Some(active) = active_recovery_instance(config) {
        let service = config.recovery.instance(active).service.trim();
        if !service.is_empty() {
            units.push(service.to_string());
        }
    }
    if config.recovery.enabled {
        for instance in [RecoveryInstanceId::Primary, RecoveryInstanceId::Standby] {
            let service = config.recovery.instance(instance).service.trim();
            if !service.is_empty() && !units.iter().any(|existing| existing == service) {
                units.push(service.to_string());
            }
        }
    }
    units
}

fn active_recovery_instance(config: &Config) -> Option<RecoveryInstanceId> {
    if !config.recovery.enabled {
        return None;
    }
    config.recovery.effective_active_instance()
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
    let mut heartbeat = tokio::time::interval(DAEMON_LOG_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;

    while stdout_open || stderr_open {
        tokio::select! {
            _ = heartbeat.tick() => {
                if tx.send(Ok(vec![b'\n'])).await.is_err() {
                    return Ok(());
                }
            }
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
    Query(query): Query<SearchPageQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let worker_window_start = SystemTime::now()
        .checked_sub(HASHRATE_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    let now = SystemTime::now();
    let (lifetime_counts, window_stats) = match spawn_blocking_result(move || {
        Ok::<_, anyhow::Error>((
            store.miner_lifetime_counts()?,
            store.miner_window_stats_since(worker_window_start)?,
        ))
    })
    .await
    {
        Ok(v) => v,
        Err(err) => return internal_error("failed loading miner summary counts", err),
    };

    let mut address_set = HashSet::new();
    address_set.extend(lifetime_counts.keys().cloned());
    address_set.extend(window_stats.keys().cloned());
    let mut addresses = address_set.into_iter().collect::<Vec<_>>();
    addresses.sort();

    let mut items = addresses
        .into_iter()
        .map(|address| {
            let window = window_stats.get(&address);
            let worker_count = window.map(|stats| stats.worker_count).unwrap_or(0);
            let hashrate = window
                .map(|stats| {
                    hashrate_from_stats_with_miner_ramp(
                        HashrateStatsInput {
                            total_diff: stats.total_difficulty,
                            count: stats.accepted_count,
                            oldest: stats.oldest,
                            newest: stats.newest,
                        },
                        MinerHashrateRamp {
                            smoothing_window: HASHRATE_WINDOW,
                            warmup_window: HASHRATE_WARMUP_WINDOW,
                            brand_new_min_window: HASHRATE_BRAND_NEW_MIN_WINDOW,
                            now,
                        },
                    )
                })
                .unwrap_or(0.0);
            let (accepted, rejected, blocks, db_last_share) = lifetime_counts
                .get(&address)
                .copied()
                .unwrap_or((0, 0, 0, None));
            let last_share_at = db_last_share
                .map(|ts| std::time::UNIX_EPOCH + std::time::Duration::from_secs(ts.max(0) as u64));
            MinerListItem {
                address,
                worker_count,
                shares_accepted: accepted,
                shares_rejected: rejected,
                blocks_found: blocks,
                hashrate,
                last_share_at,
            }
        })
        .collect::<Vec<MinerListItem>>();

    if let Some(search) = non_empty(&query.search) {
        let search = search.to_ascii_lowercase();
        items.retain(|item| item.address.to_ascii_lowercase().contains(&search));
    }

    match query
        .sort
        .as_deref()
        .map(str::trim)
        .unwrap_or("hashrate_desc")
    {
        "address_asc" => items.sort_by(|a, b| a.address.cmp(&b.address)),
        "accepted_desc" => items.sort_by_key(|b| std::cmp::Reverse(b.shares_accepted)),
        "rejected_desc" => items.sort_by_key(|b| std::cmp::Reverse(b.shares_rejected)),
        "last_share_desc" => items.sort_by_key(|b| std::cmp::Reverse(b.last_share_at)),
        _ => items.sort_by(|a, b| {
            b.hashrate
                .partial_cmp(&a.hashrate)
                .unwrap_or(std::cmp::Ordering::Equal)
        }),
    }

    let (limit, offset) = page_bounds(query.limit, query.offset);
    Json(PagedResponse::from_unpaged(items, limit, offset)).into_response()
}

async fn handle_miner_balance(
    Path(address): Path<String>,
    Query(query): Query<MinerBalanceQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let include_pending_estimate = query.include_pending_estimate.unwrap_or(true);
    json_result(
        state
            .cached_miner_balance_payload(&address, include_pending_estimate)
            .await,
        "failed loading miner balance",
    )
}

async fn handle_miner(
    Path(address): Path<String>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    match state.cached_miner_detail_payload(&address).await {
        Ok(payload) => {
            if payload.found {
                Json(payload.body).into_response()
            } else {
                (StatusCode::NOT_FOUND, Json(payload.body)).into_response()
            }
        }
        Err(err) => internal_error("failed loading miner data", err),
    }
}

async fn handle_miner_hashrate(
    Path(address): Path<String>,
    Query(query): Query<RangeQuery>,
    client: ClientIp,
    State(state): State<ApiState>,
) -> Response {
    let label = normalized_range_label(query.range.as_deref());
    let key = format!("miner_hashrate:{label}:{address}");
    let task_state = state.clone();
    state
        .serve_public_read(
            &client,
            "miner_hashrate",
            key,
            miner_hashrate_cache_ttl(label),
            "failed loading miner hashrate history",
            async move {
                let store = Arc::clone(&task_state.store);
                let points = task_state
                    .public_blocking(move || {
                        miner_hashrate_points(&store, &address, label, SystemTime::now())
                    })
                    .await?;
                json_bytes(&points)
            },
        )
        .await
}

fn miner_hashrate_points(
    store: &PoolStore,
    address: &str,
    range_label: &str,
    now: SystemTime,
) -> anyhow::Result<Vec<MinerHashratePointResponse>> {
    let (range_duration, bucket_secs) = miner_hashrate_range(Some(range_label));
    let since = now.checked_sub(range_duration).unwrap_or(UNIX_EPOCH);
    let buckets = store.hashrate_history_for_miner(address, since, bucket_secs)?;
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

    let mut points = Vec::<MinerHashratePointResponse>::new();
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
            points.push(MinerHashratePointResponse {
                timestamp: ts,
                hashrate: smoothed,
            });
            ts = ts.saturating_add(step);
        }
    }
    Ok(points)
}

async fn handle_blocks(
    Query(query): Query<BlocksQuery>,
    client: ClientIp,
    State(state): State<ApiState>,
) -> Response {
    let (limit, offset) = page_bounds(query.limit, query.offset);
    let status = non_empty(&query.status)
        .filter(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "confirmed" | "orphaned" | "pending"
            )
        })
        .map(str::to_ascii_lowercase);
    let key = format!(
        "blocks:{limit}:{offset}:{}",
        status.as_deref().unwrap_or("all")
    );
    let task_state = state.clone();
    state
        .serve_public_read(
            &client,
            "blocks",
            key,
            PUBLIC_BLOCKS_CACHE_TTL,
            "failed loading blocks",
            async move { json_bytes(&load_blocks_page(task_state, limit, offset, status).await?) },
        )
        .await
}

async fn load_blocks_page(
    state: ApiState,
    limit: usize,
    offset: usize,
    status: Option<String>,
) -> anyhow::Result<PagedResponse<BlockPageItemResponse>> {
    let daemon_chain_height = state.node.chain_height();
    let store = Arc::clone(&state.store);
    let node = Arc::clone(&state.node);
    let (mut blocks, total) = state
        .public_blocking(move || {
            let (mut blocks, total) = store.get_blocks_page_up_to(
                daemon_chain_height,
                status.as_deref(),
                limit as i64,
                offset as i64,
            )?;
            flag_chain_mismatched_blocks(node.as_ref(), daemon_chain_height, &mut blocks);
            Ok::<_, anyhow::Error>((blocks, total))
        })
        .await?;
    for block in &mut blocks {
        hydrate_provisional_block_reward(block);
    }
    let target_hashes = blocks
        .iter()
        .map(|block| block.hash.clone())
        .collect::<HashSet<_>>();
    let store = Arc::clone(&state.store);
    let started_at = Instant::now();
    let luck_by_hash = state
        .public_blocking(move || {
            let hashes = target_hashes.into_iter().collect::<Vec<_>>();
            store.get_luck_rounds_for_hashes(&hashes)
        })
        .await;
    record_api_operation_observation(
        &state,
        "luck_details_load",
        started_at.elapsed(),
        luck_by_hash.is_err(),
    );
    let luck_by_hash = luck_by_hash
        .map_err(|err| err.context("failed loading block luck details"))?
        .into_iter()
        .map(|(hash, round)| (hash, luck_round_response_from_db(round)))
        .collect::<HashMap<_, _>>();
    let items = blocks
        .into_iter()
        .map(|block| {
            let block_hash = block.hash.clone();
            block_page_item_response(block, luck_by_hash.get(block_hash.as_str()))
        })
        .collect::<Vec<_>>();

    Ok(PagedResponse::new(items, total as usize))
}

#[derive(Clone, Serialize)]
struct MinerPendingBlockEstimate {
    height: u64,
    hash: String,
    estimated_credit: u64,
    credit_withheld: bool,
    validation_state: PendingPreviewValidation,
    validation_detail: String,
    confirmations_remaining: u64,
    timestamp: SystemTime,
}

#[derive(Clone, Serialize, Default)]
struct MinerPendingEstimate {
    estimated_pending: u64,
    blocks: Vec<MinerPendingBlockEstimate>,
}

#[derive(Clone, Serialize)]
struct MinerVerificationHold {
    mode: String,
    reason: Option<String>,
    started_at: Option<SystemTime>,
    verified_only_until: Option<SystemTime>,
    quarantined_until: Option<SystemTime>,
    validation_hold_cause: Option<pool_common::db::ValidationHoldCause>,
    validation_pending_provisional: Option<u64>,
}

#[derive(Clone, Default)]
struct AddressPreviewStats {
    seen_shares: u64,
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_difficulty_ready: u64,
    provisional_shares_delayed: u64,
    risky: bool,
}

impl AddressPreviewStats {
    fn eligible_difficulty(&self) -> u64 {
        self.verified_difficulty
            .saturating_add(self.provisional_difficulty_ready)
    }

    fn has_window_activity(&self) -> bool {
        self.seen_shares > 0
    }

    fn has_eligible_work(&self) -> bool {
        self.eligible_difficulty() > 0
    }

    fn verified_ratio(&self) -> f64 {
        let total = self.eligible_difficulty();
        if total == 0 {
            0.0
        } else {
            self.verified_difficulty as f64 / total as f64
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PendingPreviewValidation {
    Ready,
    AwaitingDelay,
    #[serde(rename = "awaiting_shares")]
    AwaitingVerifiedShares,
    ExtraVerification,
}

struct PreparedPendingEstimateBlock {
    block: DbBlock,
    confirmations: u64,
    previews: Vec<ShareWindowAddressPreview>,
}

fn collect_address_preview_stats_from_summary(
    preview: Option<&ShareWindowAddressPreview>,
    risky: bool,
) -> AddressPreviewStats {
    let Some(preview) = preview else {
        return AddressPreviewStats {
            risky,
            ..AddressPreviewStats::default()
        };
    };

    AddressPreviewStats {
        seen_shares: preview.seen_shares,
        verified_shares: preview.verified_shares,
        verified_difficulty: preview.verified_difficulty,
        provisional_difficulty_ready: preview.provisional_difficulty_ready,
        provisional_shares_delayed: preview.provisional_shares_delayed,
        risky,
    }
}

fn prepare_duration_pending_estimate_blocks(
    store: &PoolStore,
    blocks: Vec<DbBlock>,
    window_duration: Duration,
    provisional_ready_cutoff: Option<SystemTime>,
    chain_height: u64,
) -> anyhow::Result<Vec<PreparedPendingEstimateBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    let latest_share_timestamps = store.latest_share_timestamps_for_block_hashes(
        &blocks
            .iter()
            .map(|block| block.hash.clone())
            .collect::<Vec<_>>(),
    )?;
    let windows = blocks
        .iter()
        .map(|block| {
            let end = latest_share_timestamps
                .get(&block.hash)
                .copied()
                .map(|share_time| share_time.max(block.timestamp))
                .unwrap_or(block.timestamp);
            (end.checked_sub(window_duration).unwrap_or(UNIX_EPOCH), end)
        })
        .collect::<Vec<_>>();
    let previews = store.summarize_share_windows(&windows, provisional_ready_cutoff)?;
    Ok(blocks
        .into_iter()
        .zip(previews)
        .map(|(block, previews)| PreparedPendingEstimateBlock {
            confirmations: chain_height.saturating_sub(block.height),
            block,
            previews,
        })
        .collect())
}

fn prepare_pending_estimate_blocks(
    store: &PoolStore,
    config: &Config,
    now: SystemTime,
    provisional_delay: Duration,
    chain_height: u64,
) -> anyhow::Result<Vec<PreparedPendingEstimateBlock>> {
    let mut blocks = store.get_unconfirmed_blocks()?;
    blocks.retain(|block| !block.orphaned);
    for block in &mut blocks {
        hydrate_provisional_block_reward(block);
    }
    blocks.retain(|block| block.reward > 0);

    prepare_duration_pending_estimate_blocks(
        store,
        blocks,
        config.runtime.pplns_window_duration(),
        now.checked_sub(provisional_delay),
        chain_height,
    )
}

fn preview_weight(
    preview: &ShareWindowAddressPreview,
    force_verify_active: bool,
    trust_policy: PayoutTrustPolicy,
) -> Option<u64> {
    let provisional_difficulty = if force_verify_active {
        0
    } else {
        preview.provisional_difficulty_ready
    };
    if preview.verified_shares < trust_policy.min_verified_shares {
        return None;
    }

    let total_uncapped = preview
        .verified_difficulty
        .saturating_add(provisional_difficulty);
    if total_uncapped == 0 {
        return None;
    }

    let counted_provisional = if trust_policy.provisional_cap_multiplier <= 0.0 {
        provisional_difficulty
    } else {
        let provisional_cap = ((preview.verified_difficulty as f64)
            * trust_policy.provisional_cap_multiplier)
            .clamp(0.0, u64::MAX as f64) as u64;
        provisional_difficulty.min(provisional_cap)
    };
    let weight = preview
        .verified_difficulty
        .saturating_add(counted_provisional);
    (weight > 0).then_some(weight)
}

fn pending_preview_validation_state(
    stats: &AddressPreviewStats,
    trust_policy: PayoutTrustPolicy,
) -> PendingPreviewValidation {
    if stats.risky && stats.has_window_activity() {
        return PendingPreviewValidation::ExtraVerification;
    }
    if !stats.has_eligible_work() && stats.provisional_shares_delayed > 0 {
        return PendingPreviewValidation::AwaitingDelay;
    }
    if stats.verified_shares < trust_policy.min_verified_shares {
        return PendingPreviewValidation::AwaitingVerifiedShares;
    }
    PendingPreviewValidation::Ready
}

fn pending_preview_validation_detail(
    cfg: &Config,
    stats: &AddressPreviewStats,
    trust_policy: PayoutTrustPolicy,
    state: PendingPreviewValidation,
) -> String {
    match state {
        PendingPreviewValidation::Ready => format!(
            "{} verified share{} and {:.1}% verified difficulty in this payout window.",
            stats.verified_shares,
            if stats.verified_shares == 1 { "" } else { "s" },
            stats.verified_ratio() * 100.0,
        ),
        PendingPreviewValidation::AwaitingDelay => format!(
            "Shares are still inside the {} provisional delay, so the preview has not opened yet.",
            cfg.runtime.provisional_share_delay.trim(),
        ),
        PendingPreviewValidation::AwaitingVerifiedShares => format!(
            "{} of {} required verified share{} reached so far.",
            stats.verified_shares,
            trust_policy.min_verified_shares,
            if trust_policy.min_verified_shares == 1 {
                ""
            } else {
                "s"
            },
        ),
        PendingPreviewValidation::ExtraVerification => {
            "This address is under a verification hold, so only fully verified shares count toward this estimate and payout right now."
                .to_string()
        }
    }
}

fn pending_estimate_snapshot_can_serve(cache: &PendingEstimateSnapshotCache, now: Instant) -> bool {
    cache
        .updated_at
        .is_some_and(|updated_at| now.duration_since(updated_at) < MINER_PENDING_ESTIMATE_STALE_TTL)
}

fn pending_estimate_snapshot_needs_refresh(
    cache: &PendingEstimateSnapshotCache,
    chain_height: u64,
    now: Instant,
) -> bool {
    if cache.refresh_in_flight {
        return false;
    }
    let Some(last_requested_at) = cache.last_requested_at else {
        return false;
    };
    if now.duration_since(last_requested_at) >= MINER_PENDING_ESTIMATE_HOT_WINDOW {
        return false;
    }
    let Some(updated_at) = cache.updated_at else {
        return true;
    };
    cache.chain_height != Some(chain_height)
        || now.duration_since(updated_at) >= MINER_PENDING_ESTIMATE_REFRESH_AFTER
}

fn replace_pending_estimate_snapshot(
    cache: &mut PendingEstimateSnapshotCache,
    chain_height: u64,
    values: HashMap<String, MinerPendingEstimate>,
    now: Instant,
) {
    cache.updated_at = Some(now);
    cache.chain_height = Some(chain_height);
    cache.values = values;
    cache.refresh_in_flight = false;
}

fn miner_verification_hold(
    state: Option<&AddressRiskState>,
    validation_state: Option<&pool_common::db::ValidationHoldState>,
    now: SystemTime,
) -> Option<MinerVerificationHold> {
    let quarantined_until = state
        .and_then(|s| s.quarantined_until)
        .filter(|until| *until > now);
    let verified_only_until = state
        .and_then(|s| s.force_verify_until)
        .into_iter()
        .chain(validation_state.and_then(|s| s.forced_until))
        .filter(|until| *until > now)
        .max();
    if quarantined_until.is_none() && verified_only_until.is_none() {
        return None;
    }

    let validation_hold_cause = validation_state.and_then(|state| state.hold_cause);
    let validation_pending_provisional = validation_state.map(|state| state.pending_provisional);
    let validation_recent_verified_difficulty =
        validation_state.map(|state| state.recent_verified_difficulty);
    let validation_recent_provisional_difficulty =
        validation_state.map(|state| state.recent_provisional_difficulty);

    Some(MinerVerificationHold {
        mode: if quarantined_until.is_some() {
            "quarantined".to_string()
        } else {
            "verified_only".to_string()
        },
        reason: state
            .and_then(|s| s.last_reason.as_deref())
            .map(str::trim)
            .filter(|reason| !reason.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                validation_hold_cause.map(|cause| {
                    cause.hold_reason(
                        validation_pending_provisional.unwrap_or_default(),
                        validation_recent_verified_difficulty.unwrap_or_default(),
                        validation_recent_provisional_difficulty.unwrap_or_default(),
                    )
                })
            }),
        started_at: state
            .and_then(|s| s.last_event_at)
            .or_else(|| validation_state.and_then(|state| state.forced_started_at)),
        verified_only_until,
        quarantined_until,
        validation_hold_cause,
        validation_pending_provisional,
    })
}

fn estimate_unconfirmed_pending_snapshot(
    store: &PoolStore,
    config: &Config,
    now: SystemTime,
    chain_height: u64,
) -> anyhow::Result<HashMap<String, MinerPendingEstimate>> {
    let provisional_delay = config.runtime.provisional_share_delay_duration();
    let required_confirmations = config.runtime.blocks_before_payout.max(0) as u64;
    let trust_policy = PayoutTrustPolicy {
        min_verified_shares: config.runtime.payout_min_verified_shares.max(0) as u64,
        provisional_cap_multiplier: config.runtime.payout_provisional_cap_multiplier.max(0.0),
    };
    let preview_trust_policy = PayoutTrustPolicy {
        min_verified_shares: 0,
        provisional_cap_multiplier: 0.0,
    };

    let prepared_blocks =
        prepare_pending_estimate_blocks(store, config, now, provisional_delay, chain_height)?;
    let mut addresses_for_risk = HashSet::<String>::new();
    for prepared in &prepared_blocks {
        let block = &prepared.block;
        addresses_for_risk.insert(block.finder.clone());
        for preview in &prepared.previews {
            addresses_for_risk.insert(preview.address.clone());
        }
    }

    let force_verify_addresses = store
        .active_force_verify_addresses(&addresses_for_risk.into_iter().collect::<Vec<_>>(), now)?;
    let mut estimates = HashMap::<String, MinerPendingEstimate>::new();

    for prepared in prepared_blocks {
        let PreparedPendingEstimateBlock {
            block,
            confirmations,
            previews,
        } = prepared;
        let distributable = block
            .reward
            .saturating_sub(config.runtime.pool_fee(block.reward));
        if previews.is_empty() {
            continue;
        }

        let mut weights = HashMap::<String, u64>::new();
        let mut stats_by_address = HashMap::<String, AddressPreviewStats>::new();
        let mut total_weight = 0u64;
        let mut remainder_destination: Option<&str> = None;
        let mut remainder_weight = 0u64;

        for preview in &previews {
            let risky = force_verify_addresses.contains(&preview.address);
            let stats = collect_address_preview_stats_from_summary(Some(preview), risky);
            stats_by_address.insert(preview.address.clone(), stats);

            let Some(weight) = preview_weight(preview, risky, preview_trust_policy) else {
                continue;
            };
            total_weight = total_weight.saturating_add(weight);
            if remainder_destination.is_none()
                || weight > remainder_weight
                || (weight == remainder_weight
                    && preview.address.as_str() < remainder_destination.unwrap_or_default())
            {
                remainder_destination = Some(preview.address.as_str());
                remainder_weight = weight;
            }
            weights.insert(preview.address.clone(), weight);
        }

        let mut estimated_credits = HashMap::<String, u64>::new();
        if total_weight > 0 {
            allocate_weighted_credits(
                &mut estimated_credits,
                weights,
                total_weight,
                distributable,
            )?;
        }

        let mut addresses = previews
            .iter()
            .map(|preview| preview.address.clone())
            .collect::<HashSet<_>>();
        addresses.extend(estimated_credits.keys().cloned());

        for address in addresses {
            let stats = if let Some(stats) = stats_by_address.get(&address) {
                stats.clone()
            } else {
                let risky = force_verify_addresses.contains(&address);
                collect_address_preview_stats_from_summary(None, risky)
            };
            let estimated_credit = estimated_credits.get(&address).copied().unwrap_or_default();
            let validation_state = pending_preview_validation_state(&stats, trust_policy);
            let show_row = estimated_credit > 0
                || matches!(
                    validation_state,
                    PendingPreviewValidation::ExtraVerification
                )
                || (stats.has_window_activity() && !stats.has_eligible_work());
            if !show_row {
                continue;
            }

            let estimate = estimates.entry(address).or_default();
            estimate.estimated_pending =
                estimate.estimated_pending.saturating_add(estimated_credit);
            estimate.blocks.push(MinerPendingBlockEstimate {
                height: block.height,
                hash: block.hash.clone(),
                estimated_credit,
                credit_withheld: false,
                validation_state,
                validation_detail: pending_preview_validation_detail(
                    config,
                    &stats,
                    trust_policy,
                    validation_state,
                ),
                confirmations_remaining: required_confirmations.saturating_sub(confirmations),
                timestamp: block.timestamp,
            });
        }
    }

    for estimate in estimates.values_mut() {
        estimate.blocks.sort_by_key(|b| std::cmp::Reverse(b.height));
    }

    Ok(estimates)
}

#[cfg(test)]
fn estimate_unconfirmed_pending_for_miner(
    store: &PoolStore,
    address: &str,
    config: &Config,
    now: SystemTime,
    chain_height: u64,
) -> anyhow::Result<MinerPendingEstimate> {
    Ok(
        estimate_unconfirmed_pending_snapshot(store, config, now, chain_height)?
            .remove(address)
            .unwrap_or_default(),
    )
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

fn collect_reward_window_stats(
    shares: &[DbShare],
    now: SystemTime,
    provisional_delay: Duration,
) -> HashMap<String, RewardWindowAddressStats> {
    let mut by_address = HashMap::<String, RewardWindowAddressStats>::new();
    for share in shares {
        let entry = by_address.entry(share.miner.clone()).or_default();
        if is_verified_share_status(&share.status) {
            entry.verified_shares = entry.verified_shares.saturating_add(1);
            entry.verified_difficulty = entry.verified_difficulty.saturating_add(share.difficulty);
        } else if is_share_payout_eligible(share, now, provisional_delay) {
            entry.provisional_shares_eligible = entry.provisional_shares_eligible.saturating_add(1);
            entry.provisional_difficulty_eligible = entry
                .provisional_difficulty_eligible
                .saturating_add(share.difficulty);
        }
    }
    by_address
}

fn reward_participant_status(
    stats: Option<&RewardWindowAddressStats>,
    trust_policy: PayoutTrustPolicy,
    risky: bool,
) -> RewardParticipantStatus {
    let Some(stats) = stats else {
        return RewardParticipantStatus::RecordedOnly;
    };
    let provisional_difficulty_eligible = if risky {
        0
    } else {
        stats.provisional_difficulty_eligible
    };
    if stats.verified_shares < trust_policy.min_verified_shares {
        return RewardParticipantStatus::AwaitingVerifiedShares;
    }
    let eligible = stats
        .verified_difficulty
        .saturating_add(provisional_difficulty_eligible);
    if eligible == 0 {
        return RewardParticipantStatus::NoEligibleShares;
    }
    if trust_policy.provisional_cap_multiplier > 0.0 && stats.verified_difficulty > 0 {
        let provisional_cap = ((stats.verified_difficulty as f64)
            * trust_policy.provisional_cap_multiplier)
            .clamp(0.0, u64::MAX as f64) as u64;
        if provisional_difficulty_eligible > provisional_cap {
            return RewardParticipantStatus::CappedProvisional;
        }
    }
    RewardParticipantStatus::Included
}

fn compute_reward_mode(
    trust_policy: PayoutTrustPolicy,
    context: &RewardModeContext<'_>,
    stats_by_address: &HashMap<String, RewardWindowAddressStats>,
) -> anyhow::Result<RewardModeComputation> {
    let mut weights = HashMap::new();
    let mut total_weight = 0u64;
    for (address, stats) in stats_by_address {
        let preview = ShareWindowAddressPreview {
            verified_shares: stats.verified_shares,
            verified_difficulty: stats.verified_difficulty,
            provisional_difficulty_ready: stats.provisional_difficulty_eligible,
            ..ShareWindowAddressPreview::default()
        };
        if let Some(weight) = preview_weight(
            &preview,
            context
                .risky_by_address
                .get(address)
                .copied()
                .unwrap_or(false),
            trust_policy,
        ) {
            weights.insert(address.clone(), weight);
            total_weight = total_weight.saturating_add(weight);
        }
    }

    let mut statuses = HashMap::<String, RewardParticipantStatus>::new();
    for (address, stats) in stats_by_address {
        statuses.insert(
            address.clone(),
            reward_participant_status(
                Some(stats),
                trust_policy,
                context
                    .risky_by_address
                    .get(address)
                    .copied()
                    .unwrap_or(false),
            ),
        );
    }

    let mut credits = HashMap::<String, u64>::new();
    if total_weight == 0 {
        return Ok(RewardModeComputation {
            weights,
            credits,
            statuses,
            total_weight,
        });
    }

    allocate_weighted_credits(
        &mut credits,
        weights.clone(),
        total_weight,
        context.distributable_reward,
    )?;

    Ok(RewardModeComputation {
        weights,
        credits,
        statuses,
        total_weight,
    })
}

fn load_block_reward_window(
    store: &PoolStore,
    config: &Config,
    block: &DbBlock,
    now: SystemTime,
    provisional_delay: Duration,
) -> anyhow::Result<(
    HashMap<String, RewardWindowAddressStats>,
    RewardWindowSummary,
)> {
    let end = reward_window_end(store, block)?;
    let start = end
        .checked_sub(config.runtime.pplns_window_duration())
        .unwrap_or(UNIX_EPOCH);
    // Match the original raw query's second-resolution inclusive lower bound.
    let start = UNIX_EPOCH + Duration::from_secs(system_time_to_unix_secs(start));
    let cutoff = if provisional_delay.is_zero() {
        Some(end.max(now))
    } else {
        now.checked_sub(provisional_delay)
    };
    let previews = store
        .summarize_share_windows(&[(start, end)], cutoff)?
        .pop()
        .unwrap_or_default();
    let share_count = previews
        .iter()
        .map(|preview| preview.seen_shares as usize)
        .sum();
    let stats = previews
        .into_iter()
        .map(|preview| {
            (
                preview.address,
                RewardWindowAddressStats {
                    verified_shares: preview.verified_shares,
                    verified_difficulty: preview.verified_difficulty,
                    provisional_shares_eligible: preview.provisional_shares_ready,
                    provisional_difficulty_eligible: preview.provisional_difficulty_ready,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let participant_count = stats.len();
    Ok((
        stats,
        RewardWindowSummary {
            label: format!("PPLNS · {}", config.runtime.pplns_window_duration.trim()),
            share_count,
            participant_count,
        },
    ))
}

fn build_block_reward_breakdown(
    store: &PoolStore,
    config: &Config,
    height: u64,
    now: SystemTime,
) -> anyhow::Result<BlockRewardBreakdownResponse> {
    let mut block = store
        .get_block(height)?
        .ok_or_else(|| anyhow::anyhow!("block {height} not found"))?;
    hydrate_provisional_block_reward(&mut block);

    let fee_amount = config.runtime.pool_fee(block.reward);
    let distributable_reward = block.reward.saturating_sub(fee_amount);
    let provisional_delay = config.runtime.provisional_share_delay_duration();

    let (recorded_stats_by_address, share_window) =
        load_block_reward_window(store, config, &block, now, provisional_delay)?;

    let mut risky_by_address = HashMap::<String, bool>::new();
    let mut addresses_for_risk = recorded_stats_by_address
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    if !addresses_for_risk
        .iter()
        .any(|address| address == &block.finder)
    {
        addresses_for_risk.push(block.finder.clone());
    }
    for address in addresses_for_risk {
        let risky = match store.should_force_verify_address(&address) {
            Ok((force_verify, _)) => force_verify,
            Err(err) => {
                tracing::warn!(
                    address = %address,
                    error = %err,
                    height,
                    "failed risk check during block reward breakdown; treating address as risky"
                );
                true
            }
        };
        risky_by_address.insert(address, risky);
    }

    let preview_trust_policy = PayoutTrustPolicy {
        min_verified_shares: 0,
        provisional_cap_multiplier: 0.0,
    };
    let payout_trust_policy = PayoutTrustPolicy::from_values(
        config.runtime.payout_min_verified_shares,
        config.runtime.payout_provisional_cap_multiplier,
    );
    let reward_context = RewardModeContext {
        distributable_reward,
        risky_by_address: &risky_by_address,
    };
    let mut display_stats_by_address = recorded_stats_by_address.clone();

    let mut preview_mode = compute_reward_mode(
        preview_trust_policy,
        &reward_context,
        &display_stats_by_address,
    )?;

    let mut payout_mode = compute_reward_mode(
        payout_trust_policy,
        &reward_context,
        &display_stats_by_address,
    )?;
    if share_window.share_count > 0 && payout_mode.total_weight == 0 {
        // Preserve explicit replay recovery for zero-weight windows. Ordinary
        // breakdowns use summaries; replay alone needs the original share rows.
        let end = reward_window_end(store, &block)?;
        let start = end
            .checked_sub(config.runtime.pplns_window_duration())
            .unwrap_or(UNIX_EPOCH);
        let mut display_shares = store.get_shares_between(start, end)?;
        match recover_share_window_by_replay(
            store,
            &mut display_shares,
            now,
            provisional_delay,
            false,
        ) {
            Ok(recovery) if recovery.attempted => {
                display_stats_by_address =
                    collect_reward_window_stats(&display_shares, now, provisional_delay);
                preview_mode = compute_reward_mode(
                    preview_trust_policy,
                    &reward_context,
                    &display_stats_by_address,
                )?;
                payout_mode = compute_reward_mode(
                    payout_trust_policy,
                    &reward_context,
                    &display_stats_by_address,
                )?;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    height,
                    error = %err,
                    "failed dry-run replay recovery for block reward breakdown"
                );
            }
        }
    }

    let actual_events = store.get_block_credit_events(height)?;
    let actual_fee_amount = match store.get_block_pool_fee_event(height)? {
        Some(event) => Some(event.amount),
        None if block.orphaned => Some(0),
        None if fee_amount == 0 && (block.paid_out || !actual_events.is_empty()) => Some(0),
        None => None,
    };
    let actual_map = actual_events
        .iter()
        .map(|event| (event.address.clone(), event.amount))
        .collect::<HashMap<String, u64>>();

    let mut all_addresses = HashSet::<String>::new();
    all_addresses.extend(recorded_stats_by_address.keys().cloned());
    all_addresses.extend(display_stats_by_address.keys().cloned());
    all_addresses.extend(preview_mode.credits.keys().cloned());
    all_addresses.extend(payout_mode.credits.keys().cloned());
    all_addresses.extend(actual_map.keys().cloned());
    all_addresses.insert(block.finder.clone());

    let mut participants = all_addresses.into_iter().collect::<Vec<_>>();
    participants.sort_by(|a, b| {
        let a_actual = actual_map.get(a).copied().unwrap_or(0);
        let b_actual = actual_map.get(b).copied().unwrap_or(0);
        let a_expected = payout_mode
            .credits
            .get(a)
            .copied()
            .unwrap_or_else(|| preview_mode.credits.get(a).copied().unwrap_or(0));
        let b_expected = payout_mode
            .credits
            .get(b)
            .copied()
            .unwrap_or_else(|| preview_mode.credits.get(b).copied().unwrap_or(0));
        b_actual
            .cmp(&a_actual)
            .then_with(|| b_expected.cmp(&a_expected))
            .then_with(|| a.cmp(b))
    });

    let participant_rows = participants
        .into_iter()
        .map(|address| {
            let stats = display_stats_by_address.get(&address);
            let actual_credit = actual_map.get(&address).copied();
            let preview_status =
                preview_mode
                    .statuses
                    .get(&address)
                    .copied()
                    .unwrap_or_else(|| {
                        if actual_credit.is_some() {
                            RewardParticipantStatus::RecordedOnly
                        } else {
                            RewardParticipantStatus::NoEligibleShares
                        }
                    });
            let payout_status = payout_mode
                .statuses
                .get(&address)
                .copied()
                .unwrap_or_else(|| {
                    if actual_credit.is_some() {
                        RewardParticipantStatus::RecordedOnly
                    } else {
                        RewardParticipantStatus::NoEligibleShares
                    }
                });
            let payout_credit = payout_mode.credits.get(&address).copied().unwrap_or(0);

            BlockRewardParticipantResponse {
                finder: address == block.finder,
                risky: risky_by_address.get(&address).copied().unwrap_or(false),
                verified_shares: stats.map(|entry| entry.verified_shares).unwrap_or(0),
                verified_difficulty: stats.map(|entry| entry.verified_difficulty).unwrap_or(0),
                provisional_shares_eligible: stats
                    .map(|entry| entry.provisional_shares_eligible)
                    .unwrap_or(0),
                provisional_difficulty_eligible: stats
                    .map(|entry| entry.provisional_difficulty_eligible)
                    .unwrap_or(0),
                preview_weight: preview_mode.weights.get(&address).copied().unwrap_or(0),
                preview_share_pct: if preview_mode.total_weight == 0 {
                    0.0
                } else {
                    preview_mode.weights.get(&address).copied().unwrap_or(0) as f64 * 100.0
                        / preview_mode.total_weight as f64
                },
                preview_credit: preview_mode.credits.get(&address).copied().unwrap_or(0),
                preview_status,
                payout_weight: payout_mode.weights.get(&address).copied().unwrap_or(0),
                payout_share_pct: if payout_mode.total_weight == 0 {
                    0.0
                } else {
                    payout_mode.weights.get(&address).copied().unwrap_or(0) as f64 * 100.0
                        / payout_mode.total_weight as f64
                },
                payout_credit,
                payout_status,
                actual_credit,
                delta_vs_payout: actual_credit.map(|actual| actual as i64 - payout_credit as i64),
                address,
            }
        })
        .collect::<Vec<_>>();

    Ok(BlockRewardBreakdownResponse {
        block: BlockRewardBlockResponse {
            height: block.height,
            reward: block.reward,
            timestamp: block.timestamp,
            orphaned: block.orphaned,
            paid_out: block.paid_out,
        },
        share_window,
        fee_amount,
        distributable_reward,
        preview_total_weight: preview_mode.total_weight,
        payout_total_weight: payout_mode.total_weight,
        actual_credit_total: actual_events
            .iter()
            .fold(0u64, |sum, event| sum.saturating_add(event.amount)),
        actual_fee_amount,
        participants: participant_rows,
    })
}

fn miner_balance_response(
    balance: &Balance,
    pending_payout: Option<&PendingPayout>,
) -> MinerBalanceResponse {
    let pending_confirmed = balance.pending;
    let pending_queued = pending_payout.map(|queued| queued.amount).unwrap_or(0);
    MinerBalanceResponse {
        pending_confirmed,
        pending_queued,
        paid: balance.paid,
    }
}

fn apply_wallet_liquidity_to_payout_eta(
    payout_eta: &mut PayoutEtaResponse,
    wallet_balance: Option<&WalletBalance>,
) {
    let Some(wallet_balance) = wallet_balance else {
        return;
    };
    payout_eta.wallet_spendable = Some(wallet_balance.spendable);
    payout_eta.wallet_pending = Some(
        wallet_balance
            .pending
            .saturating_add(wallet_balance.pending_unconfirmed),
    );
}

fn missing_block_effort_rounds(mut blocks: Vec<DbBlock>) -> Vec<(SystemTime, DbBlock)> {
    blocks.sort_by_key(|block| block.timestamp);
    let mut blocks = blocks.into_iter();
    let Some(mut previous) = blocks.next() else {
        return Vec::new();
    };

    let mut missing = Vec::new();
    for current in blocks {
        if current.effort_pct.is_none() {
            missing.push((previous.timestamp, current.clone()));
        }
        previous = current;
    }
    missing
}

fn backfill_block_effort(store: &PoolStore) -> anyhow::Result<u64> {
    let missing = missing_block_effort_rounds(store.get_all_blocks()?);
    let mut updated = 0u64;
    for (round_start, mut block) in missing {
        let round_work = store.accepted_work_between(round_start, block.timestamp)?;
        block.effort_pct = Some(if block.difficulty > 0 {
            (round_work as f64 / block.difficulty as f64) * 100.0
        } else {
            0.0
        });
        store.add_block(&block)?;
        updated += 1;
    }
    Ok(updated)
}

#[cfg(test)]
fn backfill_round_heights(blocks: Vec<DbBlock>) -> Vec<(SystemTime, u64)> {
    missing_block_effort_rounds(blocks)
        .into_iter()
        .map(|(round_start, block)| (round_start, block.height))
        .collect()
}

fn compute_chain_aware_luck_page(
    store: &PoolStore,
    node: &NodeClient,
    max_height: u64,
    limit: usize,
    offset: usize,
) -> anyhow::Result<(Vec<LuckRoundResponse>, usize)> {
    // Aggregate in Postgres; do not load and decode every share in every round
    // or calculate skipped pages when the caller requests a later page.
    let (rows, total) =
        store.get_luck_rounds_page_up_to(limit as i64, offset as i64, max_height)?;
    let mut rounds = rows
        .into_iter()
        .map(luck_round_response_from_db)
        .collect::<Vec<_>>();
    flag_chain_mismatched_luck_rows(node, max_height, &mut rounds);
    Ok((rounds, total as usize))
}

fn compute_chain_aware_block_totals(
    store: &PoolStore,
    node: &NodeClient,
    daemon_chain_height: u64,
) -> anyhow::Result<(u64, u64, u64)> {
    let (unique_total_blocks, unique_orphaned_blocks) = store.get_unique_block_identity_counts()?;
    let (live_confirmed_blocks, live_orphaned_blocks, _live_pending_blocks) =
        store.get_block_status_counts()?;

    let recent_blocks = store.get_recent_blocks(CHAIN_AWARE_ORPHAN_LOOKBACK_BLOCKS)?;
    let heights = recent_blocks
        .iter()
        .filter(|b| !b.orphaned && b.height <= daemon_chain_height)
        .map(|b| b.height)
        .collect::<Vec<_>>();
    let hashes = node.get_block_hashes(&heights).unwrap_or_else(|err| {
        tracing::warn!(error = %err, "failed to read canonical block identities for totals");
        HashMap::new()
    });
    let (extra_orphaned_blocks, confirmed_blocks_to_reclassify) =
        count_chain_reclassifications(&recent_blocks, daemon_chain_height, &|height| {
            Ok(hashes.get(&height).cloned())
        })?;

    Ok((
        unique_total_blocks,
        live_confirmed_blocks.saturating_sub(confirmed_blocks_to_reclassify),
        unique_orphaned_blocks
            .max(live_orphaned_blocks)
            .saturating_add(extra_orphaned_blocks),
    ))
}

// Compare against a single canonical snapshot fetched by the caller.
fn count_chain_reclassifications<F>(
    blocks: &[DbBlock],
    chain_height: u64,
    lookup: &F,
) -> anyhow::Result<(u64, u64)>
where
    F: Fn(u64) -> anyhow::Result<Option<String>> + Sync,
{
    std::thread::scope(|scope| {
        let chunk_size = blocks.len().div_ceil(MAX_CONCURRENT_CHAIN_READS).max(1);
        let tasks = blocks.chunks(chunk_size).map(|chunk| scope.spawn(move || {
            let mut extra = 0u64;
            let mut confirmed = 0u64;
            for block in chunk {
                if block.orphaned { continue; }
                let mismatch = if block.height > chain_height {
                    true
                } else {
                    match lookup(block.height) {
                        Ok(Some(hash)) => hash != block.hash,
                        Ok(None) => false,
                        Err(err) => {
                            tracing::warn!(height = block.height, error = %err,
                                "failed to compare pool block against daemon while computing effective totals");
                            false
                        }
                    }
                };
                if mismatch {
                    extra += 1;
                    confirmed += u64::from(block.confirmed);
                }
            }
            (extra, confirmed)
        })).collect::<Vec<_>>();
        let mut totals = (0u64, 0u64);
        for task in tasks {
            let (extra, confirmed) = task
                .join()
                .map_err(|_| anyhow::anyhow!("chain comparison worker panicked"))?;
            totals.0 += extra;
            totals.1 += confirmed;
        }
        Ok(totals)
    })
}

fn flag_chain_mismatched_blocks(
    node: &NodeClient,
    daemon_chain_height: u64,
    blocks: &mut [DbBlock],
) {
    let heights = blocks
        .iter()
        .filter(|b| !b.orphaned && b.height <= daemon_chain_height)
        .map(|b| b.height)
        .collect::<Vec<_>>();
    match node.get_block_hashes(&heights) {
        Ok(hashes) => {
            for block in blocks {
                if hashes
                    .get(&block.height)
                    .is_some_and(|hash| hash != &block.hash)
                {
                    block.confirmed = false;
                    block.orphaned = true;
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "failed to compare pool blocks against daemon"),
    }
}

fn flag_chain_mismatched_luck_rows(
    node: &NodeClient,
    daemon_chain_height: u64,
    rows: &mut [LuckRoundResponse],
) {
    let heights = rows
        .iter()
        .filter(|r| !r.orphaned && r.block_height <= daemon_chain_height)
        .map(|r| r.block_height)
        .collect::<Vec<_>>();
    match node.get_block_hashes(&heights) {
        Ok(hashes) => {
            for row in rows {
                if hashes
                    .get(&row.block_height)
                    .is_some_and(|hash| hash != &row.block_hash)
                {
                    row.confirmed = false;
                    row.orphaned = true;
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "failed to compare luck rows against daemon"),
    }
}

fn luck_round_response_from_db(round: DbLuckRound) -> LuckRoundResponse {
    let effort_pct = if round.difficulty > 0 {
        (round.round_work as f64 / round.difficulty as f64) * 100.0
    } else {
        0.0
    };
    LuckRoundResponse {
        block_height: round.block_height,
        block_hash: round.block_hash,
        timestamp: round.timestamp,
        effort_pct,
        duration_seconds: round.duration_seconds,
        orphaned: round.orphaned,
        confirmed: round.confirmed,
    }
}

fn block_page_item_response(
    block: DbBlock,
    luck: Option<&LuckRoundResponse>,
) -> BlockPageItemResponse {
    BlockPageItemResponse {
        height: block.height,
        hash: block.hash,
        reward: block.reward,
        timestamp: block.timestamp,
        confirmed: block.confirmed,
        orphaned: block.orphaned,
        effort_pct: luck.map(|row| row.effort_pct),
        duration_seconds: luck.map(|row| row.duration_seconds),
    }
}

async fn handle_luck_history(
    Query(query): Query<PageQuery>,
    client: ClientIp,
    State(state): State<ApiState>,
) -> Response {
    let (limit, offset) = page_bounds(query.limit, query.offset);
    let task_state = state.clone();
    state
        .serve_public_read(
            &client,
            "luck",
            format!("luck:{limit}:{offset}"),
            PUBLIC_LUCK_CACHE_TTL,
            "failed loading luck history",
            async move {
                let store = Arc::clone(&task_state.store);
                let node = Arc::clone(&task_state.node);
                let daemon_chain_height = task_state.node.chain_height();
                let started_at = Instant::now();
                let result = task_state
                    .public_blocking(move || {
                        compute_chain_aware_luck_page(
                            store.as_ref(),
                            node.as_ref(),
                            daemon_chain_height,
                            limit,
                            offset,
                        )
                    })
                    .await;
                record_api_operation_observation(
                    &task_state,
                    "luck_page_load",
                    started_at.elapsed(),
                    result.is_err(),
                );
                let (items, total) = result?;
                json_bytes(&PagedResponse::new(items, total))
            },
        )
        .await
}

async fn handle_public_payouts(
    Query(query): Query<PageQuery>,
    client: ClientIp,
    State(state): State<ApiState>,
) -> Response {
    let (limit, offset) = page_bounds(query.limit, query.offset);
    let task_state = state.clone();
    state
        .serve_public_read(
            &client,
            "payouts_recent",
            format!("payouts_recent:{limit}:{offset}"),
            PUBLIC_PAYOUTS_CACHE_TTL,
            "failed loading payouts",
            async move {
                let store = Arc::clone(&task_state.store);
                let (batches, total) = task_state
                    .public_blocking(move || {
                        store.get_public_payout_batches_page(limit as i64, offset as i64)
                    })
                    .await?;
                json_bytes(&PagedResponse::new(batches, total as usize))
            },
        )
        .await
}

async fn handle_admin_block_reward_breakdown(
    Path(height): Path<u64>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let cfg = state.config.clone();
    let now = SystemTime::now();
    let task_state = state.clone();
    match state
        .reward_breakdown_reads
        .read(height, move || {
            let started = Instant::now();
            let result = build_block_reward_breakdown(&store, &cfg, height, now)
                .and_then(|value| serde_json::to_value(value).map_err(Into::into));
            record_api_task_observation(
                &task_state,
                "reward_breakdown_compute",
                started.elapsed(),
                result.is_err(),
            );
            result
        })
        .await
    {
        ReadResult::Ready(result) => match result.as_ref() {
            Ok(breakdown) => Json(breakdown.clone()).into_response(),
            Err(err) => {
                if err.contains("not found") {
                    return error_response(StatusCode::NOT_FOUND, err.clone());
                }
                internal_error(
                    "failed loading block reward breakdown",
                    anyhow::anyhow!(err.clone()),
                )
            }
        },
        ReadResult::Pending => (
            StatusCode::ACCEPTED,
            [
                (header::RETRY_AFTER, "1"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            Json(json!({"status": "pending"})),
        )
            .into_response(),
        ReadResult::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            Json(json!({"error": "reward breakdown busy; retry shortly"})),
        )
            .into_response(),
    }
}

impl ApiState {
    pub(crate) fn new(
        config: Config,
        store: Arc<PoolStore>,
        jobs: Arc<JobManager>,
        node: Arc<NodeClient>,
    ) -> Self {
        let detail_concurrency = (config.runtime.database_pool_size.max(2) as usize / 2)
            .clamp(1, MAX_CONCURRENT_MINER_DETAIL_LOADS);
        // Leave most of a small DB pool for balances, stats, and background
        // work; cached public reads rarely miss.
        let public_read_concurrency = ((config.runtime.database_pool_size.max(1) as usize - 1) / 2)
            .clamp(1, MAX_CONCURRENT_PUBLIC_READ_LOADS);
        let client_ip_policy = Arc::new(ClientIpPolicy::new(
            &config.api_client_ip_header,
            &config.api_trusted_proxies,
        ));
        Self {
            recovery: Arc::new(RecoveryAgentClient::new(
                config.recovery.socket_path.clone(),
            )),
            miner_release: crate::miner_release::MinerRelease::new(&config.latest_miner_version),
            config,
            store,
            jobs,
            node,
            db_totals_cache: Arc::new(Mutex::new(DbTotalsCache::default())),
            network_hashrate_cache: Arc::new(Mutex::new(NetworkHashrateCache::default())),
            insights_cache: Arc::new(Mutex::new(InsightsCache::new())),
            rejection_analytics_cache: Arc::new(Mutex::new(RejectionAnalyticsCache::default())),
            stats_response_cache: Arc::new(Mutex::new(StatsResponseCache::new())),
            cache_refresh_locks: Arc::new(CacheRefreshLocks::default()),
            pending_estimate_snapshot_cache: Arc::new(Mutex::new(
                PendingEstimateSnapshotCache::default(),
            )),
            pending_estimate_snapshot_notify: Arc::new(Notify::new()),
            miner_balance_response_cache: Arc::new(Mutex::new(MinerBalanceResponseCache::new())),
            miner_detail_response_cache: Arc::new(Mutex::new(MinerDetailResponseCache::new())),
            miner_detail_loads: Arc::new(Semaphore::new(detail_concurrency)),
            reward_breakdown_reads: Arc::new(BackgroundReads::default()),
            site_metrics: Arc::new(crate::site_metrics::SiteMetrics::default()),
            public_telemetry_rate_limiter: Arc::new(Mutex::new(
                PublicTelemetryRateLimiter::default(),
            )),
            client_ip_policy,
            public_read_cache: Arc::new(Mutex::new(PublicReadCache::new(
                PUBLIC_READ_CACHE_MAX_ENTRIES,
                PUBLIC_READ_CACHE_MAX_BYTES,
                PUBLIC_READ_STALE_TTL,
            ))),
            public_read_loads: Arc::new(Semaphore::new(public_read_concurrency)),
            checkpoint_file_cache: Arc::new(Mutex::new(None)),
            monitor_ingest_last_checked_at: Arc::new(tokio::sync::Mutex::new(None)),
            admin_auth_failures: Arc::new(Mutex::new(WindowedCounter::new(
                ADMIN_AUTH_FAILURE_WINDOW,
                ADMIN_AUTH_FAILURE_MAX_CLIENTS,
            ))),
            performance: Arc::new(ApiPerformanceTracker::default()),
            live_runtime_snapshot_cache: Arc::new(Mutex::new(LiveRuntimeSnapshotCache::new())),
            started_at: Instant::now(),
        }
    }

    async fn persisted_runtime_snapshot(&self) -> Option<PersistedRuntimeSnapshot> {
        let _refresh = self.cache_refresh_locks.lock("runtime_snapshot").await;
        {
            let cache = self.live_runtime_snapshot_cache.lock();
            if let Some(value) = cache.get(LIVE_RUNTIME_SNAPSHOT_CACHE_TTL) {
                return value;
            }
        }

        let store = Arc::clone(&self.store);
        let started_at = Instant::now();
        let loaded = match spawn_blocking_result(
            move || -> anyhow::Result<Option<PersistedRuntimeSnapshot>> {
                let Some(raw) = store.get_meta(LIVE_RUNTIME_SNAPSHOT_META_KEY)? else {
                    return Ok(None);
                };
                Ok(Some(serde_json::from_slice(&raw)?))
            },
        )
        .await
        {
            Ok(value) => {
                record_api_operation_observation(
                    self,
                    "persisted_runtime_snapshot_load",
                    started_at.elapsed(),
                    false,
                );
                value
            }
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "persisted_runtime_snapshot_load",
                    started_at.elapsed(),
                    true,
                );
                tracing::warn!(error = %err, "failed loading persisted live runtime snapshot");
                None
            }
        };

        let mut cache = self.live_runtime_snapshot_cache.lock();
        cache.set(loaded.clone());
        loaded
    }

    async fn cached_stats_response(&self) -> anyhow::Result<StatsResponse> {
        let _refresh = self.cache_refresh_locks.lock("stats").await;
        {
            let cache = self.stats_response_cache.lock();
            if let Some(value) = cache.get(STATS_RESPONSE_CACHE_TTL) {
                self.performance.caches.record_hit("stats_response");
                return Ok(value);
            }
        }

        self.performance.caches.record_miss("stats_response");
        let fresh = self.load_stats_response().await?;
        let mut cache = self.stats_response_cache.lock();
        cache.set(fresh.clone());
        Ok(fresh)
    }

    async fn load_stats_response(&self) -> anyhow::Result<StatsResponse> {
        let started_at = Instant::now();
        let connected_miners = self
            .persisted_runtime_snapshot()
            .await
            .map(|snapshot| snapshot.connected_miners)
            .unwrap_or_default();
        let totals = self.db_totals().await?;
        let current_job = self.jobs.current_job();
        let current_job_height = current_job.as_ref().map(|j| j.height);
        let network_hashrate = self.network_hashrate_for_job(current_job.as_ref()).await;

        let store = Arc::clone(&self.store);
        let pool_hashrate_started_at = Instant::now();
        let (pool_hashrate, avg_effort_pct, effort_sample_size) =
            tokio::task::spawn_blocking(move || {
                let hashrate = db_pool_hashrate(&store);
                let since = SystemTime::now()
                    .checked_sub(POOL_YIELD_WINDOW)
                    .unwrap_or(UNIX_EPOCH);
                let (avg_effort_pct, sample_size) =
                    store.avg_effort_pct_since(since).unwrap_or((None, 0));
                (hashrate, avg_effort_pct, sample_size)
            })
            .await
            .unwrap_or((0.0, None, 0));
        record_api_operation_observation(
            self,
            "pool_hashrate_load",
            pool_hashrate_started_at.elapsed(),
            false,
        );

        let response = StatsResponse {
            pool: PoolSummary {
                miners: connected_miners,
                hashrate: pool_hashrate,
                raw_hashrate: pool_hashrate,
                effective_hashrate: avg_effort_pct
                    .filter(|effort| {
                        effort_sample_size >= MIN_POOL_YIELD_SAMPLE_SIZE
                            && effort.is_finite()
                            && *effort > 0.0
                    })
                    .map(|effort| pool_hashrate * 100.0 / effort),
                block_yield_efficiency_pct: avg_effort_pct
                    .filter(|effort| {
                        effort_sample_size >= MIN_POOL_YIELD_SAMPLE_SIZE
                            && effort.is_finite()
                            && *effort > 0.0
                    })
                    .map(|effort| 10_000.0 / effort),
                block_yield_sample_size: effort_sample_size,
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
                orphan_rate_30d_pct: if totals.blocks_30d == 0 {
                    0.0
                } else {
                    (totals.orphaned_blocks_30d as f64 / totals.blocks_30d as f64) * 100.0
                },
                paid_to_miners_total: totals.paid_to_miners_total,
            },
            chain: ChainSummary {
                current_job_height,
                network_hashrate,
            },
        };
        record_api_operation_observation(self, "stats_load", started_at.elapsed(), false);
        Ok(response)
    }

    async fn admin_balance_overview(&self) -> anyhow::Result<AdminBalanceOverviewResponse> {
        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        let next_sweep_at = self
            .persisted_runtime_snapshot()
            .await
            .and_then(|snapshot| snapshot.payouts.next_sweep_at);
        let fee_address = self
            .config
            .runtime
            .pool_fee_wallet_address
            .trim()
            .to_string();
        spawn_blocking_result(move || {
            std::thread::scope(|scope| {
                let wallet = scope.spawn(|| node.get_wallet_balance());
                let balances = store.get_all_balances()?;
                let source_by_address = store
                    .list_pending_balance_source_summaries()?
                    .into_iter()
                    .map(|source| (source.address.clone(), source))
                    .collect::<HashMap<_, BalanceSourceSummary>>();
                let pending_payouts = store.get_pending_payouts()?;
                let is_pool_fee_balance =
                    |address: &str| !fee_address.is_empty() && address.trim() == fee_address;
                let pool_fee_paid_total = balances
                    .iter()
                    .filter(|balance| is_pool_fee_balance(&balance.address))
                    .fold(0u64, |acc, balance| acc.saturating_add(balance.paid));
                let pool_fee_unpaid_total = balances
                    .iter()
                    .filter(|balance| is_pool_fee_balance(&balance.address))
                    .fold(0u64, |acc, balance| acc.saturating_add(balance.pending));
                let miner_paid_total = balances
                    .iter()
                    .filter(|balance| !is_pool_fee_balance(&balance.address))
                    .fold(0u64, |acc, balance| acc.saturating_add(balance.paid));
                let miner_unpaid_total = balances
                    .iter()
                    .filter(|balance| !is_pool_fee_balance(&balance.address))
                    .fold(0u64, |acc, balance| acc.saturating_add(balance.pending));
                let mut clean_unpaid_count = 0usize;
                let mut clean_unpaid_amount = 0u64;
                let mut orphan_backed_unpaid_amount = 0u64;
                let mut balance_source_drift_amount = 0u64;
                let mut pool_fee_clean_unpaid_amount = 0u64;
                let mut pool_fee_orphan_backed_unpaid_amount = 0u64;
                let mut pool_fee_balance_source_drift_amount = 0u64;
                for source in source_by_address.values() {
                    if is_pool_fee_balance(&source.address) {
                        pool_fee_clean_unpaid_amount =
                            pool_fee_clean_unpaid_amount.saturating_add(source.canonical_pending);
                        pool_fee_orphan_backed_unpaid_amount = pool_fee_orphan_backed_unpaid_amount
                            .saturating_add(source.orphan_pending);
                    } else {
                        if source.canonical_pending > 0 {
                            clean_unpaid_count = clean_unpaid_count.saturating_add(1);
                        }
                        clean_unpaid_amount =
                            clean_unpaid_amount.saturating_add(source.canonical_pending);
                        orphan_backed_unpaid_amount =
                            orphan_backed_unpaid_amount.saturating_add(source.orphan_pending);
                    }
                }
                for balance in &balances {
                    let source = source_by_address
                        .get(&balance.address)
                        .cloned()
                        .unwrap_or_default();
                    let source_total = source
                        .canonical_pending
                        .saturating_add(source.orphan_pending);
                    let balance_above_sources = balance.pending.saturating_sub(source_total);
                    if is_pool_fee_balance(&balance.address) {
                        pool_fee_balance_source_drift_amount = pool_fee_balance_source_drift_amount
                            .saturating_add(balance_above_sources);
                    } else {
                        balance_source_drift_amount =
                            balance_source_drift_amount.saturating_add(balance_above_sources);
                    }
                }
                let queued_amount = pending_payouts
                    .iter()
                    .fold(0u64, |acc, payout| acc.saturating_add(payout.amount));
                let net_block_reward_total = store.get_total_confirmed_block_rewards()?;
                let pool_fee_total = store.get_total_pool_fees()?;

                let wallet_balance = wallet
                    .join()
                    .map_err(|_| anyhow::anyhow!("wallet balance worker panicked"))??;
                Ok::<_, anyhow::Error>(AdminBalanceOverviewResponse {
                    wallet: AdminBalanceOverviewWallet {
                        spendable: wallet_balance.spendable,
                        pending: wallet_balance.pending,
                        total: wallet_balance.total,
                    },
                    payouts: AdminBalanceOverviewPayouts {
                        clean_unpaid_count,
                        queued_count: pending_payouts.len(),
                        queued_amount,
                        next_sweep_at,
                    },
                    ledger: AdminBalanceOverviewLedger {
                        miner_paid_total,
                        miner_unpaid_total,
                        miner_clean_unpaid_total: clean_unpaid_amount,
                        miner_orphan_backed_unpaid_total: orphan_backed_unpaid_amount,
                        miner_balance_source_drift_total: balance_source_drift_amount,
                        net_block_reward_total,
                        pool_fee_total,
                        pool_fee_clean_unpaid_total: pool_fee_clean_unpaid_amount,
                        pool_fee_orphan_backed_unpaid_total: pool_fee_orphan_backed_unpaid_amount,
                        pool_fee_balance_source_drift_total: pool_fee_balance_source_drift_amount,
                        pool_fee_balance_total: pool_fee_paid_total
                            .saturating_add(pool_fee_unpaid_total),
                    },
                })
            })
        })
        .await
    }

    async fn admin_reconciliation_issues(
        &self,
    ) -> anyhow::Result<AdminReconciliationIssuesResponse> {
        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        spawn_blocking_result(move || {
            let orphaned_blocks = store.list_orphaned_block_credit_issues()?;
            let payout_rows = store.list_unreconciled_completed_payout_rows()?;

            let mut grouped = HashMap::<String, Vec<UnreconciledCompletedPayoutRow>>::new();
            for row in payout_rows {
                grouped.entry(row.tx_hash.clone()).or_default().push(row);
            }

            let mut tx_rows = grouped.into_iter().collect::<Vec<_>>();
            tx_rows.sort_by(|(_, a), (_, b)| {
                let a_latest = a
                    .iter()
                    .map(|row| row.timestamp)
                    .max()
                    .unwrap_or(UNIX_EPOCH);
                let b_latest = b
                    .iter()
                    .map(|row| row.timestamp)
                    .max()
                    .unwrap_or(UNIX_EPOCH);
                b_latest.cmp(&a_latest)
            });

            let txids = tx_rows
                .iter()
                .map(|(hash, _)| hash.clone())
                .collect::<Vec<_>>();
            let existing = node.get_existing_transactions(&txids)?;
            let mut missing_payouts = Vec::<AdminMissingCompletedPayoutIssueResponse>::new();
            for (tx_hash, rows) in tx_rows {
                if existing.contains(&tx_hash) {
                    continue;
                }

                let mut addresses = rows
                    .iter()
                    .map(|row| row.address.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                addresses.sort();

                let total_amount = rows
                    .iter()
                    .fold(0u64, |acc, row| acc.saturating_add(row.amount));
                let total_fee = rows
                    .iter()
                    .fold(0u64, |acc, row| acc.saturating_add(row.fee));
                let linked_amount = rows
                    .iter()
                    .fold(0u64, |acc, row| acc.saturating_add(row.linked_amount));
                let orphaned_linked_amount = rows.iter().fold(0u64, |acc, row| {
                    acc.saturating_add(row.orphaned_linked_amount)
                });
                let latest_timestamp = rows
                    .iter()
                    .map(|row| row.timestamp)
                    .max()
                    .unwrap_or(UNIX_EPOCH);

                missing_payouts.push(AdminMissingCompletedPayoutIssueResponse {
                    tx_hash,
                    payout_row_count: rows.len(),
                    total_amount,
                    total_fee,
                    latest_timestamp,
                    addresses,
                    live_linked_amount: linked_amount.saturating_sub(orphaned_linked_amount),
                    orphaned_linked_amount,
                    unlinked_amount: total_amount.saturating_sub(linked_amount),
                });
            }

            let orphaned_blocks = orphaned_blocks
                .into_iter()
                .map(|issue| AdminOrphanedBlockIssueResponse {
                    height: issue.height,
                    hash: issue.hash,
                    credit_event_count: issue.credit_event_count,
                    credited_address_count: issue.credited_address_count,
                    remaining_credit_amount: issue.remaining_credit_amount,
                    paid_credit_amount: issue.paid_credit_amount,
                    remaining_fee_amount: issue.remaining_fee_amount,
                    paid_fee_amount: issue.paid_fee_amount,
                    pending_payout_count: issue.pending_payout_count,
                    broadcast_pending_payout_count: issue.broadcast_pending_payout_count,
                })
                .collect::<Vec<_>>();

            Ok(AdminReconciliationIssuesResponse {
                generated_at: SystemTime::now(),
                missing_payouts,
                orphaned_blocks,
            })
        })
        .await
    }

    async fn resolve_missing_completed_payout_issue(
        &self,
        tx_hash: &str,
        action: ManualCompletedPayoutResolutionKind,
    ) -> anyhow::Result<()> {
        if self.node.get_tx_status_optional(tx_hash)?.is_some() {
            return Err(anyhow::anyhow!(
                "tx {} still exists on the current daemon chain",
                tx_hash
            ));
        }

        let store = Arc::clone(&self.store);
        let tx_hash_owned = tx_hash.to_string();
        spawn_blocking_result(move || {
            store.resolve_completed_payout_tx_override(&tx_hash_owned, action)
        })
        .await?;

        self.clear_miner_response_caches();
        Ok(())
    }

    async fn import_confirmed_wallet_payouts(
        &self,
        tx_hashes: Vec<String>,
    ) -> anyhow::Result<AdminReconciliationPayoutImportResponse> {
        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        let config = self.config.clone();
        let (report, imported_txs) = spawn_blocking_result(move || {
            let idempotency_path = daemon_send_idempotency_path(&config);
            let payout_txs = load_confirmed_payout_import_txs(&idempotency_path, &tx_hashes)?;
            for payout_tx in &payout_txs {
                let Some(status) = node.get_tx_status_optional(&payout_tx.tx_hash)? else {
                    return Err(anyhow::anyhow!(
                        "tx {} is missing from the current daemon chain",
                        payout_tx.tx_hash
                    ));
                };
                if status.confirmations == 0 {
                    return Err(anyhow::anyhow!(
                        "tx {} is not confirmed on the current daemon chain",
                        payout_tx.tx_hash
                    ));
                }
            }

            let report = store.import_confirmed_payout_txs(&payout_txs)?;
            let imported_txs = payout_txs
                .into_iter()
                .map(|payout_tx| {
                    let total_amount = payout_tx
                        .recipients
                        .iter()
                        .fold(0u64, |acc, recipient| acc.saturating_add(recipient.amount));
                    let total_fee = payout_tx
                        .recipients
                        .iter()
                        .fold(0u64, |acc, recipient| acc.saturating_add(recipient.fee));
                    let mut addresses = payout_tx
                        .recipients
                        .iter()
                        .map(|recipient| recipient.address.clone())
                        .collect::<Vec<_>>();
                    addresses.sort();
                    addresses.dedup();
                    AdminReconciliationImportedPayoutTxResponse {
                        tx_hash: payout_tx.tx_hash,
                        payout_row_count: payout_tx.recipients.len(),
                        total_amount,
                        total_fee,
                        timestamp: payout_tx.timestamp,
                        addresses,
                    }
                })
                .collect::<Vec<_>>();
            Ok::<_, anyhow::Error>((report, imported_txs))
        })
        .await?;

        self.clear_miner_response_caches();

        Ok(AdminReconciliationPayoutImportResponse {
            imported_tx_count: report.imported_txs,
            imported_payout_rows: report.imported_payout_rows,
            imported_amount: report.imported_amount,
            imported_fee: report.imported_fee,
            canceled_pending_payouts: report.canceled_pending_payouts,
            recorded_manual_offset_amount: report.recorded_manual_offset_amount,
            imported_txs,
        })
    }

    async fn apply_live_manual_payout_offsets(
        &self,
    ) -> anyhow::Result<AdminReconciliationManualOffsetApplyResponse> {
        let store = Arc::clone(&self.store);
        let report =
            spawn_blocking_result(move || store.apply_manual_payout_offsets_to_live_pending())
                .await?;

        self.clear_miner_response_caches();

        Ok(AdminReconciliationManualOffsetApplyResponse {
            scanned_offset_addresses: report.scanned_offset_addresses,
            offset_amount_before: report.offset_amount_before,
            applied_address_count: report.applied_address_count,
            applied_amount: report.applied_amount,
            remaining_offset_amount: report.remaining_offset_amount,
            applications: report
                .applications
                .into_iter()
                .map(
                    |application| AdminReconciliationManualOffsetApplicationResponse {
                        address: application.address,
                        applied_amount: application.applied_amount,
                        remaining_offset_amount: application.remaining_offset_amount,
                        remaining_balance_pending: application.remaining_balance_pending,
                        remaining_canonical_pending: application.remaining_canonical_pending,
                    },
                )
                .collect(),
        })
    }

    async fn admin_share_diagnostics(&self) -> anyhow::Result<AdminShareDiagnosticsResponse> {
        let now = SystemTime::now();
        let persisted_runtime = self.persisted_runtime_snapshot().await;
        let validation = persisted_runtime
            .as_ref()
            .map(|snapshot| snapshot.validation)
            .unwrap_or_default();
        let submit = persisted_runtime
            .as_ref()
            .map(|snapshot| snapshot.submit.clone())
            .unwrap_or_default();
        let store = Arc::clone(&self.store);
        let windows =
            spawn_blocking_result(move || collect_admin_share_windows(&store, now)).await?;

        Ok(AdminShareDiagnosticsResponse {
            windows,
            submit,
            validation,
        })
    }

    fn clear_miner_response_caches(&self) {
        self.miner_balance_response_cache.lock().clear();
        self.miner_detail_response_cache.lock().clear();
    }

    async fn cached_pending_estimate_for_miner(
        &self,
        address: &str,
        chain_height: u64,
    ) -> anyhow::Result<MinerPendingEstimate> {
        // The snapshot load must only ever run in a detached task: if it ran
        // inline here and the client disconnected, the request future would be
        // dropped mid-load and `refresh_in_flight` would stay true forever,
        // hanging every later balance request on `notified()`.
        let mut wait_attempts = 0u32;
        loop {
            let now = Instant::now();
            let mut schedule_refresh = false;
            let mut cached_values = None;
            let mut should_load = false;
            {
                let mut cache = self.pending_estimate_snapshot_cache.lock();
                cache.last_requested_at = Some(now);
                if pending_estimate_snapshot_can_serve(&cache, now) {
                    if pending_estimate_snapshot_needs_refresh(&cache, chain_height, now) {
                        cache.refresh_in_flight = true;
                        schedule_refresh = true;
                    }
                    cached_values = Some(cache.values.clone());
                } else if !cache.refresh_in_flight {
                    cache.refresh_in_flight = true;
                    should_load = true;
                }
            }
            if let Some(values) = cached_values {
                if schedule_refresh {
                    self.spawn_pending_estimate_snapshot_refresh(chain_height);
                }
                self.performance.caches.record_hit("pending_estimate");
                return Ok(values.get(address).cloned().unwrap_or_default());
            }
            if should_load {
                self.performance.caches.record_miss("pending_estimate");
                self.spawn_pending_estimate_snapshot_refresh(chain_height);
            }
            wait_attempts += 1;
            if wait_attempts > PENDING_ESTIMATE_SNAPSHOT_MAX_WAIT_ATTEMPTS {
                return Ok(MinerPendingEstimate::default());
            }
            // `Notify::notify_waiters` only wakes tasks already parked in
            // `notified()`, so a waiter can race the wakeup; the timeout also
            // keeps a slow or failed refresh from holding requests past
            // nginx's proxy timeout. Degrade to an empty estimate instead.
            if tokio::time::timeout(
                PENDING_ESTIMATE_SNAPSHOT_WAIT_TIMEOUT,
                self.pending_estimate_snapshot_notify.notified(),
            )
            .await
            .is_err()
            {
                return Ok(MinerPendingEstimate::default());
            }
        }
    }

    fn spawn_pending_estimate_snapshot_refresh(&self, chain_height: u64) {
        let state = self.clone();
        tokio::spawn(async move {
            state.refresh_pending_estimate_snapshot(chain_height).await;
        });
    }

    async fn load_pending_estimate_snapshot(
        &self,
        chain_height: u64,
    ) -> anyhow::Result<HashMap<String, MinerPendingEstimate>> {
        let store = Arc::clone(&self.store);
        let cfg = self.config.clone();
        let now = SystemTime::now();
        let started_at = Instant::now();
        let result = spawn_blocking_result(move || {
            estimate_unconfirmed_pending_snapshot(&store, &cfg, now, chain_height)
        })
        .await;

        match result {
            Ok(values) => {
                record_api_operation_observation(
                    self,
                    "pending_estimate_snapshot_load",
                    started_at.elapsed(),
                    false,
                );
                let mut cache = self.pending_estimate_snapshot_cache.lock();
                replace_pending_estimate_snapshot(
                    &mut cache,
                    chain_height,
                    values.clone(),
                    Instant::now(),
                );
                self.pending_estimate_snapshot_notify.notify_waiters();
                Ok(values)
            }
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "pending_estimate_snapshot_load",
                    started_at.elapsed(),
                    true,
                );
                self.pending_estimate_snapshot_cache
                    .lock()
                    .refresh_in_flight = false;
                self.pending_estimate_snapshot_notify.notify_waiters();
                Err(err)
            }
        }
    }

    async fn refresh_pending_estimate_snapshot(&self, chain_height: u64) {
        let started_at = Instant::now();
        match self.load_pending_estimate_snapshot(chain_height).await {
            Ok(_) => {
                record_api_task_observation(
                    self,
                    "pending_estimate_snapshot_refresh",
                    started_at.elapsed(),
                    false,
                );
            }
            Err(err) => {
                tracing::warn!(error = %err, "background pending estimate snapshot refresh failed");
                record_api_task_observation(
                    self,
                    "pending_estimate_snapshot_refresh",
                    started_at.elapsed(),
                    true,
                );
            }
        }
    }

    async fn cached_miner_balance_payload(
        &self,
        address: &str,
        include_pending_estimate: bool,
    ) -> anyhow::Result<MinerBalancePayload> {
        let cache_key = format!("{address}:{include_pending_estimate}");
        let _refresh = self
            .cache_refresh_locks
            .lock(&format!("balance:{cache_key}"))
            .await;
        if let Some(cached) = {
            let mut cache = self.miner_balance_response_cache.lock();
            cache.get(
                &cache_key,
                MINER_BALANCE_RESPONSE_CACHE_TTL,
                MINER_BALANCE_RESPONSE_CACHE_MAX_ENTRIES,
            )
        } {
            self.performance.caches.record_hit("miner_balance");
            return Ok(cached);
        }

        self.performance.caches.record_miss("miner_balance");
        let fresh = self
            .load_miner_balance_payload(address, include_pending_estimate)
            .await?;
        let mut cache = self.miner_balance_response_cache.lock();
        cache.insert(
            cache_key,
            fresh.clone(),
            MINER_BALANCE_RESPONSE_CACHE_TTL,
            MINER_BALANCE_RESPONSE_CACHE_MAX_ENTRIES,
        );
        Ok(fresh)
    }

    async fn cached_miner_detail_payload(
        &self,
        address: &str,
    ) -> anyhow::Result<MinerDetailPayload> {
        let cache_key = address.to_string();
        let _refresh = self
            .cache_refresh_locks
            .lock(&format!("detail:{cache_key}"))
            .await;
        if let Some(cached) = {
            let mut cache = self.miner_detail_response_cache.lock();
            cache.get(
                &cache_key,
                MINER_DETAIL_RESPONSE_CACHE_TTL,
                MINER_DETAIL_RESPONSE_CACHE_MAX_ENTRIES,
            )
        } {
            self.performance.caches.record_hit("miner_detail");
            return Ok(cached);
        }

        self.performance.caches.record_miss("miner_detail");
        let fresh = self.load_miner_detail_payload(address).await?;
        let mut cache = self.miner_detail_response_cache.lock();
        cache.insert(
            cache_key,
            fresh.clone(),
            MINER_DETAIL_RESPONSE_CACHE_TTL,
            MINER_DETAIL_RESPONSE_CACHE_MAX_ENTRIES,
        );
        Ok(fresh)
    }

    async fn load_miner_balance_payload(
        &self,
        address: &str,
        include_pending_estimate: bool,
    ) -> anyhow::Result<MinerBalancePayload> {
        let started_at = Instant::now();
        let chain_height = self.node.chain_height();
        let addr = address.to_string();
        let store = Arc::clone(&self.store);
        let db_result = spawn_blocking_result(
            move || -> anyhow::Result<(Balance, Option<PendingPayout>, bool)> {
                Ok((
                    store.get_balance(&addr)?,
                    store.get_pending_payout(&addr)?,
                    store.miner_has_any_activity(&addr)?,
                ))
            },
        )
        .await;
        let (balance, pending_payout, has_activity) = match db_result {
            Ok(value) => value,
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "miner_balance_load",
                    started_at.elapsed(),
                    true,
                );
                return Err(err);
            }
        };

        let pending_estimate = if include_pending_estimate {
            if has_activity {
                match self
                    .cached_pending_estimate_for_miner(address, chain_height)
                    .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        tracing::warn!(
                            address = %address,
                            error = %err,
                            "failed loading pending estimate for miner balance"
                        );
                        MinerPendingEstimate::default()
                    }
                }
            } else {
                MinerPendingEstimate::default()
            }
        } else {
            MinerPendingEstimate::default()
        };

        let payload = MinerBalancePayload {
            address: address.to_string(),
            balance: miner_balance_response(&balance, pending_payout.as_ref()),
            pending_estimate,
        };
        record_api_operation_observation(self, "miner_balance_load", started_at.elapsed(), false);
        Ok(payload)
    }

    async fn load_miner_detail_payload(&self, address: &str) -> anyhow::Result<MinerDetailPayload> {
        let started_at = Instant::now();
        let store = Arc::clone(&self.store);
        let provisional_cutoff = SystemTime::now()
            .checked_sub(self.config.runtime.provisional_share_delay_duration())
            .unwrap_or(UNIX_EPOCH);
        let addr = address.to_string();
        let db_result = limited_blocking_result(Arc::clone(&self.miner_detail_loads), move || {
            std::thread::scope(|scope| {
                let worker_snapshot = scope.spawn(|| {
                    let now = SystemTime::now();
                    store.miner_worker_snapshot(
                        &addr,
                        now.checked_sub(Duration::from_secs(86400))
                            .unwrap_or(UNIX_EPOCH),
                        now.checked_sub(HASHRATE_WINDOW).unwrap_or(UNIX_EPOCH),
                    )
                });
                let shares = store.get_shares_for_miner(&addr, MINER_DETAIL_SHARE_LIMIT)?;
                let mining_since = store.first_share_at_for_miner(&addr)?;
                let payouts = store
                    .get_recent_visible_payouts_for_address(&addr, MINER_PAYOUT_HISTORY_LIMIT)?;
                let hr = db_miner_hashrate(&store, &addr);
                let blocks_found = store.get_block_count_for_miner(&addr)?;
                let risk_state = store.get_address_risk(&addr)?;
                let validation_state = store.validation_hold_state(&addr, provisional_cutoff)?;
                let has_any_activity = store.miner_has_any_activity(&addr)?;
                let workers = worker_snapshot
                    .join()
                    .map_err(|_| anyhow::anyhow!("worker snapshot reader panicked"))??;
                Ok::<_, anyhow::Error>((
                    shares,
                    mining_since,
                    payouts,
                    hr,
                    workers.totals,
                    workers.hashrate,
                    workers.versions,
                    blocks_found,
                    risk_state,
                    validation_state,
                    has_any_activity,
                ))
            })
        })
        .await;
        let db_result = match db_result {
            Ok(value) => value,
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "miner_detail_load",
                    started_at.elapsed(),
                    true,
                );
                return Err(err);
            }
        };
        let (
            shares,
            mining_since,
            payouts,
            hashrate,
            workers_raw,
            worker_hashrate_raw,
            worker_versions_raw,
            blocks_found,
            risk_state,
            validation_state,
            has_any_activity,
        ) = db_result;

        let verification_hold = miner_verification_hold(
            risk_state.as_ref(),
            validation_state.as_ref(),
            SystemTime::now(),
        );

        let now = SystemTime::now();
        let worker_hashrate_by_name = worker_hashrate_by_name(hashrate, worker_hashrate_raw);
        let total_accepted: u64 = workers_raw.iter().map(|(_, a, _, _, _)| *a).sum();
        let total_rejected: u64 = workers_raw.iter().map(|(_, _, r, _, _)| *r).sum();
        let workers_active = filter_active_workers_for_miner(workers_raw, now, HASHRATE_WINDOW);
        let workers_sorted = sort_workers_for_miner(
            workers_active,
            &worker_hashrate_by_name,
            now,
            HASHRATE_WINDOW,
        );

        let latest_miner_version = self.miner_release.version();
        let worker_versions: HashMap<String, (String, Option<String>)> = worker_versions_raw
            .into_iter()
            .map(|(worker, version, backend)| (worker, (version, backend)))
            .collect();
        let worker_rows = workers_sorted
            .iter()
            .map(|(worker, accepted, rejected, _total_diff, last_share_ts)| {
                let worker_hr = worker_hashrate_by_name.get(worker).copied().unwrap_or(0.0);
                let (miner_version, backend) = worker_versions
                    .get(worker)
                    .map(|(version, backend)| (Some(version.clone()), backend.clone()))
                    .unwrap_or((None, None));
                let version_status =
                    worker_version_status(miner_version.as_deref(), &latest_miner_version);
                MinerWorkerResponse {
                    worker: worker.clone(),
                    hashrate: worker_hr,
                    accepted: *accepted,
                    rejected: *rejected,
                    last_share_at: *last_share_ts,
                    miner_version,
                    backend,
                    version_status,
                }
            })
            .collect();
        let share_rows = shares
            .into_iter()
            .map(|share| MinerShareResponse {
                job_id: share.job_id,
                worker: share.worker,
                difficulty: share.difficulty,
                status: share.status,
                created_at: share.created_at,
            })
            .collect::<Vec<_>>();
        let payout_rows = payouts
            .into_iter()
            .map(|payout| MinerPayoutResponse {
                amount: payout.amount,
                fee: payout.fee,
                tx_hash: payout.tx_hash,
                timestamp: payout.timestamp,
                confirmed: payout.confirmed,
            })
            .collect::<Vec<_>>();

        let body = MinerDetailResponse {
            shares: share_rows,
            mining_since,
            hashrate,
            verification_hold,
            payouts: payout_rows,
            workers: worker_rows,
            blocks_found,
            total_accepted,
            total_rejected,
            latest_miner_version,
            error: (!has_any_activity).then(|| "miner not found".to_string()),
        };

        let payload = MinerDetailPayload {
            found: has_any_activity,
            body,
        };
        record_api_operation_observation(self, "miner_detail_load", started_at.elapsed(), false);
        Ok(payload)
    }

    async fn stats_insights(&self) -> anyhow::Result<StatsInsightsResponse> {
        let _refresh = self.cache_refresh_locks.lock("insights").await;
        {
            let cache = self.insights_cache.lock();
            if let Some(value) = cache.get(INSIGHTS_CACHE_TTL) {
                return Ok(value);
            }
        }

        let current_job = self.jobs.current_job();
        let current_difficulty = current_job
            .as_ref()
            .map(|job| job.network_difficulty.max(1));
        let network_hashrate = self.network_hashrate_for_job(current_job.as_ref()).await;
        let daemon_chain_height = self.node.chain_height();

        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        let now = SystemTime::now();
        let (pool_hashrate, round_start, round_work, mut payout_eta, luck_history, avg_effort_pct) =
            spawn_blocking_result(move || {
                let pool_hashrate = db_pool_hashrate(&store);

                let mut blocks = store.get_recent_blocks_up_to(64, daemon_chain_height)?;
                flag_chain_mismatched_blocks(node.as_ref(), daemon_chain_height, &mut blocks);
                let round_start = blocks
                    .iter()
                    .filter(|block| !block.orphaned)
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

                let pending_total_amount = store
                    .get_pending_payouts()?
                    .iter()
                    .fold(0u64, |acc, payout| acc.saturating_add(payout.amount));
                let payout_eta = PayoutEtaResponse {
                    next_sweep_at: None,
                    pending_total_amount,
                    wallet_spendable: None,
                    wallet_pending: None,
                };
                let luck_history = compute_chain_aware_luck_page(
                    store.as_ref(),
                    node.as_ref(),
                    daemon_chain_height,
                    16,
                    0,
                )?
                .0;
                let avg_effort_pct = store.avg_effort_pct_up_to(daemon_chain_height)?;

                Ok::<_, anyhow::Error>((
                    pool_hashrate,
                    round_start,
                    round_work,
                    payout_eta,
                    luck_history,
                    avg_effort_pct,
                ))
            })
            .await?;

        let node = Arc::clone(&self.node);
        let wallet_balance = spawn_blocking_result(move || node.get_wallet_balance())
            .await
            .ok();
        let persisted_runtime = self.persisted_runtime_snapshot().await;
        if let Some(snapshot) = persisted_runtime.as_ref() {
            payout_eta.next_sweep_at = snapshot.payouts.next_sweep_at;
        }
        apply_wallet_liquidity_to_payout_eta(&mut payout_eta, wallet_balance.as_ref());

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

        let response = StatsInsightsResponse {
            round: RoundProgressResponse {
                elapsed_seconds,
                effort_pct,
                expected_block_seconds,
                timer_effort_pct,
            },
            payout_eta,
            avg_effort_pct,
            luck_history,
            rejections: RejectionAnalyticsSnapshot::default(),
        };

        let mut cache = self.insights_cache.lock();
        cache.set(response.clone());
        Ok(response)
    }

    async fn rejection_analytics_snapshot(
        &self,
        window: Duration,
    ) -> anyhow::Result<RejectionAnalyticsSnapshot> {
        let window_seconds = window.as_secs().max(1);
        let _refresh = self
            .cache_refresh_locks
            .lock(&format!("rejections:{window_seconds}"))
            .await;
        {
            let cache = self.rejection_analytics_cache.lock();
            if let Some(entry) = cache.entries.get(&window_seconds) {
                if entry.updated_at.elapsed() < REJECTION_ANALYTICS_CACHE_TTL {
                    self.performance.caches.record_hit("rejection_analytics");
                    return Ok(entry.value.clone());
                }
            }
        }

        self.performance.caches.record_miss("rejection_analytics");
        let since = SystemTime::now().checked_sub(window).unwrap_or(UNIX_EPOCH);
        let started_at = Instant::now();
        let outcome_store = Arc::clone(&self.store);
        let by_reason_store = Arc::clone(&self.store);
        let totals_by_reason_store = Arc::clone(&self.store);
        let outcome_task =
            tokio::task::spawn_blocking(move || outcome_store.share_outcome_counts_since(since));
        let by_reason_task = tokio::task::spawn_blocking(move || {
            by_reason_store.rejection_reason_counts_since(since)
        });
        let totals_by_reason_task = tokio::task::spawn_blocking(move || {
            totals_by_reason_store.total_rejection_reason_counts()
        });
        let (outcome_result, by_reason_result, totals_by_reason_result) =
            tokio::join!(outcome_task, by_reason_task, totals_by_reason_task);
        let load_value = || {
            let (accepted, rejected) = join_result(outcome_result)?;
            let by_reason = join_result(by_reason_result)?;
            let totals_by_reason = join_result(totals_by_reason_result)?;
            Ok::<_, anyhow::Error>((accepted, rejected, by_reason, totals_by_reason))
        };
        let (accepted, rejected, by_reason, totals_by_reason) = match load_value() {
            Ok(value) => {
                record_api_operation_observation(
                    self,
                    "rejection_analytics_load",
                    started_at.elapsed(),
                    false,
                );
                value
            }
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "rejection_analytics_load",
                    started_at.elapsed(),
                    true,
                );
                return Err(err);
            }
        };
        let snapshot = RejectionAnalyticsSnapshot {
            accepted,
            rejected,
            by_reason,
            totals_by_reason,
        };

        let mut cache = self.rejection_analytics_cache.lock();
        cache.entries.insert(
            window_seconds,
            TimedCacheEntry {
                updated_at: Instant::now(),
                value: snapshot.clone(),
            },
        );
        Ok(snapshot)
    }

    async fn build_status_response(&self) -> anyhow::Result<StatusPageResponse> {
        let now = SystemTime::now();
        let pool_uptime_seconds = self.started_at.elapsed().as_secs();
        let uptime_windows = [
            ("10m", Duration::from_secs(10 * 60)),
            ("6h", Duration::from_secs(6 * 3600)),
            ("24h", Duration::from_secs(24 * 3600)),
            ("7d", Duration::from_secs(7 * 24 * 3600)),
        ];
        let store = Arc::clone(&self.store);
        let (latest_local, latest_external, incidents, uptime_summaries) = self
            .public_blocking(move || {
                let latest_local =
                    store.get_latest_monitor_heartbeat(Some(LOCAL_MONITOR_SOURCE))?;
                let latest_external =
                    store.get_latest_monitor_heartbeat(Some(CLOUDFLARE_MONITOR_SOURCE))?;
                let incidents = store.get_recent_monitor_incidents(32, Some("public"))?;
                let mut uptime_summaries = Vec::with_capacity(uptime_windows.len());
                for (label, window) in uptime_windows {
                    let since = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
                    uptime_summaries.push((
                        label.to_string(),
                        store.get_monitor_uptime_summary(since, Some(LOCAL_MONITOR_SOURCE))?,
                        store.get_monitor_uptime_summary(since, Some(CLOUDFLARE_MONITOR_SOURCE))?,
                    ));
                }
                Ok::<_, anyhow::Error>((latest_local, latest_external, incidents, uptime_summaries))
            })
            .await?;

        let latest_local = latest_local.as_ref();
        let latest_external = latest_external.as_ref();
        let template_age = latest_local.and_then(|row| row.template_age_seconds);
        let template_refresh_millis = latest_local.and_then(|row| row.last_refresh_millis);
        let services = StatusServices {
            public_http: service_health_from_heartbeat(
                latest_external,
                PUBLIC_MONITOR_HEALTH_TTL,
                |row| row.public_http_up,
            ),
            api: service_health_from_heartbeat(latest_local, LOCAL_MONITOR_HEALTH_TTL, |row| {
                row.api_up
            }),
            stratum: service_health_from_heartbeat(latest_local, LOCAL_MONITOR_HEALTH_TTL, |row| {
                row.stratum_up
            }),
            database: service_health_from_heartbeat(
                latest_local,
                LOCAL_MONITOR_HEALTH_TTL,
                |row| Some(row.db_up),
            ),
            daemon: service_health_from_heartbeat(latest_local, LOCAL_MONITOR_HEALTH_TTL, |row| {
                row.daemon_up
            }),
        };
        let healthy = services.api.healthy
            && services.stratum.healthy
            && services.database.healthy
            && services.daemon.healthy
            && !latest_local
                .and_then(|row| row.daemon_syncing)
                .unwrap_or(false)
            && template_refresh_millis.is_none_or(|lag| lag < TEMPLATE_REFRESH_WARN_AFTER_MILLIS)
            && latest_external
                .and_then(|row| row.public_http_up)
                .unwrap_or(true);
        let daemon = daemon_health_from_heartbeat(latest_local);
        let template = TemplateHealth {
            observed: template_refresh_millis.is_some() || template_age.is_some(),
            fresh: template_refresh_millis
                .is_some_and(|lag| lag < TEMPLATE_REFRESH_WARN_AFTER_MILLIS),
            age_seconds: template_age,
            last_refresh_millis: template_refresh_millis,
        };
        let uptime = uptime_summaries
            .into_iter()
            .map(|(label, local, external)| build_monitor_uptime_window(&label, &local, &external))
            .collect();

        Ok(StatusPageResponse {
            healthy,
            pool_uptime_seconds,
            services,
            daemon,
            template,
            uptime,
            incidents: incidents
                .into_iter()
                .map(|incident| status_incident_from_monitor(incident, now))
                .collect(),
        })
    }

    async fn db_totals(&self) -> anyhow::Result<DbTotals> {
        let _refresh = self.cache_refresh_locks.lock("db_totals").await;
        let chain_height = self.node.chain_height();
        {
            let cache = self.db_totals_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < DB_TOTALS_CACHE_TTL)
                && cache.chain_height == Some(chain_height)
            {
                return Ok(cache.totals);
            }
        }

        let started_at = Instant::now();
        let block_totals_store = Arc::clone(&self.store);
        let block_totals_node = Arc::clone(&self.node);
        let paid_to_miners_store = Arc::clone(&self.store);
        let block_totals_task =
            tokio::task::spawn_blocking(move || -> anyhow::Result<(u64, u64, u64)> {
                compute_chain_aware_block_totals(
                    block_totals_store.as_ref(),
                    block_totals_node.as_ref(),
                    chain_height,
                )
            });
        let paid_to_miners_task =
            tokio::task::spawn_blocking(move || paid_to_miners_store.get_total_paid_to_miners());
        let windowed_blocks_store = Arc::clone(&self.store);
        let windowed_blocks_task = tokio::task::spawn_blocking(move || {
            let cutoff = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|now| now.as_secs() as i64)
                .unwrap_or_default()
                - 30 * 24 * 3600;
            windowed_blocks_store.get_block_counts_since(cutoff)
        });
        let (block_totals_result, paid_to_miners_result, windowed_blocks_result) =
            tokio::join!(block_totals_task, paid_to_miners_task, windowed_blocks_task);
        let load_value = || -> anyhow::Result<DbTotals> {
            let (total_blocks, confirmed_blocks, orphaned_blocks) =
                join_result(block_totals_result)?;
            let paid_to_miners_total = join_result(paid_to_miners_result)?;
            let (blocks_30d, orphaned_blocks_30d) = join_result(windowed_blocks_result)?;
            Ok(DbTotals {
                total_blocks,
                confirmed_blocks,
                orphaned_blocks,
                blocks_30d,
                orphaned_blocks_30d,
                paid_to_miners_total,
            })
        };
        let totals = match load_value() {
            Ok(value) => {
                record_api_operation_observation(
                    self,
                    "db_totals_load",
                    started_at.elapsed(),
                    false,
                );
                value
            }
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "db_totals_load",
                    started_at.elapsed(),
                    true,
                );
                return Err(err);
            }
        };

        let mut cache = self.db_totals_cache.lock();
        cache.totals = totals;
        cache.chain_height = Some(chain_height);
        cache.updated_at = Some(Instant::now());
        Ok(totals)
    }

    async fn network_hashrate_for_job(
        &self,
        job: Option<&pool_runtime::engine::Job>,
    ) -> Option<f64> {
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
        let started_at = Instant::now();
        let sampled = spawn_blocking_result(move || {
            estimate_explorer_network_hashrate_hps(node.as_ref(), chain_height, difficulty)
        })
        .await
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0);
        record_api_operation_observation(
            self,
            "network_hashrate_load",
            started_at.elapsed(),
            sampled.is_none(),
        );

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
    // Match the daemon explorer: observed work / observed time over the most
    // recent positive block intervals.
    if chain_height < 2 {
        return Ok(0.0);
    }

    let mut total_time = 0i64;
    let mut total_work = 0u64;
    let mut count = 0usize;
    let mut current = node.get_block_by_height_optional(chain_height)?;
    let mut height = chain_height;

    while height > 0 && count < EXPLORER_HASHRATE_SAMPLE_COUNT {
        let previous = node.get_block_by_height_optional(height - 1)?;
        if let (Some(block), Some(prev_block)) = (&current, &previous) {
            let block_time = block.timestamp - prev_block.timestamp;
            if block_time > 0 {
                total_time += block_time;
                // Keep compatibility with older daemon responses that did not
                // expose per-block difficulty, while preferring exact work
                // whenever it is available.
                total_work = total_work.saturating_add(if block.difficulty > 0 {
                    block.difficulty
                } else {
                    difficulty
                });
                count += 1;
            }
        }
        current = previous;
        height -= 1;
    }

    if count > 0 && total_time > 0 {
        return Ok(total_work as f64 / total_time as f64);
    }

    Ok(0.0)
}

fn public_telemetry_route_kind_for_path(path: &str) -> Option<PublicTelemetryRouteKind> {
    if path == "/api/stats" {
        return Some(PublicTelemetryRouteKind::Stats);
    }

    let rest = path.strip_prefix("/api/miner/")?;
    let mut parts = rest.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(address), None, None) if !address.is_empty() => Some(PublicTelemetryRouteKind::Miner),
        (Some(address), Some("balance"), None) if !address.is_empty() => {
            Some(PublicTelemetryRouteKind::Miner)
        }
        _ => None,
    }
}

fn api_performance_route_name(uri: &Uri) -> Option<&'static str> {
    let path = uri.path();
    if !path.starts_with("/api/") {
        return None;
    }

    Some(match path {
        "/api/stats" => "stats",
        "/api/checkpoints" => "checkpoints",
        "/api/stats/history" => "stats_history",
        "/api/stats/insights" => "stats_insights",
        "/api/luck" => "luck",
        "/api/status" => "status",
        "/api/monitor/public" => "monitor_public",
        "/api/blocks" => "blocks",
        "/api/payouts/recent" => "payouts_recent",
        "/api/health" => "health",
        "/api/admin/perf" => "admin_perf",
        "/api/admin/balance-overview" => "admin_balance_overview",
        "/api/admin/reconciliation/issues" => "admin_reconciliation_issues",
        "/api/admin/reconciliation/payouts/resolve" => "admin_reconciliation_payout_resolution",
        "/api/admin/reconciliation/payouts/import-confirmed" => {
            "admin_reconciliation_payout_import"
        }
        "/api/admin/reconciliation/manual-offsets/apply-live-pending" => {
            "admin_reconciliation_manual_offset_apply"
        }
        "/api/admin/reconciliation/orphan-blocks/retry-cleanup" => {
            "admin_orphaned_block_cleanup_retry"
        }
        "/api/admin/shares" => "admin_share_diagnostics",
        _ if path.ends_with("/balance") && path.starts_with("/api/miner/") => "miner_balance",
        _ if path.starts_with("/api/miner/") && !path.ends_with("/hashrate") => "miner_detail",
        _ => "other_api",
    })
}

fn record_api_route_observation(
    state: &ApiState,
    route: &str,
    status: StatusCode,
    duration: Duration,
) {
    let failed = status.as_u16() >= 400;
    let slow = duration.as_millis()
        >= match route {
            "stats" | "stats_history" | "health" | "miner_balance" | "payouts_recent" => 100,
            "luck" => 500,
            "admin_balance_overview"
            | "admin_reconciliation_issues"
            | "admin_reconciliation_payout_import"
            | "admin_reconciliation_manual_offset_apply"
            | "admin_reconciliation_payout_resolution"
            | "admin_orphaned_block_cleanup_retry"
            | "admin_share_diagnostics" => 500,
            _ => 250,
        };
    state
        .performance
        .routes
        .record(route, duration, failed, slow);
    if failed {
        tracing::warn!(
            component = "api_perf",
            operation = "route",
            route,
            status = status.as_u16(),
            duration_ms = duration.as_millis() as u64,
            "api request completed with error"
        );
    } else if slow || state.performance.should_sample_success() {
        tracing::info!(
            component = "api_perf",
            operation = "route",
            route,
            status = status.as_u16(),
            duration_ms = duration.as_millis() as u64,
            "api request observed"
        );
    }
}

fn record_api_operation_observation(
    state: &ApiState,
    operation: &str,
    duration: Duration,
    failed: bool,
) {
    let slow = duration.as_millis()
        >= match operation {
            "persisted_runtime_snapshot_load"
            | "stats_load"
            | "db_totals_load"
            | "rejection_analytics_load"
            | "pool_hashrate_load"
            | "luck_page_load"
            | "luck_details_load" => 100,
            "pending_estimate_snapshot_load" => 500,
            _ => 250,
        };
    state
        .performance
        .operations
        .record(operation, duration, failed, slow);
    if failed {
        tracing::warn!(
            component = "api_perf",
            operation,
            duration_ms = duration.as_millis() as u64,
            "api blocking operation failed"
        );
    } else if slow || state.performance.should_sample_success() {
        tracing::info!(
            component = "api_perf",
            operation,
            duration_ms = duration.as_millis() as u64,
            "api blocking operation observed"
        );
    }
}

fn record_api_task_observation(state: &ApiState, task: &str, duration: Duration, failed: bool) {
    let slow = duration.as_millis()
        >= match task {
            "pending_estimate_snapshot_refresh" => 500,
            _ => 250,
        };
    state.performance.tasks.record(task, duration, failed, slow);
    if failed {
        tracing::warn!(
            component = "api_perf",
            operation = task,
            duration_ms = duration.as_millis() as u64,
            "api background task failed"
        );
    } else if slow || state.performance.should_sample_success() {
        tracing::info!(
            component = "api_perf",
            operation = task,
            duration_ms = duration.as_millis() as u64,
            "api background task observed"
        );
    }
}

async fn observe_api_request_performance(
    State(state): State<ApiState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let route = api_performance_route_name(req.uri());
    let started_at = Instant::now();
    let response = next.run(req).await;
    if let Some(route) = route {
        record_api_route_observation(&state, route, response.status(), started_at.elapsed());
    }
    response
}

async fn limit_public_telemetry_requests(
    State(state): State<ApiState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let route = public_telemetry_route_kind_for_path(req.uri().path());
    let client_ip = state.client_ip(req.headers(), req.extensions());
    if let (Some(route), Some(client_ip)) = (route, client_ip.key()) {
        let allowed = {
            let mut limiter = state.public_telemetry_rate_limiter.lock();
            limiter.allow(client_ip, route, Instant::now())
        };
        if !allowed {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(
                    header::RETRY_AFTER,
                    PUBLIC_TELEMETRY_RATE_LIMIT_RETRY_AFTER_SECS.to_string(),
                )],
                Json(serde_json::json!({
                    "error":"rate limit exceeded",
                    "detail":"telemetry endpoint polled too aggressively; back off and retry"
                })),
            )
                .into_response();
        }
    }

    next.run(req).await
}

impl ApiState {
    pub(crate) fn public_base_url(&self) -> String {
        crate::ui::public_base_url(&self.config.pool_url)
    }

    fn client_ip(&self, headers: &HeaderMap, extensions: &axum::http::Extensions) -> ClientIp {
        let peer = extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|info| info.0.ip());
        ClientIp(self.client_ip_policy.resolve(headers, peer).map(client_key))
    }

    fn public_db_statement_timeout(&self) -> Option<Duration> {
        match self.config.api_public_db_statement_timeout_ms {
            0 => None,
            millis => Some(Duration::from_millis(millis)),
        }
    }

    /// Run a blocking store read for a public request under the public
    /// `statement_timeout`.
    async fn public_blocking<F, R>(&self, operation: F) -> anyhow::Result<R>
    where
        F: FnOnce() -> anyhow::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let timeout = self.public_db_statement_timeout();
        spawn_blocking_result(move || with_statement_timeout(timeout, operation)).await
    }

    /// Serve an expensive public read through the shared response cache.
    ///
    /// Fresh hits are returned without limits. A miss is single-flighted per
    /// key, counts against the client's [`PUBLIC_READ_MISS_RATE_LIMIT`], and
    /// needs one of the few `public_read_loads` permits. When any of those
    /// would refuse the request, or the load fails, a stale copy is served if
    /// one exists; otherwise 429/503/500. Loads run detached so a dropped
    /// request still completes and caches its result.
    async fn serve_public_read<Fut>(
        &self,
        client: &ClientIp,
        cache_name: &'static str,
        key: String,
        ttl: Duration,
        error_message: &'static str,
        load: Fut,
    ) -> Response
    where
        Fut: Future<Output = anyhow::Result<Bytes>> + Send + 'static,
    {
        let lookup = self
            .public_read_cache
            .lock()
            .lookup(&key, ttl, Instant::now());
        let stale = match lookup {
            CacheLookup::Fresh(body) => {
                self.performance.caches.record_hit(cache_name);
                return cached_json_response(body, "hit");
            }
            CacheLookup::Stale(body) => Some(body),
            CacheLookup::Miss => None,
        };

        if let Some(client_key) = client.key() {
            let allowed = self.public_telemetry_rate_limiter.lock().allow(
                client_key,
                PublicTelemetryRouteKind::PublicRead,
                Instant::now(),
            );
            if !allowed {
                return match stale {
                    Some(body) => cached_json_response(body, "stale"),
                    None => public_rate_limited_response(),
                };
            }
        }

        let refresh = match &stale {
            // Someone is already refreshing: the stale copy is good enough.
            Some(body) => match self.cache_refresh_locks.try_lock(&key) {
                Some(guard) => guard,
                None => return cached_json_response(body.clone(), "stale"),
            },
            None => match tokio::time::timeout(
                PUBLIC_READ_COALESCE_WAIT,
                self.cache_refresh_locks.lock(&key),
            )
            .await
            {
                Ok(guard) => guard,
                Err(_) => return public_read_overloaded_response(),
            },
        };
        if let CacheLookup::Fresh(body) =
            self.public_read_cache
                .lock()
                .lookup(&key, ttl, Instant::now())
        {
            self.performance.caches.record_hit(cache_name);
            return cached_json_response(body, "hit");
        }
        self.performance.caches.record_miss(cache_name);

        let permit = match tokio::time::timeout(
            PUBLIC_READ_ADMISSION_WAIT,
            Arc::clone(&self.public_read_loads).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            _ => {
                return match stale {
                    Some(body) => cached_json_response(body, "stale"),
                    None => public_read_overloaded_response(),
                };
            }
        };

        let cache = Arc::clone(&self.public_read_cache);
        let task = tokio::spawn(async move {
            let _permit = permit;
            let _refresh = refresh;
            let result = load.await;
            if let Ok(body) = &result {
                cache.lock().insert(key, body.clone(), Instant::now());
            }
            result
        });
        let result = task
            .await
            .map_err(|err| anyhow::anyhow!("public read task failed: {err}"))
            .and_then(|result| result);
        match (result, stale) {
            (Ok(body), _) => cached_json_response(body, "miss"),
            (Err(err), Some(body)) => {
                tracing::warn!(error = %err, cache = cache_name, "{error_message}; serving stale copy");
                cached_json_response(body, "stale")
            }
            (Err(err), None) => internal_error(error_message, err),
        }
    }
}

fn json_bytes<T: Serialize>(value: &T) -> anyhow::Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(value)?))
}

fn cached_json_response(body: Bytes, cache_state: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (HeaderName::from_static("x-cache"), cache_state),
        ],
        body,
    )
        .into_response()
}

fn public_rate_limited_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(
            header::RETRY_AFTER,
            PUBLIC_TELEMETRY_RATE_LIMIT_RETRY_AFTER_SECS.to_string(),
        )],
        Json(serde_json::json!({
            "error": "rate limit exceeded",
            "detail": "too many uncached requests; back off and retry"
        })),
    )
        .into_response()
}

fn public_read_overloaded_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "5")],
        Json(json!({"error": "pool API is busy; retry shortly"})),
    )
        .into_response()
}

async fn require_api_key(
    State(state): State<ApiState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let client = state.client_ip(req.headers(), req.extensions());
    let client_key = client.key().unwrap_or("unknown").to_string();
    let provided = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    match check_admin_key(
        &state.admin_auth_failures,
        state.config.api_key.trim(),
        &client_key,
        provided,
        Instant::now(),
    ) {
        AdminAuth::Allowed => next.run(req).await,
        AdminAuth::Unauthorized => error_response(StatusCode::UNAUTHORIZED, "unauthorized"),
        AdminAuth::Throttled { retry_after_secs } => {
            tracing::warn!(client = %client_key, "admin authentication throttled after repeated failures");
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, retry_after_secs.to_string())],
                Json(json!({"error": "too many failed authentication attempts; retry later"})),
            )
                .into_response()
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AdminAuth {
    Allowed,
    Unauthorized,
    Throttled { retry_after_secs: u64 },
}

fn check_admin_key(
    failures: &Mutex<WindowedCounter>,
    expected: &str,
    client_key: &str,
    provided: Option<&str>,
    now: Instant,
) -> AdminAuth {
    let mut failures = failures.lock();
    // A throttled client is refused even with the right key so the response
    // cannot be used as an oracle.
    if failures.count(client_key, now) >= ADMIN_AUTH_FAILURE_LIMIT {
        return AdminAuth::Throttled {
            retry_after_secs: failures.retry_after_secs(now),
        };
    }
    let Some(provided) = provided else {
        return AdminAuth::Unauthorized;
    };
    // Compare fixed-length digests so neither content nor length leaks.
    let matches = !expected.is_empty()
        && constant_time_eq(
            &Sha256::digest(provided.as_bytes()),
            &Sha256::digest(expected.as_bytes()),
        );
    if matches {
        failures.remove(client_key);
        AdminAuth::Allowed
    } else {
        failures.record(client_key, now);
        AdminAuth::Unauthorized
    }
}

fn non_empty(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|v| !v.is_empty())
}

fn checkpoints_path(config: &Config) -> PathBuf {
    let configured = config.checkpoints_path.trim();
    if !configured.is_empty() {
        return PathBuf::from(configured);
    }
    PathBuf::from(config.runtime.daemon_data_dir.trim()).join(CHECKPOINTS_FILENAME)
}

fn checkpoints_public_url(config: &Config) -> String {
    format!(
        "{}/{}",
        config.pool_url.trim().trim_end_matches('/'),
        CHECKPOINTS_FILENAME
    )
}

fn unavailable_checkpoints_metadata(config: &Config) -> CheckpointsMetadataResponse {
    CheckpointsMetadataResponse {
        available: false,
        url: checkpoints_public_url(config),
        entries: 0,
        latest_height: None,
        latest_hash: None,
        bytes: None,
        sha256: None,
        updated_at: None,
    }
}

/// File identity used to reuse a parsed/hashed checkpoint file until it changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CheckpointFingerprint {
    len: u64,
    modified: Option<SystemTime>,
    inode: u64,
}

impl CheckpointFingerprint {
    fn of(metadata: &fs::Metadata) -> Self {
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(metadata);
        #[cfg(not(unix))]
        let inode = 0;
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            inode,
        }
    }
}

struct CachedCheckpointFile {
    path: PathBuf,
    fingerprint: CheckpointFingerprint,
    /// Parsed snapshot, or the validation error for an invalid file.
    result: Result<Arc<CheckpointFileSnapshot>, String>,
}

/// Last loaded checkpoint file. Requests only `stat` the file; it is re-read
/// and re-hashed (up to 32 MiB) only when its size, mtime, or inode changes.
type CheckpointFileCache = Mutex<Option<CachedCheckpointFile>>;

async fn load_checkpoint_file_snapshot(
    state: &ApiState,
) -> Result<Arc<CheckpointFileSnapshot>, CheckpointFileError> {
    let config = state.config.clone();
    let cache = Arc::clone(&state.checkpoint_file_cache);
    tokio::task::spawn_blocking(move || load_checkpoint_file_snapshot_blocking(&config, &cache))
        .await
        .map_err(|err| CheckpointFileError::Internal(format!("checkpoint loader failed: {err}")))?
}

fn load_checkpoint_file_snapshot_blocking(
    config: &Config,
    cache: &CheckpointFileCache,
) -> Result<Arc<CheckpointFileSnapshot>, CheckpointFileError> {
    let path = checkpoints_path(config);
    // Held across a reload on purpose: concurrent misses (on blocking threads)
    // wait for one read instead of each reading and hashing the file.
    let mut cached = cache.lock();
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            *cached = None;
            return Err(CheckpointFileError::NotFound);
        }
        Err(err) => return Err(CheckpointFileError::Io(err)),
    };
    if !metadata.is_file() {
        return Err(CheckpointFileError::Invalid(
            "not a regular file".to_string(),
        ));
    }
    if metadata.len() > MAX_CHECKPOINTS_FILE_BYTES {
        return Err(CheckpointFileError::TooLarge(metadata.len()));
    }
    let fingerprint = CheckpointFingerprint::of(&metadata);
    if let Some(entry) = cached.as_ref() {
        if entry.path == path && entry.fingerprint == fingerprint {
            return entry.result.clone().map_err(CheckpointFileError::Invalid);
        }
    }

    let body = fs::read(&path).map_err(CheckpointFileError::Io)?;
    if u64::try_from(body.len()).unwrap_or(u64::MAX) > MAX_CHECKPOINTS_FILE_BYTES {
        return Err(CheckpointFileError::TooLarge(
            u64::try_from(body.len()).unwrap_or(u64::MAX),
        ));
    }
    let result = parse_checkpoint_snapshot(config, body, &metadata);
    // Only remember the result if the file did not change while being read.
    let unchanged = fs::metadata(&path)
        .map(|after| CheckpointFingerprint::of(&after) == fingerprint)
        .unwrap_or(false);
    let result = match result {
        Ok(snapshot) => Ok(Arc::new(snapshot)),
        Err(CheckpointFileError::Invalid(message)) => Err(message),
        Err(other) => return Err(other),
    };
    if unchanged {
        *cached = Some(CachedCheckpointFile {
            path,
            fingerprint,
            result: result.clone(),
        });
    }
    result.map_err(CheckpointFileError::Invalid)
}

fn parse_checkpoint_snapshot(
    config: &Config,
    body: Vec<u8>,
    metadata: &fs::Metadata,
) -> Result<CheckpointFileSnapshot, CheckpointFileError> {
    let (entries, latest_height, latest_hash) = parse_checkpoint_file(&body)?;
    let sha256 = hex::encode(Sha256::digest(&body));
    let updated_at = metadata.modified().ok().map(system_time_to_unix_secs);
    let bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);

    Ok(CheckpointFileSnapshot {
        body: Bytes::from(body),
        metadata: CheckpointsMetadataResponse {
            available: true,
            url: checkpoints_public_url(config),
            entries,
            latest_height: Some(latest_height),
            latest_hash: Some(latest_hash),
            bytes: Some(bytes),
            sha256: Some(sha256),
            updated_at,
        },
    })
}

fn parse_checkpoint_file(body: &[u8]) -> Result<(usize, u64, String), CheckpointFileError> {
    let text = std::str::from_utf8(body)
        .map_err(|err| CheckpointFileError::Invalid(format!("invalid UTF-8: {err}")))?;

    let mut entries = 0usize;
    let mut latest_height = 0u64;
    let mut latest_hash = String::new();

    for (index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line_no = index + 1;
        let (height_raw, hash_raw) = line.split_once(':').ok_or_else(|| {
            CheckpointFileError::Invalid(format!("line {line_no}: expected height:hash"))
        })?;
        let height = height_raw.trim().parse::<u64>().map_err(|err| {
            CheckpointFileError::Invalid(format!("line {line_no}: invalid height: {err}"))
        })?;
        if height == 0 {
            return Err(CheckpointFileError::Invalid(format!(
                "line {line_no}: height must be non-zero"
            )));
        }

        let hash = hash_raw.trim().trim_start_matches("0x");
        if hash.len() != 64 || hex::decode(hash).is_err() {
            return Err(CheckpointFileError::Invalid(format!(
                "line {line_no}: invalid 32-byte hash"
            )));
        }

        entries += 1;
        if height >= latest_height {
            latest_height = height;
            latest_hash = hash.to_ascii_uppercase();
        }
    }

    if entries == 0 {
        return Err(CheckpointFileError::Invalid(
            "no checkpoint entries found".to_string(),
        ));
    }

    Ok((entries, latest_height, latest_hash))
}

fn checkpoint_etag(sha256: &str) -> String {
    format!("\"sha256:{sha256}\"")
}

fn if_none_match_contains(value: Option<&HeaderValue>, etag: &str) -> bool {
    value
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || candidate == etag)
        })
        .unwrap_or(false)
}

fn apply_checkpoint_response_headers(
    headers: &mut HeaderMap,
    metadata: &CheckpointsMetadataResponse,
    etag: &str,
) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CHECKPOINTS_CACHE_CONTROL),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"checkpoints.dat\""),
    );
    if !etag.is_empty() {
        if let Ok(value) = HeaderValue::from_str(etag) {
            headers.insert(header::ETAG, value);
        }
    }
    if let Some(bytes) = metadata.bytes {
        if let Ok(value) = HeaderValue::from_str(&bytes.to_string()) {
            headers.insert(header::CONTENT_LENGTH, value);
        }
    }
    if let Some(height) = metadata.latest_height {
        if let Ok(value) = HeaderValue::from_str(&height.to_string()) {
            headers.insert(HeaderName::from_static("x-checkpoint-height"), value);
        }
    }
    if let Some(sha256) = metadata.sha256.as_deref() {
        if let Ok(value) = HeaderValue::from_str(sha256) {
            headers.insert(HeaderName::from_static("x-checkpoint-sha256"), value);
        }
    }
}

fn checkpoint_file_error_response(err: CheckpointFileError) -> Response {
    match err {
        CheckpointFileError::NotFound => {
            error_response(StatusCode::NOT_FOUND, "checkpoints unavailable")
        }
        CheckpointFileError::TooLarge(bytes) => {
            tracing::warn!(bytes, "checkpoint file too large to serve");
            error_response(StatusCode::SERVICE_UNAVAILABLE, "checkpoints unavailable")
        }
        CheckpointFileError::Invalid(message) => {
            tracing::warn!(error = %message, "checkpoint file invalid");
            error_response(StatusCode::SERVICE_UNAVAILABLE, "checkpoints unavailable")
        }
        CheckpointFileError::Io(err) => internal_error("failed loading checkpoints", err.into()),
        CheckpointFileError::Internal(message) => {
            tracing::warn!(error = %message, "failed loading checkpoints");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed loading checkpoints",
            )
        }
    }
}

fn page_bounds(limit: Option<usize>, offset: Option<usize>) -> (usize, usize) {
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
    let offset = offset.unwrap_or(0).min(1_000_000);
    (limit, offset)
}

fn system_time_to_unix_secs(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
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

pub(crate) fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

fn json_result<T: Serialize>(result: anyhow::Result<T>, msg: &str) -> Response {
    result.map_or_else(
        |err| internal_error(msg, err),
        |value| Json(value).into_response(),
    )
}

fn no_content_result<T>(result: anyhow::Result<T>, msg: &str) -> Response {
    result.map_or_else(
        |err| internal_error(msg, err),
        |_| StatusCode::NO_CONTENT.into_response(),
    )
}

fn internal_error(msg: &str, err: anyhow::Error) -> Response {
    if err.is::<TelemetryOverloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "5")],
            Json(json!({"error": err.to_string()})),
        )
            .into_response();
    }
    tracing::warn!(error = %err, "{msg}");
    error_response(StatusCode::INTERNAL_SERVER_ERROR, msg)
}

async fn handle_site_event(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    state.site_metrics.collect(headers, body)
}

async fn handle_site_metrics(State(state): State<ApiState>) -> Response {
    state.site_metrics.report()
}

#[cfg(test)]
mod tests {
    #[test]
    fn parallel_chain_checks_preserve_reclassification_rules() {
        let blocks = (1..=6)
            .map(|height| pool_common::db::DbBlock {
                height,
                hash: format!("hash-{height}"),
                difficulty: 1,
                finder: String::new(),
                finder_worker: String::new(),
                reward: 0,
                timestamp: std::time::UNIX_EPOCH,
                confirmed: height != 2,
                orphaned: height == 5,
                paid_out: false,
                effort_pct: None,
            })
            .collect::<Vec<_>>();
        let looked_up = std::sync::Mutex::new(Vec::new());
        let counts = super::count_chain_reclassifications(&blocks, 4, &|height| {
            looked_up.lock().unwrap().push(height);
            match height {
                1 => Ok(Some("hash-1".into())),
                2 => Ok(Some("replacement".into())),
                3 => Ok(None),
                4 => Err(anyhow::anyhow!("temporary daemon failure")),
                _ => panic!("orphaned and above-tip blocks must not need a daemon lookup"),
            }
        })
        .unwrap();
        assert_eq!(counts, (2, 1));
        let mut looked_up = looked_up.into_inner().unwrap();
        looked_up.sort();
        assert_eq!(looked_up, vec![1, 2, 3, 4]);
        assert_eq!(
            super::count_chain_reclassifications(&[], 0, &|_| Ok(None)).unwrap(),
            (0, 0)
        );
    }

    #[tokio::test]
    async fn brief_detail_burst_waits_for_capacity() {
        let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let occupied = budget.clone().acquire_owned().await.unwrap();
        let waiting = tokio::spawn(super::limited_blocking_result(budget, || Ok(42)));
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(occupied);
        assert_eq!(waiting.await.unwrap().unwrap(), 42);
    }

    #[tokio::test]
    async fn canceled_request_retains_blocking_query_budget_until_work_finishes() {
        let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task_budget = budget.clone();
        let request = tokio::spawn(async move {
            super::limited_blocking_result(task_budget, move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
        });
        started_rx.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        let error = super::limited_blocking_result(budget.clone(), || Ok(()))
            .await
            .unwrap_err();
        assert!(error.is::<super::TelemetryOverloaded>());
        let response = super::internal_error("test", error);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "5");
        release_tx.send(()).unwrap();
        let permit = tokio::time::timeout(std::time::Duration::from_secs(2), budget.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert!(super::limited_blocking_result(budget, || Ok(()))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn cache_refresh_coalesces_concurrent_misses() {
        let locks = std::sync::Arc::new(super::CacheRefreshLocks::default());
        let loads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cached = std::sync::Arc::new(super::Mutex::new(None));
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(32));
        let mut requests = Vec::new();
        for _ in 0..32 {
            let (locks, loads, cached, barrier) = (
                locks.clone(),
                loads.clone(),
                cached.clone(),
                barrier.clone(),
            );
            requests.push(tokio::spawn(async move {
                barrier.wait().await;
                let _refresh = locks.lock("detail:miner").await;
                if let Some(value) = *cached.lock() {
                    return value;
                }
                loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::task::yield_now().await;
                *cached.lock() = Some(42);
                42
            }));
        }
        for request in requests {
            assert_eq!(request.await.unwrap(), 42);
        }
        assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cache_refresh_keys_are_independent_and_unused_keys_are_pruned() {
        let locks = super::CacheRefreshLocks::default();
        let first = locks.lock("detail:first").await;
        let second = tokio::time::timeout(Duration::from_secs(1), locks.lock("detail:second"))
            .await
            .expect("different miners must not block each other");
        drop(first);
        drop(second);
        let _third = locks.lock("balance:first").await;
        assert_eq!(locks.entries.lock().len(), 1);
    }

    #[tokio::test]
    async fn cache_refresh_cancellation_releases_waiters() {
        let locks = std::sync::Arc::new(super::CacheRefreshLocks::default());
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let owner_locks = locks.clone();
        let owner = tokio::spawn(async move {
            let _refresh = owner_locks.lock("stats").await;
            ready_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready_rx.await.unwrap();
        let waiter_locks = locks.clone();
        let waiter = tokio::spawn(async move { waiter_locks.lock("stats").await });
        owner.abort();
        let _ = owner.await;
        let _guard = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("canceled loader must release the key")
            .unwrap();
    }

    use std::collections::{BTreeMap, HashMap};
    use std::env;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::{
        pending_estimate_snapshot_can_serve, pending_estimate_snapshot_needs_refresh,
        public_telemetry_route_kind_for_path, CheckpointFileError, HashrateStatsInput,
        MinerHashrateRamp, PendingEstimateSnapshotCache, PendingPreviewValidation,
        PublicTelemetryRateLimiter, PublicTelemetryRouteKind, RewardParticipantStatus,
        MINER_PENDING_ESTIMATE_HOT_WINDOW, MINER_PENDING_ESTIMATE_REFRESH_AFTER,
        PUBLIC_TELEMETRY_MINER_RATE_LIMIT, PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW,
        PUBLIC_TELEMETRY_STATS_RATE_LIMIT,
    };
    use crate::config::Config;
    use axum::body::to_bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::{header, HeaderValue, Method, StatusCode, Uri};
    use axum::response::IntoResponse;
    use axum::Json;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;
    use pool_common::db::AddressRiskEscalation;
    use pool_common::db::{
        Balance, DbBlock, MonitorHeartbeat, PendingPayout, PoolFeeRecord, ShareReplayData,
    };
    use pool_runtime::engine::ShareRecord;
    use pool_runtime::jobs::{JobManager, JobRuntimeSnapshot};
    use pool_runtime::node::{NodeClient, WalletBalance};
    use pool_runtime::payout::PayoutRuntimeSnapshot;
    use pool_runtime::service_state::PersistedRuntimeSnapshot;
    use pool_runtime::store::PoolStore;
    use pool_runtime::validation::{PersistedValidationAddressState, ValidationSnapshot};
    use tempfile::tempdir;

    use crate::ui::{app_fallback_response, is_api_request_path};

    use super::{
        api_performance_route_name, apply_wallet_liquidity_to_payout_eta, backfill_block_effort,
        backfill_round_heights, block_page_item_response, build_block_reward_breakdown,
        checkpoint_etag, checkpoints_path, checkpoints_public_url, daemon_debug_log_path,
        daemon_health_from_heartbeat, daemon_log_commands, daemon_send_idempotency_path,
        estimate_unconfirmed_pending_for_miner, estimated_block_reward,
        filter_active_workers_for_miner, handle_admin_clear_address_risk_history,
        handle_admin_share_diagnostics, handle_health, handle_miner, handle_miners,
        hashrate_from_stats_with_miner_ramp, hashrate_from_stats_with_warmup,
        history_range_duration, hydrate_provisional_block_reward, if_none_match_contains,
        load_confirmed_payout_import_txs, luck_round_response_from_db, miner_balance_response,
        miner_has_activity, miner_hashrate_range, page_bounds, parse_checkpoint_file,
        parse_version_triple, rejection_window_duration, sort_workers_for_miner,
        system_time_to_unix_secs, trim_log_line, worker_hashrate_by_name, worker_version_status,
        ApiState, ClearAddressRiskHistoryRequest, PayoutEtaResponse, SearchPageQuery,
        DAEMON_LOG_LINE_LIMIT, HASHRATE_BRAND_NEW_MIN_WINDOW, HASHRATE_WARMUP_WINDOW,
        HASHRATE_WINDOW, LIVE_RUNTIME_SNAPSHOT_META_KEY,
    };

    const TEST_POSTGRES_URL_ENV: &str = "BLOCKNET_POOL_TEST_POSTGRES_URL";

    fn test_store() -> Option<Arc<PoolStore>> {
        let url = env::var(TEST_POSTGRES_URL_ENV).ok()?;
        match PoolStore::open(&url, 2) {
            Ok(store) => Some(store),
            Err(err) => {
                eprintln!("skipping postgres test: failed to connect to test database: {err}");
                None
            }
        }
    }

    macro_rules! require_test_store {
        () => {
            match test_store() {
                Some(store) => store,
                None => {
                    eprintln!(
                        "skipping postgres test: set {} to run postgres integration checks",
                        TEST_POSTGRES_URL_ENV
                    );
                    return;
                }
            }
        };
    }

    fn pplns_test_config(window_duration: &str) -> Config {
        Config {
            runtime: pool_runtime::config::Config {
                pplns_window_duration: window_duration.to_string(),
                pool_fee_pct: 0.0,
                blocks_before_payout: 60,
                ..pool_runtime::config::Config::default()
            },
            ..Config::default()
        }
    }

    fn daemon_data_dir_config() -> Config {
        Config {
            runtime: pool_runtime::config::Config {
                daemon_data_dir: "/var/lib/blocknet/data".to_string(),
                ..pool_runtime::config::Config::default()
            },
            ..Config::default()
        }
    }

    #[test]
    fn checkpoints_path_defaults_to_daemon_data_dir_and_can_be_overridden() {
        let mut cfg = daemon_data_dir_config();
        assert_eq!(
            checkpoints_path(&cfg),
            std::path::PathBuf::from("/var/lib/blocknet/data/checkpoints.dat")
        );

        cfg.checkpoints_path = "/srv/blocknet/checkpoints.dat".to_string();
        assert_eq!(
            checkpoints_path(&cfg),
            std::path::PathBuf::from("/srv/blocknet/checkpoints.dat")
        );
    }

    #[test]
    fn checkpoints_public_url_uses_pool_url() {
        let mut cfg = Config {
            pool_url: "https://bntpool.com/".to_string(),
            ..Config::default()
        };
        assert_eq!(
            checkpoints_public_url(&cfg),
            "https://bntpool.com/checkpoints.dat"
        );

        cfg.pool_url = "https://bntpool.com".to_string();
        assert_eq!(
            checkpoints_public_url(&cfg),
            "https://bntpool.com/checkpoints.dat"
        );
    }

    #[test]
    fn parse_checkpoint_file_extracts_latest_valid_entry() {
        let first_hash = "A".repeat(64);
        let latest_hash = "b".repeat(64);
        let body = format!("# generated by daemon\n100:{first_hash}\n200:0x{latest_hash}\n");

        let (entries, latest_height, latest_hash) =
            parse_checkpoint_file(body.as_bytes()).expect("valid checkpoints");

        assert_eq!(entries, 2);
        assert_eq!(latest_height, 200);
        assert_eq!(latest_hash, "B".repeat(64));
    }

    #[test]
    fn parse_checkpoint_file_rejects_malformed_content() {
        for body in [
            String::new(),
            "# comment only\n".to_string(),
            "100\n".to_string(),
            format!("0:{}", "A".repeat(64)),
            "100:not-hex".to_string(),
        ] {
            assert!(matches!(
                parse_checkpoint_file(body.as_bytes()),
                Err(CheckpointFileError::Invalid(_))
            ));
        }
    }

    #[test]
    fn admin_key_check_throttles_repeated_failures_per_client() {
        use super::{
            check_admin_key, AdminAuth, ADMIN_AUTH_FAILURE_LIMIT, ADMIN_AUTH_FAILURE_WINDOW,
        };
        let failures =
            super::Mutex::new(super::WindowedCounter::new(ADMIN_AUTH_FAILURE_WINDOW, 64));
        let key = "k".repeat(64);
        let start = Instant::now();
        assert_eq!(
            check_admin_key(&failures, &key, "a", Some(&key), start),
            AdminAuth::Allowed
        );
        // Missing keys are not guesses and never count.
        for _ in 0..(ADMIN_AUTH_FAILURE_LIMIT * 2) {
            assert_eq!(
                check_admin_key(&failures, &key, "a", None, start),
                AdminAuth::Unauthorized
            );
        }
        // Prefixes and different lengths are rejected.
        assert_eq!(
            check_admin_key(&failures, &key, "a", Some(&key[..63]), start),
            AdminAuth::Unauthorized
        );
        // A correct key clears earlier failures.
        assert_eq!(
            check_admin_key(&failures, &key, "a", Some(&key), start),
            AdminAuth::Allowed
        );
        for _ in 0..ADMIN_AUTH_FAILURE_LIMIT {
            assert_eq!(
                check_admin_key(&failures, &key, "a", Some("wrong"), start),
                AdminAuth::Unauthorized
            );
        }
        let throttled = check_admin_key(&failures, &key, "a", Some(&key), start);
        assert!(
            matches!(throttled, AdminAuth::Throttled { retry_after_secs } if retry_after_secs > 0)
        );
        // Other clients are unaffected; the throttle lifts after the window.
        assert_eq!(
            check_admin_key(&failures, &key, "b", Some(&key), start),
            AdminAuth::Allowed
        );
        assert_eq!(
            check_admin_key(
                &failures,
                &key,
                "a",
                Some(&key),
                start + ADMIN_AUTH_FAILURE_WINDOW
            ),
            AdminAuth::Allowed
        );
        // An empty configured key never authorizes.
        assert_eq!(
            check_admin_key(&failures, "", "c", Some("anything"), start),
            AdminAuth::Unauthorized
        );
    }

    #[test]
    fn monitor_ingest_rejects_stale_future_missing_and_replayed_events() {
        use super::{check_monitor_event_freshness, CloudflareIngestEvent};
        let event = |checked_at: Option<u64>, ended_at: Option<u64>| CloudflareIngestEvent {
            service: "public_http".into(),
            status: "up".into(),
            started_at: None,
            ended_at,
            checked_at,
            summary: None,
            detail: None,
        };
        let now = 1_800_000_000;
        assert_eq!(
            check_monitor_event_freshness(&event(Some(now), None), now, now - 60),
            Ok(now)
        );
        assert_eq!(
            check_monitor_event_freshness(&event(Some(now - 300), Some(now - 300)), now, 0),
            Ok(now - 300)
        );
        for rejected in [
            event(None, None),
            event(Some(now - 301), None),
            event(Some(now + 301), None),
            event(Some(now), Some(now - 3600)),
        ] {
            assert_eq!(
                check_monitor_event_freshness(&rejected, now, 0)
                    .unwrap_err()
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        for last in [now, now + 1] {
            assert_eq!(
                check_monitor_event_freshness(&event(Some(now), None), now, last)
                    .unwrap_err()
                    .0,
                StatusCode::CONFLICT
            );
        }
    }

    #[test]
    fn checkpoint_file_is_reparsed_only_when_it_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoints.dat");
        let hash = "ab".repeat(32);
        std::fs::write(&path, format!("1:{hash}\n")).unwrap();
        let cfg = Config {
            checkpoints_path: path.display().to_string(),
            ..Config::default()
        };
        let cache = super::Mutex::new(None);
        let first = super::load_checkpoint_file_snapshot_blocking(&cfg, &cache).unwrap();
        let second = super::load_checkpoint_file_snapshot_blocking(&cfg, &cache).unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged file must be reused"
        );
        assert_eq!(first.metadata.latest_height, Some(1));

        std::fs::write(&path, format!("1:{hash}\n2:{hash}\n")).unwrap();
        let third = super::load_checkpoint_file_snapshot_blocking(&cfg, &cache).unwrap();
        assert!(!Arc::ptr_eq(&first, &third));
        assert_eq!(third.metadata.latest_height, Some(2));

        std::fs::write(&path, "garbage\n").unwrap();
        assert!(matches!(
            super::load_checkpoint_file_snapshot_blocking(&cfg, &cache),
            Err(CheckpointFileError::Invalid(_))
        ));
        assert!(matches!(
            super::load_checkpoint_file_snapshot_blocking(&cfg, &cache),
            Err(CheckpointFileError::Invalid(_))
        ));
        std::fs::remove_file(&path).unwrap();
        assert!(matches!(
            super::load_checkpoint_file_snapshot_blocking(&cfg, &cache),
            Err(CheckpointFileError::NotFound)
        ));
    }

    #[test]
    fn if_none_match_accepts_strong_etag_and_wildcard() {
        let etag = checkpoint_etag("abc123");
        let header = HeaderValue::from_str(&format!("\"old\", {etag}")).unwrap();
        assert!(if_none_match_contains(Some(&header), &etag));
        assert!(if_none_match_contains(
            Some(&HeaderValue::from_static("*")),
            &etag
        ));
        assert!(!if_none_match_contains(
            Some(&HeaderValue::from_static("\"other\"")),
            &etag
        ));
    }

    fn recovery_proxy_config(proxy_include: &std::path::Path) -> Config {
        Config {
            recovery: pool_recovery::RecoveryConfig {
                proxy_include_path: proxy_include.display().to_string(),
                ..pool_recovery::RecoveryConfig::default()
            },
            ..Config::default()
        }
    }

    fn test_api_state(store: Arc<PoolStore>) -> ApiState {
        let cfg = Config::default();
        let node =
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("build test node client"));
        let jobs = JobManager::new(Arc::clone(&node), cfg.runtime.clone());
        ApiState::new(cfg, store, jobs, node)
    }

    #[test]
    fn cache_refresh_coalesces_miner_requests_postgres() {
        let store = require_test_store!();
        // Retain state outside the runtime: synchronous Postgres clients must
        // also be dropped outside a Tokio worker.
        let state = test_api_state(store);
        let runtime = test_runtime();
        for expected_loads in 1..=2 {
            // Simulate expiration so both rounds exercise concurrent misses.
            state.clear_miner_response_caches();
            runtime.block_on(async {
                let mut requests = Vec::new();
                for _ in 0..32 {
                    let state = state.clone();
                    requests.push(tokio::spawn(async move {
                        let balance = state
                            .cached_miner_balance_payload("cache-test-miner", false)
                            .await
                            .unwrap();
                        assert_eq!(balance.address, "cache-test-miner");
                        let detail = state
                            .cached_miner_detail_payload("cache-test-miner")
                            .await
                            .unwrap();
                        assert!(!detail.found);
                    }));
                }
                for request in requests {
                    request.await.unwrap();
                }
            });
            let perf = state.performance.snapshot();
            for name in ["miner_balance_load", "miner_detail_load"] {
                assert_eq!(perf.operations[name].count, expected_loads);
            }
            for name in ["miner_balance", "miner_detail"] {
                assert_eq!(perf.caches[name].misses, expected_loads);
                assert_eq!(perf.caches[name].hits, expected_loads * 31);
            }
        }
    }

    #[test]
    fn public_read_cache_coalesces_limits_and_degrades_to_stale_postgres() {
        use axum::body::Bytes;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let state = test_api_state(require_test_store!());
        let runtime = test_runtime();
        let ttl = Duration::from_secs(60);
        let cache_state = |response: &axum::response::Response| {
            response.headers()["x-cache"].to_str().unwrap().to_string()
        };
        runtime.block_on(async {
            // A burst on one key performs a single load.
            let loads = Arc::new(AtomicUsize::new(0));
            let mut requests = Vec::new();
            for _ in 0..16 {
                let (state, loads) = (state.clone(), loads.clone());
                requests.push(tokio::spawn(async move {
                    let response = state
                        .serve_public_read(
                            &super::ClientIp::default(),
                            "test",
                            "test:burst".to_string(),
                            ttl,
                            "test load failed",
                            async move {
                                loads.fetch_add(1, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                Ok(Bytes::from_static(b"[1]"))
                            },
                        )
                        .await;
                    assert_eq!(response.status(), StatusCode::OK);
                }));
            }
            for request in requests {
                request.await.unwrap();
            }
            assert_eq!(loads.load(Ordering::SeqCst), 1);

            // Plant a stale entry to exercise degradation paths.
            let old = Instant::now()
                .checked_sub(Duration::from_secs(120))
                .expect("monotonic clock older than two minutes");
            state.public_read_cache.lock().insert(
                "test:stale".to_string(),
                Bytes::from_static(b"\"stale\""),
                old,
            );

            // Misses count against the client's budget; hits do not.
            let client = super::ClientIp(Some("203.0.113.70".to_string()));
            for idx in 0..super::PUBLIC_READ_MISS_RATE_LIMIT {
                let response = state
                    .serve_public_read(
                        &client,
                        "test",
                        format!("test:distinct:{idx}"),
                        ttl,
                        "test load failed",
                        async { Ok(Bytes::from_static(b"[]")) },
                    )
                    .await;
                assert_eq!(response.status(), StatusCode::OK);
            }
            let hit = state
                .serve_public_read(&client, "test", "test:burst".into(), ttl, "x", async {
                    panic!("fresh entries must not reload")
                })
                .await;
            assert_eq!(cache_state(&hit), "hit");
            let limited = state
                .serve_public_read(&client, "test", "test:new".into(), ttl, "x", async {
                    panic!("rate limited requests must not load")
                })
                .await;
            assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
            assert!(limited.headers().contains_key(header::RETRY_AFTER));
            let degraded = state
                .serve_public_read(&client, "test", "test:stale".into(), ttl, "x", async {
                    panic!("rate limited requests must not load")
                })
                .await;
            assert_eq!(degraded.status(), StatusCode::OK);
            assert_eq!(cache_state(&degraded), "stale");

            // Saturated read budget: stale copy if any, else 503.
            let permits = state.public_read_loads.available_permits() as u32;
            let busy = state.public_read_loads.acquire_many(permits).await.unwrap();
            let anonymous = super::ClientIp::default();
            let overloaded = state
                .serve_public_read(&anonymous, "test", "test:cold".into(), ttl, "x", async {
                    panic!("no permit available")
                })
                .await;
            assert_eq!(overloaded.status(), StatusCode::SERVICE_UNAVAILABLE);
            let stale = state
                .serve_public_read(&anonymous, "test", "test:stale".into(), ttl, "x", async {
                    panic!("no permit available")
                })
                .await;
            assert_eq!(cache_state(&stale), "stale");
            drop(busy);

            // Failed loads fall back to stale data, or report an error.
            let failed = state
                .serve_public_read(&anonymous, "test", "test:stale".into(), ttl, "x", async {
                    anyhow::bail!("database unavailable")
                })
                .await;
            assert_eq!(failed.status(), StatusCode::OK);
            assert_eq!(cache_state(&failed), "stale");
            let failed = state
                .serve_public_read(&anonymous, "test", "test:none".into(), ttl, "x", async {
                    anyhow::bail!("database unavailable")
                })
                .await;
            assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        });
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    }

    #[test]
    fn detail_overload_preserves_cache_hits_and_balance_access_postgres() {
        let state = test_api_state(require_test_store!());
        let runtime = test_runtime();
        runtime.block_on(async {
            state
                .cached_miner_detail_payload("budget-cached-miner")
                .await
                .unwrap();
            let permits = state.miner_detail_loads.available_permits() as u32;
            let _busy = state
                .miner_detail_loads
                .acquire_many(permits)
                .await
                .unwrap();
            assert!(state
                .cached_miner_detail_payload("budget-cached-miner")
                .await
                .is_ok());
            let err = state
                .cached_miner_detail_payload("budget-uncached-miner")
                .await
                .err()
                .expect("uncached detail must reject when budget is full");
            assert!(err.is::<super::TelemetryOverloaded>());
            assert!(state
                .cached_miner_balance_payload("budget-cached-miner", false)
                .await
                .is_ok());
        });
    }

    fn response_json(
        runtime: &tokio::runtime::Runtime,
        response: axum::response::Response,
    ) -> serde_json::Value {
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        serde_json::from_slice(&body).expect("decode json")
    }

    type TestShareInput<'a> = (
        &'a str,
        &'a str,
        &'a str,
        u64,
        u64,
        &'static str,
        SystemTime,
    );
    type TestShareBatchInput<'a> = (
        &'a str,
        &'a str,
        &'a str,
        u64,
        u64,
        &'static str,
        bool,
        SystemTime,
    );
    type TestBlockInput<'a> = (u64, &'a str, &'a str, &'a str, u64, SystemTime, bool, bool);

    fn test_share(share: TestShareInput<'_>) -> ShareRecord {
        let (job_id, miner, worker, difficulty, nonce, status, created_at) = share;
        ShareRecord {
            job_id: job_id.to_string(),
            miner: miner.to_string(),
            worker: worker.to_string(),
            difficulty,
            nonce,
            status,
            was_sampled: true,
            block_hash: None,
            claimed_hash: None,
            reject_reason: None,
            created_at,
        }
    }

    fn add_test_share(store: &PoolStore, share: TestShareInput<'_>) {
        store.add_share(test_share(share)).expect("add share");
    }

    fn add_test_shares(store: &PoolStore, shares: &[TestShareBatchInput<'_>]) {
        for (job_id, miner, worker, difficulty, nonce, status, was_sampled, created_at) in
            shares.iter().copied()
        {
            let mut share =
                test_share((job_id, miner, worker, difficulty, nonce, status, created_at));
            share.was_sampled = was_sampled;
            store.add_share(share).expect("add share");
        }
    }

    fn test_block(block: TestBlockInput<'_>) -> DbBlock {
        let (height, hash, finder, finder_worker, reward, timestamp, confirmed, orphaned) = block;
        DbBlock {
            height,
            hash: hash.to_string(),
            difficulty: 200,
            finder: finder.to_string(),
            finder_worker: finder_worker.to_string(),
            reward,
            timestamp,
            confirmed,
            orphaned,
            paid_out: false,
            effort_pct: None,
        }
    }

    fn add_test_block(store: &PoolStore, block: TestBlockInput<'_>) {
        store.add_block(&test_block(block)).expect("add block");
    }

    #[test]
    fn effort_backfill_uses_exact_accepted_work_at_round_boundaries() {
        let store = require_test_store!();
        let start = UNIX_EPOCH + Duration::from_secs(600);
        let end = UNIX_EPOCH + Duration::from_secs(900);
        add_test_block(
            &store,
            (
                910001,
                "backfill-first",
                "miner",
                "rig",
                100,
                start,
                true,
                false,
            ),
        );
        add_test_block(
            &store,
            (
                910002,
                "backfill-second",
                "miner",
                "rig",
                100,
                end,
                true,
                false,
            ),
        );
        for (nonce, status, at) in [
            (1, "verified", start),
            (2, "provisional", end),
            (3, "rejected", end),
            (4, "", start),
            (5, "verified", end + Duration::from_secs(1)),
        ] {
            add_test_share(&store, ("backfill", "miner", "rig", 10, nonce, status, at));
        }
        assert_eq!(store.accepted_work_between(start, end).unwrap(), 20);
        assert_eq!(backfill_block_effort(&store).unwrap(), 1);
        assert!(store
            .get_block(910002)
            .unwrap()
            .unwrap()
            .effort_pct
            .is_some());
        assert_eq!(backfill_block_effort(&store).unwrap(), 0);
    }

    #[test]
    fn effort_backfill_skips_first_block_and_only_selects_missing_rounds() {
        let base = UNIX_EPOCH + Duration::from_secs(10_000);
        let first = test_block((1, "first", "miner", "w", 1, base, true, false));
        let mut second = test_block((
            2,
            "second",
            "miner",
            "w",
            1,
            base + Duration::from_secs(60),
            true,
            false,
        ));
        second.effort_pct = Some(50.0);
        let third = test_block((
            3,
            "third",
            "miner",
            "w",
            1,
            base + Duration::from_secs(120),
            true,
            false,
        ));

        assert_eq!(
            backfill_round_heights(vec![third.clone(), first.clone(), second.clone()]),
            vec![(second.timestamp, third.height)]
        );

        let mut filled_third = third;
        filled_third.effort_pct = Some(25.0);
        assert!(backfill_round_heights(vec![filled_third, first, second]).is_empty());
    }

    #[test]
    fn api_performance_route_name_classifies_hot_routes() {
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/stats")),
            Some("stats")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/checkpoints")),
            Some("checkpoints")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/miner/test/balance")),
            Some("miner_balance")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/admin/perf")),
            Some("admin_perf")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/admin/balance-overview")),
            Some("admin_balance_overview")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static(
                "/api/admin/reconciliation/payouts/import-confirmed",
            )),
            Some("admin_reconciliation_payout_import")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static(
                "/api/admin/reconciliation/manual-offsets/apply-live-pending",
            )),
            Some("admin_reconciliation_manual_offset_apply")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/luck")),
            Some("luck")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/blocks")),
            Some("blocks")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/payouts/recent")),
            Some("payouts_recent")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/stats/history")),
            Some("stats_history")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/stats/insights")),
            Some("stats_insights")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/unknown")),
            Some("other_api")
        );
    }

    #[test]
    fn pending_estimate_snapshot_needs_refresh_only_for_hot_stale_entries() {
        let now = Instant::now();
        let hot_stale = PendingEstimateSnapshotCache {
            updated_at: Some(now - MINER_PENDING_ESTIMATE_REFRESH_AFTER - Duration::from_secs(1)),
            last_requested_at: Some(now),
            chain_height: Some(100),
            values: HashMap::new(),
            refresh_in_flight: false,
        };
        let cold_stale = PendingEstimateSnapshotCache {
            updated_at: Some(now - MINER_PENDING_ESTIMATE_REFRESH_AFTER - Duration::from_secs(1)),
            last_requested_at: Some(
                now - MINER_PENDING_ESTIMATE_HOT_WINDOW - Duration::from_secs(1),
            ),
            chain_height: Some(100),
            values: HashMap::new(),
            refresh_in_flight: false,
        };
        let hot_fresh = PendingEstimateSnapshotCache {
            updated_at: Some(now),
            last_requested_at: Some(now),
            chain_height: Some(100),
            values: HashMap::new(),
            refresh_in_flight: false,
        };

        assert!(pending_estimate_snapshot_needs_refresh(
            &hot_stale, 100, now
        ));
        assert!(!pending_estimate_snapshot_needs_refresh(
            &cold_stale,
            100,
            now
        ));
        assert!(!pending_estimate_snapshot_needs_refresh(
            &hot_fresh, 100, now
        ));
    }

    #[test]
    fn pending_estimate_snapshot_can_serve_stale_same_height_entries() {
        let now = Instant::now();
        let entry = PendingEstimateSnapshotCache {
            updated_at: Some(now - MINER_PENDING_ESTIMATE_REFRESH_AFTER - Duration::from_secs(1)),
            last_requested_at: Some(now),
            chain_height: Some(77),
            values: HashMap::new(),
            refresh_in_flight: false,
        };

        assert!(pending_estimate_snapshot_can_serve(&entry, now));
        assert!(pending_estimate_snapshot_needs_refresh(&entry, 77, now));
        assert!(pending_estimate_snapshot_needs_refresh(&entry, 78, now));
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
    fn api_request_path_detection_is_boundary_safe() {
        assert!(is_api_request_path("/api"));
        assert!(is_api_request_path("/api/status"));
        assert!(!is_api_request_path("/apiary"));
        assert!(!is_api_request_path("/status"));
    }

    #[test]
    fn public_telemetry_route_detection_matches_expected_paths() {
        assert!(matches!(
            public_telemetry_route_kind_for_path("/api/stats"),
            Some(PublicTelemetryRouteKind::Stats)
        ));
        assert!(matches!(
            public_telemetry_route_kind_for_path("/api/miner/test-address/balance"),
            Some(PublicTelemetryRouteKind::Miner)
        ));
        assert!(matches!(
            public_telemetry_route_kind_for_path("/api/miner/test-address"),
            Some(PublicTelemetryRouteKind::Miner)
        ));
        assert!(public_telemetry_route_kind_for_path("/api/miner/test-address/hashrate").is_none());
        assert!(public_telemetry_route_kind_for_path("/api/status").is_none());
    }

    #[test]
    fn public_telemetry_rate_limiter_enforces_window_limit_and_resets() {
        let mut limiter = PublicTelemetryRateLimiter::default();
        let start = Instant::now();
        for _ in 0..PUBLIC_TELEMETRY_STATS_RATE_LIMIT {
            assert!(limiter.allow("203.0.113.9", PublicTelemetryRouteKind::Stats, start));
        }
        assert!(!limiter.allow("203.0.113.9", PublicTelemetryRouteKind::Stats, start));
        assert!(limiter.allow(
            "203.0.113.9",
            PublicTelemetryRouteKind::Stats,
            start + PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW
        ));
    }

    #[test]
    fn public_telemetry_rate_limiter_shares_budget_for_miner_routes() {
        let mut limiter = PublicTelemetryRateLimiter::default();
        let start = Instant::now();
        for _ in 0..PUBLIC_TELEMETRY_MINER_RATE_LIMIT {
            assert!(limiter.allow("203.0.113.11", PublicTelemetryRouteKind::Miner, start,));
        }
        assert!(!limiter.allow("203.0.113.11", PublicTelemetryRouteKind::Miner, start,));
    }

    #[test]
    fn app_fallback_returns_json_404_for_unknown_api_paths() {
        let runtime = test_runtime();

        let response = app_fallback_response(
            Method::GET,
            &"/api/does-not-exist".parse().expect("uri"),
            "https://bntpool.com",
        );

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert!(content_type.starts_with("application/json"));

        let payload = response_json(&runtime, response);
        assert_eq!(payload["error"], "not found");
    }

    #[test]
    fn app_fallback_renders_ui_for_known_spa_routes() {
        let runtime = test_runtime();

        let response = app_fallback_response(
            Method::GET,
            &"/admin".parse().expect("uri"),
            "https://bntpool.com",
        );

        assert_eq!(response.status(), StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let html = String::from_utf8(body.to_vec()).expect("utf8 html");
        assert!(html.contains("<title>Pool Admin Dashboard</title>"));
        assert!(html.contains(r#"<meta property="og:image""#));
        assert!(html.contains(r#"<div id="root"></div>"#));
    }

    #[test]
    fn daemon_health_from_heartbeat_uses_process_block_details() {
        let heartbeat = MonitorHeartbeat {
            sampled_at: UNIX_EPOCH + Duration::from_secs(10),
            api_up: Some(true),
            stratum_up: Some(true),
            db_up: true,
            daemon_up: Some(true),
            public_http_up: None,
            daemon_syncing: Some(false),
            chain_height: Some(44),
            template_age_seconds: None,
            last_refresh_millis: None,
            summary_state: "healthy".to_string(),
        };

        let health = daemon_health_from_heartbeat(Some(&heartbeat));
        assert_eq!(health.chain_height, Some(44));

        let failed_health = daemon_health_from_heartbeat(Some(&MonitorHeartbeat {
            daemon_up: Some(false),
            ..heartbeat
        }));
        assert!(!failed_health.reachable);
    }

    #[test]
    fn health_handler_reports_persisted_pool_activity() {
        let store = require_test_store!();
        let snapshot = PersistedRuntimeSnapshot {
            sampled_at: SystemTime::now(),
            connected_miners: 0,
            connected_workers: 0,
            estimated_hashrate: 0.0,
            last_share_at: None,
            jobs: JobRuntimeSnapshot::default(),
            payouts: PayoutRuntimeSnapshot::default(),
            submit: Default::default(),
            validation: ValidationSnapshot::default(),
            runtime_tasks: BTreeMap::new(),
            latency: Default::default(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state(store);
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_health(State(state)))
            .into_response();
        let payload = response_json(&runtime, response);

        assert_eq!(payload["pool_activity"]["connected_miners"], 0);
    }

    #[test]
    fn clear_risk_history_succeeds_without_live_validator() {
        let store = require_test_store!();
        store
            .escalate_address_risk(AddressRiskEscalation::new(
                "no-live-validator",
                "manual review",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
            ))
            .expect("seed risk state");

        let state = test_api_state(Arc::clone(&store));
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_admin_clear_address_risk_history(
                State(state),
                Json(ClearAddressRiskHistoryRequest {
                    address: "no-live-validator".to_string(),
                }),
            ))
            .into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store
            .get_address_risk("no-live-validator")
            .expect("load risk state")
            .is_none());
    }

    #[test]
    fn health_handler_includes_active_verification_holds() {
        let store = require_test_store!();
        let now = SystemTime::now();

        store
            .escalate_address_risk(AddressRiskEscalation::new(
                "risk-addr",
                "invalid share proof",
                Duration::from_secs(60),
                1,
                Duration::from_secs(120),
                Duration::from_secs(120),
                Duration::from_secs(600),
            ))
            .expect("escalate address risk");
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: "validator-addr".to_string(),
                total_shares: 42,
                sampled_shares: 7,
                invalid_samples: 1,
                risk_sampled_shares: 7,
                risk_invalid_samples: 1,
                forced_started_at: Some(now),
                forced_until: Some(now + Duration::from_secs(180)),
                forced_sampled_shares: 7,
                forced_invalid_samples: 1,
                resume_forced_at: None,
                hold_cause: Some(pool_common::db::ValidationHoldCause::InvalidSamples),
                last_seen_at: now,
            })
            .expect("persist validation state");

        let state = test_api_state(store);
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_health(State(state)))
            .into_response();
        let payload = response_json(&runtime, response);
        let holds = payload["active_verification_holds"]
            .as_array()
            .expect("active verification hold array");

        assert_eq!(holds.len(), 2);

        let risk_row = holds
            .iter()
            .find(|row| row["address"] == "risk-addr")
            .expect("risk row");
        assert_eq!(risk_row["last_reason"], "invalid share proof");
        assert!(risk_row["quarantined_until"].is_object());
        assert!(risk_row["force_verify_until"].is_object());

        let validation_row = holds
            .iter()
            .find(|row| row["address"] == "validator-addr")
            .expect("validation row");
        assert!(validation_row["validation_forced_until"].is_object());
        assert!(validation_row["quarantined_until"].is_null());
        assert_eq!(
            validation_row["validation_hold_cause"].as_str(),
            Some("invalid_samples")
        );
        assert_eq!(
            validation_row["reason"].as_str(),
            Some("recent invalid sampled shares are under review")
        );
    }

    #[test]
    fn admin_share_diagnostics_reports_windows_and_runtime_pressure() {
        let store = require_test_store!();
        let now = SystemTime::now();

        add_test_share(
            &store,
            ("share-ok", "miner-a", "rig-1", 100, 1, "verified", now),
        );
        let mut invalid_share =
            test_share(("share-invalid", "miner-a", "rig-1", 100, 2, "rejected", now));
        invalid_share.reject_reason = Some("invalid share proof".to_string());
        store
            .add_share(invalid_share)
            .expect("insert invalid share");
        let mut quarantined_share = test_share((
            "share-quarantine",
            "miner-a",
            "rig-1",
            100,
            3,
            "rejected",
            now,
        ));
        quarantined_share.reject_reason = Some("address quarantined".to_string());
        store
            .add_share(quarantined_share)
            .expect("insert quarantine share");

        let snapshot = PersistedRuntimeSnapshot {
            sampled_at: now,
            connected_miners: 4,
            connected_workers: 7,
            estimated_hashrate: 42.5,
            last_share_at: Some(now),
            jobs: JobRuntimeSnapshot::default(),
            payouts: PayoutRuntimeSnapshot::default(),
            submit: Default::default(),
            validation: ValidationSnapshot {
                in_flight: 2,
                candidate_queue_depth: 5,
                regular_queue_depth: 9,
                sampled_shares: 4,
                fraud_detections: 0,
                ..ValidationSnapshot::default()
            },
            runtime_tasks: BTreeMap::new(),
            latency: Default::default(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state(store);
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_admin_share_diagnostics(State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        let windows = payload["windows"].as_array().expect("window rows");
        let five_min = windows
            .iter()
            .find(|row| row["label"] == "5m")
            .expect("5m row");
        assert_eq!(five_min["accepted"], 1);
        assert_eq!(five_min["rejected"], 2);
        assert_eq!(five_min["by_reason"][0]["reason"], "address quarantined");
        assert_eq!(five_min["by_reason"][0]["count"], 1);

        assert_eq!(payload["validation"]["candidate_queue_depth"], 5);
        assert_eq!(payload["validation"]["regular_queue_depth"], 9);
    }

    #[test]
    fn admin_share_diagnostics_prefers_persisted_runtime_counters_over_idle_api_snapshot() {
        let store = require_test_store!();
        let now = SystemTime::now();

        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: "miner-live".to_string(),
                total_shares: 5,
                sampled_shares: 1,
                invalid_samples: 0,
                risk_sampled_shares: 1,
                risk_invalid_samples: 0,
                forced_started_at: None,
                forced_until: None,
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: None,
                last_seen_at: now,
            })
            .expect("persist validation state");
        store
            .add_validation_provisional("miner-live", Some(42), now)
            .expect("persist provisional");

        let snapshot = PersistedRuntimeSnapshot {
            sampled_at: now,
            connected_miners: 3,
            connected_workers: 4,
            estimated_hashrate: 12.5,
            last_share_at: Some(now),
            jobs: JobRuntimeSnapshot::default(),
            payouts: PayoutRuntimeSnapshot::default(),
            submit: Default::default(),
            validation: ValidationSnapshot {
                hot_accepts: 74,
                audit_enqueued: 13,
                audit_verified: 13,
                audit_duration: pool_runtime::telemetry::PercentileSummary {
                    p50_millis: Some(1404),
                    p95_millis: Some(1502),
                    ..Default::default()
                },
                ..ValidationSnapshot::default()
            },
            runtime_tasks: BTreeMap::new(),
            latency: Default::default(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state(store);
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_admin_share_diagnostics(State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        assert_eq!(payload["validation"]["hot_accepts"], 74);
        assert_eq!(payload["validation"]["audit_enqueued"], 13);
        assert_eq!(payload["validation"]["audit_verified"], 13);
        assert_eq!(payload["validation"]["audit_duration"]["p50_millis"], 1404);
    }

    #[test]
    fn miners_handler_includes_db_only_miners_after_restart() {
        let store = require_test_store!();
        let now = SystemTime::now();
        add_test_share(
            &store,
            (
                "job-verified",
                "miner-db-only",
                "worker-1",
                250,
                9,
                "verified",
                now,
            ),
        );
        let mut rejected = test_share((
            "job-rejected",
            "miner-db-only",
            "worker-1",
            250,
            10,
            "rejected",
            now,
        ));
        rejected.reject_reason = Some("bad share".to_string());
        store.add_share(rejected).expect("add rejected share");

        let state = test_api_state(Arc::clone(&store));
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_miners(
                Query(SearchPageQuery::default()),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        let miner = payload["items"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["address"].as_str() == Some("miner-db-only"))
            })
            .expect("db-only miner row");
        assert_eq!(miner["shares_accepted"], 1);
        assert_eq!(miner["shares_rejected"], 1);
        assert_eq!(miner["blocks_found"], 0);
    }

    #[test]
    fn miner_handler_omits_workers_without_recent_shares() {
        let store = require_test_store!();
        let now = SystemTime::now();
        let stale_share_at = now
            .checked_sub(HASHRATE_WINDOW + Duration::from_secs(15 * 60))
            .expect("stale share timestamp");
        let recent_share_at = now
            .checked_sub(Duration::from_secs(5 * 60))
            .expect("recent share timestamp");

        for (job_id, worker, nonce, created_at) in [
            ("job-stale", "worker-stale", 1u64, stale_share_at),
            ("job-recent", "worker-recent", 2u64, recent_share_at),
        ] {
            add_test_share(
                &store,
                (
                    job_id,
                    "miner-workers",
                    worker,
                    250,
                    nonce,
                    "verified",
                    created_at,
                ),
            );
        }

        let state = test_api_state(Arc::clone(&store));
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_miner(
                Path("miner-workers".to_string()),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        let workers = payload["workers"].as_array().expect("worker rows");
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0]["worker"].as_str(), Some("worker-recent"));
        assert_eq!(payload["total_accepted"].as_u64(), Some(2));
    }

    #[test]
    fn miner_handler_reports_lifetime_mining_since() {
        let store = require_test_store!();
        let first_share_at = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let recent_share_at = first_share_at + Duration::from_secs(3 * 24 * 60 * 60);

        for (job_id, nonce, created_at) in [
            ("job-oldest", 1u64, first_share_at),
            ("job-newest", 2u64, recent_share_at),
        ] {
            add_test_share(
                &store,
                (
                    job_id,
                    "miner-since",
                    "worker-1",
                    250,
                    nonce,
                    "verified",
                    created_at,
                ),
            );
        }

        let state = test_api_state(Arc::clone(&store));
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_miner(Path("miner-since".to_string()), State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        assert_eq!(
            payload["shares"]
                .as_array()
                .map(|items| items.len())
                .unwrap_or_default(),
            2
        );
        assert_eq!(
            payload["mining_since"]["secs_since_epoch"].as_u64(),
            Some(system_time_to_unix_secs(first_share_at))
        );
    }

    #[test]
    fn miners_handler_counts_only_recent_workers_when_only_db_history_is_available() {
        let store = require_test_store!();
        let now = SystemTime::now();
        let stale_share_at = now
            .checked_sub(HASHRATE_WINDOW + Duration::from_secs(10 * 60))
            .expect("stale share timestamp");

        add_test_share(
            &store,
            (
                "job-stale-worker",
                "miner-stale-workers",
                "worker-old",
                250,
                1,
                "verified",
                stale_share_at,
            ),
        );

        let state = test_api_state(Arc::clone(&store));
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_miners(
                Query(SearchPageQuery {
                    limit: Some(25),
                    offset: Some(0),
                    search: None,
                    sort: Some("address_asc".to_string()),
                }),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        let stale = payload["items"]
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|item| item["address"].as_str() == Some("miner-stale-workers"))
            })
            .expect("stale miner row");

        assert_eq!(stale["worker_count"].as_u64(), Some(0));
    }

    #[test]
    fn miner_handler_includes_active_verification_hold_details() {
        let store = require_test_store!();
        let created_at = UNIX_EPOCH + Duration::from_secs(5_000);
        add_test_share(
            &store,
            (
                "job-hold",
                "miner-hold",
                "worker-1",
                250,
                1,
                "verified",
                created_at,
            ),
        );
        store
            .escalate_address_risk(AddressRiskEscalation::new(
                "miner-hold",
                "low difficulty share",
                Duration::from_secs(6 * 60 * 60),
                0,
                Duration::from_secs(15 * 60),
                Duration::from_secs(2 * 60 * 60),
                Duration::from_secs(2 * 60 * 60),
            ))
            .expect("seed hold");

        let state = test_api_state(store);
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_miner(Path("miner-hold".to_string()), State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        assert_eq!(
            payload["verification_hold"]["mode"].as_str(),
            Some("verified_only")
        );
        assert_eq!(
            payload["verification_hold"]["reason"].as_str(),
            Some("low difficulty share")
        );
        assert!(payload["verification_hold"]["verified_only_until"].is_object());
    }

    #[test]
    fn miner_handler_reports_validation_backlog_hold_reason() {
        let store = require_test_store!();
        let now = SystemTime::now();
        let mut share = test_share((
            "job-backlog",
            "miner-backlog",
            "worker-1",
            250,
            1,
            "provisional",
            now,
        ));
        share.was_sampled = false;
        store.add_share(share).expect("add share");
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: "miner-backlog".to_string(),
                total_shares: 20,
                sampled_shares: 0,
                invalid_samples: 0,
                risk_sampled_shares: 0,
                risk_invalid_samples: 0,
                forced_started_at: None,
                forced_until: Some(now + Duration::from_secs(180)),
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: Some(pool_common::db::ValidationHoldCause::ProvisionalBacklog),
                last_seen_at: now,
            })
            .expect("persist validation state");
        store
            .add_validation_provisional("miner-backlog", None, now)
            .expect("persist provisional");

        let state = test_api_state(store);
        let runtime = test_runtime();
        let response = runtime
            .block_on(handle_miner(
                Path("miner-backlog".to_string()),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let payload = response_json(&runtime, response);

        assert_eq!(
            payload["verification_hold"]["validation_hold_cause"].as_str(),
            Some("provisional_backlog")
        );
        assert_eq!(
            payload["verification_hold"]["reason"].as_str(),
            Some("recent provisional diff 250 has no recent verified diff yet")
        );
        assert_eq!(
            payload["verification_hold"]["validation_pending_provisional"].as_u64(),
            Some(1)
        );
    }

    #[test]
    fn miner_balance_response_tracks_queued_amount() {
        let balance = Balance {
            address: "miner-a".to_string(),
            pending: 250,
            paid: 900,
        };
        let queued = PendingPayout {
            address: "miner-a".to_string(),
            amount: 100,
            initiated_at: UNIX_EPOCH,
            send_started_at: None,
            tx_hash: None,
            fee: None,
            sent_at: None,
            batch_id: None,
            send_daemon: None,
        };
        let response = miner_balance_response(&balance, Some(&queued));
        assert_eq!(response.pending_confirmed, 250);
        assert_eq!(response.pending_queued, 100);
        assert_eq!(response.paid, 900);
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_matches_weighted_split() {
        let store = require_test_store!();
        let cfg = pplns_test_config("24h");

        let base = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let block_ts = base + Duration::from_secs(120);
        add_test_shares(
            &store,
            &[
                ("j-1", "miner-a", "wa", 100, 1, "verified", true, base),
                (
                    "j-1",
                    "miner-b",
                    "wb",
                    100,
                    2,
                    "verified",
                    true,
                    base + Duration::from_secs(10),
                ),
            ],
        );
        add_test_block(
            &store,
            (99, "blk-99", "miner-a", "wa", 1_000, block_ts, false, false),
        );

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
    fn estimate_unconfirmed_pending_for_miner_batches_overlapping_duration_windows() {
        let store = require_test_store!();
        let cfg = pplns_test_config("1h");

        let base = UNIX_EPOCH + Duration::from_secs(6_000_000);
        add_test_shares(
            &store,
            &[
                ("j-batch-a", "miner-a", "wa", 100, 1, "verified", true, base),
                (
                    "j-batch-b",
                    "miner-b",
                    "wb",
                    100,
                    2,
                    "verified",
                    true,
                    base + Duration::from_secs(50 * 60),
                ),
                (
                    "j-batch-c",
                    "miner-c",
                    "wc",
                    100,
                    3,
                    "verified",
                    true,
                    base + Duration::from_secs(90 * 60),
                ),
            ],
        );
        for (height, hash, finder, timestamp) in [
            (
                399_u64,
                "blk-batch-1".to_string(),
                "miner-a".to_string(),
                base + Duration::from_secs(55 * 60),
            ),
            (
                400_u64,
                "blk-batch-2".to_string(),
                "miner-b".to_string(),
                base + Duration::from_secs(100 * 60),
            ),
        ] {
            add_test_block(
                &store,
                (height, &hash, &finder, "w", 900, timestamp, false, false),
            );
        }

        let miner_a = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            base + Duration::from_secs(101 * 60),
            401,
        )
        .expect("estimate miner a");
        assert_eq!(miner_a.estimated_pending, 450);
        assert_eq!(miner_a.blocks.len(), 1);
        assert_eq!(miner_a.blocks[0].height, 399);

        let miner_b = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-b",
            &cfg,
            base + Duration::from_secs(101 * 60),
            401,
        )
        .expect("estimate miner b");
        assert_eq!(miner_b.estimated_pending, 900);
        assert_eq!(miner_b.blocks.len(), 2);
        assert_eq!(miner_b.blocks[0].height, 400);
        assert_eq!(miner_b.blocks[1].height, 399);
    }

    #[test]
    fn block_reward_breakdown_surfaces_recorded_credits_and_fee() {
        let store = require_test_store!();
        let mut cfg = pplns_test_config("24h");
        cfg.runtime.pool_fee_pct = 10.0;
        cfg.runtime.pool_fee_wallet_address = "pool-fee-destination".to_string();
        cfg.runtime.provisional_share_delay = "0s".to_string();

        let base = UNIX_EPOCH + Duration::from_secs(3_000_000);
        let block_ts = base + Duration::from_secs(120);
        add_test_shares(
            &store,
            &[
                ("j-a", "miner-a", "wa", 100, 1, "verified", true, base),
                (
                    "j-b",
                    "miner-b",
                    "wb",
                    100,
                    2,
                    "verified",
                    true,
                    base + Duration::from_secs(1),
                ),
            ],
        );
        add_test_block(
            &store,
            (
                299, "blk-paid", "miner-a", "wa", 1_000, block_ts, true, false,
            ),
        );
        store
            .apply_block_credits_and_mark_paid_with_fee(
                299,
                &[("miner-a".to_string(), 450), ("miner-b".to_string(), 450)],
                Some(&PoolFeeRecord {
                    amount: 100,
                    fee_address: cfg.runtime.pool_fee_wallet_address.clone(),
                    timestamp: block_ts,
                }),
            )
            .expect("apply block credits");

        let breakdown =
            build_block_reward_breakdown(&store, &cfg, 299, block_ts + Duration::from_secs(10))
                .expect("reward breakdown");
        assert_eq!(breakdown.fee_amount, 100);
        assert_eq!(breakdown.actual_credit_total, 900);
        assert_eq!(breakdown.actual_fee_amount, Some(100));
        assert_eq!(breakdown.share_window.share_count, 2);

        let miner_a = breakdown
            .participants
            .iter()
            .find(|row| row.address == "miner-a")
            .expect("miner-a row");
        assert_eq!(miner_a.preview_credit, 450);
        assert_eq!(miner_a.payout_credit, 450);
        assert_eq!(miner_a.actual_credit, Some(450));
        assert_eq!(miner_a.delta_vs_payout, Some(0));
    }

    #[test]
    fn block_reward_breakdown_marks_capped_provisional_rows() {
        let store = require_test_store!();
        let mut cfg = pplns_test_config("24h");
        cfg.runtime.provisional_share_delay = "0s".to_string();
        cfg.runtime.payout_min_verified_shares = 1;
        cfg.runtime.payout_provisional_cap_multiplier = 1.0;

        let base = UNIX_EPOCH + Duration::from_secs(4_000_000);
        let block_ts = base + Duration::from_secs(120);
        add_test_shares(
            &store,
            &[
                (
                    "j-cap-a-verified",
                    "miner-a",
                    "wa",
                    20,
                    1,
                    "verified",
                    true,
                    base,
                ),
                (
                    "j-cap-a-provisional",
                    "miner-a",
                    "wa",
                    100,
                    2,
                    "provisional",
                    false,
                    base + Duration::from_secs(1),
                ),
                (
                    "j-cap-b-verified",
                    "miner-b",
                    "wb",
                    20,
                    3,
                    "verified",
                    true,
                    base + Duration::from_secs(2),
                ),
            ],
        );
        add_test_block(
            &store,
            (
                298, "blk-cap", "miner-b", "wb", 1_200, block_ts, false, false,
            ),
        );

        let breakdown =
            build_block_reward_breakdown(&store, &cfg, 298, block_ts + Duration::from_secs(10))
                .expect("reward breakdown");
        let miner_a = breakdown
            .participants
            .iter()
            .find(|row| row.address == "miner-a")
            .expect("miner-a row");
        assert_eq!(miner_a.preview_weight, 120);
        assert_eq!(miner_a.payout_weight, 40);
        assert_eq!(
            miner_a.payout_status,
            RewardParticipantStatus::CappedProvisional
        );
    }

    #[test]
    fn block_reward_breakdown_replays_zero_weight_window_for_payout_view() {
        let store = require_test_store!();
        let mut cfg = pplns_test_config("24h");
        cfg.runtime.provisional_share_delay = "0s".to_string();
        cfg.runtime.payout_min_verified_shares = 1;

        let base = UNIX_EPOCH + Duration::from_secs(4_100_000);
        let block_ts = base + Duration::from_secs(120);
        let mut replay_share =
            test_share(("j-replay-a", "miner-a", "wa", 1, 1, "provisional", base));
        replay_share.was_sampled = false;
        store
            .add_share_with_replay(
                replay_share,
                Some(ShareReplayData {
                    job_id: "j-replay-a".to_string(),
                    header_base: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    network_target: [0xff; 32],
                    created_at: base,
                }),
            )
            .expect("add replay share");
        add_test_block(
            &store,
            (
                297,
                "blk-replay-view",
                "miner-a",
                "wa",
                1_000,
                block_ts,
                false,
                false,
            ),
        );

        let breakdown =
            build_block_reward_breakdown(&store, &cfg, 297, block_ts + Duration::from_secs(10))
                .expect("reward breakdown");
        let miner_a = breakdown
            .participants
            .iter()
            .find(|row| row.address == "miner-a")
            .expect("miner-a row");
        assert_eq!(miner_a.verified_shares, 1);
        assert_eq!(miner_a.verified_difficulty, 1);
        assert_eq!(miner_a.provisional_shares_eligible, 0);
        assert_eq!(miner_a.preview_credit, 1_000);
        assert_eq!(miner_a.payout_credit, 1_000);
        assert_eq!(miner_a.payout_status, RewardParticipantStatus::Included);
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_uses_tentative_preview_not_payout_gate() {
        let store = require_test_store!();
        let mut cfg = pplns_test_config("24h");
        cfg.runtime.provisional_share_delay = "0s".to_string();
        cfg.runtime.payout_min_verified_shares = 3;

        let base = UNIX_EPOCH + Duration::from_secs(2_000_000);
        let block_ts = base + Duration::from_secs(120);
        add_test_shares(
            &store,
            &[
                (
                    "j-preview-a-verified",
                    "miner-a",
                    "wa",
                    10,
                    1,
                    "verified",
                    true,
                    base,
                ),
                (
                    "j-preview-a-provisional",
                    "miner-a",
                    "wa",
                    90,
                    2,
                    "provisional",
                    true,
                    base + Duration::from_secs(1),
                ),
                (
                    "j-preview-b",
                    "miner-b",
                    "wb",
                    100,
                    3,
                    "verified",
                    true,
                    base + Duration::from_secs(2),
                ),
            ],
        );
        add_test_block(
            &store,
            (
                199,
                "blk-preview",
                "miner-a",
                "wa",
                1_000,
                block_ts,
                false,
                false,
            ),
        );

        let estimate = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            block_ts + Duration::from_secs(1),
            200,
        )
        .expect("estimate");
        assert_eq!(estimate.estimated_pending, 500);
        assert_eq!(estimate.blocks.len(), 1);
        assert_eq!(
            estimate.blocks[0].validation_state,
            PendingPreviewValidation::AwaitingVerifiedShares
        );
        assert_eq!(
            serde_json::to_value(&estimate.blocks[0]).expect("serialize")["validation_state"],
            "awaiting_shares"
        );
        assert!(estimate.blocks[0]
            .validation_detail
            .contains("required verified shares"));
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_counts_verified_shares_during_extra_verification() {
        let store = require_test_store!();
        let mut cfg = pplns_test_config("24h");
        cfg.runtime.provisional_share_delay = "0s".to_string();
        cfg.runtime.payout_min_verified_shares = 1;
        cfg.runtime.payout_provisional_cap_multiplier = 19.0;

        store
            .escalate_address_risk(AddressRiskEscalation::new(
                "miner-a",
                "invalid share proof",
                Duration::from_secs(60 * 60),
                0,
                Duration::from_secs(60),
                Duration::from_secs(60 * 60),
                Duration::from_secs(60 * 60),
            ))
            .expect("seed risk");

        let base = UNIX_EPOCH + Duration::from_secs(2_050_000);
        let block_ts = base + Duration::from_secs(120);
        add_test_shares(
            &store,
            &[
                (
                    "j-risk-a-verified",
                    "miner-a",
                    "wa",
                    10,
                    1,
                    "verified",
                    true,
                    base,
                ),
                (
                    "j-risk-a-provisional",
                    "miner-a",
                    "wa",
                    90,
                    2,
                    "provisional",
                    true,
                    base + Duration::from_secs(1),
                ),
                (
                    "j-risk-b",
                    "miner-b",
                    "wb",
                    10,
                    3,
                    "verified",
                    true,
                    base + Duration::from_secs(2),
                ),
            ],
        );
        add_test_block(
            &store,
            (
                249,
                "blk-risk-preview",
                "miner-a",
                "wa",
                1_000,
                block_ts,
                false,
                false,
            ),
        );

        let estimate = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            block_ts + Duration::from_secs(1),
            250,
        )
        .expect("estimate");
        assert_eq!(estimate.estimated_pending, 500);
        assert_eq!(estimate.blocks.len(), 1);
        assert_eq!(estimate.blocks[0].estimated_credit, 500);
        assert!(!estimate.blocks[0].credit_withheld);
        assert_eq!(
            estimate.blocks[0].validation_state,
            PendingPreviewValidation::ExtraVerification
        );
        assert!(estimate.blocks[0]
            .validation_detail
            .contains("only fully verified shares count"));
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_uses_winning_share_timestamp_for_window_end() {
        let store = require_test_store!();
        let cfg = pplns_test_config("24h");

        let base = UNIX_EPOCH + Duration::from_secs(2_100_000);
        let block_ts = base;
        add_test_share(
            &store,
            (
                "j-anchor-a",
                "miner-a",
                "wa",
                100,
                1,
                "verified",
                base - Duration::from_secs(10),
            ),
        );
        let mut winning_share = test_share((
            "j-anchor-b",
            "miner-b",
            "wb",
            100,
            2,
            "verified",
            base + Duration::from_secs(10),
        ));
        winning_share.block_hash = Some("blk-anchor".to_string());
        store.add_share(winning_share).expect("add winning share");
        add_test_block(
            &store,
            (
                109,
                "blk-anchor",
                "miner-b",
                "wb",
                1_000,
                block_ts,
                false,
                false,
            ),
        );

        let estimate = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            base + Duration::from_secs(20),
            110,
        )
        .expect("estimate");
        assert_eq!(estimate.estimated_pending, 500);
        assert_eq!(estimate.blocks.len(), 1);
        assert_eq!(estimate.blocks[0].estimated_credit, 500);
        assert_eq!(
            estimate.blocks[0].validation_state,
            PendingPreviewValidation::Ready
        );
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_waits_for_delayed_window() {
        let store = require_test_store!();
        let mut cfg = pplns_test_config("24h");
        cfg.runtime.provisional_share_delay = "1h".to_string();

        let base = UNIX_EPOCH + Duration::from_secs(2_200_000);
        let block_ts = base + Duration::from_secs(120);
        let mut delayed_share =
            test_share(("j-delay-a", "miner-a", "wa", 100, 1, "provisional", base));
        delayed_share.was_sampled = false;
        store.add_share(delayed_share).expect("add delayed share");
        add_test_block(
            &store,
            (
                119,
                "blk-delay",
                "miner-a",
                "wa",
                1_000,
                block_ts,
                false,
                false,
            ),
        );

        let estimate = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            block_ts + Duration::from_secs(1),
            120,
        )
        .expect("estimate");
        assert_eq!(estimate.estimated_pending, 0);
        assert_eq!(estimate.blocks.len(), 1);
        assert_eq!(estimate.blocks[0].estimated_credit, 0);
        assert_eq!(
            estimate.blocks[0].validation_state,
            PendingPreviewValidation::AwaitingDelay
        );
    }

    #[test]
    fn page_bounds_clamps_limits_and_offsets() {
        assert_eq!(page_bounds(None, None), (25, 0));
        assert_eq!(page_bounds(Some(0), Some(2)), (1, 2));
        assert_eq!(page_bounds(Some(5_000), Some(2_000_000)), (200, 1_000_000));
    }

    #[test]
    fn get_luck_rounds_page_returns_all_rounds_and_can_truncate() {
        let store = require_test_store!();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let base = UNIX_EPOCH + Duration::from_secs(2_000_000);
        let base_height = 700_000_000 + (unique % 10_000) as u64 * 10;

        for (job_id, created_at, difficulty) in [
            (
                format!("job-{unique}-1"),
                base + Duration::from_secs(10),
                40_u64,
            ),
            (
                format!("job-{unique}-1"),
                base + Duration::from_secs(20),
                60_u64,
            ),
            (
                format!("job-{unique}-2"),
                base + Duration::from_secs(70),
                100_u64,
            ),
        ] {
            add_test_share(
                &store,
                (
                    &job_id, "miner-a", "wa", difficulty, difficulty, "verified", created_at,
                ),
            );
        }

        for (height, hash, timestamp) in [
            (base_height, format!("blk-{unique}-100"), base),
            (
                base_height + 1,
                format!("blk-{unique}-101"),
                base + Duration::from_secs(60),
            ),
            (
                base_height + 2,
                format!("blk-{unique}-102"),
                base + Duration::from_secs(120),
            ),
        ] {
            let mut block = test_block((
                height, &hash, "miner-a", "wa", 1_000, timestamp, true, false,
            ));
            block.difficulty = 100;
            store.add_block(&block).expect("add block");
        }

        let (full, total) = store
            .get_luck_rounds_page(25, 0)
            .expect("full paged luck history");
        let full = full
            .into_iter()
            .filter(|row| row.block_height >= base_height && row.block_height <= base_height + 2)
            .collect::<Vec<_>>();
        assert_eq!(
            total,
            store
                .get_all_blocks()
                .expect("load blocks")
                .len()
                .saturating_sub(1) as u64
        );
        assert_eq!(full.len(), 2);
        assert_eq!(full[0].block_height, base_height + 2);
        assert_eq!(full[0].round_work, 100);
        assert_eq!(full[0].duration_seconds, 60);
        assert_eq!(
            luck_round_response_from_db(full[0].clone()).effort_pct,
            100.0
        );
        assert_eq!(full[1].block_height, base_height + 1);
        assert_eq!(full[1].round_work, 100);

        let (truncated, _total) = store
            .get_luck_rounds_page(1, 0)
            .expect("truncated paged luck history");
        let truncated = truncated
            .into_iter()
            .filter(|row| row.block_height >= base_height && row.block_height <= base_height + 2)
            .collect::<Vec<_>>();
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].block_height, base_height + 2);
    }

    #[test]
    fn get_luck_rounds_for_hashes_returns_only_requested_rows() {
        let store = require_test_store!();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let base = UNIX_EPOCH + Duration::from_secs(3_000_000);
        let base_height = 710_000_000 + (unique % 10_000) as u64 * 10;
        let requested_hash = format!("blk-{unique}-102");

        for (job_id, created_at, difficulty) in [
            (
                format!("job-{unique}-1"),
                base + Duration::from_secs(10),
                40_u64,
            ),
            (
                format!("job-{unique}-1"),
                base + Duration::from_secs(20),
                60_u64,
            ),
            (
                format!("job-{unique}-2"),
                base + Duration::from_secs(70),
                100_u64,
            ),
        ] {
            add_test_share(
                &store,
                (
                    &job_id, "miner-a", "wa", difficulty, difficulty, "verified", created_at,
                ),
            );
        }

        for (height, hash, timestamp) in [
            (base_height, format!("blk-{unique}-100"), base),
            (
                base_height + 1,
                format!("blk-{unique}-101"),
                base + Duration::from_secs(60),
            ),
            (
                base_height + 2,
                requested_hash.clone(),
                base + Duration::from_secs(120),
            ),
        ] {
            let mut block = test_block((
                height, &hash, "miner-a", "wa", 1_000, timestamp, true, false,
            ));
            block.difficulty = 100;
            store.add_block(&block).expect("add block");
        }

        let details = store
            .get_luck_rounds_for_hashes(std::slice::from_ref(&requested_hash))
            .expect("details");

        assert_eq!(details.len(), 1);
        let row = details.get(&requested_hash).expect("row");
        assert_eq!(row.block_height, base_height + 2);
        assert_eq!(row.round_work, 100);

        let row = luck_round_response_from_db(row.clone());
        let response = block_page_item_response(
            store
                .get_block(base_height + 2)
                .expect("get block")
                .expect("block exists"),
            Some(&row),
        );
        assert_eq!(response.effort_pct, Some(100.0));
        assert_eq!(response.duration_seconds, Some(60));
    }

    #[test]
    fn miner_has_any_activity_detects_historical_share_history() {
        let store = require_test_store!();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let address = format!("miner-activity-{unique}");

        assert!(!store
            .miner_has_any_activity(&address)
            .expect("address should start inactive"));

        let job_id = format!("job-activity-{unique}");
        add_test_share(
            &store,
            (
                &job_id,
                &address,
                "wa",
                32,
                32,
                "verified",
                UNIX_EPOCH + Duration::from_secs(4_000_000),
            ),
        );

        assert!(store
            .miner_has_any_activity(&address)
            .expect("address should have activity after share"));
    }

    #[test]
    fn daemon_debug_log_path_uses_daemon_data_dir() {
        let cfg = daemon_data_dir_config();
        assert_eq!(
            daemon_debug_log_path(&cfg).to_string_lossy(),
            "/var/lib/blocknet/data/debug.log"
        );
    }

    #[test]
    fn daemon_send_idempotency_path_uses_daemon_data_dir() {
        let cfg = daemon_data_dir_config();
        assert_eq!(
            daemon_send_idempotency_path(&cfg).to_string_lossy(),
            "/var/lib/blocknet/data/send-idempotency.json"
        );
    }

    #[test]
    fn daemon_log_commands_include_journal_and_tail() {
        let cfg = daemon_data_dir_config();
        let commands = daemon_log_commands(&cfg, 200);
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].program, "journalctl");
        assert!(commands[0]
            .args
            .iter()
            .any(|a| a == "blocknetd@primary.service"));
        assert!(commands[0].args.iter().any(|a| a == "-q"));
        assert!(commands[0].args.iter().any(|a| a == "-a"));
        assert!(commands[0].args.iter().any(|a| a == "-f"));
        assert_eq!(commands[1].program, "journalctl");
        assert!(commands[1]
            .args
            .iter()
            .any(|a| a == "blocknetd@standby.service"));
        assert_eq!(commands[2].program, "tail");
        assert!(commands[2].args.iter().any(|a| a == "-F"));
        assert!(commands[2]
            .args
            .iter()
            .any(|a| a == "/var/lib/blocknet/data/debug.log"));
    }

    #[test]
    fn daemon_log_commands_prefer_active_recovery_unit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let proxy_include = dir.path().join("blocknet-daemon-active-upstream.inc");
        std::fs::write(&proxy_include, "proxy_pass http://127.0.0.1:18332;\n")
            .expect("write proxy include");

        let cfg = recovery_proxy_config(&proxy_include);

        let commands = daemon_log_commands(&cfg, 50);
        assert_eq!(commands[0].program, "journalctl");
        assert!(commands[0]
            .args
            .iter()
            .any(|a| a == "blocknetd@standby.service"));
    }

    #[test]
    fn daemon_debug_log_path_uses_active_recovery_data_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let proxy_include = dir.path().join("blocknet-daemon-active-upstream.inc");
        std::fs::write(&proxy_include, "proxy_pass http://127.0.0.1:18332;\n")
            .expect("write proxy include");

        let mut cfg = daemon_data_dir_config();
        cfg.recovery.proxy_include_path = proxy_include.display().to_string();
        cfg.recovery.standby.data_dir = "/var/lib/blocknet-standby/data".to_string();

        assert_eq!(
            daemon_debug_log_path(&cfg).to_string_lossy(),
            "/var/lib/blocknet-standby/data/debug.log"
        );
    }

    #[test]
    fn daemon_send_idempotency_path_uses_active_recovery_data_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let proxy_include = dir.path().join("blocknet-daemon-active-upstream.inc");
        std::fs::write(&proxy_include, "proxy_pass http://127.0.0.1:18332;\n")
            .expect("write proxy include");

        let mut cfg = daemon_data_dir_config();
        cfg.recovery.proxy_include_path = proxy_include.display().to_string();
        cfg.recovery.standby.data_dir = "/var/lib/blocknet-standby/data".to_string();

        assert_eq!(
            daemon_send_idempotency_path(&cfg).to_string_lossy(),
            "/var/lib/blocknet-standby/data/send-idempotency.json"
        );
    }

    #[test]
    fn load_confirmed_payout_import_txs_reads_successful_live_send_entries() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("send-idempotency.json");
        let payload = serde_json::json!({
            "entries": {
                "preview": {
                    "status": 200,
                    "created_at_unix_nano": 50,
                    "body_base64": BASE64_STANDARD.encode(serde_json::to_vec(&serde_json::json!({
                        "txid": "dry-run",
                        "dry_run": true,
                        "fee": 7,
                        "recipients": [{"address":"miner-a","amount": 10}]
                    })).expect("encode dry run body"))
                },
                "wanted": {
                    "status": 200,
                    "created_at_unix_nano": 1234,
                    "body_base64": BASE64_STANDARD.encode(serde_json::to_vec(&serde_json::json!({
                        "txid": "wanted-tx",
                        "dry_run": false,
                        "fee": 9,
                        "recipients": [
                            {"address":"miner-b","amount": 30},
                            {"address":"miner-a","amount": 10},
                            {"address":"miner-b","amount": 20}
                        ]
                    })).expect("encode wanted body"))
                },
                "ignored-status": {
                    "status": 500,
                    "created_at_unix_nano": 2000,
                    "body_base64": BASE64_STANDARD.encode(serde_json::to_vec(&serde_json::json!({
                        "txid": "wanted-tx",
                        "dry_run": false,
                        "fee": 11,
                        "recipients": [{"address":"miner-c","amount": 1}]
                    })).expect("encode ignored body"))
                }
            }
        });
        std::fs::write(
            &path,
            serde_json::to_vec(&payload).expect("serialize journal"),
        )
        .expect("write journal");

        let imported =
            load_confirmed_payout_import_txs(&path, &[String::from("wanted-tx")]).expect("import");

        assert_eq!(imported.len(), 1);
        let payout_tx = &imported[0];
        assert_eq!(payout_tx.tx_hash, "wanted-tx");
        assert_eq!(payout_tx.timestamp, UNIX_EPOCH + Duration::from_nanos(1234));
        assert_eq!(payout_tx.recipients.len(), 2);
        assert_eq!(payout_tx.recipients[0].address, "miner-a");
        assert_eq!(payout_tx.recipients[0].amount, 10);
        assert_eq!(payout_tx.recipients[0].fee, 1);
        assert_eq!(payout_tx.recipients[1].address, "miner-b");
        assert_eq!(payout_tx.recipients[1].amount, 50);
        assert_eq!(payout_tx.recipients[1].fee, 8);
    }

    #[test]
    fn trim_log_line_caps_size() {
        let input = "x".repeat(DAEMON_LOG_LINE_LIMIT + 100);
        let trimmed = trim_log_line(&input);
        assert!(trimmed.len() < input.len());
        assert!(trimmed.contains("...[truncated]"));
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
    fn history_range_helpers_parse_supported_ranges() {
        for (input, expected) in [
            (None, Duration::from_secs(86400)),
            (Some("1h"), Duration::from_secs(3600)),
            (Some("7d"), Duration::from_secs(7 * 86400)),
            (Some("30d"), Duration::from_secs(30 * 86400)),
            (Some(" 30d "), Duration::from_secs(30 * 86400)),
            (Some("bad"), Duration::from_secs(86400)),
        ] {
            assert_eq!(history_range_duration(input), expected);
        }

        for (input, expected) in [
            (Some("1h"), (Duration::from_secs(3600), 120)),
            (Some("7d"), (Duration::from_secs(7 * 86400), 3600)),
            (Some("30d"), (Duration::from_secs(30 * 86400), 14400)),
            (Some("bad"), (Duration::from_secs(86400), 600)),
        ] {
            assert_eq!(miner_hashrate_range(input), expected);
        }
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
            HashrateStatsInput {
                total_diff: 600,
                count: 1,
                oldest: Some(first),
                newest: Some(first),
            },
            MinerHashrateRamp {
                smoothing_window: HASHRATE_WINDOW,
                warmup_window: HASHRATE_WARMUP_WINDOW,
                brand_new_min_window: HASHRATE_BRAND_NEW_MIN_WINDOW,
                now,
            },
        );
        assert!((hr - 10.0).abs() < 1e-9);
    }

    #[test]
    fn miner_hashrate_stale_single_share_uses_share_age() {
        let now = UNIX_EPOCH + Duration::from_secs(2_000);
        let last_share = now - Duration::from_secs(1_200);
        let hr = hashrate_from_stats_with_miner_ramp(
            HashrateStatsInput {
                total_diff: 600,
                count: 1,
                oldest: Some(last_share),
                newest: Some(last_share),
            },
            MinerHashrateRamp {
                smoothing_window: HASHRATE_WINDOW,
                warmup_window: HASHRATE_WARMUP_WINDOW,
                brand_new_min_window: HASHRATE_BRAND_NEW_MIN_WINDOW,
                now,
            },
        );
        assert!((hr - 0.5).abs() < 1e-9);
    }

    #[test]
    fn miner_hashrate_brand_new_uses_observed_window_with_two_shares() {
        let now = UNIX_EPOCH + Duration::from_secs(2_000);
        let first = now - Duration::from_secs(60);
        let hr = hashrate_from_stats_with_miner_ramp(
            HashrateStatsInput {
                total_diff: 600,
                count: 2,
                oldest: Some(first),
                newest: Some(now),
            },
            MinerHashrateRamp {
                smoothing_window: HASHRATE_WINDOW,
                warmup_window: HASHRATE_WARMUP_WINDOW,
                brand_new_min_window: HASHRATE_BRAND_NEW_MIN_WINDOW,
                now,
            },
        );
        assert!((hr - 10.0).abs() < 1e-9);
    }

    #[test]
    fn filter_active_workers_drops_stale_entries() {
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let workers = vec![
            ("stale".to_string(), 1, 0, 0, 2_000),
            ("active".to_string(), 1, 0, 0, 9_900),
        ];

        let filtered = filter_active_workers_for_miner(workers, now, HASHRATE_WINDOW);
        let names: Vec<String> = filtered
            .into_iter()
            .map(|(name, _, _, _, _)| name)
            .collect();

        assert_eq!(names, vec!["active"]);
    }

    #[test]
    fn parse_version_triple_handles_common_forms() {
        assert_eq!(parse_version_triple("0.2.15"), Some((0, 2, 15)));
        assert_eq!(parse_version_triple("v1.10.3"), Some((1, 10, 3)));
        assert_eq!(parse_version_triple(" 0.2.15 "), Some((0, 2, 15)));
        assert_eq!(parse_version_triple("0.3.0-rc.1"), Some((0, 3, 0)));
        assert_eq!(parse_version_triple("0.3.0+build5"), Some((0, 3, 0)));
        assert_eq!(parse_version_triple("0.2"), None);
        assert_eq!(parse_version_triple("0.2.15.1"), None);
        assert_eq!(parse_version_triple("garbage"), None);
        assert_eq!(parse_version_triple(""), None);
    }

    #[test]
    fn worker_version_status_derivation_covers_edge_cases() {
        // Matching the latest release is current, as is running ahead of it.
        assert_eq!(worker_version_status(Some("0.2.15"), "0.2.15"), "current");
        assert_eq!(worker_version_status(Some("0.2.16"), "0.2.15"), "current");
        assert_eq!(worker_version_status(Some("0.3.0"), "0.2.15"), "current");

        // Older reported versions are outdated.
        assert_eq!(worker_version_status(Some("0.2.14"), "0.2.15"), "outdated");
        assert_eq!(worker_version_status(Some("0.1.20"), "0.2.15"), "outdated");

        // A reported-but-unparseable version still earns a nudge.
        assert_eq!(worker_version_status(Some("garbage"), "0.2.15"), "outdated");

        // No reported version means an older seine or a non-seine client;
        // we cannot tell which, so the status is unknown.
        assert_eq!(worker_version_status(None, "0.2.15"), "unknown");

        // A misconfigured latest version must not nudge against a broken
        // baseline.
        assert_eq!(
            worker_version_status(Some("0.2.15"), "not-a-version"),
            "unknown"
        );
        assert_eq!(worker_version_status(None, ""), "unknown");
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
        let now = SystemTime::now();
        let mut block = test_block((3707, "abc", "addr", "rig", 0, now, false, false));
        block.difficulty = 1;
        hydrate_provisional_block_reward(&mut block);
        assert_eq!(block.reward, estimated_block_reward(3707));
    }

    #[test]
    fn hydrate_provisional_reward_does_not_change_confirmed_blocks() {
        let now = SystemTime::now();
        let mut block = test_block((3707, "abc", "addr", "rig", 123, now, true, false));
        block.difficulty = 1;
        hydrate_provisional_block_reward(&mut block);
        assert_eq!(block.reward, 123);
    }

    #[test]
    fn payout_eta_includes_locked_wallet_balance() {
        let mut payout_eta = PayoutEtaResponse {
            next_sweep_at: None,
            pending_total_amount: 90,
            wallet_spendable: None,
            wallet_pending: None,
        };
        let wallet_balance = WalletBalance {
            spendable: 25,
            pending: 65,
            pending_unconfirmed: 0,
            pending_unconfirmed_eta: 0,
            total: 90,
        };

        apply_wallet_liquidity_to_payout_eta(&mut payout_eta, Some(&wallet_balance));

        assert_eq!(payout_eta.wallet_spendable, Some(25));
        assert_eq!(payout_eta.wallet_pending, Some(65));
    }
}
