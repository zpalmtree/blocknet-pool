use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path as StdPath, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, SecondsFormat, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_stream::StreamExt;

use crate::config::Config;
use crate::db::{
    ActiveVerificationHold, AddressRiskState, Balance, DbBlock, DbLuckRound, DbShare,
    MonitorHeartbeat, MonitorIncident, Payout, PendingPayout, PoolFeeEvent, PublicPayoutBatch,
};
use crate::dev_fee::{SEINE_DEV_FEE_ADDRESS, SEINE_DEV_FEE_REFERENCE_TARGET_PCT};
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::node::{
    NodeClient, NodeCurrentProcessBlock, NodeLastProcessBlock, WalletBalance, WalletOutput,
};
use crate::payout::{
    is_share_payout_eligible, recover_share_window_by_replay,
    resolve_pool_fee_destination_from_address, reward_window_end, weight_shares, PayoutTrustPolicy,
};
use crate::pgdb::{
    BalanceSourceSummary, ConfirmedPayoutImportRecipient, ConfirmedPayoutImportTx,
    ManualCompletedPayoutResolutionKind, ManualPayoutOffsetApplication, MonitorUptimeSummary,
    OrphanedBlockCreditIssue, ShareWindowAddressPreview, UnreconciledCompletedPayoutRow,
};
use crate::pool_activity::{assess_pool_activity, POOL_ACTIVITY_SNAPSHOT_STALE_AFTER};
use crate::recovery::{RecoveryAgentClient, RecoveryInstanceId, RecoveryOperation, RecoveryStatus};
use crate::service_state::{
    PersistedPayoutRuntime, PersistedRuntimeSnapshot, PersistedValidationSummary,
    LIVE_RUNTIME_SNAPSHOT_META_KEY,
};
use crate::stats::{
    MinerStats, PoolSnapshot, PoolStats, RejectionAnalyticsSnapshot, RejectionReasonCount,
};
use crate::store::PoolStore;
use crate::telemetry::{
    ApiPerformanceSnapshot, NamedCacheCounterTracker, NamedTimedOperationTracker,
};
use crate::validation::{
    ValidationEngine, ValidationSnapshot, SHARE_STATUS_PROVISIONAL, SHARE_STATUS_VERIFIED,
};

const DB_TOTALS_CACHE_TTL: Duration = Duration::from_secs(15);
const DAEMON_HEALTH_CACHE_TTL: Duration = Duration::from_secs(5);
const POOL_HEALTH_CACHE_TTL: Duration = Duration::from_secs(5);
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
const INSIGHTS_CACHE_TTL: Duration = Duration::from_secs(30);
const REJECTION_ANALYTICS_CACHE_TTL: Duration = Duration::from_secs(30);
const STATS_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(15);
const CHAIN_AWARE_ORPHAN_LOOKBACK_BLOCKS: i64 = 1024;
const MINER_BALANCE_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(5);
const MINER_DETAIL_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(10);
const MINER_BALANCE_RESPONSE_CACHE_MAX_ENTRIES: usize = 2048;
const MINER_DETAIL_RESPONSE_CACHE_MAX_ENTRIES: usize = 1024;
const PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(5);
const PUBLIC_TELEMETRY_STATS_RATE_LIMIT: u32 = 12;
const PUBLIC_TELEMETRY_MINER_RATE_LIMIT: u32 = 12;
const PUBLIC_TELEMETRY_RATE_LIMIT_RETENTION: Duration = Duration::from_secs(60);
const PUBLIC_TELEMETRY_RATE_LIMIT_RETRY_AFTER_SECS: u64 = 5;
const PUBLIC_TELEMETRY_RATE_LIMIT_MAX_BUCKETS: usize = 4096;
const STATUS_SAMPLES_RETENTION: Duration = Duration::from_secs(8 * 24 * 60 * 60);
const STATUS_MAX_INCIDENTS: usize = 256;
const STATUS_HISTORY_META_KEY: &str = "status_history_v1";
const MINER_PAYOUT_HISTORY_LIMIT: i64 = 50;
const PROPORTIONAL_WINDOW: Duration = Duration::from_secs(60 * 60);
const MAX_MINER_HASHRATE_DB_LOOKUPS: usize = 4096;
const OG_IMAGE_PATH: &str = "/og-image.png";
pub const DEFAULT_MAX_SSE_SUBSCRIBERS: usize = 256;
const DEFAULT_DAEMON_LOG_TAIL: usize = 200;
const MAX_DAEMON_LOG_TAIL: usize = 2000;
const DAEMON_LOG_LINE_LIMIT: usize = 8192;
const DAEMON_LOG_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MINER_PENDING_ESTIMATE_REFRESH_AFTER: Duration = Duration::from_secs(15);
const MINER_PENDING_ESTIMATE_STALE_TTL: Duration = Duration::from_secs(60);
const MINER_PENDING_ESTIMATE_HOT_WINDOW: Duration = Duration::from_secs(60);
const ADMIN_DEV_FEE_HINT_LIMIT: i64 = 12;
const LOCAL_MONITOR_SOURCE: &str = "local";
const CLOUDFLARE_MONITOR_SOURCE: &str = "cloudflare";
const PERF_SUCCESS_LOG_SAMPLE_RATE: u64 = 128;

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
pub struct ApiState {
    pub config: Config,
    pub store: Arc<PoolStore>,
    pub stats: Arc<PoolStats>,
    pub jobs: Arc<JobManager>,
    pub node: Arc<NodeClient>,
    pub validation: Option<Arc<ValidationEngine>>,
    pub db_totals_cache: Arc<Mutex<DbTotalsCache>>,
    pub daemon_health_cache: Arc<Mutex<DaemonHealthCache>>,
    pub pool_health_cache: Arc<Mutex<PoolHealthCache>>,
    pub network_hashrate_cache: Arc<Mutex<NetworkHashrateCache>>,
    pub insights_cache: Arc<Mutex<InsightsCache>>,
    pub rejection_analytics_cache: Arc<Mutex<RejectionAnalyticsCache>>,
    pub stats_response_cache: Arc<Mutex<StatsResponseCache>>,
    pub pending_estimate_snapshot_cache: Arc<Mutex<PendingEstimateSnapshotCache>>,
    pub pending_estimate_snapshot_notify: Arc<Notify>,
    pub miner_balance_response_cache: Arc<Mutex<MinerBalanceResponseCache>>,
    pub miner_detail_response_cache: Arc<Mutex<MinerDetailResponseCache>>,
    pub public_telemetry_rate_limiter: Arc<Mutex<PublicTelemetryRateLimiter>>,
    pub performance: Arc<ApiPerformanceTracker>,
    pub recovery: Arc<RecoveryAgentClient>,
    pub live_runtime_snapshot_cache: Arc<Mutex<LiveRuntimeSnapshotCache>>,
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
    accepted_shares: u64,
    rejected_shares: u64,
    total_blocks: u64,
    confirmed_blocks: u64,
    orphaned_blocks: u64,
    pool_fees_collected: u64,
    paid_to_miners_total: u64,
}

#[derive(Debug, Default)]
pub struct DbTotalsCache {
    updated_at: Option<Instant>,
    chain_height: Option<u64>,
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
    current_process_block: Option<NodeCurrentProcessBlock>,
    last_process_block: Option<NodeLastProcessBlock>,
    error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PoolHealth {
    healthy: bool,
    database_reachable: bool,
    error: Option<String>,
}

#[derive(Debug, Default)]
pub struct DaemonHealthCache {
    updated_at: Option<Instant>,
    value: Option<DaemonHealth>,
}

#[derive(Debug, Default)]
pub struct PoolHealthCache {
    updated_at: Option<Instant>,
    value: Option<PoolHealth>,
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

#[derive(Debug, Default)]
pub struct RejectionAnalyticsCache {
    entries: HashMap<u64, TimedCacheEntry<RejectionAnalyticsSnapshot>>,
}

#[derive(Debug, Clone, Default)]
pub struct StatsResponseCache {
    updated_at: Option<Instant>,
    value: Option<StatsResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct PendingEstimateSnapshotCache {
    updated_at: Option<Instant>,
    last_requested_at: Option<Instant>,
    chain_height: Option<u64>,
    values: HashMap<String, MinerPendingEstimate>,
    refresh_in_flight: bool,
}

#[derive(Debug, Clone)]
struct TimedCacheEntry<T> {
    updated_at: Instant,
    value: T,
}

#[derive(Debug, Clone, Serialize)]
struct MinerBalancePayload {
    address: String,
    balance: MinerBalanceResponse,
    pending_estimate: MinerPendingEstimate,
    pending_payout: Option<PendingPayout>,
}

#[derive(Debug, Default)]
pub struct MinerBalanceResponseCache {
    entries: HashMap<String, TimedCacheEntry<MinerBalancePayload>>,
    last_cleanup_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct MinerDetailPayload {
    found: bool,
    body: serde_json::Value,
}

#[derive(Debug, Default)]
pub struct MinerDetailResponseCache {
    entries: HashMap<String, TimedCacheEntry<MinerDetailPayload>>,
    last_cleanup_at: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct CacheStateSummary {
    entries: usize,
    age_millis: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct ApiPerformanceResponse {
    sampled_at: Option<SystemTime>,
    routes: std::collections::BTreeMap<String, crate::telemetry::TimedOperationSummary>,
    operations: std::collections::BTreeMap<String, crate::telemetry::TimedOperationSummary>,
    tasks: std::collections::BTreeMap<String, crate::telemetry::TimedOperationSummary>,
    caches: std::collections::BTreeMap<String, crate::telemetry::CacheCounterSummary>,
    cache_states: std::collections::BTreeMap<String, CacheStateSummary>,
}

fn instant_age_millis(updated_at: Option<Instant>) -> Option<u64> {
    updated_at.map(|updated| updated.elapsed().as_millis().min(u64::MAX as u128) as u64)
}

fn latest_timed_cache_update<K, T>(entries: &HashMap<K, TimedCacheEntry<T>>) -> Option<Instant>
where
    K: std::cmp::Eq + std::hash::Hash,
{
    entries.values().map(|entry| entry.updated_at).max()
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

#[derive(Debug, Clone, Copy)]
enum PublicTelemetryRouteKind {
    Stats,
    MinerBalance,
    MinerDetail,
}

impl PublicTelemetryRouteKind {
    fn limit(self) -> u32 {
        match self {
            Self::Stats => PUBLIC_TELEMETRY_STATS_RATE_LIMIT,
            Self::MinerBalance | Self::MinerDetail => PUBLIC_TELEMETRY_MINER_RATE_LIMIT,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Stats => "stats",
            Self::MinerBalance | Self::MinerDetail => "miner",
        }
    }
}

#[derive(Debug)]
struct PublicTelemetryRateBucket {
    window_started_at: Instant,
    request_count: u32,
    last_seen_at: Instant,
}

#[derive(Debug, Default)]
pub struct PublicTelemetryRateLimiter {
    buckets: HashMap<String, PublicTelemetryRateBucket>,
    last_cleanup_at: Option<Instant>,
}

impl PublicTelemetryRateLimiter {
    fn allow(&mut self, client_ip: &str, route: PublicTelemetryRouteKind, now: Instant) -> bool {
        if self
            .last_cleanup_at
            .is_none_or(|last| now.duration_since(last) >= PUBLIC_TELEMETRY_RATE_LIMIT_RETENTION)
        {
            self.buckets.retain(|_, bucket| {
                now.duration_since(bucket.last_seen_at) < PUBLIC_TELEMETRY_RATE_LIMIT_RETENTION
            });
            self.last_cleanup_at = Some(now);
        }

        let key = format!("{}:{client_ip}", route.as_str());
        let limit = route.limit();
        let bucket = self
            .buckets
            .entry(key)
            .or_insert_with(|| PublicTelemetryRateBucket {
                window_started_at: now,
                request_count: 0,
                last_seen_at: now,
            });

        if now.duration_since(bucket.window_started_at) >= PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW {
            bucket.window_started_at = now;
            bucket.request_count = 0;
        }

        bucket.last_seen_at = now;
        if bucket.request_count >= limit {
            return false;
        }
        bucket.request_count = bucket.request_count.saturating_add(1);
        if self.buckets.len() > PUBLIC_TELEMETRY_RATE_LIMIT_MAX_BUCKETS {
            let mut oldest = self
                .buckets
                .iter()
                .map(|(key, bucket)| (key.clone(), bucket.last_seen_at))
                .collect::<Vec<_>>();
            oldest.sort_by_key(|(_, last_seen_at)| *last_seen_at);
            let remove_count = oldest
                .len()
                .saturating_sub(PUBLIC_TELEMETRY_RATE_LIMIT_MAX_BUCKETS);
            for (key, _) in oldest.into_iter().take(remove_count) {
                self.buckets.remove(&key);
            }
        }
        true
    }
}

#[derive(Debug, Default)]
pub struct ApiPerformanceTracker {
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
        }
    }

    fn record_route(&self, route: &str, duration: Duration, failed: bool, slow: bool) {
        self.routes.record(route, duration, failed, slow);
    }

    fn record_operation(&self, operation: &str, duration: Duration, failed: bool, slow: bool) {
        self.operations.record(operation, duration, failed, slow);
    }

    fn record_task(&self, task: &str, duration: Duration, failed: bool, slow: bool) {
        self.tasks.record(task, duration, failed, slow);
    }

    fn record_cache_hit(&self, cache: &str) {
        self.caches.record_hit(cache);
    }

    fn record_cache_miss(&self, cache: &str) {
        self.caches.record_miss(cache);
    }

    fn should_sample_success(&self) -> bool {
        self.log_sample_counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(PERF_SUCCESS_LOG_SAMPLE_RATE)
    }
}

#[derive(Debug, Default)]
pub struct LiveRuntimeSnapshotCache {
    updated_at: Option<Instant>,
    value: Option<PersistedRuntimeSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusSample {
    timestamp: SystemTime,
    daemon_reachable: bool,
    #[serde(default)]
    database_reachable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenIncident {
    id: u64,
    kind: String,
    severity: String,
    started_at: SystemTime,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct StatusHistory {
    samples: VecDeque<StatusSample>,
    incidents: VecDeque<StatusIncident>,
    open_daemon_down: Option<OpenIncident>,
    open_daemon_syncing: Option<OpenIncident>,
    open_pool_database_down: Option<OpenIncident>,
    next_incident_id: u64,
}

pub fn load_persisted_status_history(store: &PoolStore) -> anyhow::Result<StatusHistory> {
    let Some(raw) = store.get_meta(STATUS_HISTORY_META_KEY)? else {
        return Ok(StatusHistory::default());
    };
    let mut history: StatusHistory = serde_json::from_slice(&raw)?;
    history.prune_to_limits(SystemTime::now());
    Ok(history)
}

#[derive(Debug, Clone, Serialize)]
struct ServiceHealth {
    observed: bool,
    healthy: bool,
    last_sample_at: Option<SystemTime>,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct StatusServices {
    public_http: ServiceHealth,
    api: ServiceHealth,
    stratum: ServiceHealth,
    database: ServiceHealth,
    daemon: ServiceHealth,
}

#[derive(Debug, Clone, Serialize)]
struct TemplateHealth {
    observed: bool,
    fresh: bool,
    age_seconds: Option<u64>,
    last_refresh_millis: Option<u64>,
}

const TEMPLATE_REFRESH_WARN_AFTER_MILLIS: u64 = 45_000;

#[derive(Debug, Clone, Serialize)]
struct UptimeWindow {
    label: String,
    window_seconds: u64,
    sample_count: usize,
    external_sample_count: usize,
    api_up_pct: Option<f64>,
    stratum_up_pct: Option<f64>,
    pool_up_pct: Option<f64>,
    daemon_up_pct: Option<f64>,
    database_up_pct: Option<f64>,
    public_http_up_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct StatusPageResponse {
    checked_at: SystemTime,
    pool_uptime_seconds: u64,
    pool: PoolHealth,
    services: StatusServices,
    daemon: DaemonHealth,
    template: TemplateHealth,
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
    configured_interval_seconds: Option<u64>,
    next_sweep_at: Option<SystemTime>,
    next_sweep_in_seconds: Option<u64>,
    pending_count: usize,
    pending_total_amount: u64,
    unpaid_count: usize,
    unpaid_amount: u64,
    wallet_spendable: Option<u64>,
    wallet_pending: Option<u64>,
    queue_shortfall_amount: u64,
    liquidity_constrained: bool,
    reserve_target_amount: Option<u64>,
    safe_spend_budget: Option<u64>,
    spendable_output_count: Option<usize>,
    small_output_count: Option<usize>,
    medium_output_count: Option<usize>,
    large_output_count: Option<usize>,
    planned_batch_count: Option<usize>,
    planned_recipient_count: Option<usize>,
    rebalance_required: bool,
    rebalance_active: bool,
    inventory_health: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MinerBalanceResponse {
    pending: u64,
    pending_confirmed: u64,
    pending_queued: u64,
    pending_unqueued: u64,
    paid: u64,
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
    avg_effort_pct: Option<f64>,
    luck_history: Vec<LuckRoundResponse>,
    rejections: RejectionAnalyticsResponse,
}

pub async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    {
        let store = Arc::clone(&state.store);
        tokio::task::spawn_blocking(move || backfill_block_effort(&store))
            .await
            .map_err(|err| anyhow::anyhow!("join error: {err}"))??;
    }

    let app_state = state.clone();
    let protected = Router::new()
        .route("/api/miners", get(handle_miners))
        .route("/api/payouts", get(handle_payouts))
        .route("/api/fees", get(handle_fees))
        .route("/api/admin/perf", get(handle_admin_perf))
        .route("/api/admin/dev-fee", get(handle_admin_dev_fee))
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
            "/api/admin/recovery/standby/start-sync",
            post(handle_recovery_start_inactive_sync),
        )
        .route(
            "/api/admin/recovery/inactive/rebuild-wallet",
            post(handle_recovery_rebuild_inactive_wallet),
        )
        .route(
            "/api/admin/recovery/standby/rebuild-wallet",
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
        .route("/ui", get(handle_ui))
        .route("/robots.txt", get(handle_robots_txt))
        .route("/sitemap.xml", get(handle_sitemap_xml))
        .route("/favicon.svg", get(handle_favicon_svg))
        .route(OG_IMAGE_PATH, get(handle_og_image_png))
        .route("/og-image.svg", get(handle_og_image_svg))
        .route("/ui-assets/app.js", get(handle_ui_asset_app_js))
        .route("/ui-assets/app.css", get(handle_ui_asset_app_css))
        .route(
            "/ui-assets/pool-entered.png",
            get(handle_ui_asset_pool_entered),
        )
        .route("/ui-assets/mining-tui.png", get(handle_ui_asset_mining_tui))
        .route("/api/info", get(handle_info))
        .route("/api/stats/history", get(handle_stats_history))
        .route("/api/stats/insights", get(handle_stats_insights))
        .route("/api/luck", get(handle_luck_history))
        .route("/api/status", get(handle_status))
        .route("/api/monitor/public", get(handle_monitor_public))
        .route(
            "/api/monitor/ingest/cloudflare",
            post(handle_monitor_ingest_cloudflare),
        )
        .route("/api/events", get(handle_events))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts/recent", get(handle_public_payouts))
        .route("/api/miner/:address/hashrate", get(handle_miner_hashrate))
        .merge(public_telemetry)
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            observe_api_request_performance,
        ))
        .fallback(handle_app_fallback)
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

const UI_INDEX_HTML: &str = include_str!(concat!(
    env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"),
    "/index.html"
));
const UI_ASSET_APP_JS: &str =
    include_str!(concat!(env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"), "/app.js"));
const UI_ASSET_APP_CSS: &str =
    include_str!(concat!(env!("BLOCKNET_POOL_FRONTEND_DIST_DIR"), "/app.css"));
const UI_ASSET_OG_IMAGE_SVG: &str = include_str!("ui/assets/og-image.svg");
const UI_ASSET_OG_IMAGE_PNG: &[u8] = include_bytes!("ui/assets/og-image.png");
const UI_ASSET_POOL_ENTERED_PNG: &[u8] = include_bytes!("ui/assets/pool-entered.png");
const UI_ASSET_MINING_TUI_PNG: &[u8] = include_bytes!("ui/assets/mining-tui.png");
const UI_FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" role="img" aria-label="Blocknet Pool"><rect x="4" y="4" width="24" height="24" rx="4" fill="#16a34a"/><rect x="9" y="9" width="14" height="14" rx="2" fill="#fff" opacity=".9"/><rect x="12" y="12" width="8" height="8" rx="1" fill="#16a34a"/></svg>"##;
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
            Self::Dashboard
                | Self::Start
                | Self::Luck
                | Self::Blocks
                | Self::Payouts
                | Self::Status
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

#[derive(Debug, Clone, Default)]
struct UiSeoContext {
    connected_miners: usize,
    connected_workers: usize,
    pool_hashrate_hps: f64,
    network_hashrate_hps: Option<f64>,
    current_block_height: Option<u64>,
    totals: Option<DbTotals>,
    recent_blocks: Vec<DbBlock>,
    recent_payout_batches: Vec<PublicPayoutBatch>,
    daemon: Option<DaemonHealth>,
    latest_incident: Option<StatusIncident>,
    latest_status_change_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct SeoFaqEntry {
    question: String,
    answer: String,
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

fn format_bnt(value: f64) -> String {
    format!("{} BNT", format_grouped_decimal_trimmed(value))
}

fn format_atomic_bnt(value: u64) -> String {
    format!(
        "{} BNT",
        format_grouped_decimal_fixed(value as f64 / 1e8, 2)
    )
}

fn format_atomic_fee(value: u64) -> String {
    if value == 0 {
        return "0 BNT".to_string();
    }
    let coins = value as f64 / 1e8;
    format!("{} BNT", format_grouped_decimal_fixed(coins, 4))
}

fn format_grouped_decimal_trimmed(value: f64) -> String {
    let raw = format_decimal(value);
    format_grouped_decimal_str(&raw)
}

fn format_grouped_decimal_fixed(value: f64, decimals: usize) -> String {
    let raw = format!("{value:.prec$}", prec = decimals);
    format_grouped_decimal_str(&raw)
}

fn format_grouped_decimal_str(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return "0".to_string();
    }

    let (sign, unsigned) = if let Some(rest) = raw.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        ("+", rest)
    } else {
        ("", raw)
    };

    let (whole, frac) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut grouped_rev = String::with_capacity(whole.len() + whole.len() / 3);
    for (idx, ch) in whole.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            grouped_rev.push(',');
        }
        grouped_rev.push(ch);
    }
    let grouped: String = grouped_rev.chars().rev().collect();
    if frac.is_empty() {
        format!("{sign}{grouped}")
    } else {
        format!("{sign}{grouped}.{frac}")
    }
}

fn human_hashrate(value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return "0 H/s".to_string();
    }
    let units = ["H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s"];
    let mut scaled = value;
    let mut idx = 0usize;
    while scaled >= 1000.0 && idx < units.len() - 1 {
        scaled /= 1000.0;
        idx += 1;
    }
    format!("{scaled:.2} {}", units[idx])
}

fn format_system_time_rfc3339(value: SystemTime) -> String {
    let dt = DateTime::<Utc>::from(value);
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn format_system_time_short(value: SystemTime) -> String {
    let dt = DateTime::<Utc>::from(value);
    dt.format("%Y-%m-%d %H:%M UTC").to_string()
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

fn pool_fee_summary(pool_fee_pct: f64, pool_fee_flat: f64) -> String {
    let mut parts = Vec::new();
    if pool_fee_pct > 0.0 {
        parts.push(format!("{}% fee", format_decimal(pool_fee_pct)));
    }
    if pool_fee_flat > 0.0 {
        parts.push(format!("{} flat fee", format_bnt(pool_fee_flat)));
    }
    if parts.is_empty() {
        "0% fee".to_string()
    } else {
        parts.join(" + ")
    }
}

fn pool_fee_summary_for_state(state: &ApiState) -> String {
    pool_fee_summary(state.pool_fee_pct, state.pool_fee_flat)
}

fn pool_payout_summary(state: &ApiState) -> String {
    format!(
        "{} minimum payout after {} confirmations",
        format_bnt(state.min_payout_amount),
        state.blocks_before_payout.max(0)
    )
}

fn start_page_faq_entries(state: &ApiState) -> Vec<SeoFaqEntry> {
    let stratum = stratum_endpoint(state);
    let payout_scheme = state.payout_scheme.trim().to_uppercase();
    let payout_rules = pool_payout_summary(state);
    vec![
        SeoFaqEntry {
            question: "What miner should I use to mine Blocknet?".to_string(),
            answer: "Use Seine, the Blocknet miner shown in the pool onboarding guide, then point it at the pool stratum endpoint.".to_string(),
        },
        SeoFaqEntry {
            question: "What pool URL should I enter?".to_string(),
            answer: format!("Use {stratum} as the pool URL in Seine or any compatible Blocknet mining configuration."),
        },
        SeoFaqEntry {
            question: "How do payouts work on this Blocknet pool?".to_string(),
            answer: format!(
                "{} payouts are used here, with {}.",
                payout_scheme,
                payout_rules
            ),
        },
        SeoFaqEntry {
            question: "How can I verify pool activity before mining?".to_string(),
            answer: "Review the public dashboard, recent blocks, payout batches, and status page before directing any hashpower to the pool.".to_string(),
        },
    ]
}

fn block_status_label(block: &DbBlock) -> &'static str {
    if block.orphaned {
        "Orphaned"
    } else if block.confirmed {
        "Confirmed"
    } else {
        "Pending"
    }
}

fn explorer_block_url(hash: &str) -> String {
    format!(
        "https://explorer.blocknetcrypto.com/block/{}",
        urlencoding::encode(hash)
    )
}

fn explorer_tx_url(hash: &str) -> String {
    format!(
        "https://explorer.blocknetcrypto.com/tx/{}",
        urlencoding::encode(hash)
    )
}

fn latest_time(candidates: impl IntoIterator<Item = Option<SystemTime>>) -> Option<SystemTime> {
    candidates.into_iter().flatten().max()
}

fn seo_title(route: UiRoute, state: &ApiState) -> String {
    match route {
        UiRoute::Dashboard => format!(
            "Blocknet Mining Pool | Live Hashrate, Blocks & Payouts | {}",
            state.pool_name
        ),
        UiRoute::Start => format!(
            "Mine Blocknet With Seine | Setup, Stratum & Payouts | {}",
            state.pool_name
        ),
        UiRoute::Luck => format!(
            "Blocknet Pool Luck | Round Effort History | {}",
            state.pool_name
        ),
        UiRoute::Blocks => format!(
            "Recent Blocknet Blocks | Confirmed, Pending & Orphaned | {}",
            state.pool_name
        ),
        UiRoute::Payouts => format!(
            "Blocknet Pool Payouts | Recent Transactions | {}",
            state.pool_name
        ),
        UiRoute::Stats => format!("Blocknet Miner Stats Lookup | {}", state.pool_name),
        UiRoute::Admin => format!("Pool Admin Dashboard | {}", state.pool_name),
        UiRoute::Status => format!(
            "Blocknet Pool Status | Uptime & Daemon Health | {}",
            state.pool_name
        ),
    }
}

fn seo_description(route: UiRoute, state: &ApiState) -> String {
    let stratum = stratum_endpoint(state);
    let fee = pool_fee_summary_for_state(state);
    let payout_scheme = state.payout_scheme.trim().to_uppercase();
    let payout_rules = pool_payout_summary(state);
    match route {
        UiRoute::Dashboard => format!(
            "Live Blocknet mining pool dashboard for {} with stratum URL {}, public blocks, payout batches, round luck, and real-time pool status.",
            state.pool_name, stratum
        ),
        UiRoute::Start => format!(
            "Start mining Blocknet with {}. Copy {}, mine with Seine, and review {} payouts, {}, and {}.",
            state.pool_name, stratum, payout_scheme, fee, payout_rules
        ),
        UiRoute::Luck => format!(
            "Track {} pool luck with round effort, expected block timing, round duration, and recent confirmed or orphaned Blocknet rounds.",
            state.pool_name
        ),
        UiRoute::Blocks => format!(
            "Browse recent Blocknet blocks found by {}, including confirmed, pending, and orphaned rounds with explorer links, rewards, and timing.",
            state.pool_name
        ),
        UiRoute::Payouts => format!(
            "Review recent Blocknet pool payouts from {} with recipient counts, payout totals, network fees, and explorer transaction links.",
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
            "Monitor {} uptime, daemon reachability, sync state, incident history, and current chain height from the public Blocknet pool status page.",
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

fn render_stat_grid(items: &[(&str, String, Option<String>)]) -> String {
    let cards = items
        .iter()
        .map(|(label, value, meta)| {
            let meta_html = meta
                .as_ref()
                .map(|entry| format!(r#"<div class="stat-meta">{}</div>"#, escape_html(entry)))
                .unwrap_or_default();
            format!(
                r#"<div class="stat-card"><div class="label">{}</div><div class="value">{}</div>{}</div>"#,
                escape_html(label),
                escape_html(value),
                meta_html
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(r#"<div class="stats-grid stats-grid-dense">{cards}</div>"#)
}

fn render_start_faq_section(entries: &[SeoFaqEntry]) -> String {
    let cards = entries
        .iter()
        .map(|entry| {
            format!(
                r#"<div class="card seo-copy-card"><h3>{}</h3><p>{}</p></div>"#,
                escape_html(&entry.question),
                escape_html(&entry.answer)
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(
        r#"<div class="section"><div class="section-header"><div><h2>Mining pool FAQ</h2><p class="section-lead">Important setup and payout questions miners usually ask before connecting to the pool.</p></div></div><div class="seo-copy-grid">{cards}</div></div>"#
    )
}

fn render_recent_blocks_section(blocks: &[DbBlock], show_view_all: bool) -> String {
    if blocks.is_empty() {
        return r#"<div class="card section"><h2>Recent block activity</h2><p class="section-lead">No pool blocks have been recorded yet.</p></div>"#
            .to_string();
    }

    let rows = blocks
        .iter()
        .take(6)
        .map(|block| {
            format!(
                r#"<tr><td><a href="{}" target="_blank" rel="noopener">{}</a></td><td>{}</td><td>{}</td><td>{}</td></tr>"#,
                escape_html(&explorer_block_url(&block.hash)),
                block.height,
                escape_html(&format_atomic_bnt(block.reward)),
                escape_html(block_status_label(block)),
                escape_html(&format_system_time_short(block.timestamp))
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let view_all = if show_view_all {
        r#"<a class="view-all" href="/blocks">Open blocks page</a>"#
    } else {
        ""
    };
    format!(
        r#"<div class="section"><div class="section-header"><div><h2>Recent block activity</h2><p class="section-lead">Fresh confirmed, pending, and orphaned rounds from the Blocknet pool with direct explorer links.</p></div>{view_all}</div><div class="card table-scroll"><table><thead><tr><th>Height</th><th>Reward</th><th>Status</th><th>Found</th></tr></thead><tbody>{rows}</tbody></table></div></div>"#
    )
}

fn render_recent_payouts_section(batches: &[PublicPayoutBatch], show_view_all: bool) -> String {
    if batches.is_empty() {
        return r#"<div class="card section"><h2>Recent payout batches</h2><p class="section-lead">No payout batches have been recorded yet.</p></div>"#
            .to_string();
    }

    let rows = batches
        .iter()
        .take(6)
        .map(|batch| {
            let primary_tx = batch.tx_hashes.first().map(|hash| {
                format!(
                    r#"<a href="{}" target="_blank" rel="noopener">{}</a>"#,
                    escape_html(&explorer_tx_url(hash)),
                    escape_html(hash)
                )
            });
            let status = if batch.confirmed {
                r#"<span class="badge badge-confirmed">confirmed</span>"#
            } else {
                r#"<span class="badge badge-pending">unconfirmed</span>"#
            };
            format!(
                r#"<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"#,
                escape_html(&format_atomic_bnt(batch.total_amount)),
                batch.recipient_count,
                escape_html(&format_atomic_fee(batch.total_fee)),
                primary_tx.unwrap_or_else(|| "-".to_string()),
                status,
                escape_html(&format_system_time_short(batch.timestamp))
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let view_all = if show_view_all {
        r#"<a class="view-all" href="/payouts">Open payouts page</a>"#
    } else {
        ""
    };
    format!(
        r#"<div class="section"><div class="section-header"><div><h2>Recent payout batches</h2><p class="section-lead">Recent on-chain payout batches with recipient counts, fees, explorer transaction links, and confirmation status.</p></div>{view_all}</div><div class="card table-scroll"><table><thead><tr><th>Total</th><th>Miners Paid</th><th>Network Fee</th><th>Transaction</th><th>Status</th><th>Paid</th></tr></thead><tbody>{rows}</tbody></table></div></div>"#
    )
}

async fn load_ui_seo_context(route: UiRoute, state: &ApiState) -> UiSeoContext {
    let stats_snapshot = state.effective_pool_snapshot().await;
    let current_job = state.jobs.current_job();
    let daemon_chain_height = state.node.chain_height();
    let need_blocks = matches!(route, UiRoute::Dashboard | UiRoute::Blocks | UiRoute::Luck);
    let need_payouts = matches!(route, UiRoute::Dashboard | UiRoute::Payouts);
    let need_totals = matches!(route, UiRoute::Dashboard | UiRoute::Blocks | UiRoute::Luck);
    let need_status = matches!(route, UiRoute::Status);

    let totals = if need_totals {
        match state.db_totals().await {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::warn!(error = %err, route = route.slug(), "failed loading seo totals");
                None
            }
        }
    } else {
        None
    };

    let network_hashrate_hps = if matches!(route, UiRoute::Dashboard) {
        state.network_hashrate_for_job(current_job.as_ref()).await
    } else {
        None
    };

    let mut context = UiSeoContext {
        connected_miners: stats_snapshot.connected_miners,
        connected_workers: stats_snapshot.connected_workers,
        pool_hashrate_hps: stats_snapshot.estimated_hashrate,
        network_hashrate_hps,
        current_block_height: current_job.as_ref().map(|job| job.height),
        totals,
        ..Default::default()
    };

    if need_blocks || need_payouts {
        let store = Arc::clone(&state.store);
        let node = Arc::clone(&state.node);
        match tokio::task::spawn_blocking(
            move || -> anyhow::Result<(Vec<DbBlock>, Vec<PublicPayoutBatch>)> {
                let mut blocks = if need_blocks {
                    store.get_recent_blocks_up_to(6, daemon_chain_height)?
                } else {
                    Vec::new()
                };
                if need_blocks {
                    flag_chain_mismatched_blocks(node.as_ref(), daemon_chain_height, &mut blocks);
                }
                let payout_batches = if need_payouts {
                    store.get_public_payout_batches_page("time_desc", 6, 0)?.0
                } else {
                    Vec::new()
                };
                Ok((blocks, payout_batches))
            },
        )
        .await
        {
            Ok(Ok((blocks, payout_batches))) => {
                context.recent_blocks = blocks;
                context.recent_payout_batches = payout_batches;
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, route = route.slug(), "failed loading seo previews");
            }
            Err(err) => {
                tracing::warn!(error = %err, route = route.slug(), "seo preview join error");
            }
        }
    }

    if need_status {
        let now = SystemTime::now();
        let daemon = state.daemon_health().await;
        let history = state.status_history.lock();
        let latest_closed_incident = history
            .incidents
            .front()
            .map(|incident| incident.ended_at.unwrap_or(incident.started_at));
        let latest_open_incident = latest_time([
            history
                .open_daemon_down
                .as_ref()
                .map(|incident| incident.started_at),
            history
                .open_daemon_syncing
                .as_ref()
                .map(|incident| incident.started_at),
        ]);
        context.daemon = Some(daemon);
        context.latest_incident = history.incidents_for_api(now).into_iter().next();
        context.latest_status_change_at = latest_time([
            history.samples.back().map(|sample| sample.timestamp),
            latest_closed_incident,
            latest_open_incident,
        ]);
    }

    context
}

fn ui_seo_last_modified(
    route: UiRoute,
    state: &ApiState,
    context: &UiSeoContext,
) -> Option<SystemTime> {
    let latest_block = context.recent_blocks.first().map(|block| block.timestamp);
    let latest_payout = context
        .recent_payout_batches
        .first()
        .map(|batch| batch.timestamp);
    match route {
        UiRoute::Dashboard => {
            latest_time([Some(state.started_at_system), latest_block, latest_payout])
        }
        UiRoute::Start | UiRoute::Stats | UiRoute::Admin => Some(state.started_at_system),
        UiRoute::Luck | UiRoute::Blocks => {
            latest_time([latest_block, Some(state.started_at_system)])
        }
        UiRoute::Payouts => latest_time([latest_payout, Some(state.started_at_system)]),
        UiRoute::Status => latest_time([
            context.latest_status_change_at,
            Some(state.started_at_system),
        ]),
    }
}

fn fallback_content(route: UiRoute, state: &ApiState, context: &UiSeoContext) -> String {
    let stratum = escape_html(&stratum_endpoint(state));
    let fee_summary = escape_html(&pool_fee_summary_for_state(state));
    let payout_summary = escape_html(&pool_payout_summary(state));
    let faq_section = render_start_faq_section(&start_page_faq_entries(state));
    match route {
        UiRoute::Dashboard => {
            let totals = context.totals.unwrap_or_default();
            let latest_solved_block = context.recent_blocks.first();
            let stats_grid = render_stat_grid(&[
                (
                    "Connected Miners",
                    context.connected_miners.to_string(),
                    None,
                ),
                (
                    "Active Workers",
                    context.connected_workers.to_string(),
                    None,
                ),
                (
                    "Pool Hashrate",
                    human_hashrate(context.pool_hashrate_hps),
                    None,
                ),
                (
                    "Network Hashrate",
                    context
                        .network_hashrate_hps
                        .map(human_hashrate)
                        .unwrap_or_else(|| "-".to_string()),
                    None,
                ),
                (
                    "Current Block",
                    context
                        .current_block_height
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    None,
                ),
                (
                    "Last Solved Block",
                    latest_solved_block
                        .map(|block| block.height.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    None,
                ),
                (
                    "Blocks Found",
                    totals.total_blocks.to_string(),
                    Some(format!(
                        "{} confirmed, {} orphaned",
                        totals.confirmed_blocks, totals.orphaned_blocks
                    )),
                ),
            ]);
            fallback_page_markup(
                route,
                "Track pool hashrate, round luck, recent blocks, payout timing, and current chain conditions from the public dashboard.",
                &format!(
                    r#"<div class="stratum-bar"><span style="font-size:14px;font-weight:600;color:var(--muted)">Stratum</span><span class="endpoint">{stratum}</span></div>{stats_grid}<div class="card section"><div class="section-header"><div><h2>Explore the pool</h2><p class="section-lead">Verify the pool before mining by reviewing the public start guide, block history, payouts, and live status.</p></div></div><div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Start Mining</h3><p><a href="/start">Follow the Seine setup guide</a> to copy the stratum URL and connect a Blocknet wallet address in minutes.</p></div><div class="card seo-copy-card"><h3>Check Recent Blocks</h3><p><a href="/blocks">Browse confirmed, pending, and orphaned rounds</a> to understand how the pool is performing block to block.</p></div><div class="card seo-copy-card"><h3>Verify Payouts</h3><p><a href="/payouts">Review recent payout batches and explorer links</a> before you point any hashpower at the pool.</p></div></div></div>{blocks}{payouts}"#,
                    blocks = render_recent_blocks_section(&context.recent_blocks, true),
                    payouts = render_recent_payouts_section(&context.recent_payout_batches, true),
                ),
            )
        }
        UiRoute::Start => fallback_page_markup(
            route,
            "Download Seine, connect to the pool stratum endpoint, and monitor your Blocknet hashrate and payouts from the public dashboard.",
            &format!(
                r#"<div class="card section"><h2>Pool information</h2><table class="info-table" style="max-width:540px"><tbody><tr><td>Stratum</td><td>{stratum}</td></tr><tr><td>Fee</td><td>{fee_summary}</td></tr><tr><td>Payouts</td><td>{scheme}</td></tr><tr><td>Min Payout</td><td>{min_payout}</td></tr><tr><td>Confirmations</td><td>{confirmations}</td></tr></tbody></table></div><div class="card section"><h2>Quick start</h2><p class="section-lead">Connect your Blocknet wallet address to the pool, start Seine, and use the dashboard to verify hashrate and payouts.</p><pre class="config-block">./seine --pool-url {stratum} --address YOUR_BLOCKNET_ADDRESS</pre></div>{faq_section}<div class="section"><div class="section-header"><div><h2>Why miners review this page first</h2><p class="section-lead">The pool guide surfaces the facts that matter before you start mining: endpoint, fees, payout rules, live status, and public payment history.</p></div></div><div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Transparent fees</h3><p>{fee_summary} with public payout batches and explorer transaction links available on the pool site.</p></div><div class="card seo-copy-card"><h3>Payout rules</h3><p>{payout_summary}. Review the <a href="/payouts">recent payouts page</a> for recent on-chain batches.</p></div><div class="card seo-copy-card"><h3>Operational visibility</h3><p>Use the <a href="/status">status page</a> and <a href="/">live dashboard</a> to confirm uptime, daemon health, and current pool activity.</p></div></div></div>"#,
                scheme = escape_html(&state.payout_scheme.trim().to_uppercase()),
                min_payout = escape_html(&format_bnt(state.min_payout_amount)),
                confirmations = state.blocks_before_payout.max(0),
            ),
        ),
        UiRoute::Luck => {
            let totals = context.totals.unwrap_or_default();
            let pending_blocks = totals
                .total_blocks
                .saturating_sub(totals.confirmed_blocks + totals.orphaned_blocks);
            let stats_grid = render_stat_grid(&[
                (
                    "Confirmed Blocks",
                    totals.confirmed_blocks.to_string(),
                    None,
                ),
                ("Pending Blocks", pending_blocks.to_string(), None),
                ("Orphaned Blocks", totals.orphaned_blocks.to_string(), None),
                (
                    "Payout Model",
                    state.payout_scheme.trim().to_uppercase(),
                    Some(pool_payout_summary(state)),
                ),
            ]);
            fallback_page_markup(
                route,
                "Compare round effort and duration over time to understand how actual block discovery compares with expected pool luck.",
                &format!(
                    r#"{stats_grid}<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Round effort matters</h3><p>Luck compares the work spent in a round with the work that was statistically expected before a block was found.</p></div><div class="card seo-copy-card"><h3>Variance is normal</h3><p>Rounds above 100% effort happen naturally. Compare recent rounds together instead of overreacting to one slow block.</p></div><div class="card seo-copy-card"><h3>Use it with block history</h3><p>Pair the <a href="/luck">luck view</a> with the <a href="/blocks">recent blocks page</a> to see how round variance translates into confirmed and orphaned blocks.</p></div></div>{blocks}"#,
                    blocks = render_recent_blocks_section(&context.recent_blocks, true),
                ),
            )
        }
        UiRoute::Blocks => {
            let totals = context.totals.unwrap_or_default();
            let pending_blocks = totals
                .total_blocks
                .saturating_sub(totals.confirmed_blocks + totals.orphaned_blocks);
            let stats_grid = render_stat_grid(&[
                ("Total Blocks", totals.total_blocks.to_string(), None),
                ("Confirmed", totals.confirmed_blocks.to_string(), None),
                ("Pending", pending_blocks.to_string(), None),
                ("Orphaned", totals.orphaned_blocks.to_string(), None),
            ]);
            fallback_page_markup(
                route,
                "Browse confirmed, pending, and orphaned pool blocks with reward, round effort, and elapsed round time for each Blocknet block.",
                &format!(
                    r#"{stats_grid}{blocks}<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Confirmed rounds</h3><p>Confirmed blocks represent rounds that cleared the pool confirmation window and are eligible for payout processing.</p></div><div class="card seo-copy-card"><h3>Pending rounds</h3><p>Pending blocks are fresh finds still moving toward payout eligibility while the chain confirmation count increases.</p></div><div class="card seo-copy-card"><h3>Orphan diagnostics</h3><p>Orphaned rounds are still part of the pool story because they show how often shares landed on losing branches.</p></div></div>"#,
                    blocks = render_recent_blocks_section(&context.recent_blocks, false),
                ),
            )
        }
        UiRoute::Payouts => {
            let stats_grid = render_stat_grid(&[
                ("Pool Fee", pool_fee_summary_for_state(state), None),
                (
                    "Payout Scheme",
                    state.payout_scheme.trim().to_uppercase(),
                    None,
                ),
                ("Min Payout", format_bnt(state.min_payout_amount), None),
                (
                    "Confirmations",
                    state.blocks_before_payout.max(0).to_string(),
                    Some("Required before payout release".to_string()),
                ),
            ]);
            fallback_page_markup(
                route,
                "Review payout totals, recipient counts, network fees, and explorer transaction links for recent pool payout batches.",
                &format!(
                    r#"{stats_grid}{payouts}<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Verified payouts</h3><p>Transaction hashes point to the public Blocknet explorer so payout batches can be checked independently.</p></div><div class="card seo-copy-card"><h3>Batch visibility</h3><p>Each payout batch shows how many miners were paid and how much value moved on-chain in that window.</p></div><div class="card seo-copy-card"><h3>Payout cadence</h3><p>Use the recent payout page together with the <a href="/status">status page</a> and <a href="/">dashboard</a> to understand pool rhythm over time.</p></div></div>"#,
                    payouts = render_recent_payouts_section(&context.recent_payout_batches, false),
                ),
            )
        }
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
        UiRoute::Status => {
            let daemon = context.daemon.clone().unwrap_or_default();
            let stats_grid = render_stat_grid(&[
                (
                    "Daemon",
                    if daemon.reachable {
                        "Online".to_string()
                    } else {
                        "Offline".to_string()
                    },
                    daemon.error.clone(),
                ),
                (
                    "Sync State",
                    if daemon.syncing.unwrap_or(false) {
                        "Syncing".to_string()
                    } else {
                        "Ready".to_string()
                    },
                    None,
                ),
                (
                    "Chain Height",
                    daemon
                        .chain_height
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    None,
                ),
                (
                    "Latest Incident",
                    context
                        .latest_incident
                        .as_ref()
                        .map(|incident| incident.severity.clone())
                        .unwrap_or_else(|| "Clear".to_string()),
                    context
                        .latest_incident
                        .as_ref()
                        .map(|incident| incident.message.clone()),
                ),
            ]);
            let latest_incident = context
                .latest_incident
                .as_ref()
                .map(|incident| {
                    format!(
                        r#"<div class="card section"><h2>Latest incident</h2><p class="section-lead"><strong>{}</strong>: {}. Started {}</p></div>"#,
                        escape_html(&incident.severity),
                        escape_html(&incident.message),
                        escape_html(&format_system_time_short(incident.started_at))
                    )
                })
                .unwrap_or_default();
            fallback_page_markup(
                route,
                "Monitor uptime, daemon reachability, sync state, and recent incident history from the public status page.",
                &format!(
                    r#"{stats_grid}{latest_incident}<div class="seo-copy-grid"><div class="card seo-copy-card"><h3>Daemon reachability</h3><p>Check whether the Blocknet daemon is online and ready to serve work before connecting miners.</p></div><div class="card seo-copy-card"><h3>Historical uptime</h3><p>Review uptime windows over time to understand how stable the pool has been under real load.</p></div><div class="card seo-copy-card"><h3>Incident tracking</h3><p>Scan recent incidents and severity to catch operational issues quickly and compare them against payouts or luck history.</p></div></div>"#
                ),
            )
        }
    }
}

fn structured_data(
    route: UiRoute,
    state: &ApiState,
    context: &UiSeoContext,
    canonical_url: &str,
    title: &str,
    description: &str,
    last_modified: Option<SystemTime>,
) -> serde_json::Value {
    let base_url = pool_base_url(state);
    let og_image_url = format!("{base_url}{OG_IMAGE_PATH}");
    let logo_url = format!("{}/favicon.svg", base_url);
    let website_id = format!("{base_url}/#website");
    let organization_id = format!("{base_url}/#organization");
    let service_id = format!("{base_url}/#service");
    let breadcrumb_id = format!("{canonical_url}#breadcrumb");
    let page_id = format!("{canonical_url}#webpage");

    let mut page = serde_json::json!({
        "@context": "https://schema.org",
        "@type": route.schema_type(),
        "@id": page_id,
        "name": title,
        "url": canonical_url,
        "description": description,
        "inLanguage": "en-US",
        "isPartOf": { "@id": website_id },
        "about": { "@id": service_id },
        "primaryImageOfPage": {
            "@type": "ImageObject",
            "url": og_image_url,
        },
    });
    if !matches!(route, UiRoute::Dashboard) {
        page["breadcrumb"] = serde_json::json!({ "@id": breadcrumb_id });
    }
    if let Some(last_modified) = last_modified {
        page["dateModified"] = serde_json::json!(format_system_time_rfc3339(last_modified));
    }

    let mut items = vec![
        serde_json::json!({
            "@context": "https://schema.org",
            "@type": "Organization",
            "@id": organization_id,
            "name": state.pool_name,
            "url": base_url,
            "logo": {
                "@type": "ImageObject",
                "url": logo_url,
            }
        }),
        serde_json::json!({
            "@context": "https://schema.org",
            "@type": "WebSite",
            "@id": website_id,
            "name": state.pool_name,
            "url": base_url,
            "description": seo_description(UiRoute::Dashboard, state),
            "publisher": { "@id": organization_id },
            "inLanguage": "en-US",
        }),
        serde_json::json!({
            "@context": "https://schema.org",
            "@type": "Service",
            "@id": service_id,
            "name": format!("{} Blocknet mining pool", state.pool_name),
            "serviceType": "Cryptocurrency mining pool",
            "provider": { "@id": organization_id },
            "url": base_url,
            "description": seo_description(UiRoute::Dashboard, state),
            "areaServed": "Worldwide",
            "offers": {
                "@type": "Offer",
                "description": format!(
                    "{} payouts, {}, {}",
                    state.payout_scheme.trim().to_uppercase(),
                    pool_fee_summary_for_state(state),
                    pool_payout_summary(state)
                ),
            }
        }),
        page,
    ];

    if matches!(route, UiRoute::Start) {
        let faq_entries = start_page_faq_entries(state);
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "HowTo",
            "name": "How to mine Blocknet with Seine",
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
                    "text": "Run the miner, then use the pool dashboard and payout pages to verify hashrate, rounds, and payment history.",
                }
            ]
        }));
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "mainEntity": faq_entries
                .into_iter()
                .map(|entry| serde_json::json!({
                    "@type": "Question",
                    "name": entry.question,
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": entry.answer,
                    }
                }))
                .collect::<Vec<_>>(),
        }));
    }

    if matches!(route, UiRoute::Blocks) && !context.recent_blocks.is_empty() {
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "ItemList",
            "name": "Recent Blocknet pool blocks",
            "itemListElement": context
                .recent_blocks
                .iter()
                .take(6)
                .enumerate()
                .map(|(idx, block)| serde_json::json!({
                    "@type": "ListItem",
                    "position": idx + 1,
                    "url": explorer_block_url(&block.hash),
                    "name": format!("Block {} {}", block.height, block_status_label(block)),
                    "description": format!(
                        "{} found {} with reward {}",
                        block_status_label(block),
                        format_system_time_short(block.timestamp),
                        format_atomic_bnt(block.reward)
                    )
                }))
                .collect::<Vec<_>>(),
        }));
    }

    if matches!(route, UiRoute::Payouts) && !context.recent_payout_batches.is_empty() {
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "ItemList",
            "name": "Recent Blocknet pool payout batches",
            "itemListElement": context
                .recent_payout_batches
                .iter()
                .take(6)
                .enumerate()
                .map(|(idx, batch)| serde_json::json!({
                    "@type": "ListItem",
                    "position": idx + 1,
                    "name": format!(
                        "Payout batch {} miners paid",
                        batch.recipient_count
                    ),
                    "url": batch
                        .tx_hashes
                        .first()
                        .map(|hash| explorer_tx_url(hash))
                        .unwrap_or_else(|| canonical_url.to_string()),
                    "description": format!(
                        "{} paid to {} miners with {} network fee at {}",
                        format_atomic_bnt(batch.total_amount),
                        batch.recipient_count,
                        format_atomic_fee(batch.total_fee),
                        format_system_time_short(batch.timestamp)
                    )
                }))
                .collect::<Vec<_>>(),
        }));
    }

    if !matches!(route, UiRoute::Dashboard) {
        items.push(serde_json::json!({
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "@id": breadcrumb_id,
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

async fn build_ui_seo_page(route: UiRoute, state: &ApiState) -> UiSeoPage {
    let site_name = state.pool_name.clone();
    let canonical_url = format!("{}{}", pool_base_url(state), route.path());
    let title = seo_title(route, state);
    let description = seo_description(route, state);
    let og_image_url = format!("{}{}", pool_base_url(state), OG_IMAGE_PATH);
    let og_image_alt = format!(
        "{} Blocknet mining pool card with stratum, fee, and payout details",
        state.pool_name
    );
    let context = load_ui_seo_context(route, state).await;
    let last_modified = ui_seo_last_modified(route, state, &context);
    let json_ld = json_for_script(&structured_data(
        route,
        state,
        &context,
        &canonical_url,
        &title,
        &description,
        last_modified,
    ));
    let content_html = fallback_content(route, state, &context);

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

async fn render_ui_html(route: UiRoute, state: &ApiState) -> String {
    let page = build_ui_seo_page(route, state).await;
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

async fn handle_ui(uri: Uri, State(state): State<ApiState>) -> Response {
    let route = UiRoute::from_path(uri.path());
    let html = render_ui_html(route, &state).await;
    let mut response = Html(html).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if let Ok(value) = HeaderValue::from_str(route.robots()) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-robots-tag"), value);
    }
    response
}

async fn handle_app_fallback(method: Method, uri: Uri, State(state): State<ApiState>) -> Response {
    if is_api_request_path(uri.path()) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"not found"})),
        )
            .into_response();
    }

    if matches!(method, Method::GET | Method::HEAD) {
        let mut response = handle_ui(uri, State(state)).await;
        if method == Method::HEAD {
            *response.body_mut() = Body::empty();
        }
        return response;
    }

    StatusCode::NOT_FOUND.into_response()
}

fn is_api_request_path(path: &str) -> bool {
    path == "/api" || path.starts_with("/api/")
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
    let store = Arc::clone(&state.store);
    let daemon_chain_height = state.node.chain_height();
    let (latest_block, latest_payout) = match tokio::task::spawn_blocking(
        move || -> anyhow::Result<(Option<SystemTime>, Option<SystemTime>)> {
            let latest_block = store
                .get_recent_blocks_up_to(1, daemon_chain_height)?
                .into_iter()
                .next()
                .map(|block| block.timestamp);
            let latest_payout = store
                .get_public_payout_batches_page("time_desc", 1, 0)?
                .0
                .into_iter()
                .next()
                .map(|batch| batch.timestamp);
            Ok((latest_block, latest_payout))
        },
    )
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "failed loading sitemap seo timestamps");
            (None, None)
        }
        Err(err) => {
            tracing::warn!(error = %err, "sitemap seo timestamp join error");
            (None, None)
        }
    };
    let status_updated = {
        let history = state.status_history.lock();
        let latest_closed_incident = history
            .incidents
            .front()
            .map(|incident| incident.ended_at.unwrap_or(incident.started_at));
        let latest_open_incident = latest_time([
            history
                .open_daemon_down
                .as_ref()
                .map(|incident| incident.started_at),
            history
                .open_daemon_syncing
                .as_ref()
                .map(|incident| incident.started_at),
        ]);
        latest_time([
            history.samples.back().map(|sample| sample.timestamp),
            latest_closed_incident,
            latest_open_incident,
        ])
    };
    let entries = [
        (
            "/",
            "weekly",
            "1.0",
            latest_time([Some(state.started_at_system), latest_block, latest_payout]),
        ),
        ("/start", "monthly", "0.95", Some(state.started_at_system)),
        (
            "/blocks",
            "hourly",
            "0.9",
            latest_time([latest_block, Some(state.started_at_system)]),
        ),
        (
            "/payouts",
            "hourly",
            "0.85",
            latest_time([latest_payout, Some(state.started_at_system)]),
        ),
        (
            "/luck",
            "daily",
            "0.75",
            latest_time([latest_block, Some(state.started_at_system)]),
        ),
        (
            "/status",
            "hourly",
            "0.8",
            latest_time([status_updated, Some(state.started_at_system)]),
        ),
    ];
    let urls = entries
        .iter()
        .map(|(path, changefreq, priority, lastmod)| {
            let lastmod_xml = lastmod
                .map(|value| {
                    format!(
                        "<lastmod>{}</lastmod>",
                        escape_html(&format_system_time_rfc3339(value))
                    )
                })
                .unwrap_or_default();
            format!(
                "<url><loc>{}</loc>{}<changefreq>{}</changefreq><priority>{}</priority></url>",
                escape_html(&format!("{base}{path}")),
                lastmod_xml,
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

async fn handle_og_image_svg() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        UI_ASSET_OG_IMAGE_SVG,
    )
}

async fn handle_og_image_png() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        UI_ASSET_OG_IMAGE_PNG,
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
    pplns_window: i32,
    pplns_window_duration: String,
    provisional_share_delay: String,
    max_provisional_recent_verified_multiplier: f64,
    sample_rate: f64,
    warmup_shares: i32,
    min_sample_every: i32,
    payout_min_verified_shares: i32,
    payout_min_verified_ratio: f64,
    payout_provisional_cap_multiplier: f64,
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
        pplns_window: state.config.pplns_window,
        pplns_window_duration: state.config.pplns_window_duration.clone(),
        provisional_share_delay: state.config.provisional_share_delay.clone(),
        max_provisional_recent_verified_multiplier: state
            .config
            .max_provisional_recent_verified_multiplier(),
        sample_rate: state.config.sample_rate,
        warmup_shares: state.config.warmup_shares,
        min_sample_every: state.config.min_sample_every,
        payout_min_verified_shares: state.config.payout_min_verified_shares,
        payout_min_verified_ratio: state.config.payout_min_verified_ratio,
        payout_provisional_cap_multiplier: state.config.payout_provisional_cap_multiplier,
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
            "/api/admin/blocks/{height}/reward-breakdown",
            "/api/health",
            "/api/daemon/logs/stream",
        ],
    })
}

#[derive(Debug, Clone, Serialize)]
struct StatsResponse {
    pool: PoolSummary,
    chain: ChainSummary,
    validation: ValidationSummary,
}

#[derive(Debug, Clone, Serialize)]
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
    paid_to_miners_total: u64,
}

#[derive(Debug, Clone, Serialize)]
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
    audit_queue_depth: usize,
    candidate_oldest_age_millis: Option<u64>,
    regular_oldest_age_millis: Option<u64>,
    audit_oldest_age_millis: Option<u64>,
    candidate_wait: crate::telemetry::PercentileSummary,
    regular_wait: crate::telemetry::PercentileSummary,
    audit_wait: crate::telemetry::PercentileSummary,
    validation_duration: crate::telemetry::PercentileSummary,
    audit_duration: crate::telemetry::PercentileSummary,
    tracked_addresses: usize,
    forced_verify_addresses: usize,
    total_shares: u64,
    sampled_shares: u64,
    invalid_samples: u64,
    pending_provisional: u64,
    fraud_detections: u64,
    candidate_false_claims: u64,
    hot_accepts: u64,
    sync_full_verifies: u64,
    audit_enqueued: u64,
    audit_verified: u64,
    audit_rejected: u64,
    audit_deferred: u64,
    overload_mode: crate::validation::OverloadMode,
    effective_sample_rate: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct SubmitSummary {
    candidate_queue_depth: usize,
    regular_queue_depth: usize,
    candidate_oldest_age_millis: Option<u64>,
    regular_oldest_age_millis: Option<u64>,
    candidate_wait: crate::telemetry::PercentileSummary,
    regular_wait: crate::telemetry::PercentileSummary,
}

fn validation_summary_from_snapshot(snapshot: ValidationSnapshot) -> ValidationSummary {
    ValidationSummary {
        in_flight: snapshot.in_flight,
        candidate_queue_depth: snapshot.candidate_queue_depth,
        regular_queue_depth: snapshot.regular_queue_depth,
        audit_queue_depth: snapshot.audit_queue_depth,
        candidate_oldest_age_millis: snapshot.candidate_oldest_age_millis,
        regular_oldest_age_millis: snapshot.regular_oldest_age_millis,
        audit_oldest_age_millis: snapshot.audit_oldest_age_millis,
        candidate_wait: snapshot.candidate_wait,
        regular_wait: snapshot.regular_wait,
        audit_wait: snapshot.audit_wait,
        validation_duration: snapshot.validation_duration,
        audit_duration: snapshot.audit_duration,
        tracked_addresses: snapshot.tracked_addresses,
        forced_verify_addresses: snapshot.forced_verify_addresses,
        total_shares: snapshot.total_shares,
        sampled_shares: snapshot.sampled_shares,
        invalid_samples: snapshot.invalid_samples,
        pending_provisional: snapshot.pending_provisional,
        fraud_detections: snapshot.fraud_detections,
        candidate_false_claims: snapshot.candidate_false_claims,
        hot_accepts: snapshot.hot_accepts,
        sync_full_verifies: snapshot.sync_full_verifies,
        audit_enqueued: snapshot.audit_enqueued,
        audit_verified: snapshot.audit_verified,
        audit_rejected: snapshot.audit_rejected,
        audit_deferred: snapshot.audit_deferred,
        overload_mode: snapshot.overload_mode,
        effective_sample_rate: snapshot.effective_sample_rate,
    }
}

fn validation_summary_from_persisted(summary: &PersistedValidationSummary) -> ValidationSummary {
    ValidationSummary {
        in_flight: summary.in_flight,
        candidate_queue_depth: summary.candidate_queue_depth,
        regular_queue_depth: summary.regular_queue_depth,
        audit_queue_depth: summary.audit_queue_depth,
        candidate_oldest_age_millis: summary.candidate_oldest_age_millis,
        regular_oldest_age_millis: summary.regular_oldest_age_millis,
        audit_oldest_age_millis: summary.audit_oldest_age_millis,
        candidate_wait: summary.candidate_wait,
        regular_wait: summary.regular_wait,
        audit_wait: summary.audit_wait,
        validation_duration: summary.validation_duration,
        audit_duration: summary.audit_duration,
        tracked_addresses: summary.tracked_addresses,
        forced_verify_addresses: summary.forced_verify_addresses,
        total_shares: summary.total_shares,
        sampled_shares: summary.sampled_shares,
        invalid_samples: summary.invalid_samples,
        pending_provisional: summary.pending_provisional,
        fraud_detections: summary.fraud_detections,
        candidate_false_claims: summary.candidate_false_claims,
        hot_accepts: summary.hot_accepts,
        sync_full_verifies: summary.sync_full_verifies,
        audit_enqueued: summary.audit_enqueued,
        audit_verified: summary.audit_verified,
        audit_rejected: summary.audit_rejected,
        audit_deferred: summary.audit_deferred,
        overload_mode: summary.overload_mode,
        effective_sample_rate: summary.effective_sample_rate,
    }
}

fn merge_percentile_summary(
    live: crate::telemetry::PercentileSummary,
    persisted: crate::telemetry::PercentileSummary,
) -> crate::telemetry::PercentileSummary {
    if persisted.samples >= live.samples {
        persisted
    } else {
        live
    }
}

fn merge_oldest_age(live: Option<u64>, persisted: Option<u64>) -> Option<u64> {
    match (live, persisted) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn overload_mode_rank(mode: crate::validation::OverloadMode) -> u8 {
    match mode {
        crate::validation::OverloadMode::Normal => 0,
        crate::validation::OverloadMode::Shed => 1,
        crate::validation::OverloadMode::Emergency => 2,
    }
}

fn merge_validation_summary(
    live: ValidationSummary,
    persisted: &PersistedValidationSummary,
) -> ValidationSummary {
    let persisted = validation_summary_from_persisted(persisted);
    let overload_mode =
        if overload_mode_rank(persisted.overload_mode) >= overload_mode_rank(live.overload_mode) {
            persisted.overload_mode
        } else {
            live.overload_mode
        };
    let effective_sample_rate = match overload_mode {
        crate::validation::OverloadMode::Emergency => 0.0,
        _ if overload_mode == live.overload_mode => live.effective_sample_rate,
        _ => persisted.effective_sample_rate,
    };
    ValidationSummary {
        in_flight: live.in_flight.max(persisted.in_flight),
        candidate_queue_depth: live
            .candidate_queue_depth
            .max(persisted.candidate_queue_depth),
        regular_queue_depth: live.regular_queue_depth.max(persisted.regular_queue_depth),
        audit_queue_depth: live.audit_queue_depth.max(persisted.audit_queue_depth),
        candidate_oldest_age_millis: merge_oldest_age(
            live.candidate_oldest_age_millis,
            persisted.candidate_oldest_age_millis,
        ),
        regular_oldest_age_millis: merge_oldest_age(
            live.regular_oldest_age_millis,
            persisted.regular_oldest_age_millis,
        ),
        audit_oldest_age_millis: merge_oldest_age(
            live.audit_oldest_age_millis,
            persisted.audit_oldest_age_millis,
        ),
        candidate_wait: merge_percentile_summary(live.candidate_wait, persisted.candidate_wait),
        regular_wait: merge_percentile_summary(live.regular_wait, persisted.regular_wait),
        audit_wait: merge_percentile_summary(live.audit_wait, persisted.audit_wait),
        validation_duration: merge_percentile_summary(
            live.validation_duration,
            persisted.validation_duration,
        ),
        audit_duration: merge_percentile_summary(live.audit_duration, persisted.audit_duration),
        tracked_addresses: live.tracked_addresses.max(persisted.tracked_addresses),
        forced_verify_addresses: live
            .forced_verify_addresses
            .max(persisted.forced_verify_addresses),
        total_shares: live.total_shares.max(persisted.total_shares),
        sampled_shares: live.sampled_shares.max(persisted.sampled_shares),
        invalid_samples: live.invalid_samples.max(persisted.invalid_samples),
        pending_provisional: live.pending_provisional.max(persisted.pending_provisional),
        fraud_detections: live.fraud_detections.max(persisted.fraud_detections),
        candidate_false_claims: live
            .candidate_false_claims
            .max(persisted.candidate_false_claims),
        hot_accepts: live.hot_accepts.max(persisted.hot_accepts),
        sync_full_verifies: live.sync_full_verifies.max(persisted.sync_full_verifies),
        audit_enqueued: live.audit_enqueued.max(persisted.audit_enqueued),
        audit_verified: live.audit_verified.max(persisted.audit_verified),
        audit_rejected: live.audit_rejected.max(persisted.audit_rejected),
        audit_deferred: live.audit_deferred.max(persisted.audit_deferred),
        overload_mode,
        effective_sample_rate,
    }
}

fn submit_summary_from_persisted(snapshot: Option<&PersistedRuntimeSnapshot>) -> SubmitSummary {
    let Some(snapshot) = snapshot else {
        return SubmitSummary::default();
    };
    SubmitSummary {
        candidate_queue_depth: snapshot.submit.candidate_queue_depth,
        regular_queue_depth: snapshot.submit.regular_queue_depth,
        candidate_oldest_age_millis: snapshot.submit.candidate_oldest_age_millis,
        regular_oldest_age_millis: snapshot.submit.regular_oldest_age_millis,
        candidate_wait: snapshot.submit.candidate_wait,
        regular_wait: snapshot.submit.regular_wait,
    }
}

fn validation_summary_is_empty(summary: &ValidationSummary) -> bool {
    summary.in_flight == 0
        && summary.candidate_queue_depth == 0
        && summary.regular_queue_depth == 0
        && summary.audit_queue_depth == 0
        && summary.candidate_oldest_age_millis.is_none()
        && summary.regular_oldest_age_millis.is_none()
        && summary.audit_oldest_age_millis.is_none()
        && summary.tracked_addresses == 0
        && summary.forced_verify_addresses == 0
        && summary.total_shares == 0
        && summary.sampled_shares == 0
        && summary.invalid_samples == 0
        && summary.pending_provisional == 0
        && summary.fraud_detections == 0
        && summary.candidate_false_claims == 0
        && summary.hot_accepts == 0
        && summary.sync_full_verifies == 0
        && summary.audit_enqueued == 0
        && summary.audit_verified == 0
        && summary.audit_rejected == 0
        && summary.audit_deferred == 0
}

fn pool_snapshot_has_live_data(snapshot: &PoolSnapshot) -> bool {
    snapshot.connected_miners > 0
        || snapshot.connected_workers > 0
        || snapshot.estimated_hashrate > 0.0
}

#[derive(Serialize)]
struct FeesResponse {
    total_collected: u64,
    total_pending: u64,
    recent: Vec<PoolFeeEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct FeePageItem {
    block_height: u64,
    amount: u64,
    fee_address: String,
    timestamp: SystemTime,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    confirmations_remaining: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct BlockRewardBreakdownResponse {
    block: DbBlock,
    payout_scheme: String,
    share_window: RewardWindowSummary,
    fee_amount: u64,
    distributable_reward: u64,
    preview_total_weight: u64,
    payout_total_weight: u64,
    actual_credit_events_available: bool,
    actual_credit_total: u64,
    actual_fee_amount: Option<u64>,
    participants: Vec<BlockRewardParticipantResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct RewardWindowSummary {
    label: String,
    start: Option<SystemTime>,
    end: SystemTime,
    share_count: usize,
    participant_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct BlockRewardParticipantResponse {
    address: String,
    finder: bool,
    risky: bool,
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_shares_eligible: u64,
    provisional_difficulty_eligible: u64,
    provisional_shares_ineligible: u64,
    provisional_difficulty_ineligible: u64,
    preview_weight: u64,
    preview_share_pct: f64,
    preview_credit: u64,
    preview_status: String,
    payout_weight: u64,
    payout_share_pct: f64,
    payout_credit: u64,
    payout_status: String,
    actual_credit: Option<u64>,
    delta_vs_payout: Option<i64>,
}

#[derive(Serialize)]
struct HealthResponse {
    uptime_seconds: u64,
    api_key_configured: bool,
    daemon: DaemonHealth,
    job: JobHealth,
    payouts: PayoutHealth,
    wallet: Option<WalletHealth>,
    validation: ValidationSummary,
    pool_activity: PoolActivityHealth,
    active_verification_holds: Vec<ActiveVerificationHold>,
}

#[derive(Debug, Clone, Serialize)]
struct PoolActivityHealth {
    state: String,
    detail: String,
    connected_miners: u64,
    connected_workers: u64,
    estimated_hashrate: f64,
    snapshot_age_seconds: Option<u64>,
    last_share_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminShareDiagnosticsResponse {
    generated_at: SystemTime,
    windows: Vec<AdminShareWindowResponse>,
    submit: SubmitSummary,
    validation: ValidationSummary,
    job: JobHealth,
    pool_activity: PoolActivityHealth,
}

#[derive(Debug, Clone, Serialize)]
struct AdminShareWindowResponse {
    label: String,
    window_seconds: u64,
    accepted: u64,
    rejected: u64,
    total: u64,
    rejection_rate_pct: f64,
    by_reason: Vec<RejectionReasonCount>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminDevFeeTelemetryResponse {
    address: String,
    reference_target_pct: f64,
    hint_floor: u64,
    windows: Vec<AdminDevFeeWindowResponse>,
    hints: AdminDevFeeHintSummaryResponse,
    recent_hints: Vec<AdminDevFeeHintRowResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminDevFeeWindowResponse {
    label: String,
    window_seconds: u64,
    pool_accepted_difficulty: u64,
    dev_accepted_difficulty: u64,
    dev_rejected_difficulty: u64,
    dev_gross_difficulty: u64,
    accepted_shares: u64,
    rejected_shares: u64,
    stale_rejected_shares: u64,
    stale_rejected_difficulty: u64,
    accepted_pct: f64,
    gross_pct: f64,
    reject_rate_pct: f64,
    stale_reject_rate_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminDevFeeHintSummaryResponse {
    total_workers: u64,
    below_floor_workers: u64,
    at_floor_workers: u64,
    above_floor_workers: u64,
    min_difficulty: Option<u64>,
    median_difficulty: Option<u64>,
    max_difficulty: Option<u64>,
    latest_updated_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminDevFeeHintRowResponse {
    worker: String,
    difficulty: u64,
    updated_at: SystemTime,
    position: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct AdminBalanceOverviewResponse {
    generated_at: SystemTime,
    wallet: AdminBalanceOverviewWallet,
    payouts: AdminBalanceOverviewPayouts,
    outputs: AdminBalanceOverviewOutputs,
    ledger: AdminBalanceOverviewLedger,
    liquidity: AdminBalanceOverviewLiquidity,
}

#[derive(Debug, Clone, Serialize)]
struct AdminBalanceOverviewWallet {
    spendable: u64,
    pending: u64,
    pending_unconfirmed: u64,
    pending_unconfirmed_eta: u64,
    total: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminBalanceOverviewPayouts {
    unpaid_count: usize,
    unpaid_amount: u64,
    clean_unpaid_count: usize,
    clean_unpaid_amount: u64,
    orphan_backed_unpaid_amount: u64,
    balance_source_drift_amount: u64,
    pool_fee_unpaid_amount: u64,
    pool_fee_clean_unpaid_amount: u64,
    pool_fee_orphan_backed_unpaid_amount: u64,
    pool_fee_balance_source_drift_amount: u64,
    queued_count: usize,
    queued_amount: u64,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
struct AdminBalanceOverviewOutputs {
    live_count: usize,
    spendable_count: usize,
    pending_count: usize,
    spendable_coinbase_count: usize,
    spendable_coinbase_amount: u64,
    spendable_regular_count: usize,
    spendable_regular_amount: u64,
    pending_coinbase_count: usize,
    pending_coinbase_amount: u64,
    pending_regular_count: usize,
    pending_regular_amount: u64,
    pending_regular_matched_payout_count: usize,
    pending_regular_matched_payout_amount: u64,
    pending_regular_unmatched_count: usize,
    pending_regular_unmatched_amount: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminBalanceOverviewLedger {
    miner_paid_total: u64,
    miner_unpaid_total: u64,
    miner_total_credited: u64,
    miner_clean_unpaid_total: u64,
    miner_orphan_backed_unpaid_total: u64,
    miner_balance_source_drift_total: u64,
    net_block_reward_total: u64,
    pool_fee_total: u64,
    pool_fee_paid_total: u64,
    pool_fee_unpaid_total: u64,
    pool_fee_clean_unpaid_total: u64,
    pool_fee_orphan_backed_unpaid_total: u64,
    pool_fee_balance_source_drift_total: u64,
    pool_fee_balance_total: u64,
    miner_rewards_balanced: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AdminBalanceOverviewLiquidity {
    spendable_minus_queued: i64,
    queue_shortfall_amount: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationIssuesResponse {
    generated_at: SystemTime,
    summary: AdminReconciliationIssuesSummary,
    missing_payouts: Vec<AdminMissingCompletedPayoutIssueResponse>,
    orphaned_blocks: Vec<AdminOrphanedBlockIssueResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationIssuesSummary {
    total_open_issues: usize,
    missing_payout_issue_count: usize,
    missing_payout_total_amount: u64,
    orphaned_block_issue_count: usize,
    orphaned_block_total_credit_amount: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminMissingCompletedPayoutIssueResponse {
    tx_hash: String,
    payout_row_count: usize,
    total_amount: u64,
    total_fee: u64,
    latest_timestamp: SystemTime,
    addresses: Vec<String>,
    linked_amount: u64,
    live_linked_amount: u64,
    orphaned_linked_amount: u64,
    unlinked_amount: u64,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationPayoutResolutionResponse {
    tx_hash: String,
    action: &'static str,
    reverted_payout_rows: u64,
    restored_pending_amount: u64,
    dropped_amount: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationPayoutImportResponse {
    imported_tx_count: u64,
    imported_payout_rows: u64,
    imported_amount: u64,
    imported_fee: u64,
    canceled_pending_payouts: u64,
    recorded_manual_offset_amount: u64,
    imported_txs: Vec<AdminReconciliationImportedPayoutTxResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationImportedPayoutTxResponse {
    tx_hash: String,
    payout_row_count: usize,
    total_amount: u64,
    total_fee: u64,
    timestamp: SystemTime,
    addresses: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationManualOffsetApplyResponse {
    scanned_offset_addresses: u64,
    offset_amount_before: u64,
    applied_address_count: u64,
    applied_amount: u64,
    remaining_offset_amount: u64,
    applications: Vec<AdminReconciliationManualOffsetApplicationResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct AdminReconciliationManualOffsetApplicationResponse {
    address: String,
    applied_amount: u64,
    remaining_offset_amount: u64,
    remaining_balance_pending: u64,
    remaining_canonical_pending: u64,
}

#[derive(Debug, Clone, Serialize)]
struct AdminOrphanedBlockCleanupResponse {
    block_height: u64,
    orphaned: bool,
    reversed_credit_events: u64,
    reversed_credit_amount: u64,
    reversed_fee_amount: u64,
    canceled_pending_payouts: u64,
    manual_reconciliation_required: bool,
}

fn map_orphaned_block_issue(issue: OrphanedBlockCreditIssue) -> AdminOrphanedBlockIssueResponse {
    AdminOrphanedBlockIssueResponse {
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
    }
}

fn map_manual_payout_offset_application(
    application: ManualPayoutOffsetApplication,
) -> AdminReconciliationManualOffsetApplicationResponse {
    AdminReconciliationManualOffsetApplicationResponse {
        address: application.address,
        applied_amount: application.applied_amount,
        remaining_offset_amount: application.remaining_offset_amount,
        remaining_balance_pending: application.remaining_balance_pending,
        remaining_canonical_pending: application.remaining_canonical_pending,
    }
}

#[derive(Debug, Clone, Serialize)]
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
    unpaid_count: usize,
    unpaid_amount: u64,
    queued_count: usize,
    queued_amount: u64,
    pending_count: usize,
    pending_amount: u64,
    last_payout: Option<Payout>,
}

#[derive(Serialize)]
struct WalletHealth {
    spendable: u64,
    pending: u64,
    pending_unconfirmed: u64,
    pending_unconfirmed_eta: u64,
    total: u64,
}

#[derive(Debug, Deserialize, Default)]
struct StatsHistoryQuery {
    range: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RewardWindowAddressStats {
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_shares_eligible: u64,
    provisional_difficulty_eligible: u64,
    provisional_shares_ineligible: u64,
    provisional_difficulty_ineligible: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewardParticipantStatus {
    Included,
    CappedProvisional,
    AwaitingVerifiedShares,
    AwaitingVerifiedRatio,
    NoEligibleShares,
    FinderFallback,
    RecordedOnly,
}

impl RewardParticipantStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Included => "included",
            Self::CappedProvisional => "capped_provisional",
            Self::AwaitingVerifiedShares => "awaiting_verified_shares",
            Self::AwaitingVerifiedRatio => "awaiting_verified_ratio",
            Self::NoEligibleShares => "no_eligible_shares",
            Self::FinderFallback => "finder_fallback",
            Self::RecordedOnly => "recorded_only",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RewardModeComputation {
    weights: HashMap<String, u64>,
    credits: HashMap<String, u64>,
    statuses: HashMap<String, RewardParticipantStatus>,
    total_weight: u64,
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
struct MinerBalanceQuery {
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
struct AdminBalancesQuery {
    search: Option<String>,
    sort: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
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
struct AdminBalanceItem {
    address: String,
    clean_payable: u64,
    orphan_backed: u64,
    pending: u64,
    paid: u64,
}

#[derive(Debug)]
struct FeePageData {
    total_collected: u64,
    total_pending: u64,
    items: Vec<FeePageItem>,
    total: usize,
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
    match state.cached_stats_response().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => return internal_error("failed loading pool stats", err).into_response(),
    }
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

fn ratio_pct(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}

fn signed_amount_delta(lhs: u64, rhs: u64) -> i64 {
    let lhs = lhs as i128;
    let rhs = rhs as i128;
    let delta = lhs - rhs;
    delta.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

fn summarize_admin_balance_outputs(
    outputs: &[WalletOutput],
    matched_payout_tx_hashes: &HashSet<String>,
) -> AdminBalanceOverviewOutputs {
    let mut summary = AdminBalanceOverviewOutputs::default();

    for output in outputs {
        let is_spendable = output.is_spendable();
        let is_spent = output.status.eq_ignore_ascii_case("spent");
        if is_spent {
            continue;
        }

        summary.live_count += 1;

        if is_spendable {
            summary.spendable_count += 1;
            if output.r#type.eq_ignore_ascii_case("coinbase") {
                summary.spendable_coinbase_count += 1;
                summary.spendable_coinbase_amount = summary
                    .spendable_coinbase_amount
                    .saturating_add(output.amount);
            } else {
                summary.spendable_regular_count += 1;
                summary.spendable_regular_amount = summary
                    .spendable_regular_amount
                    .saturating_add(output.amount);
            }
            continue;
        }

        summary.pending_count += 1;
        if output.r#type.eq_ignore_ascii_case("coinbase") {
            summary.pending_coinbase_count += 1;
            summary.pending_coinbase_amount = summary
                .pending_coinbase_amount
                .saturating_add(output.amount);
            continue;
        }

        summary.pending_regular_count += 1;
        summary.pending_regular_amount =
            summary.pending_regular_amount.saturating_add(output.amount);
        if matched_payout_tx_hashes.contains(&output.txid) {
            summary.pending_regular_matched_payout_count += 1;
            summary.pending_regular_matched_payout_amount = summary
                .pending_regular_matched_payout_amount
                .saturating_add(output.amount);
        } else {
            summary.pending_regular_unmatched_count += 1;
            summary.pending_regular_unmatched_amount = summary
                .pending_regular_unmatched_amount
                .saturating_add(output.amount);
        }
    }

    summary
}

fn effective_job_health(
    state: &ApiState,
    persisted_runtime: Option<&PersistedRuntimeSnapshot>,
) -> JobHealth {
    let current_job = state.jobs.current_job();
    let mut current_height = current_job.as_ref().map(|job| job.height);
    let mut current_difficulty = current_job.as_ref().map(|job| job.network_difficulty);
    let mut template_id = current_job.as_ref().and_then(|job| job.template_id.clone());
    let mut template_age_seconds = state.jobs.current_job_age().map(|age| age.as_secs());
    let mut last_refresh_millis = state
        .jobs
        .last_refresh_elapsed()
        .map(|age| age.as_millis() as u64);
    let mut tracked_templates = state.jobs.tracked_job_count();
    let mut active_assignments = state.jobs.active_assignment_count();

    if let Some(persisted) = persisted_runtime {
        current_height = current_height.or(persisted.jobs.current_height);
        current_difficulty = current_difficulty.or(persisted.jobs.current_difficulty);
        if template_id.is_none() {
            template_id = persisted.jobs.template_id.clone();
        }
        template_age_seconds = template_age_seconds.or(persisted.jobs.template_age_seconds);
        last_refresh_millis = last_refresh_millis.or(persisted.jobs.last_refresh_millis);
        tracked_templates = tracked_templates.max(persisted.jobs.tracked_templates);
        active_assignments = active_assignments.max(persisted.jobs.active_assignments);
    }

    JobHealth {
        current_height,
        current_difficulty,
        template_id,
        template_age_seconds,
        last_refresh_millis,
        tracked_templates,
        active_assignments,
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
        let total = accepted.saturating_add(rejected);
        rows.push(AdminShareWindowResponse {
            label: label.to_string(),
            window_seconds: window.as_secs(),
            accepted,
            rejected,
            total,
            rejection_rate_pct: ratio_pct(rejected, total),
            by_reason,
        });
    }
    Ok(rows)
}

fn unix_secs_to_system_time(value: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(value)
}

fn service_health_from_local(
    latest: Option<&MonitorHeartbeat>,
    healthy_fn: impl Fn(&MonitorHeartbeat) -> Option<bool>,
    missing_message: &str,
) -> ServiceHealth {
    let Some(row) = latest else {
        return ServiceHealth {
            observed: false,
            healthy: false,
            last_sample_at: None,
            message: Some(missing_message.to_string()),
        };
    };
    let fresh = SystemTime::now()
        .duration_since(row.sampled_at)
        .unwrap_or_default()
        <= Duration::from_secs(30);
    let healthy = healthy_fn(row).unwrap_or(false) && fresh;
    ServiceHealth {
        observed: fresh,
        healthy,
        last_sample_at: Some(row.sampled_at),
        message: if fresh {
            None
        } else {
            Some(missing_message.to_string())
        },
    }
}

fn service_health_from_public(latest: Option<&MonitorHeartbeat>) -> ServiceHealth {
    let Some(row) = latest else {
        return ServiceHealth {
            observed: false,
            healthy: false,
            last_sample_at: None,
            message: Some("no recent public HTTP probe".to_string()),
        };
    };
    let fresh = SystemTime::now()
        .duration_since(row.sampled_at)
        .unwrap_or_default()
        <= Duration::from_secs(10 * 60);
    ServiceHealth {
        observed: fresh,
        healthy: row.public_http_up.unwrap_or(false) && fresh,
        last_sample_at: Some(row.sampled_at),
        message: if fresh {
            None
        } else {
            Some("public HTTP probe is stale".to_string())
        },
    }
}

fn daemon_health_from_heartbeat(latest: Option<&MonitorHeartbeat>) -> DaemonHealth {
    let details = latest.and_then(parse_monitor_heartbeat_details);
    DaemonHealth {
        reachable: latest.and_then(|row| row.daemon_up).unwrap_or(false),
        chain_height: latest.and_then(|row| row.chain_height),
        peers: None,
        syncing: latest.and_then(|row| row.daemon_syncing),
        mempool_size: None,
        best_hash: None,
        current_process_block: details
            .as_ref()
            .and_then(|details| details.daemon_current_process_block.clone()),
        last_process_block: details
            .as_ref()
            .and_then(|details| details.daemon_last_process_block.clone()),
        error: latest
            .filter(|row| row.daemon_up == Some(false))
            .and_then(|row| {
                details
                    .as_ref()
                    .and_then(|details| details.daemon_error.clone())
                    .or_else(|| row.details_json.clone())
            }),
    }
}

#[derive(Debug, Clone, Deserialize)]
struct MonitorHeartbeatDetails {
    #[serde(default)]
    daemon_error: Option<String>,
    #[serde(default)]
    daemon_current_process_block: Option<NodeCurrentProcessBlock>,
    #[serde(default)]
    daemon_last_process_block: Option<NodeLastProcessBlock>,
}

fn parse_monitor_heartbeat_details(row: &MonitorHeartbeat) -> Option<MonitorHeartbeatDetails> {
    row.details_json
        .as_ref()
        .and_then(|raw| serde_json::from_str(raw).ok())
}

fn build_monitor_uptime_window(
    label: &str,
    window: Duration,
    local: &MonitorUptimeSummary,
    external: &MonitorUptimeSummary,
) -> UptimeWindow {
    UptimeWindow {
        label: label.to_string(),
        window_seconds: window.as_secs(),
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
    StatusIncident {
        id: incident.id as u64,
        kind: incident.kind,
        severity: incident.severity,
        started_at: incident.started_at,
        ended_at: incident.ended_at,
        duration_seconds: incident
            .ended_at
            .unwrap_or(now)
            .duration_since(incident.started_at)
            .ok()
            .map(|elapsed| elapsed.as_secs()),
        message: incident.summary,
        ongoing: incident.ended_at.is_none(),
    }
}

fn cloudflare_heartbeat(
    sampled_at: SystemTime,
    public_http_up: bool,
    synthetic: bool,
    detail: Option<String>,
) -> crate::db::MonitorHeartbeatUpsert {
    crate::db::MonitorHeartbeatUpsert {
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

    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
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

async fn handle_status(State(state): State<ApiState>) -> impl IntoResponse {
    match state.build_status_response().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => internal_error("failed loading status page", err).into_response(),
    }
}

async fn handle_monitor_public(State(state): State<ApiState>) -> impl IntoResponse {
    let checked_at = SystemTime::now();
    let latest = {
        let store = Arc::clone(&state.store);
        tokio::task::spawn_blocking(move || {
            let local = store.get_latest_monitor_heartbeat(Some(LOCAL_MONITOR_SOURCE))?;
            let external = store.get_latest_monitor_heartbeat(Some(CLOUDFLARE_MONITOR_SOURCE))?;
            Ok::<_, anyhow::Error>((local, external))
        })
        .await
    };

    let (local, external) = match latest {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "failed loading public monitor heartbeat");
            (None, None)
        }
        Err(err) => {
            tracing::warn!(error = %err, "public monitor heartbeat join failed");
            (None, None)
        }
    };

    Json(serde_json::json!({
        "ok": true,
        "checked_at": checked_at,
        "pool_name": state.pool_name,
        "summary_state": local.as_ref().map(|row| row.summary_state.clone()).unwrap_or_else(|| "unknown".to_string()),
        "latest_local_sample_at": local.as_ref().map(|row| row.sampled_at),
        "latest_public_http_sample_at": external.as_ref().map(|row| row.sampled_at),
    }))
}

#[derive(Debug, Deserialize)]
struct CloudflareIngestEvent {
    service: String,
    status: String,
    started_at: Option<u64>,
    ended_at: Option<u64>,
    checked_at: Option<u64>,
    summary: Option<String>,
    detail: Option<String>,
}

async fn handle_monitor_ingest_cloudflare(
    State(state): State<ApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let secret = state.config.monitor_ingest_secret.trim();
    if secret.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"monitor ingest secret not configured"})),
        )
            .into_response();
    }

    let provided = headers
        .get("x-monitor-signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !verify_monitor_signature(secret, provided, &body) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"invalid monitor signature"})),
        )
            .into_response();
    }

    let event: CloudflareIngestEvent = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("invalid JSON payload: {err}")})),
            )
                .into_response();
        }
    };

    if !event.service.trim().eq_ignore_ascii_case("public_http") {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"unsupported monitor service"})),
        )
            .into_response();
    }

    let store = Arc::clone(&state.store);
    let action = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let ended_at = unix_secs_to_system_time(
            event
                .ended_at
                .or(event.checked_at)
                .unwrap_or_else(|| system_time_to_unix_secs(SystemTime::now())),
        );
        let started_at = event.started_at.map(unix_secs_to_system_time);
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
            store.upsert_monitor_incident(&crate::db::MonitorIncidentUpsert {
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
            store.upsert_monitor_incident(&crate::db::MonitorIncidentUpsert {
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
        Ok(Ok(())) => StatusCode::ACCEPTED.into_response(),
        Ok(Err(err)) => {
            internal_error("failed storing cloudflare monitor event", err).into_response()
        }
        Err(err) => internal_error(
            "failed storing cloudflare monitor event",
            anyhow::anyhow!("join error: {err}"),
        )
        .into_response(),
    }
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
                let snap = state.effective_pool_snapshot().await;
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

async fn handle_admin_dev_fee(State(state): State<ApiState>) -> impl IntoResponse {
    match state.admin_dev_fee_telemetry().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => internal_error("failed loading dev fee telemetry", err).into_response(),
    }
}

async fn handle_admin_perf(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.performance_response()).into_response()
}

async fn handle_admin_balances(
    Query(query): Query<AdminBalancesQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let search = query.search.clone().unwrap_or_default();
    let sort = query
        .sort
        .clone()
        .unwrap_or_else(|| "pending_desc".to_string());
    let (limit, offset) = page_bounds(query.limit, query.offset);

    match tokio::task::spawn_blocking(move || -> anyhow::Result<PagedResponse<AdminBalanceItem>> {
        let all = store.get_all_balances()?;
        let source_by_address = store
            .list_balance_source_summaries()?
            .into_iter()
            .map(|source| (source.address.clone(), source))
            .collect::<HashMap<_, BalanceSourceSummary>>();
        let mut filtered: Vec<_> = if search.is_empty() {
            all
        } else {
            let needle = search.to_lowercase();
            all.into_iter()
                .filter(|b| b.address.to_lowercase().contains(&needle))
                .collect()
        };

        match sort.as_str() {
            "pending_asc" => filtered.sort_by(|a, b| a.pending.cmp(&b.pending)),
            "paid_desc" => filtered.sort_by(|a, b| b.paid.cmp(&a.paid)),
            "paid_asc" => filtered.sort_by(|a, b| a.paid.cmp(&b.paid)),
            "address_asc" => filtered.sort_by(|a, b| a.address.cmp(&b.address)),
            "address_desc" => filtered.sort_by(|a, b| b.address.cmp(&a.address)),
            _ => filtered.sort_by(|a, b| b.pending.cmp(&a.pending)), // pending_desc default
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
                AdminBalanceItem {
                    address: b.address,
                    clean_payable: source.canonical_pending,
                    orphan_backed: source.orphan_pending,
                    pending: b.pending,
                    paid: b.paid,
                }
            })
            .collect();
        let returned = items.len();

        Ok(PagedResponse {
            items,
            page: PageMeta {
                limit,
                offset,
                returned,
                total,
            },
        })
    })
    .await
    {
        Ok(Ok(resp)) => Json(resp).into_response(),
        Ok(Err(err)) => internal_error("failed loading balances", err).into_response(),
        Err(err) => internal_error(
            "failed loading balances",
            anyhow::anyhow!("join error: {err}"),
        )
        .into_response(),
    }
}

async fn handle_admin_balance_overview(State(state): State<ApiState>) -> impl IntoResponse {
    match state.admin_balance_overview().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => internal_error("failed loading balance overview", err).into_response(),
    }
}

async fn handle_admin_reconciliation_issues(State(state): State<ApiState>) -> impl IntoResponse {
    match state.admin_reconciliation_issues().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => internal_error("failed loading reconciliation issues", err).into_response(),
    }
}

async fn handle_admin_reconciliation_payout_resolution(
    State(state): State<ApiState>,
    Json(request): Json<AdminReconciliationPayoutResolutionRequest>,
) -> impl IntoResponse {
    let tx_hash = request.tx_hash.trim();
    if tx_hash.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"tx_hash is required"})),
        )
            .into_response();
    }

    match state
        .resolve_missing_completed_payout_issue(tx_hash, request.action)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(err) => {
            internal_error("failed resolving reconciliation payout issue", err).into_response()
        }
    }
}

async fn handle_admin_reconciliation_payout_import(
    State(state): State<ApiState>,
    Json(request): Json<AdminReconciliationPayoutImportRequest>,
) -> impl IntoResponse {
    let tx_hashes = match normalize_reconciliation_import_tx_hashes(&request.tx_hashes) {
        Ok(tx_hashes) => tx_hashes,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": err.to_string() })),
            )
                .into_response();
        }
    };

    match state.import_confirmed_wallet_payouts(tx_hashes).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => {
            internal_error("failed importing confirmed payout txs into ledger", err).into_response()
        }
    }
}

async fn handle_admin_reconciliation_manual_offset_apply(
    State(state): State<ApiState>,
) -> impl IntoResponse {
    match state.apply_live_manual_payout_offsets().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => internal_error("failed applying manual payout offsets", err).into_response(),
    }
}

async fn handle_admin_orphaned_block_cleanup_retry(
    State(state): State<ApiState>,
    Json(request): Json<AdminOrphanedBlockCleanupRequest>,
) -> impl IntoResponse {
    if request.block_height == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"block_height must be positive"})),
        )
            .into_response();
    }

    let store = Arc::clone(&state.store);
    match tokio::task::spawn_blocking(move || {
        store.reconcile_existing_orphaned_block_credits(request.block_height)
    })
    .await
    {
        Ok(Ok(result)) => Json(AdminOrphanedBlockCleanupResponse {
            block_height: request.block_height,
            orphaned: result.orphaned,
            reversed_credit_events: result.reversed_credit_events,
            reversed_credit_amount: result.reversed_credit_amount,
            reversed_fee_amount: result.reversed_fee_amount,
            canceled_pending_payouts: result.canceled_pending_payouts,
            manual_reconciliation_required: result.manual_reconciliation_required,
        })
        .into_response(),
        Ok(Err(err)) => {
            internal_error("failed retrying orphaned block cleanup", err).into_response()
        }
        Err(err) => internal_error(
            "failed retrying orphaned block cleanup",
            anyhow::anyhow!("join error: {err}"),
        )
        .into_response(),
    }
}

async fn handle_health(State(state): State<ApiState>) -> impl IntoResponse {
    let provisional_cutoff = SystemTime::now()
        .checked_sub(state.config.provisional_share_delay_duration())
        .unwrap_or(UNIX_EPOCH);
    let daemon = state.daemon_health().await;
    let validation = state.effective_validation_summary().await;
    let persisted_runtime = state.persisted_runtime_snapshot().await;
    let pool_activity = pool_activity_health(SystemTime::now(), persisted_runtime.as_ref());
    let job = effective_job_health(&state, persisted_runtime.as_ref());

    let store = Arc::clone(&state.store);
    let payout_health =
        match tokio::task::spawn_blocking(move || -> anyhow::Result<PayoutHealth> {
            let balances = store.get_all_balances()?;
            let unpaid_count = balances
                .iter()
                .filter(|balance| balance.pending > 0)
                .count();
            let unpaid_amount = balances
                .iter()
                .fold(0u64, |acc, balance| acc.saturating_add(balance.pending));
            let pending = store.get_pending_payouts()?;
            let queued_count = pending.len();
            let queued_amount = pending
                .iter()
                .fold(0u64, |acc, payout| acc.saturating_add(payout.amount));
            let last_payout = store.get_recent_payouts(1)?.into_iter().next();
            Ok(PayoutHealth {
                unpaid_count,
                unpaid_amount,
                queued_count,
                queued_amount,
                pending_count: queued_count,
                pending_amount: queued_amount,
                last_payout,
            })
        })
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => {
                return internal_error("failed loading payout health", err).into_response();
            }
            Err(err) => {
                return internal_error(
                    "failed loading payout health",
                    anyhow::anyhow!("join error: {err}"),
                )
                .into_response();
            }
        };

    let node = Arc::clone(&state.node);
    let wallet = match tokio::task::spawn_blocking(move || node.get_wallet_balance()).await {
        Ok(Ok(v)) => Some(WalletHealth {
            spendable: v.spendable,
            pending: v.pending,
            pending_unconfirmed: v.pending_unconfirmed,
            pending_unconfirmed_eta: v.pending_unconfirmed_eta,
            total: v.total,
        }),
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "failed loading wallet balance for health");
            None
        }
        Err(err) => {
            tracing::warn!(error = %err, "failed joining wallet balance task for health");
            None
        }
    };

    let store = Arc::clone(&state.store);
    let active_verification_holds = match tokio::task::spawn_blocking(move || {
        store.list_active_verification_holds(provisional_cutoff)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => {
            return internal_error("failed loading active verification holds", err).into_response();
        }
        Err(err) => {
            return internal_error(
                "failed loading active verification holds",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response();
        }
    };

    let response = HealthResponse {
        uptime_seconds: state.started_at.elapsed().as_secs(),
        api_key_configured: !state.api_key.trim().is_empty(),
        daemon,
        job,
        payouts: payout_health,
        wallet,
        validation,
        pool_activity,
        active_verification_holds,
    };

    Json(response).into_response()
}

async fn handle_admin_share_diagnostics(State(state): State<ApiState>) -> impl IntoResponse {
    match state.admin_share_diagnostics().await {
        Ok(response) => Json(response).into_response(),
        Err(err) => internal_error("failed loading admin share diagnostics", err).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct RecoveryCutoverRequest {
    target: RecoveryInstanceId,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AdminReconciliationPayoutResolutionAction {
    RestorePending,
    DropPaid,
}

impl AdminReconciliationPayoutResolutionAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::RestorePending => "restore_pending",
            Self::DropPaid => "drop_paid",
        }
    }

    fn into_store_kind(self) -> ManualCompletedPayoutResolutionKind {
        match self {
            Self::RestorePending => ManualCompletedPayoutResolutionKind::RestorePending,
            Self::DropPaid => ManualCompletedPayoutResolutionKind::DropPaid,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AdminReconciliationPayoutResolutionRequest {
    tx_hash: String,
    action: AdminReconciliationPayoutResolutionAction,
}

#[derive(Debug, Deserialize)]
struct AdminReconciliationPayoutImportRequest {
    tx_hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AdminOrphanedBlockCleanupRequest {
    block_height: u64,
}

#[derive(Debug, Deserialize)]
struct ClearAddressRiskHistoryRequest {
    address: String,
}

#[derive(Debug, Deserialize)]
struct WalletSendIdempotencyJournal {
    #[serde(default)]
    entries: HashMap<String, WalletSendIdempotencyEntry>,
}

#[derive(Debug, Deserialize)]
struct WalletSendIdempotencyEntry {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body_base64: String,
    #[serde(default)]
    created_at_unix_nano: i64,
}

#[derive(Debug, Deserialize)]
struct WalletSendIdempotencyBody {
    #[serde(default)]
    txid: String,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    fee: u64,
    #[serde(default)]
    recipients: Vec<WalletSendIdempotencyRecipient>,
}

#[derive(Debug, Clone, Deserialize)]
struct WalletSendIdempotencyRecipient {
    address: String,
    #[serde(default)]
    amount: u64,
}

#[derive(Debug, Serialize)]
struct ClearAddressRiskHistoryResponse {
    ok: bool,
    address: String,
}

async fn handle_recovery_status(State(state): State<ApiState>) -> impl IntoResponse {
    if !state.config.recovery.enabled {
        return Json(RecoveryStatus::disabled(&state.config.payout_pause_file)).into_response();
    }
    match state.recovery.status().await {
        Ok(status) => Json(status).into_response(),
        Err(err) => internal_error("failed loading recovery status", err).into_response(),
    }
}

async fn handle_recovery_pause_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    match recovery_operation_response(&state, state.recovery.pause_payouts().await).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn handle_recovery_resume_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    match recovery_operation_response(&state, state.recovery.resume_payouts().await).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn handle_recovery_start_inactive_sync(State(state): State<ApiState>) -> impl IntoResponse {
    match recovery_operation_response(&state, state.recovery.start_inactive_sync().await).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn handle_recovery_rebuild_inactive_wallet(
    State(state): State<ApiState>,
) -> impl IntoResponse {
    match recovery_operation_response(&state, state.recovery.rebuild_inactive_wallet().await).await
    {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn handle_recovery_cutover(
    State(state): State<ApiState>,
    Json(request): Json<RecoveryCutoverRequest>,
) -> impl IntoResponse {
    match recovery_operation_response(&state, state.recovery.cutover(request.target).await).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn handle_recovery_purge_inactive_daemon(State(state): State<ApiState>) -> impl IntoResponse {
    match recovery_operation_response(&state, state.recovery.purge_inactive_daemon().await).await {
        Ok(response) => response,
        Err(response) => response,
    }
}

async fn handle_admin_clear_address_risk_history(
    State(state): State<ApiState>,
    Json(request): Json<ClearAddressRiskHistoryRequest>,
) -> impl IntoResponse {
    let address = request.address.trim();
    if address.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"address is required"})),
        )
            .into_response();
    }

    let address = address.to_string();
    let store = Arc::clone(&state.store);
    match tokio::task::spawn_blocking({
        let address = address.clone();
        move || store.clear_address_risk_history(&address)
    })
    .await
    {
        Ok(Ok(())) => {
            if let Some(validation) = &state.validation {
                validation.clear_address_state(&address);
            }
            Json(ClearAddressRiskHistoryResponse { ok: true, address }).into_response()
        }
        Ok(Err(err)) => internal_error("failed clearing address risk history", err).into_response(),
        Err(err) => internal_error(
            "failed clearing address risk history",
            anyhow::anyhow!("join error: {err}"),
        )
        .into_response(),
    }
}

async fn recovery_operation_response(
    state: &ApiState,
    result: anyhow::Result<RecoveryOperation>,
) -> std::result::Result<Response, Response> {
    if !state.config.recovery.enabled {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"recovery controls are disabled"})),
        )
            .into_response());
    }
    match result {
        Ok(operation) => Ok(Json(operation).into_response()),
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
                Err(internal_error("failed starting recovery operation", err).into_response())
            } else {
                Err((status, Json(serde_json::json!({"error": message}))).into_response())
            }
        }
    }
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

    let mut commands = Vec::new();
    for unit in daemon_log_units(config) {
        let mut journal_args = vec![
            "-u".to_string(),
            unit,
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
    if let Some(instance) = active_recovery_instance(config) {
        let data_dir = config.recovery.instance(instance).data_dir.trim();
        if !data_dir.is_empty() {
            return PathBuf::from(data_dir).join("send-idempotency.json");
        }
    }
    let data_dir = config.daemon_data_dir.trim();
    if data_dir.is_empty() {
        return PathBuf::from("data").join("send-idempotency.json");
    }
    PathBuf::from(data_dir).join("send-idempotency.json")
}

fn load_confirmed_payout_import_txs(
    path: &StdPath,
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
        let encoded_body = entry.body_base64.trim();
        if encoded_body.is_empty() {
            continue;
        }
        let decoded_body = BASE64_STANDARD.decode(encoded_body).map_err(|err| {
            anyhow::anyhow!(
                "failed decoding wallet send entry from {}: {err}",
                path.display()
            )
        })?;
        let body: WalletSendIdempotencyBody =
            serde_json::from_slice(&decoded_body).map_err(|err| {
                anyhow::anyhow!(
                    "failed parsing wallet send entry from {}: {err}",
                    path.display()
                )
            })?;
        if body.dry_run {
            continue;
        }
        let tx_hash = body.txid.trim();
        if tx_hash.is_empty() || !requested.contains(tx_hash) {
            continue;
        }

        let recipients = aggregate_wallet_send_idempotency_recipients(&body.recipients);
        if recipients.is_empty() {
            return Err(anyhow::anyhow!(
                "wallet send tx {} in {} has no recipients",
                tx_hash,
                path.display()
            ));
        }

        let manifest = ConfirmedPayoutImportTx {
            tx_hash: tx_hash.to_string(),
            timestamp: system_time_from_unix_nanos(entry.created_at_unix_nano),
            recipients: allocate_imported_payout_fees(&recipients, body.fee),
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

fn aggregate_wallet_send_idempotency_recipients(
    recipients: &[WalletSendIdempotencyRecipient],
) -> Vec<(String, u64)> {
    let mut by_address = HashMap::<String, u64>::new();
    for recipient in recipients {
        let address = recipient.address.trim();
        if address.is_empty() || recipient.amount == 0 {
            continue;
        }
        by_address
            .entry(address.to_string())
            .and_modify(|amount| *amount = amount.saturating_add(recipient.amount))
            .or_insert(recipient.amount);
    }
    let mut aggregated = by_address.into_iter().collect::<Vec<_>>();
    aggregated.sort_by(|a, b| a.0.cmp(&b.0));
    aggregated
}

fn allocate_imported_payout_fees(
    recipients: &[(String, u64)],
    total_fee: u64,
) -> Vec<ConfirmedPayoutImportRecipient> {
    let total_amount = recipients
        .iter()
        .fold(0u64, |acc, (_, amount)| acc.saturating_add(*amount))
        .max(1);
    let mut remaining_fee = total_fee;
    recipients
        .iter()
        .enumerate()
        .map(|(idx, (address, amount))| {
            let fee = if idx + 1 == recipients.len() {
                remaining_fee
            } else {
                let proportional =
                    ((total_fee as u128) * (*amount as u128) / (total_amount as u128)) as u64;
                proportional.min(remaining_fee)
            };
            remaining_fee = remaining_fee.saturating_sub(fee);
            ConfirmedPayoutImportRecipient {
                address: address.clone(),
                amount: *amount,
                fee,
            }
        })
        .collect()
}

fn system_time_from_unix_nanos(unix_nanos: i64) -> SystemTime {
    if unix_nanos <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_nanos(unix_nanos as u64)
}

fn daemon_debug_log_path(config: &Config) -> PathBuf {
    if let Some(instance) = active_recovery_instance(config) {
        let data_dir = config.recovery.instance(instance).data_dir.trim();
        if !data_dir.is_empty() {
            return PathBuf::from(data_dir).join("debug.log");
        }
    }
    let data_dir = config.daemon_data_dir.trim();
    if data_dir.is_empty() {
        return PathBuf::from("data").join("debug.log");
    }
    PathBuf::from(data_dir).join("debug.log")
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
    if !units.iter().any(|existing| existing == "blocknetd.service") {
        units.push("blocknetd.service".to_string());
    }
    units
}

fn active_recovery_instance(config: &Config) -> Option<RecoveryInstanceId> {
    let proxy_target = detect_recovery_proxy_target(config);
    let cookie_target = detect_recovery_active_cookie_target(config);
    match (proxy_target, cookie_target) {
        (Some(proxy), Some(cookie)) if proxy == cookie => Some(proxy),
        (Some(proxy), None) => Some(proxy),
        (None, Some(cookie)) => Some(cookie),
        _ => None,
    }
}

fn detect_recovery_proxy_target(config: &Config) -> Option<RecoveryInstanceId> {
    if !config.recovery.enabled {
        return None;
    }
    let raw = fs::read_to_string(config.recovery.proxy_include_path.trim()).ok()?;
    let primary_api = config.recovery.primary.api.trim();
    let standby_api = config.recovery.standby.api.trim();
    if !primary_api.is_empty() && raw.contains(primary_api) {
        Some(RecoveryInstanceId::Primary)
    } else if !standby_api.is_empty() && raw.contains(standby_api) {
        Some(RecoveryInstanceId::Standby)
    } else {
        None
    }
}

fn detect_recovery_active_cookie_target(config: &Config) -> Option<RecoveryInstanceId> {
    if !config.recovery.enabled {
        return None;
    }
    let link = StdPath::new(config.recovery.active_cookie_path.trim());
    let target = fs::read_link(link).ok()?;
    if path_matches(
        &target,
        StdPath::new(config.recovery.primary.cookie_path.trim()),
    ) {
        Some(RecoveryInstanceId::Primary)
    } else if path_matches(
        &target,
        StdPath::new(config.recovery.standby.cookie_path.trim()),
    ) {
        Some(RecoveryInstanceId::Standby)
    } else {
        None
    }
}

fn path_matches(actual: &StdPath, expected: &StdPath) -> bool {
    if actual == expected {
        return true;
    }
    match (fs::canonicalize(actual), fs::canonicalize(expected)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
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
                if !send_log_keepalive(tx).await {
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

async fn send_log_keepalive(tx: &mpsc::Sender<Result<Vec<u8>, Infallible>>) -> bool {
    tx.send(Ok(vec![b'\n'])).await.is_ok()
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
    let live_stats = state.stats.all_miner_stats();
    let fallback_hashrates = state.stats.estimate_all_miner_hashrates();
    let store = Arc::clone(&state.store);
    let worker_window_start = SystemTime::now()
        .checked_sub(HASHRATE_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    let (lifetime_counts, worker_counts) = match tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((
            store.miner_lifetime_counts()?,
            store.miner_worker_counts_since(worker_window_start)?,
        ))
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => {
            tracing::warn!(error = %err, "failed loading miner summary counts from db");
            (HashMap::new(), HashMap::new())
        }
        Err(err) => {
            tracing::warn!(error = %err, "failed joining miner summary db task");
            (HashMap::new(), HashMap::new())
        }
    };

    let mut address_set = live_stats.keys().cloned().collect::<HashSet<_>>();
    address_set.extend(lifetime_counts.keys().cloned());
    address_set.extend(worker_counts.keys().cloned());
    let mut addresses = address_set.into_iter().collect::<Vec<_>>();
    addresses.sort();

    if !query.paged.unwrap_or(false) {
        let mut out = HashMap::<String, MinerStats>::with_capacity(addresses.len());
        for address in addresses {
            let mut stats = live_stats
                .get(&address)
                .cloned()
                .unwrap_or_else(|| MinerStats {
                    address: address.clone(),
                    workers: HashSet::new(),
                    shares_accepted: 0,
                    shares_rejected: 0,
                    blocks_found: 0,
                    last_share_at: None,
                });
            if let Some((accepted, rejected, blocks, db_last_share)) = lifetime_counts.get(&address)
            {
                stats.shares_accepted = *accepted;
                stats.shares_rejected = *rejected;
                stats.blocks_found = *blocks;
                stats.last_share_at = db_last_share
                    .map(|ts| {
                        std::time::UNIX_EPOCH + std::time::Duration::from_secs(ts.max(0) as u64)
                    })
                    .or(stats.last_share_at);
            }
            out.insert(address, stats);
        }
        return Json(out).into_response();
    }

    let hashrates = if addresses.len() > MAX_MINER_HASHRATE_DB_LOOKUPS {
        tracing::warn!(
            miner_count = addresses.len(),
            lookup_cap = MAX_MINER_HASHRATE_DB_LOOKUPS,
            "miner hashrate DB lookup skipped for large miner set; using in-memory estimates"
        );
        fallback_hashrates
    } else {
        let store = Arc::clone(&state.store);
        let addresses_for_hashrate = addresses.clone();
        match tokio::task::spawn_blocking(move || {
            let mut hr_map = HashMap::with_capacity(addresses_for_hashrate.len());
            for address in &addresses_for_hashrate {
                hr_map.insert(address.clone(), db_miner_hashrate(&store, address));
            }
            Ok::<_, anyhow::Error>(hr_map)
        })
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "failed loading miner hashrates from db; using in-memory estimates");
                fallback_hashrates
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed joining miner hashrate db task; using in-memory estimates");
                fallback_hashrates
            }
        }
    };
    let mut items = addresses
        .into_iter()
        .map(|address| {
            let live = live_stats.get(&address);
            let mut workers = live
                .map(|stats| stats.workers.iter().cloned().collect::<Vec<String>>())
                .unwrap_or_default();
            workers.sort();
            let worker_count = if workers.is_empty() {
                worker_counts.get(&address).copied().unwrap_or(0)
            } else {
                workers.len()
            };
            let hashrate = hashrates.get(&address).copied().unwrap_or(0.0);
            let (accepted, rejected, blocks, db_last_share) =
                lifetime_counts.get(&address).copied().unwrap_or((
                    live.map(|stats| stats.shares_accepted).unwrap_or(0),
                    live.map(|stats| stats.shares_rejected).unwrap_or(0),
                    live.map(|stats| stats.blocks_found).unwrap_or(0),
                    None,
                ));
            let last_share_at = db_last_share
                .map(|ts| std::time::UNIX_EPOCH + std::time::Duration::from_secs(ts.max(0) as u64))
                .or_else(|| live.and_then(|stats| stats.last_share_at));
            MinerListItem {
                address,
                worker_count,
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
    Query(query): Query<MinerBalanceQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let include_pending_estimate = query.include_pending_estimate.unwrap_or(true);
    match state
        .cached_miner_balance_payload(&address, include_pending_estimate)
        .await
    {
        Ok(payload) => Json(payload).into_response(),
        Err(err) => internal_error("failed loading miner balance", err).into_response(),
    }
}

async fn handle_miner(
    Path(address): Path<String>,
    Query(query): Query<MinerDetailQuery>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let share_limit = share_limit(query.share_limit);
    let include_pending_estimate = query.include_pending_estimate.unwrap_or(false);
    match state
        .cached_miner_detail_payload(&address, share_limit, include_pending_estimate)
        .await
    {
        Ok(payload) => {
            if payload.found {
                Json(payload.body).into_response()
            } else {
                (StatusCode::NOT_FOUND, Json(payload.body)).into_response()
            }
        }
        Err(err) => internal_error("failed loading miner data", err).into_response(),
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
    let node = Arc::clone(&state.node);
    let daemon_chain_height = state.node.chain_height();

    if query.legacy_mode() {
        let mut blocks = match tokio::task::spawn_blocking(move || {
            let mut blocks = store.get_recent_blocks_up_to(100, daemon_chain_height)?;
            flag_chain_mismatched_blocks(node.as_ref(), daemon_chain_height, &mut blocks);
            blocks.sort_by(|a, b| {
                b.timestamp
                    .cmp(&a.timestamp)
                    .then_with(|| b.height.cmp(&a.height))
            });
            Ok::<_, anyhow::Error>(blocks)
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
                .into_response();
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
        Some("height_desc") => "height_desc",
        Some("reward_desc") => "reward_desc",
        Some("reward_asc") => "reward_asc",
        Some("time_desc") => "time_desc",
        Some("time_asc") => "time_asc",
        _ => "time_desc",
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
    let node = Arc::clone(&state.node);
    let (mut blocks, total) = match tokio::task::spawn_blocking(move || {
        let (mut blocks, total) = store.get_blocks_page_up_to(
            daemon_chain_height,
            finder.as_deref(),
            status.as_deref(),
            sort,
            limit as i64,
            offset as i64,
        )?;
        flag_chain_mismatched_blocks(node.as_ref(), daemon_chain_height, &mut blocks);
        Ok::<_, anyhow::Error>((blocks, total))
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
            .into_response();
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
    let started_at = Instant::now();
    let luck_by_hash = match tokio::task::spawn_blocking(move || {
        let hashes = target_hashes.into_iter().collect::<Vec<_>>();
        store.get_luck_rounds_for_hashes(&hashes)
    })
    .await
    {
        Ok(Ok(v)) => {
            record_api_operation_observation(
                &state,
                "luck_details_load",
                started_at.elapsed(),
                false,
            );
            v.into_iter()
                .map(|(hash, round)| (hash, luck_round_response_from_db(round)))
                .collect::<HashMap<_, _>>()
        }
        Ok(Err(err)) => {
            record_api_operation_observation(
                &state,
                "luck_details_load",
                started_at.elapsed(),
                true,
            );
            return internal_error("failed loading block luck details", err).into_response();
        }
        Err(err) => {
            record_api_operation_observation(
                &state,
                "luck_details_load",
                started_at.elapsed(),
                true,
            );
            return internal_error(
                "failed loading block luck details",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response();
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
    confirmed: bool,
    #[serde(skip_serializing)]
    batch_id: Option<String>,
}

/// Group payouts into batches by timestamp proximity (5 min window).
fn batch_payouts(payouts: &[Payout]) -> Vec<PublicPayout> {
    if payouts.is_empty() {
        return Vec::new();
    }
    let batch_window = Duration::from_secs(5 * 60);
    let mut batches: Vec<PublicPayout> = Vec::new();
    for p in payouts {
        let payout_batch_id = p
            .batch_id
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        let merged = batches.last_mut().and_then(|b| {
            let same_batch_id = payout_batch_id
                .zip(b.batch_id.as_deref())
                .is_some_and(|(a, b)| a == b);
            let diff = b
                .timestamp
                .duration_since(p.timestamp)
                .or_else(|_| p.timestamp.duration_since(b.timestamp))
                .unwrap_or(Duration::ZERO);
            if same_batch_id || diff <= batch_window {
                Some(b)
            } else {
                None
            }
        });
        if let Some(batch) = merged {
            batch.total_amount += p.amount;
            batch.total_fee += p.fee;
            batch.recipient_count += 1;
            batch.confirmed &= p.confirmed;
            if batch.batch_id.is_none() {
                batch.batch_id = payout_batch_id.map(str::to_string);
            }
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
                confirmed: p.confirmed,
                batch_id: payout_batch_id.map(str::to_string),
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
    credit_withheld: bool,
    validation_state: String,
    validation_label: String,
    validation_tone: String,
    validation_detail: String,
    confirmations_remaining: u64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone, Serialize, Default)]
struct MinerPendingEstimate {
    estimated_pending: u64,
    blocks: Vec<MinerPendingBlockEstimate>,
}

#[derive(Debug, Clone, Serialize)]
struct MinerVerificationHold {
    mode: String,
    reason: Option<String>,
    started_at: Option<SystemTime>,
    verified_only_until: Option<SystemTime>,
    quarantined_until: Option<SystemTime>,
    active_risk_strikes: u64,
    active_fraud_strikes: u64,
    validation_hold_cause: Option<crate::db::ValidationHoldCause>,
    validation_pending_provisional: Option<u64>,
    validation_recent_verified_difficulty: Option<u64>,
    validation_recent_provisional_difficulty: Option<u64>,
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingPreviewValidation {
    Ready,
    FinderFallback,
    AwaitingDelay,
    AwaitingVerifiedShares,
    AwaitingVerifiedRatio,
    ExtraVerification,
}

#[derive(Debug, Clone)]
struct PreparedPendingEstimateBlock {
    block: DbBlock,
    confirmations: u64,
    previews: Vec<ShareWindowAddressPreview>,
}

#[derive(Debug, Clone)]
struct PendingEstimateDurationWindow {
    index: usize,
    start: SystemTime,
    end: SystemTime,
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

fn duration_based_pending_window(config: &Config) -> Option<Duration> {
    if config.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
        let duration = config.pplns_window_duration_duration();
        return (!duration.is_zero()).then_some(duration);
    }

    Some(PROPORTIONAL_WINDOW)
}

fn update_window_preview_for_share(
    preview: &mut ShareWindowAddressPreview,
    share: &DbShare,
    provisional_ready_cutoff: Option<SystemTime>,
    add: bool,
) {
    let apply = |value: &mut u64, delta: u64| {
        if add {
            *value = value.saturating_add(delta);
        } else {
            *value = value.saturating_sub(delta);
        }
    };

    apply(&mut preview.seen_shares, 1);
    match share.status.as_str() {
        "" | SHARE_STATUS_VERIFIED => {
            apply(&mut preview.verified_shares, 1);
            apply(&mut preview.verified_difficulty, share.difficulty);
        }
        SHARE_STATUS_PROVISIONAL => {
            let ready = provisional_ready_cutoff
                .map(|cutoff| share.created_at <= cutoff)
                .unwrap_or(false);
            if ready {
                apply(&mut preview.provisional_shares_ready, 1);
                apply(&mut preview.provisional_difficulty_ready, share.difficulty);
            } else {
                apply(&mut preview.provisional_shares_delayed, 1);
            }
        }
        _ => {}
    }
}

fn clone_sorted_share_window_previews(
    by_address: &HashMap<String, ShareWindowAddressPreview>,
) -> Vec<ShareWindowAddressPreview> {
    let mut previews = by_address
        .iter()
        .filter_map(|(address, preview)| {
            (preview.seen_shares > 0).then(|| {
                let mut preview = preview.clone();
                preview.address = address.clone();
                preview
            })
        })
        .collect::<Vec<_>>();
    previews.sort_by(|a, b| a.address.cmp(&b.address));
    previews
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
        &blocks.iter().map(|b| b.hash.clone()).collect::<Vec<_>>(),
    )?;
    let mut windows = Vec::<PendingEstimateDurationWindow>::with_capacity(blocks.len());
    let mut min_start: Option<SystemTime> = None;
    let mut max_end: Option<SystemTime> = None;

    for (index, block) in blocks.iter().enumerate() {
        let end = latest_share_timestamps
            .get(&block.hash)
            .copied()
            .map(|share_time| share_time.max(block.timestamp))
            .unwrap_or(block.timestamp);
        let start = end.checked_sub(window_duration).unwrap_or(UNIX_EPOCH);
        min_start = Some(
            min_start
                .map(|existing| existing.min(start))
                .unwrap_or(start),
        );
        max_end = Some(max_end.map(|existing| existing.max(end)).unwrap_or(end));
        windows.push(PendingEstimateDurationWindow { index, start, end });
    }

    let (Some(min_start), Some(max_end)) = (min_start, max_end) else {
        return Ok(Vec::new());
    };
    let mut shares = store.get_shares_between(min_start, max_end)?;
    shares.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    windows.sort_by(|a, b| a.end.cmp(&b.end).then_with(|| a.index.cmp(&b.index)));

    let mut previews_by_block = vec![Vec::<ShareWindowAddressPreview>::new(); blocks.len()];
    let mut active = HashMap::<String, ShareWindowAddressPreview>::new();
    let mut left = 0usize;
    let mut right = 0usize;

    for window in windows {
        while right < shares.len() && shares[right].created_at <= window.end {
            let share = &shares[right];
            let preview =
                active
                    .entry(share.miner.clone())
                    .or_insert_with(|| ShareWindowAddressPreview {
                        address: share.miner.clone(),
                        ..ShareWindowAddressPreview::default()
                    });
            update_window_preview_for_share(preview, share, provisional_ready_cutoff, true);
            right += 1;
        }
        while left < right && shares[left].created_at < window.start {
            let share = &shares[left];
            let mut remove_address = false;
            if let Some(preview) = active.get_mut(&share.miner) {
                update_window_preview_for_share(preview, share, provisional_ready_cutoff, false);
                remove_address = preview.seen_shares == 0;
            }
            if remove_address {
                active.remove(&share.miner);
            }
            left += 1;
        }
        previews_by_block[window.index] = clone_sorted_share_window_previews(&active);
    }

    Ok(blocks
        .into_iter()
        .zip(previews_by_block)
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

    if let Some(window_duration) = duration_based_pending_window(config) {
        return prepare_duration_pending_estimate_blocks(
            store,
            blocks,
            window_duration,
            now.checked_sub(provisional_delay),
            chain_height,
        );
    }

    let mut prepared_blocks = Vec::<PreparedPendingEstimateBlock>::with_capacity(blocks.len());
    for block in blocks {
        let window_end = reward_window_end(store, &block)?;
        let previews =
            pending_estimate_window_preview(store, config, now, provisional_delay, window_end)?;
        prepared_blocks.push(PreparedPendingEstimateBlock {
            confirmations: chain_height.saturating_sub(block.height),
            block,
            previews,
        });
    }
    Ok(prepared_blocks)
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

    if trust_policy.min_verified_ratio > 0.0 {
        let verified_ratio = preview.verified_difficulty as f64 / total_uncapped as f64;
        if verified_ratio < trust_policy.min_verified_ratio {
            return None;
        }
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

fn pending_estimate_window_preview(
    store: &PoolStore,
    config: &Config,
    now: SystemTime,
    provisional_delay: Duration,
    window_end: SystemTime,
) -> anyhow::Result<Vec<ShareWindowAddressPreview>> {
    let provisional_ready_cutoff = now.checked_sub(provisional_delay);
    if config.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
        let duration = config.pplns_window_duration_duration();
        if duration.is_zero() {
            return store.summarize_last_n_shares_before(
                window_end,
                i64::from(config.pplns_window.max(1)),
                provisional_ready_cutoff,
            );
        }
        let since = window_end.checked_sub(duration).unwrap_or(UNIX_EPOCH);
        return store.summarize_shares_between(since, window_end, provisional_ready_cutoff);
    }

    let since = window_end
        .checked_sub(PROPORTIONAL_WINDOW)
        .unwrap_or(UNIX_EPOCH);
    store.summarize_shares_between(since, window_end, provisional_ready_cutoff)
}

fn pending_preview_validation_state(
    stats: &AddressPreviewStats,
    trust_policy: PayoutTrustPolicy,
    finder_fallback: bool,
) -> PendingPreviewValidation {
    if finder_fallback {
        return PendingPreviewValidation::FinderFallback;
    }
    if stats.risky && stats.has_window_activity() {
        return PendingPreviewValidation::ExtraVerification;
    }
    if !stats.has_eligible_work() && stats.provisional_shares_delayed > 0 {
        return PendingPreviewValidation::AwaitingDelay;
    }
    if stats.verified_shares < trust_policy.min_verified_shares {
        return PendingPreviewValidation::AwaitingVerifiedShares;
    }
    if trust_policy.min_verified_ratio > 0.0
        && stats.has_eligible_work()
        && stats.verified_ratio() < trust_policy.min_verified_ratio
    {
        return PendingPreviewValidation::AwaitingVerifiedRatio;
    }
    PendingPreviewValidation::Ready
}

fn pending_preview_validation_label(state: PendingPreviewValidation) -> &'static str {
    match state {
        PendingPreviewValidation::Ready => "Ready",
        PendingPreviewValidation::FinderFallback => "Finder fallback",
        PendingPreviewValidation::AwaitingDelay => "Waiting for delay",
        PendingPreviewValidation::AwaitingVerifiedShares => "Waiting for shares",
        PendingPreviewValidation::AwaitingVerifiedRatio => "Waiting for ratio",
        PendingPreviewValidation::ExtraVerification => "Verified only",
    }
}

fn pending_preview_validation_tone(state: PendingPreviewValidation) -> &'static str {
    match state {
        PendingPreviewValidation::Ready | PendingPreviewValidation::FinderFallback => "ok",
        PendingPreviewValidation::AwaitingDelay
        | PendingPreviewValidation::AwaitingVerifiedShares
        | PendingPreviewValidation::AwaitingVerifiedRatio => "warn",
        PendingPreviewValidation::ExtraVerification => "warn",
    }
}

fn pending_preview_validation_state_key(state: PendingPreviewValidation) -> &'static str {
    match state {
        PendingPreviewValidation::Ready => "ready",
        PendingPreviewValidation::FinderFallback => "finder_fallback",
        PendingPreviewValidation::AwaitingDelay => "awaiting_delay",
        PendingPreviewValidation::AwaitingVerifiedShares => "awaiting_shares",
        PendingPreviewValidation::AwaitingVerifiedRatio => "awaiting_ratio",
        PendingPreviewValidation::ExtraVerification => "extra_verification",
    }
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
        PendingPreviewValidation::FinderFallback => {
            "No share window was recorded for this block, so the finder gets the fallback credit."
                .to_string()
        }
        PendingPreviewValidation::AwaitingDelay => format!(
            "Shares are still inside the {} provisional delay, so the preview has not opened yet.",
            cfg.provisional_share_delay.trim(),
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
        PendingPreviewValidation::AwaitingVerifiedRatio => format!(
            "{:.1}% of this window is verified so far; payouts require {:.1}%.",
            stats.verified_ratio() * 100.0,
            trust_policy.min_verified_ratio * 100.0,
        ),
        PendingPreviewValidation::ExtraVerification => {
            "This address is under a verification hold, so only fully verified shares count toward this estimate and payout right now."
                .to_string()
        }
    }
}

fn active_window_strikes(strikes: u64, window_until: Option<SystemTime>, now: SystemTime) -> u64 {
    window_until
        .filter(|until| *until > now)
        .map(|_| strikes)
        .unwrap_or_default()
}

fn pool_activity_health(
    now: SystemTime,
    persisted_runtime: Option<&PersistedRuntimeSnapshot>,
) -> PoolActivityHealth {
    let assessment =
        assess_pool_activity(now, persisted_runtime, POOL_ACTIVITY_SNAPSHOT_STALE_AFTER);
    PoolActivityHealth {
        state: assessment.state.to_string(),
        detail: assessment.detail,
        connected_miners: assessment.connected_miners,
        connected_workers: assessment.connected_workers,
        estimated_hashrate: assessment.estimated_hashrate,
        snapshot_age_seconds: assessment.snapshot_age_seconds,
        last_share_age_seconds: assessment.last_share_age_seconds,
    }
}

fn validation_hold_reason(
    cause: Option<crate::db::ValidationHoldCause>,
    pending_provisional: u64,
    recent_verified_difficulty: u64,
    recent_provisional_difficulty: u64,
) -> Option<String> {
    match cause {
        Some(crate::db::ValidationHoldCause::InvalidSamples) => {
            Some("recent invalid sampled shares are under review".to_string())
        }
        Some(crate::db::ValidationHoldCause::ProvisionalBacklog) => Some(
            if recent_provisional_difficulty > 0 || recent_verified_difficulty > 0 {
                match recent_verified_difficulty {
                    0 => format!(
                        "recent provisional diff {} has no recent verified diff yet",
                        recent_provisional_difficulty
                    ),
                    verified => format!(
                        "recent provisional diff {} vs {} verified",
                        recent_provisional_difficulty, verified
                    ),
                }
            } else if pending_provisional > 0 {
                format!(
                    "{pending_provisional} provisional share{} waiting for full verification",
                    if pending_provisional == 1 { "" } else { "s" }
                )
            } else {
                "recent provisional backlog is draining".to_string()
            },
        ),
        Some(crate::db::ValidationHoldCause::PayoutCoverage) => {
            Some("boosting verified-share coverage so payout weight stays proportional".to_string())
        }
        None => None,
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
    validation_state: Option<&crate::db::ValidationHoldState>,
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
                validation_hold_reason(
                    validation_hold_cause,
                    validation_pending_provisional.unwrap_or_default(),
                    validation_recent_verified_difficulty.unwrap_or_default(),
                    validation_recent_provisional_difficulty.unwrap_or_default(),
                )
            }),
        started_at: state
            .and_then(|s| s.last_event_at)
            .or_else(|| validation_state.and_then(|state| state.forced_started_at)),
        verified_only_until,
        quarantined_until,
        active_risk_strikes: state
            .map(|s| active_window_strikes(s.strikes, s.strike_window_until, now))
            .unwrap_or_default(),
        active_fraud_strikes: state
            .map(|s| {
                active_window_strikes(
                    s.suspected_fraud_strikes,
                    s.suspected_fraud_window_until,
                    now,
                )
            })
            .unwrap_or_default(),
        validation_hold_cause,
        validation_pending_provisional,
        validation_recent_verified_difficulty,
        validation_recent_provisional_difficulty,
    })
}

fn estimate_unconfirmed_pending_snapshot(
    store: &PoolStore,
    config: &Config,
    now: SystemTime,
    chain_height: u64,
) -> anyhow::Result<HashMap<String, MinerPendingEstimate>> {
    let provisional_delay = config.provisional_share_delay_duration();
    let required_confirmations = config.blocks_before_payout.max(0) as u64;
    let trust_policy = PayoutTrustPolicy {
        min_verified_shares: config.payout_min_verified_shares.max(0) as u64,
        min_verified_ratio: config.payout_min_verified_ratio.clamp(0.0, 1.0),
        provisional_cap_multiplier: config.payout_provisional_cap_multiplier.max(0.0),
    };
    let preview_trust_policy = PayoutTrustPolicy {
        min_verified_shares: 0,
        min_verified_ratio: 0.0,
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
        let mut distributable = block.reward.saturating_sub(config.pool_fee(block.reward));
        if previews.is_empty() {
            let estimated_credit = distributable;
            let validation_state = PendingPreviewValidation::FinderFallback;
            let estimate = estimates.entry(block.finder.clone()).or_default();
            estimate.estimated_pending =
                estimate.estimated_pending.saturating_add(estimated_credit);
            estimate.blocks.push(MinerPendingBlockEstimate {
                height: block.height,
                hash: block.hash.clone(),
                reward: block.reward,
                estimated_credit,
                credit_withheld: false,
                validation_state: pending_preview_validation_state_key(validation_state)
                    .to_string(),
                validation_label: pending_preview_validation_label(validation_state).to_string(),
                validation_tone: pending_preview_validation_tone(validation_state).to_string(),
                validation_detail: pending_preview_validation_detail(
                    config,
                    &AddressPreviewStats::default(),
                    trust_policy,
                    validation_state,
                ),
                confirmations_remaining: required_confirmations.saturating_sub(confirmations),
                timestamp: block.timestamp,
            });
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
        if config.block_finder_bonus && config.block_finder_bonus_pct > 0.0 {
            let bonus = (distributable as f64 * config.block_finder_bonus_pct / 100.0) as u64;
            credit_address(&mut estimated_credits, &block.finder, bonus)?;
            distributable = distributable.saturating_sub(bonus);
        }
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
            let validation_state = pending_preview_validation_state(&stats, trust_policy, false);
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
                reward: block.reward,
                estimated_credit,
                credit_withheld: false,
                validation_state: pending_preview_validation_state_key(validation_state)
                    .to_string(),
                validation_label: pending_preview_validation_label(validation_state).to_string(),
                validation_tone: pending_preview_validation_tone(validation_state).to_string(),
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
        estimate.blocks.sort_by(|a, b| b.height.cmp(&a.height));
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
        match share.status.as_str() {
            "" | SHARE_STATUS_VERIFIED => {
                entry.verified_shares = entry.verified_shares.saturating_add(1);
                entry.verified_difficulty =
                    entry.verified_difficulty.saturating_add(share.difficulty);
            }
            SHARE_STATUS_PROVISIONAL => {
                if is_share_payout_eligible(share, now, provisional_delay) {
                    entry.provisional_shares_eligible =
                        entry.provisional_shares_eligible.saturating_add(1);
                    entry.provisional_difficulty_eligible = entry
                        .provisional_difficulty_eligible
                        .saturating_add(share.difficulty);
                } else {
                    entry.provisional_shares_ineligible =
                        entry.provisional_shares_ineligible.saturating_add(1);
                    entry.provisional_difficulty_ineligible = entry
                        .provisional_difficulty_ineligible
                        .saturating_add(share.difficulty);
                }
            }
            _ => {}
        }
    }
    by_address
}

fn equivalent_verified_ratio_for_provisional_cap(multiplier: f64) -> Option<f64> {
    if multiplier <= 0.0 {
        None
    } else {
        Some(1.0 / (1.0 + multiplier.max(0.0)))
    }
}

fn provisional_difficulty_cap(verified_difficulty: u64, multiplier: f64) -> Option<u64> {
    if multiplier <= 0.0 || verified_difficulty == 0 {
        None
    } else {
        Some(
            ((verified_difficulty as f64) * multiplier.max(0.0)).clamp(0.0, u64::MAX as f64) as u64,
        )
    }
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
    if trust_policy.min_verified_ratio > 0.0 {
        let verified_ratio = stats.verified_difficulty as f64 / eligible as f64;
        if verified_ratio < trust_policy.min_verified_ratio {
            return RewardParticipantStatus::AwaitingVerifiedRatio;
        }
    }
    if let Some(provisional_cap) = provisional_difficulty_cap(
        stats.verified_difficulty,
        trust_policy.provisional_cap_multiplier,
    ) {
        if provisional_difficulty_eligible > provisional_cap {
            return RewardParticipantStatus::CappedProvisional;
        }
    }
    RewardParticipantStatus::Included
}

fn compute_reward_mode(
    shares: &[DbShare],
    block: &DbBlock,
    distributable_reward: u64,
    trust_policy: PayoutTrustPolicy,
    risky_by_address: &HashMap<String, bool>,
    now: SystemTime,
    provisional_delay: Duration,
    block_finder_bonus: bool,
    block_finder_bonus_pct: f64,
    stats_by_address: &HashMap<String, RewardWindowAddressStats>,
) -> anyhow::Result<RewardModeComputation> {
    let (weights, total_weight) =
        weight_shares(shares, now, provisional_delay, trust_policy, |address| {
            Ok(risky_by_address.get(address).copied().unwrap_or(false))
        })?;

    let mut statuses = HashMap::<String, RewardParticipantStatus>::new();
    for (address, stats) in stats_by_address {
        statuses.insert(
            address.clone(),
            reward_participant_status(
                Some(stats),
                trust_policy,
                risky_by_address.get(address).copied().unwrap_or(false),
            ),
        );
    }

    let mut credits = HashMap::<String, u64>::new();
    if shares.is_empty() {
        credit_address(&mut credits, &block.finder, distributable_reward)?;
        statuses.insert(
            block.finder.clone(),
            RewardParticipantStatus::FinderFallback,
        );
        return Ok(RewardModeComputation {
            weights,
            credits,
            statuses,
            total_weight,
        });
    }
    if total_weight == 0 {
        return Ok(RewardModeComputation {
            weights,
            credits,
            statuses,
            total_weight,
        });
    }

    let mut weighted_amount = distributable_reward;
    if block_finder_bonus && block_finder_bonus_pct > 0.0 {
        let bonus = (distributable_reward as f64 * block_finder_bonus_pct / 100.0) as u64;
        credit_address(&mut credits, &block.finder, bonus)?;
        weighted_amount = weighted_amount.saturating_sub(bonus);
    }
    allocate_weighted_credits(&mut credits, weights.clone(), total_weight, weighted_amount)?;

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
) -> anyhow::Result<(Vec<DbShare>, RewardWindowSummary)> {
    let end = reward_window_end(store, block)?;
    if config.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
        let duration = config.pplns_window_duration_duration();
        if duration.is_zero() {
            let shares =
                store.get_last_n_shares_before(end, i64::from(config.pplns_window.max(1)))?;
            let start = shares.iter().map(|share| share.created_at).min();
            return Ok((
                shares.clone(),
                RewardWindowSummary {
                    label: format!("PPLNS · last {} shares", config.pplns_window.max(1)),
                    start,
                    end,
                    share_count: shares.len(),
                    participant_count: 0,
                },
            ));
        }

        let start = end.checked_sub(duration).unwrap_or(UNIX_EPOCH);
        let shares = store.get_shares_between(start, end)?;
        return Ok((
            shares.clone(),
            RewardWindowSummary {
                label: format!("PPLNS · {}", config.pplns_window_duration.trim()),
                start: Some(start),
                end,
                share_count: shares.len(),
                participant_count: 0,
            },
        ));
    }

    let start = end.checked_sub(PROPORTIONAL_WINDOW).unwrap_or(UNIX_EPOCH);
    let shares = store.get_shares_between(start, end)?;
    Ok((
        shares.clone(),
        RewardWindowSummary {
            label: "Proportional · 1h".to_string(),
            start: Some(start),
            end,
            share_count: shares.len(),
            participant_count: 0,
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

    let fee_amount = config.pool_fee(block.reward);
    let distributable_reward = block.reward.saturating_sub(fee_amount);
    let provisional_delay = config.provisional_share_delay_duration();

    let (shares, mut share_window) = load_block_reward_window(store, config, &block)?;
    let recorded_stats_by_address = collect_reward_window_stats(&shares, now, provisional_delay);
    share_window.participant_count = recorded_stats_by_address.len();

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
        min_verified_ratio: 0.0,
        provisional_cap_multiplier: 0.0,
    };
    let payout_trust_policy = PayoutTrustPolicy::from_values(
        config.payout_min_verified_shares,
        config.payout_min_verified_ratio,
        config.payout_provisional_cap_multiplier,
    );
    let mut display_shares = shares.clone();
    let mut display_stats_by_address = recorded_stats_by_address.clone();

    let mut preview_mode = compute_reward_mode(
        &display_shares,
        &block,
        distributable_reward,
        preview_trust_policy,
        &risky_by_address,
        now,
        provisional_delay,
        config.block_finder_bonus,
        config.block_finder_bonus_pct,
        &display_stats_by_address,
    )?;

    let mut payout_mode = compute_reward_mode(
        &display_shares,
        &block,
        distributable_reward,
        payout_trust_policy,
        &risky_by_address,
        now,
        provisional_delay,
        config.block_finder_bonus,
        config.block_finder_bonus_pct,
        &display_stats_by_address,
    )?;
    if !display_shares.is_empty() && payout_mode.total_weight == 0 {
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
                    &display_shares,
                    &block,
                    distributable_reward,
                    preview_trust_policy,
                    &risky_by_address,
                    now,
                    provisional_delay,
                    config.block_finder_bonus,
                    config.block_finder_bonus_pct,
                    &display_stats_by_address,
                )?;
                payout_mode = compute_reward_mode(
                    &display_shares,
                    &block,
                    distributable_reward,
                    payout_trust_policy,
                    &risky_by_address,
                    now,
                    provisional_delay,
                    config.block_finder_bonus,
                    config.block_finder_bonus_pct,
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
                provisional_shares_ineligible: stats
                    .map(|entry| entry.provisional_shares_ineligible)
                    .unwrap_or(0),
                provisional_difficulty_ineligible: stats
                    .map(|entry| entry.provisional_difficulty_ineligible)
                    .unwrap_or(0),
                preview_weight: preview_mode.weights.get(&address).copied().unwrap_or(0),
                preview_share_pct: if preview_mode.total_weight == 0 {
                    0.0
                } else {
                    preview_mode.weights.get(&address).copied().unwrap_or(0) as f64 * 100.0
                        / preview_mode.total_weight as f64
                },
                preview_credit: preview_mode.credits.get(&address).copied().unwrap_or(0),
                preview_status: preview_status.as_str().to_string(),
                payout_weight: payout_mode.weights.get(&address).copied().unwrap_or(0),
                payout_share_pct: if payout_mode.total_weight == 0 {
                    0.0
                } else {
                    payout_mode.weights.get(&address).copied().unwrap_or(0) as f64 * 100.0
                        / payout_mode.total_weight as f64
                },
                payout_credit,
                payout_status: payout_status.as_str().to_string(),
                actual_credit,
                delta_vs_payout: actual_credit.map(|actual| actual as i64 - payout_credit as i64),
                address,
            }
        })
        .collect::<Vec<_>>();

    Ok(BlockRewardBreakdownResponse {
        block,
        payout_scheme: config.payout_scheme.clone(),
        share_window,
        fee_amount,
        distributable_reward,
        preview_total_weight: preview_mode.total_weight,
        payout_total_weight: payout_mode.total_weight,
        actual_credit_events_available: !actual_events.is_empty(),
        actual_credit_total: actual_events
            .iter()
            .fold(0u64, |sum, event| sum.saturating_add(event.amount)),
        actual_fee_amount,
        participants: participant_rows,
    })
}

fn payout_window_description(cfg: &Config) -> String {
    if cfg.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
        let duration = cfg.pplns_window_duration.trim();
        if !duration.is_empty() && duration != "0s" {
            return format!("the {} before each found block", duration);
        }
        return format!(
            "the last {} shares before each found block",
            cfg.pplns_window.max(1)
        );
    }
    "the 1h round before each found block".to_string()
}

fn payout_weighting_policy_description(cfg: &Config) -> String {
    let min_verified_shares = cfg.payout_min_verified_shares.max(0) as u64;
    let verified_shares = if min_verified_shares == 0 {
        "no minimum verified shares".to_string()
    } else {
        format!(
            "at least {} verified share{}",
            min_verified_shares,
            if min_verified_shares == 1 { "" } else { "s" }
        )
    };
    let min_verified_ratio = cfg.payout_min_verified_ratio.clamp(0.0, 1.0);
    let provisional_cap_multiplier = cfg.payout_provisional_cap_multiplier.max(0.0);

    match (
        min_verified_ratio > 0.0,
        equivalent_verified_ratio_for_provisional_cap(provisional_cap_multiplier),
    ) {
        (true, Some(full_credit_ratio)) => format!(
            "{} and {:.0}% verified difficulty in each payout window, while provisional difficulty is capped at {}x verified difficulty (full weight once about {}% of that window is verified)",
            verified_shares,
            min_verified_ratio * 100.0,
            format_decimal(provisional_cap_multiplier),
            format_decimal(full_credit_ratio * 100.0)
        ),
        (true, None) => format!(
            "{} and {:.0}% verified difficulty in each payout window",
            verified_shares,
            min_verified_ratio * 100.0
        ),
        (false, Some(full_credit_ratio)) => format!(
            "{}, then caps provisional difficulty at {}x verified difficulty (full weight once about {}% of that window is verified)",
            verified_shares,
            format_decimal(provisional_cap_multiplier),
            format_decimal(full_credit_ratio * 100.0)
        ),
        (false, None) => verified_shares,
    }
}

fn pending_balance_note(
    cfg: &Config,
    hashrate: f64,
    total_accepted: u64,
    estimate: &MinerPendingEstimate,
) -> Option<String> {
    if hashrate <= 0.0 && total_accepted == 0 {
        return None;
    }
    if !estimate.blocks.is_empty() {
        if estimate
            .blocks
            .iter()
            .any(|block| block.validation_state == "extra_verification")
        {
            return Some(
                "This address is under a verification hold, so only fully verified shares count toward unconfirmed estimates and payout right now. Confirmed balance and completed payouts are unaffected."
                    .to_string(),
            );
        }

        if estimate.blocks.iter().any(|block| {
            matches!(
                block.validation_state.as_str(),
                "awaiting_delay" | "awaiting_shares" | "awaiting_ratio"
            )
        }) {
            return Some(format!(
                "Unconfirmed preview is separate from confirmed balance. It starts after shares clear the {} provisional delay, and final payout weighting uses {}.",
                cfg.provisional_share_delay.trim(),
                payout_weighting_policy_description(cfg),
            ));
        }
        return Some(format!(
            "Unconfirmed preview is separate from confirmed balance. This pool credits shares from {}, so hashrate submitted after a block does not change that block's estimate. These amounts can still move until each block reaches {} confirmations or is orphaned.",
            payout_window_description(cfg),
            cfg.blocks_before_payout.max(0),
        ));
    }
    Some(format!(
        "Pending stays at 0 until the pool finds and credits blocks. Your accepted shares still count toward future block rewards while they remain in {}.",
        payout_window_description(cfg),
    ))
}

fn payout_status_note(
    cfg: &Config,
    pending_confirmed: u64,
    pending_payout: Option<&PendingPayout>,
) -> Option<String> {
    if let Some(queued) = pending_payout {
        let remaining = pending_confirmed.saturating_sub(queued.amount);
        if remaining > 0 {
            return Some(format!(
                "{} is already queued for payout. Another {} of confirmed balance is still waiting behind that queued send. The payout processor runs about every {} and keeps retrying queued payouts until they clear.",
                format_atomic_bnt(queued.amount),
                format_atomic_bnt(remaining),
                cfg.payout_interval.trim(),
            ));
        }
        return Some(format!(
            "{} is already queued for payout. The payout processor runs about every {} and keeps retrying queued payouts until they clear.",
            format_atomic_bnt(queued.amount),
            cfg.payout_interval.trim(),
        ));
    }

    if pending_confirmed == 0 {
        return None;
    }

    let min_payout_atomic = (cfg.min_payout_amount.max(0.0) * 100_000_000.0).round() as u64;
    if min_payout_atomic > 0 && pending_confirmed < min_payout_atomic {
        return Some(format!(
            "{} of confirmed balance is below the {} minimum payout. It stays in confirmed pending until more rewards arrive.",
            format_atomic_bnt(pending_confirmed),
            format_bnt(cfg.min_payout_amount),
        ));
    }

    Some(format!(
        "{} of confirmed balance clears the {} minimum payout and will be picked up on the next payout sweep (configured every {}).",
        format_atomic_bnt(pending_confirmed),
        format_bnt(cfg.min_payout_amount),
        cfg.payout_interval.trim(),
    ))
}

fn miner_balance_response(
    balance: &Balance,
    pending_payout: Option<&PendingPayout>,
) -> MinerBalanceResponse {
    let pending_confirmed = balance.pending;
    let pending_queued = pending_payout.map(|queued| queued.amount).unwrap_or(0);
    MinerBalanceResponse {
        pending: pending_confirmed,
        pending_confirmed,
        pending_queued,
        pending_unqueued: pending_confirmed.saturating_sub(pending_queued),
        paid: balance.paid,
    }
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

    let balances = store.get_all_balances()?;
    let unpaid_count = balances
        .iter()
        .filter(|balance| balance.pending > 0)
        .count();
    let unpaid_amount = balances
        .iter()
        .fold(0u64, |acc, balance| acc.saturating_add(balance.pending));

    Ok(PayoutEtaResponse {
        last_payout_at,
        estimated_next_payout_at,
        eta_seconds,
        typical_interval_seconds,
        configured_interval_seconds: None,
        next_sweep_at: None,
        next_sweep_in_seconds: None,
        pending_count: pending.len(),
        pending_total_amount,
        unpaid_count,
        unpaid_amount,
        wallet_spendable: None,
        wallet_pending: None,
        queue_shortfall_amount: 0,
        liquidity_constrained: false,
        reserve_target_amount: None,
        safe_spend_budget: None,
        spendable_output_count: None,
        small_output_count: None,
        medium_output_count: None,
        large_output_count: None,
        planned_batch_count: None,
        planned_recipient_count: None,
        rebalance_required: false,
        rebalance_active: false,
        inventory_health: None,
    })
}

fn apply_runtime_schedule_to_payout_eta(
    payout_eta: &mut PayoutEtaResponse,
    payout_runtime: Option<&PersistedPayoutRuntime>,
) {
    let Some(runtime) = payout_runtime else {
        return;
    };
    if runtime.payout_interval_seconds > 0 {
        payout_eta.configured_interval_seconds = Some(runtime.payout_interval_seconds);
    }
    payout_eta.next_sweep_at = runtime.next_sweep_at;
    payout_eta.next_sweep_in_seconds = runtime.next_sweep_at.and_then(|next| {
        next.duration_since(SystemTime::now())
            .ok()
            .map(|duration| duration.as_secs())
    });
    payout_eta.reserve_target_amount =
        (runtime.reserve_target_amount > 0).then_some(runtime.reserve_target_amount);
    payout_eta.safe_spend_budget = Some(runtime.safe_spend_budget);
    payout_eta.spendable_output_count = Some(runtime.spendable_output_count);
    payout_eta.small_output_count = Some(runtime.small_output_count);
    payout_eta.medium_output_count = Some(runtime.medium_output_count);
    payout_eta.large_output_count = Some(runtime.large_output_count);
    payout_eta.planned_batch_count = Some(runtime.planned_batch_count);
    payout_eta.planned_recipient_count = Some(runtime.planned_recipient_count);
    payout_eta.rebalance_required = runtime.rebalance_required;
    payout_eta.rebalance_active = runtime.rebalance_active;
    payout_eta.inventory_health =
        (!runtime.inventory_health.trim().is_empty()).then(|| runtime.inventory_health.clone());
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
    payout_eta.queue_shortfall_amount = payout_eta
        .pending_total_amount
        .saturating_sub(wallet_balance.spendable);
    payout_eta.liquidity_constrained =
        payout_eta.pending_total_amount > 0 && payout_eta.queue_shortfall_amount > 0;
}

fn backfill_block_effort(store: &PoolStore) -> anyhow::Result<()> {
    let blocks = store.get_all_blocks()?;
    if blocks.len() < 2 {
        return Ok(());
    }
    let needs_backfill = blocks.iter().any(|b| b.effort_pct.is_none());
    if !needs_backfill {
        return Ok(());
    }
    let rounds = compute_luck_history(store, blocks, None)?;
    let mut updated = 0u64;
    for round in &rounds {
        if let Some(mut block) = store.get_block(round.block_height)? {
            if block.effort_pct.is_none() {
                block.effort_pct = Some(round.effort_pct);
                store.add_block(&block)?;
                updated += 1;
            }
        }
    }
    if updated > 0 {
        tracing::info!(updated, "backfilled block effort_pct");
    }
    Ok(())
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

        if current.effort_pct.is_none() {
            let mut updated = current.clone();
            updated.effort_pct = Some(effort_pct);
            let _ = store.add_block(&updated);
        }

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

    rounds.sort_by(|a, b| {
        b.block_height
            .cmp(&a.block_height)
            .then_with(|| b.timestamp.cmp(&a.timestamp))
    });
    if let Some(max_items) = max_items {
        rounds.truncate(max_items);
    }
    Ok(rounds)
}

fn compute_chain_aware_luck_page(
    store: &PoolStore,
    node: &NodeClient,
    max_height: u64,
    limit: usize,
    offset: usize,
) -> anyhow::Result<(Vec<LuckRoundResponse>, usize)> {
    let block_window = (limit.saturating_add(offset)).saturating_add(1).max(2) as i64;
    let blocks = store.get_recent_blocks_up_to(block_window, max_height)?;
    let mut rounds = compute_luck_history(store, blocks, None)?;
    flag_chain_mismatched_luck_rows(node, max_height, &mut rounds);
    let total = store.get_block_count_up_to(max_height)?.saturating_sub(1) as usize;
    let items = rounds
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Ok((items, total))
}

fn compute_chain_aware_block_totals(
    store: &PoolStore,
    node: &NodeClient,
    daemon_chain_height: u64,
) -> anyhow::Result<(u64, u64, u64)> {
    let (unique_total_blocks, unique_orphaned_blocks) = store.get_unique_block_identity_counts()?;
    let (live_confirmed_blocks, live_orphaned_blocks, _live_pending_blocks) =
        store.get_block_status_counts()?;

    let mut extra_orphaned_blocks = 0u64;
    let mut confirmed_blocks_to_reclassify = 0u64;
    let recent_blocks = store.get_recent_blocks(CHAIN_AWARE_ORPHAN_LOOKBACK_BLOCKS)?;
    for block in recent_blocks {
        if block.orphaned {
            continue;
        }

        let mismatch = if block.height > daemon_chain_height {
            true
        } else {
            match node.get_block_by_height_optional(block.height) {
                Ok(Some(node_block)) => node_block.hash != block.hash,
                Ok(None) => false,
                Err(err) => {
                    tracing::warn!(
                        height = block.height,
                        error = %err,
                        "failed to compare pool block against daemon while computing effective totals"
                    );
                    false
                }
            }
        };
        if !mismatch {
            continue;
        }

        extra_orphaned_blocks = extra_orphaned_blocks.saturating_add(1);
        if block.confirmed {
            confirmed_blocks_to_reclassify = confirmed_blocks_to_reclassify.saturating_add(1);
        }
    }

    Ok((
        unique_total_blocks,
        live_confirmed_blocks.saturating_sub(confirmed_blocks_to_reclassify),
        unique_orphaned_blocks
            .max(live_orphaned_blocks)
            .saturating_add(extra_orphaned_blocks),
    ))
}

fn flag_chain_mismatched_blocks(
    node: &NodeClient,
    daemon_chain_height: u64,
    blocks: &mut [DbBlock],
) {
    for block in blocks {
        if block.orphaned || block.height > daemon_chain_height {
            continue;
        }
        match node.get_block_by_height_optional(block.height) {
            Ok(Some(node_block)) if node_block.hash != block.hash => {
                block.confirmed = false;
                block.orphaned = true;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    height = block.height,
                    error = %err,
                    "failed to compare pool block against daemon for public response"
                );
            }
        }
    }
}

fn flag_chain_mismatched_luck_rows(
    node: &NodeClient,
    daemon_chain_height: u64,
    rows: &mut [LuckRoundResponse],
) {
    for row in rows {
        if row.orphaned || row.block_height > daemon_chain_height {
            continue;
        }
        match node.get_block_by_height_optional(row.block_height) {
            Ok(Some(node_block)) if node_block.hash != row.block_hash => {
                row.confirmed = false;
                row.orphaned = true;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    height = row.block_height,
                    error = %err,
                    "failed to compare luck row against daemon for public response"
                );
            }
        }
    }
}

fn luck_round_response_from_db(round: DbLuckRound) -> LuckRoundResponse {
    let effort_pct = if round.difficulty > 0 {
        (round.round_work as f64 / round.difficulty as f64) * 100.0
    } else {
        0.0
    };
    let timer_effort_pct = if ROUND_TARGET_SECONDS > 0.0 {
        (round.duration_seconds as f64 / ROUND_TARGET_SECONDS) * 100.0
    } else {
        0.0
    };

    LuckRoundResponse {
        block_height: round.block_height,
        block_hash: round.block_hash,
        timestamp: round.timestamp,
        difficulty: round.difficulty,
        round_work: round.round_work,
        effort_pct,
        duration_seconds: round.duration_seconds,
        timer_effort_pct,
        effort_band: classify_effort(effort_pct),
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
    let node = Arc::clone(&state.node);
    let daemon_chain_height = state.node.chain_height();
    let started_at = Instant::now();

    let (items, total) = match tokio::task::spawn_blocking(move || {
        compute_chain_aware_luck_page(
            store.as_ref(),
            node.as_ref(),
            daemon_chain_height,
            limit,
            offset,
        )
    })
    .await
    {
        Ok(Ok((items, total))) => {
            record_api_operation_observation(&state, "luck_page_load", started_at.elapsed(), false);
            (items, total)
        }
        Ok(Err(err)) => {
            record_api_operation_observation(&state, "luck_page_load", started_at.elapsed(), true);
            return internal_error("failed loading luck history", err).into_response();
        }
        Err(err) => {
            record_api_operation_observation(&state, "luck_page_load", started_at.elapsed(), true);
            return internal_error(
                "failed loading luck history",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response();
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
            .into_response();
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
            confirmed: batch.confirmed,
            batch_id: None,
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
                .into_response();
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
            .into_response();
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
                    .into_response();
            }
        };

        return Json(FeesResponse {
            total_collected,
            total_pending: 0,
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
    let cfg = state.config.clone();
    let current_height = state.node.chain_height();
    let FeePageData {
        total_collected,
        total_pending,
        items: fees,
        total,
    } = match tokio::task::spawn_blocking(move || {
        build_fee_page(
            store.as_ref(),
            &cfg,
            current_height,
            fee_address.as_deref(),
            sort,
            limit,
            offset,
        )
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading fees", err).into_response(),
        Err(err) => {
            return internal_error("failed loading fees", anyhow::anyhow!("join error: {err}"))
                .into_response();
        }
    };
    let returned = fees.len();

    Json(serde_json::json!({
        "total_collected": total_collected,
        "total_pending": total_pending,
        "recent": PagedResponse {
            items: fees,
            page: PageMeta {
                limit,
                offset,
                returned,
                total,
            },
        },
    }))
    .into_response()
}

async fn handle_admin_block_reward_breakdown(
    Path(height): Path<u64>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let cfg = state.config.clone();
    let now = SystemTime::now();
    match tokio::task::spawn_blocking(move || {
        build_block_reward_breakdown(&store, &cfg, height, now)
    })
    .await
    {
        Ok(Ok(breakdown)) => Json(breakdown).into_response(),
        Ok(Err(err)) => {
            if err.to_string().contains("not found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": err.to_string() })),
                )
                    .into_response();
            }
            internal_error("failed loading block reward breakdown", err).into_response()
        }
        Err(err) => internal_error(
            "failed loading block reward breakdown",
            anyhow::anyhow!("join error: {err}"),
        )
        .into_response(),
    }
}

impl ApiState {
    fn cache_states(&self) -> std::collections::BTreeMap<String, CacheStateSummary> {
        let stats_response = self.stats_response_cache.lock();
        let db_totals = self.db_totals_cache.lock();
        let insights = self.insights_cache.lock();
        let rejection_analytics = self.rejection_analytics_cache.lock();
        let daemon_health = self.daemon_health_cache.lock();
        let pool_health = self.pool_health_cache.lock();
        let network_hashrate = self.network_hashrate_cache.lock();
        let persisted_runtime = self.live_runtime_snapshot_cache.lock();
        let pending_estimates = self.pending_estimate_snapshot_cache.lock();
        let miner_balance = self.miner_balance_response_cache.lock();
        let miner_detail = self.miner_detail_response_cache.lock();

        std::collections::BTreeMap::from([
            (
                "stats_response".to_string(),
                CacheStateSummary {
                    entries: usize::from(stats_response.value.is_some()),
                    age_millis: instant_age_millis(stats_response.updated_at),
                },
            ),
            (
                "db_totals".to_string(),
                CacheStateSummary {
                    entries: usize::from(db_totals.updated_at.is_some()),
                    age_millis: instant_age_millis(db_totals.updated_at),
                },
            ),
            (
                "insights".to_string(),
                CacheStateSummary {
                    entries: usize::from(insights.value.is_some()),
                    age_millis: instant_age_millis(insights.updated_at),
                },
            ),
            (
                "rejection_analytics".to_string(),
                CacheStateSummary {
                    entries: rejection_analytics.entries.len(),
                    age_millis: instant_age_millis(latest_timed_cache_update(
                        &rejection_analytics.entries,
                    )),
                },
            ),
            (
                "daemon_health".to_string(),
                CacheStateSummary {
                    entries: usize::from(daemon_health.value.is_some()),
                    age_millis: instant_age_millis(daemon_health.updated_at),
                },
            ),
            (
                "pool_health".to_string(),
                CacheStateSummary {
                    entries: usize::from(pool_health.value.is_some()),
                    age_millis: instant_age_millis(pool_health.updated_at),
                },
            ),
            (
                "network_hashrate".to_string(),
                CacheStateSummary {
                    entries: usize::from(network_hashrate.hashrate_hps.is_some()),
                    age_millis: instant_age_millis(network_hashrate.updated_at),
                },
            ),
            (
                "persisted_runtime_snapshot".to_string(),
                CacheStateSummary {
                    entries: usize::from(persisted_runtime.value.is_some()),
                    age_millis: instant_age_millis(persisted_runtime.updated_at),
                },
            ),
            (
                "pending_estimate_snapshot".to_string(),
                CacheStateSummary {
                    entries: pending_estimates.values.len(),
                    age_millis: instant_age_millis(pending_estimates.updated_at),
                },
            ),
            (
                "miner_balance_response".to_string(),
                CacheStateSummary {
                    entries: miner_balance.entries.len(),
                    age_millis: instant_age_millis(latest_timed_cache_update(
                        &miner_balance.entries,
                    )),
                },
            ),
            (
                "miner_detail_response".to_string(),
                CacheStateSummary {
                    entries: miner_detail.entries.len(),
                    age_millis: instant_age_millis(latest_timed_cache_update(
                        &miner_detail.entries,
                    )),
                },
            ),
        ])
    }

    fn performance_response(&self) -> ApiPerformanceResponse {
        let snapshot = self.performance.snapshot();
        ApiPerformanceResponse {
            sampled_at: snapshot.sampled_at,
            routes: snapshot.routes,
            operations: snapshot.operations,
            tasks: snapshot.tasks,
            caches: snapshot.caches,
            cache_states: self.cache_states(),
        }
    }

    pub async fn sample_status(&self) {
        let started_at = Instant::now();
        let mut failed = false;
        let (daemon, pool) = tokio::join!(self.daemon_health(), self.pool_health());
        let snapshot = {
            let mut history = self.status_history.lock();
            history.record_sample(
                SystemTime::now(),
                daemon.reachable,
                pool.database_reachable,
                daemon.syncing,
                daemon.error.as_deref(),
                pool.error.as_deref(),
            );
            history.clone()
        };
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let payload = serde_json::to_vec(&snapshot)?;
            store.set_meta(STATUS_HISTORY_META_KEY, &payload)?;
            Ok(())
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                failed = true;
                tracing::warn!(error = %err, "failed persisting status history");
            }
            Err(err) => {
                failed = true;
                tracing::warn!(error = %err, "status history persist task join failed");
            }
        }
        record_api_task_observation(self, "status_sample", started_at.elapsed(), failed);
    }

    async fn persisted_runtime_snapshot(&self) -> Option<PersistedRuntimeSnapshot> {
        {
            let cache = self.live_runtime_snapshot_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < DAEMON_HEALTH_CACHE_TTL)
            {
                return cache.value.clone();
            }
        }

        let store = Arc::clone(&self.store);
        let started_at = Instant::now();
        let loaded = match tokio::task::spawn_blocking(
            move || -> anyhow::Result<Option<PersistedRuntimeSnapshot>> {
                let Some(raw) = store.get_meta(LIVE_RUNTIME_SNAPSHOT_META_KEY)? else {
                    return Ok(None);
                };
                Ok(Some(serde_json::from_slice(&raw)?))
            },
        )
        .await
        {
            Ok(Ok(value)) => {
                record_api_operation_observation(
                    self,
                    "persisted_runtime_snapshot_load",
                    started_at.elapsed(),
                    false,
                );
                value
            }
            Ok(Err(err)) => {
                record_api_operation_observation(
                    self,
                    "persisted_runtime_snapshot_load",
                    started_at.elapsed(),
                    true,
                );
                tracing::warn!(error = %err, "failed loading persisted live runtime snapshot");
                None
            }
            Err(err) => {
                record_api_operation_observation(
                    self,
                    "persisted_runtime_snapshot_load",
                    started_at.elapsed(),
                    true,
                );
                tracing::warn!(error = %err, "live runtime snapshot task join failed");
                None
            }
        };

        let mut cache = self.live_runtime_snapshot_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.value = loaded.clone();
        loaded
    }

    async fn effective_pool_snapshot(&self) -> PoolSnapshot {
        let mut live = self.stats.snapshot();
        if pool_snapshot_has_live_data(&live) {
            return live;
        }
        if let Some(persisted) = self.persisted_runtime_snapshot().await {
            live.total_shares_accepted = live
                .total_shares_accepted
                .max(persisted.total_shares_accepted);
            live.connected_miners = persisted.connected_miners;
            live.connected_workers = persisted.connected_workers;
            if live.estimated_hashrate <= 0.0 {
                live.estimated_hashrate = persisted.estimated_hashrate;
            }
            if live.last_share_at.is_none() {
                live.last_share_at = persisted.last_share_at;
            }
        }
        live
    }

    async fn effective_validation_summary(&self) -> ValidationSummary {
        let live = self
            .validation
            .as_ref()
            .map(|validation| validation_summary_from_snapshot(validation.snapshot()));
        if let Some(persisted) = self.persisted_runtime_snapshot().await {
            let Some(live) = live else {
                return validation_summary_from_persisted(&persisted.validation);
            };
            if validation_summary_is_empty(&live) {
                return validation_summary_from_persisted(&persisted.validation);
            }
            return merge_validation_summary(live, &persisted.validation);
        }
        live.unwrap_or_else(|| {
            validation_summary_from_persisted(&PersistedValidationSummary::default())
        })
    }

    async fn cached_stats_response(&self) -> anyhow::Result<StatsResponse> {
        {
            let cache = self.stats_response_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < STATS_RESPONSE_CACHE_TTL)
            {
                if let Some(value) = cache.value.clone() {
                    self.performance.record_cache_hit("stats_response");
                    return Ok(value);
                }
            }
        }

        self.performance.record_cache_miss("stats_response");
        let fresh = self.load_stats_response().await?;
        let mut cache = self.stats_response_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.value = Some(fresh.clone());
        Ok(fresh)
    }

    async fn load_stats_response(&self) -> anyhow::Result<StatsResponse> {
        let started_at = Instant::now();
        let snap = self.effective_pool_snapshot().await;
        let validation = self.effective_validation_summary().await;
        let totals = self.db_totals().await?;
        let current_job = self.jobs.current_job();
        let current_job_height = current_job.as_ref().map(|j| j.height);
        let network_hashrate = self.network_hashrate_for_job(current_job.as_ref()).await;

        let store = Arc::clone(&self.store);
        let pool_hashrate_started_at = Instant::now();
        let pool_hashrate = tokio::task::spawn_blocking(move || db_pool_hashrate(&store))
            .await
            .unwrap_or(0.0);
        record_api_operation_observation(
            self,
            "pool_hashrate_load",
            pool_hashrate_started_at.elapsed(),
            false,
        );

        let response = StatsResponse {
            pool: PoolSummary {
                miners: snap.connected_miners,
                workers: snap.connected_workers,
                hashrate: pool_hashrate,
                shares_accepted: totals.accepted_shares,
                shares_rejected: totals.rejected_shares,
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
                paid_to_miners_total: totals.paid_to_miners_total,
            },
            chain: ChainSummary {
                current_job_height,
                network_hashrate,
                daemon_chain_height: self.node.chain_height(),
                daemon_syncing: self.node.syncing(),
            },
            validation,
        };
        record_api_operation_observation(self, "stats_load", started_at.elapsed(), false);
        Ok(response)
    }

    async fn admin_dev_fee_telemetry(&self) -> anyhow::Result<AdminDevFeeTelemetryResponse> {
        let store = Arc::clone(&self.store);
        let hint_floor = self.config.initial_share_difficulty.max(1);
        tokio::task::spawn_blocking(move || {
            let windows = [
                ("1h", Duration::from_secs(60 * 60)),
                ("6h", Duration::from_secs(6 * 60 * 60)),
                ("24h", Duration::from_secs(24 * 60 * 60)),
            ];
            let now = SystemTime::now();
            let mut window_rows = Vec::with_capacity(windows.len());
            for (label, window) in windows {
                let since = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
                let (pool_accepted_difficulty, _, _, _) = store.hashrate_stats_pool(since)?;
                let dev = store.miner_share_window_stats_since(SEINE_DEV_FEE_ADDRESS, since)?;
                let dev_gross_difficulty = dev
                    .accepted_difficulty
                    .saturating_add(dev.rejected_difficulty);
                let rejected_total = dev.rejected_count;
                window_rows.push(AdminDevFeeWindowResponse {
                    label: label.to_string(),
                    window_seconds: window.as_secs(),
                    pool_accepted_difficulty,
                    dev_accepted_difficulty: dev.accepted_difficulty,
                    dev_rejected_difficulty: dev.rejected_difficulty,
                    dev_gross_difficulty,
                    accepted_shares: dev.accepted_count,
                    rejected_shares: rejected_total,
                    stale_rejected_shares: dev.stale_rejected_count,
                    stale_rejected_difficulty: dev.stale_rejected_difficulty,
                    accepted_pct: ratio_pct(dev.accepted_difficulty, pool_accepted_difficulty),
                    gross_pct: ratio_pct(dev_gross_difficulty, pool_accepted_difficulty),
                    reject_rate_pct: ratio_pct(
                        dev.rejected_count,
                        dev.accepted_count.saturating_add(rejected_total),
                    ),
                    stale_reject_rate_pct: ratio_pct(dev.stale_rejected_count, rejected_total),
                });
            }

            let summary = store.vardiff_hint_summary(SEINE_DEV_FEE_ADDRESS, hint_floor)?;
            let recent_hints = store
                .recent_vardiff_hint_diagnostics(SEINE_DEV_FEE_ADDRESS, ADMIN_DEV_FEE_HINT_LIMIT)?
                .into_iter()
                .map(|row| AdminDevFeeHintRowResponse {
                    position: if row.difficulty < hint_floor {
                        "below-floor"
                    } else if row.difficulty == hint_floor {
                        "at-floor"
                    } else {
                        "above-floor"
                    },
                    worker: row.worker,
                    difficulty: row.difficulty,
                    updated_at: row.updated_at,
                })
                .collect::<Vec<_>>();

            Ok::<_, anyhow::Error>(AdminDevFeeTelemetryResponse {
                address: SEINE_DEV_FEE_ADDRESS.to_string(),
                reference_target_pct: SEINE_DEV_FEE_REFERENCE_TARGET_PCT,
                hint_floor,
                windows: window_rows,
                hints: AdminDevFeeHintSummaryResponse {
                    total_workers: summary.total_workers,
                    below_floor_workers: summary.below_floor_workers,
                    at_floor_workers: summary.at_floor_workers,
                    above_floor_workers: summary.above_floor_workers,
                    min_difficulty: summary.min_difficulty,
                    median_difficulty: summary.median_difficulty,
                    max_difficulty: summary.max_difficulty,
                    latest_updated_at: summary.latest_updated_at,
                },
                recent_hints,
            })
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))?
    }

    async fn admin_balance_overview(&self) -> anyhow::Result<AdminBalanceOverviewResponse> {
        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        let fee_address = self.config.pool_wallet_address.trim().to_string();
        tokio::task::spawn_blocking(move || {
            let generated_at = SystemTime::now();
            let wallet_balance = node.get_wallet_balance()?;
            let wallet_outputs = node.get_wallet_outputs()?;
            let pending_regular_tx_hashes = wallet_outputs
                .outputs
                .iter()
                .filter(|output| {
                    !output.is_spendable()
                        && !output.status.eq_ignore_ascii_case("spent")
                        && !output.r#type.eq_ignore_ascii_case("coinbase")
                })
                .map(|output| output.txid.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let matched_payout_tx_hashes =
                store.find_existing_payout_tx_hashes(&pending_regular_tx_hashes)?;
            let output_summary =
                summarize_admin_balance_outputs(&wallet_outputs.outputs, &matched_payout_tx_hashes);

            let balances = store.get_all_balances()?;
            let source_by_address = store
                .list_balance_source_summaries()?
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
            let unpaid_count = balances
                .iter()
                .filter(|balance| balance.pending > 0 && !is_pool_fee_balance(&balance.address))
                .count();
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
                    pool_fee_orphan_backed_unpaid_amount =
                        pool_fee_orphan_backed_unpaid_amount.saturating_add(source.orphan_pending);
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
                    pool_fee_balance_source_drift_amount =
                        pool_fee_balance_source_drift_amount.saturating_add(balance_above_sources);
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
            let miner_total_credited = miner_paid_total.saturating_add(miner_unpaid_total);
            let canonical_miner_reward_total =
                net_block_reward_total.saturating_sub(pool_fee_total);
            let spendable_minus_queued =
                signed_amount_delta(wallet_balance.spendable, queued_amount);

            Ok::<_, anyhow::Error>(AdminBalanceOverviewResponse {
                generated_at,
                wallet: AdminBalanceOverviewWallet {
                    spendable: wallet_balance.spendable,
                    pending: wallet_balance.pending,
                    pending_unconfirmed: wallet_balance.pending_unconfirmed,
                    pending_unconfirmed_eta: wallet_balance.pending_unconfirmed_eta,
                    total: wallet_balance.total,
                },
                payouts: AdminBalanceOverviewPayouts {
                    unpaid_count,
                    unpaid_amount: miner_unpaid_total,
                    clean_unpaid_count,
                    clean_unpaid_amount,
                    orphan_backed_unpaid_amount,
                    balance_source_drift_amount,
                    pool_fee_unpaid_amount: pool_fee_unpaid_total,
                    pool_fee_clean_unpaid_amount,
                    pool_fee_orphan_backed_unpaid_amount,
                    pool_fee_balance_source_drift_amount,
                    queued_count: pending_payouts.len(),
                    queued_amount,
                },
                outputs: output_summary,
                ledger: AdminBalanceOverviewLedger {
                    miner_paid_total,
                    miner_unpaid_total,
                    miner_total_credited,
                    miner_clean_unpaid_total: clean_unpaid_amount,
                    miner_orphan_backed_unpaid_total: orphan_backed_unpaid_amount,
                    miner_balance_source_drift_total: balance_source_drift_amount,
                    net_block_reward_total,
                    pool_fee_total,
                    pool_fee_paid_total,
                    pool_fee_unpaid_total,
                    pool_fee_clean_unpaid_total: pool_fee_clean_unpaid_amount,
                    pool_fee_orphan_backed_unpaid_total: pool_fee_orphan_backed_unpaid_amount,
                    pool_fee_balance_source_drift_total: pool_fee_balance_source_drift_amount,
                    pool_fee_balance_total: pool_fee_paid_total
                        .saturating_add(pool_fee_unpaid_total),
                    miner_rewards_balanced: miner_total_credited == canonical_miner_reward_total,
                },
                liquidity: AdminBalanceOverviewLiquidity {
                    spendable_minus_queued,
                    queue_shortfall_amount: queued_amount.saturating_sub(wallet_balance.spendable),
                },
            })
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))?
    }

    async fn admin_reconciliation_issues(
        &self,
    ) -> anyhow::Result<AdminReconciliationIssuesResponse> {
        let store = Arc::clone(&self.store);
        let (orphaned_blocks, payout_rows) = tokio::task::spawn_blocking(move || {
            Ok::<_, anyhow::Error>((
                store.list_orphaned_block_credit_issues()?,
                store.list_unreconciled_completed_payout_rows()?,
            ))
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

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

        let mut missing_payouts = Vec::<AdminMissingCompletedPayoutIssueResponse>::new();
        for (tx_hash, rows) in tx_rows {
            let status = self.node.get_tx_status_optional(&tx_hash)?;
            if status.is_some() {
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
                linked_amount,
                live_linked_amount: linked_amount.saturating_sub(orphaned_linked_amount),
                orphaned_linked_amount,
                unlinked_amount: total_amount.saturating_sub(linked_amount),
            });
        }

        let orphaned_block_total_credit_amount = orphaned_blocks.iter().fold(0u64, |acc, issue| {
            acc.saturating_add(issue.remaining_credit_amount)
                .saturating_add(issue.remaining_fee_amount)
        });
        let orphaned_blocks = orphaned_blocks
            .into_iter()
            .map(map_orphaned_block_issue)
            .collect::<Vec<_>>();
        let missing_payout_total_amount = missing_payouts
            .iter()
            .fold(0u64, |acc, issue| acc.saturating_add(issue.total_amount));

        Ok(AdminReconciliationIssuesResponse {
            generated_at: SystemTime::now(),
            summary: AdminReconciliationIssuesSummary {
                total_open_issues: missing_payouts.len().saturating_add(orphaned_blocks.len()),
                missing_payout_issue_count: missing_payouts.len(),
                missing_payout_total_amount,
                orphaned_block_issue_count: orphaned_blocks.len(),
                orphaned_block_total_credit_amount,
            },
            missing_payouts,
            orphaned_blocks,
        })
    }

    async fn resolve_missing_completed_payout_issue(
        &self,
        tx_hash: &str,
        action: AdminReconciliationPayoutResolutionAction,
    ) -> anyhow::Result<AdminReconciliationPayoutResolutionResponse> {
        if self.node.get_tx_status_optional(tx_hash)?.is_some() {
            return Err(anyhow::anyhow!(
                "tx {} still exists on the current daemon chain",
                tx_hash
            ));
        }

        let store = Arc::clone(&self.store);
        let tx_hash_owned = tx_hash.to_string();
        let result = tokio::task::spawn_blocking(move || {
            store.resolve_completed_payout_tx_override(&tx_hash_owned, action.into_store_kind())
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        self.clear_miner_response_caches();

        Ok(AdminReconciliationPayoutResolutionResponse {
            tx_hash: tx_hash.to_string(),
            action: action.as_str(),
            reverted_payout_rows: result.reverted_payout_rows,
            restored_pending_amount: result.restored_pending_amount,
            dropped_amount: result.dropped_orphaned_amount,
        })
    }

    async fn import_confirmed_wallet_payouts(
        &self,
        tx_hashes: Vec<String>,
    ) -> anyhow::Result<AdminReconciliationPayoutImportResponse> {
        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        let config = self.config.clone();
        let (report, imported_txs) = tokio::task::spawn_blocking(move || {
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
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

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
        let report = tokio::task::spawn_blocking(move || {
            store.apply_manual_payout_offsets_to_live_pending()
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

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
                .map(map_manual_payout_offset_application)
                .collect(),
        })
    }

    async fn admin_share_diagnostics(&self) -> anyhow::Result<AdminShareDiagnosticsResponse> {
        let now = SystemTime::now();
        let validation = self.effective_validation_summary().await;
        let persisted_runtime = self.persisted_runtime_snapshot().await;
        let submit = submit_summary_from_persisted(persisted_runtime.as_ref());
        let job = effective_job_health(self, persisted_runtime.as_ref());
        let pool_activity = pool_activity_health(now, persisted_runtime.as_ref());
        let store = Arc::clone(&self.store);
        let windows = tokio::task::spawn_blocking(move || collect_admin_share_windows(&store, now))
            .await
            .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        Ok(AdminShareDiagnosticsResponse {
            generated_at: now,
            windows,
            submit,
            validation,
            job,
            pool_activity,
        })
    }

    fn clear_miner_response_caches(&self) {
        self.miner_balance_response_cache.lock().entries.clear();
        self.miner_detail_response_cache.lock().entries.clear();
    }

    async fn cached_pending_estimate_for_miner(
        &self,
        address: &str,
        chain_height: u64,
    ) -> anyhow::Result<MinerPendingEstimate> {
        loop {
            let now = Instant::now();
            let mut schedule_refresh = false;
            let mut cached_values = None;
            let mut wait_for_refresh = false;
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
                } else if cache.refresh_in_flight {
                    wait_for_refresh = true;
                } else {
                    cache.refresh_in_flight = true;
                    should_load = true;
                }
            }
            if let Some(values) = cached_values {
                if schedule_refresh {
                    self.spawn_pending_estimate_snapshot_refresh(chain_height);
                }
                self.performance.record_cache_hit("pending_estimate");
                return Ok(values.get(address).cloned().unwrap_or_default());
            }
            if wait_for_refresh {
                self.pending_estimate_snapshot_notify.notified().await;
                continue;
            }

            debug_assert!(should_load);
            self.performance.record_cache_miss("pending_estimate");
            let values = self.load_pending_estimate_snapshot(chain_height).await?;
            return Ok(values.get(address).cloned().unwrap_or_default());
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
        let result = tokio::task::spawn_blocking(move || {
            estimate_unconfirmed_pending_snapshot(&store, &cfg, now, chain_height)
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"));

        match result {
            Ok(Ok(values)) => {
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
            Ok(Err(err)) => {
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
        if let Some(cached) = {
            let mut cache = self.miner_balance_response_cache.lock();
            let now = Instant::now();
            if cache
                .last_cleanup_at
                .is_none_or(|last| now.duration_since(last) >= MINER_BALANCE_RESPONSE_CACHE_TTL)
            {
                prune_timed_cache_entries(
                    &mut cache.entries,
                    MINER_BALANCE_RESPONSE_CACHE_TTL,
                    MINER_BALANCE_RESPONSE_CACHE_MAX_ENTRIES,
                    now,
                );
                cache.last_cleanup_at = Some(now);
            }
            cache
                .entries
                .get(&cache_key)
                .filter(|entry| entry.updated_at.elapsed() < MINER_BALANCE_RESPONSE_CACHE_TTL)
                .map(|entry| entry.value.clone())
        } {
            self.performance.record_cache_hit("miner_balance");
            return Ok(cached);
        }

        self.performance.record_cache_miss("miner_balance");
        let fresh = self
            .load_miner_balance_payload(address, include_pending_estimate)
            .await?;
        let mut cache = self.miner_balance_response_cache.lock();
        prune_timed_cache_entries(
            &mut cache.entries,
            MINER_BALANCE_RESPONSE_CACHE_TTL,
            MINER_BALANCE_RESPONSE_CACHE_MAX_ENTRIES.saturating_sub(1),
            Instant::now(),
        );
        cache.entries.insert(
            cache_key,
            TimedCacheEntry {
                updated_at: Instant::now(),
                value: fresh.clone(),
            },
        );
        Ok(fresh)
    }

    async fn cached_miner_detail_payload(
        &self,
        address: &str,
        share_limit: i64,
        include_pending_estimate: bool,
    ) -> anyhow::Result<MinerDetailPayload> {
        let cache_key = format!("{address}:{share_limit}:{include_pending_estimate}");
        if let Some(cached) = {
            let mut cache = self.miner_detail_response_cache.lock();
            let now = Instant::now();
            if cache
                .last_cleanup_at
                .is_none_or(|last| now.duration_since(last) >= MINER_DETAIL_RESPONSE_CACHE_TTL)
            {
                prune_timed_cache_entries(
                    &mut cache.entries,
                    MINER_DETAIL_RESPONSE_CACHE_TTL,
                    MINER_DETAIL_RESPONSE_CACHE_MAX_ENTRIES,
                    now,
                );
                cache.last_cleanup_at = Some(now);
            }
            cache
                .entries
                .get(&cache_key)
                .filter(|entry| entry.updated_at.elapsed() < MINER_DETAIL_RESPONSE_CACHE_TTL)
                .map(|entry| entry.value.clone())
        } {
            self.performance.record_cache_hit("miner_detail");
            return Ok(cached);
        }

        self.performance.record_cache_miss("miner_detail");
        let fresh = self
            .load_miner_detail_payload(address, share_limit, include_pending_estimate)
            .await?;
        let mut cache = self.miner_detail_response_cache.lock();
        prune_timed_cache_entries(
            &mut cache.entries,
            MINER_DETAIL_RESPONSE_CACHE_TTL,
            MINER_DETAIL_RESPONSE_CACHE_MAX_ENTRIES.saturating_sub(1),
            Instant::now(),
        );
        cache.entries.insert(
            cache_key,
            TimedCacheEntry {
                updated_at: Instant::now(),
                value: fresh.clone(),
            },
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
        let db_result = tokio::task::spawn_blocking(
            move || -> anyhow::Result<(Balance, Option<PendingPayout>, bool)> {
                Ok((
                    store.get_balance(&addr)?,
                    store.get_pending_payout(&addr)?,
                    store.miner_has_any_activity(&addr)?,
                ))
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"));
        let (balance, pending_payout, has_activity) = match db_result {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                record_api_operation_observation(
                    self,
                    "miner_balance_load",
                    started_at.elapsed(),
                    true,
                );
                return Err(err);
            }
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
            pending_payout,
        };
        record_api_operation_observation(self, "miner_balance_load", started_at.elapsed(), false);
        Ok(payload)
    }

    async fn load_miner_detail_payload(
        &self,
        address: &str,
        share_limit: i64,
        include_pending_estimate: bool,
    ) -> anyhow::Result<MinerDetailPayload> {
        let started_at = Instant::now();
        let store = Arc::clone(&self.store);
        let chain_height = self.node.chain_height();
        let provisional_cutoff = SystemTime::now()
            .checked_sub(self.config.provisional_share_delay_duration())
            .unwrap_or(UNIX_EPOCH);
        let addr = address.to_string();
        let db_result = tokio::task::spawn_blocking(move || {
            let shares = store.get_shares_for_miner(&addr, share_limit)?;
            let mining_since = store.first_share_at_for_miner(&addr)?;
            let balance = store.get_balance(&addr)?;
            let pending_payout = store.get_pending_payout(&addr)?;
            let payouts =
                store.get_recent_visible_payouts_for_address(&addr, MINER_PAYOUT_HISTORY_LIMIT)?;
            let hr = db_miner_hashrate(&store, &addr);
            let since_hr_window = SystemTime::now()
                .checked_sub(HASHRATE_WINDOW)
                .unwrap_or(UNIX_EPOCH);
            let since_24h = SystemTime::now()
                .checked_sub(Duration::from_secs(86400))
                .unwrap_or(UNIX_EPOCH);
            let workers_raw = store.worker_stats_for_miner(&addr, since_24h)?;
            let worker_hashrate_raw =
                store.worker_hashrate_stats_for_miner(&addr, since_hr_window)?;
            let miner_blocks = store.get_blocks_for_miner(&addr)?;
            let risk_state = store.get_address_risk(&addr)?;
            let validation_state = store.validation_hold_state(&addr, provisional_cutoff)?;
            let has_any_activity = store.miner_has_any_activity(&addr)?;
            Ok::<_, anyhow::Error>((
                shares,
                mining_since,
                balance,
                pending_payout,
                payouts,
                hr,
                workers_raw,
                worker_hashrate_raw,
                miner_blocks,
                risk_state,
                validation_state,
                has_any_activity,
            ))
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"));
        let db_result = match db_result {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                record_api_operation_observation(
                    self,
                    "miner_detail_load",
                    started_at.elapsed(),
                    true,
                );
                return Err(err);
            }
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
            balance,
            pending_payout,
            payouts,
            hashrate,
            workers_raw,
            worker_hashrate_raw,
            mut miner_blocks,
            risk_state,
            validation_state,
            has_any_activity,
        ) = db_result;
        for block in &mut miner_blocks {
            hydrate_provisional_block_reward(block);
        }

        let pending_estimate = if include_pending_estimate {
            if has_any_activity {
                match self
                    .cached_pending_estimate_for_miner(address, chain_height)
                    .await
                {
                    Ok(value) => value,
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
            }
        } else {
            MinerPendingEstimate::default()
        };
        let verification_hold = miner_verification_hold(
            risk_state.as_ref(),
            validation_state.as_ref(),
            SystemTime::now(),
        );

        let pending_confirmed = balance.pending;
        let balance_json = miner_balance_response(&balance, pending_payout.as_ref());

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

        let pending_note = if include_pending_estimate {
            pending_balance_note(&self.config, hashrate, total_accepted, &pending_estimate)
        } else {
            None
        };
        let payout_note =
            payout_status_note(&self.config, pending_confirmed, pending_payout.as_ref());

        let has_activity = has_any_activity;

        let mut body = serde_json::json!({
            "shares": shares,
            "mining_since": mining_since,
            "hashrate": hashrate,
            "balance": balance_json,
            "pending_estimate": pending_estimate,
            "pending_note": pending_note,
            "payout_note": payout_note,
            "verification_hold": verification_hold,
            "pending_payout": pending_payout,
            "payouts": payouts,
            "workers": workers_json,
            "blocks_found": miner_blocks,
            "total_accepted": total_accepted,
            "total_rejected": total_rejected,
        });

        if has_activity {
            body["stats"] = serde_json::Value::Null;
        } else {
            body["error"] = serde_json::Value::String("miner not found".to_string());
        }

        let payload = MinerDetailPayload {
            found: has_activity,
            body,
        };
        record_api_operation_observation(self, "miner_detail_load", started_at.elapsed(), false);
        Ok(payload)
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
        let daemon_chain_height = self.node.chain_height();

        let store = Arc::clone(&self.store);
        let node = Arc::clone(&self.node);
        let now = SystemTime::now();
        let (pool_hashrate, round_start, round_work, mut payout_eta, luck_history, avg_effort_pct) =
            tokio::task::spawn_blocking(move || {
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

                let payout_eta = compute_payout_eta(&store)?;
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
            .await
            .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        let node = Arc::clone(&self.node);
        let wallet_balance = tokio::task::spawn_blocking(move || node.get_wallet_balance())
            .await
            .ok()
            .and_then(Result::ok);
        let persisted_runtime = self.persisted_runtime_snapshot().await;
        apply_runtime_schedule_to_payout_eta(
            &mut payout_eta,
            persisted_runtime.as_ref().map(|snapshot| &snapshot.payouts),
        );
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
            avg_effort_pct,
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
        let window_seconds = window.as_secs().max(1);
        {
            let cache = self.rejection_analytics_cache.lock();
            if let Some(entry) = cache.entries.get(&window_seconds) {
                if entry.updated_at.elapsed() < REJECTION_ANALYTICS_CACHE_TTL {
                    self.performance.record_cache_hit("rejection_analytics");
                    return Ok(entry.value.clone());
                }
            }
        }

        self.performance.record_cache_miss("rejection_analytics");
        let mut snapshot = self.stats.rejection_analytics(window);
        let since = SystemTime::now().checked_sub(window).unwrap_or(UNIX_EPOCH);
        let started_at = Instant::now();
        let outcome_store = Arc::clone(&self.store);
        let total_rejected_store = Arc::clone(&self.store);
        let by_reason_store = Arc::clone(&self.store);
        let totals_by_reason_store = Arc::clone(&self.store);
        let outcome_task =
            tokio::task::spawn_blocking(move || outcome_store.share_outcome_counts_since(since));
        let total_rejected_task =
            tokio::task::spawn_blocking(move || total_rejected_store.total_rejected_share_count());
        let by_reason_task = tokio::task::spawn_blocking(move || {
            by_reason_store.rejection_reason_counts_since(since)
        });
        let totals_by_reason_task = tokio::task::spawn_blocking(move || {
            totals_by_reason_store.total_rejection_reason_counts()
        });
        let (outcome_result, total_rejected_result, by_reason_result, totals_by_reason_result) = tokio::join!(
            outcome_task,
            total_rejected_task,
            by_reason_task,
            totals_by_reason_task,
        );
        let load_value =
            || -> anyhow::Result<(u64, u64, u64, Vec<RejectionReasonCount>, Vec<RejectionReasonCount>)> {
                let (accepted, rejected) = outcome_result
                    .map_err(|err| anyhow::anyhow!("join error: {err}"))??;
                let total_rejected = total_rejected_result
                    .map_err(|err| anyhow::anyhow!("join error: {err}"))??;
                let by_reason =
                    by_reason_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
                let totals_by_reason =
                    totals_by_reason_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
                Ok((
                    accepted,
                    rejected,
                    total_rejected,
                    by_reason,
                    totals_by_reason,
                ))
            };
        let (accepted, rejected, total_rejected, by_reason, mut totals_by_reason) =
            match load_value() {
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
        let (latest_local, latest_external, incidents, uptime_summaries) = tokio::task::spawn_blocking(
            move || -> anyhow::Result<(
                Option<MonitorHeartbeat>,
                Option<MonitorHeartbeat>,
                Vec<MonitorIncident>,
                Vec<(String, Duration, MonitorUptimeSummary, MonitorUptimeSummary)>,
            )> {
                let latest_local = store.get_latest_monitor_heartbeat(Some(LOCAL_MONITOR_SOURCE))?;
                let latest_external =
                    store.get_latest_monitor_heartbeat(Some(CLOUDFLARE_MONITOR_SOURCE))?;
                let incidents = store.get_recent_monitor_incidents(32, Some("public"))?;
                let mut uptime_summaries = Vec::with_capacity(uptime_windows.len());
                for (label, window) in uptime_windows {
                    let since = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
                    uptime_summaries.push((
                        label.to_string(),
                        window,
                        store.get_monitor_uptime_summary(since, Some(LOCAL_MONITOR_SOURCE))?,
                        store.get_monitor_uptime_summary(since, Some(CLOUDFLARE_MONITOR_SOURCE))?,
                    ));
                }
                Ok((latest_local, latest_external, incidents, uptime_summaries))
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        let latest_local = latest_local.as_ref();
        let latest_external = latest_external.as_ref();
        let template_age = latest_local.and_then(|row| row.template_age_seconds);
        let template_refresh_millis = latest_local.and_then(|row| row.last_refresh_millis);
        let services = StatusServices {
            public_http: service_health_from_public(latest_external),
            api: service_health_from_local(
                latest_local,
                |row| row.api_up,
                "no recent API heartbeat",
            ),
            stratum: service_health_from_local(
                latest_local,
                |row| row.stratum_up,
                "no recent Stratum heartbeat",
            ),
            database: service_health_from_local(
                latest_local,
                |row| Some(row.db_up),
                "no recent database heartbeat",
            ),
            daemon: service_health_from_local(
                latest_local,
                |row| row.daemon_up,
                "no recent daemon heartbeat",
            ),
        };
        let pool_healthy = services.api.healthy
            && services.stratum.healthy
            && services.database.healthy
            && services.daemon.healthy
            && !latest_local
                .and_then(|row| row.daemon_syncing)
                .unwrap_or(false)
            && !template_refresh_millis
                .is_some_and(|lag| lag >= TEMPLATE_REFRESH_WARN_AFTER_MILLIS)
            && latest_external
                .and_then(|row| row.public_http_up)
                .unwrap_or(true);
        let pool = PoolHealth {
            healthy: pool_healthy,
            database_reachable: latest_local.map(|row| row.db_up).unwrap_or(false),
            error: latest_local
                .filter(|row| !row.db_up)
                .and_then(|row| row.details_json.clone()),
        };
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
            .map(|(label, window, local, external)| {
                build_monitor_uptime_window(&label, window, &local, &external)
            })
            .collect();

        Ok(StatusPageResponse {
            checked_at: now,
            pool_uptime_seconds,
            pool,
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
        let total_shares_store = Arc::clone(&self.store);
        let rejected_shares_store = Arc::clone(&self.store);
        let block_totals_store = Arc::clone(&self.store);
        let block_totals_node = Arc::clone(&self.node);
        let pool_fees_store = Arc::clone(&self.store);
        let paid_to_miners_store = Arc::clone(&self.store);
        let total_shares_task =
            tokio::task::spawn_blocking(move || total_shares_store.get_total_share_count());
        let rejected_shares_task =
            tokio::task::spawn_blocking(move || rejected_shares_store.total_rejected_share_count());
        let block_totals_task =
            tokio::task::spawn_blocking(move || -> anyhow::Result<(u64, u64, u64)> {
                compute_chain_aware_block_totals(
                    block_totals_store.as_ref(),
                    block_totals_node.as_ref(),
                    chain_height,
                )
            });
        let pool_fees_task =
            tokio::task::spawn_blocking(move || pool_fees_store.get_total_pool_fees());
        let paid_to_miners_task =
            tokio::task::spawn_blocking(move || paid_to_miners_store.get_total_paid_to_miners());
        let (
            total_shares_result,
            rejected_shares_result,
            block_totals_result,
            pool_fees_result,
            paid_to_miners_result,
        ) = tokio::join!(
            total_shares_task,
            rejected_shares_task,
            block_totals_task,
            pool_fees_task,
            paid_to_miners_task,
        );
        let load_value = || -> anyhow::Result<DbTotals> {
            let total_shares =
                total_shares_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
            let rejected_shares =
                rejected_shares_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
            let (total_blocks, confirmed_blocks, orphaned_blocks) =
                block_totals_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
            let pool_fees_collected =
                pool_fees_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
            let paid_to_miners_total =
                paid_to_miners_result.map_err(|err| anyhow::anyhow!("join error: {err}"))??;
            Ok(DbTotals {
                total_shares,
                accepted_shares: total_shares.saturating_sub(rejected_shares),
                rejected_shares,
                total_blocks,
                confirmed_blocks,
                orphaned_blocks,
                pool_fees_collected,
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
                current_process_block: status.current_process_block,
                last_process_block: status.last_process_block,
                error: None,
            },
            Ok(Err(err)) => DaemonHealth {
                reachable: false,
                chain_height: None,
                peers: None,
                syncing: None,
                mempool_size: None,
                best_hash: None,
                current_process_block: None,
                last_process_block: None,
                error: Some(err.to_string()),
            },
            Err(err) => DaemonHealth {
                reachable: false,
                chain_height: None,
                peers: None,
                syncing: None,
                mempool_size: None,
                best_hash: None,
                current_process_block: None,
                last_process_block: None,
                error: Some(format!("join error: {err}")),
            },
        };

        let mut cache = self.daemon_health_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.value = Some(value.clone());
        value
    }

    async fn pool_health(&self) -> PoolHealth {
        {
            let cache = self.pool_health_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < POOL_HEALTH_CACHE_TTL)
            {
                if let Some(cached) = cache.value.clone() {
                    return cached;
                }
            }
        }

        let store = Arc::clone(&self.store);
        let sampled =
            tokio::task::spawn_blocking(move || store.get_meta(STATUS_HISTORY_META_KEY)).await;

        let value = match sampled {
            Ok(Ok(_)) => PoolHealth {
                healthy: true,
                database_reachable: true,
                error: None,
            },
            Ok(Err(err)) => PoolHealth {
                healthy: false,
                database_reachable: false,
                error: Some(err.to_string()),
            },
            Err(err) => PoolHealth {
                healthy: false,
                database_reachable: false,
                error: Some(format!("join error: {err}")),
            },
        };

        let mut cache = self.pool_health_cache.lock();
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
        let started_at = Instant::now();
        let sampled = tokio::task::spawn_blocking(move || {
            estimate_explorer_network_hashrate_hps(node.as_ref(), chain_height, difficulty)
        })
        .await
        .ok()
        .and_then(Result::ok)
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

fn public_telemetry_route_kind_for_path(path: &str) -> Option<PublicTelemetryRouteKind> {
    match path {
        "/api/stats" => Some(PublicTelemetryRouteKind::Stats),
        _ if path.ends_with("/balance") && path.starts_with("/api/miner/") => {
            Some(PublicTelemetryRouteKind::MinerBalance)
        }
        _ if path.starts_with("/api/miner/") => Some(PublicTelemetryRouteKind::MinerDetail),
        _ => None,
    }
}

fn forwarded_client_ip(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
}

fn query_includes_pending_estimate(uri: &Uri) -> bool {
    uri.query()
        .map(|query| query.contains("include_pending_estimate=true"))
        .unwrap_or(false)
}

fn api_performance_route_name(uri: &Uri) -> Option<&'static str> {
    let path = uri.path();
    if !path.starts_with("/api/") || path == "/api/events" {
        return None;
    }

    Some(match path {
        "/api/stats" => "stats",
        "/api/stats/history" => "stats_history",
        "/api/stats/insights" => "stats_insights",
        "/api/luck" => "luck",
        "/api/status" => "status",
        "/api/blocks" => "blocks",
        "/api/payouts/recent" => "payouts_recent",
        "/api/health" => "health",
        "/api/admin/perf" => "admin_perf",
        "/api/admin/dev-fee" => "admin_dev_fee",
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
        _ if path.ends_with("/balance") && path.starts_with("/api/miner/") => {
            if query_includes_pending_estimate(uri) {
                "miner_balance_pending"
            } else {
                "miner_balance"
            }
        }
        _ if path.starts_with("/api/miner/") && !path.ends_with("/hashrate") => {
            if query_includes_pending_estimate(uri) {
                "miner_detail_pending"
            } else {
                "miner_detail"
            }
        }
        _ => "other_api",
    })
}

fn api_route_slow_threshold_millis(route: &str) -> u64 {
    match route {
        "stats" | "stats_history" | "health" | "miner_balance" | "payouts_recent" => 100,
        "status" | "stats_insights" | "blocks" => 250,
        "luck" => 500,
        "miner_balance_pending" | "miner_detail" | "other_api" => 250,
        "miner_detail_pending"
        | "admin_dev_fee"
        | "admin_balance_overview"
        | "admin_reconciliation_issues"
        | "admin_reconciliation_payout_import"
        | "admin_reconciliation_manual_offset_apply"
        | "admin_reconciliation_payout_resolution"
        | "admin_orphaned_block_cleanup_retry"
        | "admin_share_diagnostics" => 500,
        _ => 250,
    }
}

fn api_operation_slow_threshold_millis(operation: &str) -> u64 {
    match operation {
        "persisted_runtime_snapshot_load"
        | "stats_load"
        | "db_totals_load"
        | "rejection_analytics_load"
        | "pool_hashrate_load"
        | "luck_page_load"
        | "luck_details_load" => 100,
        "network_hashrate_load" | "miner_balance_load" | "miner_detail_load" => 250,
        "pending_estimate_snapshot_load" => 500,
        _ => 250,
    }
}

fn api_task_slow_threshold_millis(task: &str) -> u64 {
    match task {
        "pending_estimate_snapshot_refresh" => 500,
        _ => 250,
    }
}

fn record_api_route_observation(
    state: &ApiState,
    route: &str,
    status: StatusCode,
    duration: Duration,
) {
    let failed = status.as_u16() >= 400;
    let slow = duration.as_millis() >= u128::from(api_route_slow_threshold_millis(route));
    state
        .performance
        .record_route(route, duration, failed, slow);
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
    let slow = duration.as_millis() >= u128::from(api_operation_slow_threshold_millis(operation));
    state
        .performance
        .record_operation(operation, duration, failed, slow);
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
    let slow = duration.as_millis() >= u128::from(api_task_slow_threshold_millis(task));
    state.performance.record_task(task, duration, failed, slow);
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
    let client_ip = forwarded_client_ip(req.headers());
    if let (Some(route), Some(client_ip)) = (route, client_ip) {
        let allowed = {
            let mut limiter = state.public_telemetry_rate_limiter.lock();
            limiter.allow(&client_ip, route, Instant::now())
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

fn fee_status_for_block(block: &DbBlock) -> &'static str {
    if block.paid_out {
        "missing"
    } else if block.confirmed {
        "ready"
    } else {
        "pending"
    }
}

fn fee_confirmations_remaining(
    block: &DbBlock,
    current_height: u64,
    required_confirmations: u64,
) -> Option<u64> {
    if block.confirmed || required_confirmations == 0 {
        return Some(0);
    }

    let depth = current_height.saturating_sub(block.height);
    Some(required_confirmations.saturating_sub(depth))
}

fn compare_fee_page_items(a: &FeePageItem, b: &FeePageItem, sort: &str) -> std::cmp::Ordering {
    let a_time = system_time_to_unix_secs(a.timestamp);
    let b_time = system_time_to_unix_secs(b.timestamp);

    match sort {
        "time_asc" => a_time
            .cmp(&b_time)
            .then_with(|| a.block_height.cmp(&b.block_height))
            .then_with(|| a.status.cmp(b.status)),
        "amount_desc" => b
            .amount
            .cmp(&a.amount)
            .then_with(|| b_time.cmp(&a_time))
            .then_with(|| b.block_height.cmp(&a.block_height)),
        "amount_asc" => a
            .amount
            .cmp(&b.amount)
            .then_with(|| a_time.cmp(&b_time))
            .then_with(|| a.block_height.cmp(&b.block_height)),
        "height_asc" => a
            .block_height
            .cmp(&b.block_height)
            .then_with(|| a_time.cmp(&b_time))
            .then_with(|| a.status.cmp(b.status)),
        "height_desc" => b
            .block_height
            .cmp(&a.block_height)
            .then_with(|| b_time.cmp(&a_time))
            .then_with(|| a.status.cmp(b.status)),
        _ => b_time
            .cmp(&a_time)
            .then_with(|| b.block_height.cmp(&a.block_height))
            .then_with(|| a.status.cmp(b.status)),
    }
}

fn build_fee_page(
    store: &PoolStore,
    cfg: &Config,
    current_height: u64,
    fee_address_filter: Option<&str>,
    sort: &str,
    limit: usize,
    offset: usize,
) -> anyhow::Result<FeePageData> {
    let fee_events = store.get_all_pool_fees()?;
    let mut total_collected = 0u64;
    let mut recorded_heights = HashSet::<u64>::with_capacity(fee_events.len());
    let mut items = Vec::<FeePageItem>::with_capacity(fee_events.len());

    for fee in fee_events {
        total_collected = total_collected.saturating_add(fee.amount);
        recorded_heights.insert(fee.block_height);
        items.push(FeePageItem {
            block_height: fee.block_height,
            amount: fee.amount,
            fee_address: fee.fee_address,
            timestamp: fee.timestamp,
            status: "collected",
            confirmations_remaining: Some(0),
        });
    }

    let required_confirmations = cfg.blocks_before_payout.max(0) as u64;
    let mut total_pending = 0u64;
    for mut block in store.get_all_blocks()? {
        hydrate_provisional_block_reward(&mut block);
        if block.orphaned || block.reward == 0 || recorded_heights.contains(&block.height) {
            continue;
        }

        let amount = cfg.pool_fee(block.reward);
        if amount == 0 {
            continue;
        }

        total_pending = total_pending.saturating_add(amount);
        items.push(FeePageItem {
            block_height: block.height,
            amount,
            fee_address: resolve_pool_fee_destination_from_address(
                &cfg.pool_wallet_address,
                &block,
            )
            .unwrap_or_default(),
            timestamp: block.timestamp,
            status: fee_status_for_block(&block),
            confirmations_remaining: fee_confirmations_remaining(
                &block,
                current_height,
                required_confirmations,
            ),
        });
    }

    if let Some(filter) = fee_address_filter
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_ascii_lowercase)
    {
        items.retain(|item| item.fee_address.to_ascii_lowercase().contains(&filter));
    }

    items.sort_by(|a, b| compare_fee_page_items(a, b, sort));
    let total = items.len();
    let items = items.into_iter().skip(offset).take(limit).collect();

    Ok(FeePageData {
        total_collected,
        total_pending,
        items,
        total,
    })
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
        database_reachable: bool,
        daemon_syncing: Option<bool>,
        daemon_error: Option<&str>,
        pool_error: Option<&str>,
    ) {
        self.samples.push_back(StatusSample {
            timestamp: now,
            daemon_reachable,
            database_reachable: Some(database_reachable),
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
                    kind: "daemon_down".to_string(),
                    severity: "critical".to_string(),
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

        if !database_reachable {
            if self.open_pool_database_down.is_none() {
                let id = self.next_incident_id;
                self.next_incident_id = self.next_incident_id.saturating_add(1);
                self.open_pool_database_down = Some(OpenIncident {
                    id,
                    kind: "pool_database_down".to_string(),
                    severity: "critical".to_string(),
                    started_at: now,
                    message: pool_error
                        .unwrap_or("pool database unreachable")
                        .to_string(),
                });
            }
        } else if let Some(open) = self.open_pool_database_down.take() {
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
                    kind: "daemon_syncing".to_string(),
                    severity: "warn".to_string(),
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

        self.prune_to_limits(now);
    }

    fn incidents_for_api(&self, now: SystemTime) -> Vec<StatusIncident> {
        let mut out = self.incidents.iter().cloned().collect::<Vec<_>>();
        if let Some(open) = self.open_pool_database_down.clone() {
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

    #[cfg(test)]
    fn pool_uptime_pct(&self, window: Duration, now: SystemTime) -> Option<f64> {
        self.uptime_pct_by(window, now, |sample| {
            sample.daemon_reachable && sample.database_reachable.unwrap_or(true)
        })
    }

    #[cfg(test)]
    fn daemon_uptime_pct(&self, window: Duration, now: SystemTime) -> Option<f64> {
        self.uptime_pct_by(window, now, |sample| sample.daemon_reachable)
    }

    #[cfg(test)]
    fn database_uptime_pct(&self, window: Duration, now: SystemTime) -> Option<f64> {
        self.uptime_pct_by(window, now, |sample| {
            sample.database_reachable.unwrap_or(true)
        })
    }

    #[cfg(test)]
    fn uptime_pct_by<F>(&self, window: Duration, now: SystemTime, is_up: F) -> Option<f64>
    where
        F: Fn(&StatusSample) -> bool,
    {
        let cutoff = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
        let mut total = 0usize;
        let mut up = 0usize;
        for sample in self
            .samples
            .iter()
            .filter(|sample| sample.timestamp >= cutoff)
        {
            total += 1;
            if is_up(sample) {
                up += 1;
            }
        }
        if total == 0 {
            None
        } else {
            Some((up as f64 / total as f64) * 100.0)
        }
    }

    #[cfg(test)]
    fn sample_count_within(&self, window: Duration, now: SystemTime) -> usize {
        let cutoff = now.checked_sub(window).unwrap_or(UNIX_EPOCH);
        self.samples
            .iter()
            .filter(|sample| sample.timestamp >= cutoff)
            .count()
    }

    fn prune_to_limits(&mut self, now: SystemTime) {
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
        while self.incidents.len() > STATUS_MAX_INCIDENTS {
            self.incidents.pop_back();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::env;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::{
        merge_validation_summary, pending_estimate_snapshot_can_serve,
        pending_estimate_snapshot_needs_refresh, public_telemetry_route_kind_for_path,
        validation_summary_from_snapshot, MinerBalanceResponseCache, MinerDetailResponseCache,
        PendingEstimateSnapshotCache, PublicTelemetryRateLimiter, PublicTelemetryRouteKind,
        RejectionAnalyticsCache, StatsResponseCache, MINER_PENDING_ESTIMATE_HOT_WINDOW,
        MINER_PENDING_ESTIMATE_REFRESH_AFTER, PUBLIC_TELEMETRY_MINER_RATE_LIMIT,
        PUBLIC_TELEMETRY_RATE_LIMIT_WINDOW, PUBLIC_TELEMETRY_STATS_RATE_LIMIT,
    };
    use crate::config::Config;
    use crate::db::{
        Balance, DbBlock, MonitorHeartbeat, Payout, PendingPayout, PoolFeeRecord, ShareReplayData,
    };
    use crate::engine::{ShareRecord, ShareStore};
    use crate::jobs::{JobManager, JobRuntimeSnapshot};
    use crate::node::{NodeClient, WalletBalance, WalletOutput};
    use crate::recovery::RecoveryAgentClient;
    use crate::service_state::{
        PersistedPayoutRuntime, PersistedRuntimeSnapshot, PersistedValidationSummary,
    };
    use crate::stats::PoolStats;
    use crate::store::PoolStore;
    use crate::validation::{
        PersistedValidationAddressState, ValidationEngine, ValidationSnapshot, ValidationStateStore,
    };
    use axum::body::to_bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::{header, Method, StatusCode, Uri};
    use axum::response::IntoResponse;
    use axum::Json;
    use base64::Engine as _;
    use pool_common::pow::Argon2PowHasher;
    use serde::Serialize;
    use tempfile::tempdir;

    use super::{
        api_performance_route_name, apply_wallet_liquidity_to_payout_eta, batch_payouts,
        block_page_item_response, build_block_reward_breakdown, build_fee_page, contains_ci,
        daemon_debug_log_path, daemon_health_from_heartbeat, daemon_log_commands,
        daemon_send_idempotency_path, estimate_unconfirmed_pending_for_miner,
        estimated_block_reward, filter_active_workers_for_miner, format_atomic_bnt,
        format_atomic_fee, format_bnt, handle_admin_clear_address_risk_history,
        handle_admin_dev_fee, handle_admin_share_diagnostics, handle_app_fallback, handle_health,
        handle_miner, handle_miners, handle_stats, hashrate_from_stats_with_miner_ramp,
        hashrate_from_stats_with_warmup, hydrate_provisional_block_reward, is_api_request_path,
        load_confirmed_payout_import_txs, load_persisted_status_history,
        luck_round_response_from_db, miner_balance_response, miner_has_activity, page_bounds,
        payout_status_note, pending_balance_note, rejection_window_duration, share_limit,
        sort_workers_for_miner, summarize_admin_balance_outputs, system_time_to_unix_secs,
        trim_log_line, worker_hashrate_by_name, ApiPerformanceTracker, ApiState,
        ClearAddressRiskHistoryRequest, DaemonHealthCache, DbTotalsCache, InsightsCache,
        LiveRuntimeSnapshotCache, MinerDetailQuery, MinerPendingBlockEstimate,
        MinerPendingEstimate, MinersQuery, NetworkHashrateCache, OpenIncident, PayoutEtaResponse,
        PoolHealthCache, StatusHistory, StatusIncident, BASE64_STANDARD, DAEMON_LOG_LINE_LIMIT,
        HASHRATE_BRAND_NEW_MIN_WINDOW, HASHRATE_WARMUP_WINDOW, HASHRATE_WINDOW,
        LIVE_RUNTIME_SNAPSHOT_META_KEY, STATUS_HISTORY_META_KEY, UI_ASSET_OG_IMAGE_PNG,
        UI_ASSET_OG_IMAGE_SVG,
    };

    const TEST_POSTGRES_URL_ENV: &str = "BLOCKNET_POOL_TEST_POSTGRES_URL";

    fn test_store() -> Option<Arc<PoolStore>> {
        let url = env::var(TEST_POSTGRES_URL_ENV).ok()?;
        match PoolStore::open_postgres_with_pool(&url, 2) {
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

    fn test_api_state(store: Arc<PoolStore>) -> ApiState {
        let mut cfg = Config::default();
        cfg.max_verifiers = 1;
        let runtime_cfg = cfg.to_runtime_config();
        let node =
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("build test node client"));
        let jobs = JobManager::new(Arc::clone(&node), runtime_cfg.clone());
        let validation = Arc::new(ValidationEngine::new(
            runtime_cfg,
            Arc::new(Argon2PowHasher::default()),
        ));
        ApiState {
            config: cfg.clone(),
            store,
            stats: Arc::new(PoolStats::new()),
            jobs,
            node,
            validation: Some(validation),
            db_totals_cache: Arc::new(parking_lot::Mutex::new(DbTotalsCache::default())),
            daemon_health_cache: Arc::new(parking_lot::Mutex::new(DaemonHealthCache::default())),
            pool_health_cache: Arc::new(parking_lot::Mutex::new(PoolHealthCache::default())),
            network_hashrate_cache: Arc::new(parking_lot::Mutex::new(
                NetworkHashrateCache::default(),
            )),
            insights_cache: Arc::new(parking_lot::Mutex::new(InsightsCache::default())),
            rejection_analytics_cache: Arc::new(parking_lot::Mutex::new(
                RejectionAnalyticsCache::default(),
            )),
            stats_response_cache: Arc::new(parking_lot::Mutex::new(StatsResponseCache::default())),
            pending_estimate_snapshot_cache: Arc::new(parking_lot::Mutex::new(
                PendingEstimateSnapshotCache::default(),
            )),
            pending_estimate_snapshot_notify: Arc::new(tokio::sync::Notify::new()),
            miner_balance_response_cache: Arc::new(parking_lot::Mutex::new(
                MinerBalanceResponseCache::default(),
            )),
            miner_detail_response_cache: Arc::new(parking_lot::Mutex::new(
                MinerDetailResponseCache::default(),
            )),
            public_telemetry_rate_limiter: Arc::new(parking_lot::Mutex::new(
                PublicTelemetryRateLimiter::default(),
            )),
            performance: Arc::new(ApiPerformanceTracker::default()),
            recovery: Arc::new(RecoveryAgentClient::new(cfg.recovery.socket_path.clone())),
            live_runtime_snapshot_cache: Arc::new(parking_lot::Mutex::new(
                LiveRuntimeSnapshotCache::default(),
            )),
            status_history: Arc::new(parking_lot::Mutex::new(StatusHistory::default())),
            sse_subscriber_limiter: Arc::new(tokio::sync::Semaphore::new(1)),
            api_key: cfg.api_key.clone(),
            pool_name: cfg.pool_name.clone(),
            pool_url: cfg.pool_url.clone(),
            stratum_port: cfg.stratum_port,
            pool_fee_pct: cfg.pool_fee_pct,
            pool_fee_flat: cfg.pool_fee_flat,
            min_payout_amount: cfg.min_payout_amount,
            blocks_before_payout: cfg.blocks_before_payout,
            payout_scheme: cfg.payout_scheme.clone(),
            started_at: Instant::now(),
            started_at_system: SystemTime::now(),
        }
    }

    fn test_api_state_without_validation(store: Arc<PoolStore>) -> ApiState {
        let mut state = test_api_state(store);
        state.validation = None;
        state
    }

    #[test]
    fn api_performance_route_name_classifies_hot_routes() {
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/stats")),
            Some("stats")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static("/api/miner/test/balance")),
            Some("miner_balance")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static(
                "/api/miner/test/balance?include_pending_estimate=true",
            )),
            Some("miner_balance_pending")
        );
        assert_eq!(
            api_performance_route_name(&Uri::from_static(
                "/api/miner/test?include_pending_estimate=true",
            )),
            Some("miner_detail_pending")
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
            api_performance_route_name(&Uri::from_static("/api/events")),
            None
        );
    }

    #[test]
    fn summarize_admin_balance_outputs_tracks_live_wallet_buckets() {
        let outputs = vec![
            WalletOutput {
                txid: "coinbase-unspent".to_string(),
                output_index: 0,
                amount: 25,
                status: "unspent".to_string(),
                r#type: "coinbase".to_string(),
                confirmations: 60,
                block_height: 10,
                spent_height: None,
                one_time_pub: String::new(),
                commitment: String::new(),
            },
            WalletOutput {
                txid: "regular-unspent".to_string(),
                output_index: 1,
                amount: 10,
                status: "unspent".to_string(),
                r#type: "regular".to_string(),
                confirmations: 12,
                block_height: 11,
                spent_height: None,
                one_time_pub: String::new(),
                commitment: String::new(),
            },
            WalletOutput {
                txid: "payout-change".to_string(),
                output_index: 1,
                amount: 7,
                status: "pending".to_string(),
                r#type: "regular".to_string(),
                confirmations: 2,
                block_height: 12,
                spent_height: None,
                one_time_pub: String::new(),
                commitment: String::new(),
            },
            WalletOutput {
                txid: "immature-coinbase".to_string(),
                output_index: 0,
                amount: 12,
                status: "pending".to_string(),
                r#type: "coinbase".to_string(),
                confirmations: 8,
                block_height: 13,
                spent_height: None,
                one_time_pub: String::new(),
                commitment: String::new(),
            },
            WalletOutput {
                txid: "mystery-regular".to_string(),
                output_index: 1,
                amount: 5,
                status: "pending".to_string(),
                r#type: "regular".to_string(),
                confirmations: 1,
                block_height: 14,
                spent_height: None,
                one_time_pub: String::new(),
                commitment: String::new(),
            },
            WalletOutput {
                txid: "historical-spent".to_string(),
                output_index: 0,
                amount: 99,
                status: "spent".to_string(),
                r#type: "coinbase".to_string(),
                confirmations: 100,
                block_height: 1,
                spent_height: Some(2),
                one_time_pub: String::new(),
                commitment: String::new(),
            },
        ];

        let matched = std::collections::HashSet::from(["payout-change".to_string()]);
        let summary = summarize_admin_balance_outputs(&outputs, &matched);

        assert_eq!(summary.live_count, 5);
        assert_eq!(summary.spendable_count, 2);
        assert_eq!(summary.pending_count, 3);
        assert_eq!(summary.spendable_coinbase_count, 1);
        assert_eq!(summary.spendable_coinbase_amount, 25);
        assert_eq!(summary.spendable_regular_count, 1);
        assert_eq!(summary.spendable_regular_amount, 10);
        assert_eq!(summary.pending_coinbase_count, 1);
        assert_eq!(summary.pending_coinbase_amount, 12);
        assert_eq!(summary.pending_regular_count, 2);
        assert_eq!(summary.pending_regular_amount, 12);
        assert_eq!(summary.pending_regular_matched_payout_count, 1);
        assert_eq!(summary.pending_regular_matched_payout_amount, 7);
        assert_eq!(summary.pending_regular_unmatched_count, 1);
        assert_eq!(summary.pending_regular_unmatched_amount, 5);
    }

    #[test]
    fn bnt_formatters_insert_grouping() {
        assert_eq!(format_atomic_bnt(932_854_000_000), "9,328.54 BNT");
        assert_eq!(format_atomic_fee(2_845_992_000_000), "28,459.9200 BNT");
        assert_eq!(format_bnt(1_138_396.96), "1,138,396.96 BNT");
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
            Some(PublicTelemetryRouteKind::MinerBalance)
        ));
        assert!(matches!(
            public_telemetry_route_kind_for_path("/api/miner/test-address"),
            Some(PublicTelemetryRouteKind::MinerDetail)
        ));
        assert!(public_telemetry_route_kind_for_path("/api/miner/test-address/hashrate").is_some());
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
            assert!(limiter.allow(
                "203.0.113.11",
                PublicTelemetryRouteKind::MinerBalance,
                start,
            ));
        }
        assert!(!limiter.allow("203.0.113.11", PublicTelemetryRouteKind::MinerDetail, start,));
    }

    #[test]
    fn app_fallback_returns_json_404_for_unknown_api_paths() {
        let store = require_test_store!();
        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        let response = runtime.block_on(handle_app_fallback(
            Method::GET,
            "/api/does-not-exist".parse().expect("uri"),
            State(state),
        ));

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert!(content_type.starts_with("application/json"));

        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode json");
        assert_eq!(payload["error"], "not found");
    }

    #[test]
    fn app_fallback_renders_ui_for_unknown_spa_paths() {
        let store = require_test_store!();
        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        let response = runtime.block_on(handle_app_fallback(
            Method::GET,
            "/admin".parse().expect("uri"),
            State(state),
        ));

        assert_eq!(response.status(), StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let html = String::from_utf8(body.to_vec()).expect("utf8 html");
        assert!(html.contains("Admin dashboard"));
    }

    #[test]
    fn build_fee_page_includes_collected_and_pending_rows() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.pool_fee_pct = 10.0;
        cfg.blocks_before_payout = 60;
        cfg.pool_wallet_address = "cold-wallet".to_string();

        let base = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let collected_reward = 100_000_000;
        let pending_reward = 200_000_000;
        let ready_reward = 300_000_000;
        let missing_reward = 400_000_000;

        store
            .add_block(&DbBlock {
                height: 100,
                hash: "collected".to_string(),
                difficulty: 1,
                finder: "finder-collected".to_string(),
                finder_worker: "worker-collected".to_string(),
                reward: collected_reward,
                timestamp: base,
                confirmed: true,
                orphaned: false,
                paid_out: true,
                effort_pct: None,
            })
            .expect("add collected block");
        store
            .record_pool_fee(
                100,
                cfg.pool_fee(collected_reward),
                &cfg.pool_wallet_address,
                base,
            )
            .expect("record collected fee");

        store
            .add_block(&DbBlock {
                height: 110,
                hash: "pending".to_string(),
                difficulty: 1,
                finder: "finder-pending".to_string(),
                finder_worker: "worker-pending".to_string(),
                reward: pending_reward,
                timestamp: base + Duration::from_secs(60),
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add pending block");
        store
            .add_block(&DbBlock {
                height: 120,
                hash: "ready".to_string(),
                difficulty: 1,
                finder: "finder-ready".to_string(),
                finder_worker: "worker-ready".to_string(),
                reward: ready_reward,
                timestamp: base + Duration::from_secs(120),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add ready block");
        store
            .add_block(&DbBlock {
                height: 130,
                hash: "missing".to_string(),
                difficulty: 1,
                finder: "finder-missing".to_string(),
                finder_worker: "worker-missing".to_string(),
                reward: missing_reward,
                timestamp: base + Duration::from_secs(180),
                confirmed: true,
                orphaned: false,
                paid_out: true,
                effort_pct: None,
            })
            .expect("add missing block");
        store
            .add_block(&DbBlock {
                height: 140,
                hash: "orphaned".to_string(),
                difficulty: 1,
                finder: "finder-orphaned".to_string(),
                finder_worker: "worker-orphaned".to_string(),
                reward: 500_000_000,
                timestamp: base + Duration::from_secs(240),
                confirmed: false,
                orphaned: true,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add orphaned block");

        let page = build_fee_page(store.as_ref(), &cfg, 150, None, "time_desc", 10, 0)
            .expect("build fee page");

        assert_eq!(page.total, 4);
        assert_eq!(page.total_collected, cfg.pool_fee(collected_reward));
        assert_eq!(
            page.total_pending,
            cfg.pool_fee(pending_reward)
                .saturating_add(cfg.pool_fee(ready_reward))
                .saturating_add(cfg.pool_fee(missing_reward))
        );

        let pending = page
            .items
            .iter()
            .find(|item| item.block_height == 110)
            .expect("pending row");
        assert_eq!(pending.status, "pending");
        assert_eq!(pending.confirmations_remaining, Some(20));
        assert_eq!(pending.fee_address, "cold-wallet");

        let ready = page
            .items
            .iter()
            .find(|item| item.block_height == 120)
            .expect("ready row");
        assert_eq!(ready.status, "ready");
        assert_eq!(ready.confirmations_remaining, Some(0));

        let missing = page
            .items
            .iter()
            .find(|item| item.block_height == 130)
            .expect("missing row");
        assert_eq!(missing.status, "missing");
        assert_eq!(missing.confirmations_remaining, Some(0));

        let collected = page
            .items
            .iter()
            .find(|item| item.block_height == 100)
            .expect("collected row");
        assert_eq!(collected.status, "collected");
    }

    #[test]
    fn build_fee_page_hydrates_provisional_rewards_for_pending_blocks() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.pool_fee_pct = 10.0;
        cfg.blocks_before_payout = 60;
        cfg.pool_wallet_address = "cold-wallet".to_string();

        let base = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let height = 3707;
        let expected_reward = estimated_block_reward(height);
        let expected_fee = cfg.pool_fee(expected_reward);

        store
            .add_block(&DbBlock {
                height,
                hash: "pending-zero-reward".to_string(),
                difficulty: 1,
                finder: "finder-pending".to_string(),
                finder_worker: "worker-pending".to_string(),
                reward: 0,
                timestamp: base,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add pending block");

        let page = build_fee_page(store.as_ref(), &cfg, height + 5, None, "time_desc", 10, 0)
            .expect("build fee page");

        assert_eq!(page.total, 1);
        assert_eq!(page.total_collected, 0);
        assert_eq!(page.total_pending, expected_fee);

        let pending = page.items.first().expect("pending row");
        assert_eq!(pending.block_height, height);
        assert_eq!(pending.amount, expected_fee);
        assert_eq!(pending.status, "pending");
        assert_eq!(pending.confirmations_remaining, Some(55));
        assert_eq!(pending.fee_address, "cold-wallet");
    }

    #[test]
    fn pending_balance_note_explains_zero_while_hashing() {
        let cfg = Config::default();
        let empty = MinerPendingEstimate::default();
        let note = pending_balance_note(&cfg, 150.0, 20, &empty);
        assert!(note.is_some());
        assert!(note
            .expect("note")
            .contains("Pending stays at 0 until the pool finds and credits blocks"));
        assert!(pending_balance_note(&cfg, 0.0, 0, &empty).is_none());

        let waiting = MinerPendingEstimate {
            estimated_pending: 0,
            blocks: vec![MinerPendingBlockEstimate {
                height: 1,
                hash: "blk".to_string(),
                reward: 1_000,
                estimated_credit: 0,
                credit_withheld: false,
                validation_state: "awaiting_ratio".to_string(),
                validation_label: "Waiting for ratio".to_string(),
                validation_tone: "warn".to_string(),
                validation_detail: "detail".to_string(),
                confirmations_remaining: 59,
                timestamp: UNIX_EPOCH,
            }],
        };
        let threshold_note =
            pending_balance_note(&cfg, 150.0, 20, &waiting).expect("threshold note");
        assert!(threshold_note.contains("separate from confirmed balance"));
        assert!(threshold_note.contains("caps provisional difficulty"));

        let mut ratio_cfg = Config::default();
        ratio_cfg.payout_min_verified_ratio = 0.5;
        ratio_cfg.payout_provisional_cap_multiplier = 0.0;
        let ratio_note = pending_balance_note(&ratio_cfg, 150.0, 20, &waiting).expect("ratio note");
        assert!(ratio_note.contains("50% verified difficulty"));

        let ready = MinerPendingEstimate {
            estimated_pending: 10,
            blocks: vec![MinerPendingBlockEstimate {
                height: 2,
                hash: "blk-ready".to_string(),
                reward: 1_000,
                estimated_credit: 10,
                credit_withheld: false,
                validation_state: "ready".to_string(),
                validation_label: "Ready".to_string(),
                validation_tone: "ok".to_string(),
                validation_detail: "detail".to_string(),
                confirmations_remaining: 59,
                timestamp: UNIX_EPOCH,
            }],
        };
        let estimate_note = pending_balance_note(&cfg, 150.0, 20, &ready).expect("estimate note");
        assert!(estimate_note.contains("can still move until each block reaches"));
    }

    #[test]
    fn pending_balance_note_explains_verified_only_during_extra_verification() {
        let cfg = Config::default();
        let extra_verification = MinerPendingEstimate {
            estimated_pending: 10,
            blocks: vec![MinerPendingBlockEstimate {
                height: 3,
                hash: "blk-verify".to_string(),
                reward: 1_000,
                estimated_credit: 10,
                credit_withheld: false,
                validation_state: "extra_verification".to_string(),
                validation_label: "Verified only".to_string(),
                validation_tone: "warn".to_string(),
                validation_detail: "detail".to_string(),
                confirmations_remaining: 59,
                timestamp: UNIX_EPOCH,
            }],
        };

        let note = pending_balance_note(&cfg, 150.0, 20, &extra_verification).expect("note");
        assert!(note.contains("only fully verified shares count"));
        assert!(note.contains("completed payouts are unaffected"));
    }

    #[test]
    fn status_history_persists_via_meta_store() {
        let store = require_test_store!();
        let base = SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .expect("recent base time");
        let mut history = StatusHistory::default();
        history.record_sample(
            base,
            false,
            true,
            Some(false),
            Some("daemon unreachable"),
            None,
        );
        history.record_sample(
            base + Duration::from_secs(30),
            true,
            true,
            Some(false),
            None,
            None,
        );

        let payload = serde_json::to_vec(&history).expect("serialize status history");
        store
            .set_meta(STATUS_HISTORY_META_KEY, &payload)
            .expect("persist status history");

        let loaded = load_persisted_status_history(store.as_ref()).expect("load status history");
        let now = base + Duration::from_secs(30);
        let incidents = loaded.incidents_for_api(now);
        assert_eq!(
            loaded.sample_count_within(Duration::from_secs(3600), now),
            2
        );
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].kind, "daemon_down");
        assert!(!incidents[0].ongoing);
        assert_eq!(
            loaded.pool_uptime_pct(Duration::from_secs(3600), now),
            Some(50.0)
        );
    }

    #[test]
    fn status_history_loads_legacy_samples_without_database_health() {
        #[derive(Serialize)]
        struct LegacyStatusSample {
            timestamp: SystemTime,
            daemon_reachable: bool,
        }

        #[derive(Serialize)]
        struct LegacyStatusHistory {
            samples: VecDeque<LegacyStatusSample>,
            incidents: VecDeque<StatusIncident>,
            open_daemon_down: Option<OpenIncident>,
            open_daemon_syncing: Option<OpenIncident>,
            next_incident_id: u64,
        }

        let base = UNIX_EPOCH + Duration::from_secs(1_000);
        let legacy = LegacyStatusHistory {
            samples: VecDeque::from([
                LegacyStatusSample {
                    timestamp: base,
                    daemon_reachable: true,
                },
                LegacyStatusSample {
                    timestamp: base + Duration::from_secs(30),
                    daemon_reachable: true,
                },
            ]),
            incidents: VecDeque::new(),
            open_daemon_down: None,
            open_daemon_syncing: None,
            next_incident_id: 7,
        };

        let loaded: StatusHistory =
            serde_json::from_slice(&serde_json::to_vec(&legacy).expect("serialize legacy history"))
                .expect("deserialize upgraded history");

        let now = base + Duration::from_secs(30);
        assert_eq!(
            loaded.database_uptime_pct(Duration::from_secs(3600), now),
            Some(100.0)
        );
        assert_eq!(
            loaded.pool_uptime_pct(Duration::from_secs(3600), now),
            Some(100.0)
        );
    }

    #[test]
    fn status_history_records_database_outages_as_incidents() {
        let base = UNIX_EPOCH + Duration::from_secs(2_000);
        let mut history = StatusHistory::default();
        history.record_sample(
            base,
            true,
            false,
            Some(false),
            None,
            Some("connection closed"),
        );
        history.record_sample(
            base + Duration::from_secs(30),
            true,
            true,
            Some(false),
            None,
            None,
        );

        let now = base + Duration::from_secs(30);
        let incidents = history.incidents_for_api(now);
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].kind, "pool_database_down");
        assert_eq!(incidents[0].message, "connection closed");
        assert_eq!(
            history.daemon_uptime_pct(Duration::from_secs(3600), now),
            Some(100.0)
        );
        assert_eq!(
            history.database_uptime_pct(Duration::from_secs(3600), now),
            Some(50.0)
        );
        assert_eq!(
            history.pool_uptime_pct(Duration::from_secs(3600), now),
            Some(50.0)
        );
    }

    #[test]
    fn daemon_health_from_heartbeat_uses_process_block_details() {
        let heartbeat = MonitorHeartbeat {
            id: 1,
            sampled_at: UNIX_EPOCH + Duration::from_secs(10),
            source: "local".to_string(),
            synthetic: false,
            api_up: Some(true),
            stratum_up: Some(true),
            db_up: true,
            daemon_up: Some(true),
            public_http_up: None,
            daemon_syncing: Some(false),
            chain_height: Some(44),
            template_age_seconds: None,
            last_refresh_millis: None,
            stratum_snapshot_age_seconds: None,
            connected_miners: None,
            connected_workers: None,
            estimated_hashrate: None,
            wallet_up: Some(true),
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
            summary_state: "healthy".to_string(),
            details_json: Some(
                serde_json::json!({
                    "daemon_error": "daemon probe timed out",
                    "daemon_current_process_block": {
                        "height": 45,
                        "tx_count": 1,
                        "stage": "validate",
                        "started_at_unix_millis": 15_000,
                        "stage_started_at_unix_millis": 15_000,
                        "elapsed_millis": 12_000,
                        "stage_elapsed_millis": 12_000
                    },
                    "daemon_last_process_block": {
                        "height": 44,
                        "tx_count": 2,
                        "completed_at_unix_millis": 14_000,
                        "validate_millis": 8_000,
                        "commit_millis": 500,
                        "reorg_millis": 100,
                        "total_millis": 8_600,
                        "accepted": true,
                        "main_chain": true,
                        "error": ""
                    }
                })
                .to_string(),
            ),
        };

        let health = daemon_health_from_heartbeat(Some(&heartbeat));
        assert_eq!(health.chain_height, Some(44));
        assert_eq!(
            health
                .current_process_block
                .as_ref()
                .map(|block| (&block.stage, block.elapsed_millis)),
            Some((&"validate".to_string(), 12_000))
        );
        assert_eq!(
            health
                .last_process_block
                .as_ref()
                .map(|block| block.total_millis),
            Some(8_600)
        );

        let failed = MonitorHeartbeat {
            daemon_up: Some(false),
            ..heartbeat
        };
        let failed_health = daemon_health_from_heartbeat(Some(&failed));
        assert_eq!(
            failed_health.error.as_deref(),
            Some("daemon probe timed out")
        );
    }

    #[test]
    fn health_handler_uses_persisted_job_snapshot_when_live_assignments_are_empty() {
        let store = require_test_store!();
        let snapshot = PersistedRuntimeSnapshot {
            sampled_at: SystemTime::now(),
            total_shares_accepted: 0,
            connected_miners: 0,
            connected_workers: 0,
            estimated_hashrate: 0.0,
            last_share_at: None,
            jobs: JobRuntimeSnapshot {
                current_height: Some(777),
                current_difficulty: Some(55),
                template_id: Some("tmpl-stratum".to_string()),
                template_age_seconds: Some(9),
                last_refresh_millis: Some(321),
                tracked_templates: 4,
                active_assignments: 12,
            },
            payouts: PersistedPayoutRuntime::default(),
            submit: Default::default(),
            validation: PersistedValidationSummary::default(),
            runtime_tasks: BTreeMap::new(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let response = runtime
            .block_on(handle_health(State(state)))
            .into_response();
        let bytes = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("health response json");

        assert_eq!(payload["job"]["current_height"], 777);
        assert_eq!(payload["job"]["current_difficulty"], 55);
        assert_eq!(payload["job"]["template_id"], "tmpl-stratum");
        assert_eq!(payload["job"]["template_age_seconds"], 9);
        assert_eq!(payload["job"]["last_refresh_millis"], 321);
        assert_eq!(payload["job"]["tracked_templates"], 4);
        assert_eq!(payload["job"]["active_assignments"], 12);
        assert_eq!(payload["pool_activity"]["state"], "idle");
        assert_eq!(payload["pool_activity"]["connected_miners"], 0);
    }

    #[test]
    fn effective_validation_summary_uses_persisted_snapshot_without_live_validator() {
        let store = require_test_store!();
        let snapshot = PersistedRuntimeSnapshot {
            sampled_at: SystemTime::now(),
            total_shares_accepted: 0,
            connected_miners: 0,
            connected_workers: 0,
            estimated_hashrate: 0.0,
            last_share_at: None,
            jobs: JobRuntimeSnapshot::default(),
            payouts: PersistedPayoutRuntime::default(),
            submit: Default::default(),
            validation: PersistedValidationSummary {
                candidate_queue_depth: 7,
                regular_queue_depth: 11,
                tracked_addresses: 5,
                effective_sample_rate: 0.25,
                ..PersistedValidationSummary::default()
            },
            runtime_tasks: BTreeMap::new(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state_without_validation(store);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let summary = runtime.block_on(state.effective_validation_summary());
        assert_eq!(summary.candidate_queue_depth, 7);
        assert_eq!(summary.regular_queue_depth, 11);
        assert_eq!(summary.tracked_addresses, 5);
        assert_eq!(summary.effective_sample_rate, 0.25);
    }

    #[test]
    fn clear_risk_history_succeeds_without_live_validator() {
        let store = require_test_store!();
        store
            .escalate_address_risk(
                "no-live-validator",
                "manual review",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("seed risk state");

        let state = test_api_state_without_validation(Arc::clone(&store));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let response = runtime
            .block_on(handle_admin_clear_address_risk_history(
                State(state),
                Json(ClearAddressRiskHistoryRequest {
                    address: "no-live-validator".to_string(),
                }),
            ))
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
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
            .escalate_address_risk(
                "risk-addr",
                "invalid share proof",
                Duration::from_secs(60),
                1,
                Duration::from_secs(120),
                Duration::from_secs(120),
                Duration::from_secs(600),
            )
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
                hold_cause: Some(crate::db::ValidationHoldCause::InvalidSamples),
                last_seen_at: now,
            })
            .expect("persist validation state");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let response = runtime
            .block_on(handle_health(State(state)))
            .into_response();
        let bytes = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("health response json");
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
    fn admin_dev_fee_handler_reports_window_and_hint_diagnostics() {
        let store = require_test_store!();
        let now = SystemTime::now();
        let dev = crate::dev_fee::SEINE_DEV_FEE_ADDRESS;

        store
            .add_share(crate::engine::ShareRecord {
                job_id: "dev-ok".to_string(),
                miner: dev.to_string(),
                worker: "seine-devfee-a".to_string(),
                difficulty: 90,
                nonce: 1,
                status: crate::validation::SHARE_STATUS_VERIFIED,
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: now,
            })
            .expect("insert accepted dev share");
        store
            .add_share(crate::engine::ShareRecord {
                job_id: "dev-stale".to_string(),
                miner: dev.to_string(),
                worker: "seine-devfee-a".to_string(),
                difficulty: 30,
                nonce: 2,
                status: crate::validation::SHARE_STATUS_REJECTED,
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("stale job".to_string()),
                created_at: now,
            })
            .expect("insert rejected dev share");
        store
            .add_share(crate::engine::ShareRecord {
                job_id: "pool-ok".to_string(),
                miner: "other-miner".to_string(),
                worker: "rig-1".to_string(),
                difficulty: 9000,
                nonce: 3,
                status: crate::validation::SHARE_STATUS_VERIFIED,
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: now,
            })
            .expect("insert accepted pool share");
        store
            .upsert_vardiff_hint(dev, "seine-devfee-a", 60, now)
            .expect("hint a");
        store
            .upsert_vardiff_hint(dev, "seine-devfee-b", 120, now)
            .expect("hint b");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_admin_dev_fee(State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("dev fee response json");

        assert_eq!(payload["address"], dev);
        assert_eq!(payload["reference_target_pct"], 1.0);
        assert_eq!(payload["hint_floor"], 60);

        let windows = payload["windows"].as_array().expect("window rows");
        let one_hour = windows
            .iter()
            .find(|row| row["label"] == "1h")
            .expect("1h row");
        assert_eq!(one_hour["dev_accepted_difficulty"], 90);
        assert_eq!(one_hour["dev_rejected_difficulty"], 30);
        assert_eq!(one_hour["stale_rejected_shares"], 1);
        assert_eq!(one_hour["accepted_shares"], 1);
        assert_eq!(one_hour["rejected_shares"], 1);

        assert_eq!(payload["hints"]["total_workers"], 2);
        assert_eq!(payload["hints"]["at_floor_workers"], 1);
        assert_eq!(payload["hints"]["above_floor_workers"], 1);
        assert_eq!(payload["hints"]["median_difficulty"], 90);

        let recent = payload["recent_hints"].as_array().expect("recent hints");
        assert_eq!(recent.len(), 2);
    }

    #[test]
    fn admin_share_diagnostics_reports_windows_and_runtime_pressure() {
        let store = require_test_store!();
        let now = SystemTime::now();

        store
            .add_share(ShareRecord {
                job_id: "share-ok".to_string(),
                miner: "miner-a".to_string(),
                worker: "rig-1".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: now,
            })
            .expect("insert accepted share");
        store
            .add_share(ShareRecord {
                job_id: "share-invalid".to_string(),
                miner: "miner-a".to_string(),
                worker: "rig-1".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "rejected",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("invalid share proof".to_string()),
                created_at: now,
            })
            .expect("insert invalid share");
        store
            .add_share(ShareRecord {
                job_id: "share-quarantine".to_string(),
                miner: "miner-a".to_string(),
                worker: "rig-1".to_string(),
                difficulty: 100,
                nonce: 3,
                status: "rejected",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("address quarantined".to_string()),
                created_at: now,
            })
            .expect("insert quarantine share");

        let snapshot = PersistedRuntimeSnapshot {
            sampled_at: now,
            total_shares_accepted: 1,
            connected_miners: 4,
            connected_workers: 7,
            estimated_hashrate: 42.5,
            last_share_at: Some(now),
            jobs: JobRuntimeSnapshot {
                current_height: Some(999),
                current_difficulty: Some(123),
                template_id: Some("tmpl-share-tab".to_string()),
                template_age_seconds: Some(6),
                last_refresh_millis: Some(1200),
                tracked_templates: 3,
                active_assignments: 14,
            },
            payouts: PersistedPayoutRuntime::default(),
            submit: Default::default(),
            validation: PersistedValidationSummary {
                in_flight: 2,
                candidate_queue_depth: 5,
                regular_queue_depth: 9,
                tracked_addresses: 3,
                forced_verify_addresses: 1,
                total_shares: 20,
                sampled_shares: 4,
                invalid_samples: 1,
                pending_provisional: 8,
                fraud_detections: 0,
                ..PersistedValidationSummary::default()
            },
            runtime_tasks: BTreeMap::new(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_admin_share_diagnostics(State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("share diagnostics json");

        let windows = payload["windows"].as_array().expect("window rows");
        let five_min = windows
            .iter()
            .find(|row| row["label"] == "5m")
            .expect("5m row");
        assert_eq!(five_min["accepted"], 1);
        assert_eq!(five_min["rejected"], 2);
        assert_eq!(five_min["total"], 3);
        assert_eq!(five_min["by_reason"][0]["reason"], "address quarantined");
        assert_eq!(five_min["by_reason"][0]["count"], 1);

        assert_eq!(payload["validation"]["candidate_queue_depth"], 5);
        assert_eq!(payload["validation"]["regular_queue_depth"], 9);
        assert_eq!(payload["job"]["active_assignments"], 14);
        assert_eq!(payload["pool_activity"]["connected_workers"], 7);
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
            total_shares_accepted: 74,
            connected_miners: 3,
            connected_workers: 4,
            estimated_hashrate: 12.5,
            last_share_at: Some(now),
            jobs: JobRuntimeSnapshot::default(),
            payouts: PersistedPayoutRuntime::default(),
            submit: Default::default(),
            validation: PersistedValidationSummary {
                hot_accepts: 74,
                audit_enqueued: 13,
                audit_verified: 13,
                audit_duration: crate::telemetry::PercentileSummary {
                    samples: 13,
                    p50_millis: Some(1404),
                    p95_millis: Some(1502),
                },
                tracked_addresses: 1,
                total_shares: 74,
                pending_provisional: 12,
                ..PersistedValidationSummary::default()
            },
            runtime_tasks: BTreeMap::new(),
        };
        store
            .set_meta(
                LIVE_RUNTIME_SNAPSHOT_META_KEY,
                &serde_json::to_vec(&snapshot).expect("serialize runtime snapshot"),
            )
            .expect("persist runtime snapshot");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_admin_share_diagnostics(State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("body bytes");
        let payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("share diagnostics json");

        assert_eq!(payload["validation"]["hot_accepts"], 74);
        assert_eq!(payload["validation"]["audit_enqueued"], 13);
        assert_eq!(payload["validation"]["audit_verified"], 13);
        assert_eq!(payload["validation"]["audit_duration"]["samples"], 13);
        assert_eq!(payload["validation"]["audit_duration"]["p50_millis"], 1404);
    }

    #[test]
    fn merge_validation_summary_keeps_sample_rate_consistent_with_selected_mode() {
        let mut live = validation_summary_from_snapshot(ValidationSnapshot::default());
        live.overload_mode = crate::validation::OverloadMode::Normal;
        live.effective_sample_rate = 0.10;

        let merged = merge_validation_summary(
            live.clone(),
            &PersistedValidationSummary {
                overload_mode: crate::validation::OverloadMode::Emergency,
                effective_sample_rate: 0.10,
                ..PersistedValidationSummary::default()
            },
        );
        assert_eq!(
            merged.overload_mode,
            crate::validation::OverloadMode::Emergency
        );
        assert_eq!(merged.effective_sample_rate, 0.0);

        live.overload_mode = crate::validation::OverloadMode::Shed;
        live.effective_sample_rate = 0.01;
        let merged = merge_validation_summary(
            live,
            &PersistedValidationSummary {
                overload_mode: crate::validation::OverloadMode::Normal,
                effective_sample_rate: 0.10,
                ..PersistedValidationSummary::default()
            },
        );
        assert_eq!(merged.overload_mode, crate::validation::OverloadMode::Shed);
        assert_eq!(merged.effective_sample_rate, 0.01);
    }

    #[test]
    fn stats_handler_uses_db_backed_share_totals() {
        let store = require_test_store!();
        let now = SystemTime::now();
        store
            .add_share(ShareRecord {
                job_id: "job-accepted".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: now,
            })
            .expect("add accepted share");
        store
            .add_share(ShareRecord {
                job_id: "job-rejected".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "rejected",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("bad hash".to_string()),
                created_at: now,
            })
            .expect("add rejected share");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime.block_on(handle_stats(State(state))).into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode stats json");

        assert_eq!(payload["pool"]["shares_accepted"], 1);
        assert_eq!(payload["pool"]["shares_rejected"], 1);
        assert_eq!(payload["pool"]["total_shares"], 2);
    }

    #[test]
    fn miners_handler_includes_db_only_miners_after_restart() {
        let store = require_test_store!();
        let now = SystemTime::now();
        store
            .add_share(ShareRecord {
                job_id: "job-verified".to_string(),
                miner: "miner-db-only".to_string(),
                worker: "worker-1".to_string(),
                difficulty: 250,
                nonce: 9,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: now,
            })
            .expect("add verified share");
        store
            .add_share(ShareRecord {
                job_id: "job-rejected".to_string(),
                miner: "miner-db-only".to_string(),
                worker: "worker-1".to_string(),
                difficulty: 250,
                nonce: 10,
                status: "rejected",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("bad share".to_string()),
                created_at: now,
            })
            .expect("add rejected share");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_miners(Query(MinersQuery::default()), State(state)))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode miners json");

        assert_eq!(payload["miner-db-only"]["shares_accepted"], 1);
        assert_eq!(payload["miner-db-only"]["shares_rejected"], 1);
        assert_eq!(payload["miner-db-only"]["blocks_found"], 0);
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
            store
                .add_share(ShareRecord {
                    job_id: job_id.to_string(),
                    miner: "miner-workers".to_string(),
                    worker: worker.to_string(),
                    difficulty: 250,
                    nonce,
                    status: "verified",
                    was_sampled: true,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at,
                })
                .expect("add share");
        }

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_miner(
                Path("miner-workers".to_string()),
                Query(MinerDetailQuery {
                    share_limit: Some(10),
                    include_pending_estimate: Some(false),
                }),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode miner json");

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
            store
                .add_share(ShareRecord {
                    job_id: job_id.to_string(),
                    miner: "miner-since".to_string(),
                    worker: "worker-1".to_string(),
                    difficulty: 250,
                    nonce,
                    status: "verified",
                    was_sampled: true,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at,
                })
                .expect("add share");
        }

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_miner(
                Path("miner-since".to_string()),
                Query(MinerDetailQuery {
                    share_limit: Some(1),
                    include_pending_estimate: Some(false),
                }),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode miner json");

        assert_eq!(
            payload["shares"]
                .as_array()
                .map(|items| items.len())
                .unwrap_or_default(),
            1
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

        store
            .add_share(ShareRecord {
                job_id: "job-stale-worker".to_string(),
                miner: "miner-stale-workers".to_string(),
                worker: "worker-old".to_string(),
                difficulty: 250,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: stale_share_at,
            })
            .expect("add stale share");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_miners(
                Query(MinersQuery {
                    paged: Some(true),
                    limit: Some(25),
                    offset: Some(0),
                    search: None,
                    sort: Some("address_asc".to_string()),
                }),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode miners json");

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
        store
            .add_share(ShareRecord {
                job_id: "job-hold".to_string(),
                miner: "miner-hold".to_string(),
                worker: "worker-1".to_string(),
                difficulty: 250,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at,
            })
            .expect("add share");
        store
            .escalate_address_risk(
                "miner-hold",
                "low difficulty share",
                Duration::from_secs(6 * 60 * 60),
                0,
                Duration::from_secs(15 * 60),
                Duration::from_secs(2 * 60 * 60),
                Duration::from_secs(2 * 60 * 60),
            )
            .expect("seed hold");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_miner(
                Path("miner-hold".to_string()),
                Query(MinerDetailQuery {
                    share_limit: Some(1),
                    include_pending_estimate: Some(false),
                }),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode miner json");

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
        store
            .add_share(ShareRecord {
                job_id: "job-backlog".to_string(),
                miner: "miner-backlog".to_string(),
                worker: "worker-1".to_string(),
                difficulty: 250,
                nonce: 1,
                status: "provisional",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: now,
            })
            .expect("add share");
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
                hold_cause: Some(crate::db::ValidationHoldCause::ProvisionalBacklog),
                last_seen_at: now,
            })
            .expect("persist validation state");
        store
            .add_validation_provisional("miner-backlog", None, now)
            .expect("persist provisional");

        let state = test_api_state(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let response = runtime
            .block_on(handle_miner(
                Path("miner-backlog".to_string()),
                Query(MinerDetailQuery {
                    share_limit: Some(1),
                    include_pending_estimate: Some(false),
                }),
                State(state),
            ))
            .into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = runtime
            .block_on(to_bytes(response.into_body(), usize::MAX))
            .expect("read body");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("decode miner json");

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
        assert_eq!(
            payload["verification_hold"]["validation_recent_verified_difficulty"].as_u64(),
            Some(0)
        );
        assert_eq!(
            payload["verification_hold"]["validation_recent_provisional_difficulty"].as_u64(),
            Some(250)
        );
    }

    #[test]
    fn payout_status_note_covers_minimum_and_queue() {
        let cfg = Config::default();
        let below_min = payout_status_note(&cfg, 5_000_000, None).expect("below minimum");
        assert!(below_min.contains("below the 0.1 BNT minimum payout"));

        let queued = PendingPayout {
            address: "miner-a".to_string(),
            amount: 100_000_000,
            initiated_at: UNIX_EPOCH,
            send_started_at: None,
            tx_hash: None,
            fee: None,
            sent_at: None,
            batch_id: None,
        };
        let queued_note =
            payout_status_note(&cfg, 250_000_000, Some(&queued)).expect("queued note");
        assert!(queued_note.contains("already queued for payout"));
        assert!(queued_note.contains("Another 1.50 BNT"));
    }

    #[test]
    fn batch_payouts_marks_batch_unconfirmed_when_any_entry_is_pending() {
        let t0 = UNIX_EPOCH + Duration::from_secs(10_000);
        let batches = batch_payouts(&[
            Payout {
                id: 1,
                address: "miner-a".to_string(),
                amount: 100,
                fee: 1,
                tx_hash: "tx-confirmed".to_string(),
                timestamp: t0,
                confirmed: true,
                batch_id: None,
            },
            Payout {
                id: 0,
                address: "miner-b".to_string(),
                amount: 200,
                fee: 2,
                tx_hash: "tx-pending".to_string(),
                timestamp: t0 + Duration::from_secs(30),
                confirmed: false,
                batch_id: None,
            },
        ]);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].recipient_count, 2);
        assert_eq!(batches[0].total_amount, 300);
        assert!(!batches[0].confirmed);
    }

    #[test]
    fn batch_payouts_uses_batch_id_across_time_gap() {
        let t0 = UNIX_EPOCH + Duration::from_secs(20_000);
        let batches = batch_payouts(&[
            Payout {
                id: 1,
                address: "miner-a".to_string(),
                amount: 100,
                fee: 1,
                tx_hash: "tx-a".to_string(),
                timestamp: t0,
                confirmed: true,
                batch_id: Some("batch-1".to_string()),
            },
            Payout {
                id: 2,
                address: "miner-b".to_string(),
                amount: 200,
                fee: 2,
                tx_hash: "tx-a".to_string(),
                timestamp: t0 + Duration::from_secs(15 * 60),
                confirmed: true,
                batch_id: Some("batch-1".to_string()),
            },
        ]);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].recipient_count, 2);
        assert_eq!(batches[0].total_amount, 300);
    }

    #[test]
    fn miner_balance_response_tracks_queued_and_unqueued_amounts() {
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
        };
        let response = miner_balance_response(&balance, Some(&queued));
        assert_eq!(response.pending, 250);
        assert_eq!(response.pending_confirmed, 250);
        assert_eq!(response.pending_queued, 100);
        assert_eq!(response.pending_unqueued, 150);
        assert_eq!(response.paid, 900);
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_matches_weighted_split() {
        let store = require_test_store!();
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
                claimed_hash: None,
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
                claimed_hash: None,
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
                effort_pct: None,
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
    fn estimate_unconfirmed_pending_for_miner_supports_last_n_pplns_window() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "0s".to_string();
        cfg.pplns_window = 2;
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;

        let base = UNIX_EPOCH + Duration::from_secs(1_500_000);
        let block_ts = base + Duration::from_secs(120);
        for share in [
            ShareRecord {
                job_id: "j-last-n-a-old".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 10,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: "j-last-n-b".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 30,
                nonce: 2,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(10),
            },
            ShareRecord {
                job_id: "j-last-n-a-new".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 60,
                nonce: 3,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(20),
            },
        ] {
            store.add_share(share).expect("add share");
        }
        store
            .add_block(&DbBlock {
                height: 149,
                hash: "blk-last-n".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add last-n block");

        let estimate = estimate_unconfirmed_pending_for_miner(
            &store,
            "miner-a",
            &cfg,
            block_ts + Duration::from_secs(1),
            150,
        )
        .expect("estimate");
        assert_eq!(estimate.estimated_pending, 667);
        assert_eq!(estimate.blocks.len(), 1);
        assert_eq!(estimate.blocks[0].estimated_credit, 667);
        assert_eq!(estimate.blocks[0].validation_state, "ready");
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_batches_overlapping_duration_windows() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "1h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;

        let base = UNIX_EPOCH + Duration::from_secs(6_000_000);
        for share in [
            ShareRecord {
                job_id: "j-batch-a".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: "j-batch-b".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(50 * 60),
            },
            ShareRecord {
                job_id: "j-batch-c".to_string(),
                miner: "miner-c".to_string(),
                worker: "wc".to_string(),
                difficulty: 100,
                nonce: 3,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(90 * 60),
            },
        ] {
            store.add_share(share).expect("add share");
        }
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
            store
                .add_block(&DbBlock {
                    height,
                    hash,
                    difficulty: 200,
                    finder,
                    finder_worker: "w".to_string(),
                    reward: 900,
                    timestamp,
                    confirmed: false,
                    orphaned: false,
                    paid_out: false,
                    effort_pct: None,
                })
                .expect("add unconfirmed block");
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
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 10.0;
        cfg.pool_fee_flat = 0.0;
        cfg.pool_wallet_address = "pool-fee-destination".to_string();
        cfg.block_finder_bonus = false;
        cfg.provisional_share_delay = "0s".to_string();

        let base = UNIX_EPOCH + Duration::from_secs(3_000_000);
        let block_ts = base + Duration::from_secs(120);
        for share in [
            ShareRecord {
                job_id: "j-a".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: "j-b".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(1),
            },
        ] {
            store.add_share(share).expect("add share");
        }
        store
            .add_block(&DbBlock {
                height: 299,
                hash: "blk-paid".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add paid block");
        store
            .apply_block_credits_and_mark_paid_with_fee(
                299,
                &[("miner-a".to_string(), 450), ("miner-b".to_string(), 450)],
                Some(&PoolFeeRecord {
                    amount: 100,
                    fee_address: cfg.pool_wallet_address.clone(),
                    timestamp: block_ts,
                }),
            )
            .expect("apply block credits");

        let breakdown =
            build_block_reward_breakdown(&store, &cfg, 299, block_ts + Duration::from_secs(10))
                .expect("reward breakdown");
        assert!(breakdown.actual_credit_events_available);
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
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_min_verified_ratio = 0.0;
        cfg.payout_provisional_cap_multiplier = 1.0;

        let base = UNIX_EPOCH + Duration::from_secs(4_000_000);
        let block_ts = base + Duration::from_secs(120);
        for share in [
            ShareRecord {
                job_id: "j-cap-a-verified".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 20,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: "j-cap-a-provisional".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "provisional",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(1),
            },
            ShareRecord {
                job_id: "j-cap-b-verified".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 20,
                nonce: 3,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(2),
            },
        ] {
            store.add_share(share).expect("add share");
        }
        store
            .add_block(&DbBlock {
                height: 298,
                hash: "blk-cap".to_string(),
                difficulty: 200,
                finder: "miner-b".to_string(),
                finder_worker: "wb".to_string(),
                reward: 1_200,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add block");

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
        assert_eq!(miner_a.payout_status, "capped_provisional");
    }

    #[test]
    fn block_reward_breakdown_replays_zero_weight_window_for_payout_view() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_min_verified_ratio = 0.0;

        let base = UNIX_EPOCH + Duration::from_secs(4_100_000);
        let block_ts = base + Duration::from_secs(120);
        store
            .add_share_with_replay(
                ShareRecord {
                    job_id: "j-replay-a".to_string(),
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty: 1,
                    nonce: 1,
                    status: "provisional",
                    was_sampled: false,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at: base,
                },
                Some(ShareReplayData {
                    job_id: "j-replay-a".to_string(),
                    header_base: vec![1, 2, 3, 4],
                    network_target: [0xff; 32],
                    created_at: base,
                }),
            )
            .expect("add replay share");
        store
            .add_block(&DbBlock {
                height: 297,
                hash: "blk-replay-view".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add block");

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
        assert_eq!(miner_a.payout_status, "included");
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_uses_tentative_preview_not_payout_gate() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_min_verified_shares = 3;
        cfg.payout_min_verified_ratio = 0.5;

        let base = UNIX_EPOCH + Duration::from_secs(2_000_000);
        let block_ts = base + Duration::from_secs(120);
        for share in [
            ShareRecord {
                job_id: "j-preview-a-verified".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 10,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: "j-preview-a-provisional".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 90,
                nonce: 2,
                status: "provisional",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(1),
            },
            ShareRecord {
                job_id: "j-preview-b".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 100,
                nonce: 3,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(2),
            },
        ] {
            store.add_share(share).expect("add preview share");
        }
        store
            .add_block(&DbBlock {
                height: 199,
                hash: "blk-preview".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add unconfirmed block");

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
        assert_eq!(estimate.blocks[0].validation_state, "awaiting_shares");
        assert_eq!(estimate.blocks[0].validation_label, "Waiting for shares");
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_counts_verified_shares_during_extra_verification() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_min_verified_ratio = 0.0;
        cfg.payout_provisional_cap_multiplier = 19.0;

        store
            .escalate_address_risk(
                "miner-a",
                "invalid share proof",
                Duration::from_secs(60 * 60),
                0,
                Duration::from_secs(60),
                Duration::from_secs(60 * 60),
                Duration::from_secs(60 * 60),
            )
            .expect("seed risk");

        let base = UNIX_EPOCH + Duration::from_secs(2_050_000);
        let block_ts = base + Duration::from_secs(120);
        for share in [
            ShareRecord {
                job_id: "j-risk-a-verified".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 10,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: "j-risk-a-provisional".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 90,
                nonce: 2,
                status: "provisional",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(1),
            },
            ShareRecord {
                job_id: "j-risk-b".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 10,
                nonce: 3,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(2),
            },
        ] {
            store.add_share(share).expect("add risk preview share");
        }
        store
            .add_block(&DbBlock {
                height: 249,
                hash: "blk-risk-preview".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add unconfirmed block");

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
        assert_eq!(estimate.blocks[0].validation_state, "extra_verification");
        assert_eq!(estimate.blocks[0].validation_label, "Verified only");
        assert!(estimate.blocks[0]
            .validation_detail
            .contains("only fully verified shares count"));
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_uses_winning_share_timestamp_for_window_end() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;

        let base = UNIX_EPOCH + Duration::from_secs(2_100_000);
        let block_ts = base;
        store
            .add_share(ShareRecord {
                job_id: "j-anchor-a".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base - Duration::from_secs(10),
            })
            .expect("add anchor share a");
        store
            .add_share(ShareRecord {
                job_id: "j-anchor-b".to_string(),
                miner: "miner-b".to_string(),
                worker: "wb".to_string(),
                difficulty: 100,
                nonce: 2,
                status: "verified",
                was_sampled: true,
                block_hash: Some("blk-anchor".to_string()),
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(10),
            })
            .expect("add winning share");
        store
            .add_block(&DbBlock {
                height: 109,
                hash: "blk-anchor".to_string(),
                difficulty: 200,
                finder: "miner-b".to_string(),
                finder_worker: "wb".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add anchor block");

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
        assert_eq!(estimate.blocks[0].validation_state, "ready");
    }

    #[test]
    fn estimate_unconfirmed_pending_for_miner_does_not_finder_fallback_on_delayed_window() {
        let store = require_test_store!();
        let mut cfg = Config::default();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "24h".to_string();
        cfg.pool_fee_pct = 0.0;
        cfg.pool_fee_flat = 0.0;
        cfg.block_finder_bonus = false;
        cfg.blocks_before_payout = 60;
        cfg.provisional_share_delay = "1h".to_string();

        let base = UNIX_EPOCH + Duration::from_secs(2_200_000);
        let block_ts = base + Duration::from_secs(120);
        store
            .add_share(ShareRecord {
                job_id: "j-delay-a".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 100,
                nonce: 1,
                status: "provisional",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            })
            .expect("add delayed share");
        store
            .add_block(&DbBlock {
                height: 119,
                hash: "blk-delay".to_string(),
                difficulty: 200,
                finder: "miner-a".to_string(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: block_ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add delayed block");

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
        assert_eq!(estimate.blocks[0].validation_state, "awaiting_delay");
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
            store
                .add_share(ShareRecord {
                    job_id,
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty,
                    nonce: difficulty,
                    status: "verified",
                    was_sampled: true,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at,
                })
                .expect("add share");
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
            store
                .add_block(&DbBlock {
                    height,
                    hash,
                    difficulty: 100,
                    finder: "miner-a".to_string(),
                    finder_worker: "wa".to_string(),
                    reward: 1_000,
                    timestamp,
                    confirmed: true,
                    orphaned: false,
                    paid_out: false,
                    effort_pct: None,
                })
                .expect("add block");
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
            store
                .add_share(ShareRecord {
                    job_id,
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty,
                    nonce: difficulty,
                    status: "verified",
                    was_sampled: true,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at,
                })
                .expect("add share");
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
            store
                .add_block(&DbBlock {
                    height,
                    hash,
                    difficulty: 100,
                    finder: "miner-a".to_string(),
                    finder_worker: "wa".to_string(),
                    reward: 1_000,
                    timestamp,
                    confirmed: true,
                    orphaned: false,
                    paid_out: false,
                    effort_pct: None,
                })
                .expect("add block");
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
        assert!(response.effort_band.is_some());
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

        store
            .add_share(ShareRecord {
                job_id: format!("job-activity-{unique}"),
                miner: address.clone(),
                worker: "wa".to_string(),
                difficulty: 32,
                nonce: 32,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: UNIX_EPOCH + Duration::from_secs(4_000_000),
            })
            .expect("add share");

        assert!(store
            .miner_has_any_activity(&address)
            .expect("address should have activity after share"));
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
    fn daemon_send_idempotency_path_uses_daemon_data_dir() {
        let cfg = Config {
            daemon_data_dir: "/var/lib/blocknet/data".to_string(),
            ..Config::default()
        };
        assert_eq!(
            daemon_send_idempotency_path(&cfg).to_string_lossy(),
            "/var/lib/blocknet/data/send-idempotency.json"
        );
    }

    #[test]
    fn daemon_log_commands_include_journal_and_tail() {
        let cfg = Config {
            daemon_data_dir: "/var/lib/blocknet/data".to_string(),
            ..Config::default()
        };
        let commands = daemon_log_commands(&cfg, 200, true);
        assert_eq!(commands.len(), 4);
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
        assert_eq!(commands[2].program, "journalctl");
        assert!(commands[2].args.iter().any(|a| a == "blocknetd.service"));
        assert_eq!(commands[3].program, "tail");
        assert!(commands[3].args.iter().any(|a| a == "-F"));
        assert!(commands[3]
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

        let mut cfg = Config::default();
        cfg.recovery.proxy_include_path = proxy_include.display().to_string();

        let commands = daemon_log_commands(&cfg, 50, false);
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

        let mut cfg = Config::default();
        cfg.daemon_data_dir = "/var/lib/blocknet/data".to_string();
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

        let mut cfg = Config::default();
        cfg.daemon_data_dir = "/var/lib/blocknet/data".to_string();
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
            effort_pct: None,
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
            effort_pct: None,
        };
        hydrate_provisional_block_reward(&mut block);
        assert_eq!(block.reward, 123);
    }

    #[test]
    fn payout_eta_includes_locked_wallet_balance() {
        let mut payout_eta = PayoutEtaResponse {
            last_payout_at: None,
            estimated_next_payout_at: None,
            eta_seconds: None,
            typical_interval_seconds: None,
            configured_interval_seconds: None,
            next_sweep_at: None,
            next_sweep_in_seconds: None,
            pending_count: 2,
            pending_total_amount: 90,
            unpaid_count: 0,
            unpaid_amount: 0,
            wallet_spendable: None,
            wallet_pending: None,
            queue_shortfall_amount: 0,
            liquidity_constrained: false,
            reserve_target_amount: None,
            safe_spend_budget: None,
            spendable_output_count: None,
            small_output_count: None,
            medium_output_count: None,
            large_output_count: None,
            planned_batch_count: None,
            planned_recipient_count: None,
            rebalance_required: false,
            rebalance_active: false,
            inventory_health: None,
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
        assert_eq!(payout_eta.queue_shortfall_amount, 65);
        assert!(payout_eta.liquidity_constrained);
    }

    #[test]
    fn og_image_png_asset_has_png_signature() {
        assert_eq!(&UI_ASSET_OG_IMAGE_PNG[..8], b"\x89PNG\r\n\x1a\n");
    }

    #[test]
    fn og_image_svg_asset_is_svg_document() {
        assert!(UI_ASSET_OG_IMAGE_SVG.contains("<svg"));
        assert!(UI_ASSET_OG_IMAGE_SVG.contains("bntpool.com"));
    }
}
