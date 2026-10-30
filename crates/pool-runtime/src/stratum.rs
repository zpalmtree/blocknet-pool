use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Notify};

use crate::admission::{AdmissionState, UnprocessedReason, UnprocessedSubmitSnapshot};
use crate::config::{Config, CANDIDATE_SUBMIT_QUEUE_SIZE};
use crate::dev_fee::is_seine_dev_fee_address;
use crate::engine::Job;
use crate::engine::{
    canonical_share_reject_reason, is_retryable_reject_reason, PoolEngine, SubmitAck,
    SubmitMetadata, SubmitQueueRoute,
};
use crate::jobs::{AssignmentRangeMode, JobManager, MinerJob, MinerJobRequest};
use crate::service_state::EngineRole;
use crate::source_risk::ip_group_key;
use crate::stats::PoolStats;
use crate::telemetry::{
    LatencyHistogramSnapshot, NamedLatencyHistograms, PercentileSummary, QueueTracker,
};
use pool_common::gateway::{
    read_frame, write_frame, EngineHello, Frame, FrameKind, GatewayRole, MAX_FRAME_PAYLOAD,
};
use pool_common::protocol::{
    normalize_worker_name, LoginParams, RefillParams, StratumNotify, StratumRequest,
    StratumResponse, SubmitParams, CAP_SAME_TEMPLATE_REBIND_V1, METHOD_LOGIN, METHOD_NOTIFICATION,
    METHOD_REFILL, METHOD_SUBMIT, NOTIFY_CONNECTION_LIMIT, NOTIFY_MINER_BLOCK_FOUND,
    NOTIFY_POOL_BLOCK_SOLVED,
};

const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Range of the reconnect delay suggested to miners refused at a connection
/// cap, spread so refused miners do not all come back at once.
const CONNECTION_LIMIT_RETRY_MS: std::ops::RangeInclusive<u64> = 15_000..=45_000;
const MAX_STRATUM_REQUEST_BYTES: usize = 8 * 1024;
const NOTIFICATION_CHANNEL_CAPACITY: usize = 256;
const INBOUND_QUEUE_CAPACITY: usize = 256;
const OUTBOUND_QUEUE_CAPACITY: usize = 256;
const OUTBOUND_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const GATEWAY_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const GATEWAY_FRAME_QUEUE: usize = 8192;
/// Per virtual connection; engine-side writes beyond this wait for the gateway.
const GATEWAY_CONN_BUFFER: usize = 64 * 1024;
const SUBMIT_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(10);
const SUBMIT_RATE_LIMIT_MAX: usize = 120;
const POST_LOGIN_IDLE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const CANDIDATE_CLAIM_WINDOW: Duration = Duration::from_secs(60);
const CANDIDATE_CLAIM_MAX_PER_WINDOW: usize = 4;
const CANDIDATE_CLAIM_MAX_INFLIGHT: usize = 1;
// Addresses are free, so address-scoped limits alone let one host flood the
// single-worker candidate lane with throwaway addresses. Bound the IP group
// (IPv4 address / IPv6 /64) and the connection as well.
const CANDIDATE_CLAIM_IP_MAX_PER_WINDOW: usize = 8;
const CANDIDATE_CLAIM_IP_MAX_INFLIGHT: usize = 2;
const CANDIDATE_CLAIM_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const CANDIDATE_SUBMIT_WORKERS: usize = 1;
// Block candidates from established sources (clean verified history on that
// address from that network) get their own small lane so a flood of false
// claims from fresh sources cannot delay a real block. A false claim from an
// established source strikes it and drops it back to the shared lane.
const PRIORITY_CANDIDATE_SUBMIT_WORKERS: usize = 1;
const PRIORITY_CANDIDATE_SUBMIT_QUEUE_SIZE: usize = 16;

fn client_submit_ack_difficulty(
    address: &str,
    current_difficulty: u64,
    next_difficulty: u64,
) -> u64 {
    if is_seine_dev_fee_address(address) {
        current_difficulty.max(1)
    } else {
        next_difficulty.max(1)
    }
}

/// Per-connection token bucket for `refill` requests: `rate` per second,
/// up to `burst` at once.
#[derive(Debug)]
struct RefillBucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    refilled_at: Instant,
}

impl RefillBucket {
    fn new(rate: f64, burst: f64, now: Instant) -> Self {
        Self {
            tokens: burst,
            rate,
            burst,
            refilled_at: now,
        }
    }

    fn try_take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.refilled_at);
        self.refilled_at = now;
        self.tokens = (self.tokens + elapsed.as_secs_f64() * self.rate).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct ConnState {
    counts: HashMap<String, usize>,
    total: usize,
    rejected_at_cap: u64,
    rejected_per_ip: u64,
}

/// The process heap as glibc's allocator accounts it. `in_use` growing means
/// live allocations grew; `free_held` growing while `in_use` is flat means
/// fragmentation (freed memory the allocator keeps).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapSnapshot {
    /// Bytes in allocator arenas obtained with brk/sbrk or arena mmaps.
    pub arena_bytes: u64,
    pub in_use_bytes: u64,
    pub free_held_bytes: u64,
    /// Large allocations served directly by mmap (Argon2 arenas among them).
    pub mmapped_bytes: u64,
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn heap_snapshot() -> Option<HeapSnapshot> {
    // SAFETY: mallinfo2 takes no arguments and only reads allocator state.
    let info = unsafe { libc::mallinfo2() };
    Some(HeapSnapshot {
        arena_bytes: info.arena as u64,
        in_use_bytes: info.uordblks as u64,
        free_held_bytes: info.fordblks as u64,
        mmapped_bytes: info.hblkhd as u64,
    })
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn heap_snapshot() -> Option<HeapSnapshot> {
    None
}

/// Returns freed heap memory to the operating system. glibc keeps memory the
/// process has freed for reuse, and under this workload's allocation pattern
/// that held memory keeps growing (`free_held_bytes`) while live use is flat.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn trim_heap() {
    // SAFETY: malloc_trim only releases free memory the allocator holds.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn trim_heap() {}

/// Miner connections against Stratum's caps, and the connections refused.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ConnectionSnapshot {
    pub open: usize,
    pub max: usize,
    pub max_per_ip: usize,
    /// Refused because the total cap was reached.
    pub rejected_at_cap_total: u64,
    /// Refused because their IP (IPv6 /64) was at the per-IP cap.
    pub rejected_per_ip_total: u64,
}

#[derive(Clone)]
struct OutboundHandle {
    normal_tx: mpsc::Sender<String>,
    latest_job: Arc<Mutex<Option<QueuedJob>>>,
    latest_job_notify: Arc<Notify>,
    job_fanout: Arc<NamedLatencyHistograms>,
}

/// The newest unwritten job for one connection, with the times the
/// `job_fanout` latency family measures from.
#[derive(Debug)]
struct QueuedJob {
    payload: String,
    queued_at: Instant,
    /// When the template behind a broadcast job was fetched; `None` for jobs
    /// sent outside a template broadcast (login, difficulty updates).
    template_created_at: Option<Instant>,
}

/// A logged-in connection as the job fanout sees it.
struct FanoutConn {
    address: String,
    /// The difficulty of the connection's latest job, shared with its
    /// connection task (which compares submit acks against it).
    difficulty: Arc<AtomicU64>,
    outbound: OutboundHandle,
    /// Wakes the connection task to close the session with `kick_reason`.
    kick: Arc<Notify>,
    kick_reason: Arc<Mutex<Option<String>>>,
}

impl FanoutConn {
    fn kick(&self, reason: String) {
        *self.kick_reason.lock() = Some(reason);
        self.kick.notify_one();
    }
}

/// A `job` notification serialized straight from the miner job, without the
/// intermediate `serde_json::Value` of `StratumNotify`. Same JSON.
#[derive(Serialize)]
struct JobNotify<'a> {
    method: &'static str,
    params: &'a MinerJob,
}

impl OutboundHandle {
    #[cfg(test)]
    fn new(capacity: usize) -> (Self, mpsc::Receiver<String>) {
        Self::with_job_fanout(capacity, Arc::default())
    }

    fn with_job_fanout(
        capacity: usize,
        job_fanout: Arc<NamedLatencyHistograms>,
    ) -> (Self, mpsc::Receiver<String>) {
        let (normal_tx, normal_rx) = mpsc::channel(capacity);
        (
            Self {
                normal_tx,
                latest_job: Arc::new(Mutex::new(None)),
                latest_job_notify: Arc::new(Notify::new()),
                job_fanout,
            },
            normal_rx,
        )
    }
}

#[derive(Debug)]
enum InboundFrame {
    Text(String),
    ReadError(String),
}

#[derive(Debug)]
enum SubmitCompletionOutcome {
    Finished(Result<SubmitAck>),
    WorkerFailure(String),
}

#[derive(Debug)]
struct SubmitCompletion {
    req_id: u64,
    job_id: String,
    nonce: u64,
    outcome: SubmitCompletionOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SubmitRuntimeSnapshot {
    #[serde(default)]
    pub submits_received_total: u64,
    #[serde(default)]
    pub candidate_classified_total: u64,
    #[serde(default)]
    pub candidate_permit_acquired_total: u64,
    #[serde(default)]
    pub candidate_permit_rejected_total: u64,
    #[serde(default)]
    pub candidate_enqueued_total: u64,
    #[serde(default)]
    pub candidate_enqueue_rejected_total: u64,
    #[serde(default)]
    pub candidate_worker_started_total: u64,
    #[serde(default)]
    pub candidate_persisted_total: u64,
    #[serde(default)]
    pub candidate_rejected_total: u64,
    #[serde(default)]
    pub candidate_stale_block_total: u64,
    #[serde(default)]
    pub candidate_block_accepted_total: u64,
    #[serde(default)]
    pub candidate_worker_failure_total: u64,
    #[serde(default)]
    pub candidate_priority_enqueued_total: u64,
    #[serde(default)]
    pub candidate_priority_queue_depth: usize,
    #[serde(default)]
    pub candidate_queue_depth: usize,
    #[serde(default)]
    pub regular_queue_depth: usize,
    #[serde(default)]
    pub candidate_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub regular_oldest_age_millis: Option<u64>,
    #[serde(default)]
    pub candidate_wait: PercentileSummary,
    #[serde(default)]
    pub regular_wait: PercentileSummary,
    /// Submits answered "busy/retry" without being judged, by reason.
    #[serde(default)]
    pub unprocessed: UnprocessedSubmitSnapshot,
    #[serde(default)]
    pub growth_mode_active: bool,
    /// Growth level (0 = off, 1, 2).
    #[serde(default)]
    pub growth_level: u8,
    /// Smoothed share rate at the normal share size (growth-mode input).
    #[serde(default)]
    pub growth_normal_size_submits_per_s: Option<f64>,
    #[serde(default)]
    pub connections: Option<ConnectionSnapshot>,
    #[serde(default)]
    pub heap: Option<HeapSnapshot>,
}

#[derive(Debug)]
struct SubmitWorkItem {
    conn_id: String,
    req_id: u64,
    params: SubmitParams,
    received_at: Instant,
    completion_tx: mpsc::UnboundedSender<SubmitCompletion>,
    candidate_permit: Option<CandidateClaimPermit>,
    /// Who the work is queued for, on the regular lane (fair shedding).
    source: Option<QueuedSource>,
}

#[derive(Debug, Clone)]
struct QueuedSource {
    address: String,
    ip_group: String,
}

/// Regular-lane occupancy from which submits are shed per source.
const FAIR_SHED_START_NUM: usize = 3;
const FAIR_SHED_START_DEN: usize = 4;

/// Regular-lane work currently queued per address and per IP group. When the
/// lane is nearly full, a source holding more than the average share of the
/// queued work is shed first, so one farm's flood turns into busy answers for
/// the farm rather than for every other miner. O(1) per submit.
#[derive(Debug, Default)]
struct QueuedSources {
    total: usize,
    by_address: HashMap<String, usize>,
    by_ip_group: HashMap<String, usize>,
}

impl QueuedSources {
    /// Whether `source` holds more than the average queued work per
    /// address, or per IP group. A lone source is never over its share, and
    /// neither is a source with a single queued submit.
    fn over_share(&self, source: &QueuedSource) -> bool {
        let over = |held: usize, sources: usize| held.saturating_mul(sources) > self.total;
        let address = self.by_address.get(&source.address).copied().unwrap_or(0);
        let ip_group = self.by_ip_group.get(&source.ip_group).copied().unwrap_or(0);
        over(address, self.by_address.len()) || over(ip_group, self.by_ip_group.len())
    }

    fn add(&mut self, source: &QueuedSource) {
        self.total += 1;
        *self.by_address.entry(source.address.clone()).or_default() += 1;
        *self.by_ip_group.entry(source.ip_group.clone()).or_default() += 1;
    }

    fn remove(&mut self, source: &QueuedSource) {
        self.total = self.total.saturating_sub(1);
        for (map, key) in [
            (&mut self.by_address, &source.address),
            (&mut self.by_ip_group, &source.ip_group),
        ] {
            if let Some(count) = map.get_mut(key) {
                *count -= 1;
                if *count == 0 {
                    map.remove(key);
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct CandidateClaimState {
    recent: VecDeque<Instant>,
    in_flight: usize,
}

#[derive(Debug, Clone, Copy)]
struct CandidateClaimLimit {
    max_per_window: usize,
    max_inflight: usize,
}

#[derive(Debug, Default)]
struct CandidateClaimTrackerState {
    entries: HashMap<String, CandidateClaimState>,
    last_sweep: Option<Instant>,
}

#[derive(Debug)]
struct CandidateClaimTracker {
    window: Duration,
    max_per_window: usize,
    max_inflight: usize,
    state: Mutex<CandidateClaimTrackerState>,
}

#[derive(Debug, Clone)]
struct CandidateClaimPermit {
    tracker: Arc<CandidateClaimTracker>,
    keys: Vec<String>,
}

/// Why a submit was not admitted to its lane. Both are answered "server
/// busy, retry" so the miner sees no protocol change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueueRejection {
    QueueFull,
    FairShed,
}

impl EnqueueRejection {
    fn message(self) -> &'static str {
        "server busy, retry"
    }

    fn reason(self) -> UnprocessedReason {
        match self {
            Self::QueueFull => UnprocessedReason::SubmitQueueFull,
            Self::FairShed => UnprocessedReason::FairShed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmitLane {
    Regular,
    Candidate,
    PriorityCandidate,
}

#[derive(Debug)]
struct SubmitDispatcher {
    candidate_tx: flume::Sender<SubmitWorkItem>,
    priority_candidate_tx: flume::Sender<SubmitWorkItem>,
    regular_tx: flume::Sender<SubmitWorkItem>,
    regular_capacity: usize,
    regular_sources: Mutex<QueuedSources>,
    admission: Arc<AdmissionState>,
    candidate_queue: Arc<QueueTracker>,
    priority_candidate_queue: Arc<QueueTracker>,
    regular_queue: Arc<QueueTracker>,
    candidate_priority_enqueued: AtomicU64,
    candidate_tracker: Arc<CandidateClaimTracker>,
    submits_received: AtomicU64,
    candidate_classified: AtomicU64,
    candidate_permit_acquired: AtomicU64,
    candidate_permit_rejected: AtomicU64,
    candidate_enqueued: AtomicU64,
    candidate_enqueue_rejected: AtomicU64,
    candidate_worker_started: AtomicU64,
    candidate_persisted: AtomicU64,
    candidate_rejected: AtomicU64,
    candidate_stale_block: AtomicU64,
    candidate_block_accepted: AtomicU64,
    candidate_worker_failure: AtomicU64,
    /// Received-to-answered latency of queued submits, by lane and outcome.
    ack_latency: NamedLatencyHistograms,
}

impl CandidateClaimState {
    fn prune(&mut self, now: Instant, window: Duration) {
        while self
            .recent
            .front()
            .is_some_and(|seen_at| now.saturating_duration_since(*seen_at) >= window)
        {
            self.recent.pop_front();
        }
    }

    fn idle(&self) -> bool {
        self.in_flight == 0 && self.recent.is_empty()
    }
}

impl CandidateClaimTracker {
    fn new(window: Duration, max_per_window: usize, max_inflight: usize) -> Arc<Self> {
        Arc::new(Self {
            window: window.max(Duration::from_secs(1)),
            max_per_window: max_per_window.max(1),
            max_inflight: max_inflight.max(1),
            state: Mutex::new(CandidateClaimTrackerState::default()),
        })
    }

    fn default_limit(&self) -> CandidateClaimLimit {
        CandidateClaimLimit {
            max_per_window: self.max_per_window,
            max_inflight: self.max_inflight,
        }
    }

    #[cfg(test)]
    fn try_acquire(self: &Arc<Self>, key: String, now: Instant) -> Result<CandidateClaimPermit> {
        let limit = self.default_limit();
        self.try_acquire_all(vec![(key, limit)], now)
    }

    /// Acquires every bucket or none. Each key carries its own limits.
    fn try_acquire_all(
        self: &Arc<Self>,
        buckets: Vec<(String, CandidateClaimLimit)>,
        now: Instant,
    ) -> Result<CandidateClaimPermit> {
        let mut state = self.state.lock();
        self.sweep_if_due_locked(&mut state, now);
        let mut busy = false;
        let mut limited = false;
        for (key, limit) in &buckets {
            let Some(entry) = state.entries.get_mut(key) else {
                continue;
            };
            entry.prune(now, self.window);
            if entry.in_flight >= limit.max_inflight.max(1) {
                busy = true;
            } else if entry.recent.len() >= limit.max_per_window.max(1) {
                limited = true;
            }
        }
        if busy {
            return Err(anyhow!("candidate claim busy, retry"));
        }
        if limited {
            return Err(anyhow!("candidate claim rate limited, retry"));
        }
        let mut keys = Vec::with_capacity(buckets.len());
        for (key, _) in buckets {
            let entry = state.entries.entry(key.clone()).or_default();
            entry.recent.push_back(now);
            entry.in_flight += 1;
            keys.push(key);
        }
        Ok(CandidateClaimPermit {
            tracker: Arc::clone(self),
            keys,
        })
    }

    fn release(&self, keys: &[String]) {
        let now = Instant::now();
        let mut state = self.state.lock();
        for key in keys {
            let Some(entry) = state.entries.get_mut(key) else {
                continue;
            };
            entry.in_flight = entry.in_flight.saturating_sub(1);
            entry.prune(now, self.window);
            if entry.idle() {
                state.entries.remove(key);
            }
        }
        self.sweep_if_due_locked(&mut state, now);
    }

    /// Drops buckets whose window history expired after their last release;
    /// without this, keys with recent history at release time were never freed.
    fn sweep_if_due_locked(&self, state: &mut CandidateClaimTrackerState, now: Instant) {
        if state.last_sweep.is_some_and(|last| {
            now.saturating_duration_since(last) < CANDIDATE_CLAIM_SWEEP_INTERVAL
        }) {
            return;
        }
        state.last_sweep = Some(now);
        let window = self.window;
        state.entries.retain(|_, entry| {
            entry.prune(now, window);
            !entry.idle()
        });
    }

    #[cfg(test)]
    fn tracked_keys(&self) -> usize {
        self.state.lock().entries.len()
    }

    #[cfg(test)]
    fn force_sweep(&self, now: Instant) {
        let mut state = self.state.lock();
        state.last_sweep = None;
        self.sweep_if_due_locked(&mut state, now);
    }
}

impl CandidateClaimPermit {
    fn release(self) {
        self.tracker.release(&self.keys);
    }
}

impl SubmitDispatcher {
    fn new(engine: Arc<PoolEngine>, cfg: &Config) -> Arc<Self> {
        let (candidate_tx, candidate_rx) =
            flume::bounded::<SubmitWorkItem>(CANDIDATE_SUBMIT_QUEUE_SIZE);
        let (priority_candidate_tx, priority_candidate_rx) =
            flume::bounded::<SubmitWorkItem>(PRIORITY_CANDIDATE_SUBMIT_QUEUE_SIZE);
        let (regular_tx, regular_rx) =
            flume::bounded::<SubmitWorkItem>(cfg.regular_submit_queue_size());
        let candidate_queue = Arc::new(QueueTracker::new(512));
        let priority_candidate_queue = Arc::new(QueueTracker::new(512));
        let regular_queue = Arc::new(QueueTracker::new(512));
        engine.attach_submit_regular_queue(Arc::clone(&regular_queue));
        let dispatcher = Arc::new(Self {
            candidate_tx,
            priority_candidate_tx,
            regular_tx,
            regular_capacity: cfg.regular_submit_queue_size().max(1),
            regular_sources: Mutex::new(QueuedSources::default()),
            admission: engine.admission_handle(),
            candidate_queue,
            priority_candidate_queue,
            regular_queue,
            candidate_priority_enqueued: AtomicU64::new(0),
            candidate_tracker: CandidateClaimTracker::new(
                CANDIDATE_CLAIM_WINDOW,
                CANDIDATE_CLAIM_MAX_PER_WINDOW,
                CANDIDATE_CLAIM_MAX_INFLIGHT,
            ),
            submits_received: AtomicU64::new(0),
            candidate_classified: AtomicU64::new(0),
            candidate_permit_acquired: AtomicU64::new(0),
            candidate_permit_rejected: AtomicU64::new(0),
            candidate_enqueued: AtomicU64::new(0),
            candidate_enqueue_rejected: AtomicU64::new(0),
            candidate_worker_started: AtomicU64::new(0),
            candidate_persisted: AtomicU64::new(0),
            candidate_rejected: AtomicU64::new(0),
            candidate_stale_block: AtomicU64::new(0),
            candidate_block_accepted: AtomicU64::new(0),
            candidate_worker_failure: AtomicU64::new(0),
            ack_latency: NamedLatencyHistograms::default(),
        });

        for _ in 0..CANDIDATE_SUBMIT_WORKERS {
            let engine = Arc::clone(&engine);
            let dispatcher = Arc::clone(&dispatcher);
            let candidate_rx = candidate_rx.clone();
            tokio::spawn(async move {
                run_submit_worker(engine, dispatcher, candidate_rx, SubmitLane::Candidate).await;
            });
        }

        for _ in 0..PRIORITY_CANDIDATE_SUBMIT_WORKERS {
            let engine = Arc::clone(&engine);
            let dispatcher = Arc::clone(&dispatcher);
            let priority_candidate_rx = priority_candidate_rx.clone();
            tokio::spawn(async move {
                run_submit_worker(
                    engine,
                    dispatcher,
                    priority_candidate_rx,
                    SubmitLane::PriorityCandidate,
                )
                .await;
            });
        }

        // Regular submits spend most of their time waiting on Postgres, so
        // several run at once; shares needing a synchronous verify are
        // bounded separately (`sync_verify_slots`) so they cannot take them
        // all.
        for _ in 0..cfg.regular_submit_worker_count() {
            let engine = Arc::clone(&engine);
            let dispatcher = Arc::clone(&dispatcher);
            let regular_rx = regular_rx.clone();
            tokio::spawn(async move {
                run_submit_worker(engine, dispatcher, regular_rx, SubmitLane::Regular).await;
            });
        }

        dispatcher
    }

    fn try_acquire_candidate_permit(
        &self,
        conn_id: &str,
        address: &str,
        peer: SocketAddr,
        established_source: bool,
        now: Instant,
    ) -> Result<CandidateClaimPermit> {
        let ip_group = ip_group_key(peer.ip());
        let per_source = self.candidate_tracker.default_limit();
        let mut buckets = vec![
            (format!("addr|{}|{}", address.trim(), ip_group), per_source),
            (format!("conn|{conn_id}"), per_source),
        ];
        // Established sources are exempt from network-level penalties, so a
        // fresh source sharing their IP cannot use up their block claims.
        if !established_source {
            buckets.push((
                format!("ip|{ip_group}"),
                CandidateClaimLimit {
                    max_per_window: CANDIDATE_CLAIM_IP_MAX_PER_WINDOW,
                    max_inflight: CANDIDATE_CLAIM_IP_MAX_INFLIGHT,
                },
            ));
        }
        match self.candidate_tracker.try_acquire_all(buckets, now) {
            Ok(permit) => {
                self.candidate_permit_acquired
                    .fetch_add(1, Ordering::Relaxed);
                Ok(permit)
            }
            Err(err) => {
                self.candidate_permit_rejected
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn record_submit_received(&self) {
        self.submits_received.fetch_add(1, Ordering::Relaxed);
    }

    fn record_candidate_classified(&self) {
        self.candidate_classified.fetch_add(1, Ordering::Relaxed);
    }

    fn lane_queue(&self, lane: SubmitLane) -> &QueueTracker {
        match lane {
            SubmitLane::Regular => &self.regular_queue,
            SubmitLane::Candidate => &self.candidate_queue,
            SubmitLane::PriorityCandidate => &self.priority_candidate_queue,
        }
    }

    fn try_enqueue(
        &self,
        item: SubmitWorkItem,
        lane: SubmitLane,
    ) -> std::result::Result<(), EnqueueRejection> {
        let queued_at = Instant::now();
        if lane == SubmitLane::Regular {
            return self.try_enqueue_regular(item, queued_at);
        }
        let tracker_id = self.lane_queue(lane).push(queued_at);
        let result = match lane {
            SubmitLane::Regular => unreachable!("regular lane handled above"),
            SubmitLane::Candidate => self.candidate_tx.try_send(item),
            SubmitLane::PriorityCandidate => self.priority_candidate_tx.try_send(item),
        };
        match result {
            Ok(()) => {
                match lane {
                    SubmitLane::Regular => {}
                    SubmitLane::Candidate => {
                        self.candidate_enqueued.fetch_add(1, Ordering::Relaxed);
                    }
                    SubmitLane::PriorityCandidate => {
                        self.candidate_enqueued.fetch_add(1, Ordering::Relaxed);
                        self.candidate_priority_enqueued
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(())
            }
            Err(flume::TrySendError::Full(item)) | Err(flume::TrySendError::Disconnected(item)) => {
                self.lane_queue(lane).remove(tracker_id);
                if let Some(permit) = item.candidate_permit {
                    permit.release();
                }
                if lane != SubmitLane::Regular {
                    self.candidate_enqueue_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(EnqueueRejection::QueueFull)
            }
        }
    }

    fn try_enqueue_regular(
        &self,
        item: SubmitWorkItem,
        queued_at: Instant,
    ) -> std::result::Result<(), EnqueueRejection> {
        let mut sources = self.regular_sources.lock();
        let depth = self.regular_tx.len();
        if depth * FAIR_SHED_START_DEN >= self.regular_capacity * FAIR_SHED_START_NUM {
            // Nearly full: vardiff must not read anyone's missing shares as
            // a hashrate drop, and the heaviest sources go first.
            self.admission.mark_pressure(queued_at);
            if item
                .source
                .as_ref()
                .is_some_and(|source| sources.over_share(source))
            {
                return Err(EnqueueRejection::FairShed);
            }
        }
        let tracker_id = self.regular_queue.push(queued_at);
        let source = item.source.clone();
        match self.regular_tx.try_send(item) {
            Ok(()) => {
                if let Some(source) = source.as_ref() {
                    sources.add(source);
                }
                Ok(())
            }
            Err(flume::TrySendError::Full(_)) | Err(flume::TrySendError::Disconnected(_)) => {
                self.regular_queue.remove(tracker_id);
                Err(EnqueueRejection::QueueFull)
            }
        }
    }

    /// The worker took `item` off the regular lane.
    fn release_regular_source(&self, item: &SubmitWorkItem) {
        if let Some(source) = item.source.as_ref() {
            self.regular_sources.lock().remove(source);
        }
    }

    fn snapshot(&self) -> SubmitRuntimeSnapshot {
        let now = Instant::now();
        let candidate = self.candidate_queue.snapshot(now);
        let priority_candidate = self.priority_candidate_queue.snapshot(now);
        let regular = self.regular_queue.snapshot(now);
        SubmitRuntimeSnapshot {
            candidate_priority_enqueued_total: self
                .candidate_priority_enqueued
                .load(Ordering::Relaxed),
            candidate_priority_queue_depth: priority_candidate.depth,
            submits_received_total: self.submits_received.load(Ordering::Relaxed),
            candidate_classified_total: self.candidate_classified.load(Ordering::Relaxed),
            candidate_permit_acquired_total: self.candidate_permit_acquired.load(Ordering::Relaxed),
            candidate_permit_rejected_total: self.candidate_permit_rejected.load(Ordering::Relaxed),
            candidate_enqueued_total: self.candidate_enqueued.load(Ordering::Relaxed),
            candidate_enqueue_rejected_total: self
                .candidate_enqueue_rejected
                .load(Ordering::Relaxed),
            candidate_worker_started_total: self.candidate_worker_started.load(Ordering::Relaxed),
            candidate_persisted_total: self.candidate_persisted.load(Ordering::Relaxed),
            candidate_rejected_total: self.candidate_rejected.load(Ordering::Relaxed),
            candidate_stale_block_total: self.candidate_stale_block.load(Ordering::Relaxed),
            candidate_block_accepted_total: self.candidate_block_accepted.load(Ordering::Relaxed),
            candidate_worker_failure_total: self.candidate_worker_failure.load(Ordering::Relaxed),
            candidate_queue_depth: candidate.depth,
            regular_queue_depth: regular.depth,
            candidate_oldest_age_millis: candidate.oldest_age_millis,
            regular_oldest_age_millis: regular.oldest_age_millis,
            candidate_wait: candidate.wait,
            regular_wait: regular.wait,
            unprocessed: UnprocessedSubmitSnapshot::default(),
            growth_mode_active: false,
            growth_level: 0,
            growth_normal_size_submits_per_s: None,
            connections: None,
            heap: None,
        }
    }
}

enum GatewaySession {
    /// The gateway retired this engine; it should exit.
    Retired,
    /// The gateway went away; reconnect.
    Lost,
}

/// Virtual connections of one gateway session: conn ID → line sender into
/// the miner's session. Removing a sender ends that session.
#[derive(Clone, Default)]
struct GatewayConns(Arc<Mutex<HashMap<u64, mpsc::Sender<String>>>>);

impl GatewayConns {
    fn insert(&self, conn: u64, tx: mpsc::Sender<String>) {
        self.0.lock().insert(conn, tx);
    }

    /// Returns whether the connection was still open.
    fn remove(&self, conn: u64) -> bool {
        self.0.lock().remove(&conn).is_some()
    }

    fn clear(&self) {
        self.0.lock().clear();
    }

    /// A miner flooding faster than its session reads is disconnected, like
    /// a TCP miner whose inbound queue overflows.
    fn deliver(&self, conn: u64, line: String, frame_tx: &mpsc::Sender<Frame>) {
        let tx = self.0.lock().get(&conn).cloned();
        if let Some(tx) = tx {
            if tx.try_send(line).is_err() {
                self.close(conn, frame_tx);
            }
        }
    }

    fn close(&self, conn: u64, frame_tx: &mpsc::Sender<Frame>) {
        if self.remove(conn) {
            let _ = frame_tx.try_send(Frame::new(FrameKind::Close, conn, Vec::new()));
        }
    }
}

pub struct StratumServer {
    shortname_resolver: Arc<crate::shortname::ShortnameResolver>,
    listen_addr: SocketAddr,
    engine: Arc<PoolEngine>,
    jobs: Arc<JobManager>,
    stats: Arc<PoolStats>,
    submit_dispatcher: Arc<SubmitDispatcher>,
    conn_state: Arc<Mutex<ConnState>>,
    max_conns: usize,
    max_conns_per_ip: usize,
    notifications: broadcast::Sender<StratumNotify>,
    post_login_idle_timeout: Duration,
    submit_rate_limit_window: Duration,
    submit_rate_limit_max: usize,
    /// `refill` limits per connection (`stratum_refill_*` config).
    refill_rate: f64,
    refill_burst: f64,
    refill_max_assignments: usize,
    /// New template → job written to each connection.
    job_fanout: Arc<NamedLatencyHistograms>,
    /// Logged-in connections, which the job fanout delivers each new
    /// template's job to (see `fan_out_job`).
    fanout_conns: Mutex<HashMap<String, Arc<FanoutConn>>>,
    /// Miner-reported timings carried on submits (`submit_timing_v1`).
    miner_submit_timing: NamedLatencyHistograms,
    /// Newest stable seine release, for the once-per-connection upgrade notice.
    miner_release: crate::miner_release::MinerRelease,
}

impl StratumServer {
    pub(crate) fn new(
        listen_addr: SocketAddr,
        engine: Arc<PoolEngine>,
        jobs: Arc<JobManager>,
        stats: Arc<PoolStats>,
        cfg: Config,
    ) -> Arc<Self> {
        let (notifications, _) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
        let submit_dispatcher = SubmitDispatcher::new(Arc::clone(&engine), &cfg);
        Arc::new(Self {
            shortname_resolver: Arc::new(crate::shortname::ShortnameResolver::default()),
            listen_addr,
            engine,
            jobs,
            stats,
            submit_dispatcher,
            conn_state: Arc::new(Mutex::new(ConnState {
                counts: HashMap::new(),
                total: 0,
                rejected_at_cap: 0,
                rejected_per_ip: 0,
            })),
            max_conns: cfg.stratum_max_connections.max(1) as usize,
            max_conns_per_ip: cfg.stratum_max_connections_per_ip.max(1) as usize,
            notifications,
            post_login_idle_timeout: POST_LOGIN_IDLE_TIMEOUT,
            submit_rate_limit_window: SUBMIT_RATE_LIMIT_WINDOW,
            submit_rate_limit_max: SUBMIT_RATE_LIMIT_MAX,
            refill_rate: cfg.stratum_refill_rate,
            refill_burst: f64::from(cfg.stratum_refill_burst.max(1)),
            refill_max_assignments: cfg.stratum_refill_max_assignments.max(1) as usize,
            job_fanout: Arc::default(),
            fanout_conns: Mutex::new(HashMap::new()),
            miner_submit_timing: NamedLatencyHistograms::default(),
            miner_release: crate::miner_release::MinerRelease::new(&cfg.latest_miner_version),
        })
    }

    /// Keeps the newest-release version fresh for the upgrade notice; returns
    /// the task so callers can drop it with the server.
    fn spawn_release_refresh(&self) -> tokio::task::JoinHandle<()> {
        let release = self.miner_release.clone();
        tokio::spawn(async move { release.refresh_forever().await })
    }

    /// Cumulative latency histograms by metric family, then lane.
    pub(crate) fn latency_histograms(
        &self,
    ) -> BTreeMap<String, BTreeMap<String, LatencyHistogramSnapshot>> {
        let dispatcher = &self.submit_dispatcher;
        let queue_waits = [
            ("candidate", SubmitLane::Candidate),
            ("priority_candidate", SubmitLane::PriorityCandidate),
            ("regular", SubmitLane::Regular),
        ]
        .into_iter()
        .map(|(lane, queue)| {
            (
                lane.to_string(),
                dispatcher.lane_queue(queue).wait_histogram(),
            )
        })
        .collect();
        BTreeMap::from([
            ("submit_ack".to_string(), dispatcher.ack_latency.snapshot()),
            ("submit_queue_wait".to_string(), queue_waits),
            (
                "block_submit".to_string(),
                self.engine.block_submit_histograms(),
            ),
            (
                "block_candidate".to_string(),
                self.engine.block_candidate_histograms(),
            ),
            (
                "tip_refresh".to_string(),
                self.jobs.tip_latency_histograms(),
            ),
            ("job_fanout".to_string(), self.job_fanout.snapshot()),
            (
                "miner_submit_timing".to_string(),
                self.miner_submit_timing.snapshot(),
            ),
        ])
    }

    /// Miner-reported timings are self-reported, so they only feed these
    /// histograms; nothing is accounted or judged from them.
    fn record_miner_submit_timing(&self, params: &SubmitParams, route: SubmitQueueRoute) {
        if let Some(micros) = params.found_to_send_us {
            let lane = match route {
                SubmitQueueRoute::Candidate => "found_to_send_candidate",
                SubmitQueueRoute::Regular => "found_to_send_share",
            };
            self.miner_submit_timing
                .record(lane, Duration::from_micros(micros));
        }
        if let Some(micros) = params.job_switch_us {
            self.miner_submit_timing
                .record("job_switch", Duration::from_micros(micros));
        }
    }

    pub(crate) fn submit_snapshot(&self) -> SubmitRuntimeSnapshot {
        let mut snapshot = self.submit_dispatcher.snapshot();
        snapshot.unprocessed = self.engine.admission().snapshot();
        snapshot.growth_mode_active = self.engine.growth_mode_active();
        snapshot.growth_level = self.engine.growth_level();
        snapshot.growth_normal_size_submits_per_s = self.engine.growth_normal_size_rate();
        snapshot.connections = Some(self.connection_snapshot());
        snapshot.heap = heap_snapshot();
        snapshot
    }

    fn connection_snapshot(&self) -> ConnectionSnapshot {
        let state = self.conn_state.lock();
        ConnectionSnapshot {
            open: state.total,
            max: self.max_conns,
            max_per_ip: self.max_conns_per_ip,
            rejected_at_cap_total: state.rejected_at_cap,
            rejected_per_ip_total: state.rejected_per_ip,
        }
    }

    /// Delivers every new template's job to all logged-in connections from
    /// one task. Connection tasks used to wake per template and build their
    /// own job under the process-wide job-state lock; with thousands of
    /// connections that queued them behind each other and behind runtime
    /// scheduling (fanout p99 ~240 ms at 5k connections on bntpool).
    fn spawn_job_fanout(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let server = Arc::clone(self);
        let mut rx_jobs = self.jobs.subscribe();
        tokio::spawn(async move {
            loop {
                match rx_jobs.recv().await {
                    Ok(job) => {
                        let server = Arc::clone(&server);
                        let _ = tokio::task::spawn_blocking(move || server.fan_out_job(&job)).await;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "job fanout lagged; serving the newest job");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        })
    }

    /// Retargets each logged-in connection, builds its job and queues it.
    /// Connections are taken in chunks by a few threads, each chunk going
    /// through retarget, one job-state lock pass and queueing before the
    /// next, so the first miners get the job within milliseconds and the
    /// lock is taken per chunk, not per connection.
    fn fan_out_job(&self, job: &Job) {
        let template_created_at = job.template_created_at;
        let conns: Vec<(String, Arc<FanoutConn>)> = self
            .fanout_conns
            .lock()
            .iter()
            .map(|(conn_id, conn)| (conn_id.clone(), Arc::clone(conn)))
            .collect();
        let chunks: Vec<&[(String, Arc<FanoutConn>)]> =
            conns.chunks(FANOUT_CHUNK_CONNECTIONS).collect();
        let next_chunk = AtomicUsize::new(0);
        let hints = Mutex::new(Vec::new());
        let threads = fanout_threads(conns.len());
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let index = next_chunk.fetch_add(1, Ordering::Relaxed);
                    let Some(chunk) = chunks.get(index) else {
                        return;
                    };
                    let chunk_hints = self.fan_out_chunk(chunk, template_created_at);
                    if !chunk_hints.is_empty() {
                        hints.lock().extend(chunk_hints);
                    }
                });
            }
        });

        let hints = hints.into_inner();
        if !hints.is_empty() {
            let engine = Arc::clone(&self.engine);
            tokio::task::spawn_blocking(move || {
                for hint in hints {
                    engine.persist_vardiff_hint(hint);
                }
            });
        }
    }

    /// One chunk of `fan_out_job`; returns the vardiff hints to persist.
    fn fan_out_chunk(
        &self,
        chunk: &[(String, Arc<FanoutConn>)],
        template_created_at: Instant,
    ) -> Vec<crate::engine::VardiffHint> {
        let reached_at = Instant::now();
        self.job_fanout.record(
            "template_to_chunk",
            reached_at.saturating_duration_since(template_created_at),
        );
        let mut requests = Vec::with_capacity(chunk.len());
        let mut targets = Vec::with_capacity(chunk.len());
        let mut hints = Vec::new();
        for (conn_id, conn) in chunk {
            if let Some(message) = self.engine.faraday_upgrade_required(conn_id) {
                // Its next login gets the same message as an error.
                conn.kick(message.to_string());
                continue;
            }
            let current = conn.difficulty.load(Ordering::Relaxed);
            // In memory only; hints are persisted after the jobs go out.
            let (next, hint) = self
                .engine
                .retarget_on_job(conn_id)
                .unwrap_or((current, None));
            hints.extend(hint);
            if next != current {
                conn.difficulty.store(next, Ordering::Relaxed);
                tracing::debug!(
                    conn = %conn_id,
                    address = %conn.address,
                    difficulty = next,
                    "stratum difficulty updated on job tick"
                );
            }
            requests.push(MinerJobRequest {
                conn_id: conn_id.clone(),
                share_difficulty: next,
                assigned_miner: conn.address.clone(),
                range_mode: AssignmentRangeMode::Fresh,
            });
            targets.push(conn);
        }
        let build_started_at = Instant::now();
        self.job_fanout.record(
            "retarget_chunk",
            build_started_at.saturating_duration_since(reached_at),
        );
        let built = self.jobs.build_miner_jobs(&requests);
        self.job_fanout
            .record("build_chunk", build_started_at.elapsed());
        for (conn, miner_job) in targets.into_iter().zip(built) {
            let Some(miner_job) = miner_job else {
                continue;
            };
            let notify = JobNotify {
                method: "job",
                params: &miner_job,
            };
            if queue_job_json(&conn.outbound, &notify, Some(template_created_at)).is_ok() {
                self.job_fanout
                    .record("template_to_queued", template_created_at.elapsed());
            }
        }
        hints
    }

    fn register_fanout_conn(&self, conn_id: &str, conn: FanoutConn) {
        self.fanout_conns
            .lock()
            .insert(conn_id.to_string(), Arc::new(conn));
    }

    fn deregister_fanout_conn(&self, conn_id: &str) {
        self.fanout_conns.lock().remove(conn_id);
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        tracing::info!(addr = %self.listen_addr, "stratum listening");
        let refresh = self.spawn_release_refresh();
        let result = self.run_tcp_listener(listener).await;
        refresh.abort();
        result
    }

    /// Serves miners relayed by the Stratum gateway (STRATUM_GATEWAY.md):
    /// connects to `socket` (again whenever the gateway restarts) until the
    /// gateway retires this engine, then returns.
    pub async fn run_gateway_client(
        self: Arc<Self>,
        socket: std::path::PathBuf,
        instance: String,
        role: Arc<EngineRole>,
    ) -> Result<()> {
        let started_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let hello = EngineHello {
            instance,
            pid: std::process::id(),
            started_at_ms,
        };
        let _refresh = AbortOnDrop(self.spawn_release_refresh());
        let _fanout = AbortOnDrop(self.spawn_job_fanout());
        let mut connect_warned = false;
        loop {
            match tokio::net::UnixStream::connect(&socket).await {
                Ok(stream) => {
                    connect_warned = false;
                    tracing::info!(socket = %socket.display(), instance = %hello.instance, "connected to stratum gateway");
                    match Arc::clone(&self).serve_gateway(stream, &hello, &role).await {
                        Ok(GatewaySession::Retired) => {
                            tracing::info!("retired by stratum gateway");
                            return Ok(());
                        }
                        Ok(GatewaySession::Lost) => {
                            tracing::warn!("stratum gateway connection closed; reconnecting");
                        }
                        Err(err) => {
                            tracing::warn!(error = %format!("{err:#}"), "stratum gateway session failed; reconnecting");
                        }
                    }
                }
                Err(err) if !connect_warned => {
                    connect_warned = true;
                    tracing::warn!(socket = %socket.display(), error = %err, "stratum gateway unavailable; retrying");
                }
                Err(_) => {}
            }
            tokio::time::sleep(GATEWAY_RECONNECT_DELAY).await;
        }
    }

    async fn serve_gateway(
        self: Arc<Self>,
        stream: tokio::net::UnixStream,
        hello: &EngineHello,
        role: &EngineRole,
    ) -> Result<GatewaySession> {
        let (mut reader, mut writer) = stream.into_split();
        let (frame_tx, mut frame_rx) = mpsc::channel::<Frame>(GATEWAY_FRAME_QUEUE);
        let mut writer_task = tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                write_frame(&mut writer, &frame).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        frame_tx
            .send(Frame::new(FrameKind::Hello, 0, serde_json::to_vec(hello)?))
            .await
            .map_err(|_| anyhow!("gateway writer stopped"))?;

        let conns = GatewayConns::default();
        let outcome = loop {
            let frame = tokio::select! {
                frame = read_frame(&mut reader) => frame,
                written = &mut writer_task => {
                    break match written {
                        Ok(Ok(())) => Ok(GatewaySession::Lost),
                        Ok(Err(err)) => Err(err),
                        Err(err) => Err(anyhow!("gateway writer task failed: {err}")),
                    };
                }
            };
            let frame = match frame {
                Ok(Some(frame)) => frame,
                Ok(None) => break Ok(GatewaySession::Lost),
                Err(err) => break Err(err),
            };
            match frame.kind {
                FrameKind::Role => match frame.payload_str().and_then(GatewayRole::parse) {
                    Ok(GatewayRole::Primary) => role.set_primary(true),
                    Ok(GatewayRole::Draining) => role.set_primary(false),
                    Err(err) => tracing::warn!(error = %err, "ignoring malformed gateway role"),
                },
                FrameKind::Open => {
                    let peer = frame
                        .payload_str()
                        .ok()
                        .and_then(|peer| peer.parse::<SocketAddr>().ok());
                    match peer {
                        Some(peer) => self.open_gateway_conn(frame.conn, peer, &conns, &frame_tx),
                        None => {
                            tracing::warn!(
                                conn = frame.conn,
                                "gateway opened a connection without a valid peer"
                            );
                            let _ = frame_tx.try_send(Frame::new(
                                FrameKind::Close,
                                frame.conn,
                                Vec::new(),
                            ));
                        }
                    }
                }
                FrameKind::Line => match String::from_utf8(frame.payload) {
                    Ok(line) => conns.deliver(frame.conn, line, &frame_tx),
                    Err(_) => conns.close(frame.conn, &frame_tx),
                },
                FrameKind::Close => {
                    conns.remove(frame.conn);
                }
                FrameKind::Retire => break Ok(GatewaySession::Retired),
                FrameKind::Hello => tracing::warn!("ignoring unexpected hello from gateway"),
            }
        };
        // Dropping every sender ends every virtual connection's session.
        conns.clear();
        writer_task.abort();
        outcome
    }

    /// Starts a session for a miner relayed by the gateway. The session runs
    /// the same handler as a TCP miner over an in-memory duplex stream; two
    /// pumps move its lines to and from gateway frames.
    fn open_gateway_conn(
        self: &Arc<Self>,
        conn: u64,
        peer: SocketAddr,
        conns: &GatewayConns,
        frame_tx: &mpsc::Sender<Frame>,
    ) {
        let ip = ip_group_key(peer.ip());
        if !self.try_track_conn(&ip) {
            tracing::warn!(ip = %ip, "rejecting relayed stratum connection due to limits");
            let _ = frame_tx.try_send(Frame::new(
                FrameKind::Line,
                conn,
                connection_limit_line().into_bytes(),
            ));
            let _ = frame_tx.try_send(Frame::new(FrameKind::Close, conn, Vec::new()));
            return;
        }
        let (engine_io, gateway_io) = tokio::io::duplex(GATEWAY_CONN_BUFFER);
        let (gateway_read, mut gateway_write) = tokio::io::split(gateway_io);
        let (line_tx, mut line_rx) = mpsc::channel::<String>(INBOUND_QUEUE_CAPACITY);
        conns.insert(conn, line_tx);

        // Gateway → session.
        tokio::spawn(async move {
            while let Some(line) = line_rx.recv().await {
                let mut bytes = line.into_bytes();
                bytes.push(b'\n');
                if gateway_write.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            let _ = gateway_write.shutdown().await;
        });

        // Session → gateway; the session ending closes the connection.
        let pump_tx = frame_tx.clone();
        let pump_conns = conns.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(gateway_read);
            while let Ok(Some(line)) = read_line_limited(&mut reader, MAX_FRAME_PAYLOAD).await {
                if pump_tx
                    .send(Frame::new(FrameKind::Line, conn, line))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            if pump_conns.remove(conn) {
                let _ = pump_tx
                    .send(Frame::new(FrameKind::Close, conn, Vec::new()))
                    .await;
            }
        });

        let this = Arc::clone(self);
        tokio::spawn(async move {
            let (engine_read, engine_write) = tokio::io::split(engine_io);
            if let Err(err) = this
                .handle_stream_conn(engine_read, engine_write, peer)
                .await
            {
                tracing::warn!(peer = %peer, error = %err, "relayed stratum connection ended with error");
            }
            this.untrack_conn(&ip);
        });
    }

    fn try_track_conn(&self, ip: &str) -> bool {
        let mut state = self.conn_state.lock();
        if state.total >= self.max_conns {
            state.rejected_at_cap += 1;
            return false;
        }
        let max_per_ip = self.max_conns_per_ip;
        let count = state.counts.entry(ip.to_string()).or_default();
        if *count >= max_per_ip {
            state.rejected_per_ip += 1;
            return false;
        }
        *count += 1;
        state.total += 1;
        true
    }

    fn untrack_conn(&self, ip: &str) {
        let mut state = self.conn_state.lock();
        if let Some(count) = state.counts.get_mut(ip) {
            if *count > 1 {
                *count -= 1;
            } else {
                state.counts.remove(ip);
            }
        }
        if state.total > 0 {
            state.total -= 1;
        }
    }

    async fn run_tcp_listener(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let _fanout = AbortOnDrop(self.spawn_job_fanout());
        loop {
            let (stream, peer) = listener.accept().await?;
            // Per-IP cap groups IPv6 by /64 (one host allocation), so a single
            // machine cannot multiply its connection budget across addresses.
            let ip = ip_group_key(peer.ip());
            if !self.try_track_conn(&ip) {
                tracing::warn!(ip = %ip, "rejecting stratum connection due to limits");
                let mut line = connection_limit_line();
                line.push('\n');
                let mut stream = stream;
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(5),
                        stream.write_all(line.as_bytes()),
                    )
                    .await;
                });
                continue;
            }

            let this = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(err) = this.handle_tcp_conn(stream, peer).await {
                    tracing::warn!(peer = %peer, error = %err, "stratum tcp connection ended with error");
                }
                this.untrack_conn(&ip);
            });
        }
    }

    async fn handle_tcp_conn(&self, stream: TcpStream, peer: SocketAddr) -> Result<()> {
        let (reader_half, writer_half) = stream.into_split();
        self.handle_stream_conn(reader_half, writer_half, peer)
            .await
    }

    /// Serves one miner over any byte stream: a TCP socket, or a virtual
    /// connection relayed by the gateway carrying the miner's real address.
    async fn handle_stream_conn<R, W>(
        &self,
        reader_half: R,
        writer_half: W,
        peer: SocketAddr,
    ) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let conn_id = peer.to_string();
        let (inbound_tx, inbound_rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
        let reader_task =
            tokio::spawn(
                async move { run_tcp_reader(BufReader::new(reader_half), inbound_tx).await },
            );
        let (outbound, outbound_rx) =
            OutboundHandle::with_job_fanout(OUTBOUND_QUEUE_CAPACITY, Arc::clone(&self.job_fanout));
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        let job_fanout = Arc::clone(&outbound.job_fanout);
        let writer_task = tokio::spawn(async move {
            run_tcp_outbound_writer(
                writer_half,
                outbound_rx,
                latest_job,
                latest_job_notify,
                job_fanout,
            )
            .await
        });
        self.handle_conn_loop(
            peer,
            conn_id,
            inbound_rx,
            reader_task,
            writer_task,
            outbound,
        )
        .await
    }

    async fn handle_conn_loop(
        &self,
        peer: SocketAddr,
        conn_id: String,
        mut inbound_rx: mpsc::Receiver<InboundFrame>,
        reader_task: tokio::task::JoinHandle<Result<()>>,
        mut writer_task: tokio::task::JoinHandle<Result<()>>,
        outbound: OutboundHandle,
    ) -> Result<()> {
        let mut writer_task_finished = false;
        let mut logged_in: Option<(String, String, u64)> = None;
        // One upgrade notice per connection, sent with the first submit that
        // reports an older miner version (login carries no version).
        let mut update_notice_sent = false;
        // The job fanout delivers template jobs; it can also ask this task to
        // close the session, and it updates the difficulty it shares here.
        let kick = Arc::new(Notify::new());
        let kick_reason = Arc::new(Mutex::new(None::<String>));
        let mut shared_difficulty: Option<Arc<AtomicU64>> = None;
        let mut rx_notifications = self.notifications.subscribe();
        let post_login_idle_timeout = self.post_login_idle_timeout;
        let submit_rate_limit_window = self.submit_rate_limit_window;
        let submit_rate_limit_max = self.submit_rate_limit_max;
        let mut submit_timestamps = VecDeque::<Instant>::new();
        let mut refill_bucket =
            RefillBucket::new(self.refill_rate, self.refill_burst, Instant::now());
        let (submit_result_tx, mut submit_result_rx) = mpsc::unbounded_channel();

        let login_deadline = tokio::time::sleep(LOGIN_TIMEOUT);
        tokio::pin!(login_deadline);
        let idle_deadline = tokio::time::sleep(post_login_idle_timeout);
        tokio::pin!(idle_deadline);

        let mut run_result = Ok(());
        // Difficulty seen when the last inbound frame was handled; published
        // at the top of the next iteration, which every `continue` reaches.
        let mut inbound_seen: Option<Option<u64>> = None;
        loop {
            if let Some(seen) = inbound_seen.take() {
                sync_difficulty_out(&logged_in, shared_difficulty.as_ref(), seen);
            }
            tokio::select! {
                writer_result = &mut writer_task => {
                    writer_task_finished = true;
                    run_result = match writer_result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(err)) => Err(err),
                        Err(err) => Err(anyhow!("stratum writer task failed: {err}")),
                    };
                    break;
                }
                _ = &mut login_deadline, if logged_in.is_none() => {
                    tracing::warn!(peer = %peer, "stratum login timeout");
                    break;
                }
                _ = &mut idle_deadline, if logged_in.is_some() => {
                    tracing::warn!(peer = %peer, "stratum idle timeout");
                    break;
                }
                maybe_submit_result = submit_result_rx.recv() => {
                    if let Some(completion) = maybe_submit_result {
                        let seen = sync_difficulty_in(&mut logged_in, shared_difficulty.as_ref());
                        let handled = self.handle_submit_completion(
                            peer,
                            &conn_id,
                            Some(&outbound),
                            &mut logged_in,
                            completion,
                        );
                        sync_difficulty_out(&logged_in, shared_difficulty.as_ref(), seen);
                        if let Err(err) = handled {
                            run_result = Err(err);
                            break;
                        }
                    }
                }
                _ = kick.notified(), if logged_in.is_some() => {
                    let reason = kick_reason
                        .lock()
                        .take()
                        .unwrap_or_else(|| "session closed by the pool".to_string());
                    tracing::info!(peer = %peer, "closing session without BLAKE2b support at the fork");
                    run_result = Err(anyhow::anyhow!(reason));
                    break;
                }
                maybe_notification = rx_notifications.recv(), if logged_in.is_some() => {
                    match maybe_notification {
                        Ok(notification) => {
                            if let Err(err) = queue_json(&outbound, &notification) {
                                run_result = Err(err);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::debug!(peer = %peer, skipped, "stratum notification receiver lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                maybe_inbound = inbound_rx.recv() => {
                    let inbound = match maybe_inbound {
                        Some(v) => v,
                        None => break,
                    };
                    let line = match inbound {
                        InboundFrame::Text(line) => line,
                        InboundFrame::ReadError(err) => {
                            let _ = queue_error(&outbound, 0, &err);
                            break;
                        }
                    };
                    if logged_in.is_some() {
                        idle_deadline
                            .as_mut()
                            .reset(tokio::time::Instant::now() + post_login_idle_timeout);
                    }
                    inbound_seen = Some(sync_difficulty_in(&mut logged_in, shared_difficulty.as_ref()));
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let req: StratumRequest = match serde_json::from_str(trimmed) {
                        Ok(v) => v,
                        Err(_) => {
                            if let Err(err) = queue_error(&outbound, 0, "invalid JSON") {
                                run_result = Err(err);
                                break;
                            }
                            continue;
                        }
                    };

                    match req.method.as_str() {
                        METHOD_LOGIN if logged_in.is_some() => {
                            // One login per connection: re-logins would mint new
                            // sessions and assignments without a new socket.
                            if let Err(err) = queue_error(&outbound, req.id, "already logged in") {
                                run_result = Err(err);
                                break;
                            }
                        }
                        METHOD_LOGIN => {
                            let params: LoginParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    if let Err(err) =
                                        queue_error(&outbound, req.id, "invalid login params")
                                    {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                }
                            };

                            let login_engine = Arc::clone(&self.engine);
                            let shortname_resolver = Arc::clone(&self.shortname_resolver);
                            let login_conn_id = conn_id.clone();
                            let login_address = params.address.trim().to_string();
                            let login_worker = params.worker.clone();
                            let login_protocol_version = params.protocol_version;
                            let login_capabilities = params.capabilities.clone();
                            let login_hint = params.difficulty_hint;
                            let login_peer_ip = peer.ip();
                            let login = tokio::task::spawn_blocking(move || {
                                let login_address = shortname_resolver.resolve(&login_address)?;
                                let result = login_engine.login_from_peer(
                                    &login_conn_id,
                                    Some(login_peer_ip),
                                    login_address.clone(),
                                    &login_worker,
                                    login_protocol_version,
                                    login_capabilities,
                                    login_hint,
                                )?;
                                Ok::<_, anyhow::Error>((result, login_address))
                            })
                            .await;

                            match login {
                                Ok(Ok((login_result, address))) => {
                                    let worker =
                                        normalize_worker_name(params.worker.as_str());
                                    let difficulty =
                                        self.engine.session_difficulty(&conn_id).unwrap_or(1);

                                    logged_in =
                                        Some((address.clone(), worker.clone(), difficulty));
                                    self.stats.add_miner(&conn_id, &address, &worker);

                                    let response = StratumResponse {
                                        id: req.id,
                                        status: Some("ok".to_string()),
                                        error: None,
                                        result: Some(serde_json::to_value(login_result)?),
                                    };
                                    if let Err(err) = queue_json(&outbound, &response) {
                                        run_result = Err(err);
                                        break;
                                    }

                                    let login_template = match self.jobs.build_miner_job(
                                        &conn_id,
                                        difficulty,
                                        &address,
                                    ) {
                                        Some(miner_job) => {
                                            let template_id = miner_job.template_id.clone();
                                            let notify = StratumNotify {
                                                method: "job".to_string(),
                                                params: serde_json::to_value(miner_job)?,
                                            };
                                            if let Err(err) = queue_json(&outbound, &notify) {
                                                run_result = Err(err);
                                                break;
                                            }
                                            Some(template_id)
                                        }
                                        None => None,
                                    };
                                    // From here the job fanout serves this connection.
                                    let shared = Arc::new(AtomicU64::new(difficulty));
                                    self.register_fanout_conn(
                                        &conn_id,
                                        FanoutConn {
                                            address: address.clone(),
                                            difficulty: Arc::clone(&shared),
                                            outbound: outbound.clone(),
                                            kick: Arc::clone(&kick),
                                            kick_reason: Arc::clone(&kick_reason),
                                        },
                                    );
                                    shared_difficulty = Some(shared);
                                    // A template installed between the login job and
                                    // registration was fanned out without us.
                                    let current_template = self
                                        .jobs
                                        .current_job()
                                        .map(|job| job.template_id);
                                    if current_template.is_some() && current_template != login_template {
                                        if let Some(miner_job) =
                                            self.jobs.build_miner_job(&conn_id, difficulty, &address)
                                        {
                                            let notify = JobNotify {
                                                method: "job",
                                                params: &miner_job,
                                            };
                                            if let Err(err) = queue_job_json(&outbound, &notify, None) {
                                                run_result = Err(err);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Ok(Err(err)) => {
                                    let _ = queue_error(&outbound, req.id, &format!("{err:#}"));
                                    break;
                                }
                                Err(err) => {
                                    let _ = queue_error(
                                        &outbound,
                                        req.id,
                                        &format!("login worker failure: {err}"),
                                    );
                                    break;
                                }
                            }
                        }
                        METHOD_SUBMIT => {
                            let params: SubmitParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    if let Err(err) =
                                        queue_error(&outbound, req.id, "invalid submit params")
                                    {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                }
                            };
                            if !update_notice_sent {
                                if let Some(message) =
                                    self.miner_update_notice(params.miner_version.as_deref())
                                {
                                    update_notice_sent = true;
                                    let notify =
                                        block_notification(NOTIFY_MINER_UPDATE_AVAILABLE, &message);
                                    if let Err(err) = queue_json(&outbound, &notify) {
                                        run_result = Err(err);
                                        break;
                                    }
                                }
                            }
                            self.submit_dispatcher.record_submit_received();
                            let now = Instant::now();
                            let cutoff = now.checked_sub(submit_rate_limit_window).unwrap_or(now);
                            while submit_timestamps
                                .front()
                                .is_some_and(|ts| *ts < cutoff)
                            {
                                submit_timestamps.pop_front();
                            }
                            if submit_timestamps.len() >= submit_rate_limit_max {
                                if let Err(err) =
                                    queue_error(&outbound, req.id, "rate limited, retry")
                                {
                                    run_result = Err(err);
                                    break;
                                }
                                if let Err(err) = self.note_unprocessed_submit(
                                    &conn_id,
                                    &mut logged_in,
                                    UnprocessedReason::RateLimited,
                                    &outbound,
                                ) {
                                    run_result = Err(err);
                                    break;
                                }
                                continue;
                            }
                            submit_timestamps.push_back(now);
                            let received_at = Instant::now();
                            let submit_job_id = params.job_id.clone();
                            let submit_nonce = params.nonce;
                            let submit_claimed_hash = params.claimed_hash.clone();
                            let prequeue_metadata = submit_metadata(&params);
                            let route = match self
                                .engine
                                .preclassify_submit_route(&params.job_id, params.claimed_hash.as_deref(), received_at)
                            {
                                Ok(route) => route,
                                Err(err) => {
                                    self.persist_prequeue_reject_async(
                                        &conn_id,
                                        &params.job_id,
                                        params.nonce,
                                        params.claimed_hash.as_deref(),
                                        received_at,
                                        &err.to_string(),
                                        submit_metadata(&params),
                                    );
                                    if let Err(queue_err) = queue_error(&outbound, req.id, &err.to_string()) {
                                        run_result = Err(queue_err);
                                        break;
                                    }
                                    continue;
                                }
                            };
                            if route == SubmitQueueRoute::Candidate {
                                self.submit_dispatcher.record_candidate_classified();
                            }
                            self.record_miner_submit_timing(&params, route);
                            let established_source = route == SubmitQueueRoute::Candidate
                                && self.engine.source_established(&conn_id);
                            let lane = match route {
                                SubmitQueueRoute::Regular => SubmitLane::Regular,
                                SubmitQueueRoute::Candidate if established_source => {
                                    SubmitLane::PriorityCandidate
                                }
                                SubmitQueueRoute::Candidate => SubmitLane::Candidate,
                            };
                            let candidate_permit = if route == SubmitQueueRoute::Candidate {
                                let Some((address, _, _)) = logged_in.as_ref() else {
                                    if let Err(err) = queue_error(&outbound, req.id, "not logged in") {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                };
                                match self.submit_dispatcher.try_acquire_candidate_permit(
                                    &conn_id,
                                    address,
                                    peer,
                                    established_source,
                                    received_at,
                                )
                                {
                                    Ok(permit) => Some(permit),
                                    Err(err) => {
                                        self.persist_prequeue_reject_async(
                                            &conn_id,
                                            &params.job_id,
                                            params.nonce,
                                            params.claimed_hash.as_deref(),
                                            received_at,
                                            &err.to_string(),
                                            submit_metadata(&params),
                                        );
                                        if let Err(queue_err) = queue_error(&outbound, req.id, &err.to_string()) {
                                            run_result = Err(queue_err);
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            } else {
                                None
                            };
                            let source = (lane == SubmitLane::Regular).then(|| QueuedSource {
                                address: logged_in
                                    .as_ref()
                                    .map(|(address, _, _)| address.clone())
                                    .unwrap_or_else(|| format!("conn|{conn_id}")),
                                ip_group: ip_group_key(peer.ip()),
                            });
                            let work = SubmitWorkItem {
                                conn_id: conn_id.clone(),
                                req_id: req.id,
                                params,
                                received_at,
                                completion_tx: submit_result_tx.clone(),
                                candidate_permit,
                                source,
                            };
                            if let Err(rejection) = self.submit_dispatcher.try_enqueue(work, lane) {
                                if let Some((address, _, _)) = logged_in.as_ref() {
                                    tracing::debug!(
                                        peer = %peer,
                                        address = %address,
                                        job_id = %submit_job_id,
                                        nonce = submit_nonce,
                                        reason = ?rejection,
                                        "submit queue saturated"
                                    );
                                }
                                let err_text = rejection.message();
                                self.persist_prequeue_reject_async(
                                    &conn_id,
                                    &submit_job_id,
                                    submit_nonce,
                                    submit_claimed_hash.as_deref(),
                                    received_at,
                                    err_text,
                                    prequeue_metadata,
                                );
                                if let Err(queue_err) = queue_error(&outbound, req.id, err_text) {
                                    run_result = Err(queue_err);
                                    break;
                                }
                                if lane == SubmitLane::Regular {
                                    if let Err(err) = self.note_unprocessed_submit(
                                        &conn_id,
                                        &mut logged_in,
                                        rejection.reason(),
                                        &outbound,
                                    ) {
                                        run_result = Err(err);
                                        break;
                                    }
                                }
                            }
                        }
                        METHOD_REFILL => {
                            if logged_in.is_none() {
                                if let Err(err) = queue_error(&outbound, req.id, "not logged in") {
                                    run_result = Err(err);
                                    break;
                                }
                                continue;
                            }
                            if !refill_bucket.try_take(Instant::now()) {
                                if let Err(err) =
                                    queue_error(&outbound, req.id, "refill rate limited")
                                {
                                    run_result = Err(err);
                                    break;
                                }
                                continue;
                            }
                            let params: RefillParams = match serde_json::from_value(req.params.clone()) {
                                Ok(v) => v,
                                Err(_) => {
                                    if let Err(err) =
                                        queue_error(&outbound, req.id, "invalid refill params")
                                    {
                                        run_result = Err(err);
                                        break;
                                    }
                                    continue;
                                }
                            };
                            let queued = match self.jobs.refill_miner_job(
                                &conn_id,
                                &params.job_id,
                                self.refill_max_assignments,
                            ) {
                                Ok(miner_job) => {
                                    let response = StratumResponse {
                                        id: req.id,
                                        status: Some("ok".to_string()),
                                        error: None,
                                        result: Some(serde_json::to_value(miner_job)?),
                                    };
                                    queue_json(&outbound, &response)
                                }
                                Err(err) => {
                                    tracing::debug!(
                                        peer = %peer,
                                        job_id = %params.job_id,
                                        reason = err.message(),
                                        "stratum refill refused"
                                    );
                                    queue_error(&outbound, req.id, err.message())
                                }
                            };
                            if let Err(err) = queued {
                                run_result = Err(err);
                                break;
                            }
                        }
                        _ => {
                            if let Err(err) = queue_error(&outbound, req.id, "unknown method") {
                                run_result = Err(err);
                                break;
                            }
                        }
                    }
                }
            }
        }

        // The registry holds a clone of `outbound`; release it so the writer
        // sees its channel close.
        if shared_difficulty.is_some() {
            self.deregister_fanout_conn(&conn_id);
        }
        drop(outbound);
        if !writer_task_finished {
            let writer_result = writer_task.await;
            if run_result.is_ok() {
                run_result = match writer_result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(err)) => Err(err),
                    Err(err) => Err(anyhow!("stratum writer task failed: {err}")),
                };
            }
        }
        match reader_task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if run_result.is_ok() {
                    run_result = Err(err);
                }
            }
            Err(err) => {
                if run_result.is_ok() {
                    run_result = Err(anyhow!("stratum reader task failed: {err}"));
                }
            }
        }

        if let Some((address, _, _)) = logged_in.take() {
            let engine = Arc::clone(&self.engine);
            let disconnect_conn_id = conn_id.clone();
            let _ = tokio::task::spawn_blocking(move || {
                engine.disconnect(&disconnect_conn_id);
            })
            .await;
            self.stats.remove_miner(&conn_id);
            tracing::debug!(peer = %peer, address = %address, "stratum miner disconnected");
        }

        run_result
    }

    fn handle_submit_completion(
        &self,
        peer: SocketAddr,
        conn_id: &str,
        outbound: Option<&OutboundHandle>,
        logged_in: &mut Option<(String, String, u64)>,
        completion: SubmitCompletion,
    ) -> Result<()> {
        match completion.outcome {
            SubmitCompletionOutcome::Finished(Ok(ack)) => {
                let mut finder_address = None::<String>;
                let mut finder_worker = None::<String>;
                let mut response_difficulty = ack.next_difficulty;
                if let Some((address, worker, difficulty)) = logged_in.as_mut() {
                    self.stats.record_accepted_share(ack.share_difficulty);
                    if ack.block_accepted {
                        finder_address = Some(address.clone());
                        finder_worker = Some(worker.clone());
                    }
                    response_difficulty =
                        client_submit_ack_difficulty(address, *difficulty, ack.next_difficulty);
                    if ack.next_difficulty != *difficulty {
                        if is_seine_dev_fee_address(address) {
                            tracing::debug!(
                                peer = %peer,
                                address = %address,
                                worker = %worker,
                                active_difficulty = *difficulty,
                                deferred_difficulty = ack.next_difficulty,
                                "stratum deferred difficulty update until next template"
                            );
                        } else {
                            *difficulty = ack.next_difficulty;
                            self.queue_difficulty_job(
                                conn_id,
                                address,
                                ack.next_difficulty,
                                outbound,
                            )?;
                            tracing::debug!(
                                peer = %peer,
                                address = %address,
                                worker = %worker,
                                difficulty = ack.next_difficulty,
                                "stratum difficulty updated"
                            );
                        }
                    }
                }
                if let Some(outbound) = outbound {
                    let response = StratumResponse {
                        id: completion.req_id,
                        status: Some("ok".to_string()),
                        error: None,
                        result: Some(serde_json::json!({
                            "accepted": ack.accepted,
                            "verified": ack.verified,
                            "status": ack.status,
                            "difficulty": response_difficulty,
                        })),
                    };
                    queue_json(outbound, &response)?;
                    if ack.block_accepted {
                        let miner_notification = block_notification(
                            NOTIFY_MINER_BLOCK_FOUND,
                            "great success: you found a block for the pool",
                        );
                        queue_json(outbound, &miner_notification)?;
                    }
                }
                if ack.block_accepted {
                    let pool_notification = block_notification(
                        NOTIFY_POOL_BLOCK_SOLVED,
                        "pool solved a block: share rewards are now pending confirmation",
                    );
                    let _ = self.notifications.send(pool_notification);
                    tracing::info!(
                        peer = %peer,
                        finder = finder_address.unwrap_or_default(),
                        worker = finder_worker.unwrap_or_default(),
                        "broadcasted pool block solved notification"
                    );
                }
            }
            SubmitCompletionOutcome::Finished(Err(err)) => {
                let err_text = err.to_string();
                let reason_code = canonical_share_reject_reason(&err_text);
                // A struck or banned source is disconnected after it is told why.
                let disconnect =
                    reason_code == "source banned" || self.engine.source_blocked(conn_id);
                if let Some((address, _, _)) = logged_in.as_ref() {
                    if log_rejection_at_info(reason_code) {
                        tracing::info!(
                            peer = %peer,
                            address = %address,
                            job_id = %completion.job_id,
                            nonce = completion.nonce,
                            reason_code,
                            error = %err_text,
                            "share rejected"
                        );
                    } else {
                        tracing::debug!(
                            peer = %peer,
                            address = %address,
                            job_id = %completion.job_id,
                            nonce = completion.nonce,
                            reason_code,
                            error = %err_text,
                            "share rejected"
                        );
                    }
                }
                if let Some(outbound) = outbound {
                    queue_error(outbound, completion.req_id, &err_text)?;
                }
                if is_retryable_reject_reason(reason_code) {
                    // The engine already counted this unprocessed share toward
                    // vardiff; deliver any difficulty it raised.
                    if let Some(next) = self.engine.session_difficulty(conn_id) {
                        self.apply_raised_difficulty(conn_id, logged_in, next, outbound)?;
                    }
                }
                if disconnect {
                    if let Some((address, _, _)) = logged_in.as_ref() {
                        tracing::warn!(
                            peer = %peer,
                            address = %address,
                            error = %err_text,
                            "disconnecting struck stratum source"
                        );
                    }
                    return Err(anyhow!("source banned"));
                }
            }
            SubmitCompletionOutcome::WorkerFailure(err_text) => {
                if let Some((address, _, _)) = logged_in.as_ref() {
                    tracing::warn!(
                        peer = %peer,
                        address = %address,
                        job_id = %completion.job_id,
                        nonce = completion.nonce,
                        error = %err_text,
                        "submit worker failure"
                    );
                }
                if let Some(outbound) = outbound {
                    queue_error(
                        outbound,
                        completion.req_id,
                        &format!("submit worker failure: {err_text}"),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Counts a submit the pool answered without processing and delivers any
    /// difficulty raise vardiff derived from it. In-memory only.
    fn note_unprocessed_submit(
        &self,
        conn_id: &str,
        logged_in: &mut Option<(String, String, u64)>,
        reason: UnprocessedReason,
        outbound: &OutboundHandle,
    ) -> Result<()> {
        match self.engine.record_unprocessed_submit(conn_id, reason) {
            Some(next) => self.apply_raised_difficulty(conn_id, logged_in, next, Some(outbound)),
            None => Ok(()),
        }
    }

    /// Moves the connection up to `next` difficulty with a fresh job, as the
    /// submit-ack path does. Dev-fee sessions keep their difficulty until the
    /// next template, also as on the ack path.
    fn apply_raised_difficulty(
        &self,
        conn_id: &str,
        logged_in: &mut Option<(String, String, u64)>,
        next: u64,
        outbound: Option<&OutboundHandle>,
    ) -> Result<()> {
        let Some((address, worker, difficulty)) = logged_in.as_mut() else {
            return Ok(());
        };
        if next <= *difficulty || is_seine_dev_fee_address(address) {
            return Ok(());
        }
        tracing::debug!(
            address = %address,
            worker = %worker,
            from = *difficulty,
            to = next,
            "stratum difficulty raised on unprocessed submits"
        );
        *difficulty = next;
        self.queue_difficulty_job(conn_id, address, next, outbound)
    }

    fn queue_difficulty_job(
        &self,
        conn_id: &str,
        address: &str,
        difficulty: u64,
        outbound: Option<&OutboundHandle>,
    ) -> Result<()> {
        if let Some(message) = self.engine.faraday_upgrade_required(conn_id) {
            return Err(anyhow::anyhow!(message));
        }
        let range_mode = if self
            .engine
            .session_supports_capability(conn_id, CAP_SAME_TEMPLATE_REBIND_V1)
        {
            AssignmentRangeMode::PreserveCurrent
        } else {
            AssignmentRangeMode::Fresh
        };
        if let Some(miner_job) = self
            .jobs
            .build_miner_job_with_range_mode(conn_id, difficulty, address, range_mode)
        {
            if let Some(outbound) = outbound {
                let notify = StratumNotify {
                    method: "job".to_string(),
                    params: serde_json::to_value(miner_job)?,
                };
                queue_job_json(outbound, &notify, None)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_prequeue_reject_async(
        &self,
        conn_id: &str,
        job_id: &str,
        nonce: u64,
        claimed_hash: Option<&str>,
        received_at: Instant,
        reason: &str,
        metadata: SubmitMetadata,
    ) {
        // Admission-control rejects ("busy, retry", "rate limited, retry") are
        // not share outcomes: the miner is told to resubmit the same nonce, so
        // persisting them would only add DB writes under saturation. They are
        // counted in the submit runtime snapshot instead.
        if is_retryable_reject_reason(canonical_share_reject_reason(reason)) {
            return;
        }
        let engine = Arc::clone(&self.engine);
        let conn_id = conn_id.to_string();
        let job_id = job_id.to_string();
        let claimed_hash = claimed_hash.map(str::to_string);
        let reason = reason.to_string();
        tokio::spawn(async move {
            let _ = tokio::task::spawn_blocking(move || {
                engine.record_prequeue_reject(
                    &conn_id,
                    &job_id,
                    nonce,
                    claimed_hash.as_deref(),
                    received_at,
                    &reason,
                    &metadata,
                );
            })
            .await;
        });
    }
}

async fn run_submit_worker(
    engine: Arc<PoolEngine>,
    dispatcher: Arc<SubmitDispatcher>,
    submit_rx: flume::Receiver<SubmitWorkItem>,
    lane: SubmitLane,
) {
    let candidate_lane = lane != SubmitLane::Regular;
    while let Ok(queued) = submit_rx.recv_async().await {
        let started_at = Instant::now();
        dispatcher.lane_queue(lane).pop_and_record_wait(started_at);
        if lane == SubmitLane::Regular {
            dispatcher.release_regular_source(&queued);
        }
        if candidate_lane {
            dispatcher
                .candidate_worker_started
                .fetch_add(1, Ordering::Relaxed);
        }
        let engine = Arc::clone(&engine);
        let conn_id = queued.conn_id.clone();
        let job_id = queued.params.job_id.clone();
        let submit_job_id = job_id.clone();
        let nonce = queued.params.nonce;
        let claimed_hash = queued.params.claimed_hash.clone();
        let metadata = submit_metadata(&queued.params);
        let completion_tx = queued.completion_tx.clone();
        let req_id = queued.req_id;
        let received_at = queued.received_at;
        let candidate_permit = queued.candidate_permit;
        let outcome = match tokio::task::spawn_blocking(move || {
            engine.submit_with_received_at_and_metadata(
                &conn_id,
                submit_job_id,
                nonce,
                claimed_hash,
                received_at,
                metadata,
            )
        })
        .await
        {
            Ok(result) => SubmitCompletionOutcome::Finished(result),
            Err(err) => SubmitCompletionOutcome::WorkerFailure(err.to_string()),
        };
        if candidate_lane {
            match &outcome {
                SubmitCompletionOutcome::Finished(Ok(ack)) => {
                    dispatcher
                        .candidate_persisted
                        .fetch_add(1, Ordering::Relaxed);
                    if ack.block_accepted {
                        dispatcher
                            .candidate_block_accepted
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                SubmitCompletionOutcome::Finished(Err(err)) => {
                    dispatcher
                        .candidate_rejected
                        .fetch_add(1, Ordering::Relaxed);
                    if canonical_share_reject_reason(&err.to_string()) == "stale block" {
                        dispatcher
                            .candidate_stale_block
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                SubmitCompletionOutcome::WorkerFailure(_) => {
                    dispatcher
                        .candidate_worker_failure
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if let Some(permit) = candidate_permit {
            permit.release();
        }
        dispatcher
            .ack_latency
            .record(submit_ack_lane(lane, &outcome), received_at.elapsed());
        if completion_tx
            .send(SubmitCompletion {
                req_id,
                job_id,
                nonce,
                outcome,
            })
            .is_err()
        {
            continue;
        }
    }
}

/// Splits regular acks by path: a synchronous full verify answers seconds
/// later than a hot accept, so mixing them would hide either tail.
fn submit_ack_lane(lane: SubmitLane, outcome: &SubmitCompletionOutcome) -> &'static str {
    match (lane, outcome) {
        (SubmitLane::Candidate, _) => "candidate",
        (SubmitLane::PriorityCandidate, _) => "priority_candidate",
        (SubmitLane::Regular, SubmitCompletionOutcome::Finished(Ok(ack))) if ack.verified => {
            "regular_verified"
        }
        (SubmitLane::Regular, SubmitCompletionOutcome::Finished(Ok(_))) => "regular_hot",
        (SubmitLane::Regular, SubmitCompletionOutcome::Finished(Err(_))) => "regular_rejected",
        (SubmitLane::Regular, SubmitCompletionOutcome::WorkerFailure(_)) => "regular_failure",
    }
}

fn sanitize_submit_metadata(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| !ch.is_control())
        .take(64)
        .collect()
}

/// Notification kind for the once-per-connection upgrade notice. Seine prints
/// unknown kinds as plain pool messages, so old versions show it too.
pub const NOTIFY_MINER_UPDATE_AVAILABLE: &str = "miner_update_available";

/// Aborts a background task when the owning future finishes or is dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl StratumServer {
    /// The upgrade notice for a miner reporting `version`, if a newer stable
    /// seine release is known. Unparseable versions never trigger a notice.
    fn miner_update_notice(&self, version: Option<&str>) -> Option<String> {
        let latest = self.miner_release.version();
        miner_update_notice(version?, &latest)
    }
}

fn miner_update_notice(current: &str, latest: &str) -> Option<String> {
    let (current_parsed, latest_parsed) = (
        parse_release_version(current)?,
        parse_release_version(latest)?,
    );
    (latest_parsed > current_parsed).then(|| {
        format!(
            "seine {} is available (this worker runs {}); newer releases mine faster on most CPUs: https://github.com/zpalmtree/seine/releases/latest",
            latest.trim(),
            current.trim()
        )
    })
}

/// Parses `v1.2.3` or `1.2.3`, ignoring any `-pre` or `+build` suffix.
fn parse_release_version(raw: &str) -> Option<(u64, u64, u64)> {
    let raw = raw.trim();
    let raw = raw.strip_prefix('v').unwrap_or(raw);
    let core = raw.split(['-', '+']).next()?;
    let mut parts = core.split('.').map(|part| part.parse::<u64>().ok());
    let version = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(version)
}

fn submit_metadata(params: &SubmitParams) -> SubmitMetadata {
    SubmitMetadata {
        miner_version: params
            .miner_version
            .as_deref()
            .and_then(nonempty_submit_metadata),
        backend: params.backend.as_deref().and_then(nonempty_submit_metadata),
        found_to_send_us: params.found_to_send_us,
        worker: params.worker.clone(),
    }
}

fn nonempty_submit_metadata(value: &str) -> Option<String> {
    let value = sanitize_submit_metadata(value);
    (!value.is_empty()).then_some(value)
}

fn queue_error(outbound: &OutboundHandle, id: u64, msg: &str) -> Result<()> {
    let response = StratumResponse {
        id,
        status: None,
        error: Some(msg.to_string()),
        result: None,
    };
    queue_json(outbound, &response)
}

async fn read_line_limited<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_bytes: usize,
) -> Result<Option<String>> {
    let mut data = Vec::<u8>::with_capacity(256);
    loop {
        let byte = match reader.read_u8().await {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                if data.is_empty() {
                    return Ok(None);
                }
                break;
            }
            Err(err) => return Err(err.into()),
        };

        if byte == b'\n' {
            break;
        }
        if data.len() >= max_bytes {
            return Err(anyhow!("request exceeds {max_bytes} bytes"));
        }
        if byte != b'\r' {
            data.push(byte);
        }
    }

    String::from_utf8(data)
        .map(Some)
        .map_err(|_| anyhow!("request is not valid UTF-8"))
}

fn queue_json<T: serde::Serialize>(outbound: &OutboundHandle, value: &T) -> Result<()> {
    let payload = serialize_json_text(value)?;
    outbound
        .normal_tx
        .try_send(payload)
        .map_err(|err| anyhow!("stratum outbound queue saturated: {err}"))
}

fn queue_job_json<T: serde::Serialize>(
    outbound: &OutboundHandle,
    value: &T,
    template_created_at: Option<Instant>,
) -> Result<()> {
    let payload = serialize_json_text(value)?;
    let queued_at = Instant::now();
    let replaced = outbound.latest_job.lock().replace(QueuedJob {
        payload,
        queued_at,
        template_created_at,
    });
    if let Some(replaced) = replaced {
        // The writer never got to the previous job: the miner skipped it.
        outbound.job_fanout.record(
            "superseded_unwritten",
            queued_at.saturating_duration_since(replaced.queued_at),
        );
    }
    outbound.latest_job_notify.notify_one();
    Ok(())
}

/// Refreshes a connection's local difficulty from the one the job fanout
/// shares with it; returns the value seen, for `sync_difficulty_out`.
fn sync_difficulty_in(
    logged_in: &mut Option<(String, String, u64)>,
    shared: Option<&Arc<AtomicU64>>,
) -> Option<u64> {
    let (Some((_, _, difficulty)), Some(shared)) = (logged_in.as_mut(), shared) else {
        return None;
    };
    *difficulty = shared.load(Ordering::Relaxed);
    Some(*difficulty)
}

/// Publishes a difficulty the connection task changed, without overwriting
/// one the fanout set in the meantime when the task left it alone.
fn sync_difficulty_out(
    logged_in: &Option<(String, String, u64)>,
    shared: Option<&Arc<AtomicU64>>,
    seen: Option<u64>,
) {
    if let (Some((_, _, difficulty)), Some(shared)) = (logged_in.as_ref(), shared) {
        if seen != Some(*difficulty) {
            shared.store(*difficulty, Ordering::Relaxed);
        }
    }
}

/// Connections per fanout chunk: one job-state lock pass each.
const FANOUT_CHUNK_CONNECTIONS: usize = 256;

/// Threads that fan one template out: one per chunk, up to half the CPUs.
fn fanout_threads(connections: usize) -> usize {
    let cpus = std::thread::available_parallelism().map_or(2, |n| n.get());
    connections
        .div_ceil(FANOUT_CHUNK_CONNECTIONS)
        .clamp(1, (cpus / 2).max(1))
}

fn serialize_json_text<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(Into::into)
}

async fn run_tcp_reader<R: tokio::io::AsyncRead + Unpin>(
    mut reader: BufReader<R>,
    inbound_tx: mpsc::Sender<InboundFrame>,
) -> Result<()> {
    loop {
        match read_line_limited(&mut reader, MAX_STRATUM_REQUEST_BYTES).await {
            Ok(Some(line)) => {
                if inbound_tx.send(InboundFrame::Text(line)).await.is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let _ = inbound_tx
                    .send(InboundFrame::ReadError(err.to_string()))
                    .await;
                break;
            }
        }
    }
    Ok(())
}

async fn run_tcp_outbound_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<String>,
    latest_job: Arc<Mutex<Option<QueuedJob>>>,
    latest_job_notify: Arc<Notify>,
    job_fanout: Arc<NamedLatencyHistograms>,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Some(payload) =
        next_outbound_payload(&mut rx, latest_job.as_ref(), &latest_job_notify).await
    {
        match payload {
            OutboundPayload::Line(data) => write_tcp_outbound_payload(&mut writer, &data).await?,
            OutboundPayload::Job(job) => {
                write_tcp_outbound_payload(&mut writer, &job.payload).await?;
                // In gateway mode "written" means handed to the gateway relay.
                job_fanout.record("queued_to_written", job.queued_at.elapsed());
                if let Some(template_created_at) = job.template_created_at {
                    job_fanout.record("template_to_written", template_created_at.elapsed());
                }
            }
        }
    }
    Ok(())
}

enum OutboundPayload {
    Line(String),
    Job(QueuedJob),
}

async fn next_outbound_payload(
    rx: &mut mpsc::Receiver<String>,
    latest_job: &Mutex<Option<QueuedJob>>,
    latest_job_notify: &Notify,
) -> Option<OutboundPayload> {
    loop {
        if let Some(job) = take_priority_job(latest_job) {
            return Some(OutboundPayload::Job(job));
        }

        tokio::select! {
            biased;
            _ = latest_job_notify.notified() => {}
            maybe = rx.recv() => {
                let Some(data) = maybe else {
                    return take_priority_job(latest_job).map(OutboundPayload::Job);
                };
                return Some(OutboundPayload::Line(data));
            }
        }
    }
}

fn take_priority_job(latest_job: &Mutex<Option<QueuedJob>>) -> Option<QueuedJob> {
    latest_job.lock().take()
}

async fn write_tcp_outbound_payload<W>(writer: &mut W, data: &str) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut line = data.as_bytes().to_vec();
    line.push(b'\n');
    match tokio::time::timeout(OUTBOUND_WRITE_TIMEOUT, writer.write_all(&line)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(err.into()),
        Err(_) => Err(anyhow!(
            "stratum outbound write timed out after {}ms",
            OUTBOUND_WRITE_TIMEOUT.as_millis()
        )),
    }
}

/// The notification sent to a miner refused at a connection cap.
fn connection_limit_line() -> String {
    let retry_after_ms = rand::thread_rng().gen_range(CONNECTION_LIMIT_RETRY_MS);
    serde_json::to_string(&StratumNotify {
        method: METHOD_NOTIFICATION.to_string(),
        params: serde_json::json!({
            "kind": NOTIFY_CONNECTION_LIMIT,
            "message": "pool connection limit reached; reconnecting shortly",
            "retry_after_ms": retry_after_ms,
        }),
    })
    .expect("notification serializes")
}

fn block_notification(kind: &str, message: &str) -> StratumNotify {
    StratumNotify {
        method: METHOD_NOTIFICATION.to_string(),
        params: serde_json::json!({
            "kind": kind,
            "message": message,
        }),
    }
}

fn log_rejection_at_info(reason_code: &str) -> bool {
    matches!(
        reason_code,
        "stale job"
            | "duplicate share"
            | "nonce out of assigned range"
            | "job not assigned"
            | "rate limited"
    )
}

#[cfg(test)]
mod tests {
    use super::ip_group_key;
    use super::{
        client_submit_ack_difficulty, log_rejection_at_info, miner_update_notice,
        nonempty_submit_metadata, parse_release_version, queue_job_json, queue_json,
        run_tcp_outbound_writer, CandidateClaimTracker, OutboundHandle, QueuedSource,
        QueuedSources, StratumResponse, StratumServer,
    };
    use crate::config::Config;
    use crate::engine::canonical_share_reject_reason;
    use crate::engine::{InMemoryJobs, InMemoryNode, Job, PoolEngine, ShareRecord, ShareStore};
    use crate::jobs::{JobManager, MinerJob};
    use crate::node::NodeClient;
    use crate::stats::PoolStats;
    use crate::validation::ValidationEngine;
    use pool_common::pow::PowHasher;
    use pool_common::protocol::{StratumNotify, StratumRequest};
    use serde::Serialize;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::io::{duplex, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    };

    const TEST_SUBMIT_DELAY: Duration = Duration::from_millis(200);

    #[test]
    fn miner_update_notice_only_for_older_parseable_versions() {
        assert!(miner_update_notice("0.2.26", "0.2.44")
            .unwrap()
            .contains("seine 0.2.44 is available (this worker runs 0.2.26)"));
        assert!(miner_update_notice("v0.2.26-3-gabc1234", "v0.2.44").is_some());
        assert!(miner_update_notice("0.2.44", "0.2.44").is_none());
        assert!(miner_update_notice("0.2.45", "0.2.44").is_none());
        assert!(miner_update_notice("seine-dev", "0.2.44").is_none());
        assert!(miner_update_notice("0.2.26", "").is_none());
        assert_eq!(parse_release_version("v0.2.44"), Some((0, 2, 44)));
        assert_eq!(parse_release_version("0.2.44.1"), None);
    }

    #[test]
    fn submit_metadata_is_trimmed_bounded_and_control_free() {
        assert_eq!(
            nonempty_submit_metadata("  nvidia\n#1  ").as_deref(),
            Some("nvidia#1")
        );
        assert_eq!(nonempty_submit_metadata(" \n\t "), None);
        assert_eq!(
            nonempty_submit_metadata(&"x".repeat(80))
                .expect("metadata")
                .len(),
            64
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct SleepyQueueHasher;

    impl SleepyQueueHasher {
        fn hash_for_nonce(nonce: u64) -> [u8; 32] {
            if nonce.is_multiple_of(2) {
                [0u8; 32]
            } else {
                let mut out = [0u8; 32];
                out[24..].copy_from_slice(&nonce.to_be_bytes());
                out
            }
        }

        fn claimed_hash_hex(nonce: u64) -> String {
            hex::encode(Self::hash_for_nonce(nonce))
        }
    }

    impl PowHasher for SleepyQueueHasher {
        fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
            std::thread::sleep(TEST_SUBMIT_DELAY);
            Ok(Self::hash_for_nonce(nonce))
        }
    }

    fn stratum_test_cfg() -> Config {
        Config {
            validation_mode: "full".to_string(),
            enable_vardiff: false,
            initial_share_difficulty: 1,
            min_share_difficulty: 1,
            max_share_difficulty: 1_000_000,
            regular_verifiers: 1,
            regular_submit_queue: 1,
            regular_validation_queue: 8,
            job_timeout: "10s".to_string(),
            ..Config::default()
        }
    }

    fn stratum_test_job(id: &str, height: u64) -> Job {
        Job {
            id: id.to_string(),
            height,
            header_base: vec![0xAB; 92],
            network_target: [0u8; 32],
            network_difficulty: 1,
            template_id: format!("tmpl-{height}"),
            prev_hash: None,
            template_created_at: Instant::now(),
            template_block: None,
        }
    }

    fn nonce_with_parity(job: &MinerJob, even: bool, offset: u64) -> u64 {
        let target_parity = if even { 0 } else { 1 };
        let mut nonce = if job.nonce_start % 2 == target_parity {
            job.nonce_start
        } else {
            job.nonce_start.saturating_add(1)
        };
        nonce = nonce.saturating_add(offset.saturating_mul(2));
        assert!(
            nonce <= job.nonce_end,
            "assignment range must have enough nonces for the test"
        );
        nonce
    }

    fn test_miner_address(seed: u8) -> String {
        match seed {
            0x55 => "RwkNt8jJNWifsxuWSKta9smTLm9mnAv1gWyk46fWSxk6xDWrAahevSTVbFUfYdFr9z4vNJWCziBh7fmJMhyB5BfxM4YYM",
            0x61 => "3DLBLxPGExg7rM7P9uEUDf7x8uFQaeVjEgWB8BpysUnw7j1f87AqGdTM7tXq6bRA21ZhkCam4Woz6kGWyCdsWgFA2FVVV",
            0x62 => "9xxFbTd1p8iwLJLY8vvE78kKKU8G2SUZ4TeDnaCy23xbhYaTESbfkHLWmFSnh42ywsvaGT4tgJbNonU6rUyQApzGu94iM",
            0x71 => "PZmWbHdrRd2vTEJda2KZDyzsJCPdoLXMpB5Cg5zU9Xg7V5JnvEwryZ1yv86GhyJQr5fXcErhh57xZQMVA8gGxMGGEHQFX",
            _ => panic!("missing test address for seed {seed:#x}"),
        }
        .to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_shortname_login_uses_canonical_address_for_jobs_stats_and_shares() {
        use crate::engine::JobRepository;
        use crate::shortname::tests::{mock_resolver, ADDRESS};
        let (mut server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let (resolver, resolver_task) = mock_resolver(ADDRESS);
        Arc::get_mut(&mut server).unwrap().shortname_resolver = Arc::new(resolver);
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let job =
            login_test_client(&mut reader, &mut writer, 1, "@ALICE", "shortname-worker").await;
        resolver_task.join().unwrap();
        let binding = jobs
            .resolve_submit_job(&job.job_id, Instant::now())
            .unwrap();
        assert_eq!(binding.assigned_miner.as_deref(), Some(ADDRESS));

        let (mut raw_reader, mut raw_writer) = connect_test_client(addr).await;
        login_test_client(&mut raw_reader, &mut raw_writer, 2, ADDRESS, "raw-worker").await;
        let snapshot = server.stats.snapshot();
        assert_eq!(
            snapshot.connected_miners, 1,
            "shortname and address share one identity"
        );
        assert_eq!(snapshot.connected_workers, 2);

        let nonce = nonce_with_parity(&job, false, 0);
        send_submit(
            &mut writer,
            3,
            &job.job_id,
            nonce,
            SleepyQueueHasher::claimed_hash_hex(nonce),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["id"], 3);
        assert_eq!(response["status"], "ok");
        drop((reader, writer, raw_reader, raw_writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    async fn build_tcp_test_server(cfg: Config) -> (Arc<StratumServer>, Arc<JobManager>) {
        let (server, jobs, _store) = build_tcp_test_server_with_store(cfg).await;
        (server, jobs)
    }

    async fn build_tcp_test_server_with_store(
        cfg: Config,
    ) -> (
        Arc<StratumServer>,
        Arc<JobManager>,
        Arc<crate::engine::InMemoryStore>,
    ) {
        tokio::task::spawn_blocking(move || {
            let node_client =
                Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node client"));
            let jobs = JobManager::new(node_client, cfg.clone());
            jobs.install_test_job(stratum_test_job("job-initial", 100));

            let validation = Arc::new(ValidationEngine::new(
                cfg.clone(),
                Arc::new(SleepyQueueHasher),
            ));
            let store = Arc::new(crate::engine::InMemoryStore::default());
            let engine = Arc::new(PoolEngine::new(
                cfg.clone(),
                validation,
                Arc::clone(&jobs) as Arc<dyn crate::engine::JobRepository>,
                Arc::clone(&store) as Arc<dyn ShareStore>,
                Arc::new(InMemoryNode::default()),
            ));
            let stats = Arc::new(PoolStats::default());
            let listen_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
            let server = StratumServer::new(listen_addr, engine, Arc::clone(&jobs), stats, cfg);
            (server, jobs, store)
        })
        .await
        .expect("build tcp test server on blocking pool")
    }

    async fn start_test_tcp_listener(
        server: Arc<StratumServer>,
    ) -> (SocketAddr, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let listener = TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let task = tokio::spawn(async move { server.run_tcp_listener(listener).await });
        (addr, task)
    }

    async fn connect_test_client(addr: SocketAddr) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
        let stream = TcpStream::connect(addr).await.expect("connect test client");
        let (reader, writer) = stream.into_split();
        (BufReader::new(reader), writer)
    }

    async fn shutdown_test_listener(
        listener_task: tokio::task::JoinHandle<anyhow::Result<()>>,
        server: Arc<StratumServer>,
        jobs: Arc<JobManager>,
    ) {
        listener_task.abort();
        let _ = listener_task.await;
        for _ in 0..40 {
            if server.conn_state.lock().total == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::task::spawn_blocking(move || {
            drop(server);
            drop(jobs);
        })
        .await
        .expect("drop test server on blocking pool");
    }

    async fn write_json_line<T: Serialize>(writer: &mut OwnedWriteHalf, value: &T) {
        let payload = serde_json::to_string(value).expect("serialize request");
        writer
            .write_all(payload.as_bytes())
            .await
            .expect("write request");
        writer.write_all(b"\n").await.expect("write newline");
        writer.flush().await.expect("flush request");
    }

    async fn read_json_line(reader: &mut BufReader<OwnedReadHalf>) -> Value {
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .expect("timed out waiting for message")
            .expect("read line");
        assert!(!line.trim().is_empty(), "expected a JSON message");
        serde_json::from_str(line.trim()).expect("decode json line")
    }

    async fn login_test_client(
        reader: &mut BufReader<OwnedReadHalf>,
        writer: &mut OwnedWriteHalf,
        req_id: u64,
        address: &str,
        worker: &str,
    ) -> MinerJob {
        write_json_line(
            writer,
            &StratumRequest {
                id: req_id,
                method: pool_common::protocol::METHOD_LOGIN.to_string(),
                params: json!({
                    "address": address,
                    "worker": worker,
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash"],
                }),
            },
        )
        .await;

        let login = read_json_line(reader).await;
        assert_eq!(login["id"].as_u64(), Some(req_id));
        assert_eq!(login["status"].as_str(), Some("ok"));

        let job = read_json_line(reader).await;
        assert_eq!(job["method"].as_str(), Some("job"));
        serde_json::from_value(job["params"].clone()).expect("decode miner job")
    }

    async fn send_submit(
        writer: &mut OwnedWriteHalf,
        req_id: u64,
        job_id: &str,
        nonce: u64,
        claimed_hash: String,
    ) {
        write_json_line(
            writer,
            &StratumRequest {
                id: req_id,
                method: pool_common::protocol::METHOD_SUBMIT.to_string(),
                params: json!({
                    "job_id": job_id,
                    "nonce": nonce,
                    "claimed_hash": claimed_hash,
                }),
            },
        )
        .await;
    }

    async fn read_response_map(
        reader: &mut BufReader<OwnedReadHalf>,
        ids: &[u64],
    ) -> (HashMap<u64, Value>, Vec<u64>) {
        let mut remaining = ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut responses = HashMap::new();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let message = read_json_line(reader).await;
            let Some(id) = message.get("id").and_then(Value::as_u64) else {
                continue;
            };
            if remaining.remove(&id) {
                order.push(id);
                responses.insert(id, message);
            }
        }
        (responses, order)
    }

    #[test]
    fn stale_and_duplicate_share_reasons_are_classified() {
        assert_eq!(
            canonical_share_reject_reason("stale job: assignment not found"),
            "stale job"
        );
        assert_eq!(
            canonical_share_reject_reason("duplicate share"),
            "duplicate share"
        );
        assert_eq!(
            canonical_share_reject_reason("nonce out of assigned range"),
            "nonce out of assigned range"
        );
        assert_eq!(
            canonical_share_reject_reason("claimed hash required"),
            "claimed hash required"
        );
        assert_eq!(
            canonical_share_reject_reason("invalid hex"),
            "invalid claimed hash"
        );
        assert_eq!(
            canonical_share_reject_reason("rate limited, retry"),
            "rate limited"
        );
        assert_eq!(
            canonical_share_reject_reason("server busy, retry"),
            "server busy"
        );
        assert_eq!(
            canonical_share_reject_reason("validation timeout"),
            "validation timeout"
        );
        assert_eq!(
            canonical_share_reject_reason("candidate claim busy, retry"),
            "candidate claim busy"
        );
    }

    #[test]
    fn only_high_signal_rejections_are_logged_at_info() {
        assert!(log_rejection_at_info("stale job"));
        assert!(log_rejection_at_info("duplicate share"));
        assert!(log_rejection_at_info("rate limited"));
        assert!(!log_rejection_at_info("claimed hash required"));
        assert!(!log_rejection_at_info("other"));
    }

    #[test]
    fn dev_fee_submit_ack_keeps_client_on_current_difficulty_until_next_template() {
        assert_eq!(
            client_submit_ack_difficulty(crate::dev_fee::SEINE_DEV_FEE_ADDRESS, 60, 240),
            60
        );
    }

    #[test]
    fn regular_submit_ack_applies_next_difficulty_immediately() {
        assert_eq!(client_submit_ack_difficulty("addr1", 60, 240), 240);
    }

    #[test]
    fn queue_json_rejects_when_outbound_queue_is_full() {
        let (outbound, _rx) = OutboundHandle::new(1);
        outbound
            .normal_tx
            .try_send("occupied".to_string())
            .expect("seeded payload should fit");

        let err = queue_json(
            &outbound,
            &StratumResponse {
                id: 1,
                status: Some("ok".to_string()),
                error: None,
                result: None,
            },
        )
        .expect_err("full outbound queue should fail fast");

        assert!(err.to_string().contains("outbound queue saturated"));
    }

    #[tokio::test]
    async fn outbound_writer_times_out_when_peer_stops_reading() {
        let (writer, _reader) = duplex(1);
        let (outbound, rx) = OutboundHandle::new(1);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        outbound
            .normal_tx
            .send("a".repeat(64))
            .await
            .expect("seed payload should enqueue");
        drop(outbound);

        let err =
            run_tcp_outbound_writer(writer, rx, latest_job, latest_job_notify, Arc::default())
                .await
                .expect_err("writer should time out when peer stops reading");

        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn outbound_writer_prioritizes_latest_job_and_coalesces_stale_jobs() {
        let (writer, mut reader) = duplex(4096);
        let (outbound, rx) = OutboundHandle::new(8);
        let latest_job = Arc::clone(&outbound.latest_job);
        let latest_job_notify = Arc::clone(&outbound.latest_job_notify);
        let job_fanout = Arc::clone(&outbound.job_fanout);

        queue_json(
            &outbound,
            &StratumResponse {
                id: 7,
                status: Some("ok".to_string()),
                error: None,
                result: Some(serde_json::json!({"accepted": true})),
            },
        )
        .expect("response should enqueue");
        queue_job_json(
            &outbound,
            &StratumNotify {
                method: "job".to_string(),
                params: serde_json::json!({"job_id": "old"}),
            },
            None,
        )
        .expect("old job should enqueue");
        queue_job_json(
            &outbound,
            &StratumNotify {
                method: "job".to_string(),
                params: serde_json::json!({"job_id": "new"}),
            },
            Some(Instant::now()),
        )
        .expect("new job should replace the stale one");
        drop(outbound);

        run_tcp_outbound_writer(
            writer,
            rx,
            latest_job,
            latest_job_notify,
            Arc::clone(&job_fanout),
        )
        .await
        .expect("writer should flush queued payloads");
        let fanout = job_fanout.snapshot();
        for lane in [
            "superseded_unwritten",
            "queued_to_written",
            "template_to_written",
        ] {
            assert_eq!(
                fanout.get(lane).map(|histogram| histogram.count()),
                Some(1),
                "{lane} should record the coalesced and written jobs"
            );
        }

        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .await
            .expect("reader should collect writer output");
        let lines = String::from_utf8(out).expect("output should be utf8");
        let messages = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("line should decode as json"))
            .collect::<Vec<_>>();

        assert_eq!(messages.len(), 2, "stale job should be coalesced away");
        assert_eq!(messages[0]["method"], "job");
        assert_eq!(messages[0]["params"]["job_id"], "new");
        assert_eq!(messages[1]["id"], 7);
    }

    #[test]
    fn fair_shed_targets_sources_holding_more_than_their_share() {
        let farm = QueuedSource {
            address: "farm".to_string(),
            ip_group: "150.5.154.10".to_string(),
        };
        let farm_other_ip = QueuedSource {
            address: "farm".to_string(),
            ip_group: "150.5.155.20".to_string(),
        };
        let miner = |i: usize| QueuedSource {
            address: format!("miner-{i}"),
            ip_group: format!("10.0.0.{i}"),
        };
        let mut sources = QueuedSources::default();
        for _ in 0..200 {
            sources.add(&farm);
            sources.add(&farm_other_ip);
        }
        for i in 0..50 {
            sources.add(&miner(i));
        }
        assert!(sources.over_share(&farm), "one address across many IPs");
        assert!(sources.over_share(&farm_other_ip));
        assert!(
            !sources.over_share(&miner(3)),
            "a miner with one queued share"
        );
        assert!(!sources.over_share(&miner(99)), "a newcomer");

        let sybil = |i: usize| QueuedSource {
            address: format!("throwaway-{i}"),
            ip_group: "203.0.113.7".to_string(),
        };
        let mut sources = QueuedSources::default();
        for i in 0..100 {
            sources.add(&sybil(i));
        }
        for i in 0..50 {
            sources.add(&miner(i));
        }
        assert!(
            sources.over_share(&sybil(500)),
            "many addresses, one IP group"
        );

        let mut alone = QueuedSources::default();
        for _ in 0..10 {
            alone.add(&farm);
        }
        assert!(
            !alone.over_share(&farm),
            "a lone source is not over its share"
        );
        for _ in 0..10 {
            alone.remove(&farm);
        }
        assert_eq!(alone.total, 0);
        assert!(alone.by_address.is_empty() && alone.by_ip_group.is_empty());
    }

    #[test]
    fn candidate_claim_tracker_enforces_inflight_and_rate_limits() {
        let tracker = CandidateClaimTracker::new(Duration::from_secs(60), 2, 1);
        let now = Instant::now();

        let first = tracker
            .try_acquire("addr|127.0.0.1".to_string(), now)
            .expect("first candidate claim should acquire");
        assert!(
            tracker
                .try_acquire("addr|127.0.0.1".to_string(), now + Duration::from_millis(1))
                .is_err(),
            "second in-flight candidate claim should be rejected"
        );
        first.release();

        let second = tracker
            .try_acquire("addr|127.0.0.1".to_string(), now + Duration::from_millis(2))
            .expect("claim should acquire once the first permit is released");
        second.release();

        assert!(
            tracker
                .try_acquire("addr|127.0.0.1".to_string(), now + Duration::from_millis(3))
                .is_err(),
            "third claim inside the same window should hit the rate limit"
        );
        assert!(
            tracker
                .try_acquire(
                    "other|127.0.0.1".to_string(),
                    now + Duration::from_millis(3)
                )
                .is_ok(),
            "other addresses should not share the rate limiter bucket"
        );
    }

    #[test]
    fn candidate_claims_are_bounded_per_ip_group_across_addresses() {
        let tracker = CandidateClaimTracker::new(Duration::from_secs(60), 4, 1);
        let ip_limit = super::CandidateClaimLimit {
            max_per_window: 3,
            max_inflight: 2,
        };
        let per_source = tracker.default_limit();
        let buckets = |address: &str, conn: &str| {
            vec![
                (format!("addr|{address}|198.51.100.7"), per_source),
                (format!("conn|{conn}"), per_source),
                ("ip|198.51.100.7".to_string(), ip_limit),
            ]
        };
        let now = Instant::now();
        let first = tracker
            .try_acquire_all(buckets("throwaway-1", "c1"), now)
            .expect("first claim");
        let second = tracker
            .try_acquire_all(buckets("throwaway-2", "c2"), now)
            .expect("second claim from the same IP with a fresh address");
        let err = tracker
            .try_acquire_all(buckets("throwaway-3", "c3"), now)
            .expect_err("IP in-flight cap holds across fresh addresses");
        assert!(err.to_string().contains("busy"));
        first.release();
        second.release();
        tracker
            .try_acquire_all(buckets("throwaway-4", "c4"), now)
            .expect("third claim in the window")
            .release();
        let err = tracker
            .try_acquire_all(buckets("throwaway-5", "c5"), now)
            .expect_err("IP window cap holds across fresh addresses");
        assert!(err.to_string().contains("rate limited"));
        // A rejected multi-bucket acquire must not consume any bucket.
        tracker
            .try_acquire_all(
                vec![
                    ("addr|honest|203.0.113.9".to_string(), per_source),
                    ("conn|honest".to_string(), per_source),
                    ("ip|203.0.113.9".to_string(), ip_limit),
                ],
                now,
            )
            .expect("other IPs are unaffected")
            .release();
    }

    #[test]
    fn candidate_claim_tracker_frees_buckets_after_window() {
        let tracker = CandidateClaimTracker::new(Duration::from_secs(60), 4, 1);
        let now = Instant::now();
        for idx in 0..50 {
            tracker
                .try_acquire(format!("addr-{idx}|127.0.0.1"), now)
                .expect("acquire")
                .release();
        }
        // Released buckets keep their window history until it expires...
        assert_eq!(tracker.tracked_keys(), 50);
        // ...and are then dropped instead of leaking forever.
        tracker.force_sweep(now + Duration::from_secs(61));
        assert_eq!(tracker.tracked_keys(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_type_prequeue_rejects_are_not_persisted() {
        let (server, jobs, store) = build_tcp_test_server_with_store(stratum_test_cfg()).await;
        let address = test_miner_address(0x55);
        let engine = Arc::clone(&server.engine);
        tokio::task::spawn_blocking(move || {
            engine
                .login(
                    "198.51.100.20:4000",
                    address,
                    "rig",
                    2,
                    vec!["submit_claimed_hash".to_string()],
                )
                .expect("login");
        })
        .await
        .unwrap();
        let job = jobs
            .build_miner_job("198.51.100.20:4000", 1, &test_miner_address(0x55))
            .expect("job");
        for reason in [
            "candidate claim busy, retry",
            "candidate claim rate limited, retry",
            "server busy, retry",
        ] {
            server.persist_prequeue_reject_async(
                "198.51.100.20:4000",
                &job.job_id,
                job.nonce_start,
                Some(&"00".repeat(32)),
                Instant::now(),
                reason,
                super::SubmitMetadata::default(),
            );
        }
        server.persist_prequeue_reject_async(
            "198.51.100.20:4000",
            &job.job_id,
            job.nonce_start + 1,
            None,
            Instant::now(),
            "claimed hash required",
            super::SubmitMetadata::default(),
        );
        for _ in 0..100 {
            if !store.shares().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let shares = store.shares();
        assert_eq!(
            shares.len(),
            1,
            "only the non-retryable reject is persisted"
        );
        assert_eq!(
            shares[0].reject_reason.as_deref(),
            Some("claimed hash required")
        );
        tokio::task::spawn_blocking(move || {
            drop(server);
            drop(jobs);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn job_tick_hint_write_runs_on_blocking_pool() {
        struct RuntimeStartingStore;

        impl ShareStore for RuntimeStartingStore {
            fn try_claim_share(
                &self,
                _job_id: &str,
                _share_key: &str,
                _nonce: u64,
            ) -> anyhow::Result<bool> {
                Ok(true)
            }

            fn add_share(&self, _share: ShareRecord) -> anyhow::Result<()> {
                Ok(())
            }

            fn upsert_vardiff_hint(
                &self,
                _address: &str,
                _worker: &str,
                _difficulty: u64,
                _updated_at: std::time::SystemTime,
            ) -> anyhow::Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                runtime.block_on(async {});
                Ok(())
            }
        }

        struct StaticHasher;

        impl PowHasher for StaticHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0x01; 32])
            }
        }

        let cfg = Config::default();
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(StaticHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        let engine = Arc::new(PoolEngine::new(
            cfg.clone(),
            validation,
            jobs,
            Arc::new(RuntimeStartingStore),
            Arc::new(InMemoryNode::default()),
        ));
        let address = "Kffdvk1q1YPCGKz8gzJoymedZucxqJtxsw5ZUMJrWrVUK4jCXChXmTBrMADRh9EzeMNNPunXrz3dyg9nMwdZPaNc7FB7v".to_string();
        engine
            .login(
                "conn1",
                address,
                "",
                2,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect("login should succeed");

        // The retarget runs inline on the runtime; only the hint write
        // touches the store, and it goes to the blocking pool.
        let (difficulty, hint) = engine.retarget_on_job("conn1").expect("session");
        assert_eq!(difficulty, cfg.initial_share_difficulty);
        let hint = hint.expect("first job tick persists a hint");
        tokio::task::spawn_blocking(move || engine.persist_vardiff_hint(hint))
            .await
            .expect("hint write should not panic on runtime-backed store");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gateway_miner_over_the_cap_is_told_to_back_off_before_closing() {
        use crate::service_state::EngineRole;
        use pool_gateway::{GatewayListeners, GatewayMetrics, Timings};

        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("gateway.sock");
        let miner_listener =
            TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
                .await
                .expect("bind gateway");
        let gateway_addr = miner_listener.local_addr().expect("gateway addr");
        let engine_listener = tokio::net::UnixListener::bind(&socket).expect("bind engine socket");
        let gateway = tokio::spawn(pool_gateway::run(
            GatewayListeners {
                miners: miner_listener,
                engines: engine_listener,
            },
            Timings {
                tick: Duration::from_millis(20),
                ..Timings::default()
            },
            1_000,
            Arc::new(GatewayMetrics::default()),
        ));
        let cfg = Config {
            stratum_max_connections: 1,
            ..stratum_test_cfg()
        };
        let (engine, jobs) = build_tcp_test_server(cfg).await;
        let role = Arc::new(EngineRole::new(false));
        let serve = tokio::spawn(Arc::clone(&engine).run_gateway_client(
            socket.clone(),
            "a".to_string(),
            Arc::clone(&role),
        ));
        for _ in 0..500 {
            if role.is_primary() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let (mut held_reader, mut held_writer) = connect_test_client(gateway_addr).await;
        let address = test_miner_address(0x55);
        login_test_client(&mut held_reader, &mut held_writer, 1, &address, "rig").await;

        let (mut reader, _writer) = connect_test_client(gateway_addr).await;
        let notice = read_json_line(&mut reader).await;
        assert_eq!(
            notice["params"]["kind"],
            pool_common::protocol::NOTIFY_CONNECTION_LIMIT
        );
        assert!(notice["params"]["retry_after_ms"].as_u64().is_some());
        let mut rest = String::new();
        let read = tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut rest))
            .await
            .expect("refused connection closes");
        assert_eq!(read.unwrap(), 0);

        drop((held_reader, held_writer));
        serve.abort();
        gateway.abort();
        tokio::task::spawn_blocking(move || drop((engine, jobs)))
            .await
            .expect("drop engine");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gateway_hands_a_miner_to_a_new_engine_without_disconnecting_it() {
        use crate::service_state::EngineRole;
        use pool_gateway::{GatewayListeners, GatewayMetrics, Timings};
        use std::sync::atomic::Ordering;

        async fn wait_until(what: &str, ready: impl Fn() -> bool) {
            for _ in 0..500 {
                if ready() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for {what}");
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("gateway.sock");
        let miner_listener =
            TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
                .await
                .expect("bind gateway");
        let gateway_addr = miner_listener.local_addr().expect("gateway addr");
        let engine_listener = tokio::net::UnixListener::bind(&socket).expect("bind engine socket");
        let metrics = Arc::new(GatewayMetrics::default());
        let timings = Timings {
            retire_grace: Duration::from_millis(500),
            tick: Duration::from_millis(20),
            migrate_per_second: 1_000,
            ..Timings::default()
        };
        let gateway = tokio::spawn(pool_gateway::run(
            GatewayListeners {
                miners: miner_listener,
                engines: engine_listener,
            },
            timings,
            1_000,
            Arc::clone(&metrics),
        ));

        let (engine_a, jobs_a) = build_tcp_test_server(stratum_test_cfg()).await;
        let role_a = Arc::new(EngineRole::new(false));
        let mut serve_a = tokio::spawn(Arc::clone(&engine_a).run_gateway_client(
            socket.clone(),
            "a".to_string(),
            Arc::clone(&role_a),
        ));
        wait_until("engine a primary", || role_a.is_primary()).await;

        let (mut reader, mut writer) = connect_test_client(gateway_addr).await;
        let address = test_miner_address(0x55);
        let job_a = login_test_client(&mut reader, &mut writer, 1, &address, "rig").await;
        let nonce = nonce_with_parity(&job_a, false, 0);
        send_submit(
            &mut writer,
            2,
            &job_a.job_id,
            nonce,
            SleepyQueueHasher::claimed_hash_hex(nonce),
        )
        .await;
        let accepted = read_json_line(&mut reader).await;
        assert_eq!(
            (accepted["id"].as_u64(), accepted["status"].as_str()),
            (Some(2), Some("ok"))
        );

        // Deploy: engine b starts; a drains and the miner moves to b.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (engine_b, jobs_b) = build_tcp_test_server(stratum_test_cfg()).await;
        let role_b = Arc::new(EngineRole::new(false));
        let serve_b = tokio::spawn(Arc::clone(&engine_b).run_gateway_client(
            socket.clone(),
            "b".to_string(),
            Arc::clone(&role_b),
        ));
        wait_until("engine b primary", || role_b.is_primary()).await;
        assert!(!role_a.is_primary(), "the replaced engine drains");

        // The miner's next message is a fresh job from b, on the same socket.
        let job = read_json_line(&mut reader).await;
        assert_eq!(job["method"].as_str(), Some("job"));
        let job_b: MinerJob = serde_json::from_value(job["params"].clone()).expect("job");
        assert_ne!(job_b.job_id, job_a.job_id);
        assert_eq!(metrics.migrations.load(Ordering::Relaxed), 1);

        // A late share for a's job is still judged by a; b judges its own.
        let late = nonce_with_parity(&job_a, false, 1);
        send_submit(
            &mut writer,
            3,
            &job_a.job_id,
            late,
            SleepyQueueHasher::claimed_hash_hex(late),
        )
        .await;
        let fresh = nonce_with_parity(&job_b, false, 0);
        send_submit(
            &mut writer,
            4,
            &job_b.job_id,
            fresh,
            SleepyQueueHasher::claimed_hash_hex(fresh),
        )
        .await;
        let (responses, _) = read_response_map(&mut reader, &[3, 4]).await;
        assert_eq!(
            responses[&3]["status"].as_str(),
            Some("ok"),
            "{:?}",
            responses[&3]
        );
        assert_eq!(
            responses[&4]["status"].as_str(),
            Some("ok"),
            "{:?}",
            responses[&4]
        );

        // After the grace, a is retired and exits; the miner never noticed.
        tokio::time::timeout(Duration::from_secs(5), &mut serve_a)
            .await
            .expect("engine a retired")
            .expect("engine a task")
            .expect("engine a exits cleanly");
        assert_eq!(metrics.retires.load(Ordering::Relaxed), 1);
        let after = nonce_with_parity(&job_b, false, 1);
        send_submit(
            &mut writer,
            5,
            &job_b.job_id,
            after,
            SleepyQueueHasher::claimed_hash_hex(after),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(
            (response["id"].as_u64(), response["status"].as_str()),
            (Some(5), Some("ok"))
        );

        drop(writer);
        drop(reader);
        serve_b.abort();
        gateway.abort();
        tokio::task::spawn_blocking(move || drop((engine_a, jobs_a, engine_b, jobs_b)))
            .await
            .expect("drop engines");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_submit_queue_saturation_returns_busy_and_connection_recovers() {
        let cfg = stratum_test_cfg();
        let (server, _jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let address = test_miner_address(0x55);
        let miner_job = login_test_client(&mut reader, &mut writer, 1, &address, "rig-a").await;

        let nonce1 = nonce_with_parity(&miner_job, false, 0);
        let nonce2 = nonce_with_parity(&miner_job, false, 1);
        let nonce3 = nonce_with_parity(&miner_job, false, 2);
        let nonce4 = nonce_with_parity(&miner_job, false, 3);
        let nonce5 = nonce_with_parity(&miner_job, false, 4);
        let nonce6 = nonce_with_parity(&miner_job, false, 5);

        for (req_id, nonce) in [
            (2, nonce1),
            (3, nonce2),
            (4, nonce3),
            (5, nonce4),
            (6, nonce5),
            (7, nonce6),
        ] {
            send_submit(
                &mut writer,
                req_id,
                &miner_job.job_id,
                nonce,
                SleepyQueueHasher::claimed_hash_hex(nonce),
            )
            .await;
        }

        let (responses, _) = read_response_map(&mut reader, &[2, 3, 4, 5, 6, 7]).await;
        let busy_count = responses
            .values()
            .filter(|message| {
                message["error"]
                    .as_str()
                    .is_some_and(|err| err.contains("server busy"))
            })
            .count();
        let ok_count = responses
            .values()
            .filter(|message| message["status"].as_str() == Some("ok"))
            .count();
        assert!(
            ok_count >= 1,
            "at least one submit should still be accepted"
        );
        assert!(
            busy_count >= 1,
            "at least one submit should fail fast once the regular submit queue is full"
        );

        let nonce7 = nonce_with_parity(&miner_job, false, 6);
        send_submit(
            &mut writer,
            8,
            &miner_job.job_id,
            nonce7,
            SleepyQueueHasher::claimed_hash_hex(nonce7),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["id"].as_u64(), Some(8));
        assert_eq!(response["status"].as_str(), Some("ok"));

        drop(writer);
        drop(reader);
        shutdown_test_listener(listener_task, server, _jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_unprocessed_submits_raise_difficulty_and_send_a_job() {
        let cfg = Config {
            validation_mode: "probabilistic".to_string(),
            sample_rate: 0.0,
            warmup_shares: 0,
            min_sample_every: 0,
            enable_vardiff: true,
            initial_share_difficulty: 100,
            vardiff_retarget_interval: "2s".to_string(),
            regular_submit_queue: 8,
            ..stratum_test_cfg()
        };
        let (mut server, jobs) = build_tcp_test_server(cfg).await;
        Arc::get_mut(&mut server).unwrap().submit_rate_limit_max = 1;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let address = test_miner_address(0x55);
        let job = login_test_client(&mut reader, &mut writer, 1, &address, "rig-a").await;
        assert_eq!(job.difficulty, 100);

        let nonce = nonce_with_parity(&job, false, 0);
        send_submit(&mut writer, 2, &job.job_id, nonce, job.target.clone()).await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["id"], 2);
        assert_eq!(response["status"], "ok");

        // The next share is turned away by the per-connection rate limit. It
        // is still a found share: a second share within ~1s of the first
        // against a 30s target must raise difficulty, and the miner must be
        // told through a new job rather than an ack it never gets.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let nonce = nonce_with_parity(&job, false, 1);
        send_submit(&mut writer, 3, &job.job_id, nonce, job.target.clone()).await;
        let mut rejected = false;
        let mut raised_job = None;
        while !(rejected && raised_job.is_some()) {
            let message = read_json_line(&mut reader).await;
            if message["id"] == 3 {
                assert!(message["error"]
                    .as_str()
                    .is_some_and(|err| err.starts_with("rate limited")));
                rejected = true;
            } else if message["method"] == "job" {
                raised_job = Some(message["params"]["difficulty"].as_u64().unwrap());
            }
        }
        assert_eq!(raised_job, Some(150));
        let conn_id = writer.local_addr().expect("client addr").to_string();
        assert_eq!(server.engine.session_difficulty(&conn_id), Some(150));
        assert_eq!(server.submit_snapshot().unprocessed.rate_limited_total, 1);

        drop((reader, writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_candidate_submit_finishes_before_other_connections_regular_backlog() {
        let mut cfg = stratum_test_cfg();
        cfg.regular_submit_queue = 3;
        let (server, _jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader_a, mut writer_a) = connect_test_client(addr).await;
        let (mut reader_b, mut writer_b) = connect_test_client(addr).await;

        let job_a = login_test_client(
            &mut reader_a,
            &mut writer_a,
            1,
            &test_miner_address(0x61),
            "rig-a",
        )
        .await;
        let job_b = login_test_client(
            &mut reader_b,
            &mut writer_b,
            2,
            &test_miner_address(0x62),
            "rig-b",
        )
        .await;

        let regular1 = nonce_with_parity(&job_a, false, 0);
        let regular2 = nonce_with_parity(&job_a, false, 1);
        let regular3 = nonce_with_parity(&job_a, false, 2);
        let candidate = nonce_with_parity(&job_b, true, 0);

        send_submit(
            &mut writer_a,
            10,
            &job_a.job_id,
            regular1,
            SleepyQueueHasher::claimed_hash_hex(regular1),
        )
        .await;
        send_submit(
            &mut writer_a,
            11,
            &job_a.job_id,
            regular2,
            SleepyQueueHasher::claimed_hash_hex(regular2),
        )
        .await;
        send_submit(
            &mut writer_a,
            12,
            &job_a.job_id,
            regular3,
            SleepyQueueHasher::claimed_hash_hex(regular3),
        )
        .await;
        send_submit(
            &mut writer_b,
            20,
            &job_b.job_id,
            candidate,
            SleepyQueueHasher::claimed_hash_hex(candidate),
        )
        .await;

        let mut responses = HashMap::new();
        let mut order = Vec::new();
        while responses.len() < 4 {
            tokio::select! {
                message = read_json_line(&mut reader_a) => {
                    if let Some(id) = message.get("id").and_then(Value::as_u64) {
                        order.push(id);
                        responses.insert(id, message);
                    }
                }
                message = read_json_line(&mut reader_b) => {
                    if let Some(id) = message.get("id").and_then(Value::as_u64) {
                        order.push(id);
                        responses.insert(id, message);
                    }
                }
            }
        }

        let candidate_pos = order
            .iter()
            .position(|id| *id == 20)
            .expect("candidate response should arrive");
        // The three regular submits race into the single verifier, so the
        // deepest-queued one is whichever finishes last, not necessarily id 12.
        let queued_regular_pos = order
            .iter()
            .rposition(|id| [10, 11, 12].contains(id))
            .expect("queued regular responses should arrive");
        assert!(
            candidate_pos < queued_regular_pos,
            "candidate submit should finish before the deepest queued regular submit"
        );
        assert_eq!(responses[&20]["status"].as_str(), Some("ok"));
        let snapshot = server.submit_snapshot();
        assert_eq!(snapshot.submits_received_total, 4);
        assert_eq!(snapshot.candidate_classified_total, 1);
        assert_eq!(snapshot.candidate_permit_acquired_total, 1);
        assert_eq!(snapshot.candidate_permit_rejected_total, 0);
        assert_eq!(snapshot.candidate_enqueued_total, 1);
        assert_eq!(snapshot.candidate_enqueue_rejected_total, 0);
        assert_eq!(snapshot.candidate_worker_started_total, 1);
        assert_eq!(snapshot.candidate_persisted_total, 1);
        assert_eq!(snapshot.candidate_rejected_total, 0);
        assert_eq!(snapshot.candidate_worker_failure_total, 0);

        drop(writer_a);
        drop(reader_a);
        drop(writer_b);
        drop(reader_b);
        shutdown_test_listener(listener_task, server, _jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_established_source_block_claims_use_priority_lane() {
        let (server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let job = login_test_client(
            &mut reader,
            &mut writer,
            1,
            &test_miner_address(0x55),
            "rig",
        )
        .await;

        // A fresh source's block claim goes through the shared lane.
        let first_candidate = nonce_with_parity(&job, true, 0);
        send_submit(
            &mut writer,
            2,
            &job.job_id,
            first_candidate,
            SleepyQueueHasher::claimed_hash_hex(first_candidate),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["status"].as_str(), Some("ok"));
        assert_eq!(
            server.submit_snapshot().candidate_priority_enqueued_total,
            0
        );

        // Verified shares establish the (address, IP) pair.
        for offset in 0..crate::source_risk::SOURCE_ESTABLISHED_MIN_VERIFIED {
            let nonce = nonce_with_parity(&job, false, offset);
            send_submit(
                &mut writer,
                10 + offset,
                &job.job_id,
                nonce,
                SleepyQueueHasher::claimed_hash_hex(nonce),
            )
            .await;
            let response = read_json_line(&mut reader).await;
            assert_eq!(response["status"].as_str(), Some("ok"), "{response}");
        }

        let candidate = nonce_with_parity(&job, true, 1);
        send_submit(
            &mut writer,
            20,
            &job.job_id,
            candidate,
            SleepyQueueHasher::claimed_hash_hex(candidate),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["status"].as_str(), Some("ok"));
        let snapshot = server.submit_snapshot();
        assert_eq!(snapshot.candidate_priority_enqueued_total, 1);
        assert_eq!(snapshot.candidate_enqueued_total, 2);

        drop((reader, writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_template_fanout_reaches_every_connection() {
        let (server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let mut clients = Vec::new();
        for idx in 0..24u8 {
            let (mut reader, mut writer) = connect_test_client(addr).await;
            let job = login_test_client(
                &mut reader,
                &mut writer,
                1,
                &test_miner_address(0x55),
                &format!("rig{idx}"),
            )
            .await;
            assert_eq!(job.height, 100);
            clients.push((reader, writer));
        }

        jobs.install_test_job(stratum_test_job("job-next", 101));

        let mut job_ids = std::collections::HashSet::new();
        for (reader, _) in clients.iter_mut() {
            let job = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let message = read_json_line(reader).await;
                    if message["method"].as_str() == Some("job") {
                        break message;
                    }
                }
            })
            .await
            .expect("every connection gets the new template's job");
            assert_eq!(job["params"]["height"].as_u64(), Some(101));
            assert!(job_ids.insert(job["params"]["job_id"].as_str().unwrap().to_string()));
        }
        assert_eq!(server.fanout_conns.lock().len(), 24);

        drop(clients);
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    async fn send_refill(writer: &mut OwnedWriteHalf, req_id: u64, job_id: &str) {
        write_json_line(
            writer,
            &StratumRequest {
                id: req_id,
                method: pool_common::protocol::METHOD_REFILL.to_string(),
                params: json!({ "job_id": job_id }),
            },
        )
        .await;
    }

    /// Sends a share for `job` and returns the pool's answer, which must be
    /// the next line: nothing else (such as a job) may arrive in between.
    async fn submit_and_answer(
        reader: &mut BufReader<OwnedReadHalf>,
        writer: &mut OwnedWriteHalf,
        req_id: u64,
        job_id: &str,
        nonce: u64,
    ) -> Value {
        send_submit(
            writer,
            req_id,
            job_id,
            nonce,
            SleepyQueueHasher::claimed_hash_hex(nonce),
        )
        .await;
        let answer = read_json_line(reader).await;
        assert_eq!(answer["id"].as_u64(), Some(req_id), "{answer}");
        answer
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_refill_adds_a_range_without_invalidating_held_jobs() {
        let (server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;

        // Answered like a submit before login.
        send_refill(&mut writer, 1, "job").await;
        let early = read_json_line(&mut reader).await;
        assert_eq!(early["id"], 1);
        assert_eq!(early["error"], "not logged in");

        let address = test_miner_address(0x55);
        let primary = login_test_client(&mut reader, &mut writer, 2, &address, "proxy").await;
        send_refill(&mut writer, 3, &primary.job_id).await;
        let answer = read_json_line(&mut reader).await;
        assert_eq!(answer["id"], 3);
        assert_eq!(answer["status"], "ok");
        assert!(answer.get("error").is_none());
        let fields: std::collections::BTreeSet<&str> = answer["result"]
            .as_object()
            .expect("result object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            fields,
            [
                "job_id",
                "template_id",
                "header_base",
                "target",
                "difficulty",
                "network_target",
                "height",
                "nonce_start",
                "nonce_end",
            ]
            .into_iter()
            .collect()
        );
        let refill: MinerJob = serde_json::from_value(answer["result"].clone()).unwrap();
        assert_ne!(refill.job_id, primary.job_id);
        assert_eq!(refill.template_id, primary.template_id);
        assert_eq!(refill.header_base, primary.header_base);
        assert_eq!(refill.target, primary.target);
        assert_eq!(refill.difficulty, primary.difficulty);
        assert_eq!(refill.network_target, primary.network_target);
        assert_eq!(refill.height, primary.height);
        assert!(refill.nonce_end < primary.nonce_start || primary.nonce_end < refill.nonce_start);

        // Shares on both ranges count; no job notification came with the refill.
        let refill_nonce = nonce_with_parity(&refill, false, 0);
        let accepted =
            submit_and_answer(&mut reader, &mut writer, 4, &refill.job_id, refill_nonce).await;
        assert_eq!(accepted["status"], "ok", "{accepted}");
        let primary_nonce = nonce_with_parity(&primary, false, 0);
        let accepted =
            submit_and_answer(&mut reader, &mut writer, 5, &primary.job_id, primary_nonce).await;
        assert_eq!(accepted["status"], "ok", "{accepted}");

        // Duplicates and nonces outside the job's own range are refused.
        let duplicate =
            submit_and_answer(&mut reader, &mut writer, 6, &refill.job_id, refill_nonce).await;
        assert!(
            duplicate["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("duplicate share")),
            "{duplicate}"
        );
        let foreign = nonce_with_parity(&primary, false, 1);
        let out_of_range =
            submit_and_answer(&mut reader, &mut writer, 7, &refill.job_id, foreign).await;
        assert_eq!(out_of_range["error"], "nonce out of assigned range");

        // Unknown jobs and other connections' jobs are stale.
        send_refill(&mut writer, 8, "0000000000000000").await;
        assert_eq!(read_json_line(&mut reader).await["error"], "stale job");
        let (mut other_reader, mut other_writer) = connect_test_client(addr).await;
        let other =
            login_test_client(&mut other_reader, &mut other_writer, 1, &address, "rig").await;
        send_refill(&mut other_writer, 2, &primary.job_id).await;
        assert_eq!(
            read_json_line(&mut other_reader).await["error"],
            "stale job"
        );
        assert!(other.nonce_end < refill.nonce_start || refill.nonce_end < other.nonce_start);

        // A new template replaces the jobs: refills of them are stale, while
        // shares on them keep the usual stale grace.
        jobs.install_test_job(stratum_test_job("job-next", 101));
        let next = read_json_line(&mut reader).await;
        assert_eq!(next["method"], "job");
        assert_eq!(next["params"]["height"], 101);
        send_refill(&mut writer, 9, &refill.job_id).await;
        assert_eq!(read_json_line(&mut reader).await["error"], "stale job");
        let late = submit_and_answer(
            &mut reader,
            &mut writer,
            10,
            &refill.job_id,
            nonce_with_parity(&refill, false, 2),
        )
        .await;
        assert_eq!(late["status"], "ok", "{late}");
        let next: MinerJob = serde_json::from_value(next["params"].clone()).unwrap();
        send_refill(&mut writer, 11, &next.job_id).await;
        let refilled = read_json_line(&mut reader).await;
        assert_eq!(refilled["status"], "ok", "{refilled}");
        assert_eq!(refilled["result"]["height"], 101);

        drop((reader, writer, other_reader, other_writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_refill_is_rate_limited_and_capped() {
        let cfg = Config {
            stratum_refill_rate: 0.001,
            stratum_refill_burst: 3,
            stratum_refill_max_assignments: 3,
            ..stratum_test_cfg()
        };
        let (server, jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let primary = login_test_client(
            &mut reader,
            &mut writer,
            1,
            &test_miner_address(0x55),
            "proxy",
        )
        .await;

        // The job plus two refills fill the cap of three live jobs.
        for id in 2..=3 {
            send_refill(&mut writer, id, &primary.job_id).await;
            assert_eq!(read_json_line(&mut reader).await["status"], "ok");
        }
        send_refill(&mut writer, 4, &primary.job_id).await;
        assert_eq!(
            read_json_line(&mut reader).await["error"],
            "refill limit reached"
        );
        // The burst of three is spent; the next waits for the rate.
        send_refill(&mut writer, 5, &primary.job_id).await;
        let limited = read_json_line(&mut reader).await;
        assert_eq!(limited["id"], 5);
        assert_eq!(limited["error"], "refill rate limited");

        drop((reader, writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_false_block_claim_disconnects_the_source() {
        let (server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let job =
            login_test_client(&mut reader, &mut writer, 1, &test_miner_address(0x61), "x").await;

        // Odd nonces do not hash below the network target; claiming zeroes is a lie.
        let nonce = nonce_with_parity(&job, false, 0);
        send_submit(&mut writer, 2, &job.job_id, nonce, "00".repeat(32)).await;
        let response = read_json_line(&mut reader).await;
        assert!(response["error"]
            .as_str()
            .is_some_and(|err| err.contains("invalid share proof")));
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .expect("connection should close promptly")
            .expect("read");
        assert_eq!(read, 0, "struck source is disconnected, got {line:?}");

        // Reconnecting with the same address from the same host is refused.
        let (mut reader2, mut writer2) = connect_test_client(addr).await;
        write_json_line(
            &mut writer2,
            &StratumRequest {
                id: 1,
                method: pool_common::protocol::METHOD_LOGIN.to_string(),
                params: json!({
                    "address": test_miner_address(0x61),
                    "worker": "x",
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash"],
                }),
            },
        )
        .await;
        let login = read_json_line(&mut reader2).await;
        assert!(login["error"]
            .as_str()
            .is_some_and(|err| err.starts_with("source banned")));

        drop((reader, writer, reader2, writer2));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_second_login_on_a_connection_is_refused() {
        let (server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let job = login_test_client(
            &mut reader,
            &mut writer,
            1,
            &test_miner_address(0x55),
            "rig",
        )
        .await;
        write_json_line(
            &mut writer,
            &StratumRequest {
                id: 2,
                method: pool_common::protocol::METHOD_LOGIN.to_string(),
                params: json!({
                    "address": test_miner_address(0x62),
                    "worker": "other",
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash"],
                }),
            },
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["id"], 2);
        assert_eq!(response["error"].as_str(), Some("already logged in"));

        // The original session keeps working on the same connection.
        let nonce = nonce_with_parity(&job, false, 0);
        send_submit(
            &mut writer,
            3,
            &job.job_id,
            nonce,
            SleepyQueueHasher::claimed_hash_hex(nonce),
        )
        .await;
        let response = read_json_line(&mut reader).await;
        assert_eq!(response["status"].as_str(), Some("ok"), "{response}");
        drop((reader, writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tcp_connection_over_the_cap_is_told_to_back_off_before_closing() {
        let cfg = Config {
            stratum_max_connections: 1,
            ..stratum_test_cfg()
        };
        let (server, jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (_held_reader, _held_writer) = connect_test_client(addr).await;
        for _ in 0..40 {
            if server.conn_state.lock().total == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let (mut reader, _writer) = connect_test_client(addr).await;
        let notice = read_json_line(&mut reader).await;
        assert_eq!(notice["method"], pool_common::protocol::METHOD_NOTIFICATION);
        assert_eq!(
            notice["params"]["kind"],
            pool_common::protocol::NOTIFY_CONNECTION_LIMIT
        );
        let retry = notice["params"]["retry_after_ms"].as_u64().unwrap();
        assert!(super::CONNECTION_LIMIT_RETRY_MS.contains(&retry), "{retry}");
        let mut rest = String::new();
        let read = tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut rest))
            .await
            .expect("refused connection closes");
        assert_eq!(read.unwrap(), 0);
        assert_eq!(server.connection_snapshot().rejected_at_cap_total, 1);
        drop((_held_reader, _held_writer));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn heap_trim_returns_freed_memory_to_the_os() {
        // Other tests allocate concurrently, so one attempt can see their
        // churn; a trim that works lowers held memory in at least one try.
        let trimmed = (0..5).any(|_| {
            // Free a few hundred MB in small blocks, which glibc keeps for reuse.
            let blocks: Vec<Vec<u8>> = (0..200_000).map(|i| vec![i as u8; 1_500]).collect();
            drop(blocks);
            let before = super::heap_snapshot().expect("mallinfo2").free_held_bytes;
            super::trim_heap();
            let after = super::heap_snapshot().expect("mallinfo2").free_held_bytes;
            after < before
        });
        assert!(trimmed, "malloc_trim never lowered held memory");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ipv6_connection_cap_is_shared_per_slash_64() {
        let (server, jobs) = build_tcp_test_server(stratum_test_cfg()).await;
        let host_a = ip_group_key("2001:db8:7:9::1".parse().unwrap());
        let host_b = ip_group_key("2001:db8:7:9:ffff::2".parse().unwrap());
        let other_net = ip_group_key("2001:db8:7:a::1".parse().unwrap());
        for _ in 0..server.max_conns_per_ip {
            assert!(server.try_track_conn(&host_a));
        }
        assert!(!server.try_track_conn(&host_b), "same /64 shares the cap");
        assert!(server.try_track_conn(&other_net));
        let connections = server.connection_snapshot();
        assert_eq!(connections.open, server.max_conns_per_ip + 1);
        assert_eq!(connections.rejected_per_ip_total, 1);
        assert_eq!(connections.rejected_at_cap_total, 0);
        tokio::task::spawn_blocking(move || {
            drop(server);
            drop(jobs);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_fork_job_closes_sessions_without_blake2b_and_refuses_their_login() {
        use pool_common::faraday::FARADAY_HEIGHT;
        let cfg = stratum_test_cfg();
        let (server, jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let address = test_miner_address(0x71);
        let login = |id: u64, worker: &str, capabilities: Value| StratumRequest {
            id,
            method: pool_common::protocol::METHOD_LOGIN.to_string(),
            params: json!({
                "address": address,
                "worker": worker,
                "protocol_version": 2,
                "capabilities": capabilities,
            }),
        };

        let (mut old_reader, mut old_writer) = connect_test_client(addr).await;
        login_test_client(&mut old_reader, &mut old_writer, 1, &address, "old").await;
        let (mut new_reader, mut new_writer) = connect_test_client(addr).await;
        write_json_line(
            &mut new_writer,
            &login(1, "new", json!(["submit_claimed_hash", "pow_blake2b_v1"])),
        )
        .await;
        assert_eq!(
            read_json_line(&mut new_reader).await["status"].as_str(),
            Some("ok")
        );
        assert_eq!(
            read_json_line(&mut new_reader).await["method"].as_str(),
            Some("job")
        );

        jobs.install_test_job(stratum_test_job("job-fork", FARADAY_HEIGHT));

        let fork_job = loop {
            let message = read_json_line(&mut new_reader).await;
            if message["method"].as_str() == Some("job") {
                break message;
            }
        };
        assert_eq!(fork_job["params"]["height"].as_u64(), Some(FARADAY_HEIGHT));

        // The session without BLAKE2b is closed and never sent fork work.
        loop {
            let mut line = String::new();
            let read =
                tokio::time::timeout(Duration::from_secs(2), old_reader.read_line(&mut line))
                    .await
                    .expect("the old session should be closed");
            if read.map_or(true, |bytes| bytes == 0) {
                break;
            }
            let message: Value = serde_json::from_str(line.trim()).expect("json");
            assert!(
                !(message["method"].as_str() == Some("job")
                    && message["params"]["height"].as_u64() >= Some(FARADAY_HEIGHT)),
                "fork work sent to a miner without BLAKE2b"
            );
        }

        // Its reconnect gets the upgrade message.
        let (mut retry_reader, mut retry_writer) = connect_test_client(addr).await;
        write_json_line(
            &mut retry_writer,
            &login(1, "old", json!(["submit_claimed_hash"])),
        )
        .await;
        let refused = read_json_line(&mut retry_reader).await;
        assert!(refused["error"]
            .as_str()
            .is_some_and(|error| error.contains("Faraday fork")));

        drop((old_writer, new_writer, retry_writer));
        drop((old_reader, new_reader, retry_reader));
        shutdown_test_listener(listener_task, server, jobs).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tcp_job_notifications_still_flow_while_submits_are_backed_up() {
        let cfg = stratum_test_cfg();
        let (server, jobs) = build_tcp_test_server(cfg).await;
        let (addr, listener_task) = start_test_tcp_listener(Arc::clone(&server)).await;
        let (mut reader, mut writer) = connect_test_client(addr).await;
        let address = test_miner_address(0x71);
        let miner_job = login_test_client(&mut reader, &mut writer, 1, &address, "rig-a").await;

        let nonce1 = nonce_with_parity(&miner_job, false, 0);
        let nonce2 = nonce_with_parity(&miner_job, false, 1);
        send_submit(
            &mut writer,
            2,
            &miner_job.job_id,
            nonce1,
            SleepyQueueHasher::claimed_hash_hex(nonce1),
        )
        .await;
        send_submit(
            &mut writer,
            3,
            &miner_job.job_id,
            nonce2,
            SleepyQueueHasher::claimed_hash_hex(nonce2),
        )
        .await;

        jobs.install_test_job(stratum_test_job("job-next", 101));

        let deadline = Instant::now() + Duration::from_millis(150);
        let message = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let message = tokio::time::timeout(remaining, read_json_line(&mut reader))
                .await
                .expect("new job should arrive before the queued submit finishes");
            if message["method"].as_str() == Some("job") {
                break message;
            }
        };
        assert_eq!(message["method"].as_str(), Some("job"));
        assert_eq!(message["params"]["height"].as_u64(), Some(101));
        assert_eq!(message["params"]["template_id"].as_str(), Some("tmpl-101"));

        drop(writer);
        drop(reader);
        shutdown_test_listener(listener_task, server, jobs).await;
    }
}
