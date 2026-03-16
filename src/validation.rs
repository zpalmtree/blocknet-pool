use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::db::{PendingAuditShare, ShareReplayUpdate, ValidationHoldCause};
use crate::pow::{check_target, PowHasher};
use crate::telemetry::{default_latency_window, LatencyWindow, PercentileSummary, QueueTracker};

pub const SHARE_STATUS_VERIFIED: &str = "verified";
pub const SHARE_STATUS_PROVISIONAL: &str = "provisional";
pub const SHARE_STATUS_REJECTED: &str = "rejected";
const VALIDATION_STATE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const VALIDATION_STATE_MAX_TRACKED: usize = 100_000;
const VALIDATION_PERSIST_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const VALIDATION_CLEAR_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const VALIDATION_AUDIT_SCHED_INTERVAL: Duration = Duration::from_secs(2);
const VALIDATION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
const VALIDATION_AUDIT_TARGET_QUEUE_MULTIPLIER: usize = 2;

#[derive(Debug, Clone)]
pub struct ValidationTask {
    pub address: String,
    pub nonce: u64,
    pub difficulty: u64,
    pub header_base: Vec<u8>,
    pub share_target: [u8; 32],
    pub network_target: [u8; 32],
    pub claimed_hash: Option<[u8; 32]>,
    pub candidate_claim: bool,
    pub force_full_verify: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ValidationResult {
    pub nonce: u64,
    pub accepted: bool,
    pub reject_reason: Option<&'static str>,
    pub hash: [u8; 32],
    pub verified: bool,
    pub is_block_candidate: bool,
    pub suspected_fraud: bool,
    pub followup_action: ValidationFollowupAction,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ValidationComputeResult {
    pub nonce: u64,
    pub accepted: bool,
    pub reject_reason: Option<&'static str>,
    pub hash: [u8; 32],
    pub verified: bool,
    pub is_block_candidate: bool,
    pub suspected_fraud: bool,
    pub candidate_false_claim: bool,
    pub overload_mode: OverloadMode,
}

#[derive(Debug, Clone, Copy)]
struct ValidationPlan {
    full_verify: bool,
    overload_mode: OverloadMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ValidationFollowupAction {
    #[default]
    None,
    Quarantine,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OverloadMode {
    #[default]
    Normal,
    Shed,
    Emergency,
}

#[derive(Debug, Clone)]
pub struct PersistedValidationAddressState {
    pub address: String,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub risk_sampled_shares: u64,
    pub risk_invalid_samples: u64,
    pub forced_started_at: Option<SystemTime>,
    pub forced_until: Option<SystemTime>,
    pub forced_sampled_shares: u64,
    pub forced_invalid_samples: u64,
    pub resume_forced_at: Option<SystemTime>,
    pub hold_cause: Option<ValidationHoldCause>,
    pub last_seen_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PersistedValidationProvisional {
    pub address: String,
    pub share_id: Option<i64>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PersistedValidationAcceptedShare {
    pub address: String,
    pub share_id: i64,
    pub created_at: SystemTime,
    pub difficulty: u64,
    pub verified: bool,
}

#[derive(Debug, Clone)]
pub struct ValidationClearEvent {
    pub id: i64,
    pub address: String,
    pub cleared_at: SystemTime,
}

#[derive(Debug, Clone, Default)]
pub struct LoadedValidationState {
    pub states: Vec<PersistedValidationAddressState>,
    pub provisionals: Vec<PersistedValidationProvisional>,
    pub accepted_window: Vec<PersistedValidationAcceptedShare>,
}

#[derive(Debug, Clone, Default)]
pub struct LoadedValidationAddressActivity {
    pub provisionals: Vec<PersistedValidationProvisional>,
    pub accepted_window: Vec<PersistedValidationAcceptedShare>,
}

pub trait ValidationStateStore: Send + Sync + 'static {
    fn load_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        accepted_window_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState>;

    fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()>;

    fn add_validation_provisional(
        &self,
        address: &str,
        share_id: Option<i64>,
        created_at: SystemTime,
    ) -> Result<()>;

    fn clean_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<()>;

    fn latest_validation_clear_event_id(&self) -> Result<i64>;

    fn load_validation_clear_events_since(&self, cursor: i64) -> Result<Vec<ValidationClearEvent>>;

    fn load_validation_address_activity(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
        accepted_window_cutoff: SystemTime,
    ) -> Result<LoadedValidationAddressActivity>;

    fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<()>;

    fn load_recent_provisional_audit_shares(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
        limit: usize,
    ) -> Result<Vec<PendingAuditShare>>;

    fn load_pending_payout_audit_shares(
        &self,
        address: &str,
        config: &Config,
        now: SystemTime,
        limit: usize,
    ) -> Result<Vec<PendingAuditShare>>;
}

#[derive(Debug, Default)]
struct NullValidationStateStore;

impl ValidationStateStore for NullValidationStateStore {
    fn load_validation_state(
        &self,
        _state_cutoff: SystemTime,
        _provisional_cutoff: SystemTime,
        _accepted_window_cutoff: SystemTime,
        _now: SystemTime,
    ) -> Result<LoadedValidationState> {
        Ok(LoadedValidationState::default())
    }

    fn upsert_validation_state(&self, _state: &PersistedValidationAddressState) -> Result<()> {
        Ok(())
    }

    fn add_validation_provisional(
        &self,
        _address: &str,
        _share_id: Option<i64>,
        _created_at: SystemTime,
    ) -> Result<()> {
        Ok(())
    }

    fn clean_validation_state(
        &self,
        _state_cutoff: SystemTime,
        _provisional_cutoff: SystemTime,
        _now: SystemTime,
    ) -> Result<()> {
        Ok(())
    }

    fn latest_validation_clear_event_id(&self) -> Result<i64> {
        Ok(0)
    }

    fn load_validation_clear_events_since(
        &self,
        _cursor: i64,
    ) -> Result<Vec<ValidationClearEvent>> {
        Ok(Vec::new())
    }

    fn load_validation_address_activity(
        &self,
        _address: &str,
        _provisional_cutoff: SystemTime,
        _accepted_window_cutoff: SystemTime,
    ) -> Result<LoadedValidationAddressActivity> {
        Ok(LoadedValidationAddressActivity::default())
    }

    fn complete_validation_audit(&self, _update: &ShareReplayUpdate) -> Result<()> {
        Ok(())
    }

    fn load_recent_provisional_audit_shares(
        &self,
        _address: &str,
        _provisional_cutoff: SystemTime,
        _limit: usize,
    ) -> Result<Vec<PendingAuditShare>> {
        Ok(Vec::new())
    }

    fn load_pending_payout_audit_shares(
        &self,
        _address: &str,
        _config: &Config,
        _now: SystemTime,
        _limit: usize,
    ) -> Result<Vec<PendingAuditShare>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
struct ValidationAddressState {
    total_shares: u64,
    sampled_shares: u64,
    invalid_samples: u64,
    risk_sampled_shares: u64,
    risk_invalid_samples: u64,
    forced_started_at: Option<SystemTime>,
    forced_until: Option<SystemTime>,
    forced_sampled_shares: u64,
    forced_invalid_samples: u64,
    resume_forced_at: Option<SystemTime>,
    hold_cause: Option<ValidationHoldCause>,
    provisional_at: VecDeque<TrackedProvisionalShare>,
    accepted_window: VecDeque<AcceptedWindowShare>,
    last_replay_backfill_at: Option<SystemTime>,
    last_seen_at: SystemTime,
}

#[derive(Debug, Clone, Copy)]
struct TrackedProvisionalShare {
    share_id: Option<i64>,
    created_at: SystemTime,
}

#[derive(Debug, Clone, Copy)]
struct AcceptedWindowShare {
    share_id: i64,
    created_at: SystemTime,
    difficulty: u64,
    verified: bool,
}

impl Default for ValidationAddressState {
    fn default() -> Self {
        Self {
            total_shares: 0,
            sampled_shares: 0,
            invalid_samples: 0,
            risk_sampled_shares: 0,
            risk_invalid_samples: 0,
            forced_started_at: None,
            forced_until: None,
            forced_sampled_shares: 0,
            forced_invalid_samples: 0,
            resume_forced_at: None,
            hold_cause: None,
            provisional_at: VecDeque::new(),
            accepted_window: VecDeque::new(),
            last_replay_backfill_at: None,
            last_seen_at: SystemTime::now(),
        }
    }
}

impl ValidationAddressState {
    fn clear_risk_window(&mut self) {
        self.risk_sampled_shares = 0;
        self.risk_invalid_samples = 0;
    }

    fn clear_forced_review(&mut self) {
        self.forced_started_at = None;
        self.forced_until = None;
        self.forced_sampled_shares = 0;
        self.forced_invalid_samples = 0;
        self.resume_forced_at = None;
        self.hold_cause = None;
        self.last_replay_backfill_at = None;
    }

    fn start_forced_review(
        &mut self,
        start_at: SystemTime,
        duration: Duration,
        cause: ValidationHoldCause,
    ) {
        self.forced_started_at = Some(start_at);
        self.forced_until = Some(start_at + duration);
        self.forced_sampled_shares = 0;
        self.forced_invalid_samples = 0;
        self.resume_forced_at = None;
        self.hold_cause = Some(cause);
        self.last_replay_backfill_at = None;
        self.clear_risk_window();
    }

    fn start_temporary_hold(
        &mut self,
        start_at: SystemTime,
        duration: Duration,
        cause: ValidationHoldCause,
    ) {
        self.forced_started_at = None;
        self.forced_until = Some(start_at + duration);
        self.resume_forced_at = None;
        self.hold_cause = Some(cause);
        self.last_replay_backfill_at = None;
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ValidationSnapshot {
    pub in_flight: i64,
    pub candidate_queue_depth: usize,
    pub regular_queue_depth: usize,
    pub audit_queue_depth: usize,
    pub candidate_oldest_age_millis: Option<u64>,
    pub regular_oldest_age_millis: Option<u64>,
    pub audit_oldest_age_millis: Option<u64>,
    pub candidate_wait: PercentileSummary,
    pub regular_wait: PercentileSummary,
    pub audit_wait: PercentileSummary,
    pub validation_duration: PercentileSummary,
    pub audit_duration: PercentileSummary,
    pub tracked_addresses: usize,
    pub forced_verify_addresses: usize,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub pending_provisional: u64,
    pub fraud_detections: u64,
    pub candidate_false_claims: u64,
    pub hot_accepts: u64,
    pub sync_full_verifies: u64,
    pub audit_enqueued: u64,
    pub audit_verified: u64,
    pub audit_rejected: u64,
    pub audit_deferred: u64,
    pub overload_mode: OverloadMode,
    pub effective_sample_rate: f64,
}

struct QueuedTask {
    task: ValidationTask,
    result_tx: flume::Sender<ValidationComputeResult>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationAuditSource {
    Sampled,
    ForcedReview,
    PayoutCoverage,
    ProvisionalBacklog,
}

#[derive(Debug, Clone)]
struct QueuedAuditTask {
    share: PendingAuditShare,
    source: ValidationAuditSource,
}

enum RegularWorkItem {
    Sync(QueuedTask),
    Audit(QueuedAuditTask),
}

#[derive(Debug, Clone, Copy)]
pub struct RegularSubmitPlan {
    pub sync_full_verify: bool,
    pub enqueue_audit: bool,
    pub audit_source: Option<ValidationAuditSource>,
    pub overload_mode: OverloadMode,
}

#[derive(Debug, Clone, Copy)]
struct OverloadState {
    mode: OverloadMode,
    below_clear_started_at: Option<Instant>,
}

impl Default for OverloadState {
    fn default() -> Self {
        Self {
            mode: OverloadMode::Normal,
            below_clear_started_at: None,
        }
    }
}

struct ValidationInner {
    config: Config,
    hasher: Arc<dyn PowHasher>,
    state: Mutex<HashMap<String, ValidationAddressState>>,
    rng: Mutex<StdRng>,
    in_flight: AtomicI64,
    fraud: AtomicU64,
    candidate_false_claims: AtomicU64,
    hot_accepts: AtomicU64,
    sync_full_verifies: AtomicU64,
    audit_enqueued: AtomicU64,
    audit_verified: AtomicU64,
    audit_rejected: AtomicU64,
    audit_deferred: AtomicU64,
    state_store: Arc<dyn ValidationStateStore>,
    last_cleanup_at: Mutex<Option<Instant>>,
    last_clear_sync_at: Mutex<Option<Instant>>,
    last_clear_event_id: Mutex<i64>,
    candidate_queue: QueueTracker,
    regular_queue: QueueTracker,
    audit_queue: QueueTracker,
    audit_pending_shares: Mutex<HashSet<i64>>,
    regular_tx: flume::Sender<RegularWorkItem>,
    audit_tx: flume::Sender<QueuedAuditTask>,
    submit_regular_queue: Mutex<Option<Arc<QueueTracker>>>,
    validation_duration: Mutex<LatencyWindow>,
    audit_duration: Mutex<LatencyWindow>,
    overload: Mutex<OverloadState>,
}

pub struct ValidationEngine {
    inner: Arc<ValidationInner>,
    candidate_tx: flume::Sender<QueuedTask>,
    regular_tx: flume::Sender<RegularWorkItem>,
    shutdown: Arc<AtomicBool>,
    maintenance_worker: Option<thread::JoinHandle<()>>,
    candidate_workers: Vec<thread::JoinHandle<()>>,
    regular_workers: Vec<thread::JoinHandle<()>>,
    audit_workers: Vec<thread::JoinHandle<()>>,
}

impl ValidationEngine {
    pub fn new(config: Config, hasher: Arc<dyn PowHasher>) -> Self {
        Self::new_with_state_store(config, hasher, Arc::new(NullValidationStateStore))
    }

    pub fn new_with_state_store(
        config: Config,
        hasher: Arc<dyn PowHasher>,
        state_store: Arc<dyn ValidationStateStore>,
    ) -> Self {
        let candidate_workers = config.candidate_verifier_count();
        let regular_workers = config.regular_verifier_count();
        let audit_workers = config.audit_verifier_count();
        let candidate_queue_size = config.candidate_validation_queue_size();
        let regular_queue_size = config.regular_validation_queue_size();
        let audit_queue_size = config.audit_validation_queue_size();

        let (candidate_tx, candidate_rx) = flume::bounded::<QueuedTask>(candidate_queue_size);
        let (regular_tx, regular_rx) = flume::bounded::<RegularWorkItem>(regular_queue_size);
        let (audit_tx, audit_rx) = flume::bounded::<QueuedAuditTask>(audit_queue_size);
        let shutdown = Arc::new(AtomicBool::new(false));
        let initial_clear_event_id = match state_store.latest_validation_clear_event_id() {
            Ok(value) => value.max(0),
            Err(err) => {
                tracing::warn!(error = %err, "failed loading validation clear cursor");
                0
            }
        };
        let initial_state = load_initial_validation_state(&config, state_store.as_ref());

        let inner = Arc::new(ValidationInner {
            config,
            hasher,
            state: Mutex::new(initial_state),
            rng: Mutex::new(StdRng::from_entropy()),
            in_flight: AtomicI64::new(0),
            fraud: AtomicU64::new(0),
            candidate_false_claims: AtomicU64::new(0),
            hot_accepts: AtomicU64::new(0),
            sync_full_verifies: AtomicU64::new(0),
            audit_enqueued: AtomicU64::new(0),
            audit_verified: AtomicU64::new(0),
            audit_rejected: AtomicU64::new(0),
            audit_deferred: AtomicU64::new(0),
            state_store,
            last_cleanup_at: Mutex::new(None),
            last_clear_sync_at: Mutex::new(None),
            last_clear_event_id: Mutex::new(initial_clear_event_id),
            candidate_queue: QueueTracker::new(512),
            regular_queue: QueueTracker::new(512),
            audit_queue: QueueTracker::new(512),
            audit_pending_shares: Mutex::new(HashSet::new()),
            regular_tx: regular_tx.clone(),
            audit_tx: audit_tx.clone(),
            submit_regular_queue: Mutex::new(None),
            validation_duration: Mutex::new(default_latency_window()),
            audit_duration: Mutex::new(default_latency_window()),
            overload: Mutex::new(OverloadState::default()),
        });

        let mut candidate_handles = Vec::with_capacity(candidate_workers);
        for _ in 0..candidate_workers {
            let inner_cloned = Arc::clone(&inner);
            let candidate_rx = candidate_rx.clone();
            let shutdown = Arc::clone(&shutdown);
            candidate_handles.push(thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    match candidate_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(queued) => inner_cloned.process(queued, true),
                        Err(flume::RecvTimeoutError::Timeout) => continue,
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }));
        }

        let mut regular_handles = Vec::with_capacity(regular_workers);
        for _ in 0..regular_workers {
            let inner_cloned = Arc::clone(&inner);
            let regular_rx = regular_rx.clone();
            let shutdown = Arc::clone(&shutdown);
            regular_handles.push(thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    match regular_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(queued) => inner_cloned.process_regular_work(queued),
                        Err(flume::RecvTimeoutError::Timeout) => continue,
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }));
        }

        let mut audit_handles = Vec::with_capacity(audit_workers);
        for _ in 0..audit_workers {
            let inner_cloned = Arc::clone(&inner);
            let audit_rx = audit_rx.clone();
            let shutdown = Arc::clone(&shutdown);
            audit_handles.push(thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    match audit_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(queued) => inner_cloned.process_background_audit(queued),
                        Err(flume::RecvTimeoutError::Timeout) => continue,
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }));
        }

        let maintenance_inner = Arc::clone(&inner);
        let maintenance_shutdown = Arc::clone(&shutdown);
        let maintenance_worker = Some(thread::spawn(move || {
            while !maintenance_shutdown.load(Ordering::Relaxed) {
                maintenance_inner.maintenance_tick();
                thread::sleep(VALIDATION_MAINTENANCE_INTERVAL);
            }
        }));

        tracing::info!(
            candidate_submit_workers = candidate_workers,
            regular_submit_workers = regular_workers,
            audit_workers = audit_workers,
            candidate_queue = candidate_queue_size,
            regular_queue = regular_queue_size,
            audit_queue = audit_queue_size,
            "validation engine initialized"
        );

        Self {
            inner,
            candidate_tx,
            regular_tx,
            shutdown,
            maintenance_worker,
            candidate_workers: candidate_handles,
            regular_workers: regular_handles,
            audit_workers: audit_handles,
        }
    }

    pub(crate) fn submit(
        &self,
        task: ValidationTask,
        candidate: bool,
    ) -> Option<flume::Receiver<ValidationComputeResult>> {
        let (tx, rx) = flume::bounded(1);
        let queued = QueuedTask {
            task,
            result_tx: tx,
        };
        let queued_at = Instant::now();
        let tracker_id = if candidate {
            self.inner.candidate_queue.push(queued_at)
        } else {
            self.inner.regular_queue.push(queued_at)
        };

        if candidate {
            match self.candidate_tx.try_send(queued) {
                Ok(()) => {
                    self.inner.evaluate_overload(Instant::now());
                    Some(rx)
                }
                Err(flume::TrySendError::Full(_)) | Err(flume::TrySendError::Disconnected(_)) => {
                    self.inner.candidate_queue.remove(tracker_id);
                    None
                }
            }
        } else {
            match self.regular_tx.try_send(RegularWorkItem::Sync(queued)) {
                Ok(()) => {
                    self.inner.evaluate_overload(Instant::now());
                    Some(rx)
                }
                Err(flume::TrySendError::Full(_)) | Err(flume::TrySendError::Disconnected(_)) => {
                    self.inner.regular_queue.remove(tracker_id);
                    None
                }
            }
        }
    }

    pub(crate) fn process_inline(&self, task: ValidationTask) -> ValidationResult {
        let started_at = Instant::now();
        self.inner.in_flight.fetch_add(1, Ordering::Relaxed);
        let prepared = self.inner.prepare_task(&task.address);
        let computed = self.inner.compute_task(&task, prepared);
        if computed.verified {
            self.inner
                .sync_full_verifies
                .fetch_add(1, Ordering::Relaxed);
        }
        let result = self.inner.finalize_result(&task, computed);
        self.inner.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.inner
            .validation_duration
            .lock()
            .record(started_at.elapsed());
        self.inner.evaluate_overload(Instant::now());
        result
    }

    pub(crate) fn complete_result(
        &self,
        task: &ValidationTask,
        computed: ValidationComputeResult,
    ) -> ValidationResult {
        self.inner.finalize_result(task, computed)
    }

    pub(crate) fn plan_regular_submit(&self, address: &str) -> RegularSubmitPlan {
        self.inner.plan_regular_submit(address)
    }

    pub(crate) fn record_hot_accept(
        &self,
        address: &str,
        share_id: i64,
        difficulty: u64,
        created_at: SystemTime,
        live_audit: Option<PendingAuditShare>,
        plan: RegularSubmitPlan,
    ) {
        self.inner.record_hot_accept(
            address,
            share_id,
            difficulty,
            created_at,
            plan.overload_mode,
        );
        if let (true, Some(source), Some(share)) =
            (plan.enqueue_audit, plan.audit_source, live_audit)
        {
            self.inner.enqueue_live_audit(share, source);
        }
    }

    pub(crate) fn attach_submit_regular_queue(&self, queue: Arc<QueueTracker>) {
        *self.inner.submit_regular_queue.lock() = Some(queue);
        self.inner.evaluate_overload(Instant::now());
    }

    pub fn snapshot(&self) -> ValidationSnapshot {
        self.inner.snapshot()
    }

    pub fn schedule_forced_review_after(&self, address: &str, start_at: SystemTime) {
        self.inner.schedule_forced_review_after(address, start_at);
    }

    pub fn clear_address_state(&self, address: &str) {
        self.inner.clear_address_state(address);
    }
}

impl Drop for ValidationEngine {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.maintenance_worker.take() {
            let _ = handle.join();
        }
        while let Some(handle) = self.candidate_workers.pop() {
            let _ = handle.join();
        }
        while let Some(handle) = self.regular_workers.pop() {
            let _ = handle.join();
        }
        while let Some(handle) = self.audit_workers.pop() {
            let _ = handle.join();
        }
    }
}

impl ValidationInner {
    fn maintenance_tick(&self) {
        self.sync_external_clears(false);
        let now = SystemTime::now();
        self.refresh_dynamic_state(now);
        let overload_mode = self.evaluate_overload(Instant::now());
        if overload_mode == OverloadMode::Normal {
            self.schedule_background_audits(now);
        }
    }

    fn process(&self, queued: QueuedTask, candidate_lane: bool) {
        let started_at = Instant::now();
        if candidate_lane {
            self.candidate_queue.pop_and_record_wait(started_at);
        } else {
            self.regular_queue.pop_and_record_wait(started_at);
        }
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let prepared = self.prepare_task(&queued.task.address);
        let result = self.compute_task(&queued.task, prepared);
        if result.verified {
            self.sync_full_verifies.fetch_add(1, Ordering::Relaxed);
        }
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.validation_duration.lock().record(started_at.elapsed());
        self.evaluate_overload(Instant::now());
        let _ = queued.result_tx.send(result);
    }

    fn process_regular_work(&self, queued: RegularWorkItem) {
        match queued {
            RegularWorkItem::Sync(task) => self.process(task, false),
            RegularWorkItem::Audit(task) => self.process_live_audit(task),
        }
    }

    fn prepare_task(&self, address: &str) -> ValidationPlan {
        self.sync_external_clears(self.address_has_live_hold(address));
        let overload_mode = self.evaluate_overload(Instant::now());
        let full_verify = self.should_fully_verify(address, overload_mode);
        ValidationPlan {
            full_verify,
            overload_mode,
        }
    }

    fn compute_task(&self, task: &ValidationTask, plan: ValidationPlan) -> ValidationComputeResult {
        let full_verify = task.force_full_verify || plan.full_verify;

        let mut result = ValidationComputeResult {
            nonce: task.nonce,
            accepted: false,
            reject_reason: None,
            hash: [0u8; 32],
            verified: full_verify,
            is_block_candidate: false,
            suspected_fraud: false,
            candidate_false_claim: false,
            overload_mode: plan.overload_mode,
        };

        if full_verify {
            let hash = match self.hasher.hash(&task.header_base, task.nonce) {
                Ok(hash) => hash,
                Err(err) => {
                    tracing::warn!(
                        address = %task.address,
                        nonce = task.nonce,
                        error = %err,
                        "share hash computation failed"
                    );
                    result.reject_reason = Some("hash computation failed");
                    return result;
                }
            };
            result.hash = hash;

            if let Some(claimed_hash) = task.claimed_hash {
                if claimed_hash != hash {
                    result.reject_reason = Some("invalid share proof");
                    result.suspected_fraud = true;
                    result.candidate_false_claim = task.candidate_claim;
                } else if !check_target(hash, task.share_target) {
                    result.reject_reason = Some("low difficulty share");
                } else {
                    result.accepted = true;
                    result.is_block_candidate = check_target(hash, task.network_target);
                }
            } else if !check_target(hash, task.share_target) {
                result.reject_reason = Some("low difficulty share");
            } else {
                result.accepted = true;
                result.is_block_candidate = check_target(hash, task.network_target);
            }
        } else {
            match task.claimed_hash {
                None => {
                    result.reject_reason = Some("claimed hash required");
                }
                Some(claimed_hash) if !check_target(claimed_hash, task.share_target) => {
                    result.reject_reason = Some("low difficulty share");
                }
                Some(claimed_hash) => {
                    result.accepted = true;
                    result.hash = claimed_hash;
                    result.is_block_candidate = check_target(claimed_hash, task.network_target);
                }
            }
        }

        result
    }

    fn should_fully_verify(&self, address: &str, overload_mode: OverloadMode) -> bool {
        if self.config.validation_mode.eq_ignore_ascii_case("full") {
            return true;
        }

        let now = SystemTime::now();
        let state = self.state.lock();
        let mut default_state = ValidationAddressState::default();
        prune_accepted_window(&mut default_state, &self.config, now);
        let st = state.get(address).unwrap_or(&default_state);
        let mut effective = st.clone();
        self.prune_provisional_locked(&mut effective, now);
        prune_accepted_window(&mut effective, &self.config, now);
        refresh_dynamic_validation_hold(&mut effective, &self.config, now);

        if effective
            .resume_forced_at
            .is_some_and(|start_at| start_at <= now)
            || effective.forced_started_at.is_some()
            || effective
                .forced_until
                .is_some_and(|deadline| now < deadline)
        {
            return true;
        }

        if overload_mode == OverloadMode::Normal
            && should_force_for_provisional_backlog(&effective, &self.config, now)
        {
            return true;
        }

        if overload_mode == OverloadMode::Normal
            && should_force_for_payout_coverage(&effective, &self.config, now)
        {
            return true;
        }

        if overload_mode == OverloadMode::Emergency {
            return false;
        }

        if overload_mode == OverloadMode::Normal {
            let next_share_idx = effective.total_shares.saturating_add(1);
            if self.config.warmup_shares > 0 && next_share_idx <= self.config.warmup_shares as u64 {
                return true;
            }
            if self.config.min_sample_every > 0
                && next_share_idx.is_multiple_of(self.config.min_sample_every as u64)
            {
                return true;
            }
        }

        let rate = self.effective_sample_rate(overload_mode);
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }

        self.rng.lock().gen::<f64>() < rate
    }

    fn plan_regular_submit(&self, address: &str) -> RegularSubmitPlan {
        self.sync_external_clears(self.address_has_live_hold(address));
        let overload_mode = self.evaluate_overload(Instant::now());
        if self.config.validation_mode.eq_ignore_ascii_case("full") {
            return RegularSubmitPlan {
                sync_full_verify: true,
                enqueue_audit: false,
                audit_source: None,
                overload_mode,
            };
        }

        let now = SystemTime::now();
        let state = self.state.lock();
        let mut default_state = ValidationAddressState::default();
        prune_accepted_window(&mut default_state, &self.config, now);
        let st = state.get(address).unwrap_or(&default_state);
        let mut effective = st.clone();
        self.prune_provisional_locked(&mut effective, now);
        prune_accepted_window(&mut effective, &self.config, now);
        refresh_dynamic_validation_hold(&mut effective, &self.config, now);

        let forced_review = effective
            .resume_forced_at
            .is_some_and(|start_at| start_at <= now)
            || effective.forced_started_at.is_some();
        let active_temporary_hold = matches!(
            effective.hold_cause,
            Some(ValidationHoldCause::ProvisionalBacklog | ValidationHoldCause::PayoutCoverage)
        ) && effective
            .forced_until
            .is_some_and(|deadline| deadline > now);

        let audit_source = if forced_review {
            Some(ValidationAuditSource::ForcedReview)
        } else if active_temporary_hold {
            match effective.hold_cause {
                Some(ValidationHoldCause::ProvisionalBacklog) => {
                    Some(ValidationAuditSource::ProvisionalBacklog)
                }
                Some(ValidationHoldCause::PayoutCoverage) => {
                    Some(ValidationAuditSource::PayoutCoverage)
                }
                _ => None,
            }
        } else if overload_mode == OverloadMode::Normal
            && should_force_for_provisional_backlog(&effective, &self.config, now)
        {
            Some(ValidationAuditSource::ProvisionalBacklog)
        } else if overload_mode == OverloadMode::Normal
            && should_force_for_payout_coverage(&effective, &self.config, now)
        {
            Some(ValidationAuditSource::PayoutCoverage)
        } else if overload_mode == OverloadMode::Emergency {
            None
        } else if overload_mode == OverloadMode::Normal {
            let next_share_idx = effective.total_shares.saturating_add(1);
            if self.config.warmup_shares > 0 && next_share_idx <= self.config.warmup_shares as u64 {
                Some(ValidationAuditSource::Sampled)
            } else if self.config.min_sample_every > 0
                && next_share_idx.is_multiple_of(self.config.min_sample_every as u64)
            {
                Some(ValidationAuditSource::Sampled)
            } else {
                let rate = self.effective_sample_rate(overload_mode);
                if rate >= 1.0 || (rate > 0.0 && self.rng.lock().gen::<f64>() < rate) {
                    Some(ValidationAuditSource::Sampled)
                } else {
                    None
                }
            }
        } else {
            let rate = self.effective_sample_rate(overload_mode);
            if rate >= 1.0 || (rate > 0.0 && self.rng.lock().gen::<f64>() < rate) {
                Some(ValidationAuditSource::Sampled)
            } else {
                None
            }
        };

        let sync_full_verify = matches!(audit_source, Some(ValidationAuditSource::ForcedReview));
        RegularSubmitPlan {
            sync_full_verify,
            enqueue_audit: !sync_full_verify && audit_source.is_some(),
            audit_source,
            overload_mode,
        }
    }

    fn finalize_result(
        &self,
        task: &ValidationTask,
        computed: ValidationComputeResult,
    ) -> ValidationResult {
        if computed.suspected_fraud {
            self.fraud.fetch_add(1, Ordering::Relaxed);
            if computed.candidate_false_claim {
                self.candidate_false_claims.fetch_add(1, Ordering::Relaxed);
            }
        }

        let invalid_sample = computed.verified && !computed.accepted;
        let provisional_accepted = computed.accepted && !computed.verified;
        let followup_action = self.update_address_state(
            &task.address,
            task.difficulty,
            computed.accepted,
            computed.verified,
            invalid_sample,
            provisional_accepted,
            computed.overload_mode,
        );

        ValidationResult {
            nonce: computed.nonce,
            accepted: computed.accepted,
            reject_reason: computed.reject_reason,
            hash: computed.hash,
            verified: computed.verified,
            is_block_candidate: computed.is_block_candidate,
            suspected_fraud: computed.suspected_fraud,
            followup_action,
        }
    }

    fn update_address_state(
        &self,
        address: &str,
        difficulty: u64,
        accepted: bool,
        sampled: bool,
        invalid_sample: bool,
        provisional_accepted: bool,
        overload_mode: OverloadMode,
    ) -> ValidationFollowupAction {
        self.update_address_state_at(
            address,
            None,
            SystemTime::now(),
            difficulty,
            accepted,
            sampled,
            invalid_sample,
            provisional_accepted,
            overload_mode,
        )
    }

    fn update_address_state_at(
        &self,
        address: &str,
        share_id: Option<i64>,
        created_at: SystemTime,
        difficulty: u64,
        accepted: bool,
        sampled: bool,
        invalid_sample: bool,
        provisional_accepted: bool,
        overload_mode: OverloadMode,
    ) -> ValidationFollowupAction {
        let now = SystemTime::now();
        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);
        let forced_review_duration = self.config.invalid_sample_force_verify_duration();

        st.total_shares = st.total_shares.saturating_add(1);
        self.prune_provisional_locked(st, now);
        prune_accepted_window(st, &self.config, now);
        refresh_dynamic_validation_hold(st, &self.config, now);
        st.last_seen_at = now;
        if st.resume_forced_at.is_some_and(|start_at| start_at <= now) {
            st.start_forced_review(
                now,
                forced_review_duration,
                ValidationHoldCause::InvalidSamples,
            );
        }
        if accepted {
            st.accepted_window.push_back(AcceptedWindowShare {
                share_id: share_id.unwrap_or_default(),
                created_at,
                difficulty: difficulty.max(1),
                verified: sampled,
            });
        }
        if provisional_accepted {
            st.provisional_at.push_back(TrackedProvisionalShare {
                share_id,
                created_at,
            });
        }
        if sampled {
            st.sampled_shares = st.sampled_shares.saturating_add(1);
            if invalid_sample {
                st.invalid_samples = st.invalid_samples.saturating_add(1);
            }
            if st.forced_started_at.is_some() {
                st.forced_sampled_shares = st.forced_sampled_shares.saturating_add(1);
                if invalid_sample {
                    st.forced_invalid_samples = st.forced_invalid_samples.saturating_add(1);
                }
            } else {
                st.risk_sampled_shares = st.risk_sampled_shares.saturating_add(1);
                if invalid_sample {
                    st.risk_invalid_samples = st.risk_invalid_samples.saturating_add(1);
                }
            }
        }

        let followup =
            self.evaluate_post_sample_state(st, now, overload_mode, sampled, invalid_sample);

        let persisted = persisted_validation_state(address, st);
        let provisional_to_persist = provisional_accepted.then_some(TrackedProvisionalShare {
            share_id,
            created_at,
        });
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        drop(state);
        self.persist_validation_state(&persisted, provisional_to_persist, now);
        followup
    }

    fn evaluate_post_sample_state(
        &self,
        st: &mut ValidationAddressState,
        now: SystemTime,
        overload_mode: OverloadMode,
        sampled: bool,
        invalid_sample: bool,
    ) -> ValidationFollowupAction {
        let forced_review_duration = self.config.invalid_sample_force_verify_duration();
        let min_samples = self.config.invalid_sample_min.max(1) as u64;
        let min_invalids = self.config.invalid_sample_count_threshold.max(1) as u64;
        let forced_clear_threshold = self.config.invalid_sample_threshold;
        let forced_quarantine_threshold = self.config.forced_validation_quarantine_threshold;

        let mut followup = ValidationFollowupAction::None;
        if st.forced_started_at.is_some() {
            let forced_ratio = ratio(st.forced_invalid_samples, st.forced_sampled_shares);
            let forced_elapsed = st.forced_until.is_some_and(|deadline| now >= deadline);
            let recovered = st.forced_sampled_shares >= min_samples
                && (st.forced_invalid_samples < min_invalids
                    || forced_ratio <= forced_clear_threshold);
            let should_quarantine = forced_elapsed
                && st.forced_sampled_shares >= min_samples
                && st.forced_invalid_samples >= min_invalids
                && forced_ratio > forced_quarantine_threshold;
            if recovered || forced_elapsed {
                st.clear_forced_review();
                st.clear_risk_window();
            }
            if should_quarantine {
                followup = ValidationFollowupAction::Quarantine;
            }
        } else {
            let risk_ratio = ratio(st.risk_invalid_samples, st.risk_sampled_shares);
            if st.risk_sampled_shares >= min_samples
                && st.risk_invalid_samples >= min_invalids
                && risk_ratio > self.config.invalid_sample_threshold
                && overload_mode == OverloadMode::Normal
            {
                st.start_forced_review(
                    now,
                    forced_review_duration,
                    ValidationHoldCause::InvalidSamples,
                );
                if sampled {
                    st.forced_sampled_shares = 1;
                    st.forced_invalid_samples = u64::from(invalid_sample);
                }
            }
        }

        if overload_mode == OverloadMode::Normal && st.forced_started_at.is_none() {
            if should_force_for_provisional_backlog(st, &self.config, now) {
                st.start_temporary_hold(
                    now,
                    self.config.provisional_share_delay_duration(),
                    ValidationHoldCause::ProvisionalBacklog,
                );
            } else if should_force_for_payout_coverage(st, &self.config, now) {
                st.start_temporary_hold(
                    now,
                    self.config.provisional_share_delay_duration(),
                    ValidationHoldCause::PayoutCoverage,
                );
            }
        }
        refresh_dynamic_validation_hold(st, &self.config, now);
        followup
    }

    fn prune_provisional_locked(&self, st: &mut ValidationAddressState, now: SystemTime) {
        if st.provisional_at.is_empty() {
            return;
        }

        let delay = self.config.provisional_share_delay_duration();
        if delay.is_zero() {
            st.provisional_at.clear();
            return;
        }

        let cutoff = now.checked_sub(delay).unwrap_or(now);
        while st
            .provisional_at
            .front()
            .is_some_and(|share| share.created_at <= cutoff)
        {
            st.provisional_at.pop_front();
        }
    }

    fn snapshot(&self) -> ValidationSnapshot {
        let now = SystemTime::now();
        let now_instant = Instant::now();
        let candidate_queue = self.candidate_queue.snapshot(now_instant);
        let regular_queue = self.regular_queue.snapshot(now_instant);
        let audit_queue = self.audit_queue.snapshot(now_instant);
        let overload_mode = self.evaluate_overload(now_instant);
        let mut snap = ValidationSnapshot {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            candidate_queue_depth: candidate_queue.depth,
            regular_queue_depth: regular_queue.depth,
            audit_queue_depth: audit_queue.depth,
            candidate_oldest_age_millis: candidate_queue.oldest_age_millis,
            regular_oldest_age_millis: regular_queue.oldest_age_millis,
            audit_oldest_age_millis: audit_queue.oldest_age_millis,
            candidate_wait: candidate_queue.wait,
            regular_wait: regular_queue.wait,
            audit_wait: audit_queue.wait,
            validation_duration: self.validation_duration.lock().snapshot(),
            audit_duration: self.audit_duration.lock().snapshot(),
            fraud_detections: self.fraud.load(Ordering::Relaxed),
            candidate_false_claims: self.candidate_false_claims.load(Ordering::Relaxed),
            hot_accepts: self.hot_accepts.load(Ordering::Relaxed),
            sync_full_verifies: self.sync_full_verifies.load(Ordering::Relaxed),
            audit_enqueued: self.audit_enqueued.load(Ordering::Relaxed),
            audit_verified: self.audit_verified.load(Ordering::Relaxed),
            audit_rejected: self.audit_rejected.load(Ordering::Relaxed),
            audit_deferred: self.audit_deferred.load(Ordering::Relaxed),
            overload_mode,
            effective_sample_rate: self.effective_sample_rate(overload_mode),
            ..ValidationSnapshot::default()
        };

        let mut state = self.state.lock();
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        snap.tracked_addresses = state.len();
        for st in state.values_mut() {
            self.prune_provisional_locked(st, now);
            prune_accepted_window(st, &self.config, now);
            refresh_dynamic_validation_hold(st, &self.config, now);
            snap.total_shares = snap.total_shares.saturating_add(st.total_shares);
            snap.sampled_shares = snap.sampled_shares.saturating_add(st.sampled_shares);
            snap.invalid_samples = snap.invalid_samples.saturating_add(st.invalid_samples);
            snap.pending_provisional = snap
                .pending_provisional
                .saturating_add(st.provisional_at.len() as u64);
            if st.forced_started_at.is_some()
                || st.forced_until.is_some_and(|deadline| deadline > now)
            {
                snap.forced_verify_addresses += 1;
            }
        }
        drop(state);

        snap
    }

    fn refresh_dynamic_state(&self, now: SystemTime) {
        let mut state = self.state.lock();
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        let mut changed = Vec::new();
        for (address, st) in state.iter_mut() {
            let before = (
                st.forced_started_at,
                st.forced_until,
                st.resume_forced_at,
                st.hold_cause,
            );
            self.prune_provisional_locked(st, now);
            prune_accepted_window(st, &self.config, now);
            refresh_dynamic_validation_hold(st, &self.config, now);
            let after = (
                st.forced_started_at,
                st.forced_until,
                st.resume_forced_at,
                st.hold_cause,
            );
            if before != after {
                changed.push(persisted_validation_state(address, st));
            }
        }
        drop(state);
        for persisted in changed {
            self.persist_validation_state(&persisted, None, now);
        }
    }

    fn enqueue_live_audit(&self, share: PendingAuditShare, source: ValidationAuditSource) {
        let queued_at = Instant::now();
        let tracker_id = self.regular_queue.push(queued_at);
        {
            let mut pending = self.audit_pending_shares.lock();
            if !pending.insert(share.share_id) {
                self.regular_queue.remove(tracker_id);
                return;
            }
        }
        let result = self
            .regular_tx
            .try_send(RegularWorkItem::Audit(QueuedAuditTask { share, source }));
        if result.is_ok() {
            self.audit_enqueued.fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.regular_queue.remove(tracker_id);
        if let Err(flume::TrySendError::Full(RegularWorkItem::Audit(task)))
        | Err(flume::TrySendError::Disconnected(RegularWorkItem::Audit(task))) = result
        {
            self.audit_pending_shares
                .lock()
                .remove(&task.share.share_id);
            self.audit_deferred.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                address = %task.share.miner,
                share_id = task.share.share_id,
                source = ?task.source,
                "live validation audit deferred because the regular validation queue is full"
            );
        }
    }

    fn enqueue_background_audit(&self, share: PendingAuditShare, source: ValidationAuditSource) {
        let queued_at = Instant::now();
        let tracker_id = self.audit_queue.push(queued_at);
        {
            let mut pending = self.audit_pending_shares.lock();
            if !pending.insert(share.share_id) {
                self.audit_queue.remove(tracker_id);
                return;
            }
        }
        let result = self.audit_tx.try_send(QueuedAuditTask { share, source });
        match result {
            Ok(()) => {
                self.audit_enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(flume::TrySendError::Full(task)) | Err(flume::TrySendError::Disconnected(task)) => {
                self.audit_queue.remove(tracker_id);
                self.audit_pending_shares
                    .lock()
                    .remove(&task.share.share_id);
                self.audit_deferred.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    address = %task.share.miner,
                    share_id = task.share.share_id,
                    source = ?task.source,
                    "background validation audit deferred because the audit queue is full"
                );
            }
        }
    }

    fn process_live_audit(&self, queued: QueuedAuditTask) {
        let started_at = Instant::now();
        self.regular_queue.pop_and_record_wait(started_at);
        self.process_audit_task(queued, started_at);
    }

    fn process_background_audit(&self, queued: QueuedAuditTask) {
        let started_at = Instant::now();
        self.audit_queue.pop_and_record_wait(started_at);
        self.process_audit_task(queued, started_at);
    }

    fn process_audit_task(&self, queued: QueuedAuditTask, started_at: Instant) {
        let task = ValidationTask {
            address: queued.share.miner.clone(),
            nonce: queued.share.nonce,
            difficulty: queued.share.difficulty.max(1),
            header_base: queued.share.header_base.clone(),
            share_target: crate::pow::difficulty_to_target(queued.share.difficulty.max(1)),
            network_target: queued.share.network_target,
            claimed_hash: queued.share.claimed_hash,
            candidate_claim: false,
            force_full_verify: true,
        };
        let computed = self.compute_task(
            &task,
            ValidationPlan {
                full_verify: true,
                overload_mode: self.evaluate_overload(Instant::now()),
            },
        );
        self.audit_duration.lock().record(started_at.elapsed());
        self.audit_pending_shares
            .lock()
            .remove(&queued.share.share_id);

        let update = ShareReplayUpdate {
            share_id: queued.share.share_id,
            status: if computed.accepted {
                SHARE_STATUS_VERIFIED.to_string()
            } else {
                SHARE_STATUS_REJECTED.to_string()
            },
            was_sampled: true,
            reject_reason: computed
                .reject_reason
                .map(canonical_audit_reject_reason)
                .map(str::to_string),
        };
        if let Err(err) = self.state_store.complete_validation_audit(&update) {
            self.audit_deferred.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                address = %queued.share.miner,
                share_id = queued.share.share_id,
                source = ?queued.source,
                error = %err,
                "failed to persist validation audit result"
            );
            return;
        }

        self.apply_audit_result(
            &queued.share.miner,
            queued.share.share_id,
            queued.share.created_at,
            queued.share.difficulty,
            computed.accepted,
            computed.reject_reason,
        );
        if computed.accepted {
            self.audit_verified.fetch_add(1, Ordering::Relaxed);
        } else {
            self.audit_rejected.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                address = %queued.share.miner,
                share_id = queued.share.share_id,
                source = ?queued.source,
                reason = computed.reject_reason.unwrap_or("invalid share"),
                "validation audit revoked a provisional share"
            );
        }
    }

    fn apply_audit_result(
        &self,
        address: &str,
        share_id: i64,
        created_at: SystemTime,
        difficulty: u64,
        accepted: bool,
        reject_reason: Option<&'static str>,
    ) {
        let now = SystemTime::now();
        let mut state = self.state.lock();
        let entry = get_or_insert_state(&mut state, address);
        self.prune_provisional_locked(entry, now);
        prune_accepted_window(entry, &self.config, now);
        refresh_dynamic_validation_hold(entry, &self.config, now);

        if let Some(pos) = entry
            .provisional_at
            .iter()
            .position(|share| share.share_id == Some(share_id))
        {
            entry.provisional_at.remove(pos);
        }
        if accepted {
            if let Some(existing) = entry
                .accepted_window
                .iter_mut()
                .find(|share| share.share_id == share_id)
            {
                existing.verified = true;
            } else {
                entry.accepted_window.push_back(AcceptedWindowShare {
                    share_id,
                    created_at,
                    difficulty: difficulty.max(1),
                    verified: true,
                });
            }
        } else if let Some(pos) = entry
            .accepted_window
            .iter()
            .position(|share| share.share_id == share_id)
        {
            entry.accepted_window.remove(pos);
        }

        let invalid_sample = !accepted;
        let _ = reject_reason;
        entry.sampled_shares = entry.sampled_shares.saturating_add(1);
        if invalid_sample {
            entry.invalid_samples = entry.invalid_samples.saturating_add(1);
        }
        if entry.forced_started_at.is_some() {
            entry.forced_sampled_shares = entry.forced_sampled_shares.saturating_add(1);
            if invalid_sample {
                entry.forced_invalid_samples = entry.forced_invalid_samples.saturating_add(1);
            }
        } else {
            entry.risk_sampled_shares = entry.risk_sampled_shares.saturating_add(1);
            if invalid_sample {
                entry.risk_invalid_samples = entry.risk_invalid_samples.saturating_add(1);
            }
        }

        let followup =
            self.evaluate_post_sample_state(entry, now, OverloadMode::Normal, true, invalid_sample);
        if followup == ValidationFollowupAction::Quarantine {
            tracing::warn!(
                address = %address,
                share_id,
                "async validation sample would have triggered forced-validation quarantine; future shares will remain in forced review"
            );
        }
        let persisted = persisted_validation_state(address, entry);
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        drop(state);
        self.persist_validation_state(&persisted, None, now);
    }

    fn schedule_background_audits(&self, now: SystemTime) {
        let instant_now = Instant::now();
        if !self.can_schedule_background_audits(instant_now) {
            return;
        }
        let audit_snapshot = self.audit_queue.snapshot(instant_now);
        let target_depth = self
            .config
            .audit_verifier_count()
            .saturating_mul(VALIDATION_AUDIT_TARGET_QUEUE_MULTIPLIER)
            .max(1);
        let mut remaining_budget = target_depth.saturating_sub(audit_snapshot.depth);
        if remaining_budget == 0 {
            return;
        }

        let provisional_cutoff = now
            .checked_sub(self.config.provisional_share_delay_duration())
            .unwrap_or(now);
        let max_addresses = self.config.audit_max_addresses_per_tick.max(1) as usize;
        let per_address_limit = self.config.audit_max_shares_per_address.max(1) as usize;
        let mut due = Vec::<(String, ValidationAuditSource)>::new();

        {
            let mut state = self.state.lock();
            for (address, st) in state.iter_mut() {
                self.prune_provisional_locked(st, now);
                prune_accepted_window(st, &self.config, now);
                refresh_dynamic_validation_hold(st, &self.config, now);
                if !should_backfill_pending_window(st, &self.config, now) {
                    continue;
                }
                let due_now = st.last_replay_backfill_at.is_none_or(|last| {
                    now.duration_since(last).unwrap_or_default() >= VALIDATION_AUDIT_SCHED_INTERVAL
                });
                if !due_now {
                    continue;
                }
                let source = match st.hold_cause {
                    Some(ValidationHoldCause::ProvisionalBacklog) => {
                        ValidationAuditSource::ProvisionalBacklog
                    }
                    _ => ValidationAuditSource::PayoutCoverage,
                };
                st.last_replay_backfill_at = Some(now);
                due.push((address.clone(), source));
            }
        }

        due.sort_by(|(left, _), (right, _)| left.cmp(right));
        let mut scheduled_addresses = 0usize;
        let mut scheduled_shares = 0usize;
        let mut share_batches = Vec::<(ValidationAuditSource, VecDeque<PendingAuditShare>)>::new();
        for (address, source) in due.into_iter().take(max_addresses) {
            let load_limit = per_address_limit.min(remaining_budget.max(1));
            let shares = match source {
                ValidationAuditSource::ProvisionalBacklog => self
                    .state_store
                    .load_recent_provisional_audit_shares(&address, provisional_cutoff, load_limit),
                _ => self.state_store.load_pending_payout_audit_shares(
                    &address,
                    &self.config,
                    now,
                    load_limit,
                ),
            };
            match shares {
                Ok(shares) => {
                    if shares.is_empty() {
                        continue;
                    }
                    scheduled_addresses += 1;
                    share_batches.push((source, shares.into_iter().collect()));
                }
                Err(err) => {
                    tracing::warn!(
                        address = %address,
                        source = ?source,
                        error = %err,
                        "failed loading background validation audit shares"
                    );
                }
            }
        }

        while remaining_budget > 0 {
            let mut progress = false;
            for (source, shares) in share_batches.iter_mut() {
                let Some(share) = shares.pop_front() else {
                    continue;
                };
                self.enqueue_background_audit(share, *source);
                scheduled_shares += 1;
                remaining_budget -= 1;
                progress = true;
                if remaining_budget == 0 {
                    break;
                }
            }
            if !progress {
                break;
            }
        }

        if scheduled_shares > 0 {
            tracing::debug!(
                scheduled_addresses,
                scheduled_shares,
                audit_budget = target_depth,
                regular_queue_depth = self.regular_queue.snapshot(Instant::now()).depth,
                audit_queue_depth = self.audit_queue.snapshot(Instant::now()).depth,
                "scheduled background validation audits"
            );
        }
    }

    fn can_schedule_background_audits(&self, now: Instant) -> bool {
        if self.evaluate_overload(now) != OverloadMode::Normal {
            return false;
        }
        let candidate = self.candidate_queue.snapshot(now);
        let regular = self.regular_queue.snapshot(now);
        if candidate.depth > 0 || regular.depth > 0 || self.in_flight.load(Ordering::Relaxed) > 0 {
            return false;
        }
        self.submit_regular_queue
            .lock()
            .as_ref()
            .map(|queue| queue.snapshot(now).depth == 0)
            .unwrap_or(true)
    }

    fn effective_sample_rate(&self, overload_mode: OverloadMode) -> f64 {
        let base = self.config.sample_rate.clamp(0.0, 1.0);
        match overload_mode {
            OverloadMode::Normal => base,
            OverloadMode::Shed => base.min(self.config.overload_sample_rate_floor),
            OverloadMode::Emergency => 0.0,
        }
    }

    fn evaluate_overload(&self, now: Instant) -> OverloadMode {
        let regular_validation = self.regular_queue.snapshot(now);
        let regular_submit = self
            .submit_regular_queue
            .lock()
            .as_ref()
            .map(|queue| queue.snapshot(now));
        let validation_pct = regular_validation.depth as f64
            / self.config.regular_validation_queue_size().max(1) as f64;
        let submit_pct = regular_submit
            .as_ref()
            .map(|snapshot| {
                snapshot.depth as f64 / self.config.regular_submit_queue_size().max(1) as f64
            })
            .unwrap_or_default();
        let queue_pct = validation_pct.max(submit_pct);
        let oldest_age = Duration::from_millis(
            regular_validation
                .oldest_age_millis
                .unwrap_or_default()
                .max(
                    regular_submit
                        .as_ref()
                        .and_then(|snapshot| snapshot.oldest_age_millis)
                        .unwrap_or_default(),
                ),
        );
        let emergency = queue_pct >= self.config.overload_emergency_queue_pct
            || oldest_age >= self.config.overload_emergency_oldest_age_duration();
        let shed = queue_pct >= self.config.overload_shed_queue_pct
            || oldest_age >= self.config.overload_shed_oldest_age_duration();
        let below_clear = queue_pct <= self.config.overload_clear_queue_pct
            && oldest_age <= self.config.overload_clear_oldest_age_duration();
        let clear_hold = self.config.overload_clear_hold_duration();

        let mut state = self.overload.lock();
        let previous_mode = state.mode;
        match state.mode {
            OverloadMode::Normal => {
                if emergency {
                    state.mode = OverloadMode::Emergency;
                    state.below_clear_started_at = None;
                } else if shed {
                    state.mode = OverloadMode::Shed;
                    state.below_clear_started_at = None;
                }
            }
            OverloadMode::Shed | OverloadMode::Emergency => {
                if below_clear {
                    if let Some(started_at) = state.below_clear_started_at {
                        if now.saturating_duration_since(started_at) >= clear_hold {
                            state.mode = OverloadMode::Normal;
                            state.below_clear_started_at = None;
                        }
                    } else {
                        state.below_clear_started_at = Some(now);
                    }
                } else {
                    if emergency {
                        state.mode = OverloadMode::Emergency;
                    } else if shed {
                        state.mode = OverloadMode::Shed;
                    }
                    state.below_clear_started_at = None;
                }
            }
        }

        if state.mode == OverloadMode::Shed && emergency {
            state.mode = OverloadMode::Emergency;
        }
        if state.mode != previous_mode {
            tracing::info!(
                previous = ?previous_mode,
                current = ?state.mode,
                validation_queue_pct = validation_pct,
                submit_queue_pct = submit_pct,
                oldest_age_millis = oldest_age.as_millis() as u64,
                "validation overload mode changed"
            );
        }
        state.mode
    }

    fn clear_address_state(&self, address: &str) {
        self.state.lock().remove(address);
    }

    fn address_has_live_hold(&self, address: &str) -> bool {
        let now = SystemTime::now();
        self.state.lock().get(address).is_some_and(|st| {
            let mut effective = st.clone();
            if self.config.provisional_share_delay_duration().is_zero() {
                effective.provisional_at.clear();
            } else {
                self.prune_provisional_locked(&mut effective, now);
            }
            prune_accepted_window(&mut effective, &self.config, now);
            refresh_dynamic_validation_hold(&mut effective, &self.config, now);
            effective.forced_started_at.is_some()
                || effective
                    .forced_until
                    .is_some_and(|deadline| deadline > now)
                || effective
                    .resume_forced_at
                    .is_some_and(|start_at| start_at > now)
        })
    }

    fn sync_external_clears(&self, force: bool) {
        if !force {
            let mut last_sync = self.last_clear_sync_at.lock();
            if last_sync.is_some_and(|last| last.elapsed() < VALIDATION_CLEAR_SYNC_INTERVAL) {
                return;
            }
            *last_sync = Some(Instant::now());
        }

        let cursor = *self.last_clear_event_id.lock();
        let events = match self.state_store.load_validation_clear_events_since(cursor) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, "failed loading validation clear events");
                return;
            }
        };
        if events.is_empty() {
            return;
        }

        let mut state = self.state.lock();
        let mut last_id = cursor;
        for event in events {
            last_id = last_id.max(event.id);
            state.remove(&event.address);
            tracing::info!(
                address = %event.address,
                cleared_at = ?event.cleared_at,
                "applied external validation clear event"
            );
        }
        drop(state);
        *self.last_clear_event_id.lock() = last_id;
        *self.last_clear_sync_at.lock() = Some(Instant::now());
    }

    fn prune_stale_state_locked(
        &self,
        state: &mut HashMap<String, ValidationAddressState>,
        now: SystemTime,
    ) {
        let cutoff = now.checked_sub(VALIDATION_STATE_RETENTION).unwrap_or(now);
        state.retain(|_, st| {
            self.prune_provisional_locked(st, now);
            prune_accepted_window(st, &self.config, now);
            refresh_dynamic_validation_hold(st, &self.config, now);
            st.last_seen_at >= cutoff
                || st.forced_started_at.is_some()
                || st.forced_until.is_some_and(|deadline| deadline > now)
                || st.resume_forced_at.is_some_and(|start_at| start_at > now)
                || !st.provisional_at.is_empty()
                || !st.accepted_window.is_empty()
        });
        if state.len() <= VALIDATION_STATE_MAX_TRACKED {
            return;
        }

        let mut removable = state
            .iter()
            .filter_map(|(address, st)| {
                if st.forced_started_at.is_some()
                    || st.forced_until.is_some_and(|deadline| deadline > now)
                    || st.resume_forced_at.is_some_and(|start_at| start_at > now)
                    || !st.provisional_at.is_empty()
                    || !st.accepted_window.is_empty()
                {
                    return None;
                }
                Some((address.clone(), st.last_seen_at))
            })
            .collect::<Vec<(String, SystemTime)>>();
        removable.sort_by_key(|(_, seen_at)| *seen_at);

        let excess = state.len().saturating_sub(VALIDATION_STATE_MAX_TRACKED);
        for (address, _) in removable.into_iter().take(excess) {
            state.remove(&address);
        }
    }

    fn persist_validation_state(
        &self,
        state: &PersistedValidationAddressState,
        provisional: Option<TrackedProvisionalShare>,
        now: SystemTime,
    ) {
        if let Some(provisional) = provisional {
            if let Err(err) = self.state_store.add_validation_provisional(
                &state.address,
                provisional.share_id,
                provisional.created_at,
            ) {
                tracing::warn!(
                    address = %state.address,
                    error = %err,
                    "failed to persist provisional validation share"
                );
            }
        }

        if let Err(err) = self.state_store.upsert_validation_state(state) {
            tracing::warn!(
                address = %state.address,
                error = %err,
                "failed to persist validation state"
            );
        }

        self.maybe_clean_persisted_state(now);
    }

    fn maybe_clean_persisted_state(&self, now: SystemTime) {
        let mut guard = self.last_cleanup_at.lock();
        if guard.is_some_and(|last| last.elapsed() < VALIDATION_PERSIST_CLEANUP_INTERVAL) {
            return;
        }
        *guard = Some(Instant::now());
        drop(guard);

        let state_cutoff = now
            .checked_sub(VALIDATION_STATE_RETENTION)
            .unwrap_or(UNIX_EPOCH);
        let provisional_cutoff = now
            .checked_sub(self.config.provisional_share_delay_duration())
            .unwrap_or(UNIX_EPOCH);
        if let Err(err) =
            self.state_store
                .clean_validation_state(state_cutoff, provisional_cutoff, now)
        {
            tracing::warn!(error = %err, "failed cleaning persisted validation state");
        }
    }

    fn schedule_forced_review_after(&self, address: &str, start_at: SystemTime) {
        let now = SystemTime::now();
        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);
        st.last_seen_at = now;
        st.clear_forced_review();
        st.clear_risk_window();
        st.resume_forced_at = Some(start_at);
        st.hold_cause = Some(ValidationHoldCause::InvalidSamples);
        let persisted = persisted_validation_state(address, st);
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        drop(state);
        self.persist_validation_state(&persisted, None, now);
    }

    fn record_hot_accept(
        &self,
        address: &str,
        share_id: i64,
        difficulty: u64,
        created_at: SystemTime,
        overload_mode: OverloadMode,
    ) {
        self.hot_accepts.fetch_add(1, Ordering::Relaxed);
        let _ = self.update_address_state_at(
            address,
            Some(share_id),
            created_at,
            difficulty,
            true,
            false,
            false,
            true,
            overload_mode,
        );
    }
}

fn get_or_insert_state<'a>(
    state: &'a mut HashMap<String, ValidationAddressState>,
    address: &str,
) -> &'a mut ValidationAddressState {
    if !state.contains_key(address) {
        state.insert(address.to_string(), ValidationAddressState::default());
    }
    state
        .get_mut(address)
        .expect("address state must be present after insert")
}

fn ratio(invalid: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        invalid as f64 / total as f64
    }
}

fn canonical_audit_reject_reason(reason: &'static str) -> &'static str {
    match reason {
        "invalid share proof" => "invalid share proof",
        "low difficulty share" => "low difficulty share",
        "claimed hash required" => "claimed hash required",
        "hash computation failed" => "hash computation failed",
        _ => "invalid share",
    }
}

fn infer_validation_hold_cause(
    forced_started_at: Option<SystemTime>,
    forced_until: Option<SystemTime>,
) -> Option<ValidationHoldCause> {
    if forced_started_at.is_some() {
        Some(ValidationHoldCause::InvalidSamples)
    } else if forced_until.is_some() {
        Some(ValidationHoldCause::ProvisionalBacklog)
    } else {
        None
    }
}

fn payout_coverage_target_ratio(config: &Config) -> Option<f64> {
    let cap_target = if config.payout_provisional_cap_multiplier > 0.0 {
        Some(1.0 / (1.0 + config.payout_provisional_cap_multiplier.max(0.0)))
    } else {
        None
    };
    let min_ratio = config.payout_min_verified_ratio.clamp(0.0, 1.0);
    match (cap_target, min_ratio > 0.0) {
        (Some(target), true) => Some(target.max(min_ratio)),
        (Some(target), false) => Some(target),
        (None, true) => Some(min_ratio),
        (None, false) => None,
    }
}

fn payout_coverage_window_start(config: &Config, now: SystemTime) -> Option<SystemTime> {
    let duration = if config.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
        config.pplns_window_duration_duration()
    } else {
        Duration::from_secs(60 * 60)
    };
    if duration.is_zero() {
        None
    } else {
        Some(now.checked_sub(duration).unwrap_or(UNIX_EPOCH))
    }
}

fn prune_accepted_window(state: &mut ValidationAddressState, config: &Config, now: SystemTime) {
    let Some(cutoff) = payout_coverage_window_start(config, now) else {
        state.accepted_window.clear();
        return;
    };
    while state
        .accepted_window
        .front()
        .is_some_and(|share| share.created_at < cutoff)
    {
        state.accepted_window.pop_front();
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PayoutCoverageStats {
    verified_shares: u64,
    verified_difficulty: u64,
    eligible_provisional_shares: u64,
    eligible_provisional_difficulty: u64,
}

impl PayoutCoverageStats {
    fn eligible_shares(self) -> u64 {
        self.verified_shares
            .saturating_add(self.eligible_provisional_shares)
    }

    fn eligible_difficulty(self) -> u64 {
        self.verified_difficulty
            .saturating_add(self.eligible_provisional_difficulty)
    }

    fn verified_ratio(self) -> f64 {
        ratio(self.verified_difficulty, self.eligible_difficulty())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ProvisionalBacklogStats {
    recent_shares: u64,
    recent_verified_difficulty: u64,
    recent_provisional_difficulty: u64,
}

fn provisional_backlog_stats(
    state: &ValidationAddressState,
    config: &Config,
    now: SystemTime,
) -> Option<ProvisionalBacklogStats> {
    let delay = config.provisional_share_delay_duration();
    if delay.is_zero() {
        return None;
    }

    let cutoff = now.checked_sub(delay).unwrap_or(now);
    let mut stats = ProvisionalBacklogStats::default();
    for share in &state.accepted_window {
        if share.created_at <= cutoff {
            continue;
        }
        stats.recent_shares = stats.recent_shares.saturating_add(1);
        if share.verified {
            stats.recent_verified_difficulty = stats
                .recent_verified_difficulty
                .saturating_add(share.difficulty);
        } else {
            stats.recent_provisional_difficulty = stats
                .recent_provisional_difficulty
                .saturating_add(share.difficulty);
        }
    }
    Some(stats)
}

fn should_force_for_provisional_backlog(
    state: &ValidationAddressState,
    config: &Config,
    now: SystemTime,
) -> bool {
    let Some(stats) = provisional_backlog_stats(state, config, now) else {
        return false;
    };
    let min_recent_shares = config.warmup_shares.max(1) as u64;
    if stats.recent_shares < min_recent_shares {
        return false;
    }
    if stats.recent_provisional_difficulty == 0 {
        return false;
    }
    if stats.recent_verified_difficulty == 0 {
        return true;
    }

    let budget_multiplier = config.max_provisional_recent_verified_multiplier();
    if budget_multiplier <= 0.0 {
        return true;
    }

    stats.recent_provisional_difficulty as f64
        > (stats.recent_verified_difficulty as f64 * budget_multiplier) + f64::EPSILON
}

fn payout_coverage_stats(
    state: &ValidationAddressState,
    config: &Config,
    now: SystemTime,
) -> Option<PayoutCoverageStats> {
    let _ = payout_coverage_window_start(config, now)?;
    let provisional_delay = config.provisional_share_delay_duration();
    let provisional_cutoff = now.checked_sub(provisional_delay).unwrap_or(now);
    let mut stats = PayoutCoverageStats::default();
    for share in &state.accepted_window {
        if share.verified {
            stats.verified_shares = stats.verified_shares.saturating_add(1);
            stats.verified_difficulty = stats.verified_difficulty.saturating_add(share.difficulty);
        } else if provisional_delay.is_zero() || share.created_at <= provisional_cutoff {
            stats.eligible_provisional_shares = stats.eligible_provisional_shares.saturating_add(1);
            stats.eligible_provisional_difficulty = stats
                .eligible_provisional_difficulty
                .saturating_add(share.difficulty);
        }
    }
    Some(stats)
}

fn should_force_for_payout_coverage(
    state: &ValidationAddressState,
    config: &Config,
    now: SystemTime,
) -> bool {
    let Some(target_ratio) = payout_coverage_target_ratio(config) else {
        return false;
    };
    let Some(stats) = payout_coverage_stats(state, config, now) else {
        return false;
    };
    let min_eligible_shares = config.warmup_shares.max(config.payout_min_verified_shares) as u64;
    if stats.eligible_shares() < min_eligible_shares.max(1) {
        return false;
    }
    stats.eligible_provisional_shares > 0 && stats.verified_ratio() + f64::EPSILON < target_ratio
}

fn should_backfill_pending_window(
    state: &ValidationAddressState,
    config: &Config,
    now: SystemTime,
) -> bool {
    let active_temporary_hold = matches!(
        state.hold_cause,
        Some(ValidationHoldCause::ProvisionalBacklog | ValidationHoldCause::PayoutCoverage)
    ) && state.forced_until.is_some_and(|deadline| deadline > now);

    active_temporary_hold || should_force_for_payout_coverage(state, config, now)
}

fn refresh_dynamic_validation_hold(
    state: &mut ValidationAddressState,
    config: &Config,
    now: SystemTime,
) {
    match state.hold_cause {
        Some(ValidationHoldCause::ProvisionalBacklog) => {
            let still_backlogged = should_force_for_provisional_backlog(state, config, now);
            let active = state.forced_until.is_some_and(|deadline| deadline > now);
            if !still_backlogged || !active {
                state.clear_forced_review();
            }
        }
        Some(ValidationHoldCause::PayoutCoverage) => {
            let active = state.forced_until.is_some_and(|deadline| deadline > now);
            if !active || !should_force_for_payout_coverage(state, config, now) {
                state.clear_forced_review();
            }
        }
        _ => {}
    }
}

fn persisted_validation_state(
    address: &str,
    state: &ValidationAddressState,
) -> PersistedValidationAddressState {
    PersistedValidationAddressState {
        address: address.to_string(),
        total_shares: state.total_shares,
        sampled_shares: state.sampled_shares,
        invalid_samples: state.invalid_samples,
        risk_sampled_shares: state.risk_sampled_shares,
        risk_invalid_samples: state.risk_invalid_samples,
        forced_started_at: state.forced_started_at,
        forced_until: state.forced_until,
        forced_sampled_shares: state.forced_sampled_shares,
        forced_invalid_samples: state.forced_invalid_samples,
        resume_forced_at: state.resume_forced_at,
        hold_cause: state.hold_cause,
        last_seen_at: state.last_seen_at,
    }
}

fn load_initial_validation_state(
    config: &Config,
    state_store: &dyn ValidationStateStore,
) -> HashMap<String, ValidationAddressState> {
    let now = SystemTime::now();
    let state_cutoff = now
        .checked_sub(VALIDATION_STATE_RETENTION)
        .unwrap_or(UNIX_EPOCH);
    let provisional_cutoff = now
        .checked_sub(config.provisional_share_delay_duration())
        .unwrap_or(UNIX_EPOCH);
    let accepted_window_cutoff = payout_coverage_window_start(config, now).unwrap_or(now);

    let loaded = match state_store.load_validation_state(
        state_cutoff,
        provisional_cutoff,
        accepted_window_cutoff,
        now,
    ) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(error = %err, "failed loading persisted validation state");
            return HashMap::new();
        }
    };

    let mut state = HashMap::<String, ValidationAddressState>::new();
    for entry in loaded.states {
        state.insert(
            entry.address.clone(),
            ValidationAddressState {
                total_shares: entry.total_shares,
                sampled_shares: entry.sampled_shares,
                invalid_samples: entry.invalid_samples,
                risk_sampled_shares: entry.risk_sampled_shares,
                risk_invalid_samples: entry.risk_invalid_samples,
                forced_started_at: entry.forced_started_at,
                forced_until: entry.forced_until.filter(|deadline| *deadline > now),
                forced_sampled_shares: entry.forced_sampled_shares,
                forced_invalid_samples: entry.forced_invalid_samples,
                resume_forced_at: entry.resume_forced_at.filter(|start_at| *start_at > now),
                hold_cause: entry.hold_cause.or_else(|| {
                    infer_validation_hold_cause(entry.forced_started_at, entry.forced_until)
                }),
                provisional_at: VecDeque::new(),
                accepted_window: VecDeque::new(),
                last_replay_backfill_at: None,
                last_seen_at: entry.last_seen_at,
            },
        );
    }

    for provisional in loaded.provisionals {
        let entry = state
            .entry(provisional.address.clone())
            .or_insert_with(ValidationAddressState::default);
        entry.provisional_at.push_back(TrackedProvisionalShare {
            share_id: provisional.share_id,
            created_at: provisional.created_at,
        });
        if provisional.created_at > entry.last_seen_at {
            entry.last_seen_at = provisional.created_at;
        }
    }

    for accepted in loaded.accepted_window {
        let entry = state
            .entry(accepted.address.clone())
            .or_insert_with(ValidationAddressState::default);
        entry.accepted_window.push_back(AcceptedWindowShare {
            share_id: accepted.share_id,
            created_at: accepted.created_at,
            difficulty: accepted.difficulty,
            verified: accepted.verified,
        });
        if accepted.created_at > entry.last_seen_at {
            entry.last_seen_at = accepted.created_at;
        }
    }

    let delay = config.provisional_share_delay_duration();
    for entry in state.values_mut() {
        if delay.is_zero() {
            entry.provisional_at.clear();
        } else {
            let cutoff = now.checked_sub(delay).unwrap_or(now);
            while entry
                .provisional_at
                .front()
                .is_some_and(|share| share.created_at <= cutoff)
            {
                entry.provisional_at.pop_front();
            }
        }
        prune_accepted_window(entry, config, now);
        if matches!(
            entry.hold_cause,
            Some(ValidationHoldCause::ProvisionalBacklog | ValidationHoldCause::PayoutCoverage)
        ) {
            refresh_dynamic_validation_hold(entry, config, now);
        }
    }

    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{DbBlock, ShareReplayData};
    use crate::engine::ShareRecord;
    use crate::pow::{difficulty_to_target, DeterministicTestHasher};
    use crate::store::PoolStore;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn test_store() -> Option<Arc<PoolStore>> {
        PoolStore::test_store()
    }

    macro_rules! require_test_store {
        () => {
            match test_store() {
                Some(store) => store,
                None => {
                    eprintln!(
                        "skipping postgres test: set {} to run postgres integration checks",
                        PoolStore::TEST_POSTGRES_URL_ENV
                    );
                    return;
                }
            }
        };
    }

    fn test_cfg() -> Config {
        Config {
            validation_mode: "probabilistic".to_string(),
            sample_rate: 0.0,
            warmup_shares: 0,
            min_sample_every: 0,
            invalid_sample_min: 1,
            invalid_sample_count_threshold: 1,
            invalid_sample_threshold: 0.01,
            max_verifiers: 1,
            max_validation_queue: 16,
            regular_validation_queue: 16,
            max_provisional_shares: 2,
            provisional_share_delay: "10m".to_string(),
            forced_verify_duration: "1h".to_string(),
            ..Config::default()
        }
    }

    #[derive(Default)]
    struct InMemoryAuditStore {
        loaded: LoadedValidationState,
        pending_payout_audits: Mutex<Vec<PendingAuditShare>>,
        completed: Mutex<HashSet<i64>>,
    }

    impl InMemoryAuditStore {
        fn new(
            loaded: LoadedValidationState,
            pending_payout_audits: Vec<PendingAuditShare>,
        ) -> Self {
            Self {
                loaded,
                pending_payout_audits: Mutex::new(pending_payout_audits),
                completed: Mutex::new(HashSet::new()),
            }
        }
    }

    impl ValidationStateStore for InMemoryAuditStore {
        fn load_validation_state(
            &self,
            _state_cutoff: SystemTime,
            _provisional_cutoff: SystemTime,
            _accepted_window_cutoff: SystemTime,
            _now: SystemTime,
        ) -> Result<LoadedValidationState> {
            Ok(self.loaded.clone())
        }

        fn upsert_validation_state(&self, _state: &PersistedValidationAddressState) -> Result<()> {
            Ok(())
        }

        fn add_validation_provisional(
            &self,
            _address: &str,
            _share_id: Option<i64>,
            _created_at: SystemTime,
        ) -> Result<()> {
            Ok(())
        }

        fn clean_validation_state(
            &self,
            _state_cutoff: SystemTime,
            _provisional_cutoff: SystemTime,
            _now: SystemTime,
        ) -> Result<()> {
            Ok(())
        }

        fn latest_validation_clear_event_id(&self) -> Result<i64> {
            Ok(0)
        }

        fn load_validation_clear_events_since(
            &self,
            _cursor: i64,
        ) -> Result<Vec<ValidationClearEvent>> {
            Ok(Vec::new())
        }

        fn load_validation_address_activity(
            &self,
            _address: &str,
            _provisional_cutoff: SystemTime,
            _accepted_window_cutoff: SystemTime,
        ) -> Result<LoadedValidationAddressActivity> {
            Ok(LoadedValidationAddressActivity::default())
        }

        fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<()> {
            self.completed.lock().insert(update.share_id);
            Ok(())
        }

        fn load_recent_provisional_audit_shares(
            &self,
            _address: &str,
            _provisional_cutoff: SystemTime,
            _limit: usize,
        ) -> Result<Vec<PendingAuditShare>> {
            Ok(Vec::new())
        }

        fn load_pending_payout_audit_shares(
            &self,
            address: &str,
            _config: &Config,
            _now: SystemTime,
            limit: usize,
        ) -> Result<Vec<PendingAuditShare>> {
            let completed = self.completed.lock().clone();
            let pending = self.pending_payout_audits.lock();
            Ok(pending
                .iter()
                .filter(|share| share.miner == address && !completed.contains(&share.share_id))
                .take(limit)
                .cloned()
                .collect())
        }
    }

    fn wait_for(predicate: impl Fn() -> bool, timeout: Duration) {
        let started = Instant::now();
        while !predicate() && started.elapsed() < timeout {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn base_task() -> ValidationTask {
        ValidationTask {
            address: "addr1".to_string(),
            nonce: 1,
            difficulty: 1,
            header_base: vec![1, 2, 3],
            share_target: [0xFF; 32],
            network_target: [0x0F; 32],
            claimed_hash: Some([0x01; 32]),
            force_full_verify: false,
            candidate_claim: false,
        }
    }

    fn matching_task(nonce: u64) -> ValidationTask {
        let header_base = vec![1, 2, 3];
        let claimed_hash = DeterministicTestHasher
            .hash(&header_base, nonce)
            .expect("deterministic hash");
        ValidationTask {
            address: "addr1".to_string(),
            nonce,
            difficulty: 1,
            header_base,
            share_target: [0xFF; 32],
            network_target: [0x0F; 32],
            claimed_hash: Some(claimed_hash),
            force_full_verify: false,
            candidate_claim: false,
        }
    }

    #[test]
    fn invalid_sample_escalates_force_verify() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.force_full_verify = true;
        task.claimed_hash = Some([0xAA; 32]);
        let result = engine.process_inline(task.clone());
        assert!(!result.accepted);
        assert!(result.suspected_fraud);
        assert_eq!(result.followup_action, ValidationFollowupAction::None);

        task.force_full_verify = false;
        task.claimed_hash = Some([0x01; 32]);
        let result2 = engine.process_inline(task);
        assert!(
            result2.verified,
            "subsequent shares should be force-verified"
        );
    }

    #[test]
    fn invalid_sample_requires_count_threshold_before_force_verify() {
        let mut cfg = test_cfg();
        cfg.invalid_sample_min = 1;
        cfg.invalid_sample_count_threshold = 3;
        cfg.invalid_sample_threshold = 0.05;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        for nonce in [11u64, 12] {
            let mut bad = matching_task(nonce);
            bad.force_full_verify = true;
            bad.share_target = [0x00; 32];
            let result = engine.process_inline(bad);
            assert_eq!(result.reject_reason, Some("low difficulty share"));
            assert!(!result.suspected_fraud);
        }

        let before_threshold = engine.process_inline(matching_task(13));
        assert!(
            !before_threshold.verified,
            "single config issues should not force verified-only mode immediately"
        );

        let mut third_bad = matching_task(14);
        third_bad.force_full_verify = true;
        third_bad.share_target = [0x00; 32];
        let third_result = engine.process_inline(third_bad);
        assert_eq!(third_result.reject_reason, Some("low difficulty share"));
        assert_eq!(third_result.followup_action, ValidationFollowupAction::None);

        let after_threshold = engine.process_inline(matching_task(15));
        assert!(
            after_threshold.verified,
            "repeated invalid samples should eventually switch the address into verified-only mode"
        );
    }

    #[test]
    fn forced_review_only_quarantines_after_review_window_expires() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut first_bad = base_task();
        first_bad.force_full_verify = true;
        first_bad.claimed_hash = Some([0xAA; 32]);
        let first_result = engine.process_inline(first_bad);
        assert_eq!(first_result.reject_reason, Some("invalid share proof"));
        assert_eq!(first_result.followup_action, ValidationFollowupAction::None);

        {
            let mut state = engine.inner.state.lock();
            let entry = state
                .get_mut("addr1")
                .expect("validation state should exist");
            entry.forced_until = Some(SystemTime::now() - Duration::from_secs(1));
        }

        let mut second_bad = base_task();
        second_bad.force_full_verify = false;
        second_bad.claimed_hash = Some([0xBB; 32]);
        let second_result = engine.process_inline(second_bad);
        assert_eq!(
            second_result.followup_action,
            ValidationFollowupAction::Quarantine
        );
    }

    #[test]
    fn provisional_backlog_budget_forces_full_verify() {
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_recent_verified_multiplier = 1.0;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut seed = matching_task(90);
        seed.force_full_verify = true;
        let seeded = engine.process_inline(seed);
        assert!(seeded.accepted && seeded.verified);

        let r1 = engine.process_inline(matching_task(1));
        assert!(r1.accepted);
        assert!(!r1.verified);

        let r2 = engine.process_inline(matching_task(2));
        assert!(r2.accepted);
        assert!(!r2.verified);

        let r3 = engine.process_inline(matching_task(3));
        assert!(
            r3.verified,
            "recent provisional difficulty above the verified budget should force full verify"
        );
    }

    #[test]
    fn provisional_backlog_hold_clears_once_backlog_drains() {
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 2;
        cfg.max_provisional_recent_verified_multiplier = 1.0;
        cfg.provisional_share_delay = "50ms".to_string();
        cfg.payout_min_verified_ratio = 0.0;
        cfg.payout_provisional_cap_multiplier = 0.0;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut seed = matching_task(90);
        seed.force_full_verify = true;
        let seeded = engine.process_inline(seed);
        assert!(seeded.accepted && seeded.verified);

        let first = engine.process_inline(matching_task(1));
        let second = engine.process_inline(matching_task(2));
        assert!(first.accepted && !first.verified);
        assert!(second.accepted && !second.verified);

        let pressured = engine.snapshot();
        assert_eq!(pressured.pending_provisional, 2);
        assert_eq!(pressured.forced_verify_addresses, 1);

        std::thread::sleep(Duration::from_millis(70));

        let recovered = engine.snapshot();
        assert_eq!(recovered.pending_provisional, 0);
        assert_eq!(recovered.forced_verify_addresses, 0);

        let after_drain = engine.process_inline(matching_task(3));
        assert!(
            !after_drain.verified,
            "backlog hold should clear once provisional pressure is gone"
        );
    }

    #[test]
    fn payout_coverage_hold_boosts_verification_until_ratio_recovers() {
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 100;
        cfg.provisional_share_delay = "50ms".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_provisional_cap_multiplier = 1.0;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let first = engine.process_inline(matching_task(1));
        assert!(first.accepted && !first.verified);

        std::thread::sleep(Duration::from_millis(70));

        let second = engine.process_inline(matching_task(2));
        assert!(
            second.verified,
            "low recent verified ratio should temporarily force verification"
        );

        let third = engine.process_inline(matching_task(3));
        assert!(
            !third.verified,
            "coverage hold should clear once the recent verified ratio recovers"
        );
    }

    #[test]
    fn payout_coverage_hold_replays_pending_window_shares_and_clears() {
        let store = require_test_store!();
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 100;
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "6h".to_string();
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_provisional_cap_multiplier = 1.0;

        let address = format!("coverage-replay-{}", rand::random::<u64>());
        let base = SystemTime::now() - Duration::from_secs(120);
        for (job_id, nonce, offset) in [("cov-prov-a", 7u64, 0u64), ("cov-prov-b", 8u64, 1u64)] {
            store
                .add_share_with_replay(
                    ShareRecord {
                        job_id: job_id.to_string(),
                        miner: address.clone(),
                        worker: "wa".to_string(),
                        difficulty: 1,
                        nonce,
                        status: SHARE_STATUS_PROVISIONAL,
                        was_sampled: false,
                        block_hash: None,
                        claimed_hash: None,
                        reject_reason: None,
                        created_at: base + Duration::from_secs(offset),
                    },
                    Some(ShareReplayData {
                        job_id: job_id.to_string(),
                        header_base: vec![1, 2, 3, offset as u8],
                        network_target: [0xff; 32],
                        created_at: base + Duration::from_secs(offset),
                    }),
                )
                .expect("add replayable provisional share");
        }
        store
            .add_share(ShareRecord {
                job_id: "cov-verified".to_string(),
                miner: address.clone(),
                worker: "wa".to_string(),
                difficulty: 1,
                nonce: 9,
                status: SHARE_STATUS_VERIFIED,
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(2),
            })
            .expect("add verified share");
        store
            .add_block(&DbBlock {
                height: 9001,
                hash: "cov-replay-block".to_string(),
                difficulty: 1,
                finder: address.clone(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: base + Duration::from_secs(30),
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add unconfirmed block");
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: address.clone(),
                total_shares: 3,
                sampled_shares: 1,
                invalid_samples: 0,
                risk_sampled_shares: 1,
                risk_invalid_samples: 0,
                forced_started_at: None,
                forced_until: Some(SystemTime::now() + Duration::from_secs(60)),
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: Some(ValidationHoldCause::PayoutCoverage),
                last_seen_at: SystemTime::now(),
            })
            .expect("seed validation hold");

        let engine = ValidationEngine::new_with_state_store(
            cfg,
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );
        engine.inner.maintenance_tick();

        let replayed = store
            .get_recent_shares(10)
            .expect("recent shares after replay")
            .into_iter()
            .filter(|share| share.miner == address && share.job_id.starts_with("cov-prov"))
            .collect::<Vec<_>>();
        assert_eq!(replayed.len(), 2);
        assert!(
            replayed
                .iter()
                .all(|share| share.status == SHARE_STATUS_VERIFIED && share.was_sampled),
            "pending-window provisionals should be replay-verified during payout coverage hold"
        );

        let mut next = matching_task(10);
        next.address = address;
        let result = engine.process_inline(next);
        assert!(
            !result.verified,
            "replayed verified weight should clear the payout coverage hold"
        );
    }

    #[test]
    fn provisional_backlog_hold_replays_existing_pending_window_shares() {
        let store = require_test_store!();
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 2;
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "6h".to_string();
        cfg.provisional_share_delay = "50ms".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_provisional_cap_multiplier = 1.0;

        let address = format!("backlog-replay-{}", rand::random::<u64>());
        let now = SystemTime::now();
        let old_share_time = now - Duration::from_millis(200);
        store
            .add_share_with_replay(
                ShareRecord {
                    job_id: "backlog-prov".to_string(),
                    miner: address.clone(),
                    worker: "wa".to_string(),
                    difficulty: 1,
                    nonce: 11,
                    status: SHARE_STATUS_PROVISIONAL,
                    was_sampled: false,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at: old_share_time,
                },
                Some(ShareReplayData {
                    job_id: "backlog-prov".to_string(),
                    header_base: vec![9, 8, 7, 6],
                    network_target: [0xff; 32],
                    created_at: old_share_time,
                }),
            )
            .expect("add backlog replay share");
        store
            .add_block(&DbBlock {
                height: 9002,
                hash: "backlog-replay-block".to_string(),
                difficulty: 1,
                finder: address.clone(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: now - Duration::from_millis(100),
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add backlog block");
        store
            .add_validation_provisional(&address, None, now - Duration::from_millis(10))
            .expect("seed provisional a");
        store
            .add_validation_provisional(&address, None, now - Duration::from_millis(20))
            .expect("seed provisional b");
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: address.clone(),
                total_shares: 3,
                sampled_shares: 0,
                invalid_samples: 0,
                risk_sampled_shares: 0,
                risk_invalid_samples: 0,
                forced_started_at: None,
                forced_until: Some(SystemTime::now() + Duration::from_secs(60)),
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: Some(ValidationHoldCause::ProvisionalBacklog),
                last_seen_at: SystemTime::now(),
            })
            .expect("seed backlog hold");

        let engine = ValidationEngine::new_with_state_store(
            cfg,
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );
        engine.inner.maintenance_tick();

        let replayed = store
            .get_recent_shares(10)
            .expect("recent shares after backlog replay")
            .into_iter()
            .find(|share| share.job_id == "backlog-prov")
            .expect("backlog replay share");
        assert_eq!(replayed.status, SHARE_STATUS_VERIFIED);
        assert!(replayed.was_sampled);
    }

    #[test]
    fn low_coverage_address_without_active_hold_replays_pending_window_shares() {
        let store = require_test_store!();
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 100;
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "6h".to_string();
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_provisional_cap_multiplier = 19.0;

        let address = format!("passive-coverage-{}", rand::random::<u64>());
        let base = SystemTime::now() - Duration::from_secs(120);

        store
            .add_share(ShareRecord {
                job_id: "passive-verified".to_string(),
                miner: address.clone(),
                worker: "wa".to_string(),
                difficulty: 1,
                nonce: 41,
                status: SHARE_STATUS_VERIFIED,
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            })
            .expect("add verified share");
        for (job_id, nonce, offset) in [
            ("passive-prov-a", 42u64, 1u64),
            ("passive-prov-b", 43u64, 2u64),
        ] {
            store
                .add_share_with_replay(
                    ShareRecord {
                        job_id: job_id.to_string(),
                        miner: address.clone(),
                        worker: "wa".to_string(),
                        difficulty: 1,
                        nonce,
                        status: SHARE_STATUS_PROVISIONAL,
                        was_sampled: false,
                        block_hash: None,
                        claimed_hash: None,
                        reject_reason: None,
                        created_at: base + Duration::from_secs(offset),
                    },
                    Some(ShareReplayData {
                        job_id: job_id.to_string(),
                        header_base: vec![4, 3, 2, offset as u8],
                        network_target: [0xff; 32],
                        created_at: base + Duration::from_secs(offset),
                    }),
                )
                .expect("add replayable low-coverage share");
        }
        store
            .add_block(&DbBlock {
                height: 9003,
                hash: "passive-coverage-block".to_string(),
                difficulty: 1,
                finder: address.clone(),
                finder_worker: "wa".to_string(),
                reward: 1_000,
                timestamp: base + Duration::from_secs(30),
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("add passive coverage block");
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: address.clone(),
                total_shares: 3,
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
                last_seen_at: SystemTime::now(),
            })
            .expect("seed passive validation state");

        let engine = ValidationEngine::new_with_state_store(
            cfg,
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );
        engine.inner.maintenance_tick();

        let replayed = store
            .get_recent_shares(10)
            .expect("recent shares after passive replay")
            .into_iter()
            .filter(|share| share.miner == address && share.job_id.starts_with("passive-prov"))
            .collect::<Vec<_>>();
        assert_eq!(replayed.len(), 2);
        assert!(
            replayed
                .iter()
                .all(|share| share.status == SHARE_STATUS_VERIFIED && share.was_sampled),
            "low-coverage inactive participants should also be replay-verified"
        );

        let mut next = matching_task(44);
        next.address = address;
        let result = engine.process_inline(next);
        assert!(
            !result.verified,
            "replay should recover coverage without leaving the address in forced verification"
        );
    }

    #[test]
    fn background_audit_scheduler_drip_feeds_small_batches() {
        let mut cfg = test_cfg();
        cfg.audit_verifiers = 1;
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 100;
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_scheme = "pplns".to_string();
        cfg.pplns_window_duration = "6h".to_string();
        cfg.payout_min_verified_shares = 1;
        cfg.payout_provisional_cap_multiplier = 1.0;

        let address = "audit-drip-feed".to_string();
        let base = SystemTime::now() - Duration::from_secs(120);
        let hasher = DeterministicTestHasher;
        let mut accepted_window = vec![PersistedValidationAcceptedShare {
            address: address.clone(),
            share_id: 1,
            created_at: base,
            difficulty: 1,
            verified: true,
        }];
        let mut pending_audits = Vec::new();

        for offset in 0..8u64 {
            let share_id = 100 + offset as i64;
            let nonce = 50 + offset;
            let header_base = vec![9, 8, 7, offset as u8];
            let created_at = base + Duration::from_secs(offset + 1);
            let claimed_hash = hasher.hash(&header_base, nonce).expect("hash");
            accepted_window.push(PersistedValidationAcceptedShare {
                address: address.clone(),
                share_id,
                created_at,
                difficulty: 1,
                verified: false,
            });
            pending_audits.push(PendingAuditShare {
                share_id,
                job_id: format!("audit-drip-{offset}"),
                miner: address.clone(),
                worker: "wa".to_string(),
                difficulty: 1,
                nonce,
                claimed_hash: Some(claimed_hash),
                header_base,
                network_target: [0xff; 32],
                created_at,
            });
        }

        let store = Arc::new(InMemoryAuditStore::new(
            LoadedValidationState {
                states: vec![PersistedValidationAddressState {
                    address: address.clone(),
                    total_shares: accepted_window.len() as u64,
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
                    last_seen_at: SystemTime::now(),
                }],
                provisionals: Vec::new(),
                accepted_window,
            },
            pending_audits,
        ));

        let engine = ValidationEngine::new_with_state_store(
            cfg,
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );

        engine.inner.maintenance_tick();
        wait_for(
            || engine.snapshot().audit_enqueued >= 2,
            Duration::from_secs(1),
        );
        assert_eq!(
            engine.snapshot().audit_enqueued,
            2,
            "one maintenance pass should only enqueue a bounded drip-feed batch"
        );

        std::thread::sleep(VALIDATION_AUDIT_SCHED_INTERVAL + Duration::from_millis(100));
        engine.inner.maintenance_tick();
        wait_for(
            || engine.snapshot().audit_enqueued >= 4,
            Duration::from_secs(1),
        );
        let second_pass = engine.snapshot().audit_enqueued;
        assert!(
            (4..8).contains(&second_pass),
            "later maintenance passes should continue draining the backlog in bounded batches, got {second_pass}"
        );
    }

    #[test]
    fn full_mode_always_verifies() {
        let mut cfg = test_cfg();
        cfg.validation_mode = "full".to_string();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = None;
        task.share_target = difficulty_to_target(1);

        let result = engine.process_inline(task);
        assert!(result.verified);
    }

    #[test]
    fn missing_claimed_hash_rejected_when_not_full_verified() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = None;

        let result = engine.process_inline(task);
        assert!(!result.accepted);
        assert_eq!(result.reject_reason, Some("claimed hash required"));
    }

    #[test]
    fn queue_submit_returns_none_when_full() {
        let mut cfg = test_cfg();
        cfg.max_validation_queue = 1;
        cfg.regular_validation_queue = 1;
        cfg.regular_verifiers = 1;

        struct SlowHasher;
        impl PowHasher for SlowHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(150));
                Ok([0x01; 32])
            }
        }

        let engine = ValidationEngine::new(cfg, Arc::new(SlowHasher));
        let mut task = base_task();
        task.force_full_verify = true;
        task.claimed_hash = Some([0x01; 32]);

        let r1 = engine.submit(task.clone(), false);
        assert!(r1.is_some());
        let started = Instant::now();
        while engine.snapshot().in_flight == 0 && started.elapsed() < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(5));
        }

        let r2 = engine.submit(task.clone(), false);
        assert!(
            r2.is_some(),
            "second submit should occupy the bounded queue"
        );
        let r3 = engine.submit(task.clone(), false);
        assert!(r3.is_none(), "third submit should fail when queue is full");
    }

    #[test]
    fn candidate_queue_is_prioritized_after_current_task() {
        let mut cfg = test_cfg();
        cfg.validation_mode = "full".to_string();

        struct SleepyHasher;
        impl PowHasher for SleepyHasher {
            fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(60));
                let mut out = [0u8; 32];
                out[31] = nonce as u8;
                Ok(out)
            }
        }

        let engine = ValidationEngine::new(cfg, Arc::new(SleepyHasher));

        let mut regular1 = base_task();
        regular1.nonce = 1;
        regular1.claimed_hash = Some([0u8; 32]);
        regular1.force_full_verify = true;

        let mut regular2 = base_task();
        regular2.nonce = 2;
        regular2.claimed_hash = Some([0u8; 32]);
        regular2.force_full_verify = true;

        let mut candidate = base_task();
        candidate.nonce = 3;
        candidate.claimed_hash = Some([0u8; 32]);
        candidate.force_full_verify = true;

        let rx1 = engine.submit(regular1, false).expect("queue regular1");
        let rx2 = engine.submit(regular2, false).expect("queue regular2");
        std::thread::sleep(Duration::from_millis(5));
        let rxc = engine.submit(candidate, true).expect("queue candidate");

        let first = rx1
            .recv_timeout(Duration::from_secs(1))
            .expect("first result");
        assert_eq!(first.nonce, 1);

        let second_candidate = rxc
            .recv_timeout(Duration::from_secs(1))
            .expect("candidate should complete second");
        assert_eq!(second_candidate.nonce, 3);

        let third = rx2
            .recv_timeout(Duration::from_secs(1))
            .expect("third result");
        assert_eq!(third.nonce, 2);
    }

    #[test]
    fn dropped_timed_out_invalid_sample_does_not_force_review() {
        struct SlowDeterministicHasher;
        impl PowHasher for SlowDeterministicHasher {
            fn hash(&self, header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(80));
                DeterministicTestHasher.hash(header_base, nonce)
            }
        }

        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.invalid_sample_min = 1;
        cfg.invalid_sample_count_threshold = 1;
        cfg.invalid_sample_threshold = 0.01;
        cfg.max_provisional_shares = 100;
        cfg.regular_verifiers = 1;
        let engine = ValidationEngine::new(cfg, Arc::new(SlowDeterministicHasher));

        let mut timed_out = matching_task(1);
        timed_out.force_full_verify = true;
        timed_out.claimed_hash = Some([0xAA; 32]);
        let rx = engine
            .submit(timed_out, false)
            .expect("timed-out share should enqueue");
        assert!(
            matches!(
                rx.recv_timeout(Duration::from_millis(10)),
                Err(flume::RecvTimeoutError::Timeout)
            ),
            "share should still be waiting when the caller gives up"
        );
        drop(rx);
        std::thread::sleep(Duration::from_millis(120));

        let next = engine.process_inline(matching_task(2));
        assert!(
            !next.verified,
            "dropped timeout should not leave the address in forced validation"
        );

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.tracked_addresses, 1);
        assert_eq!(snapshot.total_shares, 1);
        assert_eq!(snapshot.sampled_shares, 0);
        assert_eq!(snapshot.invalid_samples, 0);
    }

    #[test]
    fn dropped_timed_out_provisional_share_does_not_build_force_verify_pressure() {
        struct SlowDeterministicHasher;
        impl PowHasher for SlowDeterministicHasher {
            fn hash(&self, header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(80));
                DeterministicTestHasher.hash(header_base, nonce)
            }
        }

        let mut cfg = test_cfg();
        cfg.max_provisional_shares = 1;
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;
        cfg.regular_verifiers = 1;
        cfg.regular_validation_queue = 2;
        let engine = ValidationEngine::new(cfg, Arc::new(SlowDeterministicHasher));

        let mut blocker = matching_task(100);
        blocker.force_full_verify = true;
        let blocker_rx = engine
            .submit(blocker, false)
            .expect("blocking share should enqueue");

        let rx = engine
            .submit(matching_task(1), false)
            .expect("timed-out provisional share should enqueue");
        assert!(
            matches!(
                rx.recv_timeout(Duration::from_millis(10)),
                Err(flume::RecvTimeoutError::Timeout)
            ),
            "share should still be waiting when the caller gives up"
        );
        drop(rx);
        let _ = blocker_rx.recv_timeout(Duration::from_secs(1));
        std::thread::sleep(Duration::from_millis(120));

        let next = engine.process_inline(matching_task(2));
        assert!(
            !next.verified,
            "dropped timeout should not count toward provisional pressure"
        );

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.pending_provisional, 1);
    }

    #[test]
    fn snapshot_tracks_state() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = Some([0x01; 32]);
        let _ = engine.process_inline(task);

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.tracked_addresses, 1);
        assert_eq!(snapshot.total_shares, 1);
    }

    #[test]
    fn hash_failure_rejects_share() {
        struct FailingHasher;
        impl PowHasher for FailingHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Err(anyhow::anyhow!("hash failed"))
            }
        }

        let mut cfg = test_cfg();
        cfg.validation_mode = "full".to_string();
        cfg.invalid_sample_min = 10;
        let engine = ValidationEngine::new(cfg, Arc::new(FailingHasher));

        let mut task = base_task();
        task.force_full_verify = true;
        task.claimed_hash = Some([0x01; 32]);

        let result = engine.process_inline(task);
        assert!(!result.accepted);
        assert_eq!(result.reject_reason, Some("hash computation failed"));
    }

    #[test]
    fn warmup_shares_force_full_verification() {
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 3;
        cfg.min_sample_every = 0;
        cfg.max_provisional_shares = 100;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let r1 = engine.process_inline(matching_task(1));
        let r2 = engine.process_inline(matching_task(2));
        let r3 = engine.process_inline(matching_task(3));
        let r4 = engine.process_inline(matching_task(4));

        assert!(r1.verified);
        assert!(r2.verified);
        assert!(r3.verified);
        assert!(!r4.verified);
    }

    #[test]
    fn min_sample_every_forces_periodic_full_verification() {
        let mut cfg = test_cfg();
        cfg.sample_rate = 0.0;
        cfg.warmup_shares = 1;
        cfg.min_sample_every = 3;
        cfg.max_provisional_shares = 100;
        cfg.max_provisional_recent_verified_multiplier = 1_000_000.0;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let r1 = engine.process_inline(matching_task(1));
        let r2 = engine.process_inline(matching_task(2));
        let r3 = engine.process_inline(matching_task(3));
        let r4 = engine.process_inline(matching_task(4));
        let r5 = engine.process_inline(matching_task(5));
        let r6 = engine.process_inline(matching_task(6));

        assert!(r1.verified);
        assert!(!r2.verified);
        assert!(r3.verified);
        assert!(!r4.verified);
        assert!(!r5.verified);
        assert!(r6.verified);
    }

    #[test]
    fn provisional_validation_state_survives_restart() {
        let cfg = test_cfg();
        let store = require_test_store!();
        let engine = ValidationEngine::new_with_state_store(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );

        let first = engine.process_inline(matching_task(1));
        let second = engine.process_inline(matching_task(2));
        assert!(first.accepted && !first.verified);
        assert!(second.accepted && !second.verified);

        let restarted = ValidationEngine::new_with_state_store(
            cfg,
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );
        let after_restart = restarted.process_inline(matching_task(3));
        assert!(
            after_restart.verified,
            "persisted provisional share pressure should survive restart"
        );
    }

    #[test]
    fn external_clear_event_resets_live_forced_state() {
        let mut cfg = test_cfg();
        cfg.max_provisional_shares = 100;
        let store = require_test_store!();
        let address = format!("validation-clear-{}", rand::random::<u64>());
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: address.clone(),
                total_shares: 12,
                sampled_shares: 3,
                invalid_samples: 1,
                risk_sampled_shares: 3,
                risk_invalid_samples: 1,
                forced_started_at: Some(SystemTime::now()),
                forced_until: Some(SystemTime::now() + Duration::from_secs(60)),
                forced_sampled_shares: 1,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: Some(ValidationHoldCause::InvalidSamples),
                last_seen_at: SystemTime::now(),
            })
            .expect("seed forced validation state");

        let engine = ValidationEngine::new_with_state_store(
            cfg,
            Arc::new(DeterministicTestHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        );

        let mut before_clear = matching_task(1);
        before_clear.address = address.clone();
        let before = engine.process_inline(before_clear);
        assert!(
            before.verified,
            "seeded forced state should force verification"
        );

        store
            .clear_address_risk_history(&address)
            .expect("clear validation history");

        let mut after_clear = matching_task(2);
        after_clear.address = address.clone();
        let after = engine.process_inline(after_clear);
        assert!(
            !after.verified,
            "external clear should remove live forced validation state"
        );
        assert!(
            store
                .validation_forced_until(&address)
                .expect("read validation row")
                .is_none(),
            "cleared address should not retain a forced-until timestamp"
        );
    }

    #[test]
    fn submit_queue_pressure_triggers_overload_mode() {
        let mut cfg = test_cfg();
        cfg.sample_rate = 1.0;
        cfg.max_provisional_shares = 100;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));
        let submit_queue = Arc::new(QueueTracker::new(32));
        engine.attach_submit_regular_queue(Arc::clone(&submit_queue));
        submit_queue.push(Instant::now() - Duration::from_secs(11));

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.overload_mode, OverloadMode::Emergency);
        assert_eq!(snapshot.effective_sample_rate, 0.0);

        let result = engine.process_inline(matching_task(1));
        assert!(
            !result.verified,
            "submit backlog emergency should suppress discretionary full verification"
        );
    }

    #[test]
    fn overload_mode_can_downgrade_from_emergency_to_shed() {
        let mut cfg = test_cfg();
        cfg.overload_shed_oldest_age = "4s".to_string();
        cfg.overload_emergency_oldest_age = "10s".to_string();
        cfg.overload_clear_oldest_age = "3s".to_string();
        cfg.overload_clear_hold = "5m".to_string();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));
        let submit_queue = Arc::new(QueueTracker::new(32));
        engine.attach_submit_regular_queue(Arc::clone(&submit_queue));

        let emergency_id = submit_queue.push(Instant::now() - Duration::from_secs(11));
        assert_eq!(engine.snapshot().overload_mode, OverloadMode::Emergency);

        submit_queue.remove(emergency_id);
        submit_queue.push(Instant::now() - Duration::from_secs(5));

        let snapshot = engine.snapshot();
        assert_eq!(
            snapshot.overload_mode,
            OverloadMode::Shed,
            "overload mode should downgrade once the queue is no longer at the emergency threshold"
        );
    }
}
