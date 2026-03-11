use std::collections::{HashMap, VecDeque};
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
use crate::pow::{check_target, PowHasher};
use crate::telemetry::{default_latency_window, LatencyWindow, PercentileSummary, QueueTracker};

pub const SHARE_STATUS_VERIFIED: &str = "verified";
pub const SHARE_STATUS_PROVISIONAL: &str = "provisional";
pub const SHARE_STATUS_REJECTED: &str = "rejected";
const VALIDATION_STATE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const VALIDATION_STATE_MAX_TRACKED: usize = 100_000;
const VALIDATION_PERSIST_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const VALIDATION_CLEAR_SYNC_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ValidationTask {
    pub address: String,
    pub nonce: u64,
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
    pub last_seen_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PersistedValidationProvisional {
    pub address: String,
    pub created_at: SystemTime,
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
}

pub trait ValidationStateStore: Send + Sync + 'static {
    fn load_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState>;

    fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()>;

    fn add_validation_provisional(&self, address: &str, created_at: SystemTime) -> Result<()>;

    fn clean_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<()>;

    fn latest_validation_clear_event_id(&self) -> Result<i64>;

    fn load_validation_clear_events_since(&self, cursor: i64) -> Result<Vec<ValidationClearEvent>>;
}

#[derive(Debug, Default)]
struct NullValidationStateStore;

impl ValidationStateStore for NullValidationStateStore {
    fn load_validation_state(
        &self,
        _state_cutoff: SystemTime,
        _provisional_cutoff: SystemTime,
        _now: SystemTime,
    ) -> Result<LoadedValidationState> {
        Ok(LoadedValidationState::default())
    }

    fn upsert_validation_state(&self, _state: &PersistedValidationAddressState) -> Result<()> {
        Ok(())
    }

    fn add_validation_provisional(&self, _address: &str, _created_at: SystemTime) -> Result<()> {
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
    provisional_at: VecDeque<SystemTime>,
    last_seen_at: SystemTime,
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
            provisional_at: VecDeque::new(),
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
    }

    fn start_forced_review(&mut self, start_at: SystemTime, duration: Duration) {
        self.forced_started_at = Some(start_at);
        self.forced_until = Some(start_at + duration);
        self.forced_sampled_shares = 0;
        self.forced_invalid_samples = 0;
        self.resume_forced_at = None;
        self.clear_risk_window();
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ValidationSnapshot {
    pub in_flight: i64,
    pub candidate_queue_depth: usize,
    pub regular_queue_depth: usize,
    pub candidate_oldest_age_millis: Option<u64>,
    pub regular_oldest_age_millis: Option<u64>,
    pub candidate_wait: PercentileSummary,
    pub regular_wait: PercentileSummary,
    pub validation_duration: PercentileSummary,
    pub tracked_addresses: usize,
    pub forced_verify_addresses: usize,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub pending_provisional: u64,
    pub fraud_detections: u64,
    pub candidate_false_claims: u64,
    pub overload_mode: OverloadMode,
    pub effective_sample_rate: f64,
}

struct QueuedTask {
    task: ValidationTask,
    result_tx: flume::Sender<ValidationComputeResult>,
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
    state_store: Arc<dyn ValidationStateStore>,
    last_cleanup_at: Mutex<Option<Instant>>,
    last_clear_sync_at: Mutex<Option<Instant>>,
    last_clear_event_id: Mutex<i64>,
    candidate_queue: QueueTracker,
    regular_queue: QueueTracker,
    submit_regular_queue: Mutex<Option<Arc<QueueTracker>>>,
    validation_duration: Mutex<LatencyWindow>,
    overload: Mutex<OverloadState>,
}

pub struct ValidationEngine {
    inner: Arc<ValidationInner>,
    candidate_tx: flume::Sender<QueuedTask>,
    regular_tx: flume::Sender<QueuedTask>,
    shutdown: Arc<AtomicBool>,
    candidate_workers: Vec<thread::JoinHandle<()>>,
    regular_workers: Vec<thread::JoinHandle<()>>,
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
        let candidate_queue_size = config.candidate_validation_queue_size();
        let regular_queue_size = config.regular_validation_queue_size();

        let (candidate_tx, candidate_rx) = flume::bounded::<QueuedTask>(candidate_queue_size);
        let (regular_tx, regular_rx) = flume::bounded::<QueuedTask>(regular_queue_size);
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
            state_store,
            last_cleanup_at: Mutex::new(None),
            last_clear_sync_at: Mutex::new(None),
            last_clear_event_id: Mutex::new(initial_clear_event_id),
            candidate_queue: QueueTracker::new(512),
            regular_queue: QueueTracker::new(512),
            submit_regular_queue: Mutex::new(None),
            validation_duration: Mutex::new(default_latency_window()),
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
                        Ok(queued) => inner_cloned.process(queued, false),
                        Err(flume::RecvTimeoutError::Timeout) => continue,
                        Err(flume::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }));
        }

        Self {
            inner,
            candidate_tx,
            regular_tx,
            shutdown,
            candidate_workers: candidate_handles,
            regular_workers: regular_handles,
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

        let result = if candidate {
            self.candidate_tx.try_send(queued)
        } else {
            self.regular_tx.try_send(queued)
        };

        match result {
            Ok(()) => {
                self.inner.evaluate_overload(Instant::now());
                Some(rx)
            }
            Err(flume::TrySendError::Full(_)) | Err(flume::TrySendError::Disconnected(_)) => {
                if candidate {
                    self.inner.candidate_queue.remove(tracker_id);
                } else {
                    self.inner.regular_queue.remove(tracker_id);
                }
                None
            }
        }
    }

    pub(crate) fn process_inline(&self, task: ValidationTask) -> ValidationResult {
        let started_at = Instant::now();
        self.inner.in_flight.fetch_add(1, Ordering::Relaxed);
        let prepared = self.inner.prepare_task(&task.address);
        let computed = self.inner.compute_task(&task, prepared);
        let result = self.inner.finalize_result(&task.address, computed);
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
        address: &str,
        computed: ValidationComputeResult,
    ) -> ValidationResult {
        self.inner.finalize_result(address, computed)
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
        while let Some(handle) = self.candidate_workers.pop() {
            let _ = handle.join();
        }
        while let Some(handle) = self.regular_workers.pop() {
            let _ = handle.join();
        }
    }
}

impl ValidationInner {
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
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.validation_duration.lock().record(started_at.elapsed());
        self.evaluate_overload(Instant::now());
        let _ = queued.result_tx.send(result);
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
        let provisional_delay = self.config.provisional_share_delay_duration();
        let default_state = ValidationAddressState::default();
        let st = state.get(address).unwrap_or(&default_state);
        let live_provisionals = live_provisional_count(st, now, provisional_delay);

        if st.resume_forced_at.is_some_and(|start_at| start_at <= now)
            || st.forced_started_at.is_some()
            || st.forced_until.is_some_and(|deadline| now < deadline)
        {
            return true;
        }

        if overload_mode == OverloadMode::Normal
            && self.config.max_provisional_shares > 0
            && live_provisionals >= self.config.max_provisional_shares as usize
        {
            return true;
        }

        if overload_mode == OverloadMode::Emergency {
            return false;
        }

        if overload_mode == OverloadMode::Normal {
            let next_share_idx = st.total_shares.saturating_add(1);
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

    fn finalize_result(
        &self,
        address: &str,
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
            address,
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
        sampled: bool,
        invalid_sample: bool,
        provisional_accepted: bool,
        overload_mode: OverloadMode,
    ) -> ValidationFollowupAction {
        let now = SystemTime::now();
        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);
        let forced_review_duration = self.config.invalid_sample_force_verify_duration();
        let min_samples = self.config.invalid_sample_min.max(1) as u64;
        let min_invalids = self.config.invalid_sample_count_threshold.max(1) as u64;
        let forced_clear_threshold = self.config.invalid_sample_threshold;
        let forced_quarantine_threshold = self.config.forced_validation_quarantine_threshold;

        st.total_shares = st.total_shares.saturating_add(1);
        self.prune_provisional_locked(st, now);
        st.last_seen_at = now;
        if st.resume_forced_at.is_some_and(|start_at| start_at <= now) {
            st.start_forced_review(now, forced_review_duration);
        }
        if overload_mode == OverloadMode::Normal
            && self.config.max_provisional_shares > 0
            && st.provisional_at.len() >= self.config.max_provisional_shares as usize
        {
            st.forced_until = Some(now + self.config.provisional_share_delay_duration());
        }
        if provisional_accepted {
            st.provisional_at.push_back(now);
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
                st.start_forced_review(now, forced_review_duration);
                if sampled {
                    st.forced_sampled_shares = 1;
                    st.forced_invalid_samples = u64::from(invalid_sample);
                }
            }
        }

        let persisted = persisted_validation_state(address, st);
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        drop(state);
        self.persist_validation_state(&persisted, provisional_accepted.then_some(now), now);
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
            .is_some_and(|timestamp| *timestamp <= cutoff)
        {
            st.provisional_at.pop_front();
        }
    }

    fn snapshot(&self) -> ValidationSnapshot {
        let now = SystemTime::now();
        let now_instant = Instant::now();
        let candidate_queue = self.candidate_queue.snapshot(now_instant);
        let regular_queue = self.regular_queue.snapshot(now_instant);
        let overload_mode = self.evaluate_overload(now_instant);
        let mut snap = ValidationSnapshot {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            candidate_queue_depth: candidate_queue.depth,
            regular_queue_depth: regular_queue.depth,
            candidate_oldest_age_millis: candidate_queue.oldest_age_millis,
            regular_oldest_age_millis: regular_queue.oldest_age_millis,
            candidate_wait: candidate_queue.wait,
            regular_wait: regular_queue.wait,
            validation_duration: self.validation_duration.lock().snapshot(),
            fraud_detections: self.fraud.load(Ordering::Relaxed),
            candidate_false_claims: self.candidate_false_claims.load(Ordering::Relaxed),
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
            snap.total_shares = snap.total_shares.saturating_add(st.total_shares);
            snap.sampled_shares = snap.sampled_shares.saturating_add(st.sampled_shares);
            snap.invalid_samples = snap.invalid_samples.saturating_add(st.invalid_samples);
            snap.pending_provisional = snap
                .pending_provisional
                .saturating_add(st.provisional_at.len() as u64);
            if st.forced_started_at.is_some() {
                snap.forced_verify_addresses += 1;
            }
        }

        snap
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
                    } else if shed && state.mode == OverloadMode::Normal {
                        state.mode = OverloadMode::Shed;
                    }
                    state.below_clear_started_at = None;
                }
            }
        }

        if state.mode == OverloadMode::Shed && emergency {
            state.mode = OverloadMode::Emergency;
        }
        state.mode
    }

    fn clear_address_state(&self, address: &str) {
        self.state.lock().remove(address);
    }

    fn address_has_live_hold(&self, address: &str) -> bool {
        let now = SystemTime::now();
        self.state.lock().get(address).is_some_and(|st| {
            st.forced_started_at.is_some()
                || st.forced_until.is_some_and(|deadline| deadline > now)
                || st.resume_forced_at.is_some_and(|start_at| start_at > now)
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
            st.last_seen_at >= cutoff
                || st.forced_started_at.is_some()
                || st.resume_forced_at.is_some_and(|start_at| start_at > now)
                || !st.provisional_at.is_empty()
        });
        if state.len() <= VALIDATION_STATE_MAX_TRACKED {
            return;
        }

        let mut removable = state
            .iter()
            .filter_map(|(address, st)| {
                if st.forced_started_at.is_some()
                    || st.resume_forced_at.is_some_and(|start_at| start_at > now)
                    || !st.provisional_at.is_empty()
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
        provisional_at: Option<SystemTime>,
        now: SystemTime,
    ) {
        if let Some(created_at) = provisional_at {
            if let Err(err) = self
                .state_store
                .add_validation_provisional(&state.address, created_at)
            {
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
        let persisted = persisted_validation_state(address, st);
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        drop(state);
        self.persist_validation_state(&persisted, None, now);
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

fn live_provisional_count(
    state: &ValidationAddressState,
    now: SystemTime,
    delay: Duration,
) -> usize {
    if state.provisional_at.is_empty() || delay.is_zero() {
        return 0;
    }

    let cutoff = now.checked_sub(delay).unwrap_or(now);
    state
        .provisional_at
        .iter()
        .filter(|timestamp| **timestamp > cutoff)
        .count()
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

    let loaded = match state_store.load_validation_state(state_cutoff, provisional_cutoff, now) {
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
                provisional_at: VecDeque::new(),
                last_seen_at: entry.last_seen_at,
            },
        );
    }

    for provisional in loaded.provisionals {
        let entry = state
            .entry(provisional.address.clone())
            .or_insert_with(ValidationAddressState::default);
        entry.provisional_at.push_back(provisional.created_at);
        if provisional.created_at > entry.last_seen_at {
            entry.last_seen_at = provisional.created_at;
        }
    }

    let delay = config.provisional_share_delay_duration();
    for entry in state.values_mut() {
        if delay.is_zero() {
            entry.provisional_at.clear();
            continue;
        }
        let cutoff = now.checked_sub(delay).unwrap_or(now);
        while entry
            .provisional_at
            .front()
            .is_some_and(|timestamp| *timestamp <= cutoff)
        {
            entry.provisional_at.pop_front();
        }
    }

    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pow::{difficulty_to_target, DeterministicTestHasher};
    use crate::store::PoolStore;
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

    fn base_task() -> ValidationTask {
        ValidationTask {
            address: "addr1".to_string(),
            nonce: 1,
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
    fn provisional_cap_forces_full_verify() {
        let cfg = test_cfg();
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let mut task = base_task();
        task.claimed_hash = Some([0x01; 32]);

        let r1 = engine.process_inline(task.clone());
        assert!(r1.accepted);
        assert!(!r1.verified);

        let r2 = engine.process_inline(task.clone());
        assert!(r2.accepted);
        assert!(!r2.verified);

        let r3 = engine.process_inline(task);
        assert!(r3.verified, "provisional cap should force full verify");
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
        let r2 = engine.submit(task.clone(), false);
        assert!(r2.is_none(), "second submit should fail when queue is full");
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
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 3;
        cfg.max_provisional_shares = 100;
        let engine = ValidationEngine::new(cfg, Arc::new(DeterministicTestHasher));

        let r1 = engine.process_inline(matching_task(1));
        let r2 = engine.process_inline(matching_task(2));
        let r3 = engine.process_inline(matching_task(3));
        let r4 = engine.process_inline(matching_task(4));
        let r5 = engine.process_inline(matching_task(5));
        let r6 = engine.process_inline(matching_task(6));

        assert!(!r1.verified);
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
        submit_queue.push(Instant::now() - Duration::from_secs(6));

        let snapshot = engine.snapshot();
        assert_eq!(snapshot.overload_mode, OverloadMode::Emergency);
        assert_eq!(snapshot.effective_sample_rate, 0.0);

        let result = engine.process_inline(matching_task(1));
        assert!(
            !result.verified,
            "submit backlog emergency should suppress discretionary full verification"
        );
    }
}
