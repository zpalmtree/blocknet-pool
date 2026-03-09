use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::config::Config;
use crate::pow::{check_target, PowHasher};

pub const SHARE_STATUS_VERIFIED: &str = "verified";
pub const SHARE_STATUS_PROVISIONAL: &str = "provisional";
pub const SHARE_STATUS_REJECTED: &str = "rejected";
const VALIDATION_STATE_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
const VALIDATION_STATE_MAX_TRACKED: usize = 100_000;
const VALIDATION_PERSIST_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct ValidationTask {
    pub address: String,
    pub nonce: u64,
    pub header_base: Vec<u8>,
    pub share_target: [u8; 32],
    pub network_target: [u8; 32],
    pub claimed_hash: Option<[u8; 32]>,
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
    pub escalate_risk: bool,
}

#[derive(Debug, Clone)]
pub struct PersistedValidationAddressState {
    pub address: String,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub forced_until: Option<SystemTime>,
    pub last_seen_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PersistedValidationProvisional {
    pub address: String,
    pub created_at: SystemTime,
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
}

#[derive(Debug, Clone)]
struct ValidationAddressState {
    total_shares: u64,
    sampled_shares: u64,
    invalid_samples: u64,
    forced_until: Option<SystemTime>,
    provisional_at: VecDeque<SystemTime>,
    last_seen_at: SystemTime,
}

impl Default for ValidationAddressState {
    fn default() -> Self {
        Self {
            total_shares: 0,
            sampled_shares: 0,
            invalid_samples: 0,
            forced_until: None,
            provisional_at: VecDeque::new(),
            last_seen_at: SystemTime::now(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ValidationSnapshot {
    pub in_flight: i64,
    pub candidate_queue_depth: usize,
    pub regular_queue_depth: usize,
    pub tracked_addresses: usize,
    pub forced_verify_addresses: usize,
    pub total_shares: u64,
    pub sampled_shares: u64,
    pub invalid_samples: u64,
    pub pending_provisional: u64,
    pub fraud_detections: u64,
}

struct QueuedTask {
    task: ValidationTask,
    result_tx: flume::Sender<ValidationResult>,
}

struct ValidationInner {
    config: Config,
    hasher: Arc<dyn PowHasher>,
    state: Mutex<HashMap<String, ValidationAddressState>>,
    rng: Mutex<StdRng>,
    in_flight: AtomicI64,
    fraud: AtomicU64,
    state_store: Arc<dyn ValidationStateStore>,
    last_cleanup_at: Mutex<Option<Instant>>,
}

pub struct ValidationEngine {
    inner: Arc<ValidationInner>,
    candidate_tx: flume::Sender<QueuedTask>,
    regular_tx: flume::Sender<QueuedTask>,
    shutdown: Arc<AtomicBool>,
    workers: Vec<thread::JoinHandle<()>>,
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
        let worker_count = if config.max_verifiers <= 0 {
            std::thread::available_parallelism()
                .map(|n| n.get().max(1) / 2)
                .unwrap_or(1)
                .max(1)
        } else {
            config.max_verifiers as usize
        };
        let queue_size = config.max_validation_queue.max(1) as usize;

        let (candidate_tx, candidate_rx) = flume::bounded::<QueuedTask>(queue_size);
        let (regular_tx, regular_rx) = flume::bounded::<QueuedTask>(queue_size);
        let shutdown = Arc::new(AtomicBool::new(false));
        let initial_state = load_initial_validation_state(&config, state_store.as_ref());

        let inner = Arc::new(ValidationInner {
            config,
            hasher,
            state: Mutex::new(initial_state),
            rng: Mutex::new(StdRng::from_entropy()),
            in_flight: AtomicI64::new(0),
            fraud: AtomicU64::new(0),
            state_store,
            last_cleanup_at: Mutex::new(None),
        });

        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let inner_cloned = Arc::clone(&inner);
            let candidate_rx = candidate_rx.clone();
            let regular_rx = regular_rx.clone();
            let shutdown = Arc::clone(&shutdown);
            workers.push(thread::spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    if let Ok(queued) = candidate_rx.try_recv() {
                        inner_cloned.process(queued);
                        continue;
                    }

                    match regular_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(queued) => inner_cloned.process(queued),
                        Err(flume::RecvTimeoutError::Timeout) => continue,
                        Err(flume::RecvTimeoutError::Disconnected) => {
                            if candidate_rx.is_disconnected() {
                                break;
                            }
                        }
                    }
                }
            }));
        }

        Self {
            inner,
            candidate_tx,
            regular_tx,
            shutdown,
            workers,
        }
    }

    pub fn submit(
        &self,
        task: ValidationTask,
        candidate: bool,
    ) -> Option<flume::Receiver<ValidationResult>> {
        let (tx, rx) = flume::bounded(1);
        let queued = QueuedTask {
            task,
            result_tx: tx,
        };

        let result = if candidate {
            self.candidate_tx.try_send(queued)
        } else {
            self.regular_tx.try_send(queued)
        };

        match result {
            Ok(()) => Some(rx),
            Err(flume::TrySendError::Full(_)) | Err(flume::TrySendError::Disconnected(_)) => None,
        }
    }

    pub fn process_inline(&self, task: ValidationTask) -> ValidationResult {
        let (tx, rx) = flume::bounded(1);
        self.inner.process(QueuedTask {
            task,
            result_tx: tx,
        });
        rx.recv().expect("inline validation should return result")
    }

    pub fn snapshot(&self) -> ValidationSnapshot {
        self.inner
            .snapshot(self.candidate_tx.len(), self.regular_tx.len())
    }
}

impl Drop for ValidationEngine {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        while let Some(handle) = self.workers.pop() {
            let _ = handle.join();
        }
    }
}

impl ValidationInner {
    fn process(&self, queued: QueuedTask) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let result = self.process_task(&queued.task);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let _ = queued.result_tx.send(result);
    }

    fn process_task(&self, task: &ValidationTask) -> ValidationResult {
        let full_verify = task.force_full_verify || self.should_fully_verify(&task.address);

        let mut result = ValidationResult {
            nonce: task.nonce,
            accepted: false,
            reject_reason: None,
            hash: [0u8; 32],
            verified: full_verify,
            is_block_candidate: false,
            suspected_fraud: false,
            escalate_risk: false,
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
                    let invalid_sample = true;
                    let provisional_accepted = false;
                    if self.update_address_state(
                        &task.address,
                        full_verify,
                        invalid_sample,
                        result.suspected_fraud,
                        provisional_accepted,
                    ) {
                        result.escalate_risk = true;
                    }
                    return result;
                }
            };
            result.hash = hash;

            if let Some(claimed_hash) = task.claimed_hash {
                if claimed_hash != hash {
                    result.reject_reason = Some("invalid share proof");
                    result.suspected_fraud = true;
                    self.fraud.fetch_add(1, Ordering::Relaxed);
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

        let invalid_sample = full_verify && !result.accepted;
        let provisional_accepted = result.accepted && !full_verify;
        if self.update_address_state(
            &task.address,
            full_verify,
            invalid_sample,
            result.suspected_fraud,
            provisional_accepted,
        ) {
            result.escalate_risk = true;
        }

        result
    }

    fn should_fully_verify(&self, address: &str) -> bool {
        if self.config.validation_mode.eq_ignore_ascii_case("full") {
            return true;
        }

        let now = SystemTime::now();

        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);
        self.prune_provisional_locked(st, now);
        st.last_seen_at = now;

        if self.config.max_provisional_shares > 0
            && st.provisional_at.len() >= self.config.max_provisional_shares as usize
        {
            st.forced_until = Some(now + self.config.provisional_share_delay_duration());
        }

        if st.forced_until.is_some_and(|deadline| now < deadline) {
            return true;
        }

        let next_share_idx = st.total_shares + 1;
        if self.config.warmup_shares > 0 && next_share_idx <= self.config.warmup_shares as u64 {
            return true;
        }
        if self.config.min_sample_every > 0
            && next_share_idx.is_multiple_of(self.config.min_sample_every as u64)
        {
            return true;
        }

        let rate = self.config.sample_rate;
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }

        self.rng.lock().gen::<f64>() < rate
    }

    fn update_address_state(
        &self,
        address: &str,
        sampled: bool,
        invalid_sample: bool,
        suspected_fraud: bool,
        provisional_accepted: bool,
    ) -> bool {
        let now = SystemTime::now();
        let mut state = self.state.lock();
        let st = get_or_insert_state(&mut state, address);

        st.total_shares = st.total_shares.saturating_add(1);
        self.prune_provisional_locked(st, now);
        st.last_seen_at = now;
        if provisional_accepted {
            st.provisional_at.push_back(now);
        }
        if sampled {
            st.sampled_shares = st.sampled_shares.saturating_add(1);
            if invalid_sample {
                st.invalid_samples = st.invalid_samples.saturating_add(1);
            }
        }

        if suspected_fraud {
            st.forced_until = Some(now + self.config.suspected_fraud_force_verify_duration());
            let persisted = persisted_validation_state(address, st);
            drop(state);
            self.persist_validation_state(&persisted, None, now);
            return true;
        }

        let min_samples = self.config.invalid_sample_min.max(1) as u64;
        let min_invalids = self.config.invalid_sample_count_threshold.max(1) as u64;
        let mut escalate = false;
        if st.sampled_shares >= min_samples {
            let invalid_ratio = if st.sampled_shares == 0 {
                0.0
            } else {
                st.invalid_samples as f64 / st.sampled_shares as f64
            };

            if st.invalid_samples >= min_invalids
                && invalid_ratio > self.config.invalid_sample_threshold
            {
                st.forced_until = Some(now + self.config.invalid_sample_force_verify_duration());
                escalate = true;
            }
        }

        let persisted = persisted_validation_state(address, st);
        if state.len() > VALIDATION_STATE_MAX_TRACKED {
            self.prune_stale_state_locked(&mut state, now);
        }
        drop(state);
        self.persist_validation_state(&persisted, provisional_accepted.then_some(now), now);
        escalate
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

    fn snapshot(&self, candidate_depth: usize, regular_depth: usize) -> ValidationSnapshot {
        let now = SystemTime::now();
        let mut snap = ValidationSnapshot {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            candidate_queue_depth: candidate_depth,
            regular_queue_depth: regular_depth,
            fraud_detections: self.fraud.load(Ordering::Relaxed),
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
            if st.forced_until.is_some_and(|deadline| now < deadline) {
                snap.forced_verify_addresses += 1;
            }
        }

        snap
    }

    fn prune_stale_state_locked(
        &self,
        state: &mut HashMap<String, ValidationAddressState>,
        now: SystemTime,
    ) {
        let cutoff = now.checked_sub(VALIDATION_STATE_RETENTION).unwrap_or(now);
        state.retain(|_, st| {
            st.last_seen_at >= cutoff
                || st.forced_until.is_some_and(|deadline| deadline > now)
                || !st.provisional_at.is_empty()
        });
        if state.len() <= VALIDATION_STATE_MAX_TRACKED {
            return;
        }

        let mut removable = state
            .iter()
            .filter_map(|(address, st)| {
                if st.forced_until.is_some_and(|deadline| deadline > now)
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

fn persisted_validation_state(
    address: &str,
    state: &ValidationAddressState,
) -> PersistedValidationAddressState {
    PersistedValidationAddressState {
        address: address.to_string(),
        total_shares: state.total_shares,
        sampled_shares: state.sampled_shares,
        invalid_samples: state.invalid_samples,
        forced_until: state.forced_until,
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
                forced_until: entry.forced_until.filter(|deadline| *deadline > now),
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
        assert!(result.escalate_risk);

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
        assert!(third_result.escalate_risk);

        let after_threshold = engine.process_inline(matching_task(15));
        assert!(
            after_threshold.verified,
            "repeated invalid samples should eventually switch the address into verified-only mode"
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
}
