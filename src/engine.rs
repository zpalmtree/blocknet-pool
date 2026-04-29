use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::db::{AddressRiskState, PendingAuditShare, ShareReplayData};
use crate::dev_fee::{
    is_seine_dev_fee_address, login_difficulty_floor, should_allow_login_difficulty_hint_raise,
    should_bootstrap_login_from_address_hints, should_persist_login_hint_immediately,
    DEV_VARDIFF_BOOTSTRAP_HINT_LIMIT,
};
use crate::pow::{check_target, difficulty_to_target};
use crate::protocol::{
    build_login_result, normalize_capabilities, normalize_protocol_version, normalize_worker_name,
    parse_hash_hex, validate_miner_address_for_network, AddressNetwork, CAP_SUBMIT_CLAIMED_HASH,
    STRATUM_PROTOCOL_VERSION_CURRENT,
};
use crate::telemetry::QueueTracker;
use crate::validation::{
    ValidationEngine, ValidationFollowupAction, ValidationResult, ValidationTask,
    SHARE_STATUS_PROVISIONAL, SHARE_STATUS_REJECTED, SHARE_STATUS_VERIFIED,
};

// Start retargeting after two accepted shares so reconnects/high starting diff
// settle faster on weak miners.
const VARDIFF_MIN_SAMPLE_COUNT: usize = 2;
const VARDIFF_RATIO_DAMPING_EXPONENT: f64 = 0.45;
const VARDIFF_MIN_ADJUSTMENT_FACTOR: f64 = 1.02;
const VARDIFF_MAX_ADJUSTMENT_FACTOR: f64 = 1.5;
const VARDIFF_HINT_DECAY_START: Duration = Duration::from_secs(60 * 60);
const VARDIFF_HINT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const VARDIFF_HINT_PERSIST_INTERVAL: Duration = Duration::from_secs(60);
const LOGIN_DIFFICULTY_HINT_MIN_FACTOR: f64 = 0.25;
const LOGIN_DIFFICULTY_HINT_MAX_FACTOR: f64 = 4.0;
const FOUND_BLOCK_OUTBOX_ENV: &str = "BLOCKNET_POOL_FOUND_BLOCK_OUTBOX";
const FOUND_BLOCK_OUTBOX_DEFAULT_PATH: &str = "data/found-block-recovery.jsonl";
const FOUND_BLOCK_PERSIST_MAX_RETRIES: usize = 3;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub height: u64,
    pub header_base: Vec<u8>,
    pub network_target: [u8; 32],
    pub network_difficulty: u64,
    pub template_id: Option<String>,
    pub full_block: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ShareRecord {
    pub job_id: String,
    pub miner: String,
    pub worker: String,
    pub difficulty: u64,
    pub nonce: u64,
    pub status: &'static str,
    pub was_sampled: bool,
    pub block_hash: Option<String>,
    pub claimed_hash: Option<String>,
    pub reject_reason: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Default)]
pub struct BlockSubmitResponse {
    pub accepted: bool,
    pub hash: Option<String>,
    pub height: Option<u64>,
}

pub trait NodeApi: Send + Sync + 'static {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse>;

    fn current_chain_height(&self) -> Result<u64> {
        Ok(0)
    }

    fn block_hash_at_height(&self, _height: u64) -> Result<Option<String>> {
        Ok(None)
    }
}

pub trait JobRepository: Send + Sync + 'static {
    fn get_job(&self, job_id: &str) -> Option<Job>;
    fn current_job(&self) -> Option<Job>;
    fn resolve_submit_job_with_reason(
        &self,
        submitted_job_id: &str,
        submitted_at: Instant,
    ) -> std::result::Result<SubmitJobBinding, SubmitJobResolveError>;
    fn resolve_submit_job(
        &self,
        submitted_job_id: &str,
        submitted_at: Instant,
    ) -> Option<SubmitJobBinding> {
        self.resolve_submit_job_with_reason(submitted_job_id, submitted_at)
            .ok()
    }
}

#[derive(Debug, Clone)]
pub struct SubmitJobBinding {
    pub job: Job,
    pub share_difficulty: Option<u64>,
    pub assigned_miner: Option<String>,
    pub nonce_start: Option<u64>,
    pub nonce_end: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitJobResolveError {
    AssignmentNotFound,
    AssignmentExpired {
        age: Duration,
        ttl: Duration,
    },
    AssignmentSuperseded {
        stale_for: Duration,
        stale_grace: Duration,
    },
    TemplateStaleBeyondGrace {
        template_job_id: String,
        current_job_id: Option<String>,
        stale_for: Duration,
        stale_grace: Duration,
    },
    TemplateMissing {
        template_job_id: String,
    },
}

impl fmt::Display for SubmitJobResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AssignmentNotFound => write!(f, "stale job: assignment not found"),
            Self::AssignmentExpired { age, ttl } => write!(
                f,
                "stale job: assignment expired (age={}, ttl={})",
                format_duration(*age),
                format_duration(*ttl)
            ),
            Self::AssignmentSuperseded {
                stale_for,
                stale_grace,
            } => write!(
                f,
                "stale job: assignment superseded by newer difficulty (stale_for={}, stale_grace={})",
                format_duration(*stale_for),
                format_duration(*stale_grace)
            ),
            Self::TemplateStaleBeyondGrace {
                template_job_id,
                current_job_id,
                stale_for,
                stale_grace,
            } => {
                let current = current_job_id.as_deref().unwrap_or("-");
                write!(
                    f,
                    "stale job: template {template_job_id} exceeded stale grace (current={current}, stale_for={}, stale_grace={})",
                    format_duration(*stale_for),
                    format_duration(*stale_grace)
                )
            }
            Self::TemplateMissing { template_job_id } => write!(
                f,
                "stale job: template {template_job_id} for assignment is no longer available"
            ),
        }
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_millis() < 1_000 {
        format!("{}ms", duration.as_millis())
    } else {
        format!("{:.3}s", duration.as_secs_f64())
    }
}

fn active_quarantine_state(
    store: &dyn ShareStore,
    address: &str,
    now: SystemTime,
) -> Result<Option<AddressRiskState>> {
    Ok(store
        .address_risk_state(address)?
        .filter(|state| state.quarantined_until.is_some_and(|until| until > now)))
}

fn quarantine_error_message(state: Option<&AddressRiskState>, now: SystemTime) -> String {
    let Some(state) = state else {
        return "address quarantined".to_string();
    };

    let mut details = Vec::new();
    if state
        .last_reason
        .as_deref()
        .is_some_and(|reason| reason == "invalid share proof")
    {
        let active_fraud_strikes = state
            .suspected_fraud_window_until
            .filter(|until| *until > now)
            .map(|_| state.suspected_fraud_strikes)
            .unwrap_or_default();
        if active_fraud_strikes > 0 {
            details.push(format!("fraud strikes={active_fraud_strikes}"));
        }
    } else {
        let active_risk_strikes =
            active_window_strikes(state.strikes, state.strike_window_until, now);
        if active_risk_strikes > 0 {
            details.push(format!("risk strikes={active_risk_strikes}"));
        }
    }
    if let Some(until) = state.quarantined_until.filter(|until| *until > now) {
        let remaining = until.duration_since(now).unwrap_or_default();
        details.push(format!(
            "until {}, remaining {}",
            humantime::format_rfc3339_seconds(until),
            humantime::format_duration(remaining)
        ));
    }

    let prefix = state
        .last_reason
        .as_deref()
        .filter(|reason| !reason.trim().is_empty())
        .map(|reason| format!("address quarantined: {reason}"))
        .unwrap_or_else(|| "address quarantined".to_string());
    if details.is_empty() {
        prefix
    } else {
        format!("{prefix} ({})", details.join(", "))
    }
}

pub(crate) fn canonical_share_reject_reason(error: &str) -> &'static str {
    let trimmed = error.trim();
    if trimmed.starts_with("stale job:") || trimmed == "stale job" {
        return "stale job";
    }
    if trimmed.starts_with("duplicate share") {
        return "duplicate share";
    }
    if trimmed.starts_with("nonce out of assigned range") {
        return "nonce out of assigned range";
    }
    if trimmed.starts_with("job not assigned to this miner") {
        return "job not assigned";
    }
    if trimmed.starts_with("claimed hash required") {
        return "claimed hash required";
    }
    if trimmed.starts_with("rate limited") {
        return "rate limited";
    }
    if trimmed.starts_with("candidate claim busy") {
        return "candidate claim busy";
    }
    if trimmed.starts_with("candidate claim rate limited") {
        return "candidate claim rate limited";
    }
    if trimmed.starts_with("server busy") {
        return "server busy";
    }
    if trimmed.starts_with("validation timeout") {
        return "validation timeout";
    }
    if trimmed.starts_with("address quarantined") {
        return "address quarantined";
    }
    if trimmed.starts_with("low difficulty share") {
        return "low difficulty share";
    }
    if trimmed.starts_with("invalid share proof") {
        return "invalid share proof";
    }
    if trimmed.starts_with("hash computation failed") {
        return "hash computation failed";
    }
    if trimmed == "not logged in" {
        return "not logged in";
    }
    if trimmed == "invalid hex"
        || trimmed == "hex length must be even"
        || trimmed.starts_with("expected 32-byte hash")
    {
        return "invalid claimed hash";
    }
    "other"
}

pub trait ShareStore: Send + Sync + 'static {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool>;
    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()>;
    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        if self.is_share_seen(job_id, nonce)? {
            return Ok(false);
        }
        self.mark_share_seen(job_id, nonce)?;
        Ok(true)
    }
    fn release_share_claim(&self, _job_id: &str, _nonce: u64) -> Result<()> {
        Ok(())
    }
    fn add_share(&self, share: ShareRecord) -> Result<()>;
    fn add_share_with_id(&self, share: ShareRecord) -> Result<i64> {
        self.add_share(share)?;
        Err(anyhow!(
            "share store does not support returning inserted share ids"
        ))
    }
    fn add_share_with_replay(
        &self,
        share: ShareRecord,
        _replay: Option<ShareReplayData>,
    ) -> Result<()> {
        self.add_share(share)
    }
    fn add_share_with_replay_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<i64> {
        self.add_share_with_replay(share, replay)?;
        Err(anyhow!(
            "share store does not support returning inserted share ids with replay data"
        ))
    }
    fn add_found_block(&self, _block: FoundBlockRecord) -> Result<()> {
        Ok(())
    }
    fn address_risk_state(&self, _address: &str) -> Result<Option<AddressRiskState>> {
        Ok(None)
    }
    fn is_address_quarantined(&self, _address: &str) -> Result<bool> {
        Ok(false)
    }
    fn should_force_verify_address(&self, _address: &str) -> Result<bool> {
        Ok(false)
    }
    fn address_risk_strikes(&self, _address: &str) -> Result<u64> {
        Ok(0)
    }
    fn escalate_address_risk(
        &self,
        _address: &str,
        _reason: &str,
        _strike_window_duration: Duration,
        _quarantine_threshold: u64,
        _quarantine_base: Duration,
        _quarantine_max: Duration,
        _force_verify_duration: Duration,
    ) -> Result<()> {
        Ok(())
    }
    fn record_suspected_fraud(
        &self,
        _address: &str,
        _reason: &str,
        _quarantine_threshold: u64,
        _strike_window_duration: Duration,
        _quarantine_base: Duration,
        _quarantine_max: Duration,
        _force_verify_duration: Duration,
    ) -> Result<()> {
        Ok(())
    }
    fn clear_address_risk_history(&self, _address: &str) -> Result<()> {
        Ok(())
    }
    fn get_vardiff_hint(&self, _address: &str, _worker: &str) -> Result<Option<(u64, SystemTime)>> {
        Ok(None)
    }
    fn upsert_vardiff_hint(
        &self,
        _address: &str,
        _worker: &str,
        _difficulty: u64,
        _updated_at: SystemTime,
    ) -> Result<()> {
        Ok(())
    }
    fn get_vardiff_hints_for_address(
        &self,
        _address: &str,
        _limit: usize,
    ) -> Result<Vec<(u64, SystemTime)>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct FoundBlockRecord {
    pub height: u64,
    pub hash: String,
    pub difficulty: u64,
    pub reward: u64,
    pub finder: String,
    pub finder_worker: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FoundBlockOutboxRecord {
    height: u64,
    hash: String,
    difficulty: u64,
    #[serde(default)]
    reward: u64,
    finder: String,
    finder_worker: String,
    timestamp_unix: i64,
}

#[derive(Debug, Clone)]
pub struct SubmitAck {
    pub accepted: bool,
    pub verified: bool,
    pub status: &'static str,
    pub block_accepted: bool,
    pub share_difficulty: u64,
    pub next_difficulty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitQueueRoute {
    Candidate,
    Regular,
}

#[derive(Debug, Clone)]
struct MinerSession {
    address: String,
    worker: String,
    difficulty: u64,
    protocol_version: u32,
    capabilities: BTreeSet<String>,
    accepted_share_times: VecDeque<Instant>,
    last_accepted_share_at: Option<Instant>,
    last_difficulty_adjustment: Option<Instant>,
    last_difficulty_hint_write: Option<Instant>,
}

pub struct PoolEngine {
    cfg: Config,
    expected_address_network: Option<AddressNetwork>,
    validation: Arc<ValidationEngine>,
    jobs: Arc<dyn JobRepository>,
    store: Arc<dyn ShareStore>,
    node: Arc<dyn NodeApi>,

    sessions: Mutex<HashMap<String, MinerSession>>,
    seen_in_memory: Mutex<HashSet<(String, u64)>>,
    found_block_outbox_lock: Mutex<()>,
}

impl PoolEngine {
    pub fn preclassify_submit_route(
        &self,
        job_id: &str,
        claimed_hash_hex: Option<&str>,
        received_at: Instant,
    ) -> Result<SubmitQueueRoute> {
        let claimed_hash = match claimed_hash_hex {
            Some(value) if !value.trim().is_empty() => {
                Some(parse_hash_hex(value).map_err(|err| anyhow!(err))?)
            }
            _ => None,
        };
        if self.cfg.stratum_submit_v2_required && claimed_hash.is_none() {
            return Err(anyhow!("claimed hash required"));
        }

        let candidate = claimed_hash.is_some_and(|hash| {
            self.jobs
                .resolve_submit_job(job_id, received_at)
                .is_some_and(|binding| check_target(hash, binding.job.network_target))
        });
        Ok(if candidate {
            SubmitQueueRoute::Candidate
        } else {
            SubmitQueueRoute::Regular
        })
    }

    pub fn record_prequeue_reject(
        &self,
        conn_id: &str,
        job_id: &str,
        nonce: u64,
        received_at: Instant,
        reason: &str,
    ) {
        let Some(session) = self.sessions.lock().get(conn_id).cloned() else {
            return;
        };
        let share_difficulty = self
            .jobs
            .resolve_submit_job(job_id, received_at)
            .and_then(|binding| binding.share_difficulty)
            .unwrap_or(session.difficulty)
            .max(1);
        self.persist_rejected_share_for_error(&session, job_id, nonce, share_difficulty, reason);
    }

    pub fn new(
        cfg: Config,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        Self::new_with_expected_address_network(cfg, None, validation, jobs, store, node)
    }

    pub fn new_with_expected_address_network(
        cfg: Config,
        expected_address_network: Option<AddressNetwork>,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        let engine = Self {
            cfg,
            expected_address_network,
            validation,
            jobs,
            store,
            node,
            sessions: Mutex::new(HashMap::new()),
            seen_in_memory: Mutex::new(HashSet::new()),
            found_block_outbox_lock: Mutex::new(()),
        };
        engine.recover_found_block_outbox();
        engine
    }

    pub fn attach_submit_regular_queue(&self, queue: Arc<QueueTracker>) {
        self.validation.attach_submit_regular_queue(queue);
    }

    pub fn login(
        &self,
        conn_id: &str,
        address: String,
        worker: Option<String>,
        protocol_version: u32,
        capabilities: Vec<String>,
    ) -> Result<crate::protocol::LoginResult> {
        self.login_with_hint(
            conn_id,
            address,
            worker,
            protocol_version,
            capabilities,
            None,
        )
    }

    pub fn login_with_hint(
        &self,
        conn_id: &str,
        address: String,
        worker: Option<String>,
        protocol_version: u32,
        capabilities: Vec<String>,
        difficulty_hint: Option<u64>,
    ) -> Result<crate::protocol::LoginResult> {
        let address = address.trim();
        if address.is_empty() || address.len() > 128 {
            return Err(anyhow!("invalid address"));
        }
        if let Err(err) = validate_miner_address_for_network(address, self.expected_address_network)
        {
            return Err(anyhow!("invalid address: {err}"));
        }

        let worker = normalize_worker_name(worker.as_deref());
        let protocol_version = normalize_protocol_version(protocol_version);
        let caps = normalize_capabilities(&capabilities);
        let cap_set = caps.iter().cloned().collect::<BTreeSet<_>>();

        if self.cfg.stratum_submit_v2_required {
            if protocol_version < STRATUM_PROTOCOL_VERSION_CURRENT {
                return Err(anyhow!("pool requires protocol_version >= 2"));
            }
            if !cap_set.contains(CAP_SUBMIT_CLAIMED_HASH) {
                return Err(anyhow!("pool requires submit_claimed_hash capability"));
            }
        }
        let now_wall = SystemTime::now();
        if let Some(state) = active_quarantine_state(self.store.as_ref(), address, now_wall)? {
            return Err(anyhow!(quarantine_error_message(Some(&state), now_wall)));
        }
        let min_diff = self.cfg.min_share_difficulty.max(1);
        let max_diff = self.cfg.max_share_difficulty.max(min_diff);
        let initial_difficulty = self.cfg.initial_share_difficulty.clamp(min_diff, max_diff);
        let mut difficulty = initial_difficulty;
        if self.cfg.enable_vardiff {
            if let Some((cached, updated_at)) = self.store.get_vardiff_hint(address, &worker)? {
                let age = now_wall.duration_since(updated_at).unwrap_or_default();
                if let Some(derived) = derive_vardiff_login_difficulty(
                    cached,
                    age,
                    initial_difficulty,
                    min_diff,
                    max_diff,
                ) {
                    difficulty = derived;
                }
            }
        }
        let address_bootstrap =
            if self.cfg.enable_vardiff && should_bootstrap_login_from_address_hints(address) {
                derive_address_vardiff_login_difficulty(
                    self.store
                        .get_vardiff_hints_for_address(address, DEV_VARDIFF_BOOTSTRAP_HINT_LIMIT)?,
                    now_wall,
                    initial_difficulty,
                    min_diff,
                    max_diff,
                )
            } else {
                None
            };
        if let Some(bootstrap) = address_bootstrap {
            difficulty = difficulty.max(bootstrap);
        }
        if let Some(hint) = difficulty_hint {
            difficulty = clamp_login_difficulty_hint(
                hint,
                difficulty,
                min_diff,
                max_diff,
                should_allow_login_difficulty_hint_raise(address),
            );
        }
        if let Some(bootstrap) = address_bootstrap {
            difficulty = difficulty.max(bootstrap);
        }
        difficulty = difficulty.max(login_difficulty_floor(address, initial_difficulty));

        let mut sessions = self.sessions.lock();
        sessions.insert(
            conn_id.to_string(),
            MinerSession {
                address: address.to_string(),
                worker: worker.clone(),
                difficulty,
                protocol_version,
                capabilities: cap_set,
                accepted_share_times: VecDeque::new(),
                last_accepted_share_at: None,
                last_difficulty_adjustment: None,
                last_difficulty_hint_write: None,
            },
        );
        drop(sessions);

        if self.cfg.enable_vardiff && should_persist_login_hint_immediately(address) {
            if let Err(err) = self
                .store
                .upsert_vardiff_hint(address, &worker, difficulty, now_wall)
            {
                tracing::warn!(
                    address = %address,
                    worker = %worker,
                    error = %err,
                    "failed to persist repaired login vardiff hint"
                );
            }
        }

        Ok(build_login_result(
            protocol_version,
            self.cfg.stratum_submit_v2_required,
        ))
    }

    pub fn disconnect(&self, conn_id: &str) {
        let session = self.sessions.lock().remove(conn_id);
        if let Some(session) = session {
            if let Err(err) = self.store.upsert_vardiff_hint(
                &session.address,
                &session.worker,
                session.difficulty.max(1),
                SystemTime::now(),
            ) {
                tracing::warn!(
                    address = %session.address,
                    worker = %session.worker,
                    error = %err,
                    "failed to persist vardiff hint on disconnect"
                );
            }
        }
    }

    pub fn submit(
        &self,
        conn_id: &str,
        job_id: String,
        nonce: u64,
        claimed_hash_hex: Option<String>,
    ) -> Result<SubmitAck> {
        self.submit_with_received_at(conn_id, job_id, nonce, claimed_hash_hex, Instant::now())
    }

    pub fn submit_with_received_at(
        &self,
        conn_id: &str,
        job_id: String,
        nonce: u64,
        claimed_hash_hex: Option<String>,
        received_at: Instant,
    ) -> Result<SubmitAck> {
        let session = {
            let sessions = self.sessions.lock();
            sessions
                .get(conn_id)
                .cloned()
                .ok_or_else(|| anyhow!("not logged in"))?
        };
        let fallback_difficulty = session.difficulty.max(1);

        let submit_job = match self
            .jobs
            .resolve_submit_job_with_reason(&job_id, received_at)
        {
            Ok(v) => v,
            Err(err) => {
                let err_text = err.to_string();
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    fallback_difficulty,
                    &err_text,
                );
                return Err(anyhow!(err_text));
            }
        };
        let share_difficulty = submit_job
            .share_difficulty
            .unwrap_or(session.difficulty)
            .max(1);
        if let Some(expected_miner) = submit_job.assigned_miner.as_ref() {
            if expected_miner != &session.address {
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    "job not assigned to this miner",
                );
                return Err(anyhow!("job not assigned to this miner"));
            }
        }
        if let (Some(start), Some(end)) = (submit_job.nonce_start, submit_job.nonce_end) {
            if nonce < start || nonce > end {
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    "nonce out of assigned range",
                );
                return Err(anyhow!("nonce out of assigned range"));
            }
        }
        let job = submit_job.job;

        let seen_key = (job_id.clone(), nonce);
        {
            let mut seen = self.seen_in_memory.lock();
            if !seen.insert(seen_key.clone()) {
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    "duplicate share",
                );
                return Err(anyhow!("duplicate share"));
            }
        }

        let claimed_hash = match claimed_hash_hex {
            Some(ref value) if !value.trim().is_empty() => match parse_hash_hex(value) {
                Ok(hash) => Some(hash),
                Err(err) => {
                    self.seen_in_memory.lock().remove(&seen_key);
                    self.persist_rejected_share_for_error(
                        &session,
                        &job_id,
                        nonce,
                        share_difficulty,
                        &err,
                    );
                    return Err(anyhow!(err));
                }
            },
            _ => None,
        };

        let require_claimed_hash = self.cfg.stratum_submit_v2_required;
        if require_claimed_hash && claimed_hash.is_none() {
            self.seen_in_memory.lock().remove(&seen_key);
            self.persist_rejected_share_for_error(
                &session,
                &job_id,
                nonce,
                share_difficulty,
                "claimed hash required",
            );
            return Err(anyhow!("claimed hash required"));
        }

        let claimed = match self.store.try_claim_share(&job_id, nonce) {
            Ok(true) => true,
            Ok(false) => {
                self.seen_in_memory.lock().remove(&seen_key);
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    "duplicate share",
                );
                return Err(anyhow!("duplicate share"));
            }
            Err(err) => {
                self.seen_in_memory.lock().remove(&seen_key);
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    &err.to_string(),
                );
                return Err(err);
            }
        };
        let job_id_for_share = job_id.clone();
        let mut release_claim_on_error = true;

        let result = (|| -> Result<SubmitAck> {
            let now_wall = SystemTime::now();
            if let Some(state) =
                active_quarantine_state(self.store.as_ref(), &session.address, now_wall)?
            {
                let message = quarantine_error_message(Some(&state), now_wall);
                release_claim_on_error = false;
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id_for_share,
                    nonce,
                    share_difficulty,
                    &message,
                );
                return Err(anyhow!(message));
            }

            let share_target = difficulty_to_target(share_difficulty);

            let candidate_claim = claimed_hash
                .map(|hash| check_target(hash, job.network_target))
                .unwrap_or(false);

            let risk_force_verify = self.store.should_force_verify_address(&session.address)?;
            let plan = self.validation.plan_regular_submit(&session.address);
            let sync_full_verify = candidate_claim
                || risk_force_verify
                || (!require_claimed_hash && claimed_hash.is_none())
                || plan.sync_full_verify;

            if sync_full_verify {
                let task = ValidationTask {
                    address: session.address.clone(),
                    nonce,
                    difficulty: share_difficulty,
                    header_base: job.header_base.clone(),
                    share_target,
                    network_target: job.network_target,
                    claimed_hash,
                    candidate_claim,
                    force_full_verify: true,
                };

                let validation = match self.validate_task(task, candidate_claim) {
                    Ok(v) => v,
                    Err(err) => {
                        self.persist_rejected_share_for_error(
                            &session,
                            &job_id_for_share,
                            nonce,
                            share_difficulty,
                            &err.to_string(),
                        );
                        return Err(err);
                    }
                };

                if !validation.accepted {
                    release_claim_on_error = false;
                    let reason = validation.reject_reason.unwrap_or("invalid share");
                    self.persist_rejected_share(
                        &session,
                        &job_id_for_share,
                        nonce,
                        share_difficulty,
                        Some(reason),
                    );
                    if validation.followup_action == ValidationFollowupAction::Quarantine {
                        self.trigger_forced_validation_quarantine(
                            &session.address,
                            "bad share ratio during forced validation",
                        );
                    }
                    return Err(anyhow!(
                        "{}",
                        validation.reject_reason.unwrap_or("invalid share")
                    ));
                }

                let mut block_hash = None;
                let mut block_accepted = false;
                if validation.is_block_candidate {
                    let computed_hash = hex_string(validation.hash);
                    block_hash = Some(computed_hash.clone());
                    let staged_found = FoundBlockRecord {
                        height: job.height,
                        hash: computed_hash.clone(),
                        difficulty: job.network_difficulty,
                        reward: job_template_reward(&job),
                        finder: session.address.clone(),
                        finder_worker: session.worker.clone(),
                        timestamp: SystemTime::now(),
                    };
                    self.stage_found_block_submission(&staged_found)?;

                    let submit = self.node.submit_block(&job, nonce)?;
                    block_accepted = submit.accepted;
                    if let Some(network_hash) = submit.hash.clone() {
                        block_hash = Some(network_hash);
                    }

                    if block_accepted {
                        let mut persisted_found = staged_found.clone();
                        persisted_found.height = submit.height.unwrap_or(staged_found.height);
                        if let Some(network_hash) = submit.hash {
                            persisted_found.hash = network_hash;
                        }
                        let accepted_hash = block_hash
                            .clone()
                            .unwrap_or_else(|| persisted_found.hash.clone());
                        tracing::warn!(
                            height = persisted_found.height,
                            hash = %accepted_hash,
                            finder = %session.address,
                            worker = %session.worker,
                            difficulty = job.network_difficulty,
                            nonce,
                            "POOL BLOCK FOUND"
                        );
                        self.persist_found_block(&staged_found, persisted_found);
                    } else if let Err(err) = self.clear_found_block_submission(&staged_found) {
                        tracing::warn!(
                            height = staged_found.height,
                            hash = %staged_found.hash,
                            finder = %staged_found.finder,
                            error = %err,
                            "failed clearing rejected block submission journal entry"
                        );
                    }
                }

                let created_at = SystemTime::now();
                self.store.add_share_with_replay(
                    ShareRecord {
                        job_id: job_id_for_share.clone(),
                        miner: session.address.clone(),
                        worker: session.worker.clone(),
                        difficulty: share_difficulty,
                        nonce,
                        status: SHARE_STATUS_VERIFIED,
                        was_sampled: true,
                        block_hash,
                        claimed_hash: claimed_hash_hex.clone(),
                        reject_reason: None,
                        created_at,
                    },
                    Some(ShareReplayData {
                        job_id: job_id_for_share.clone(),
                        header_base: job.header_base.clone(),
                        network_target: job.network_target,
                        created_at,
                    }),
                )?;
                if let Err(err) = self.store.release_share_claim(&job_id_for_share, nonce) {
                    tracing::warn!(
                        job_id = %job_id_for_share,
                        nonce,
                        error = %err,
                        "failed to release share claim after successful persistence"
                    );
                }
                if validation.followup_action == ValidationFollowupAction::Quarantine {
                    self.trigger_forced_validation_quarantine(
                        &session.address,
                        "bad share ratio during forced validation",
                    );
                }

                let next_difficulty = self.note_share_and_maybe_adjust_difficulty(conn_id);
                return Ok(SubmitAck {
                    accepted: true,
                    verified: true,
                    status: SHARE_STATUS_VERIFIED,
                    block_accepted,
                    share_difficulty,
                    next_difficulty,
                });
            }

            let claimed_hash = match claimed_hash {
                Some(hash) => hash,
                None => {
                    self.persist_rejected_share_for_error(
                        &session,
                        &job_id_for_share,
                        nonce,
                        share_difficulty,
                        "claimed hash required",
                    );
                    return Err(anyhow!("claimed hash required"));
                }
            };
            if !check_target(claimed_hash, share_target) {
                release_claim_on_error = false;
                self.persist_rejected_share(
                    &session,
                    &job_id_for_share,
                    nonce,
                    share_difficulty,
                    Some("low difficulty share"),
                );
                return Err(anyhow!("low difficulty share"));
            }

            let created_at = SystemTime::now();
            let share_id = self.store.add_share_with_replay_and_id(
                ShareRecord {
                    job_id: job_id_for_share.clone(),
                    miner: session.address.clone(),
                    worker: session.worker.clone(),
                    difficulty: share_difficulty,
                    nonce,
                    status: SHARE_STATUS_PROVISIONAL,
                    was_sampled: false,
                    block_hash: None,
                    claimed_hash: claimed_hash_hex.clone(),
                    reject_reason: None,
                    created_at,
                },
                Some(ShareReplayData {
                    job_id: job_id_for_share.clone(),
                    header_base: job.header_base.clone(),
                    network_target: job.network_target,
                    created_at,
                }),
            )?;
            if let Err(err) = self.store.release_share_claim(&job_id_for_share, nonce) {
                tracing::warn!(
                    job_id = %job_id_for_share,
                    nonce,
                    error = %err,
                    "failed to release share claim after provisional persistence"
                );
            }

            self.validation.record_hot_accept(
                &session.address,
                share_id,
                share_difficulty,
                created_at,
                plan.enqueue_audit.then_some(PendingAuditShare {
                    share_id,
                    job_id: job_id_for_share.clone(),
                    miner: session.address.clone(),
                    worker: session.worker.clone(),
                    difficulty: share_difficulty,
                    nonce,
                    claimed_hash: Some(claimed_hash),
                    header_base: job.header_base.clone(),
                    network_target: job.network_target,
                    created_at,
                }),
                plan,
            );

            let next_difficulty = self.note_share_and_maybe_adjust_difficulty(conn_id);
            Ok(SubmitAck {
                accepted: true,
                verified: false,
                status: SHARE_STATUS_PROVISIONAL,
                block_accepted: false,
                share_difficulty,
                next_difficulty,
            })
        })();

        if claimed && result.is_err() && release_claim_on_error {
            if let Err(err) = self.store.release_share_claim(&job_id, nonce) {
                tracing::warn!(
                    job_id = %job_id,
                    nonce,
                    error = %err,
                    "failed to release share claim after submit failure"
                );
            }
        }

        self.seen_in_memory.lock().remove(&seen_key);
        result
    }

    fn trigger_forced_validation_quarantine(&self, address: &str, reason: &str) {
        if let Err(err) = self.store.escalate_address_risk(
            address,
            reason,
            self.cfg.invalid_escalation_window_duration(),
            self.cfg.invalid_escalation_quarantine_strikes.max(0) as u64,
            self.cfg.quarantine_duration_duration(),
            self.cfg.max_quarantine_duration_duration(),
            Duration::from_secs(0),
        ) {
            tracing::warn!(
                address = %address,
                error = %err,
                "failed to persist forced-validation quarantine"
            );
            return;
        }

        match self.store.address_risk_state(address) {
            Ok(Some(state)) => {
                if let Some(quarantined_until) = state
                    .quarantined_until
                    .filter(|until| *until > SystemTime::now())
                {
                    self.validation
                        .schedule_forced_review_after(address, quarantined_until);
                }
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    address = %address,
                    error = %err,
                    "failed to load quarantine state after forced-validation quarantine"
                );
            }
        }
    }

    fn persist_rejected_share(
        &self,
        session: &MinerSession,
        job_id: &str,
        nonce: u64,
        difficulty: u64,
        reject_reason: Option<&str>,
    ) {
        if let Err(err) = self.store.add_share(ShareRecord {
            job_id: job_id.to_string(),
            miner: session.address.clone(),
            worker: session.worker.clone(),
            difficulty: difficulty.max(1),
            nonce,
            status: SHARE_STATUS_REJECTED,
            was_sampled: false,
            block_hash: None,
            claimed_hash: None,
            reject_reason: reject_reason
                .map(|value| canonical_share_reject_reason(value).to_string()),
            created_at: SystemTime::now(),
        }) {
            tracing::warn!(
                address = %session.address,
                worker = %session.worker,
                job_id = %job_id,
                nonce,
                error = %err,
                "failed to persist rejected share"
            );
            return;
        }
        if let Err(err) = self.store.release_share_claim(job_id, nonce) {
            tracing::warn!(
                job_id = %job_id,
                nonce,
                error = %err,
                "failed to release share claim after rejected persistence"
            );
        }
    }

    fn persist_rejected_share_for_error(
        &self,
        session: &MinerSession,
        job_id: &str,
        nonce: u64,
        difficulty: u64,
        error: &str,
    ) {
        self.persist_rejected_share(session, job_id, nonce, difficulty, Some(error));
    }

    pub fn recover_found_block_outbox(&self) {
        let path = found_block_outbox_path();
        let _guard = self.found_block_outbox_lock.lock();
        self.recover_found_block_outbox_locked(&path);
    }

    fn stage_found_block_submission(&self, found: &FoundBlockRecord) -> Result<()> {
        let path = found_block_outbox_path();
        let _guard = self.found_block_outbox_lock.lock();
        append_found_block_outbox_record(&path, found).with_context(|| {
            format!(
                "failed to stage block submission recovery record at {}",
                path.display()
            )
        })
    }

    fn persist_found_block(&self, staged: &FoundBlockRecord, found: FoundBlockRecord) {
        let mut last_error = String::new();
        for attempt in 1..=FOUND_BLOCK_PERSIST_MAX_RETRIES {
            match self.store.add_found_block(found.clone()) {
                Ok(()) => {
                    if let Err(err) = self.clear_found_block_submission(staged) {
                        tracing::warn!(
                            height = staged.height,
                            hash = %staged.hash,
                            finder = %staged.finder,
                            error = %err,
                            "failed clearing persisted block recovery journal entry"
                        );
                    }
                    return;
                }
                Err(err) => {
                    last_error = err.to_string();
                    tracing::warn!(
                        height = found.height,
                        hash = %found.hash,
                        finder = %found.finder,
                        attempt,
                        error = %err,
                        "failed to persist accepted block"
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(100 * attempt as u64));
        }

        tracing::error!(
            height = found.height,
            hash = %found.hash,
            finder = %found.finder,
            attempts = FOUND_BLOCK_PERSIST_MAX_RETRIES,
            error = %last_error,
            "accepted block left in recovery journal after failed persistence attempts"
        );
    }

    fn clear_found_block_submission(&self, found: &FoundBlockRecord) -> Result<()> {
        let primary_path = found_block_outbox_path();
        let _guard = self.found_block_outbox_lock.lock();
        clear_found_block_outbox_record(&primary_path, found)
    }

    fn recover_found_block_outbox_locked(&self, path: &PathBuf) {
        let file = match OpenOptions::new().read(true).open(path) {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to open found-block recovery journal"
                );
                return;
            }
        };

        let chain_height = match self.node.current_chain_height() {
            Ok(height) => height,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to query daemon before replaying block recovery journal"
                );
                return;
            }
        };

        let reader = BufReader::new(file);
        let mut retained = Vec::<String>::new();
        let mut recovered = 0u64;
        let mut dropped = 0u64;

        for (idx, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        line = idx + 1,
                        error = %err,
                        "failed reading found-block recovery journal line"
                    );
                    continue;
                }
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let record: FoundBlockOutboxRecord = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        line = idx + 1,
                        error = %err,
                        "invalid found-block recovery journal record"
                    );
                    retained.push(trimmed.to_string());
                    continue;
                }
            };

            let found = FoundBlockRecord {
                height: record.height,
                hash: record.hash,
                difficulty: record.difficulty,
                reward: record.reward,
                finder: record.finder,
                finder_worker: record.finder_worker,
                timestamp: unix_to_system_time(record.timestamp_unix),
            };

            if chain_height < found.height {
                retained.push(trimmed.to_string());
                continue;
            }

            match self.node.block_hash_at_height(found.height) {
                Ok(Some(hash)) if hash.eq_ignore_ascii_case(&found.hash) => {
                    match self.store.add_found_block(found) {
                        Ok(()) => {
                            recovered = recovered.saturating_add(1);
                        }
                        Err(err) => {
                            tracing::warn!(
                                path = %path.display(),
                                line = idx + 1,
                                error = %err,
                                "failed replaying accepted block from recovery journal"
                            );
                            retained.push(trimmed.to_string());
                        }
                    }
                }
                Ok(Some(hash)) => {
                    dropped = dropped.saturating_add(1);
                    tracing::info!(
                        path = %path.display(),
                        line = idx + 1,
                        height = found.height,
                        expected_hash = %found.hash,
                        chain_hash = %hash,
                        "dropping stale block recovery journal entry after chain mismatch"
                    );
                }
                Ok(None) => {
                    dropped = dropped.saturating_add(1);
                    tracing::info!(
                        path = %path.display(),
                        line = idx + 1,
                        height = found.height,
                        expected_hash = %found.hash,
                        "dropping stale block recovery journal entry after chain advanced past height"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        line = idx + 1,
                        error = %err,
                        "failed checking candidate block in recovery journal against daemon"
                    );
                    retained.push(trimmed.to_string());
                }
            }
        }

        if retained.is_empty() {
            if let Err(err) = fs::remove_file(path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        path = %path.display(),
                        error = %err,
                        "failed removing drained block recovery journal"
                    );
                }
            }
        } else {
            let mut data = retained.join("\n");
            data.push('\n');
            if let Err(err) = fs::write(path, data.as_bytes()) {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed rewriting block recovery journal"
                );
            }
        }

        if recovered > 0 || dropped > 0 {
            tracing::info!(
                path = %path.display(),
                recovered,
                dropped,
                "replayed block recovery journal"
            );
        }
    }

    fn validate_task(&self, task: ValidationTask, candidate: bool) -> Result<ValidationResult> {
        if let Some(rx) = self.validation.submit(task.clone(), candidate) {
            let timeout = self.cfg.validation_wait_timeout_duration();
            let computed = rx
                .recv_timeout(timeout)
                .map_err(|_| anyhow!("validation timeout"));
            return computed.map(|computed| self.validation.complete_result(&task, computed));
        }

        if candidate {
            return Ok(self.validation.process_inline(task));
        }

        Err(anyhow!("server busy, retry"))
    }

    pub fn session_protocol_version(&self, conn_id: &str) -> Option<u32> {
        self.sessions
            .lock()
            .get(conn_id)
            .map(|session| session.protocol_version)
    }

    pub fn session_difficulty(&self, conn_id: &str) -> Option<u64> {
        self.sessions
            .lock()
            .get(conn_id)
            .map(|session| session.difficulty)
    }

    pub fn retarget_on_job_if_needed(&self, conn_id: &str) -> Option<u64> {
        if !self.cfg.enable_vardiff {
            return self.session_difficulty(conn_id);
        }

        let mut hint_to_write: Option<(String, String, u64)> = None;
        let mut sessions = self.sessions.lock();
        let session = sessions.get_mut(conn_id)?;
        let min_diff = self.cfg.min_share_difficulty.max(1);
        let max_diff = self.cfg.max_share_difficulty.max(min_diff);
        session.difficulty = session.difficulty.clamp(min_diff, max_diff);

        // Keep dev-fee sessions on submit-only retargeting so their long idle
        // periods do not collapse difficulty between the brief fee windows.
        if !is_seine_dev_fee_address(&session.address) {
            let now = Instant::now();
            let target_interval = vardiff_target_interval_seconds(&self.cfg);
            let tolerance = self.cfg.vardiff_tolerance.clamp(0.01, 0.95);
            if let Some(last) = session.last_accepted_share_at {
                let observed_interval = now.duration_since(last).as_secs_f64();
                let upper = target_interval * (1.0 + tolerance);
                if observed_interval > upper {
                    let raw_ratio = observed_interval / target_interval;
                    let ratio = raw_ratio
                        .powf(VARDIFF_RATIO_DAMPING_EXPONENT)
                        .clamp(VARDIFF_MIN_ADJUSTMENT_FACTOR, VARDIFF_MAX_ADJUSTMENT_FACTOR);
                    let next_diff = ((session.difficulty as f64) / ratio).floor().max(1.0) as u64;
                    let next_diff = next_diff.clamp(min_diff, max_diff);
                    if next_diff != session.difficulty
                        && vardiff_change_is_material(&self.cfg, session.difficulty, next_diff)
                    {
                        let retarget_interval =
                            vardiff_retarget_interval_for_direction(&self.cfg, false);
                        let can_retarget = session
                            .last_difficulty_adjustment
                            .is_none_or(|last| now.duration_since(last) >= retarget_interval);
                        if can_retarget {
                            let miner = compact_address(&session.address);
                            tracing::debug!(
                                "vardiff {:>4} -> {:>4} (job tick idle {:>5.1}s, target {:>4.0}s, miner {})",
                                session.difficulty,
                                next_diff,
                                observed_interval,
                                target_interval,
                                miner
                            );
                            session.difficulty = next_diff;
                            session.last_difficulty_adjustment = Some(now);
                        }
                    }
                }
            }

            let should_write_hint = session
                .last_difficulty_hint_write
                .is_none_or(|last| now.duration_since(last) >= VARDIFF_HINT_PERSIST_INTERVAL);
            if should_write_hint {
                session.last_difficulty_hint_write = Some(now);
                hint_to_write = Some((
                    session.address.clone(),
                    session.worker.clone(),
                    session.difficulty,
                ));
            }
        }

        let difficulty = session.difficulty;
        drop(sessions);

        if let Some((address, worker, difficulty)) = hint_to_write {
            if let Err(err) =
                self.store
                    .upsert_vardiff_hint(&address, &worker, difficulty, SystemTime::now())
            {
                tracing::warn!(
                    address = %address,
                    worker = %worker,
                    error = %err,
                    "failed to persist vardiff hint on job tick"
                );
            }
        }

        Some(difficulty)
    }

    pub fn session_capabilities(&self, conn_id: &str) -> Option<Vec<String>> {
        self.sessions.lock().get(conn_id).map(|session| {
            session
                .capabilities
                .iter()
                .cloned()
                .collect::<Vec<String>>()
        })
    }

    pub fn session_supports_capability(&self, conn_id: &str, capability: &str) -> bool {
        let probe = capability.trim().to_ascii_lowercase();
        self.sessions
            .lock()
            .get(conn_id)
            .is_some_and(|session| session.capabilities.contains(&probe))
    }

    fn note_share_and_maybe_adjust_difficulty(&self, conn_id: &str) -> u64 {
        if !self.cfg.enable_vardiff {
            return self
                .sessions
                .lock()
                .get(conn_id)
                .map(|session| session.difficulty)
                .unwrap_or(self.cfg.initial_share_difficulty);
        }

        let mut hint_to_write: Option<(String, String, u64)> = None;
        let mut sessions = self.sessions.lock();
        let Some(session) = sessions.get_mut(conn_id) else {
            return self.cfg.initial_share_difficulty;
        };

        let min_diff = self.cfg.min_share_difficulty.max(1);
        let max_diff = self.cfg.max_share_difficulty.max(min_diff);
        session.difficulty = session.difficulty.clamp(min_diff, max_diff);

        let now = Instant::now();
        let window = self
            .cfg
            .vardiff_window_duration()
            .max(Duration::from_secs(30));
        let target_interval = vardiff_target_interval_seconds(&self.cfg);
        let tolerance = self.cfg.vardiff_tolerance.clamp(0.01, 0.95);

        let last_share_age = session
            .last_accepted_share_at
            .map(|last| now.duration_since(last));
        session.last_accepted_share_at = Some(now);
        session.accepted_share_times.push_back(now);
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while session
            .accepted_share_times
            .front()
            .is_some_and(|ts| *ts < cutoff)
        {
            session.accepted_share_times.pop_front();
        }

        if session.accepted_share_times.len() >= VARDIFF_MIN_SAMPLE_COUNT {
            if let Some(oldest) = session.accepted_share_times.front().copied() {
                let elapsed = now.duration_since(oldest).as_secs_f64();
                if elapsed >= 1.0 {
                    let observed_interval =
                        elapsed / ((session.accepted_share_times.len() - 1) as f64);
                    let lower = target_interval * (1.0 - tolerance);
                    let upper = target_interval * (1.0 + tolerance);

                    let mut next_diff = session.difficulty;
                    if observed_interval < lower {
                        let raw_ratio = target_interval / observed_interval;
                        let ratio = raw_ratio
                            .powf(VARDIFF_RATIO_DAMPING_EXPONENT)
                            .clamp(VARDIFF_MIN_ADJUSTMENT_FACTOR, VARDIFF_MAX_ADJUSTMENT_FACTOR);
                        next_diff = ((session.difficulty as f64) * ratio).ceil() as u64;
                    } else if observed_interval > upper {
                        let raw_ratio = observed_interval / target_interval;
                        let ratio = raw_ratio
                            .powf(VARDIFF_RATIO_DAMPING_EXPONENT)
                            .clamp(VARDIFF_MIN_ADJUSTMENT_FACTOR, VARDIFF_MAX_ADJUSTMENT_FACTOR);
                        next_diff = ((session.difficulty as f64) / ratio).floor() as u64;
                    }

                    next_diff = next_diff.clamp(min_diff, max_diff);
                    if next_diff != session.difficulty
                        && vardiff_change_is_material(&self.cfg, session.difficulty, next_diff)
                    {
                        let increasing = next_diff > session.difficulty;
                        let retarget_interval =
                            vardiff_retarget_interval_for_direction(&self.cfg, increasing);
                        let can_retarget = session
                            .last_difficulty_adjustment
                            .is_none_or(|last| now.duration_since(last) >= retarget_interval);
                        if can_retarget {
                            let miner = compact_address(&session.address);
                            tracing::debug!(
                                "vardiff {:>4} -> {:>4} (observed {:>5.1}s, target {:>4.0}s, miner {})",
                                session.difficulty,
                                next_diff,
                                observed_interval,
                                target_interval,
                                miner
                            );
                            session.difficulty = next_diff;
                            session.last_difficulty_adjustment = Some(now);
                        }
                    }
                }
            }
        } else {
            // If the previous accepted share fell out of the vardiff window, we can still
            // decay difficulty based on idle time before this accepted share — but only
            // if the gap is within the window. Gaps longer than the window indicate an
            // intermittent session (e.g. dev fee), not a struggling miner.
            if let Some(age) = last_share_age {
                let observed_interval = age.as_secs_f64();
                let window_secs = window.as_secs_f64();
                let upper = target_interval * (1.0 + tolerance);
                if observed_interval <= window_secs && observed_interval > upper {
                    let raw_ratio = observed_interval / target_interval;
                    let ratio = raw_ratio
                        .powf(VARDIFF_RATIO_DAMPING_EXPONENT)
                        .clamp(VARDIFF_MIN_ADJUSTMENT_FACTOR, VARDIFF_MAX_ADJUSTMENT_FACTOR);
                    let next_diff = ((session.difficulty as f64) / ratio).floor().max(1.0) as u64;
                    let next_diff = next_diff.clamp(min_diff, max_diff);
                    if next_diff != session.difficulty
                        && vardiff_change_is_material(&self.cfg, session.difficulty, next_diff)
                    {
                        let retarget_interval =
                            vardiff_retarget_interval_for_direction(&self.cfg, false);
                        let can_retarget = session
                            .last_difficulty_adjustment
                            .is_none_or(|last| now.duration_since(last) >= retarget_interval);
                        if can_retarget {
                            let miner = compact_address(&session.address);
                            tracing::debug!(
                                "vardiff {:>4} -> {:>4} (idle {:>5.1}s, target {:>4.0}s, miner {})",
                                session.difficulty,
                                next_diff,
                                observed_interval,
                                target_interval,
                                miner
                            );
                            session.difficulty = next_diff;
                            session.last_difficulty_adjustment = Some(now);
                        }
                    }
                }
            }
        }

        let should_write_hint = session
            .last_difficulty_hint_write
            .is_none_or(|last| now.duration_since(last) >= VARDIFF_HINT_PERSIST_INTERVAL);
        if should_write_hint {
            session.last_difficulty_hint_write = Some(now);
            hint_to_write = Some((
                session.address.clone(),
                session.worker.clone(),
                session.difficulty,
            ));
        }
        let difficulty = session.difficulty;
        drop(sessions);

        if let Some((address, worker, difficulty)) = hint_to_write {
            if let Err(err) =
                self.store
                    .upsert_vardiff_hint(&address, &worker, difficulty, SystemTime::now())
            {
                tracing::warn!(
                    address = %address,
                    worker = %worker,
                    error = %err,
                    "failed to persist vardiff hint"
                );
            }
        }

        difficulty
    }
}

fn derive_vardiff_login_difficulty(
    cached_difficulty: u64,
    age: Duration,
    initial_difficulty: u64,
    min_diff: u64,
    max_diff: u64,
) -> Option<u64> {
    if age > VARDIFF_HINT_MAX_AGE {
        return None;
    }

    let cached = cached_difficulty.clamp(min_diff, max_diff);
    let initial = initial_difficulty.clamp(min_diff, max_diff);
    if age <= VARDIFF_HINT_DECAY_START {
        return Some(cached);
    }

    let decay_span = VARDIFF_HINT_MAX_AGE.saturating_sub(VARDIFF_HINT_DECAY_START);
    if decay_span.is_zero() {
        return Some(cached);
    }
    let decay_age = age.saturating_sub(VARDIFF_HINT_DECAY_START).min(decay_span);
    let ratio = decay_age.as_secs_f64() / decay_span.as_secs_f64();
    let blended = (cached as f64) * (1.0 - ratio) + (initial as f64) * ratio;
    Some((blended.round() as u64).clamp(min_diff, max_diff))
}

fn derive_address_vardiff_login_difficulty(
    hints: Vec<(u64, SystemTime)>,
    now_wall: SystemTime,
    initial_difficulty: u64,
    min_diff: u64,
    max_diff: u64,
) -> Option<u64> {
    let initial = initial_difficulty.clamp(min_diff, max_diff);
    let mut derived = hints
        .into_iter()
        .filter_map(|(cached, updated_at)| {
            let age = now_wall.duration_since(updated_at).unwrap_or_default();
            derive_vardiff_login_difficulty(cached, age, initial, min_diff, max_diff)
        })
        .collect::<Vec<_>>();
    if derived.is_empty() {
        return None;
    }

    let mut informative = derived
        .iter()
        .copied()
        .filter(|difficulty| *difficulty > initial)
        .collect::<Vec<_>>();
    if !informative.is_empty() {
        derived = std::mem::take(&mut informative);
    }

    Some(median_difficulty(&mut derived))
}

fn median_difficulty(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        values[mid - 1].saturating_add(values[mid]) / 2
    }
}

fn clamp_login_difficulty_hint(
    hint: u64,
    baseline: u64,
    min_diff: u64,
    max_diff: u64,
    allow_raise: bool,
) -> u64 {
    let baseline = baseline.max(1).clamp(min_diff, max_diff);
    let hint = hint.max(1).clamp(min_diff, max_diff);

    let lower = ((baseline as f64) * LOGIN_DIFFICULTY_HINT_MIN_FACTOR)
        .floor()
        .max(1.0) as u64;
    let upper = if allow_raise {
        ((baseline as f64) * LOGIN_DIFFICULTY_HINT_MAX_FACTOR)
            .ceil()
            .max(lower as f64) as u64
    } else {
        baseline.max(lower)
    };

    let bounded_lower = lower.max(min_diff);
    let bounded_upper = upper.min(max_diff).max(bounded_lower);
    hint.clamp(bounded_lower, bounded_upper)
}

fn vardiff_target_interval_seconds(cfg: &Config) -> f64 {
    let window = cfg.vardiff_window_duration().max(Duration::from_secs(30));
    let target_shares = cfg.vardiff_target_shares.max(1) as f64;
    (window.as_secs_f64() / target_shares).max(1.0)
}

fn vardiff_retarget_interval_for_direction(cfg: &Config, increasing: bool) -> Duration {
    let increase = cfg
        .vardiff_retarget_interval_duration()
        .clamp(Duration::from_secs(2), Duration::from_secs(8));
    if increasing {
        increase
    } else {
        cfg.vardiff_decrease_retarget_interval_duration()
            .clamp(increase, Duration::from_secs(60))
    }
}

fn vardiff_change_is_material(cfg: &Config, current: u64, next: u64) -> bool {
    if current == 0 || current == next {
        return false;
    }
    let delta = current.abs_diff(next) as f64;
    let baseline = current.max(1) as f64;
    delta / baseline >= cfg.vardiff_min_change_pct
}

fn compact_address(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.len() <= 12 {
        return trimmed.to_string();
    }
    format!("{}...{}", &trimmed[..6], &trimmed[trimmed.len() - 6..])
}

fn hex_string(hash: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

fn job_template_reward(job: &Job) -> u64 {
    fn as_u64(value: &serde_json::Value) -> Option<u64> {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| u64::try_from(v).ok()))
            .or_else(|| value.as_str().and_then(|v| v.trim().parse::<u64>().ok()))
    }

    let Some(block) = job.full_block.as_ref() else {
        return 0;
    };

    block
        .get("reward")
        .or_else(|| block.get("Reward"))
        .and_then(as_u64)
        .or_else(|| {
            block
                .get("header")
                .and_then(|header| header.get("reward").or_else(|| header.get("Reward")))
                .and_then(as_u64)
        })
        .unwrap_or(0)
}

fn found_block_outbox_path() -> PathBuf {
    let configured = env::var(FOUND_BLOCK_OUTBOX_ENV)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(path) = configured {
        return PathBuf::from(path);
    }
    PathBuf::from(FOUND_BLOCK_OUTBOX_DEFAULT_PATH)
}

fn system_time_to_unix(ts: SystemTime) -> i64 {
    i64::try_from(
        ts.duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
    .unwrap_or(i64::MAX)
}

fn unix_to_system_time(ts: i64) -> SystemTime {
    if ts <= 0 {
        return SystemTime::UNIX_EPOCH;
    }
    SystemTime::UNIX_EPOCH + Duration::from_secs(ts as u64)
}

fn quarantine_until_for_strikes(
    now: SystemTime,
    strikes: u64,
    quarantine_base: Duration,
    quarantine_max: Duration,
) -> SystemTime {
    if strikes == 0 || quarantine_base.is_zero() {
        return now;
    }

    let mut duration = quarantine_base;
    if !quarantine_max.is_zero() && duration > quarantine_max {
        duration = quarantine_max;
    }
    for _ in 1..strikes {
        duration = duration.saturating_mul(2);
        if !quarantine_max.is_zero() && duration >= quarantine_max {
            duration = quarantine_max;
            break;
        }
    }
    now + duration
}

fn active_window_strikes(strikes: u64, window_until: Option<SystemTime>, now: SystemTime) -> u64 {
    window_until
        .filter(|until| *until > now)
        .map(|_| strikes)
        .unwrap_or_default()
}

impl FoundBlockOutboxRecord {
    fn from_found(found: &FoundBlockRecord) -> Self {
        Self {
            height: found.height,
            hash: found.hash.clone(),
            difficulty: found.difficulty,
            reward: found.reward,
            finder: found.finder.clone(),
            finder_worker: found.finder_worker.clone(),
            timestamp_unix: system_time_to_unix(found.timestamp),
        }
    }

    fn matches_found(&self, found: &FoundBlockRecord) -> bool {
        self.height == found.height
            && self.hash.eq_ignore_ascii_case(&found.hash)
            && self.difficulty == found.difficulty
            && self.reward == found.reward
            && self.finder == found.finder
            && self.finder_worker == found.finder_worker
            && self.timestamp_unix == system_time_to_unix(found.timestamp)
    }
}

fn append_found_block_outbox_record(path: &PathBuf, found: &FoundBlockRecord) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    if let Ok(meta) = fs::symlink_metadata(path) {
        if meta.file_type().is_symlink() {
            return Err(anyhow!(
                "refusing to write found-block outbox symlink: {}",
                path.display()
            ));
        }
    }

    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    let record = FoundBlockOutboxRecord::from_found(found);
    serde_json::to_writer(&mut file, &record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn clear_found_block_outbox_record(path: &PathBuf, found: &FoundBlockRecord) -> Result<()> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };

    let reader = BufReader::new(file);
    let mut retained = Vec::<String>::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let should_drop = serde_json::from_str::<FoundBlockOutboxRecord>(trimmed)
            .map(|record| record.matches_found(found))
            .unwrap_or(false);
        if !should_drop {
            retained.push(trimmed.to_string());
        }
    }

    if retained.is_empty() {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    } else {
        let mut data = retained.join("\n");
        data.push('\n');
        fs::write(path, data.as_bytes())?;
        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryStore {
    seen: Mutex<HashSet<(String, u64)>>,
    shares: Mutex<Vec<ShareRecord>>,
    blocks: Mutex<Vec<FoundBlockRecord>>,
    address_risk: Mutex<HashMap<String, AddressRiskState>>,
    vardiff_hints: Mutex<HashMap<(String, String), (u64, SystemTime)>>,
    replays: Mutex<HashMap<String, ShareReplayData>>,
}

impl InMemoryStore {
    pub fn shares(&self) -> Vec<ShareRecord> {
        self.shares.lock().clone()
    }

    pub fn blocks(&self) -> Vec<FoundBlockRecord> {
        self.blocks.lock().clone()
    }

    pub fn seed_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) {
        self.vardiff_hints.lock().insert(
            (address.to_string(), worker.to_string()),
            (difficulty.max(1), updated_at),
        );
    }

    pub fn vardiff_hint(&self, address: &str, worker: &str) -> Option<(u64, SystemTime)> {
        self.vardiff_hints
            .lock()
            .get(&(address.to_string(), worker.to_string()))
            .copied()
    }

    pub fn replay(&self, job_id: &str) -> Option<ShareReplayData> {
        self.replays.lock().get(job_id).cloned()
    }

    fn has_persisted_share(&self, job_id: &str, nonce: u64) -> bool {
        self.shares
            .lock()
            .iter()
            .any(|share| share.job_id == job_id && share.nonce == nonce)
    }
}

impl ShareStore for InMemoryStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        Ok(self.has_persisted_share(job_id, nonce)
            || self.seen.lock().contains(&(job_id.to_string(), nonce)))
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.seen.lock().insert((job_id.to_string(), nonce));
        Ok(())
    }

    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        if self.has_persisted_share(job_id, nonce) {
            return Ok(false);
        }
        let key = (job_id.to_string(), nonce);
        let mut seen = self.seen.lock();
        if seen.contains(&key) {
            return Ok(false);
        }
        seen.insert(key);
        Ok(true)
    }

    fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.seen.lock().remove(&(job_id.to_string(), nonce));
        Ok(())
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        self.shares.lock().push(share);
        Ok(())
    }

    fn add_share_with_id(&self, share: ShareRecord) -> Result<i64> {
        let mut shares = self.shares.lock();
        shares.push(share);
        Ok(shares.len() as i64)
    }

    fn add_share_with_replay(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<()> {
        if let Some(replay) = replay {
            self.replays.lock().insert(replay.job_id.clone(), replay);
        }
        self.add_share(share)
    }

    fn add_share_with_replay_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<i64> {
        if let Some(replay) = replay {
            self.replays.lock().insert(replay.job_id.clone(), replay);
        }
        self.add_share_with_id(share)
    }

    fn add_found_block(&self, block: FoundBlockRecord) -> Result<()> {
        self.blocks.lock().push(block);
        Ok(())
    }

    fn address_risk_state(&self, address: &str) -> Result<Option<AddressRiskState>> {
        Ok(self.address_risk.lock().get(address).cloned())
    }

    fn is_address_quarantined(&self, address: &str) -> Result<bool> {
        let now = SystemTime::now();
        Ok(self
            .address_risk
            .lock()
            .get(address)
            .and_then(|state| state.quarantined_until)
            .is_some_and(|until| until > now))
    }

    fn should_force_verify_address(&self, address: &str) -> Result<bool> {
        let now = SystemTime::now();
        Ok(self.address_risk.lock().get(address).is_some_and(|state| {
            state.force_verify_until.is_some_and(|until| until > now)
                || state.quarantined_until.is_some_and(|until| until > now)
        }))
    }

    fn address_risk_strikes(&self, address: &str) -> Result<u64> {
        let now = SystemTime::now();
        Ok(self
            .address_risk
            .lock()
            .get(address)
            .map(|state| active_window_strikes(state.strikes, state.strike_window_until, now))
            .unwrap_or_default())
    }

    fn escalate_address_risk(
        &self,
        address: &str,
        reason: &str,
        strike_window_duration: Duration,
        quarantine_threshold: u64,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
    ) -> Result<()> {
        let now = SystemTime::now();
        let mut risk = self.address_risk.lock();
        let state = risk
            .entry(address.to_string())
            .or_insert_with(|| AddressRiskState {
                address: address.to_string(),
                ..AddressRiskState::default()
            });
        let active_strikes = active_window_strikes(state.strikes, state.strike_window_until, now);
        state.strikes = active_strikes.saturating_add(1);
        state.last_reason = Some(reason.to_string());
        state.last_event_at = Some(now);
        state.strike_window_until = if strike_window_duration.is_zero() {
            None
        } else {
            let candidate = now + strike_window_duration;
            Some(match state.strike_window_until {
                Some(existing) if existing > candidate => existing,
                _ => candidate,
            })
        };
        if !force_verify_duration.is_zero() {
            let candidate = now + force_verify_duration;
            state.force_verify_until = Some(match state.force_verify_until {
                Some(existing) if existing > candidate => existing,
                _ => candidate,
            });
        }
        if quarantine_threshold > 0
            && state.strikes >= quarantine_threshold
            && !quarantine_base.is_zero()
        {
            let quarantine_strikes = state
                .strikes
                .saturating_sub(quarantine_threshold)
                .saturating_add(1);
            let candidate = quarantine_until_for_strikes(
                now,
                quarantine_strikes,
                quarantine_base,
                quarantine_max,
            );
            state.quarantined_until = Some(match state.quarantined_until {
                Some(existing) if existing > candidate => existing,
                _ => candidate,
            });
        }
        Ok(())
    }

    fn record_suspected_fraud(
        &self,
        address: &str,
        reason: &str,
        quarantine_threshold: u64,
        strike_window_duration: Duration,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
    ) -> Result<()> {
        let now = SystemTime::now();
        let mut risk = self.address_risk.lock();
        let state = risk
            .entry(address.to_string())
            .or_insert_with(|| AddressRiskState {
                address: address.to_string(),
                ..AddressRiskState::default()
            });
        let active_window = state
            .suspected_fraud_window_until
            .is_some_and(|until| until > now);
        state.last_reason = Some(reason.to_string());
        state.last_event_at = Some(now);
        if !force_verify_duration.is_zero() {
            let candidate = now + force_verify_duration;
            state.force_verify_until = Some(match state.force_verify_until {
                Some(existing) if existing > candidate => existing,
                _ => candidate,
            });
        }
        state.suspected_fraud_window_until = if strike_window_duration.is_zero() {
            None
        } else {
            let candidate = now + strike_window_duration;
            Some(match state.suspected_fraud_window_until {
                Some(existing) if existing > candidate => existing,
                _ => candidate,
            })
        };
        state.suspected_fraud_strikes = if active_window {
            state.suspected_fraud_strikes.saturating_add(1)
        } else {
            1
        };
        if quarantine_threshold > 0
            && state.suspected_fraud_strikes >= quarantine_threshold
            && !quarantine_base.is_zero()
        {
            let quarantine_strikes = state
                .suspected_fraud_strikes
                .saturating_sub(quarantine_threshold)
                .saturating_add(1);
            let candidate = quarantine_until_for_strikes(
                now,
                quarantine_strikes,
                quarantine_base,
                quarantine_max,
            );
            state.quarantined_until = Some(match state.quarantined_until {
                Some(existing) if existing > candidate => existing,
                _ => candidate,
            });
        }
        Ok(())
    }

    fn get_vardiff_hint(&self, address: &str, worker: &str) -> Result<Option<(u64, SystemTime)>> {
        Ok(self.vardiff_hint(address, worker))
    }

    fn clear_address_risk_history(&self, address: &str) -> Result<()> {
        self.address_risk.lock().remove(address);
        Ok(())
    }

    fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        self.seed_vardiff_hint(address, worker, difficulty, updated_at);
        Ok(())
    }

    fn get_vardiff_hints_for_address(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<(u64, SystemTime)>> {
        let mut hints = self
            .vardiff_hints
            .lock()
            .iter()
            .filter_map(|((hint_address, _), value)| (hint_address == address).then_some(*value))
            .collect::<Vec<_>>();
        hints.sort_by_key(|(_, updated_at)| {
            std::cmp::Reverse(
                updated_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default(),
            )
        });
        hints.truncate(limit);
        Ok(hints)
    }
}

#[derive(Default)]
pub struct InMemoryJobs {
    current: Mutex<Option<String>>,
    jobs: Mutex<HashMap<String, Job>>,
    assignments: Mutex<HashMap<String, InMemoryAssignment>>,
}

#[derive(Debug, Clone)]
struct InMemoryAssignment {
    template_job_id: String,
    share_difficulty: u64,
    assigned_miner: Option<String>,
    nonce_start: u64,
    nonce_end: u64,
}

impl InMemoryJobs {
    pub fn insert(&self, job: Job) {
        let id = job.id.clone();
        self.jobs.lock().insert(id.clone(), job);
        *self.current.lock() = Some(id);
    }

    pub fn insert_assignment(
        &self,
        assignment_id: impl Into<String>,
        template_job_id: impl Into<String>,
        share_difficulty: u64,
        assigned_miner: Option<String>,
        nonce_start: u64,
        nonce_end: u64,
    ) {
        self.assignments.lock().insert(
            assignment_id.into(),
            InMemoryAssignment {
                template_job_id: template_job_id.into(),
                share_difficulty: share_difficulty.max(1),
                assigned_miner,
                nonce_start,
                nonce_end,
            },
        );
    }
}

impl JobRepository for InMemoryJobs {
    fn get_job(&self, job_id: &str) -> Option<Job> {
        self.jobs.lock().get(job_id).cloned()
    }

    fn current_job(&self) -> Option<Job> {
        let current = self.current.lock().clone()?;
        self.jobs.lock().get(&current).cloned()
    }

    fn resolve_submit_job_with_reason(
        &self,
        submitted_job_id: &str,
        _submitted_at: Instant,
    ) -> std::result::Result<SubmitJobBinding, SubmitJobResolveError> {
        if let Some(assignment) = self.assignments.lock().get(submitted_job_id).cloned() {
            let job = self
                .jobs
                .lock()
                .get(&assignment.template_job_id)
                .cloned()
                .ok_or_else(|| SubmitJobResolveError::TemplateMissing {
                    template_job_id: assignment.template_job_id.clone(),
                })?;
            return Ok(SubmitJobBinding {
                job,
                share_difficulty: Some(assignment.share_difficulty),
                assigned_miner: assignment.assigned_miner,
                nonce_start: Some(assignment.nonce_start),
                nonce_end: Some(assignment.nonce_end),
            });
        }

        self.jobs
            .lock()
            .get(submitted_job_id)
            .cloned()
            .map(|job| SubmitJobBinding {
                job,
                share_difficulty: None,
                assigned_miner: None,
                nonce_start: None,
                nonce_end: None,
            })
            .ok_or(SubmitJobResolveError::AssignmentNotFound)
    }
}

#[derive(Default)]
pub struct InMemoryNode {
    pub submits: Mutex<Vec<(String, u64)>>,
    pub accepted: bool,
}

impl NodeApi for InMemoryNode {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        self.submits.lock().push((job.id.clone(), nonce));
        Ok(BlockSubmitResponse {
            accepted: self.accepted,
            hash: Some(format!("{:x}", nonce)),
            height: Some(job.height),
        })
    }

    fn current_chain_height(&self) -> Result<u64> {
        Ok(0)
    }

    fn block_hash_at_height(&self, _height: u64) -> Result<Option<String>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pow::{difficulty_to_target, DeterministicTestHasher, PowHasher};

    fn cfg() -> Config {
        Config {
            validation_mode: "probabilistic".to_string(),
            sample_rate: 0.0,
            warmup_shares: 0,
            min_sample_every: 0,
            max_verifiers: 1,
            max_validation_queue: 8,
            regular_validation_queue: 8,
            job_timeout: "10s".to_string(),
            initial_share_difficulty: 1,
            ..Config::default()
        }
    }

    fn job() -> Job {
        Job {
            id: "job1".to_string(),
            height: 100,
            header_base: vec![1, 2, 3],
            network_target: [0x0f; 32],
            network_difficulty: 1,
            template_id: Some("tmpl1".to_string()),
            full_block: None,
        }
    }

    fn submit_hash_cap() -> Vec<String> {
        vec!["submit_claimed_hash".to_string()]
    }

    fn test_miner_address() -> String {
        bs58::encode([0x11; 64]).into_string()
    }

    fn other_miner_address() -> String {
        bs58::encode([0x22; 64]).into_string()
    }

    #[test]
    fn job_template_reward_reads_numeric_reward() {
        let mut j = job();
        j.full_block = Some(serde_json::json!({"reward": 123456789u64}));
        assert_eq!(job_template_reward(&j), 123456789);
    }

    #[test]
    fn job_template_reward_reads_header_string_reward() {
        let mut j = job();
        j.full_block = Some(serde_json::json!({"header": {"Reward": "456"}}));
        assert_eq!(job_template_reward(&j), 456);
    }

    #[test]
    fn non_fraud_strikes_reset_after_window_expires() {
        let store = InMemoryStore::default();
        store
            .escalate_address_risk(
                "addr1",
                "low difficulty share",
                Duration::from_secs(60),
                3,
                Duration::from_secs(15 * 60),
                Duration::from_secs(2 * 60 * 60),
                Duration::from_secs(2 * 60 * 60),
            )
            .expect("first non-fraud event should persist");
        {
            let mut risk = store.address_risk.lock();
            let state = risk.get_mut("addr1").expect("risk state should exist");
            state.strikes = 2;
            state.strike_window_until = Some(SystemTime::now() - Duration::from_secs(1));
        }

        store
            .escalate_address_risk(
                "addr1",
                "low difficulty share",
                Duration::from_secs(60),
                3,
                Duration::from_secs(15 * 60),
                Duration::from_secs(2 * 60 * 60),
                Duration::from_secs(2 * 60 * 60),
            )
            .expect("expired risk window should reset strike count");

        let state = store
            .address_risk_state("addr1")
            .expect("read risk state")
            .expect("risk state should exist");
        assert_eq!(state.strikes, 1);
        assert!(state.quarantined_until.is_none());
    }

    #[test]
    fn first_non_fraud_quarantine_uses_base_duration_at_threshold() {
        let store = InMemoryStore::default();
        for _ in 0..3 {
            store
                .escalate_address_risk(
                    "addr1",
                    "low difficulty share",
                    Duration::from_secs(6 * 60 * 60),
                    3,
                    Duration::from_secs(15 * 60),
                    Duration::from_secs(2 * 60 * 60),
                    Duration::from_secs(2 * 60 * 60),
                )
                .expect("non-fraud event should persist");
        }

        let state = store
            .address_risk_state("addr1")
            .expect("read risk state")
            .expect("risk state should exist");
        let last_event = state.last_event_at.expect("last event timestamp");
        let quarantined_until = state.quarantined_until.expect("quarantine should start");
        assert_eq!(
            quarantined_until
                .duration_since(last_event)
                .unwrap_or_default(),
            Duration::from_secs(15 * 60)
        );
    }

    #[test]
    fn login_negotiates_and_records_session() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(cfg, validation, jobs, store, node);

        let result = engine
            .login(
                "conn1",
                test_miner_address(),
                Some("rig01".to_string()),
                2,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect("login");

        assert_eq!(result.protocol_version, 2);
        assert_eq!(engine.session_protocol_version("conn1"), Some(2));
        let caps = engine.session_capabilities("conn1").expect("caps");
        assert!(caps.iter().any(|v| v == "submit_claimed_hash"));
    }

    #[test]
    fn login_rejects_invalid_address_format() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let err = engine
            .login(
                "conn1",
                "bench_addr_e2e".to_string(),
                None,
                2,
                submit_hash_cap(),
            )
            .expect_err("invalid base58 address should be rejected at login");
        assert!(err.to_string().contains("invalid address"));
    }

    #[test]
    fn login_uses_recent_vardiff_hint() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 10;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let store = Arc::new(InMemoryStore::default());
        let address = test_miner_address();
        store.seed_vardiff_hint(
            &address,
            "rig01",
            222,
            SystemTime::now()
                .checked_sub(Duration::from_secs(30))
                .unwrap_or(SystemTime::UNIX_EPOCH),
        );

        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login(
                "conn1",
                address,
                Some("rig01".to_string()),
                2,
                submit_hash_cap(),
            )
            .expect("login");
        assert_eq!(engine.session_difficulty("conn1"), Some(222));
    }

    #[test]
    fn login_ignores_stale_vardiff_hint() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 17;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let store = Arc::new(InMemoryStore::default());
        let address = test_miner_address();
        store.seed_vardiff_hint(
            &address,
            "rig01",
            333,
            SystemTime::now()
                .checked_sub(VARDIFF_HINT_MAX_AGE + Duration::from_secs(5))
                .unwrap_or(SystemTime::UNIX_EPOCH),
        );

        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login(
                "conn1",
                address,
                Some("rig01".to_string()),
                2,
                submit_hash_cap(),
            )
            .expect("login");
        assert_eq!(engine.session_difficulty("conn1"), Some(17));
    }

    #[test]
    fn login_decays_aged_vardiff_hint_toward_initial() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 100;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let store = Arc::new(InMemoryStore::default());
        let address = test_miner_address();
        let decay_span = VARDIFF_HINT_MAX_AGE.saturating_sub(VARDIFF_HINT_DECAY_START);
        let aged_by = VARDIFF_HINT_DECAY_START + decay_span / 2;
        store.seed_vardiff_hint(
            &address,
            "rig01",
            1_000,
            SystemTime::now()
                .checked_sub(aged_by)
                .unwrap_or(SystemTime::UNIX_EPOCH),
        );

        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login(
                "conn1",
                address,
                Some("rig01".to_string()),
                2,
                submit_hash_cap(),
            )
            .expect("login");
        let effective = engine
            .session_difficulty("conn1")
            .expect("session difficulty should be set");
        assert!(effective < 1_000, "aged hint should decay downward");
        assert!(
            effective > 100,
            "decayed hint should remain above initial midpoint"
        );
    }

    #[test]
    fn login_applies_client_difficulty_hint_with_safety_bounds() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 100;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login_with_hint(
                "conn1",
                test_miner_address(),
                Some("rig01".to_string()),
                2,
                submit_hash_cap(),
                Some(10_000),
            )
            .expect("login");
        // Normal-user login hints may lower difficulty, but they should not
        // raise above the pool's own baseline anymore.
        assert_eq!(engine.session_difficulty("conn1"), Some(100));

        engine
            .login_with_hint(
                "conn2",
                test_miner_address(),
                Some("rig02".to_string()),
                2,
                submit_hash_cap(),
                Some(1),
            )
            .expect("login");
        // Baseline=100, min factor=0.25x, so hint should clamp to 25.
        assert_eq!(engine.session_difficulty("conn2"), Some(25));
    }

    #[test]
    fn dev_fee_login_allows_higher_client_difficulty_hint_with_safety_bounds() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 60;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login_with_hint(
                "conn-dev",
                crate::dev_fee::SEINE_DEV_FEE_ADDRESS.to_string(),
                Some("seine-devfee-1".to_string()),
                2,
                submit_hash_cap(),
                Some(10_000),
            )
            .expect("login");
        assert_eq!(engine.session_difficulty("conn-dev"), Some(240));
    }

    #[test]
    fn dev_fee_login_ignores_poisoned_low_cached_and_client_hints() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 60;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let store = Arc::new(InMemoryStore::default());
        store.seed_vardiff_hint(
            crate::dev_fee::SEINE_DEV_FEE_ADDRESS,
            "seine-devfee-1",
            1,
            SystemTime::now(),
        );
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login_with_hint(
                "conn-dev",
                crate::dev_fee::SEINE_DEV_FEE_ADDRESS.to_string(),
                Some("seine-devfee-1".to_string()),
                2,
                submit_hash_cap(),
                Some(1),
            )
            .expect("dev fee login");

        assert_eq!(
            engine.session_difficulty("conn-dev"),
            Some(60),
            "dev fee sessions should restart at least at the configured initial difficulty"
        );
        let persisted = store
            .vardiff_hint(crate::dev_fee::SEINE_DEV_FEE_ADDRESS, "seine-devfee-1")
            .expect("repaired dev hint should persist immediately");
        assert_eq!(persisted.0, 60);
    }

    #[test]
    fn dev_fee_login_bootstraps_from_recent_address_hints() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 60;
        cfg.min_share_difficulty = 1;
        cfg.max_share_difficulty = 10_000;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let store = Arc::new(InMemoryStore::default());
        let now = SystemTime::now();
        store.seed_vardiff_hint(
            crate::dev_fee::SEINE_DEV_FEE_ADDRESS,
            "seine-devfee-poisoned",
            1,
            now,
        );
        store.seed_vardiff_hint(
            crate::dev_fee::SEINE_DEV_FEE_ADDRESS,
            "seine-devfee-healthy-1",
            180,
            now,
        );
        store.seed_vardiff_hint(
            crate::dev_fee::SEINE_DEV_FEE_ADDRESS,
            "seine-devfee-healthy-2",
            300,
            now,
        );
        store.seed_vardiff_hint(
            crate::dev_fee::SEINE_DEV_FEE_ADDRESS,
            "seine-devfee-healthy-3",
            420,
            now,
        );
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login_with_hint(
                "conn-dev-boostrapped",
                crate::dev_fee::SEINE_DEV_FEE_ADDRESS.to_string(),
                Some("seine-devfee-new".to_string()),
                2,
                submit_hash_cap(),
                Some(1),
            )
            .expect("dev fee login");

        assert_eq!(
            engine.session_difficulty("conn-dev-boostrapped"),
            Some(300),
            "dev fee sessions should reuse recent address-level difficulty instead of warming from poisoned hints"
        );
        let persisted = store
            .vardiff_hint(crate::dev_fee::SEINE_DEV_FEE_ADDRESS, "seine-devfee-new")
            .expect("bootstrapped dev hint should persist immediately");
        assert_eq!(persisted.0, 300);
    }

    #[test]
    fn disconnect_persists_vardiff_hint_even_without_shares() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 77;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let store = Arc::new(InMemoryStore::default());
        let address = test_miner_address();
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login(
                "conn1",
                address.clone(),
                Some("rig01".to_string()),
                2,
                submit_hash_cap(),
            )
            .expect("login");
        engine.disconnect("conn1");

        let hint = store
            .vardiff_hint(&address, "rig01")
            .expect("disconnect should persist vardiff hint");
        assert_eq!(hint.0, 77);
    }

    #[test]
    fn login_rejects_old_protocol() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let err = engine
            .login("conn1", test_miner_address(), None, 1, submit_hash_cap())
            .expect_err("should reject protocol 1");
        assert!(err.to_string().contains("protocol_version"));
    }

    #[test]
    fn login_rejects_missing_submit_claimed_hash_capability() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let err = engine
            .login("conn1", test_miner_address(), None, 2, vec![])
            .expect_err("should require submit_claimed_hash capability");
        assert!(err.to_string().contains("submit_claimed_hash"));
    }

    #[test]
    fn login_allows_legacy_protocol_when_v2_not_required() {
        let mut cfg = cfg();
        cfg.stratum_submit_v2_required = false;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let result = engine
            .login("conn1", test_miner_address(), None, 1, vec![])
            .expect("legacy login should be accepted when v2 is optional");
        assert_eq!(result.protocol_version, 1);
        assert!(result.required_capabilities.is_empty());
    }

    #[test]
    fn submit_rejects_when_not_logged_in() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        let err = engine
            .submit("missing", "job1".to_string(), 1, Some("00".repeat(32)))
            .expect_err("must fail");
        assert!(err.to_string().contains("not logged"));
    }

    #[test]
    fn submit_returns_explicit_stale_reason_when_assignment_is_missing() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        let err = engine
            .submit(
                "conn1",
                "nonexistent-assignment".to_string(),
                11,
                Some("ff".repeat(32)),
            )
            .expect_err("missing assignment must return stale reason");
        assert!(err.to_string().contains("assignment not found"));
        let shares = store.shares();
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].status, SHARE_STATUS_REJECTED);
    }

    #[test]
    fn submit_accepts_valid_share_and_records_it() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::clone(&jobs) as Arc<dyn JobRepository>,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        // In probabilistic mode with sample_rate=0 and no forced full verify, claimed hash drives acceptance.
        let ack = engine
            .submit("conn1", "job1".to_string(), 42, Some("ff".repeat(32)))
            .expect("share should be accepted");

        assert!(ack.accepted);
        assert_eq!(ack.status, SHARE_STATUS_PROVISIONAL);
        assert!(!ack.verified);
        assert!(!ack.block_accepted);

        let shares = store.shares();
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].nonce, 42);
    }

    #[test]
    fn regular_submits_stay_fast_while_live_audit_hashing_is_busy() {
        struct SlowMatchingHasher;
        impl PowHasher for SlowMatchingHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(120));
                Ok([0xff; 32])
            }
        }

        let mut cfg = cfg();
        cfg.sample_rate = 1.0;
        cfg.regular_verifiers = 1;
        cfg.regular_validation_queue = 1;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;

        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(SlowMatchingHasher),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        let first_started = Instant::now();
        let first = engine
            .submit("conn1", "job1".to_string(), 500, Some("ff".repeat(32)))
            .expect("first share");
        let first_elapsed = first_started.elapsed();
        let second_started = Instant::now();
        let second = engine
            .submit("conn1", "job1".to_string(), 501, Some("ff".repeat(32)))
            .expect("second share");
        let second_elapsed = second_started.elapsed();

        assert_eq!(first.status, SHARE_STATUS_PROVISIONAL);
        assert_eq!(second.status, SHARE_STATUS_PROVISIONAL);
        assert!(
            first_elapsed < Duration::from_millis(80),
            "first hot accept should not wait on the async audit lane"
        );
        assert!(
            second_elapsed < Duration::from_millis(80),
            "second hot accept should still stay fast while the audit worker is hashing"
        );

        let snapshot = validation.snapshot();
        assert_eq!(snapshot.hot_accepts, 2);
        assert!(
            snapshot.audit_enqueued >= 1,
            "sampled hot accepts should enqueue live audit work"
        );
    }

    #[test]
    fn candidate_submit_bypasses_busy_regular_audit_lane() {
        struct SplitHasher;
        impl PowHasher for SplitHasher {
            fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(120));
                Ok(if nonce == 777 { [0x00; 32] } else { [0xff; 32] })
            }
        }

        let mut cfg = cfg();
        cfg.sample_rate = 1.0;
        cfg.regular_verifiers = 1;
        cfg.regular_validation_queue = 1;
        cfg.warmup_shares = 0;
        cfg.min_sample_every = 0;

        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(SplitHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            network_target: [0x01; 32],
            ..job()
        });
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        let regular = engine
            .submit("conn1", "job1".to_string(), 700, Some("ff".repeat(32)))
            .expect("regular share");
        assert_eq!(regular.status, SHARE_STATUS_PROVISIONAL);

        let candidate_started = Instant::now();
        let candidate = engine
            .submit("conn1", "job1".to_string(), 777, Some("00".repeat(32)))
            .expect("candidate share");
        let candidate_elapsed = candidate_started.elapsed();

        assert!(
            candidate.block_accepted,
            "candidate should still reach block submit"
        );
        assert!(
            candidate_elapsed < Duration::from_millis(220),
            "candidate path should not wait behind the regular audit worker"
        );
        assert_eq!(node.submits.lock().len(), 1);
    }

    #[test]
    fn submit_persists_vardiff_hint_for_session() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 1;

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::clone(&jobs) as Arc<dyn JobRepository>,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );

        let address = test_miner_address();
        engine
            .login(
                "conn1",
                address.clone(),
                Some("rig01".to_string()),
                2,
                submit_hash_cap(),
            )
            .expect("login");

        let _ack = engine
            .submit("conn1", "job1".to_string(), 4242, Some("ff".repeat(32)))
            .expect("share should be accepted");

        let hint = store
            .vardiff_hint(&address, "rig01")
            .expect("vardiff hint should be persisted");
        assert_eq!(hint.0, 1);
    }

    #[test]
    fn submit_rejects_duplicate_share() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        engine
            .submit("conn1", "job1".to_string(), 7, Some("ff".repeat(32)))
            .expect("first submit");

        let err = engine
            .submit("conn1", "job1".to_string(), 7, Some("ff".repeat(32)))
            .expect_err("duplicate must fail");
        assert!(err.to_string().contains("duplicate"));
        let shares = store.shares();
        assert_eq!(shares.len(), 2);
        assert_eq!(shares[0].status, SHARE_STATUS_PROVISIONAL);
        assert_eq!(shares[1].status, SHARE_STATUS_REJECTED);
    }

    #[test]
    fn candidate_share_triggers_block_submission() {
        struct CandidateHasher;
        impl PowHasher for CandidateHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0x00; 32])
            }
        }

        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(CandidateHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            network_target: [0x01; 32],
            ..job()
        });
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: true,
            ..InMemoryNode::default()
        });

        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        let ack = engine
            .submit("conn1", "job1".to_string(), 9, Some("00".repeat(32)))
            .expect("submit candidate");

        assert!(ack.block_accepted);
        assert_eq!(node.submits.lock().len(), 1);

        let shares = store.shares();
        assert_eq!(shares.len(), 1);
        assert!(shares[0].block_hash.is_some());
        assert_eq!(store.blocks().len(), 1);
    }

    #[test]
    fn vardiff_increases_difficulty_for_fast_submitters() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.vardiff_target_shares = 1;
        cfg.vardiff_window = "5m".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();
        cfg.validation_mode = "probabilistic".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let _ = engine
            .submit("conn1", "job1".to_string(), 100, Some("ff".repeat(32)))
            .expect("share 1");
        std::thread::sleep(Duration::from_millis(300));
        let _ = engine
            .submit("conn1", "job1".to_string(), 101, Some("ff".repeat(32)))
            .expect("share 2");
        std::thread::sleep(Duration::from_millis(300));
        let _ = engine
            .submit("conn1", "job1".to_string(), 102, Some("ff".repeat(32)))
            .expect("share 3");
        std::thread::sleep(Duration::from_millis(300));
        let _ = engine
            .submit("conn1", "job1".to_string(), 103, Some("ff".repeat(32)))
            .expect("share 4");
        std::thread::sleep(Duration::from_millis(300));
        let ack = engine
            .submit("conn1", "job1".to_string(), 104, Some("ff".repeat(32)))
            .expect("share 5");

        assert!(ack.next_difficulty > ack.share_difficulty);
    }

    #[test]
    fn vardiff_step_is_capped_per_retarget() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.vardiff_target_shares = 1;
        cfg.vardiff_window = "5m".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();
        cfg.validation_mode = "probabilistic".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let _ = engine
            .submit("conn1", "job1".to_string(), 200, Some("ff".repeat(32)))
            .expect("share 1");
        std::thread::sleep(Duration::from_millis(300));
        let _ = engine
            .submit("conn1", "job1".to_string(), 201, Some("ff".repeat(32)))
            .expect("share 2");
        std::thread::sleep(Duration::from_millis(300));
        let _ = engine
            .submit("conn1", "job1".to_string(), 202, Some("ff".repeat(32)))
            .expect("share 3");
        std::thread::sleep(Duration::from_millis(300));
        let _ = engine
            .submit("conn1", "job1".to_string(), 203, Some("ff".repeat(32)))
            .expect("share 4");
        std::thread::sleep(Duration::from_millis(300));
        let ack = engine
            .submit("conn1", "job1".to_string(), 204, Some("ff".repeat(32)))
            .expect("share 5");

        assert!(ack.next_difficulty >= ack.share_difficulty);
        let capped = ((ack.share_difficulty as f64) * VARDIFF_MAX_ADJUSTMENT_FACTOR).ceil() as u64;
        assert!(ack.next_difficulty <= capped);
    }

    #[test]
    fn vardiff_decreases_after_idle_gap_with_sparse_window_samples() {
        struct ZeroHasher;
        impl PowHasher for ZeroHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0u8; 32])
            }
        }

        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.validation_mode = "full".to_string();
        cfg.initial_share_difficulty = 100;
        cfg.vardiff_target_shares = 60;
        // Window must be longer than the sleep so the idle gap falls within it;
        // gaps exceeding the window are treated as intermittent and skip decay.
        cfg.vardiff_window = "5s".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(ZeroHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let first = engine
            .submit("conn1", "job1".to_string(), 300, Some("00".repeat(32)))
            .expect("share 1");
        assert_eq!(first.share_difficulty, 100);
        assert_eq!(first.next_difficulty, 100);

        {
            let mut sessions = engine.sessions.lock();
            let session = sessions
                .get_mut("conn1")
                .expect("session should exist after login");
            // Simulate a sparse current vardiff window while preserving last-share age.
            session.accepted_share_times.clear();
        }

        std::thread::sleep(Duration::from_millis(2_200));

        let second = engine
            .submit("conn1", "job1".to_string(), 301, Some("00".repeat(32)))
            .expect("share 2");
        assert!(
            second.next_difficulty < second.share_difficulty,
            "difficulty should decay after an idle gap even when prior share aged out of window"
        );
    }

    #[test]
    fn vardiff_job_tick_decays_idle_user_sessions() {
        struct ZeroHasher;
        impl PowHasher for ZeroHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0u8; 32])
            }
        }

        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.validation_mode = "full".to_string();
        cfg.initial_share_difficulty = 100;
        cfg.vardiff_target_shares = 60;
        cfg.vardiff_window = "1s".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(ZeroHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let first = engine
            .submit("conn1", "job1".to_string(), 400, Some("00".repeat(32)))
            .expect("share 1");
        assert_eq!(first.next_difficulty, 100);

        std::thread::sleep(Duration::from_millis(2_200));

        let before = engine
            .session_difficulty("conn1")
            .expect("session difficulty should exist");
        let after = engine
            .retarget_on_job_if_needed("conn1")
            .expect("session difficulty should still exist");

        assert!(
            after < before,
            "job-tick retarget should decay idle user sessions so they can recover from overshoot"
        );
    }

    #[test]
    fn vardiff_job_tick_does_not_decay_idle_dev_fee_sessions() {
        struct ZeroHasher;
        impl PowHasher for ZeroHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0u8; 32])
            }
        }

        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.validation_mode = "full".to_string();
        cfg.initial_share_difficulty = 100;
        cfg.vardiff_target_shares = 60;
        cfg.vardiff_window = "1s".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(ZeroHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login(
                "conn-dev",
                crate::dev_fee::SEINE_DEV_FEE_ADDRESS.to_string(),
                None,
                2,
                submit_hash_cap(),
            )
            .expect("login");
        let first = engine
            .submit("conn-dev", "job1".to_string(), 401, Some("00".repeat(32)))
            .expect("share 1");
        assert_eq!(first.next_difficulty, 100);

        std::thread::sleep(Duration::from_millis(2_200));

        let before = engine
            .session_difficulty("conn-dev")
            .expect("session difficulty should exist");
        let after = engine
            .retarget_on_job_if_needed("conn-dev")
            .expect("session difficulty should still exist");

        assert_eq!(
            after, before,
            "job-tick retarget should not decay idle dev-fee sessions"
        );
    }

    #[test]
    fn clear_address_risk_history_removes_in_memory_risk_state() {
        let store = InMemoryStore::default();
        let address = test_miner_address();

        store
            .escalate_address_risk(
                &address,
                "bad share",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("seed risk state");
        assert!(store
            .address_risk_state(&address)
            .expect("read risk state")
            .is_some());

        store
            .clear_address_risk_history(&address)
            .expect("clear risk history");

        assert!(store
            .address_risk_state(&address)
            .expect("read cleared risk state")
            .is_none());
    }

    #[test]
    fn queue_full_submit_returns_busy_without_inline_bypass() {
        struct SlowHasher;
        impl PowHasher for SlowHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(120));
                Ok([0x01; 32])
            }
        }

        let mut cfg = cfg();
        cfg.max_validation_queue = 1;
        cfg.regular_validation_queue = 1;
        cfg.validation_mode = "full".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(SlowHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            network_target: difficulty_to_target(1),
            ..job()
        });

        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        // Fill queue with one submit so next submit sees queue pressure.
        let _ = engine.submit("conn1", "job1".to_string(), 1, Some("00".repeat(32)));

        // v2 regular share should return busy when queue is full.
        let err = engine
            .submit("conn1", "job1".to_string(), 2, Some("ff".repeat(32)))
            .expect_err("regular share should be rejected when queue is full");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn legacy_submit_without_claimed_hash_is_rejected() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        let err = engine
            .submit("conn1", "job1".to_string(), 3, None)
            .expect_err("legacy submit without claimed hash should fail");
        assert!(err.to_string().contains("claimed hash required"));
    }

    #[test]
    fn legacy_submit_without_claimed_hash_is_accepted_when_v2_not_required() {
        let mut cfg = cfg();
        cfg.stratum_submit_v2_required = false;
        cfg.validation_mode = "probabilistic".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 1, vec![])
            .expect("legacy login should succeed");

        let ack = engine
            .submit("conn1", "job1".to_string(), 3, None)
            .expect("legacy submit should be accepted when v2 is optional");
        assert!(ack.accepted);
        assert!(ack.verified, "legacy submits are force-verified");
        assert_eq!(ack.status, SHARE_STATUS_VERIFIED);
    }

    #[test]
    fn seen_cache_entry_is_released_after_rejected_submit() {
        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());

        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");

        let err = engine
            .submit("conn1", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("expected invalid share proof");
        assert!(!err.to_string().is_empty());
        assert!(engine.seen_in_memory.lock().is_empty());

        let second = engine
            .submit("conn1", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("replay should be treated as duplicate");
        assert!(second.to_string().contains("duplicate"));
    }

    #[test]
    fn suspected_fraud_strikes_reset_after_window_expires() {
        let store = InMemoryStore::default();
        store
            .record_suspected_fraud(
                "addr1",
                "invalid share proof",
                3,
                Duration::from_secs(24 * 60 * 60),
                Duration::from_secs(60 * 60),
                Duration::from_secs(24 * 60 * 60),
                Duration::from_secs(60),
            )
            .expect("first fraud event should persist");
        {
            let mut risk = store.address_risk.lock();
            let state = risk.get_mut("addr1").expect("risk state should exist");
            state.suspected_fraud_strikes = 2;
            state.suspected_fraud_window_until = Some(SystemTime::now() - Duration::from_secs(1));
        }

        store
            .record_suspected_fraud(
                "addr1",
                "invalid share proof",
                3,
                Duration::from_secs(24 * 60 * 60),
                Duration::from_secs(60 * 60),
                Duration::from_secs(24 * 60 * 60),
                Duration::from_secs(60),
            )
            .expect("expired fraud window should reset strike count");

        let state = store
            .address_risk_state("addr1")
            .expect("read risk state")
            .expect("risk state should exist");
        assert_eq!(state.suspected_fraud_strikes, 1);
    }

    #[test]
    fn quarantine_error_message_includes_reason_and_expiry() {
        let now = SystemTime::now();
        let message = quarantine_error_message(
            Some(&AddressRiskState {
                address: "addr1".to_string(),
                last_reason: Some("invalid share proof".to_string()),
                quarantined_until: Some(now + Duration::from_secs(120)),
                suspected_fraud_strikes: 3,
                suspected_fraud_window_until: Some(now + Duration::from_secs(120)),
                ..AddressRiskState::default()
            }),
            now,
        );
        assert!(message.contains("invalid share proof"));
        assert!(message.contains("fraud strikes=3"));
        assert!(message.contains("remaining"));
    }

    #[test]
    fn invalid_proof_burst_does_not_quarantine_immediately() {
        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            store,
            Arc::new(InMemoryNode::default()),
        );
        let address = test_miner_address();

        engine
            .login("conn1", address.clone(), None, 2, submit_hash_cap())
            .expect("initial login");

        for nonce in [100u64, 101] {
            let err = engine
                .submit("conn1", "job1".to_string(), nonce, Some("ff".repeat(32)))
                .expect_err("invalid proof should be rejected");
            assert!(err.to_string().contains("invalid share proof"));
        }

        engine
            .login("conn2", address.clone(), None, 2, submit_hash_cap())
            .expect("address should remain allowed while only forced review is active");

        let third = engine
            .submit("conn1", "job1".to_string(), 102, Some("ff".repeat(32)))
            .expect_err("third invalid proof still returns the proof rejection");
        assert!(third.to_string().contains("invalid share proof"));

        engine
            .login("conn3", address, None, 2, submit_hash_cap())
            .expect("invalid proofs alone should not immediately quarantine the address");
    }

    #[test]
    fn seen_cache_entry_is_released_after_accepted_submit() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        engine
            .submit("conn1", "job1".to_string(), 89, Some("ff".repeat(32)))
            .expect("share should be accepted");

        assert!(engine.seen_in_memory.lock().is_empty());
    }

    #[test]
    fn submit_uses_assignment_bound_share_difficulty() {
        let mut cfg = cfg();
        cfg.initial_share_difficulty = 64;
        cfg.validation_mode = "probabilistic".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        jobs.insert_assignment(
            "assign-old-diff",
            "job1",
            1,
            Some(test_miner_address()),
            0,
            1_000_000,
        );
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let ack = engine
            .submit(
                "conn1",
                "assign-old-diff".to_string(),
                89,
                Some("ff".repeat(32)),
            )
            .expect("share should be accepted using assignment difficulty");
        assert_eq!(ack.share_difficulty, 1);
    }

    #[test]
    fn submit_rejects_assignment_from_different_miner() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        jobs.insert_assignment(
            "assign-other-miner",
            "job1",
            1,
            Some(other_miner_address()),
            0,
            1_000_000,
        );
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let err = engine
            .submit(
                "conn1",
                "assign-other-miner".to_string(),
                42,
                Some("ff".repeat(32)),
            )
            .expect_err("submit should fail for wrong miner assignment");
        assert!(err.to_string().contains("not assigned"));
    }

    #[test]
    fn submit_rejects_nonce_outside_assignment_range() {
        let cfg = cfg();
        let validation = ValidationEngine::new(cfg.clone(), Arc::new(DeterministicTestHasher));
        let validation = Arc::new(validation);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        jobs.insert_assignment(
            "assign-range",
            "job1",
            1,
            Some(test_miner_address()),
            100,
            199,
        );
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        engine
            .login("conn1", test_miner_address(), None, 2, submit_hash_cap())
            .expect("login");
        let err = engine
            .submit(
                "conn1",
                "assign-range".to_string(),
                42,
                Some("ff".repeat(32)),
            )
            .expect_err("submit should fail for nonce outside assignment");
        assert!(err.to_string().contains("out of assigned range"));
    }
}
