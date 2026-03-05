use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::pow::{check_target, difficulty_to_target};
use crate::protocol::{
    build_login_result, normalize_capabilities, normalize_protocol_version, normalize_worker_name,
    parse_hash_hex, validate_miner_address, CAP_SUBMIT_CLAIMED_HASH,
    STRATUM_PROTOCOL_VERSION_CURRENT,
};
use crate::validation::{
    ValidationEngine, ValidationResult, ValidationTask, SHARE_STATUS_PROVISIONAL,
    SHARE_STATUS_VERIFIED,
};

const VARDIFF_MIN_SAMPLE_COUNT: usize = 3;
const VARDIFF_RATIO_DAMPING_EXPONENT: f64 = 0.45;
const VARDIFF_MIN_ADJUSTMENT_FACTOR: f64 = 1.02;
const VARDIFF_MAX_ADJUSTMENT_FACTOR: f64 = 1.5;
const VARDIFF_HINT_MAX_AGE: Duration = Duration::from_secs(60 * 60);
const VARDIFF_HINT_PERSIST_INTERVAL: Duration = Duration::from_secs(60);
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
    fn add_found_block(&self, _block: FoundBlockRecord) -> Result<()> {
        Ok(())
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
        _quarantine_base: Duration,
        _quarantine_max: Duration,
        _force_verify_duration: Duration,
        _apply_quarantine: bool,
    ) -> Result<()> {
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

#[derive(Debug, Clone)]
struct MinerSession {
    address: String,
    worker: String,
    difficulty: u64,
    protocol_version: u32,
    capabilities: BTreeSet<String>,
    accepted_share_times: VecDeque<Instant>,
    last_difficulty_adjustment: Option<Instant>,
    last_difficulty_hint_write: Option<Instant>,
}

pub struct PoolEngine {
    cfg: Config,
    validation: Arc<ValidationEngine>,
    jobs: Arc<dyn JobRepository>,
    store: Arc<dyn ShareStore>,
    node: Arc<dyn NodeApi>,

    sessions: Mutex<HashMap<String, MinerSession>>,
    seen_in_memory: Mutex<HashSet<(String, u64)>>,
    found_block_outbox_lock: Mutex<()>,
}

impl PoolEngine {
    pub fn new(
        cfg: Config,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        let engine = Self {
            cfg,
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

    pub fn login(
        &self,
        conn_id: &str,
        address: String,
        worker: Option<String>,
        protocol_version: u32,
        capabilities: Vec<String>,
    ) -> Result<crate::protocol::LoginResult> {
        let address = address.trim();
        if address.is_empty() || address.len() > 128 {
            return Err(anyhow!("invalid address"));
        }
        if let Err(err) = validate_miner_address(address) {
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
        if self.store.is_address_quarantined(address)? {
            return Err(anyhow!("address quarantined"));
        }
        let min_diff = self.cfg.min_share_difficulty.max(1);
        let max_diff = self.cfg.max_share_difficulty.max(min_diff);
        let mut difficulty = self.cfg.initial_share_difficulty.clamp(min_diff, max_diff);
        if self.cfg.enable_vardiff {
            if let Some((cached, updated_at)) = self.store.get_vardiff_hint(address, &worker)? {
                let age = SystemTime::now()
                    .duration_since(updated_at)
                    .unwrap_or_default();
                if age <= VARDIFF_HINT_MAX_AGE {
                    difficulty = cached.clamp(min_diff, max_diff);
                }
            }
        }

        let mut sessions = self.sessions.lock();
        sessions.insert(
            conn_id.to_string(),
            MinerSession {
                address: address.to_string(),
                worker,
                difficulty,
                protocol_version,
                capabilities: cap_set,
                accepted_share_times: VecDeque::new(),
                last_difficulty_adjustment: None,
                last_difficulty_hint_write: None,
            },
        );

        Ok(build_login_result(
            protocol_version,
            self.cfg.stratum_submit_v2_required,
        ))
    }

    pub fn disconnect(&self, conn_id: &str) {
        self.sessions.lock().remove(conn_id);
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

        let submit_job = self
            .jobs
            .resolve_submit_job_with_reason(&job_id, received_at)
            .map_err(|err| anyhow!(err.to_string()))?;
        let share_difficulty = submit_job
            .share_difficulty
            .unwrap_or(session.difficulty)
            .max(1);
        if let Some(expected_miner) = submit_job.assigned_miner.as_ref() {
            if expected_miner != &session.address {
                return Err(anyhow!("job not assigned to this miner"));
            }
        }
        if let (Some(start), Some(end)) = (submit_job.nonce_start, submit_job.nonce_end) {
            if nonce < start || nonce > end {
                return Err(anyhow!("nonce out of assigned range"));
            }
        }
        let job = submit_job.job;

        let seen_key = (job_id.clone(), nonce);
        {
            let mut seen = self.seen_in_memory.lock();
            if !seen.insert(seen_key.clone()) {
                return Err(anyhow!("duplicate share"));
            }
        }

        let claimed_hash = match claimed_hash_hex {
            Some(ref value) if !value.trim().is_empty() => match parse_hash_hex(value) {
                Ok(hash) => Some(hash),
                Err(err) => {
                    self.seen_in_memory.lock().remove(&seen_key);
                    return Err(anyhow!(err));
                }
            },
            _ => None,
        };

        let require_claimed_hash = self.cfg.stratum_submit_v2_required;
        if require_claimed_hash && claimed_hash.is_none() {
            self.seen_in_memory.lock().remove(&seen_key);
            return Err(anyhow!("claimed hash required"));
        }

        let claimed = match self.store.try_claim_share(&job_id, nonce) {
            Ok(true) => true,
            Ok(false) => {
                self.seen_in_memory.lock().remove(&seen_key);
                return Err(anyhow!("duplicate share"));
            }
            Err(err) => {
                self.seen_in_memory.lock().remove(&seen_key);
                return Err(err);
            }
        };
        let job_id_for_share = job_id.clone();
        let mut release_claim_on_error = true;

        let result = (|| -> Result<SubmitAck> {
            if self.store.is_address_quarantined(&session.address)? {
                release_claim_on_error = false;
                return Err(anyhow!("address quarantined"));
            }

            let share_target = difficulty_to_target(share_difficulty);

            let candidate_claim = claimed_hash
                .map(|hash| check_target(hash, job.network_target))
                .unwrap_or(false);

            let task = ValidationTask {
                address: session.address.clone(),
                nonce,
                header_base: job.header_base.clone(),
                share_target,
                network_target: job.network_target,
                claimed_hash,
                force_full_verify: candidate_claim
                    || self.store.should_force_verify_address(&session.address)?
                    || (!require_claimed_hash && claimed_hash.is_none()),
            };

            let validation = self.validate_task(task, candidate_claim)?;

            if !validation.accepted {
                release_claim_on_error = false;
                if validation.suspected_fraud || validation.escalate_risk {
                    let reason = validation.reject_reason.unwrap_or("risk escalation");
                    let strikes = if validation.suspected_fraud || !validation.escalate_risk {
                        0
                    } else {
                        match self.store.address_risk_strikes(&session.address) {
                            Ok(v) => v,
                            Err(err) => {
                                tracing::warn!(
                                    address = %session.address,
                                    error = %err,
                                    "failed to read address risk strikes before escalation"
                                );
                                0
                            }
                        }
                    };
                    let apply_quarantine =
                        should_apply_quarantine_for_failed_share(&self.cfg, &validation, strikes);
                    if let Err(err) = self.store.escalate_address_risk(
                        &session.address,
                        reason,
                        self.cfg.quarantine_duration_duration(),
                        self.cfg.max_quarantine_duration_duration(),
                        self.cfg.forced_verify_duration(),
                        apply_quarantine,
                    ) {
                        tracing::warn!(
                            address = %session.address,
                            error = %err,
                            "failed to persist risk escalation"
                        );
                    }
                }
                return Err(anyhow!(
                    "{}",
                    validation.reject_reason.unwrap_or("invalid share")
                ));
            }

            let status = if validation.verified {
                SHARE_STATUS_VERIFIED
            } else {
                SHARE_STATUS_PROVISIONAL
            };

            let mut block_hash = None;
            let mut block_accepted = false;
            if validation.is_block_candidate {
                let computed_hash = hex_string(validation.hash);
                block_hash = Some(computed_hash.clone());
                let submit = self.node.submit_block(&job, nonce)?;
                block_accepted = submit.accepted;
                if let Some(network_hash) = submit.hash {
                    block_hash = Some(network_hash);
                }

                if block_accepted {
                    let accepted_height = submit.height.unwrap_or(job.height);
                    let accepted_hash = block_hash.clone().unwrap_or(computed_hash.clone());
                    tracing::warn!(
                        height = accepted_height,
                        hash = %accepted_hash,
                        finder = %session.address,
                        worker = %session.worker,
                        difficulty = job.network_difficulty,
                        nonce,
                        "POOL BLOCK FOUND"
                    );
                    let found = FoundBlockRecord {
                        height: accepted_height,
                        hash: accepted_hash,
                        difficulty: job.network_difficulty,
                        reward: job_template_reward(&job),
                        finder: session.address.clone(),
                        finder_worker: session.worker.clone(),
                        timestamp: SystemTime::now(),
                    };
                    self.persist_found_block(found)?;
                }
            }

            self.store.add_share(ShareRecord {
                job_id: job_id_for_share.clone(),
                miner: session.address.clone(),
                worker: session.worker.clone(),
                difficulty: share_difficulty,
                nonce,
                status,
                was_sampled: validation.verified,
                block_hash,
                created_at: SystemTime::now(),
            })?;

            let next_difficulty = self.note_share_and_maybe_adjust_difficulty(conn_id);

            Ok(SubmitAck {
                accepted: true,
                verified: validation.verified,
                status,
                block_accepted,
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

    pub fn recover_found_block_outbox(&self) {
        let path = found_block_outbox_path();
        let _guard = self.found_block_outbox_lock.lock();
        self.recover_found_block_outbox_locked(&path);
    }

    fn persist_found_block(&self, found: FoundBlockRecord) -> Result<()> {
        let mut last_error = String::new();
        for attempt in 1..=FOUND_BLOCK_PERSIST_MAX_RETRIES {
            match self.store.add_found_block(found.clone()) {
                Ok(()) => return Ok(()),
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

        self.append_found_block_outbox(&found)?;
        Err(anyhow!(
            "accepted block not persisted after {} attempts: {}",
            FOUND_BLOCK_PERSIST_MAX_RETRIES,
            last_error
        ))
    }

    fn append_found_block_outbox(&self, found: &FoundBlockRecord) -> Result<()> {
        let primary_path = found_block_outbox_path();
        let _guard = self.found_block_outbox_lock.lock();

        match append_found_block_outbox_record(&primary_path, found) {
            Ok(()) => {
                tracing::error!(
                    path = %primary_path.display(),
                    height = found.height,
                    hash = %found.hash,
                    finder = %found.finder,
                    "recorded accepted block in recovery outbox"
                );
                Ok(())
            }
            Err(primary_err) => Err(anyhow!(
                "failed to write accepted block outbox: {}",
                primary_err
            )),
        }
    }

    fn recover_found_block_outbox_locked(&self, path: &PathBuf) {
        let file = match OpenOptions::new().read(true).open(path) {
            Ok(v) => v,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
            Err(err) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %err,
                    "failed to open found-block recovery outbox"
                );
                return;
            }
        };

        let reader = BufReader::new(file);
        let mut retained = Vec::<String>::new();
        let mut recovered = 0u64;

        for (idx, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        line = idx + 1,
                        error = %err,
                        "failed reading found-block recovery outbox line"
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
                        "invalid found-block recovery outbox record"
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

            match self.store.add_found_block(found) {
                Ok(()) => {
                    recovered = recovered.saturating_add(1);
                }
                Err(err) => {
                    tracing::warn!(
                        path = %path.display(),
                        line = idx + 1,
                        error = %err,
                        "failed replaying found-block recovery record"
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
                        "failed removing drained found-block recovery outbox"
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
                    "failed rewriting found-block recovery outbox"
                );
            }
        }

        if recovered > 0 {
            tracing::info!(
                path = %path.display(),
                recovered,
                "recovered accepted blocks from outbox"
            );
        }
    }

    fn validate_task(&self, task: ValidationTask, candidate: bool) -> Result<ValidationResult> {
        if let Some(rx) = self.validation.submit(task, candidate) {
            let mut timeout = self.cfg.job_timeout_duration();
            timeout = timeout.clamp(Duration::from_secs(5), Duration::from_secs(60));
            return rx
                .recv_timeout(timeout)
                .map_err(|_| anyhow!("validation timeout"));
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

    pub fn session_capabilities(&self, conn_id: &str) -> Option<Vec<String>> {
        self.sessions.lock().get(conn_id).map(|session| {
            session
                .capabilities
                .iter()
                .cloned()
                .collect::<Vec<String>>()
        })
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
        let retarget_interval = self
            .cfg
            .vardiff_retarget_interval_duration()
            .clamp(Duration::from_secs(2), Duration::from_secs(8));
        let target_interval = vardiff_target_interval_seconds(&self.cfg);
        let tolerance = self.cfg.vardiff_tolerance.clamp(0.01, 0.95);

        session.accepted_share_times.push_back(now);
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while session
            .accepted_share_times
            .front()
            .is_some_and(|ts| *ts < cutoff)
        {
            session.accepted_share_times.pop_front();
        }

        if session.accepted_share_times.len() >= VARDIFF_MIN_SAMPLE_COUNT
            && !session
                .last_difficulty_adjustment
                .is_some_and(|last| now.duration_since(last) < retarget_interval)
        {
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
                    session.last_difficulty_adjustment = Some(now);

                    if next_diff != session.difficulty {
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

fn vardiff_target_interval_seconds(cfg: &Config) -> f64 {
    let window = cfg.vardiff_window_duration().max(Duration::from_secs(30));
    let target_shares = cfg.vardiff_target_shares.max(1) as f64;
    (window.as_secs_f64() / target_shares).max(1.0)
}

fn should_apply_quarantine_for_failed_share(
    cfg: &Config,
    validation: &ValidationResult,
    current_strikes: u64,
) -> bool {
    if validation.suspected_fraud {
        return true;
    }
    if !validation.escalate_risk {
        return false;
    }

    let threshold = cfg.invalid_escalation_quarantine_strikes.max(0) as u64;
    threshold > 0 && current_strikes.saturating_add(1) >= threshold
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
    let record = FoundBlockOutboxRecord {
        height: found.height,
        hash: found.hash.clone(),
        difficulty: found.difficulty,
        reward: found.reward,
        finder: found.finder.clone(),
        finder_worker: found.finder_worker.clone(),
        timestamp_unix: system_time_to_unix(found.timestamp),
    };
    serde_json::to_writer(&mut file, &record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

#[derive(Default)]
pub struct InMemoryStore {
    seen: Mutex<HashSet<(String, u64)>>,
    shares: Mutex<Vec<ShareRecord>>,
    blocks: Mutex<Vec<FoundBlockRecord>>,
    vardiff_hints: Mutex<HashMap<(String, String), (u64, SystemTime)>>,
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
}

impl ShareStore for InMemoryStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        Ok(self.seen.lock().contains(&(job_id.to_string(), nonce)))
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.seen.lock().insert((job_id.to_string(), nonce));
        Ok(())
    }

    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
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

    fn add_found_block(&self, block: FoundBlockRecord) -> Result<()> {
        self.blocks.lock().push(block);
        Ok(())
    }

    fn get_vardiff_hint(&self, address: &str, worker: &str) -> Result<Option<(u64, SystemTime)>> {
        Ok(self.vardiff_hint(address, worker))
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
    fn quarantine_decision_for_failed_share_is_thresholded() {
        let mut cfg = cfg();
        cfg.invalid_escalation_quarantine_strikes = 3;

        let mut validation = ValidationResult {
            nonce: 0,
            accepted: false,
            reject_reason: Some("low difficulty share"),
            hash: [0u8; 32],
            verified: true,
            is_block_candidate: false,
            suspected_fraud: false,
            escalate_risk: true,
        };

        assert!(!should_apply_quarantine_for_failed_share(
            &cfg,
            &validation,
            1
        ));
        assert!(should_apply_quarantine_for_failed_share(
            &cfg,
            &validation,
            2
        ));

        cfg.invalid_escalation_quarantine_strikes = 0;
        assert!(
            !should_apply_quarantine_for_failed_share(&cfg, &validation, 100),
            "threshold 0 disables non-fraud quarantine escalation"
        );

        validation.escalate_risk = false;
        assert!(!should_apply_quarantine_for_failed_share(
            &cfg,
            &validation,
            2
        ));

        validation.suspected_fraud = true;
        assert!(should_apply_quarantine_for_failed_share(
            &cfg,
            &validation,
            0
        ));
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
                "nonexistent-assignment".to_string(),
                11,
                Some("ff".repeat(32)),
            )
            .expect_err("missing assignment must return stale reason");
        assert!(err.to_string().contains("assignment not found"));
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
