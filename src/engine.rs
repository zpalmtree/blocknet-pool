use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Result};
use parking_lot::Mutex;

use crate::config::Config;
use crate::pow::{check_target, difficulty_to_target};
use crate::protocol::{
    build_login_result, normalize_capabilities, normalize_protocol_version, parse_hash_hex,
    should_inline_validation_on_queue_full, CAP_SUBMIT_CLAIMED_HASH,
    STRATUM_PROTOCOL_VERSION_CURRENT,
};
use crate::validation::{
    ValidationEngine, ValidationResult, ValidationTask, SHARE_STATUS_PROVISIONAL,
    SHARE_STATUS_VERIFIED,
};

const VARDIFF_MIN_SAMPLE_COUNT: usize = 5;
const VARDIFF_RATIO_DAMPING_EXPONENT: f64 = 0.5;
const VARDIFF_MIN_ADJUSTMENT_FACTOR: f64 = 1.05;
const VARDIFF_MAX_ADJUSTMENT_FACTOR: f64 = 2.0;

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
    fn resolve_submit_job(&self, submitted_job_id: &str) -> Option<SubmitJobBinding>;
}

#[derive(Debug, Clone)]
pub struct SubmitJobBinding {
    pub job: Job,
    pub share_difficulty: Option<u64>,
    pub assigned_miner: Option<String>,
    pub nonce_start: Option<u64>,
    pub nonce_end: Option<u64>,
}

pub trait ShareStore: Send + Sync + 'static {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool>;
    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()>;
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
}

#[derive(Debug, Clone)]
pub struct FoundBlockRecord {
    pub height: u64,
    pub hash: String,
    pub difficulty: u64,
    pub finder: String,
    pub finder_worker: String,
    pub timestamp: SystemTime,
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
}

pub struct PoolEngine {
    cfg: Config,
    validation: Arc<ValidationEngine>,
    jobs: Arc<dyn JobRepository>,
    store: Arc<dyn ShareStore>,
    node: Arc<dyn NodeApi>,

    sessions: Mutex<HashMap<String, MinerSession>>,
    seen_in_memory: Mutex<HashSet<(String, u64)>>,
}

impl PoolEngine {
    pub fn new(
        cfg: Config,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        Self {
            cfg,
            validation,
            jobs,
            store,
            node,
            sessions: Mutex::new(HashMap::new()),
            seen_in_memory: Mutex::new(HashSet::new()),
        }
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

        let worker = worker
            .unwrap_or_else(|| "default".to_string())
            .chars()
            .take(64)
            .collect::<String>();
        let protocol_version = normalize_protocol_version(protocol_version);
        let caps = normalize_capabilities(&capabilities);
        let cap_set = caps.iter().cloned().collect::<BTreeSet<_>>();

        if self.cfg.stratum_submit_v2_required
            && protocol_version < STRATUM_PROTOCOL_VERSION_CURRENT
        {
            return Err(anyhow!("pool requires protocol_version >= 2"));
        }
        if self.cfg.stratum_submit_v2_required
            && !caps.is_empty()
            && !cap_set.contains(CAP_SUBMIT_CLAIMED_HASH)
        {
            return Err(anyhow!("pool requires submit_claimed_hash capability"));
        }
        if self.store.is_address_quarantined(address)? {
            return Err(anyhow!("address quarantined"));
        }

        let mut sessions = self.sessions.lock();
        sessions.insert(
            conn_id.to_string(),
            MinerSession {
                address: address.to_string(),
                worker,
                difficulty: self
                    .cfg
                    .initial_share_difficulty
                    .clamp(self.cfg.min_share_difficulty, self.cfg.max_share_difficulty),
                protocol_version,
                capabilities: cap_set,
                accepted_share_times: VecDeque::new(),
                last_difficulty_adjustment: None,
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
        let session = {
            let sessions = self.sessions.lock();
            sessions
                .get(conn_id)
                .cloned()
                .ok_or_else(|| anyhow!("not logged in"))?
        };

        let submit_job = self
            .jobs
            .resolve_submit_job(&job_id)
            .ok_or_else(|| anyhow!("stale job"))?;
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

        let result = (|| -> Result<SubmitAck> {
            if self.store.is_address_quarantined(&session.address)? {
                return Err(anyhow!("address quarantined"));
            }

            if self.store.is_share_seen(&job_id, nonce)? {
                return Err(anyhow!("duplicate share"));
            }

            let claimed_hash = match claimed_hash_hex {
                Some(ref value) if !value.trim().is_empty() => {
                    Some(parse_hash_hex(value).map_err(|err| anyhow!(err))?)
                }
                _ => None,
            };

            if self.cfg.stratum_submit_v2_required && claimed_hash.is_none() {
                return Err(anyhow!("claimed hash required"));
            }

            let share_target = difficulty_to_target(share_difficulty);

            let candidate_hint = claimed_hash
                .map(|hash| check_target(hash, job.network_target))
                .unwrap_or(false);

            let task = ValidationTask {
                address: session.address.clone(),
                nonce,
                header_base: job.header_base.clone(),
                share_target,
                network_target: job.network_target,
                claimed_hash,
                force_full_verify: candidate_hint
                    || claimed_hash.is_none()
                    || self.store.should_force_verify_address(&session.address)?,
            };

            let validation = self.validate_task(task, claimed_hash.is_some(), candidate_hint)?;

            if !validation.accepted {
                if validation.suspected_fraud || validation.escalate_risk {
                    let reason = validation.reject_reason.unwrap_or("risk escalation");
                    if let Err(err) = self.store.escalate_address_risk(
                        &session.address,
                        reason,
                        self.cfg.quarantine_duration_duration(),
                        self.cfg.max_quarantine_duration_duration(),
                        self.cfg.forced_verify_duration(),
                        validation.suspected_fraud,
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

            self.store.mark_share_seen(&job_id, nonce)?;

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
                    let found = FoundBlockRecord {
                        height: submit.height.unwrap_or(job.height),
                        hash: block_hash.clone().unwrap_or(computed_hash),
                        difficulty: job.network_difficulty,
                        finder: session.address.clone(),
                        finder_worker: session.worker.clone(),
                        timestamp: SystemTime::now(),
                    };
                    if let Err(err) = self.store.add_found_block(found) {
                        tracing::error!(
                            address = %session.address,
                            height = job.height,
                            error = %err,
                            "critical: failed to persist found block"
                        );
                    }
                }
            }

            self.store.add_share(ShareRecord {
                job_id,
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

        self.seen_in_memory.lock().remove(&seen_key);
        result
    }

    fn validate_task(
        &self,
        task: ValidationTask,
        has_claimed_hash: bool,
        candidate_hint: bool,
    ) -> Result<ValidationResult> {
        if let Some(rx) = self.validation.submit(task.clone(), candidate_hint) {
            let mut timeout = self.cfg.job_timeout_duration();
            timeout = timeout.clamp(Duration::from_secs(5), Duration::from_secs(60));
            return rx
                .recv_timeout(timeout)
                .map_err(|_| anyhow!("validation timeout"));
        }

        if should_inline_validation_on_queue_full(has_claimed_hash, candidate_hint) {
            Ok(self.validation.process_inline(task))
        } else {
            Err(anyhow!("server busy, retry"))
        }
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
        let mut sessions = self.sessions.lock();
        let Some(session) = sessions.get_mut(conn_id) else {
            return self.cfg.initial_share_difficulty;
        };

        let min_diff = self.cfg.min_share_difficulty.max(1);
        let max_diff = self.cfg.max_share_difficulty.max(min_diff);
        session.difficulty = session.difficulty.clamp(min_diff, max_diff);

        if !self.cfg.enable_vardiff {
            return session.difficulty;
        }

        let now = Instant::now();
        let window = self
            .cfg
            .vardiff_window_duration()
            .max(Duration::from_secs(30));
        let retarget_interval = self
            .cfg
            .vardiff_retarget_interval_duration()
            .max(Duration::from_secs(5));
        let target_shares = self.cfg.vardiff_target_shares.max(1) as f64;
        let target_interval = (window.as_secs_f64() / target_shares).max(1.0);
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

        if session.accepted_share_times.len() < VARDIFF_MIN_SAMPLE_COUNT {
            return session.difficulty;
        }
        if session
            .last_difficulty_adjustment
            .is_some_and(|last| now.duration_since(last) < retarget_interval)
        {
            return session.difficulty;
        }

        let Some(oldest) = session.accepted_share_times.front().copied() else {
            return session.difficulty;
        };
        let elapsed = now.duration_since(oldest).as_secs_f64();
        if elapsed < 1.0 {
            return session.difficulty;
        }

        let observed_interval = elapsed / ((session.accepted_share_times.len() - 1) as f64);
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
            tracing::info!(
                conn_id,
                address = %session.address,
                old_difficulty = session.difficulty,
                new_difficulty = next_diff,
                observed_interval_s = observed_interval,
                target_interval_s = target_interval,
                "adjusted share difficulty"
            );
            session.difficulty = next_diff;
        }

        session.difficulty
    }
}

fn hex_string(hash: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

#[derive(Default)]
pub struct InMemoryStore {
    seen: Mutex<HashSet<(String, u64)>>,
    shares: Mutex<Vec<ShareRecord>>,
    blocks: Mutex<Vec<FoundBlockRecord>>,
}

impl InMemoryStore {
    pub fn shares(&self) -> Vec<ShareRecord> {
        self.shares.lock().clone()
    }

    pub fn blocks(&self) -> Vec<FoundBlockRecord> {
        self.blocks.lock().clone()
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

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        self.shares.lock().push(share);
        Ok(())
    }

    fn add_found_block(&self, block: FoundBlockRecord) -> Result<()> {
        self.blocks.lock().push(block);
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

    fn resolve_submit_job(&self, submitted_job_id: &str) -> Option<SubmitJobBinding> {
        if let Some(assignment) = self.assignments.lock().get(submitted_job_id).cloned() {
            let job = self.jobs.lock().get(&assignment.template_job_id).cloned()?;
            return Some(SubmitJobBinding {
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
    use std::sync::atomic::{AtomicBool, Ordering};

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
                "BTestAddr".to_string(),
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
    fn login_rejects_old_protocol_when_required() {
        let mut cfg = cfg();
        cfg.stratum_submit_v2_required = true;

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
                "BTestAddr".to_string(),
                None,
                1,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect_err("should reject protocol 1 when v2 required");
        assert!(err.to_string().contains("protocol_version"));
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> [u8; 32] {
                [0x00; 32]
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
        assert!(ack.next_difficulty <= ack.share_difficulty.saturating_mul(2));
    }

    #[test]
    fn queue_full_regular_submit_returns_busy_but_legacy_inline_works() {
        struct SlowHasher {
            hit: AtomicBool,
        }
        impl PowHasher for SlowHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> [u8; 32] {
                self.hit.store(true, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(120));
                [0x01; 32]
            }
        }

        let mut cfg = cfg();
        cfg.max_validation_queue = 1;
        cfg.validation_mode = "full".to_string();

        let hasher = Arc::new(SlowHasher {
            hit: AtomicBool::new(false),
        });
        let validation = ValidationEngine::new(cfg.clone(), hasher);
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        // Fill queue with one candidate so next regular v2 submit sees queue pressure.
        let _ = engine.submit("conn1", "job1".to_string(), 1, Some("00".repeat(32)));

        // v2 regular share should return busy when queue is full.
        let err = engine
            .submit("conn1", "job1".to_string(), 2, Some("ff".repeat(32)))
            .expect_err("regular share should be rejected when queue is full");
        assert!(!err.to_string().is_empty());

        // Legacy path (no claimed hash) should still inline and not return busy.
        engine
            .submit("conn1", "job1".to_string(), 3, None)
            .expect("legacy share should not be dropped");
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
            .expect("login");

        let err = engine
            .submit("conn1", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("expected invalid share proof");
        assert!(!err.to_string().is_empty());
        assert!(engine.seen_in_memory.lock().is_empty());
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            Some("BTestAddr".to_string()),
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            Some("BOtherAddr".to_string()),
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
            Some("BTestAddr".to_string()),
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
            .login("conn1", "BTestAddr".to_string(), None, 2, vec![])
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
