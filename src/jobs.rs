use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::config::Config;
use crate::engine::{Job, JobRepository, SubmitJobBinding};
use crate::node::{is_http_status, NodeClient};
use crate::pow::difficulty_to_target;

const NONCE_RANGE_SIZE: u64 = 1_000_000;
const JOB_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(1);
const REWARD_ADDR_CACHE_TTL: Duration = Duration::from_secs(30);
const TEMPLATE_WALLET_RECOVERY_COOLDOWN: Duration = Duration::from_secs(10);
const MISSING_WALLET_PASSWORD_LOG_COOLDOWN: Duration = Duration::from_secs(60);
const MAX_ACTIVE_ASSIGNMENTS: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinerJob {
    pub job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub header_base: String,
    pub target: String,
    pub difficulty: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_target: Option<String>,
    pub height: u64,
    pub nonce_start: u64,
    pub nonce_end: u64,
}

#[derive(Debug)]
struct RewardAddressCache {
    address: Option<String>,
    checked_at: Option<Instant>,
}

#[derive(Debug)]
pub struct JobManager {
    node: Arc<NodeClient>,
    cfg: Config,

    state: RwLock<JobState>,
    nonce_counter: AtomicU64,
    last_refresh: Mutex<Option<Instant>>,
    reward_cache: Mutex<RewardAddressCache>,
    last_template_wallet_recovery: Mutex<Option<Instant>>,
    last_missing_wallet_password_log: Mutex<Option<Instant>>,

    tx: broadcast::Sender<Job>,
}

#[derive(Debug, Default)]
struct JobState {
    current: Option<Job>,
    jobs: HashMap<String, Job>,
    order: VecDeque<String>,
    assignments: HashMap<String, MinerAssignment>,
    assignment_order: VecDeque<String>,
}

#[derive(Debug, Clone)]
struct MinerAssignment {
    template_job_id: String,
    share_difficulty: u64,
    assigned_miner: String,
    nonce_start: u64,
    nonce_end: u64,
}

impl JobManager {
    pub fn new(node: Arc<NodeClient>, cfg: Config) -> Arc<Self> {
        let (tx, _) = broadcast::channel::<Job>(64);
        Arc::new(Self {
            node,
            cfg,
            state: RwLock::new(JobState::default()),
            nonce_counter: AtomicU64::new(0),
            last_refresh: Mutex::new(None),
            reward_cache: Mutex::new(RewardAddressCache {
                address: None,
                checked_at: None,
            }),
            last_template_wallet_recovery: Mutex::new(None),
            last_missing_wallet_password_log: Mutex::new(None),
            tx,
        })
    }

    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            {
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.refresh_template();
                })
                .await;
            }

            let mut ticker = tokio::time::interval(this.cfg.block_poll_duration());
            loop {
                ticker.tick().await;
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.refresh_template();
                })
                .await;
            }
        });
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Job> {
        self.tx.subscribe()
    }

    pub fn build_miner_job(&self, share_difficulty: u64, assigned_miner: &str) -> Option<MinerJob> {
        let job = self.current_job()?;
        let start = self
            .nonce_counter
            .fetch_add(NONCE_RANGE_SIZE, Ordering::Relaxed);
        let assignment_id = generate_job_id();
        let share_difficulty = share_difficulty.max(1);
        let share_target = difficulty_to_target(share_difficulty);
        let nonce_end = start.saturating_add(NONCE_RANGE_SIZE - 1);
        let assigned_miner = assigned_miner.trim().to_string();
        if assigned_miner.is_empty() {
            return None;
        }

        let mut state = self.state.write();
        state
            .jobs
            .entry(job.id.clone())
            .or_insert_with(|| job.clone());
        state.assignments.insert(
            assignment_id.clone(),
            MinerAssignment {
                template_job_id: job.id.clone(),
                share_difficulty,
                assigned_miner,
                nonce_start: start,
                nonce_end,
            },
        );
        state.assignment_order.push_back(assignment_id.clone());
        while state.assignment_order.len() > MAX_ACTIVE_ASSIGNMENTS {
            if let Some(oldest) = state.assignment_order.pop_front() {
                state.assignments.remove(&oldest);
            }
        }

        Some(MinerJob {
            job_id: assignment_id,
            template_id: job.template_id.clone(),
            header_base: hex_encode(&job.header_base),
            target: hex_encode(&share_target),
            difficulty: share_difficulty,
            network_target: Some(hex_encode(&job.network_target)),
            height: job.height,
            nonce_start: start,
            nonce_end,
        })
    }

    pub fn refresh_template(&self) {
        {
            let mut guard = self.last_refresh.lock();
            if let Some(last) = *guard {
                if last.elapsed() < JOB_REFRESH_MIN_INTERVAL {
                    return;
                }
            }
            *guard = Some(Instant::now());
        }

        let reward_address = self.resolve_reward_address();
        let template = match self.node.get_block_template(reward_address.as_deref()) {
            Ok(t) => t,
            Err(err) => {
                if self.try_recover_wallet_for_template(&err) {
                    match self.node.get_block_template(reward_address.as_deref()) {
                        Ok(t) => t,
                        Err(retry_err) => {
                            tracing::warn!(
                                error = %retry_err,
                                "failed to fetch block template after wallet recovery"
                            );
                            return;
                        }
                    }
                } else {
                    tracing::warn!(error = %err, "failed to fetch block template");
                    return;
                }
            }
        };

        let parsed = match parse_template_into_job(&template) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to parse block template");
                return;
            }
        };

        let mut state = self.state.write();
        if let Some(current) = state.current.as_ref() {
            if current.height == parsed.height && current.network_target == parsed.network_target {
                return;
            }
        }

        state.current = Some(parsed.clone());
        state.jobs.insert(parsed.id.clone(), parsed.clone());
        state.order.retain(|id| id != &parsed.id);
        state.order.push_back(parsed.id.clone());
        self.nonce_counter.store(0, Ordering::Relaxed);

        // Keep recent jobs bounded.
        let mut removed_jobs = Vec::new();
        while state.order.len() > 16 {
            if let Some(oldest) = state.order.pop_front() {
                if state
                    .current
                    .as_ref()
                    .is_some_and(|current| current.id == oldest)
                {
                    continue;
                }
                state.jobs.remove(&oldest);
                removed_jobs.push(oldest);
            }
        }
        if !removed_jobs.is_empty() {
            state
                .assignments
                .retain(|_, assignment| !removed_jobs.contains(&assignment.template_job_id));
            let valid_assignment_ids = state.assignments.keys().cloned().collect::<HashSet<_>>();
            state
                .assignment_order
                .retain(|assignment_id| valid_assignment_ids.contains(assignment_id));
        }

        let _ = self.tx.send(parsed);
    }

    fn resolve_reward_address(&self) -> Option<String> {
        if let Some(configured) = configured_pool_address(&self.cfg.pool_wallet_address) {
            return Some(configured);
        }

        {
            let cache = self.reward_cache.lock();
            if cache
                .checked_at
                .is_some_and(|t| t.elapsed() < REWARD_ADDR_CACHE_TTL)
            {
                return cache.address.clone();
            }
        }

        let mut cache = self.reward_cache.lock();
        cache.checked_at = Some(Instant::now());

        match self.node.get_wallet_address() {
            Ok(addr) => {
                let resolved = addr.address.trim().to_string();
                if !resolved.is_empty() {
                    cache.address = Some(resolved.clone());
                }
                cache.address.clone()
            }
            Err(err) => {
                let password = std::env::var("BLOCKNET_WALLET_PASSWORD")
                    .ok()
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty());

                if let Some(password) = password {
                    if is_http_status(&err, 503) {
                        let _ = self.node.wallet_load(&password);
                        let _ = self.node.wallet_unlock(&password);
                    } else if is_http_status(&err, 403) {
                        let _ = self.node.wallet_unlock(&password);
                    }
                    if let Ok(addr) = self.node.get_wallet_address() {
                        let resolved = addr.address.trim().to_string();
                        if !resolved.is_empty() {
                            cache.address = Some(resolved.clone());
                            return cache.address.clone();
                        }
                    }
                }

                tracing::warn!(error = %err, "wallet address unavailable");
                cache.address.clone()
            }
        }
    }

    fn try_recover_wallet_for_template(&self, err: &anyhow::Error) -> bool {
        if !(is_http_status(err, 503) || is_http_status(err, 403)) {
            return false;
        }

        let now = Instant::now();
        {
            let mut guard = self.last_template_wallet_recovery.lock();
            if guard
                .is_some_and(|last| now.duration_since(last) < TEMPLATE_WALLET_RECOVERY_COOLDOWN)
            {
                return false;
            }
            *guard = Some(now);
        }

        let password = std::env::var("BLOCKNET_WALLET_PASSWORD")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let Some(password) = password else {
            let mut guard = self.last_missing_wallet_password_log.lock();
            if guard
                .is_none_or(|last| now.duration_since(last) >= MISSING_WALLET_PASSWORD_LOG_COOLDOWN)
            {
                *guard = Some(now);
                tracing::warn!(
                    "wallet recovery for template fetch skipped: BLOCKNET_WALLET_PASSWORD is not set"
                );
            }
            return false;
        };

        if is_http_status(err, 503) {
            if let Err(load_err) = self.node.wallet_load(&password) {
                tracing::warn!(
                    error = %load_err,
                    "wallet recovery failed during load step for template fetch"
                );
                return false;
            }
            if let Err(unlock_err) = self.node.wallet_unlock(&password) {
                tracing::warn!(
                    error = %unlock_err,
                    "wallet recovery failed during unlock step for template fetch"
                );
                return false;
            }
        } else if is_http_status(err, 403) {
            if let Err(unlock_err) = self.node.wallet_unlock(&password) {
                tracing::warn!(
                    error = %unlock_err,
                    "wallet recovery failed during unlock step for template fetch"
                );
                return false;
            }
        }

        tracing::info!("wallet recovered for template fetch; retrying blocktemplate");
        true
    }
}

fn configured_pool_address(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

impl JobRepository for JobManager {
    fn get_job(&self, job_id: &str) -> Option<Job> {
        self.state.read().jobs.get(job_id).cloned()
    }

    fn current_job(&self) -> Option<Job> {
        self.state.read().current.clone()
    }

    fn resolve_submit_job(&self, submitted_job_id: &str) -> Option<SubmitJobBinding> {
        let state = self.state.read();
        let assignment = state.assignments.get(submitted_job_id)?;
        let job = state.jobs.get(&assignment.template_job_id)?.clone();
        Some(SubmitJobBinding {
            job,
            share_difficulty: Some(assignment.share_difficulty),
            assigned_miner: Some(assignment.assigned_miner.clone()),
            nonce_start: Some(assignment.nonce_start),
            nonce_end: Some(assignment.nonce_end),
        })
    }
}

fn parse_template_into_job(template: &crate::node::BlockTemplate) -> anyhow::Result<Job> {
    let header = template
        .block
        .get("header")
        .ok_or_else(|| anyhow::anyhow!("template missing header"))?;

    let height = header
        .get("height")
        .or_else(|| header.get("Height"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("template header missing height"))?;

    let difficulty = header
        .get("difficulty")
        .or_else(|| header.get("Difficulty"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("template header missing difficulty"))?;

    let network_target = hex_decode_32(&template.target)?;
    let header_base = hex_decode(&template.header_base)?;

    let id = generate_job_id();
    Ok(Job {
        id,
        height,
        header_base,
        network_target,
        network_difficulty: difficulty.max(1),
        template_id: Some(template.template_id.trim().to_string()).filter(|v| !v.is_empty()),
        full_block: Some(template.block.clone()),
    })
}

fn generate_job_id() -> String {
    let bytes: [u8; 8] = rand::random();
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

fn hex_decode(input: &str) -> anyhow::Result<Vec<u8>> {
    let v = input.trim();
    if !v.len().is_multiple_of(2) {
        return Err(anyhow::anyhow!("hex length must be even"));
    }

    let mut out = Vec::with_capacity(v.len() / 2);
    let bytes = v.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let hi = from_hex(bytes[i]).ok_or_else(|| anyhow::anyhow!("invalid hex"))?;
        let lo = from_hex(bytes[i + 1]).ok_or_else(|| anyhow::anyhow!("invalid hex"))?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_decode_32(input: &str) -> anyhow::Result<[u8; 32]> {
    let raw = hex_decode(input)?;
    if raw.len() != 32 {
        return Err(anyhow::anyhow!("expected 32-byte hex value"));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&raw);
    Ok(out)
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_pool_address_prefers_non_empty_value() {
        assert_eq!(
            configured_pool_address("  addr123  ").as_deref(),
            Some("addr123")
        );
        assert!(configured_pool_address("   ").is_none());
    }

    #[test]
    fn miner_job_carries_targets_and_nonce_window() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config::default(),
        );

        {
            let mut state = manager.state.write();
            state.current = Some(Job {
                id: "job1".into(),
                height: 1,
                header_base: vec![0xAA; 92],
                network_target: [0xBB; 32],
                network_difficulty: 1,
                template_id: Some("tmpl".into()),
                full_block: None,
            });
        }

        let job = manager
            .build_miner_job(1, "addr1")
            .expect("build miner job should work");
        assert_ne!(job.job_id, "job1");
        assert_eq!(job.difficulty, 1);
        assert_eq!(job.nonce_end, job.nonce_start + NONCE_RANGE_SIZE - 1);
        assert!(job.network_target.is_some());
        let bound = manager
            .resolve_submit_job(&job.job_id)
            .expect("assignment should resolve");
        assert_eq!(bound.job.id, "job1");
        assert_eq!(bound.share_difficulty, Some(1));
        assert_eq!(bound.assigned_miner.as_deref(), Some("addr1"));
        assert_eq!(bound.nonce_start, Some(job.nonce_start));
        assert_eq!(bound.nonce_end, Some(job.nonce_end));
    }
}
