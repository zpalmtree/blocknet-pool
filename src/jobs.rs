use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::engine::{Job, JobRepository, SubmitJobBinding, SubmitJobResolveError};
use crate::node::{http_error_body_contains, is_http_status, NodeClient};
use crate::pow::difficulty_to_target;

const NONCE_RANGE_SIZE: u64 = 1_000_000;
const MAX_DB_NONCE: u64 = i64::MAX as u64;
const JOB_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(1);
const MIN_BLOCK_POLL_INTERVAL: Duration = Duration::from_secs(1);
const REWARD_ADDR_CACHE_TTL: Duration = Duration::from_secs(30);
const TEMPLATE_WALLET_RECOVERY_COOLDOWN: Duration = Duration::from_secs(10);
const MISSING_WALLET_PASSWORD_LOG_COOLDOWN: Duration = Duration::from_secs(60);
const WALLET_LOAD_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const WALLET_LOAD_WAIT_POLL: Duration = Duration::from_millis(250);
const MAX_ACTIVE_ASSIGNMENTS: usize = 65_536;
const MAX_ASSIGNMENT_AGE: Duration = Duration::from_secs(10 * 60);
const SSE_RETRY_DELAY_MIN: Duration = Duration::from_millis(100);
const SSE_RETRY_DELAY_MAX: Duration = Duration::from_secs(10);

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

#[derive(Debug, Default)]
struct TipEventState {
    last_new_block: Option<LastTipEvent>,
}

#[derive(Debug, Clone)]
struct LastTipEvent {
    hash: String,
    height: Option<u64>,
}

#[derive(Debug, Default)]
struct SseFrameState {
    event_name: String,
    data_lines: Vec<String>,
}

impl SseFrameState {
    fn reset(&mut self) {
        self.event_name.clear();
        self.data_lines.clear();
    }
}

#[derive(Debug, Clone)]
struct NewBlockEvent {
    hash: String,
    height: Option<u64>,
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
    tip_events: Mutex<TipEventState>,

    tx: broadcast::Sender<Job>,
}

#[derive(Debug, Default)]
struct JobState {
    current: Option<Job>,
    jobs: HashMap<String, Job>,
    job_meta: HashMap<String, JobTemplateMeta>,
    order: VecDeque<String>,
    assignments: HashMap<String, MinerAssignment>,
    assignment_order: VecDeque<String>,
}

#[derive(Debug, Clone, Copy)]
struct JobTemplateMeta {
    created_at: Instant,
    stale_since: Option<Instant>,
}

#[derive(Debug, Clone)]
struct MinerAssignment {
    template_job_id: String,
    share_difficulty: u64,
    assigned_miner: String,
    nonce_start: u64,
    nonce_end: u64,
    created_at: Instant,
}

impl JobManager {
    pub fn new(node: Arc<NodeClient>, cfg: Config) -> Arc<Self> {
        let (tx, _) = broadcast::channel::<Job>(64);
        Arc::new(Self {
            node,
            cfg,
            state: RwLock::new(JobState::default()),
            nonce_counter: AtomicU64::new(random_nonce_slot()),
            last_refresh: Mutex::new(None),
            reward_cache: Mutex::new(RewardAddressCache {
                address: None,
                checked_at: None,
            }),
            last_template_wallet_recovery: Mutex::new(None),
            last_missing_wallet_password_log: Mutex::new(None),
            tip_events: Mutex::new(TipEventState::default()),
            tx,
        })
    }

    pub fn start(self: &Arc<Self>) {
        if self.cfg.sse_enabled {
            let this = Arc::clone(self);
            if let Err(err) = thread::Builder::new()
                .name("pool-tip-events".to_string())
                .spawn(move || this.run_tip_listener())
            {
                tracing::warn!(error = %err, "failed to start tip listener thread");
            }
        }

        let this = Arc::clone(self);
        tokio::spawn(async move {
            {
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.refresh_template();
                })
                .await;
            }

            let poll_interval = bounded_block_poll_interval(this.cfg.block_poll_duration());
            let mut ticker = tokio::time::interval(poll_interval);
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

    fn run_tip_listener(&self) {
        let mut reconnect_delay = SSE_RETRY_DELAY_MIN;
        loop {
            match self.node.open_events_stream() {
                Ok(resp) => {
                    reconnect_delay = SSE_RETRY_DELAY_MIN;
                    if let Err(err) = self.stream_tip_events(resp) {
                        tracing::warn!(error = %err, "tip events stream ended; reconnecting");
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "failed to open daemon events stream");
                }
            }

            thread::sleep(reconnect_delay);
            reconnect_delay = (reconnect_delay.saturating_mul(2)).min(SSE_RETRY_DELAY_MAX);
        }
    }

    fn stream_tip_events(&self, resp: reqwest::blocking::Response) -> anyhow::Result<()> {
        let mut frame = SseFrameState::default();
        let reader = BufReader::new(resp);
        for line_result in reader.lines() {
            let line = line_result?;
            self.process_sse_line(&line, &mut frame);
        }
        self.process_sse_frame(&frame);
        anyhow::bail!("SSE stream closed by peer")
    }

    fn process_sse_line(&self, line: &str, frame: &mut SseFrameState) {
        if line.is_empty() {
            self.process_sse_frame(frame);
            frame.reset();
            return;
        }

        if line.starts_with(':') {
            return;
        }

        let (field, raw_value) = line
            .split_once(':')
            .map_or((line, ""), |(f, rest)| (f, rest));
        let value = raw_value.strip_prefix(' ').unwrap_or(raw_value);

        match field {
            "event" => {
                frame.event_name.clear();
                frame.event_name.push_str(value);
            }
            "data" => frame.data_lines.push(value.to_string()),
            _ => {}
        }
    }

    fn process_sse_frame(&self, frame: &SseFrameState) {
        if frame.event_name != "new_block" || frame.data_lines.is_empty() {
            return;
        }

        let payload = frame.data_lines.join("\n");
        let Some(event) = parse_new_block_event_payload(&payload) else {
            tracing::warn!("failed parsing daemon new_block SSE payload");
            return;
        };

        self.on_new_block_event(&event.hash, event.height);
    }

    fn on_new_block_event(&self, hash: &str, event_height: Option<u64>) {
        let current_template_height = self
            .state
            .read()
            .current
            .as_ref()
            .map_or(0, |job| job.height);
        let should_refresh = {
            let mut tip_events = self.tip_events.lock();
            should_refresh_for_new_block_event(
                &mut tip_events,
                hash,
                event_height,
                current_template_height,
                self.cfg.refresh_on_same_height,
            )
        };

        if !should_refresh {
            return;
        }

        tracing::debug!(
            hash = %hash.trim(),
            event_height,
            current_template_height,
            "daemon tip event marked template stale"
        );
        self.refresh_template();
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Job> {
        self.tx.subscribe()
    }

    pub fn last_refresh_elapsed(&self) -> Option<Duration> {
        (*self.last_refresh.lock()).map(|last| last.elapsed())
    }

    pub fn tracked_job_count(&self) -> usize {
        self.state.read().jobs.len()
    }

    pub fn active_assignment_count(&self) -> usize {
        self.state.read().assignments.len()
    }

    pub fn current_job_age(&self) -> Option<Duration> {
        let state = self.state.read();
        let current = state.current.as_ref()?;
        state
            .job_meta
            .get(&current.id)
            .map(|meta| meta.created_at.elapsed())
    }

    pub fn build_miner_job(&self, share_difficulty: u64, assigned_miner: &str) -> Option<MinerJob> {
        let job = self.current_job()?;
        let slot_count = nonce_slot_count();
        let slot = self.nonce_counter.fetch_add(1, Ordering::Relaxed) % slot_count;
        let start = slot.saturating_mul(NONCE_RANGE_SIZE);
        let assignment_id = generate_job_id();
        let share_difficulty = share_difficulty.max(1);
        let share_target = difficulty_to_target(share_difficulty);
        let nonce_end = start.saturating_add(NONCE_RANGE_SIZE - 1);
        debug_assert!(nonce_end <= MAX_DB_NONCE);
        let assigned_miner = assigned_miner.trim().to_string();
        if assigned_miner.is_empty() {
            return None;
        }

        let mut state = self.state.write();
        let now = Instant::now();
        state
            .jobs
            .entry(job.id.clone())
            .or_insert_with(|| job.clone());
        state
            .job_meta
            .entry(job.id.clone())
            .or_insert(JobTemplateMeta {
                created_at: now,
                stale_since: None,
            });
        state.assignments.insert(
            assignment_id.clone(),
            MinerAssignment {
                template_job_id: job.id.clone(),
                share_difficulty,
                assigned_miner,
                nonce_start: start,
                nonce_end,
                created_at: Instant::now(),
            },
        );
        state.assignment_order.push_back(assignment_id.clone());
        let current_template_job_id = state.current.as_ref().map(|current| current.id.clone());
        prune_expired_assignments_locked(
            &mut state,
            self.cfg.job_timeout_duration().min(MAX_ASSIGNMENT_AGE),
            current_template_job_id.as_deref(),
        );
        let mut cap_evicted = 0usize;
        while state.assignment_order.len() > MAX_ACTIVE_ASSIGNMENTS {
            if let Some(oldest) = state.assignment_order.pop_front() {
                if state.assignments.remove(&oldest).is_some() {
                    cap_evicted = cap_evicted.saturating_add(1);
                }
            }
        }
        if cap_evicted > 0 {
            tracing::info!(
                cap_evicted,
                active_assignments = state.assignments.len(),
                cap = MAX_ACTIVE_ASSIGNMENTS,
                "evicted assignments due to active assignment cap"
            );
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
        let now = Instant::now();
        if let Some(current) = state.current.as_ref() {
            if same_template_identity(current, &parsed) {
                return;
            }
        }

        if let Some(prev_id) = state.current.as_ref().map(|job| job.id.clone()) {
            if let Some(meta) = state.job_meta.get_mut(&prev_id) {
                if meta.stale_since.is_none() {
                    meta.stale_since = Some(now);
                }
            } else {
                state.job_meta.insert(
                    prev_id,
                    JobTemplateMeta {
                        created_at: now,
                        stale_since: Some(now),
                    },
                );
            }
        }

        state.current = Some(parsed.clone());
        state.jobs.insert(parsed.id.clone(), parsed.clone());
        state.job_meta.insert(
            parsed.id.clone(),
            JobTemplateMeta {
                created_at: now,
                stale_since: None,
            },
        );
        state.order.retain(|id| id != &parsed.id);
        state.order.push_back(parsed.id.clone());
        self.nonce_counter
            .store(random_nonce_slot(), Ordering::Relaxed);

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
                state.job_meta.remove(&oldest);
                removed_jobs.push(oldest);
            }
        }
        if !removed_jobs.is_empty() {
            let assignments_before = state.assignments.len();
            state
                .assignments
                .retain(|_, assignment| !removed_jobs.contains(&assignment.template_job_id));
            let valid_assignment_ids = state.assignments.keys().cloned().collect::<HashSet<_>>();
            state
                .assignment_order
                .retain(|assignment_id| valid_assignment_ids.contains(assignment_id));
            let assignments_removed = assignments_before.saturating_sub(state.assignments.len());
            if assignments_removed > 0 {
                tracing::info!(
                    removed_templates = removed_jobs.len(),
                    assignments_removed,
                    active_assignments = state.assignments.len(),
                    "removed assignments tied to retired templates"
                );
            }
        }
        let current_template_job_id = state.current.as_ref().map(|current| current.id.clone());
        prune_expired_assignments_locked(
            &mut state,
            self.cfg.job_timeout_duration().min(MAX_ASSIGNMENT_AGE),
            current_template_job_id.as_deref(),
        );

        let _ = self.tx.send(parsed);
    }

    fn resolve_reward_address(&self) -> Option<String> {
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
                        let _ = self.recover_wallet_loaded_and_unlocked(&password);
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
        if !is_wallet_recoverable_template_error(err) {
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
            if !self.recover_wallet_loaded_and_unlocked(&password) {
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

    fn recover_wallet_loaded_and_unlocked(&self, password: &str) -> bool {
        match self.node.wallet_load(password) {
            Ok(_) => {}
            Err(load_err) => {
                if !is_http_status(&load_err, 409) {
                    tracing::warn!(
                        error = %load_err,
                        "wallet recovery failed during load step for template fetch"
                    );
                    return false;
                }
            }
        }

        // 409 from /wallet/load can mean either already loaded or currently loading.
        // Wait briefly for the wallet to become available before attempting unlock.
        if !self.wait_for_wallet_available(WALLET_LOAD_WAIT_TIMEOUT) {
            tracing::warn!(
                timeout_secs = WALLET_LOAD_WAIT_TIMEOUT.as_secs(),
                "wallet recovery timed out waiting for wallet load to complete"
            );
            return false;
        }

        if let Err(unlock_err) = self.node.wallet_unlock(password) {
            if is_http_status(&unlock_err, 503)
                && self.wait_for_wallet_available(WALLET_LOAD_WAIT_POLL)
            {
                if self.node.wallet_unlock(password).is_ok() {
                    return true;
                }
            }
            tracing::warn!(
                error = %unlock_err,
                "wallet recovery failed during unlock step for template fetch"
            );
            return false;
        }

        true
    }

    fn wait_for_wallet_available(&self, timeout: Duration) -> bool {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        loop {
            if self.node.get_wallet_address().is_ok() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(WALLET_LOAD_WAIT_POLL);
        }
    }
}

fn is_wallet_recoverable_template_error(err: &anyhow::Error) -> bool {
    if is_http_status(err, 403) {
        return true;
    }
    if !is_http_status(err, 503) {
        return false;
    }
    http_error_body_contains(err, 503, "wallet")
}

impl JobRepository for JobManager {
    fn get_job(&self, job_id: &str) -> Option<Job> {
        self.state.read().jobs.get(job_id).cloned()
    }

    fn current_job(&self) -> Option<Job> {
        self.state.read().current.clone()
    }

    fn resolve_submit_job_with_reason(
        &self,
        submitted_job_id: &str,
        submitted_at: Instant,
    ) -> std::result::Result<SubmitJobBinding, SubmitJobResolveError> {
        let state = self.state.read();
        let assignment = match state.assignments.get(submitted_job_id) {
            Some(assignment) => assignment,
            None => {
                tracing::info!(
                    submitted_job_id,
                    active_assignments = state.assignments.len(),
                    assignment_order_len = state.assignment_order.len(),
                    tracked_templates = state.jobs.len(),
                    current_template_job_id = state
                        .current
                        .as_ref()
                        .map(|job| job.id.as_str())
                        .unwrap_or("-"),
                    "submit assignment lookup miss"
                );
                return Err(SubmitJobResolveError::AssignmentNotFound);
            }
        };
        let current_job_id = state.current.as_ref().map(|job| job.id.as_str());
        let is_current_template = Some(assignment.template_job_id.as_str()) == current_job_id;
        let assignment_ttl = self.cfg.job_timeout_duration().min(MAX_ASSIGNMENT_AGE);
        let assignment_age = submitted_at.saturating_duration_since(assignment.created_at);
        if !is_current_template && assignment_age > assignment_ttl {
            return Err(SubmitJobResolveError::AssignmentExpired {
                age: assignment_age,
                ttl: assignment_ttl,
            });
        }

        if !is_current_template {
            let meta = state.job_meta.get(&assignment.template_job_id).copied();
            let stale_since = meta.and_then(|value| value.stale_since).unwrap_or_else(|| {
                meta.map(|value| value.created_at)
                    .unwrap_or(assignment.created_at)
            });
            let stale_for = submitted_at.saturating_duration_since(stale_since);
            let grace = self.cfg.stale_submit_grace_duration();
            if grace.is_zero() || stale_for > grace {
                return Err(SubmitJobResolveError::TemplateStaleBeyondGrace {
                    template_job_id: assignment.template_job_id.clone(),
                    current_job_id: current_job_id.map(str::to_string),
                    stale_for,
                    stale_grace: grace,
                });
            }
        }

        let job = state
            .jobs
            .get(&assignment.template_job_id)
            .cloned()
            .ok_or_else(|| SubmitJobResolveError::TemplateMissing {
                template_job_id: assignment.template_job_id.clone(),
            })?;
        Ok(SubmitJobBinding {
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

fn nonce_slot_count() -> u64 {
    let max_start = MAX_DB_NONCE.saturating_sub(NONCE_RANGE_SIZE - 1);
    (max_start / NONCE_RANGE_SIZE).saturating_add(1).max(1)
}

fn random_nonce_slot() -> u64 {
    rand::random::<u64>() % nonce_slot_count()
}

fn bounded_block_poll_interval(configured: Duration) -> Duration {
    configured.max(MIN_BLOCK_POLL_INTERVAL)
}

fn same_template_identity(current: &Job, parsed: &Job) -> bool {
    current.height == parsed.height
        && current.network_target == parsed.network_target
        && match (job_prev_hash(current), job_prev_hash(parsed)) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
}

fn job_prev_hash(job: &Job) -> Option<[u8; 32]> {
    let header = job.full_block.as_ref()?.get("header")?;
    let prev_hash = header
        .get("PrevHash")
        .or_else(|| header.get("prevhash"))
        .or_else(|| header.get("prevHash"))
        .or_else(|| header.get("prev_hash"))?;

    if let Some(values) = prev_hash.as_array() {
        if values.len() != 32 {
            return None;
        }
        let mut out = [0u8; 32];
        for (idx, value) in values.iter().enumerate() {
            let n = value.as_u64()?;
            if n > u8::MAX as u64 {
                return None;
            }
            out[idx] = n as u8;
        }
        return Some(out);
    }

    prev_hash.as_str().and_then(|v| hex_decode_32(v).ok())
}

fn parse_new_block_event_payload(payload: &str) -> Option<NewBlockEvent> {
    let value: Value = serde_json::from_str(payload).ok()?;
    let hash = value.get("hash").and_then(Value::as_str)?;
    let height = value.get("height").and_then(Value::as_u64);
    Some(NewBlockEvent {
        hash: hash.to_string(),
        height,
    })
}

fn should_refresh_for_new_block_event(
    state: &mut TipEventState,
    hash: &str,
    event_height: Option<u64>,
    current_template_height: u64,
    refresh_on_same_height: bool,
) -> bool {
    let Some(normalized_hash) = normalize_tip_hash(hash) else {
        return false;
    };

    if let Some(height) = event_height {
        if current_template_height != 0 && height.saturating_add(1) < current_template_height {
            return false;
        }
    }

    if let Some(last) = state.last_new_block.as_ref() {
        let same_height = matches!(
            (last.height, event_height),
            (Some(last_height), Some(height)) if last_height == height
        );
        if same_height {
            let hash_changed = last.hash != normalized_hash;
            state.last_new_block = Some(LastTipEvent {
                hash: normalized_hash,
                height: event_height,
            });
            return refresh_on_same_height && hash_changed;
        }

        if last.hash == normalized_hash && last.height == event_height {
            return false;
        }
    }

    state.last_new_block = Some(LastTipEvent {
        hash: normalized_hash,
        height: event_height,
    });
    true
}

fn normalize_tip_hash(hash: &str) -> Option<String> {
    let normalized = hash.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn prune_expired_assignments_locked(
    state: &mut JobState,
    max_age: Duration,
    current_template_job_id: Option<&str>,
) {
    if max_age.is_zero() {
        return;
    }
    let now = Instant::now();
    state.assignments.retain(|_, assignment| {
        let age = now.saturating_duration_since(assignment.created_at);
        if current_template_job_id.is_some_and(|current| assignment.template_job_id == current) {
            return true;
        }
        age <= max_age
    });
    let valid_assignment_ids = state.assignments.keys().cloned().collect::<HashSet<_>>();
    state
        .assignment_order
        .retain(|assignment_id| valid_assignment_ids.contains(assignment_id));
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
    use crate::node::HttpError;

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
        assert!(job.nonce_end <= MAX_DB_NONCE);
        assert!(job.network_target.is_some());
        let bound = manager
            .resolve_submit_job(&job.job_id, Instant::now())
            .expect("assignment should resolve");
        assert_eq!(bound.job.id, "job1");
        assert_eq!(bound.share_difficulty, Some(1));
        assert_eq!(bound.assigned_miner.as_deref(), Some("addr1"));
        assert_eq!(bound.nonce_start, Some(job.nonce_start));
        assert_eq!(bound.nonce_end, Some(job.nonce_end));
    }

    #[test]
    fn resolve_submit_job_allows_assignment_older_than_ttl_while_template_is_current() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config {
                job_timeout: "1s".to_string(),
                ..Config::default()
            },
        );

        let template = Job {
            id: "job1".into(),
            height: 1,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: Some("tmpl".into()),
            full_block: None,
        };

        {
            let mut state = manager.state.write();
            state.current = Some(template.clone());
            state.jobs.insert(template.id.clone(), template);
            state.assignments.insert(
                "assign-old".to_string(),
                MinerAssignment {
                    template_job_id: "job1".to_string(),
                    share_difficulty: 1,
                    assigned_miner: "addr1".to_string(),
                    nonce_start: 0,
                    nonce_end: NONCE_RANGE_SIZE - 1,
                    created_at: Instant::now() - Duration::from_secs(2),
                },
            );
        }

        assert!(manager
            .resolve_submit_job("assign-old", Instant::now())
            .is_some());
    }

    #[test]
    fn prune_expired_assignments_keeps_current_template_assignments() {
        let mut state = JobState::default();
        state.current = Some(Job {
            id: "job-current".to_string(),
            height: 10,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-current".to_string()),
            full_block: None,
        });

        state.assignments.insert(
            "assign-current".to_string(),
            MinerAssignment {
                template_job_id: "job-current".to_string(),
                share_difficulty: 1,
                assigned_miner: "addr1".to_string(),
                nonce_start: 0,
                nonce_end: NONCE_RANGE_SIZE - 1,
                created_at: Instant::now() - Duration::from_secs(30),
            },
        );
        state.assignments.insert(
            "assign-old".to_string(),
            MinerAssignment {
                template_job_id: "job-old".to_string(),
                share_difficulty: 1,
                assigned_miner: "addr1".to_string(),
                nonce_start: NONCE_RANGE_SIZE,
                nonce_end: NONCE_RANGE_SIZE.saturating_mul(2).saturating_sub(1),
                created_at: Instant::now() - Duration::from_secs(30),
            },
        );
        state
            .assignment_order
            .push_back("assign-current".to_string());
        state.assignment_order.push_back("assign-old".to_string());

        let current = state.current.as_ref().map(|job| job.id.clone());
        prune_expired_assignments_locked(&mut state, Duration::from_secs(1), current.as_deref());

        assert!(state.assignments.contains_key("assign-current"));
        assert!(!state.assignments.contains_key("assign-old"));
        assert_eq!(state.assignment_order.len(), 1);
        assert_eq!(
            state.assignment_order.front().map(String::as_str),
            Some("assign-current")
        );
    }

    #[test]
    fn resolve_submit_job_reports_assignment_not_found() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config::default(),
        );

        let err = manager
            .resolve_submit_job_with_reason("missing-assignment", Instant::now())
            .expect_err("missing assignment should return explicit reason");
        assert_eq!(err, SubmitJobResolveError::AssignmentNotFound);
    }

    #[test]
    fn resolve_submit_job_reports_expired_assignment_reason() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config {
                job_timeout: "1s".to_string(),
                stale_submit_grace: "10m".to_string(),
                ..Config::default()
            },
        );

        let old_template = Job {
            id: "job-old".into(),
            height: 1,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-old".into()),
            full_block: None,
        };
        let current_template = Job {
            id: "job-current".into(),
            height: 2,
            header_base: vec![0xCC; 92],
            network_target: [0xDD; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-current".into()),
            full_block: None,
        };

        let now = Instant::now();
        {
            let mut state = manager.state.write();
            state.current = Some(current_template.clone());
            state
                .jobs
                .insert(old_template.id.clone(), old_template.clone());
            state
                .jobs
                .insert(current_template.id.clone(), current_template);
            state.assignments.insert(
                "assign-old".to_string(),
                MinerAssignment {
                    template_job_id: old_template.id.clone(),
                    share_difficulty: 1,
                    assigned_miner: "addr1".to_string(),
                    nonce_start: 0,
                    nonce_end: NONCE_RANGE_SIZE - 1,
                    created_at: now - Duration::from_secs(2),
                },
            );
            state.job_meta.insert(
                old_template.id.clone(),
                JobTemplateMeta {
                    created_at: now - Duration::from_secs(3),
                    stale_since: Some(now - Duration::from_millis(500)),
                },
            );
        }

        let err = manager
            .resolve_submit_job_with_reason("assign-old", now)
            .expect_err("expired assignment should return explicit reason");
        assert!(matches!(
            err,
            SubmitJobResolveError::AssignmentExpired { .. }
        ));
    }

    #[test]
    fn resolve_submit_job_allows_recent_previous_template_within_grace() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config {
                job_timeout: "5m".to_string(),
                stale_submit_grace: "5s".to_string(),
                ..Config::default()
            },
        );

        let old_job = Job {
            id: "job-old".into(),
            height: 10,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-old".into()),
            full_block: None,
        };
        let new_job = Job {
            id: "job-new".into(),
            height: 11,
            header_base: vec![0xCC; 92],
            network_target: [0xDD; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-new".into()),
            full_block: None,
        };

        let now = Instant::now();
        {
            let mut state = manager.state.write();
            state.current = Some(new_job.clone());
            state.jobs.insert(old_job.id.clone(), old_job.clone());
            state.jobs.insert(new_job.id.clone(), new_job);
            state.assignments.insert(
                "assign-old".to_string(),
                MinerAssignment {
                    template_job_id: old_job.id.clone(),
                    share_difficulty: 1,
                    assigned_miner: "addr1".to_string(),
                    nonce_start: 0,
                    nonce_end: NONCE_RANGE_SIZE - 1,
                    created_at: now - Duration::from_secs(30),
                },
            );
            state.job_meta.insert(
                old_job.id.clone(),
                JobTemplateMeta {
                    created_at: now - Duration::from_secs(60),
                    stale_since: Some(now - Duration::from_secs(2)),
                },
            );
        }

        assert!(manager.resolve_submit_job("assign-old", now).is_some());
    }

    #[test]
    fn resolve_submit_job_rejects_previous_template_after_grace() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config {
                job_timeout: "5m".to_string(),
                stale_submit_grace: "2s".to_string(),
                ..Config::default()
            },
        );

        let old_job = Job {
            id: "job-old".into(),
            height: 10,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-old".into()),
            full_block: None,
        };
        let new_job = Job {
            id: "job-new".into(),
            height: 11,
            header_base: vec![0xCC; 92],
            network_target: [0xDD; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-new".into()),
            full_block: None,
        };

        let now = Instant::now();
        {
            let mut state = manager.state.write();
            state.current = Some(new_job.clone());
            state.jobs.insert(old_job.id.clone(), old_job.clone());
            state.jobs.insert(new_job.id.clone(), new_job);
            state.assignments.insert(
                "assign-old".to_string(),
                MinerAssignment {
                    template_job_id: old_job.id.clone(),
                    share_difficulty: 1,
                    assigned_miner: "addr1".to_string(),
                    nonce_start: 0,
                    nonce_end: NONCE_RANGE_SIZE - 1,
                    created_at: now - Duration::from_secs(30),
                },
            );
            state.job_meta.insert(
                old_job.id.clone(),
                JobTemplateMeta {
                    created_at: now - Duration::from_secs(60),
                    stale_since: Some(now - Duration::from_secs(10)),
                },
            );
        }

        assert!(manager.resolve_submit_job("assign-old", now).is_none());

        let err = manager
            .resolve_submit_job_with_reason("assign-old", now)
            .expect_err("stale template should return explicit reason");
        assert!(matches!(
            err,
            SubmitJobResolveError::TemplateStaleBeyondGrace { .. }
        ));
    }

    #[test]
    fn resolve_submit_job_uses_received_time_for_stale_grace() {
        let manager = JobManager::new(
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
            Config {
                job_timeout: "5m".to_string(),
                stale_submit_grace: "5s".to_string(),
                ..Config::default()
            },
        );

        let old_job = Job {
            id: "job-old".into(),
            height: 10,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-old".into()),
            full_block: None,
        };
        let new_job = Job {
            id: "job-new".into(),
            height: 11,
            header_base: vec![0xCC; 92],
            network_target: [0xDD; 32],
            network_difficulty: 1,
            template_id: Some("tmpl-new".into()),
            full_block: None,
        };

        let now = Instant::now();
        {
            let mut state = manager.state.write();
            state.current = Some(new_job.clone());
            state.jobs.insert(old_job.id.clone(), old_job.clone());
            state.jobs.insert(new_job.id.clone(), new_job);
            state.assignments.insert(
                "assign-old".to_string(),
                MinerAssignment {
                    template_job_id: old_job.id.clone(),
                    share_difficulty: 1,
                    assigned_miner: "addr1".to_string(),
                    nonce_start: 0,
                    nonce_end: NONCE_RANGE_SIZE - 1,
                    created_at: now - Duration::from_secs(30),
                },
            );
            state.job_meta.insert(
                old_job.id.clone(),
                JobTemplateMeta {
                    created_at: now - Duration::from_secs(60),
                    stale_since: Some(now - Duration::from_secs(6)),
                },
            );
        }

        let received_in_grace = now - Duration::from_secs(2);
        assert!(manager
            .resolve_submit_job("assign-old", received_in_grace)
            .is_some());

        let processed_too_late = now;
        assert!(manager
            .resolve_submit_job("assign-old", processed_too_late)
            .is_none());
    }

    #[test]
    fn wallet_recovery_filters_syncing_503_errors() {
        let syncing = anyhow::anyhow!(HttpError {
            path: "/api/mining/blocktemplate".to_string(),
            status_code: 503,
            body: "{\"error\":\"node is syncing\"}".to_string(),
        });
        let wallet_missing = anyhow::anyhow!(HttpError {
            path: "/api/mining/blocktemplate".to_string(),
            status_code: 503,
            body: "{\"error\":\"wallet not loaded\"}".to_string(),
        });
        let locked = anyhow::anyhow!(HttpError {
            path: "/api/mining/blocktemplate".to_string(),
            status_code: 403,
            body: "{\"error\":\"wallet locked\"}".to_string(),
        });

        assert!(!is_wallet_recoverable_template_error(&syncing));
        assert!(is_wallet_recoverable_template_error(&wallet_missing));
        assert!(is_wallet_recoverable_template_error(&locked));
    }

    #[test]
    fn nonce_slots_wrap_and_stay_db_compatible() {
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

        let slot_count = nonce_slot_count();
        manager
            .nonce_counter
            .store(slot_count.saturating_sub(1), Ordering::Relaxed);

        let first = manager
            .build_miner_job(1, "addr1")
            .expect("first miner job should be built");
        let second = manager
            .build_miner_job(1, "addr1")
            .expect("second miner job should be built");

        assert_eq!(first.nonce_start, (slot_count - 1) * NONCE_RANGE_SIZE);
        assert_eq!(second.nonce_start, 0);
        assert!(first.nonce_end <= MAX_DB_NONCE);
        assert!(second.nonce_end <= MAX_DB_NONCE);
    }

    #[test]
    fn template_identity_ignores_template_churn_fields() {
        fn template_block(
            prev_hash: [u8; 32],
            merkle_root: [u8; 32],
            timestamp: u64,
        ) -> serde_json::Value {
            serde_json::json!({
                "header": {
                    "Height": 100,
                    "Difficulty": 1000,
                    "PrevHash": prev_hash,
                    "MerkleRoot": merkle_root,
                    "Timestamp": timestamp,
                    "Nonce": 0
                }
            })
        }

        let base = Job {
            id: "j1".to_string(),
            height: 100,
            header_base: vec![1, 2, 3],
            network_target: [0x11; 32],
            network_difficulty: 1000,
            template_id: Some("t1".to_string()),
            full_block: Some(template_block([0xAA; 32], [0x10; 32], 1000)),
        };
        let mut same = base.clone();
        same.id = "j2".to_string();
        assert!(same_template_identity(&base, &same));

        let mut different_template_id = same.clone();
        different_template_id.template_id = Some("t2".to_string());
        assert!(same_template_identity(&base, &different_template_id));

        let mut different_header = same;
        different_header.header_base[0] = 9;
        assert!(same_template_identity(&base, &different_header));

        let mut different_merkle = base.clone();
        different_merkle.full_block = Some(template_block([0xAA; 32], [0x77; 32], 1001));
        assert!(same_template_identity(&base, &different_merkle));

        let mut different_timestamp = base.clone();
        different_timestamp.full_block = Some(template_block([0xAA; 32], [0x10; 32], 2000));
        assert!(same_template_identity(&base, &different_timestamp));

        let mut different_prev_hash = base.clone();
        different_prev_hash.full_block = Some(template_block([0xBB; 32], [0x10; 32], 1000));
        assert!(!same_template_identity(&base, &different_prev_hash));

        let mut different_height = base.clone();
        different_height.height += 1;
        assert!(!same_template_identity(&base, &different_height));

        let mut different_target = base.clone();
        different_target.network_target[0] ^= 0xff;
        assert!(!same_template_identity(&base, &different_target));
    }

    #[test]
    fn parse_new_block_event_payload_ignores_timestamp_field() {
        let payload = r#"{"hash":"ABC123","height":42,"timestamp":1730000000}"#;
        let parsed = parse_new_block_event_payload(payload).expect("event should parse");
        assert_eq!(parsed.hash, "ABC123");
        assert_eq!(parsed.height, Some(42));
    }

    #[test]
    fn duplicate_new_block_events_are_coalesced() {
        let mut state = TipEventState::default();
        assert!(should_refresh_for_new_block_event(
            &mut state,
            "abc",
            Some(100),
            0,
            false
        ));
        assert!(!should_refresh_for_new_block_event(
            &mut state,
            "abc",
            Some(100),
            0,
            false
        ));
    }

    #[test]
    fn same_height_hash_change_respects_refresh_setting() {
        let mut coalesced = TipEventState::default();
        assert!(should_refresh_for_new_block_event(
            &mut coalesced,
            "aaa",
            Some(100),
            0,
            false
        ));
        assert!(!should_refresh_for_new_block_event(
            &mut coalesced,
            "bbb",
            Some(100),
            0,
            false
        ));

        let mut enabled = TipEventState::default();
        assert!(should_refresh_for_new_block_event(
            &mut enabled,
            "aaa",
            Some(100),
            0,
            true
        ));
        assert!(should_refresh_for_new_block_event(
            &mut enabled,
            "bbb",
            Some(100),
            0,
            true
        ));
    }

    #[test]
    fn historical_new_block_events_are_ignored_when_template_is_ahead() {
        let mut state = TipEventState::default();
        assert!(!should_refresh_for_new_block_event(
            &mut state,
            "abc",
            Some(1759),
            1761,
            false
        ));
        assert!(should_refresh_for_new_block_event(
            &mut state,
            "def",
            Some(1760),
            1761,
            false
        ));
    }

    #[test]
    fn timestamp_only_changes_do_not_trigger_refresh() {
        let mut state = TipEventState::default();

        let event_a =
            parse_new_block_event_payload(r#"{"hash":"abc","height":500,"timestamp":1000}"#)
                .expect("event should parse");
        assert!(should_refresh_for_new_block_event(
            &mut state,
            &event_a.hash,
            event_a.height,
            0,
            false
        ));

        let event_b =
            parse_new_block_event_payload(r#"{"hash":"abc","height":500,"timestamp":1001}"#)
                .expect("event should parse");
        assert!(!should_refresh_for_new_block_event(
            &mut state,
            &event_b.hash,
            event_b.height,
            0,
            false
        ));
    }

    #[test]
    fn block_poll_interval_is_bounded_to_safe_minimum() {
        assert_eq!(
            bounded_block_poll_interval(Duration::from_secs(0)),
            Duration::from_secs(1)
        );
        assert_eq!(
            bounded_block_poll_interval(Duration::from_millis(10)),
            Duration::from_secs(1)
        );
        assert_eq!(
            bounded_block_poll_interval(Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }
}
