#[cfg(test)]
use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::env;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::admission::{AdmissionState, UnprocessedReason};
use crate::config::{Config, LiveConfig};
use crate::dev_fee::{
    is_seine_dev_fee_address, login_difficulty_floor, DEV_VARDIFF_BOOTSTRAP_HINT_LIMIT,
};
use crate::share_dedupe::{template_share_key, NonceLedger};
use crate::source_risk::{
    ip_group_key, SourcePolicy, SourceRef, SourceRiskTracker, StrikeEvidence, StrikeKind,
    StrikeRule,
};
use crate::telemetry::{LatencyHistogramSnapshot, NamedLatencyHistograms, QueueTracker};
use crate::validation::{
    AuditAttribution, AuditFeedback, OverloadMode, ProbationReservation, ValidationEngine,
    ValidationFollowupAction, ValidationResult, ValidationTask, SHARE_STATUS_PROVISIONAL,
    SHARE_STATUS_REJECTED, SHARE_STATUS_VERIFIED,
};
use pool_common::db::{
    active_window_strikes, AddressRiskEscalation, AddressRiskState, PendingAuditShare, ShareKey,
    ShareReplayData, ValidationHoldCause,
};
use pool_common::faraday;
use pool_common::pow::{check_target, difficulty_to_target};
use pool_common::protocol::{
    build_login_result, normalize_worker_name, parse_hash_hex, validate_miner_address_for_network,
    AddressNetwork, CAP_SUBMIT_CLAIMED_HASH, STRATUM_PROTOCOL_VERSION_CURRENT,
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
/// Comma-separated outboxes of other Stratum instances behind the gateway.
/// Setting it also makes this instance lock its own outbox while running.
const FOUND_BLOCK_OUTBOX_PEERS_ENV: &str = "BLOCKNET_POOL_FOUND_BLOCK_OUTBOX_PEERS";
const OUTBOX_LOCK_ATTEMPTS: u32 = 50;
const FOUND_BLOCK_PERSIST_MAX_RETRIES: usize = 3;
const VALIDATION_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
/// Stored reject reason for provisional shares revoked by a source strike.
pub(crate) const SOURCE_REVOKED_REJECT_REASON: &str = "source revoked";
const PROVEN_FAKE_REJECT_REASON: &str = "invalid share proof";

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub height: u64,
    pub header_base: Vec<u8>,
    pub network_target: [u8; 32],
    pub network_difficulty: u64,
    pub template_id: String,
    pub prev_hash: Option<[u8; 32]>,
    pub template_created_at: Instant,
    /// Full daemon template block, retained so a solved block can still be
    /// submitted if the daemon has discarded the compact template ID (for
    /// example after a restart cleared its in-memory lease cache).
    /// Raw JSON of the daemon's template block (see `BlockTemplate::block`).
    pub template_block: Option<std::sync::Arc<str>>,
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
pub(crate) struct SubmitMetadata {
    pub miner_version: Option<String>,
    pub backend: Option<String>,
    /// Miner-reported found → sent delay (`submit_timing_v1`), for block logs.
    pub found_to_send_us: Option<u64>,
}

#[derive(Debug, Default)]
struct RejectedShareEvidence<'a> {
    claimed_hash_hex: Option<&'a str>,
    replay: Option<ShareReplayData>,
    reject_reason: Option<&'a str>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BlockSubmitResponse {
    pub accepted: bool,
    pub hash: Option<String>,
    pub height: Option<u64>,
    /// The daemon's own `timings_us.total` for this submission, when reported.
    pub daemon_total_us: Option<u64>,
}

pub(crate) trait NodeApi: Send + Sync + 'static {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse>;

    fn current_chain_height(&self) -> Result<u64> {
        Ok(0)
    }

    fn block_hash_at_height(&self, _height: u64) -> Result<Option<String>> {
        Ok(None)
    }
}

pub(crate) trait JobRepository: Send + Sync + 'static {
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
    /// Whether submits can still resolve to this template job. Used to bound
    /// template-scoped dedupe state; repositories that cannot tell keep it.
    fn template_retained(&self, _template_job_id: &str) -> bool {
        true
    }
    /// The daemon accepted a block this pool found at `height`; starts the
    /// own-block → next-job latency measurement.
    fn note_block_accepted(&self, _height: u64) {}
    /// Height of the job miners are currently given, if any. Picks the share
    /// difficulty era; must not block.
    fn current_height(&self) -> Option<u64> {
        None
    }
}

/// Share difficulty bounds for the era of the current job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DifficultyEra {
    min: u64,
    max: u64,
    initial: u64,
    faraday: bool,
}

impl DifficultyEra {
    /// A difficulty carried over from before the fork is below the fork floor
    /// and restarts at the fork's initial difficulty instead of the floor.
    /// Only used in the Faraday era; earlier code paths keep their own clamps.
    fn admit(&self, difficulty: u64) -> u64 {
        if difficulty < self.min {
            self.initial
        } else {
            difficulty.min(self.max)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubmitJobBinding {
    pub job: Job,
    pub share_difficulty: Option<u64>,
    pub assigned_miner: Option<String>,
    pub nonce_start: Option<u64>,
    pub nonce_end: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubmitJobResolveError {
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
    let active_risk_strikes = active_window_strikes(state.strikes, state.strike_window_until, now);
    if active_risk_strikes > 0 {
        details.push(format!("risk strikes={active_risk_strikes}"));
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
    if trimmed.contains("block rejected as stale") {
        return "stale block";
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
    if trimmed.starts_with("source banned") {
        return "source banned";
    }
    if trimmed.starts_with("source revoked") {
        return "source revoked";
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

/// Canonical reject reasons that tell the miner to retry: the share was never
/// judged on its proof, so it must not block a resubmission of the same nonce.
pub(crate) const RETRYABLE_REJECT_REASONS: &[&str] = &[
    "rate limited",
    "candidate claim busy",
    "candidate claim rate limited",
    "server busy",
    "validation timeout",
    "hash computation failed",
];

pub(crate) fn is_retryable_reject_reason(canonical_reason: &str) -> bool {
    RETRYABLE_REJECT_REASONS.contains(&canonical_reason)
}

/// What a submit needs to know about its address's risk, from one read.
/// Holds raw deadlines so a cached copy stays correct as time passes.
#[derive(Debug, Clone, Default)]
pub(crate) struct SubmitRisk {
    pub state: Option<AddressRiskState>,
    /// `validation_address_states.forced_until` (forced review or a hold).
    pub validation_forced_until: Option<SystemTime>,
    pub validation_hold_cause: Option<ValidationHoldCause>,
    /// For stores that only report a precomputed force-verify verdict.
    pub force_verify: bool,
}

impl SubmitRisk {
    pub(crate) fn active_quarantine(&self, now: SystemTime) -> Option<&AddressRiskState> {
        self.state
            .as_ref()
            .filter(|state| state.quarantined_until.is_some_and(|until| until > now))
    }

    /// Mandatory synchronous verification for a submit. Temporary coverage
    /// holds are handled by the validation planner's background audits, while
    /// the broader payout eligibility checks still honor every active hold.
    pub(crate) fn force_verify_at(&self, now: SystemTime) -> bool {
        self.force_verify
            || self.state.as_ref().is_some_and(|state| {
                state.force_verify_until.is_some_and(|until| until > now)
                    || state.quarantined_until.is_some_and(|until| until > now)
            })
            || (self
                .validation_forced_until
                .is_some_and(|until| until > now)
                && !matches!(
                    self.validation_hold_cause,
                    Some(
                        ValidationHoldCause::ProvisionalBacklog
                            | ValidationHoldCause::PayoutCoverage
                    )
                ))
    }
}

pub(crate) trait ShareStore: Send + Sync + 'static {
    /// Claims a share for crediting. `job_id` is the assignment id (checked
    /// against persisted rows) and `share_key` is the template-scoped key from
    /// [`template_share_key`]; the claim on `(share_key, nonce)` is kept after
    /// the share is persisted so the same work cannot be credited again under
    /// another assignment id.
    fn try_claim_share(&self, job_id: &str, share_key: &str, nonce: u64) -> Result<bool>;
    /// Drops a template-scoped claim after a retryable failure.
    fn release_share_claim(&self, _share_key: &str, _nonce: u64) -> Result<()> {
        Ok(())
    }
    fn add_share(&self, share: ShareRecord) -> Result<()>;
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
    fn add_share_with_replay_metadata_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
        _metadata: &SubmitMetadata,
    ) -> Result<i64> {
        self.add_share_with_replay_and_id(share, replay)
    }
    fn add_share_with_replay_metadata(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
        _metadata: &SubmitMetadata,
    ) -> Result<()> {
        self.add_share_with_replay(share, replay)
    }
    fn add_found_block(&self, _block: FoundBlockRecord) -> Result<()> {
        Ok(())
    }
    fn address_risk_state(&self, _address: &str) -> Result<Option<AddressRiskState>> {
        Ok(None)
    }
    fn should_force_verify_address(&self, _address: &str) -> Result<bool> {
        Ok(false)
    }
    /// Quarantine and force-verify inputs for one submit. Stores may serve
    /// this from a short-lived cache; see `PoolStore`.
    fn submit_risk(&self, address: &str) -> Result<SubmitRisk> {
        Ok(SubmitRisk {
            state: self.address_risk_state(address)?,
            validation_forced_until: None,
            validation_hold_cause: None,
            force_verify: self.should_force_verify_address(address)?,
        })
    }
    fn escalate_address_risk(&self, _escalation: AddressRiskEscalation<'_>) -> Result<()> {
        Ok(())
    }
    /// Marks still-provisional shares as rejected with `reason`.
    fn revoke_provisional_shares(&self, _shares: &[ShareKey], _reason: &str) -> Result<u64> {
        Ok(0)
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

/// Most early block submissions started per window. Real blocks arrive a
/// few times an hour at most, so this only ever binds on false claims, and
/// caps them at a handful of extra daemon hashes per window.
const EARLY_SUBMIT_MAX_PER_WINDOW: usize = 4;
const EARLY_SUBMIT_WINDOW: Duration = Duration::from_secs(600);

type EarlySubmitOutcome = (
    FoundBlockRecord,
    Result<BlockSubmitResponse>,
    Duration,
    Instant,
);

/// A block claim already sent to the daemon while the pool verifies it.
/// `wait` hands the daemon's answer to the normal block path; dropping it
/// unclaimed (the pool's verify rejected or never judged the share) still
/// waits for the daemon and reconciles the staged journal entry.
struct EarlyBlockSubmit<'a> {
    engine: &'a PoolEngine,
    staged: FoundBlockRecord,
    started_at: Instant,
    handle: Option<std::thread::JoinHandle<(Result<BlockSubmitResponse>, Duration)>>,
}

impl EarlyBlockSubmit<'_> {
    fn join(&mut self) -> Option<(Result<BlockSubmitResponse>, Duration)> {
        let handle = self.handle.take()?;
        Some(handle.join().unwrap_or_else(|_| {
            (
                Err(anyhow!("early block submit thread panicked")),
                self.started_at.elapsed(),
            )
        }))
    }

    fn wait(mut self) -> EarlySubmitOutcome {
        let (submitted, elapsed) = self.join().expect("early block submit joined once");
        (self.staged.clone(), submitted, elapsed, self.started_at)
    }
}

impl Drop for EarlyBlockSubmit<'_> {
    fn drop(&mut self) {
        if let Some((submitted, _)) = self.join() {
            self.engine
                .reconcile_unclaimed_early_submit(&self.staged, submitted);
        }
    }
}

/// A connection's difficulty to persist so a reconnect resumes near it.
#[derive(Debug, Clone)]
pub(crate) struct VardiffHint {
    address: String,
    worker: String,
    difficulty: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FoundBlockRecord {
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
pub(crate) struct SubmitAck {
    pub accepted: bool,
    pub verified: bool,
    pub status: &'static str,
    pub block_accepted: bool,
    pub share_difficulty: u64,
    pub next_difficulty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmitQueueRoute {
    Candidate,
    Regular,
}

#[derive(Debug, Clone)]
struct MinerSession {
    address: String,
    worker: String,
    difficulty: u64,
    capabilities: BTreeSet<String>,
    /// Recent shares for the rate estimate: accepted shares plus submits the
    /// pool answered without processing (see `record_unprocessed_submit`).
    share_times: VecDeque<Instant>,
    last_share_at: Option<Instant>,
    last_unprocessed_at: Option<Instant>,
    last_difficulty_adjustment: Option<Instant>,
    last_difficulty_hint_write: Option<Instant>,
    source: SourceRef,
}

pub struct PoolEngine {
    /// Settings Stratum runs with: `base_cfg`, plus the growth-mode overrides
    /// while growth mode is active.
    live_cfg: Arc<LiveConfig>,
    /// Settings as configured (file and reloads).
    base_cfg: Mutex<Config>,
    /// Growth level (0 = off; see `crate::growth`).
    growth_level: std::sync::atomic::AtomicU8,
    /// Last smoothed share rate at the normal share size (f64 bits).
    growth_rate_bits: std::sync::atomic::AtomicU64,
    expected_address_network: Option<AddressNetwork>,
    validation: Arc<ValidationEngine>,
    jobs: Arc<dyn JobRepository>,
    store: Arc<dyn ShareStore>,
    node: Arc<dyn NodeApi>,

    sessions: Mutex<HashMap<String, MinerSession>>,
    seen_in_memory: NonceLedger,
    source_risk: Arc<SourceRiskTracker>,
    found_block_outbox_lock: Mutex<()>,
    /// Held while this process owns its outbox (see `OutboxLock`).
    _outbox_ownership: Option<OutboxLock>,
    admission: Arc<AdmissionState>,
    /// Daemon `submitblock` round trips by outcome; the daemon re-hashes the
    /// block, so this is the pool-side half of block propagation delay.
    block_submit_latency: NamedLatencyHistograms,
    /// Stages of a block candidate from Stratum receipt to daemon acceptance.
    block_candidate_latency: NamedLatencyHistograms,
    /// Start times of recent early block submissions (see
    /// `start_early_block_submit`), bounding what false claims can cost the
    /// daemon.
    early_submit_starts: Mutex<VecDeque<Instant>>,
}

/// One proven-fake share (the pool's hash differs from the claimed hash).
struct ProvenFake<'a> {
    reason: &'a str,
    /// Skip proportionality (false block claims).
    immediate: bool,
    /// The address's persisted samples before this failure.
    address_good: u64,
    address_invalid: u64,
    /// This failure counts toward the address's persisted invalid samples.
    count_at_address: bool,
}

/// Penalizes a source for a proven fake share, proportionally: a source
/// without verified history, or with an invalid rate clearly above honest
/// hardware/backend error rates, gets a full strike (revoke its unverified
/// window, ban, disconnect); anything else gets a soft strike (the share is
/// rejected, the failure is counted and the source's next shares audited).
fn penalize_proven_fake(
    cfg: &Config,
    tracker: &SourceRiskTracker,
    store: &dyn ShareStore,
    validation: Option<&ValidationEngine>,
    source: &SourceRef,
    fake: ProvenFake<'_>,
) -> AuditAttribution {
    let reason = fake.reason;
    if !cfg.source_penalties_enabled {
        // Shadow mode: report what would happen and fall back to the
        // address-level accounting (attributed = false).
        tracing::warn!(
            address = %source.address,
            ip_group = %source.ip_group,
            session = source.session_id,
            reason,
            "source would be struck for a proven fake share (source_penalties_enabled=false)"
        );
        return AuditAttribution::default();
    }
    let (source_good, source_invalid) = tracker.record_failure(source);
    let evidence = StrikeEvidence {
        source_good,
        source_invalid,
        address_good: fake.address_good,
        address_invalid: fake
            .address_invalid
            .saturating_add(u64::from(fake.count_at_address)),
    };
    let kind = if fake.immediate {
        StrikeKind::Full
    } else {
        StrikeRule::from_config(cfg).decide(evidence)
    };
    if kind == StrikeKind::Soft {
        let audits = cfg.source_soft_strike_audits.max(0) as u32;
        tracker.soft_strike(source, audits);
        tracing::warn!(
            address = %source.address,
            ip_group = %source.ip_group,
            session = source.session_id,
            reason,
            source_good,
            source_invalid,
            address_good = evidence.address_good,
            address_invalid = evidence.address_invalid,
            boost_audits = audits,
            "soft strike: proven fake share from a source with clean history; share rejected, no revocation"
        );
        return AuditAttribution {
            attributed: true,
            count_invalid_at_address: fake.count_at_address,
            revoked: Vec::new(),
        };
    }

    let outcome = tracker.strike(source, reason);
    let mut revoked_rows = 0u64;
    for (address, shares) in &outcome.revoked {
        match store.revoke_provisional_shares(shares, SOURCE_REVOKED_REJECT_REASON) {
            Ok(rows) => revoked_rows = revoked_rows.saturating_add(rows),
            Err(err) => tracing::warn!(
                address = %address,
                shares = shares.len(),
                error = %err,
                "failed to revoke provisional shares after source strike"
            ),
        }
        if let Some(validation) = validation {
            validation.forget_revoked_shares(address, shares);
        }
    }
    tracing::warn!(
        address = %source.address,
        ip_group = %source.ip_group,
        session = source.session_id,
        reason,
        source_good,
        source_invalid,
        address_good = evidence.address_good,
        address_invalid = evidence.address_invalid,
        pair_strikes = outcome.pair_strikes,
        pair_banned_secs = outcome.pair_banned_for.as_secs(),
        ip_banned_secs = outcome.ip_banned_for.map(|ban| ban.as_secs()),
        revoked_shares = outcome.revoked_share_count(),
        revoked_rows,
        "source struck for proven fake share"
    );
    AuditAttribution {
        attributed: true,
        count_invalid_at_address: fake.count_at_address,
        revoked: outcome.revoked,
    }
}

/// Routes audit outcomes back to per-source tracking.
struct EngineAuditFeedback {
    live_cfg: Arc<LiveConfig>,
    tracker: Arc<SourceRiskTracker>,
    store: Arc<dyn ShareStore>,
}

impl EngineAuditFeedback {
    fn cfg(&self) -> Arc<Config> {
        self.live_cfg.get()
    }
}

impl AuditFeedback for EngineAuditFeedback {
    fn on_audit_result(
        &self,
        share_id: i64,
        address: &str,
        proven_fake: bool,
        address_good: u64,
        address_invalid: u64,
    ) -> AuditAttribution {
        let Some(source) = self.tracker.source_for_share(share_id) else {
            return AuditAttribution::default();
        };
        if source.address.as_ref() != address {
            return AuditAttribution::default();
        }
        if !proven_fake {
            self.tracker.record_audit_ok(share_id);
            return AuditAttribution {
                attributed: true,
                count_invalid_at_address: false,
                revoked: Vec::new(),
            };
        }
        let count_at_address = self.tracker.source_good(&source)
            >= self.cfg().source_full_strike_min_source_good.max(0) as u64;
        // The caller forgets revoked shares in validation state itself.
        penalize_proven_fake(
            &self.cfg(),
            &self.tracker,
            self.store.as_ref(),
            None,
            &source,
            ProvenFake {
                reason: PROVEN_FAKE_REJECT_REASON,
                immediate: false,
                address_good,
                address_invalid,
                count_at_address,
            },
        )
    }

    fn quarantine_unattributed(&self, address: &str, reason: &str) -> Option<SystemTime> {
        escalate_forced_validation_quarantine(&self.cfg(), self.store.as_ref(), address, reason)
    }

    fn audit_skipped(&self, share_id: i64) {
        self.tracker.record_audit_skipped(share_id);
    }

    fn degraded_review_batch(&self, budget: usize) -> Vec<ShareKey> {
        self.tracker
            .take_degraded_review_batch(budget, self.cfg().sample_rate)
    }
}

fn escalate_forced_validation_quarantine(
    cfg: &Config,
    store: &dyn ShareStore,
    address: &str,
    reason: &str,
) -> Option<SystemTime> {
    if let Err(err) = store.escalate_address_risk(AddressRiskEscalation::new(
        address,
        reason,
        cfg.invalid_escalation_window_duration(),
        cfg.invalid_escalation_quarantine_strikes.max(0) as u64,
        cfg.quarantine_duration(),
        cfg.max_quarantine_duration(),
        Duration::from_secs(0),
    )) {
        tracing::warn!(
            address = %address,
            error = %err,
            "failed to persist forced-validation quarantine"
        );
        return None;
    }
    match store.address_risk_state(address) {
        Ok(state) => state
            .and_then(|state| state.quarantined_until)
            .filter(|until| *until > SystemTime::now()),
        Err(err) => {
            tracing::warn!(
                address = %address,
                error = %err,
                "failed to load quarantine state after forced-validation quarantine"
            );
            None
        }
    }
}

fn source_ip_group(conn_id: &str, peer_ip: Option<IpAddr>) -> String {
    match peer_ip {
        Some(ip) => ip_group_key(ip),
        None => format!("conn:{conn_id}"),
    }
}

impl PoolEngine {
    pub(crate) fn preclassify_submit_route(
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
        if claimed_hash.is_none() {
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_prequeue_reject(
        &self,
        conn_id: &str,
        job_id: &str,
        nonce: u64,
        claimed_hash_hex: Option<&str>,
        received_at: Instant,
        reason: &str,
        metadata: &SubmitMetadata,
    ) {
        let Some(session) = self.sessions.lock().get(conn_id).cloned() else {
            return;
        };
        let binding = self.jobs.resolve_submit_job(job_id, received_at);
        let share_difficulty = binding
            .as_ref()
            .and_then(|binding| binding.share_difficulty)
            .unwrap_or(session.difficulty)
            .max(1);
        let created_at = SystemTime::now();
        let replay = binding.map(|binding| ShareReplayData {
            job_id: job_id.to_string(),
            header_base: binding.job.header_base,
            network_target: binding.job.network_target,
            created_at,
        });
        self.persist_rejected_share_with_evidence(
            &session,
            job_id,
            nonce,
            share_difficulty,
            RejectedShareEvidence {
                claimed_hash_hex,
                replay,
                reject_reason: Some(reason),
            },
            metadata,
        );
    }

    #[cfg(test)]
    pub(crate) fn new(
        cfg: Config,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        Self::new_with_expected_address_network(cfg, None, validation, jobs, store, node)
    }

    pub(crate) fn new_with_expected_address_network(
        cfg: Config,
        expected_address_network: Option<AddressNetwork>,
        validation: Arc<ValidationEngine>,
        jobs: Arc<dyn JobRepository>,
        store: Arc<dyn ShareStore>,
        node: Arc<dyn NodeApi>,
    ) -> Self {
        let source_risk = Arc::new(SourceRiskTracker::new(SourcePolicy::from_config(&cfg)));
        let admission = Arc::new(AdmissionState::new(cfg.sync_verify_slot_count()));
        let base_cfg = Mutex::new(cfg.clone());
        let live_cfg = Arc::new(LiveConfig::new(cfg));
        validation.attach_audit_feedback(Arc::new(EngineAuditFeedback {
            live_cfg: Arc::clone(&live_cfg),
            tracker: Arc::clone(&source_risk),
            store: Arc::clone(&store),
        }));
        let engine = Self {
            live_cfg,
            base_cfg,
            growth_level: std::sync::atomic::AtomicU8::new(0),
            growth_rate_bits: std::sync::atomic::AtomicU64::new(f64::NAN.to_bits()),
            expected_address_network,
            validation,
            jobs,
            store,
            node,
            sessions: Mutex::new(HashMap::new()),
            seen_in_memory: NonceLedger::default(),
            source_risk,
            found_block_outbox_lock: Mutex::new(()),
            _outbox_ownership: claim_found_block_outbox(),
            admission,
            block_submit_latency: NamedLatencyHistograms::default(),
            block_candidate_latency: NamedLatencyHistograms::default(),
            early_submit_starts: Mutex::new(VecDeque::new()),
        };
        engine.recover_found_block_outbox();
        engine
    }

    /// Snapshot of the current configuration; hot settings may change
    /// between calls (see `Config::with_hot_settings_from`).
    pub(crate) fn cfg(&self) -> Arc<Config> {
        self.live_cfg.get()
    }

    /// Settings as configured, without growth-mode overrides.
    pub(crate) fn base_cfg(&self) -> Config {
        self.base_cfg.lock().clone()
    }

    /// Applies the hot settings of `next` to the configured settings, then
    /// re-derives what the engine, its audit feedback and the validation
    /// engine run with; returns the names of configured settings that changed.
    pub fn reload_hot_config(&self, next: &Config) -> Vec<&'static str> {
        let mut base = self.base_cfg.lock();
        let (updated, changed) = base.with_hot_settings_from(next);
        *base = updated;
        self.apply_effective(&base);
        changed
    }

    pub(crate) fn growth_normal_size_rate(&self) -> Option<f64> {
        let rate = f64::from_bits(
            self.growth_rate_bits
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        rate.is_finite().then_some(rate)
    }

    pub(crate) fn record_growth_rate(&self, rate: f64) {
        self.growth_rate_bits
            .store(rate.to_bits(), std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn growth_mode_active(&self) -> bool {
        self.growth_level() > 0
    }

    pub(crate) fn growth_level(&self) -> u8 {
        self.growth_level.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Switches the growth level (see `crate::growth`); returns whether it
    /// changed.
    pub(crate) fn set_growth_level(&self, level: u8) -> bool {
        let base = self.base_cfg.lock();
        if self
            .growth_level
            .swap(level, std::sync::atomic::Ordering::SeqCst)
            == level
        {
            return false;
        }
        self.apply_effective(&base);
        tracing::info!(
            level,
            vardiff_target_shares = self.cfg().vardiff_target_shares,
            sample_rate = self.cfg().sample_rate,
            "growth mode changed"
        );
        true
    }

    fn apply_effective(&self, base: &Config) {
        let level = self.growth_level().min(base.max_growth_level());
        let effective = base.with_growth_level(level);
        self.live_cfg.apply_hot(&effective);
        self.validation.reload_hot_config(&effective);
    }

    pub(crate) fn attach_submit_regular_queue(&self, queue: Arc<QueueTracker>) {
        self.validation.attach_submit_regular_queue(queue);
    }

    #[cfg(test)]
    pub(crate) fn login(
        &self,
        conn_id: &str,
        address: String,
        worker: &str,
        protocol_version: u32,
        capabilities: Vec<String>,
    ) -> Result<pool_common::protocol::LoginResult> {
        self.login_with_hint(
            conn_id,
            address,
            worker,
            protocol_version,
            capabilities,
            None,
        )
    }

    /// Test helper: derives the peer IP from a `ip:port` connection id.
    #[cfg(test)]
    pub(crate) fn login_with_hint(
        &self,
        conn_id: &str,
        address: String,
        worker: &str,
        protocol_version: u32,
        capabilities: Vec<String>,
        difficulty_hint: Option<u64>,
    ) -> Result<pool_common::protocol::LoginResult> {
        let peer_ip = conn_id
            .parse::<std::net::SocketAddr>()
            .ok()
            .map(|addr| addr.ip());
        self.login_from_peer(
            conn_id,
            peer_ip,
            address,
            worker,
            protocol_version,
            capabilities,
            difficulty_hint,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn login_from_peer(
        &self,
        conn_id: &str,
        peer_ip: Option<IpAddr>,
        address: String,
        worker: &str,
        protocol_version: u32,
        capabilities: Vec<String>,
        difficulty_hint: Option<u64>,
    ) -> Result<pool_common::protocol::LoginResult> {
        let address = address.trim();
        if address.is_empty() || address.len() > 128 {
            return Err(anyhow!("invalid address"));
        }
        if let Err(err) = validate_miner_address_for_network(address, self.expected_address_network)
        {
            return Err(anyhow!("invalid address: {err}"));
        }

        let worker = normalize_worker_name(worker);
        let cap_set = capabilities.into_iter().collect::<BTreeSet<_>>();

        if protocol_version != STRATUM_PROTOCOL_VERSION_CURRENT {
            return Err(anyhow!("pool requires protocol_version 2"));
        }
        if !cap_set.contains(CAP_SUBMIT_CLAIMED_HASH) {
            return Err(anyhow!("pool requires submit_claimed_hash capability"));
        }
        let ip_group = source_ip_group(conn_id, peer_ip);
        let now_wall = SystemTime::now();
        // Address quarantine comes from address-level evidence; a source with
        // its own clean verified history on this address is not locked out.
        if !self.source_risk.is_established(address, &ip_group) {
            if let Some(state) = active_quarantine_state(self.store.as_ref(), address, now_wall)? {
                return Err(anyhow!(quarantine_error_message(Some(&state), now_wall)));
            }
        }
        let era = self.difficulty_era();
        let (min_diff, max_diff, initial_difficulty) = (era.min, era.max, era.initial);
        // From the fork, hints saved before it are Argon2-sized; ignore them
        // rather than clamp them up to the fork floor.
        let era_scaled = |difficulty: u64| !era.faraday || difficulty >= era.min;
        let difficulty_hint = difficulty_hint.filter(|hint| era_scaled(*hint));
        let mut difficulty = initial_difficulty;
        if self.cfg().enable_vardiff {
            if let Some((cached, updated_at)) = self
                .store
                .get_vardiff_hint(address, &worker)?
                .filter(|(cached, _)| era_scaled(*cached))
            {
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
        let address_bootstrap = if self.cfg().enable_vardiff && is_seine_dev_fee_address(address) {
            derive_address_vardiff_login_difficulty(
                self.store
                    .get_vardiff_hints_for_address(address, DEV_VARDIFF_BOOTSTRAP_HINT_LIMIT)?
                    .into_iter()
                    .filter(|(cached, _)| era_scaled(*cached))
                    .collect(),
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
                is_seine_dev_fee_address(address),
            );
        }
        if let Some(bootstrap) = address_bootstrap {
            difficulty = difficulty.max(bootstrap);
        }
        difficulty = difficulty.max(login_difficulty_floor(address, initial_difficulty));
        if era.faraday {
            difficulty = era.admit(difficulty);
        }

        let (address_good, _) = self.validation.address_sample_counts(address);
        let address_vouched =
            address_good >= self.cfg().source_full_strike_min_address_good.max(0) as u64;
        let source = self
            .source_risk
            .open_session_vouched(address, &ip_group, address_vouched)
            .map_err(|message| anyhow!(message))?;
        let mut sessions = self.sessions.lock();
        let replaced = sessions.insert(
            conn_id.to_string(),
            MinerSession {
                address: address.to_string(),
                worker: worker.clone(),
                difficulty,
                capabilities: cap_set,
                share_times: VecDeque::new(),
                last_share_at: None,
                last_unprocessed_at: None,
                last_difficulty_adjustment: None,
                last_difficulty_hint_write: None,
                source,
            },
        );
        drop(sessions);
        if let Some(replaced) = replaced {
            self.source_risk.close_session(replaced.source.session_id);
        }

        if self.cfg().enable_vardiff && is_seine_dev_fee_address(address) {
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

        Ok(build_login_result())
    }

    pub(crate) fn disconnect(&self, conn_id: &str) {
        let session = self.sessions.lock().remove(conn_id);
        if let Some(session) = session {
            self.source_risk.close_session(session.source.session_id);
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

    #[cfg(test)]
    pub(crate) fn submit(
        &self,
        conn_id: &str,
        job_id: String,
        nonce: u64,
        claimed_hash_hex: Option<String>,
    ) -> Result<SubmitAck> {
        self.submit_with_received_at(conn_id, job_id, nonce, claimed_hash_hex, Instant::now())
    }

    #[cfg(test)]
    pub(crate) fn submit_with_received_at(
        &self,
        conn_id: &str,
        job_id: String,
        nonce: u64,
        claimed_hash_hex: Option<String>,
        received_at: Instant,
    ) -> Result<SubmitAck> {
        self.submit_with_received_at_and_metadata(
            conn_id,
            job_id,
            nonce,
            claimed_hash_hex,
            received_at,
            SubmitMetadata::default(),
        )
    }

    pub(crate) fn submit_with_received_at_and_metadata(
        &self,
        conn_id: &str,
        job_id: String,
        nonce: u64,
        claimed_hash_hex: Option<String>,
        received_at: Instant,
        metadata: SubmitMetadata,
    ) -> Result<SubmitAck> {
        let session = {
            let sessions = self.sessions.lock();
            sessions
                .get(conn_id)
                .cloned()
                .ok_or_else(|| anyhow!("not logged in"))?
        };
        // Struck or banned sources are cut off before any store work; the
        // Stratum layer disconnects them on this error.
        if let Err(message) = self.source_risk.check_session(&session.source) {
            return Err(anyhow!(message));
        }
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
                    &metadata,
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
                    &metadata,
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
                    &metadata,
                );
                return Err(anyhow!("nonce out of assigned range"));
            }
        }
        let job = submit_job.job;

        // Dedupe on the template header, not the assignment id: a same-template
        // vardiff rebind reuses the nonce window under a new assignment id.
        let share_key = template_share_key(&job.header_base);
        let ledger_now = Instant::now();
        self.seen_in_memory
            .prune_if_due(ledger_now, |template_job_id| {
                self.jobs.template_retained(template_job_id)
            });
        if !self
            .seen_in_memory
            .reserve(&share_key, &job.id, nonce, ledger_now)
        {
            self.persist_rejected_share_for_error(
                &session,
                &job_id,
                nonce,
                share_difficulty,
                "duplicate share",
                &metadata,
            );
            return Err(anyhow!("duplicate share"));
        }

        let claimed_hash = match claimed_hash_hex {
            Some(ref value) if !value.trim().is_empty() => match parse_hash_hex(value) {
                Ok(hash) => Some(hash),
                Err(err) => {
                    self.seen_in_memory.release(&share_key, nonce);
                    self.persist_rejected_share_for_error(
                        &session,
                        &job_id,
                        nonce,
                        share_difficulty,
                        &err,
                        &metadata,
                    );
                    return Err(anyhow!(err));
                }
            },
            _ => None,
        };

        if claimed_hash.is_none() {
            self.seen_in_memory.release(&share_key, nonce);
            self.persist_rejected_share_for_error(
                &session,
                &job_id,
                nonce,
                share_difficulty,
                "claimed hash required",
                &metadata,
            );
            return Err(anyhow!("claimed hash required"));
        }

        match self.store.try_claim_share(&job_id, &share_key, nonce) {
            Ok(true) => {}
            Ok(false) => {
                // Another holder owns the persisted claim; ours was never taken.
                self.seen_in_memory.release(&share_key, nonce);
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    "duplicate share",
                    &metadata,
                );
                return Err(anyhow!("duplicate share"));
            }
            Err(err) => {
                self.seen_in_memory.release(&share_key, nonce);
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id,
                    nonce,
                    share_difficulty,
                    &err.to_string(),
                    &metadata,
                );
                return Err(err);
            }
        };
        let job_id_for_share = job_id.clone();
        let mut release_claim_on_error = true;

        let source_established = self.source_risk.pair_established(&session.source);
        let result = (|| -> Result<SubmitAck> {
            let now_wall = SystemTime::now();
            // One risk read serves both the quarantine check and the
            // force-verify decision below.
            let risk = self.store.submit_risk(&session.address)?;
            let quarantine = if source_established {
                None
            } else {
                risk.active_quarantine(now_wall)
            };
            if let Some(state) = quarantine {
                let message = quarantine_error_message(Some(state), now_wall);
                release_claim_on_error = false;
                self.persist_rejected_share_for_error(
                    &session,
                    &job_id_for_share,
                    nonce,
                    share_difficulty,
                    &message,
                    &metadata,
                );
                return Err(anyhow!(message));
            }

            let share_target = difficulty_to_target(share_difficulty);

            let candidate_claim = claimed_hash
                .map(|hash| check_target(hash, job.network_target))
                .unwrap_or(false);

            let risk_force_verify = risk.force_verify_at(now_wall);
            // Only sources with their own verified history on this address
            // add to its persisted invalid count (strike leniency history).
            let source_vouched = self.source_risk.source_good(&session.source)
                >= self.cfg().source_full_strike_min_source_good.max(0) as u64;
            let count_invalid_at_address = source_vouched || !self.cfg().source_penalties_enabled;
            let (address_good, address_invalid) =
                self.validation.address_sample_counts(&session.address);
            let plan = self.validation.plan_regular_submit(&session.address);
            // From the fork a share costs one BLAKE2b compression to check, so
            // every share is hashed inline: no sampling, no verifier queue.
            let cheap_pow = faraday::is_faraday(job.height);
            let sync_full_verify =
                cheap_pow || candidate_claim || risk_force_verify || plan.sync_full_verify;

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
                    attributed: self.cfg().source_penalties_enabled,
                    count_invalid_at_address,
                };

                // A block claim from an established source is hashed inline
                // rather than queued behind other sources' candidate checks;
                // it is a one-shot privilege, since a false claim strikes the
                // source and removes it.
                let inline_priority = candidate_claim && source_established;
                // The same claim also goes to the daemon now rather than after
                // the pool's hash: the daemon re-verifies the PoW before it
                // accepts, so the two checks overlap instead of running back
                // to back. The pool's own verdict still decides the share.
                let mut early_submit = match claimed_hash {
                    Some(hash) if inline_priority => {
                        self.start_early_block_submit(&session, &job, nonce, hash)
                    }
                    _ => None,
                };
                // This submit now holds a regular submit worker until the
                // verifier answers. Bound how many may do that at once so a
                // few addresses under forced verification cannot occupy every
                // worker; past the bound only their shares are told to retry.
                // Block candidates run on their own lane and are exempt.
                let _sync_verify_permit = if candidate_claim || cheap_pow {
                    None
                } else {
                    match self.admission.sync_verify().try_acquire() {
                        Some(permit) => Some(permit),
                        None => {
                            self.record_unprocessed_submit(
                                conn_id,
                                UnprocessedReason::SyncVerifyBusy,
                            );
                            return Err(anyhow!("server busy, retry"));
                        }
                    }
                };
                let validate_started_at = Instant::now();
                let validation =
                    match self.validate_task(task, candidate_claim, inline_priority || cheap_pow) {
                        Ok(v) => v,
                        Err(err) => {
                            // Busy/timeout: the share was never judged, so let the
                            // miner resubmit it.
                            let reason_code = canonical_share_reject_reason(&err.to_string());
                            release_claim_on_error = is_retryable_reject_reason(reason_code);
                            let unprocessed = match reason_code {
                                "server busy" => Some(UnprocessedReason::ValidationQueueFull),
                                "validation timeout" => Some(UnprocessedReason::ValidationTimeout),
                                _ => None,
                            };
                            if let Some(reason) = unprocessed {
                                // Like the pre-queue admission rejects, these are
                                // not share outcomes: count them (and feed vardiff)
                                // instead of writing a row under saturation.
                                self.record_unprocessed_submit(conn_id, reason);
                                return Err(err);
                            }
                            let created_at = SystemTime::now();
                            self.persist_rejected_share_with_evidence(
                                &session,
                                &job_id_for_share,
                                nonce,
                                share_difficulty,
                                RejectedShareEvidence {
                                    claimed_hash_hex: claimed_hash_hex.as_deref(),
                                    replay: Some(ShareReplayData {
                                        job_id: job_id_for_share.clone(),
                                        header_base: job.header_base.clone(),
                                        network_target: job.network_target,
                                        created_at,
                                    }),
                                    reject_reason: Some(&err.to_string()),
                                },
                                &metadata,
                            );
                            return Err(err);
                        }
                    };

                if !validation.accepted {
                    let reason = validation.reject_reason.unwrap_or("invalid share");
                    release_claim_on_error =
                        is_retryable_reject_reason(canonical_share_reject_reason(reason));
                    if reason == PROVEN_FAKE_REJECT_REASON {
                        // A false block claim is a full strike at once: an
                        // honest error would also have to land below the
                        // network target, and each claim costs a full hash in
                        // the scarce candidate lane.
                        penalize_proven_fake(
                            &self.cfg(),
                            &self.source_risk,
                            self.store.as_ref(),
                            Some(&self.validation),
                            &session.source,
                            ProvenFake {
                                reason: if candidate_claim {
                                    "false block candidate claim"
                                } else {
                                    PROVEN_FAKE_REJECT_REASON
                                },
                                immediate: candidate_claim,
                                address_good,
                                address_invalid,
                                count_at_address: count_invalid_at_address,
                            },
                        );
                    }
                    let created_at = SystemTime::now();
                    self.persist_rejected_share_with_evidence(
                        &session,
                        &job_id_for_share,
                        nonce,
                        share_difficulty,
                        RejectedShareEvidence {
                            claimed_hash_hex: claimed_hash_hex.as_deref(),
                            replay: Some(ShareReplayData {
                                job_id: job_id_for_share.clone(),
                                header_base: job.header_base.clone(),
                                network_target: job.network_target,
                                created_at,
                            }),
                            reject_reason: Some(reason),
                        },
                        &metadata,
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

                self.source_risk.record_verified(&session.source);
                let mut block_hash = None;
                let mut block_accepted = false;
                if validation.is_block_candidate {
                    let validated_at = Instant::now();
                    self.block_candidate_latency.record(
                        "validate",
                        validated_at.saturating_duration_since(validate_started_at),
                    );
                    self.block_candidate_latency.record(
                        "received_to_validated",
                        validated_at.saturating_duration_since(received_at),
                    );
                    let computed_hash = hex::encode(validation.hash);
                    block_hash = Some(computed_hash.clone());
                    let (staged_found, submitted, submit_elapsed, submit_started_at) =
                        if let Some(early) = early_submit.take() {
                            let wait_started_at = Instant::now();
                            let outcome = early.wait();
                            self.block_candidate_latency
                                .record("early_submit_wait", wait_started_at.elapsed());
                            outcome
                        } else {
                            let staged_found = FoundBlockRecord {
                                height: job.height,
                                hash: computed_hash.clone(),
                                difficulty: job.network_difficulty,
                                reward: 0,
                                finder: session.address.clone(),
                                finder_worker: session.worker.clone(),
                                timestamp: SystemTime::now(),
                            };
                            if let Err(err) = self.stage_found_block_submission(&staged_found) {
                                release_claim_on_error = false;
                                let created_at = SystemTime::now();
                                self.persist_rejected_share_with_evidence(
                                    &session,
                                    &job_id_for_share,
                                    nonce,
                                    share_difficulty,
                                    RejectedShareEvidence {
                                        claimed_hash_hex: claimed_hash_hex.as_deref(),
                                        replay: Some(ShareReplayData {
                                            job_id: job_id_for_share.clone(),
                                            header_base: job.header_base.clone(),
                                            network_target: job.network_target,
                                            created_at,
                                        }),
                                        reject_reason: Some(&err.to_string()),
                                    },
                                    &metadata,
                                );
                                return Err(err);
                            }

                            let submit_started_at = Instant::now();
                            self.block_candidate_latency.record(
                                "stage_journal",
                                submit_started_at.saturating_duration_since(validated_at),
                            );
                            let submitted = self.node.submit_block(&job, nonce);
                            let submit_elapsed = submit_started_at.elapsed();
                            (staged_found, submitted, submit_elapsed, submit_started_at)
                        };
                    self.block_submit_latency.record(
                        match &submitted {
                            Ok(submit) if submit.accepted => "accepted",
                            Ok(_) => "rejected",
                            Err(_) => "error",
                        },
                        submit_elapsed,
                    );
                    let submit = match submitted {
                        Ok(submit) => submit,
                        Err(err) => {
                            release_claim_on_error = false;
                            let created_at = SystemTime::now();
                            self.persist_rejected_share_with_evidence(
                                &session,
                                &job_id_for_share,
                                nonce,
                                share_difficulty,
                                RejectedShareEvidence {
                                    claimed_hash_hex: claimed_hash_hex.as_deref(),
                                    replay: Some(ShareReplayData {
                                        job_id: job_id_for_share.clone(),
                                        header_base: job.header_base.clone(),
                                        network_target: job.network_target,
                                        created_at,
                                    }),
                                    reject_reason: Some(&err.to_string()),
                                },
                                &metadata,
                            );
                            return Err(err);
                        }
                    };
                    block_accepted = submit.accepted;
                    if let Some(network_hash) = submit.hash.clone() {
                        block_hash = Some(network_hash);
                    }

                    if block_accepted {
                        let received_to_accepted = received_at.elapsed();
                        self.block_candidate_latency
                            .record("received_to_daemon_accepted", received_to_accepted);
                        let daemon_http_overhead = submit.daemon_total_us.map(|daemon_us| {
                            submit_elapsed.saturating_sub(Duration::from_micros(daemon_us))
                        });
                        if let Some(overhead) = daemon_http_overhead {
                            self.block_candidate_latency
                                .record("daemon_http_overhead", overhead);
                        }
                        let mut persisted_found = staged_found.clone();
                        persisted_found.height = submit.height.unwrap_or(staged_found.height);
                        if let Some(network_hash) = submit.hash {
                            persisted_found.hash = network_hash;
                        }
                        self.jobs.note_block_accepted(persisted_found.height);
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
                            template_age_seconds = job.template_created_at.elapsed().as_secs(),
                            received_to_accepted_ms = received_to_accepted.as_millis() as u64,
                            validate_ms = validated_at
                                .saturating_duration_since(validate_started_at)
                                .as_millis() as u64,
                            stage_journal_ms = submit_started_at
                                .saturating_duration_since(validated_at)
                                .as_millis() as u64,
                            daemon_submit_ms = submit_elapsed.as_millis() as u64,
                            daemon_total_us = submit.daemon_total_us,
                            miner_found_to_send_us = metadata.found_to_send_us,
                            "POOL BLOCK FOUND"
                        );
                        let persist_started_at = Instant::now();
                        self.persist_found_block(&staged_found, persisted_found);
                        self.block_candidate_latency
                            .record("persist_found_block", persist_started_at.elapsed());
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
                // From here on a failure may still have committed the row
                // (an error after COMMIT reached the server), so the claim is
                // kept: releasing it could credit a resubmit twice.
                release_claim_on_error = false;
                self.store.add_share_with_replay_metadata(
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
                    &metadata,
                )?;
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
                        &metadata,
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
                    &metadata,
                );
                return Err(anyhow!("low difficulty share"));
            }

            // Not-yet-verified addresses are verified on the probation lane.
            // If it is saturated the share is still accepted (seine never
            // resubmits busy rejects), but it carries no payout weight until
            // the address has verified shares, and it joins the source's
            // degraded cohort for a later review.
            let mut probation_reserved = false;
            let mut probation_saturated = false;
            if plan.probation {
                match self
                    .validation
                    .reserve_probation(&session.address, &session.source.ip_group)
                {
                    ProbationReservation::Reserved => probation_reserved = true,
                    ProbationReservation::AlreadyPending => {}
                    ProbationReservation::Saturated => probation_saturated = true,
                }
            }

            let created_at = SystemTime::now();
            // As above: once the insert is attempted, keep the claim.
            release_claim_on_error = false;
            let persisted = self.store.add_share_with_replay_metadata_and_id(
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
                &metadata,
            );
            let share_id = match persisted {
                Ok(share_id) => share_id,
                Err(err) => {
                    if probation_reserved {
                        self.validation
                            .cancel_probation(&session.address, &session.source.ip_group);
                    }
                    return Err(err);
                }
            };
            // Shares accepted while sampling is reduced or off are queued for
            // a later per-source review instead of escaping verification.
            self.source_risk.record_provisional(
                &session.source,
                share_id,
                created_at,
                share_difficulty,
                plan.overload_mode != OverloadMode::Normal || probation_saturated,
            );
            let probation_share = probation_reserved.then(|| PendingAuditShare {
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
            });

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
            // Enqueued after the hot accept so a fast audit cannot finish
            // before the share is in the validation state.
            if let Some(share) = probation_share {
                self.validation
                    .enqueue_probation_audit(share, &session.source.ip_group);
            } else if !plan.enqueue_audit && self.source_risk.take_boost(&session.source) {
                // Soft-struck source: audit its next shares on the audit lane.
                self.validation
                    .enqueue_source_boost_audit(PendingAuditShare {
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
                    });
            }

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

        // Credited shares and shares rejected on proof keep both the in-memory
        // and persisted template-scoped claims; only retryable failures (store
        // errors, busy, timeout) release them.
        if result.is_err() && release_claim_on_error {
            if let Err(err) = self.store.release_share_claim(&share_key, nonce) {
                tracing::warn!(
                    job_id = %job_id,
                    share_key = %share_key,
                    nonce,
                    error = %err,
                    "failed to release share claim after retryable submit failure"
                );
            }
            self.seen_in_memory.release(&share_key, nonce);
        }
        result
    }

    fn trigger_forced_validation_quarantine(&self, address: &str, reason: &str) {
        if let Some(quarantined_until) =
            escalate_forced_validation_quarantine(&self.cfg(), self.store.as_ref(), address, reason)
        {
            self.validation
                .schedule_forced_review_after(address, quarantined_until);
        }
    }

    /// Whether this connection's source has clean verified history on its
    /// address (eligible for the priority block-candidate lane).
    pub(crate) fn source_established(&self, conn_id: &str) -> bool {
        let source = self
            .sessions
            .lock()
            .get(conn_id)
            .map(|session| session.source.clone());
        source.is_some_and(|source| self.source_risk.pair_established(&source))
    }

    /// Whether this connection's source has been cut off (struck session,
    /// or banned pair / IP group). The Stratum layer disconnects on true.
    pub(crate) fn source_blocked(&self, conn_id: &str) -> bool {
        let source = self
            .sessions
            .lock()
            .get(conn_id)
            .map(|session| session.source.clone());
        source.is_some_and(|source| self.source_risk.session_blocked(&source))
    }

    fn persist_rejected_share(
        &self,
        session: &MinerSession,
        job_id: &str,
        nonce: u64,
        difficulty: u64,
        reject_reason: Option<&str>,
        metadata: &SubmitMetadata,
    ) {
        self.persist_rejected_share_with_evidence(
            session,
            job_id,
            nonce,
            difficulty,
            RejectedShareEvidence {
                reject_reason,
                ..RejectedShareEvidence::default()
            },
            metadata,
        );
    }

    fn persist_rejected_share_with_evidence(
        &self,
        session: &MinerSession,
        job_id: &str,
        nonce: u64,
        difficulty: u64,
        evidence: RejectedShareEvidence<'_>,
        metadata: &SubmitMetadata,
    ) {
        let claimed_hash = evidence
            .claimed_hash_hex
            .and_then(|value| parse_hash_hex(value).ok())
            .map(hex::encode);
        if let Err(err) = self.store.add_share_with_replay_metadata(
            ShareRecord {
                job_id: job_id.to_string(),
                miner: session.address.clone(),
                worker: session.worker.clone(),
                difficulty: difficulty.max(1),
                nonce,
                status: SHARE_STATUS_REJECTED,
                was_sampled: false,
                block_hash: None,
                claimed_hash,
                reject_reason: evidence
                    .reject_reason
                    .map(|value| canonical_share_reject_reason(value).to_string()),
                created_at: SystemTime::now(),
            },
            evidence.replay,
            metadata,
        ) {
            tracing::warn!(
                address = %session.address,
                worker = %session.worker,
                job_id = %job_id,
                nonce,
                error = %err,
                "failed to persist rejected share"
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
        metadata: &SubmitMetadata,
    ) {
        self.persist_rejected_share(session, job_id, nonce, difficulty, Some(error), metadata);
    }

    pub(crate) fn recover_found_block_outbox(&self) {
        let path = found_block_outbox_path();
        let _guard = self.found_block_outbox_lock.lock();
        self.recover_found_block_outbox_locked(&path);
        // A peer instance's outbox is replayed only while its owner is gone,
        // so a block it is submitting right now is never handled twice.
        for peer in found_block_outbox_peers() {
            match OutboxLock::try_acquire(&peer) {
                Ok(Some(_peer_lock)) => self.recover_found_block_outbox_locked(&peer),
                Ok(None) => {}
                Err(err) => tracing::warn!(
                    path = %peer.display(),
                    error = %err,
                    "cannot lock peer found-block outbox"
                ),
            }
        }
    }

    /// Stages the journal entry and sends an established source's block claim
    /// to the daemon on its own thread. Returns `None` (the caller submits
    /// after verifying, as before) when disabled, over the rate bound, or if
    /// staging fails.
    fn start_early_block_submit(
        &self,
        session: &MinerSession,
        job: &Job,
        nonce: u64,
        claimed_hash: [u8; 32],
    ) -> Option<EarlyBlockSubmit<'_>> {
        if !self.cfg().parallel_candidate_submit {
            return None;
        }
        {
            let now = Instant::now();
            let mut starts = self.early_submit_starts.lock();
            while starts
                .front()
                .is_some_and(|started| now.duration_since(*started) >= EARLY_SUBMIT_WINDOW)
            {
                starts.pop_front();
            }
            if starts.len() >= EARLY_SUBMIT_MAX_PER_WINDOW {
                tracing::warn!(
                    finder = %session.address,
                    worker = %session.worker,
                    "early block submit rate bound reached; submitting after verify"
                );
                return None;
            }
            starts.push_back(now);
        }

        let staged = FoundBlockRecord {
            height: job.height,
            hash: hex::encode(claimed_hash),
            difficulty: job.network_difficulty,
            reward: 0,
            finder: session.address.clone(),
            finder_worker: session.worker.clone(),
            timestamp: SystemTime::now(),
        };
        if let Err(err) = self.stage_found_block_submission(&staged) {
            tracing::warn!(error = %err, "failed staging early block submission");
            return None;
        }
        let node = Arc::clone(&self.node);
        let job = job.clone();
        let started_at = Instant::now();
        let spawned = std::thread::Builder::new()
            .name("early-block-submit".to_string())
            .spawn(move || {
                let submitted = node.submit_block(&job, nonce);
                (submitted, started_at.elapsed())
            });
        match spawned {
            Ok(handle) => Some(EarlyBlockSubmit {
                engine: self,
                staged,
                started_at,
                handle: Some(handle),
            }),
            Err(err) => {
                tracing::warn!(error = %err, "failed starting early block submission");
                if let Err(err) = self.clear_found_block_submission(&staged) {
                    tracing::warn!(error = %err, "failed clearing early block journal entry");
                }
                None
            }
        }
    }

    /// Settles an early submission whose share the pool's verify did not
    /// confirm. The daemon accepting it means consensus verified the block,
    /// so it is recorded as found (and flagged: the two verifiers disagreed);
    /// otherwise the staged journal entry is dropped.
    fn reconcile_unclaimed_early_submit(
        &self,
        staged: &FoundBlockRecord,
        submitted: Result<BlockSubmitResponse>,
    ) {
        match submitted {
            Ok(submit) if submit.accepted => {
                let mut found = staged.clone();
                found.height = submit.height.unwrap_or(staged.height);
                if let Some(hash) = submit.hash {
                    found.hash = hash;
                }
                tracing::error!(
                    height = found.height,
                    hash = %found.hash,
                    finder = %staged.finder,
                    worker = %staged.finder_worker,
                    "daemon accepted an early block submission the pool did not verify as a block"
                );
                self.jobs.note_block_accepted(found.height);
                self.persist_found_block(staged, found);
            }
            Ok(_) => {
                if let Err(err) = self.clear_found_block_submission(staged) {
                    tracing::warn!(
                        height = staged.height,
                        hash = %staged.hash,
                        error = %err,
                        "failed clearing rejected early block journal entry"
                    );
                }
            }
            Err(err) => {
                // Unknown outcome: the journal entry stays, and recovery
                // replays it only if the block is on chain.
                tracing::warn!(
                    height = staged.height,
                    hash = %staged.hash,
                    error = %err,
                    "early block submission failed for a claim the pool did not verify"
                );
            }
        }
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

    fn validate_task(
        &self,
        task: ValidationTask,
        candidate: bool,
        inline_priority: bool,
    ) -> Result<ValidationResult> {
        if inline_priority {
            return Ok(self.validation.process_inline(task));
        }
        if let Some(rx) = self.validation.submit(task.clone(), candidate) {
            let computed = rx
                .recv_timeout(VALIDATION_WAIT_TIMEOUT)
                .map_err(|_| anyhow!("validation timeout"));
            return computed.map(|computed| self.validation.complete_result(&task, computed));
        }

        if candidate {
            return Ok(self.validation.process_inline(task));
        }

        Err(anyhow!("server busy, retry"))
    }

    pub(crate) fn block_submit_histograms(&self) -> BTreeMap<String, LatencyHistogramSnapshot> {
        self.block_submit_latency.snapshot()
    }

    pub(crate) fn block_candidate_histograms(&self) -> BTreeMap<String, LatencyHistogramSnapshot> {
        self.block_candidate_latency.snapshot()
    }

    pub(crate) fn session_difficulty(&self, conn_id: &str) -> Option<u64> {
        self.sessions
            .lock()
            .get(conn_id)
            .map(|session| session.difficulty)
    }

    fn difficulty_era(&self) -> DifficultyEra {
        if self.jobs.current_height().is_some_and(faraday::is_faraday) {
            return DifficultyEra {
                min: faraday::FARADAY_MIN_SHARE_DIFFICULTY,
                max: faraday::FARADAY_MAX_SHARE_DIFFICULTY,
                initial: faraday::FARADAY_INITIAL_SHARE_DIFFICULTY,
                faraday: true,
            };
        }
        let cfg = self.cfg();
        let min = cfg.min_share_difficulty.max(1);
        let max = cfg.max_share_difficulty.max(min);
        DifficultyEra {
            min,
            max,
            initial: cfg.initial_share_difficulty.clamp(min, max),
            faraday: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn retarget_on_job_if_needed(&self, conn_id: &str) -> Option<u64> {
        let (difficulty, hint) = self.retarget_on_job(conn_id)?;
        if let Some(hint) = hint {
            self.persist_vardiff_hint(hint);
        }
        Some(difficulty)
    }

    /// The in-memory half of a job-tick retarget: the connection's next
    /// difficulty, plus a vardiff hint when one is due. It never touches the
    /// store, so a job can be sent before the hint is persisted.
    pub(crate) fn retarget_on_job(&self, conn_id: &str) -> Option<(u64, Option<VardiffHint>)> {
        let era = self.difficulty_era();
        if !self.cfg().enable_vardiff {
            if era.faraday {
                let mut sessions = self.sessions.lock();
                let session = sessions.get_mut(conn_id)?;
                session.difficulty = era.admit(session.difficulty);
                return Some((session.difficulty, None));
            }
            return self
                .session_difficulty(conn_id)
                .map(|difficulty| (difficulty, None));
        }

        let mut hint_to_write: Option<VardiffHint> = None;
        let mut sessions = self.sessions.lock();
        let session = sessions.get_mut(conn_id)?;
        let (min_diff, max_diff) = (era.min, era.max);
        session.difficulty = if era.faraday {
            era.admit(session.difficulty)
        } else {
            session.difficulty.clamp(min_diff, max_diff)
        };

        // Keep dev-fee sessions on submit-only retargeting so their long idle
        // periods do not collapse difficulty between the brief fee windows.
        if !is_seine_dev_fee_address(&session.address) {
            let now = Instant::now();
            let target_interval = vardiff_target_interval_seconds(&self.cfg());
            let tolerance = self.cfg().vardiff_tolerance.clamp(0.01, 0.95);
            // Idle is measured from the last share of any kind, including one
            // the pool was too busy to process: that miner is not idle.
            if let Some(last) = session.last_share_at {
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
                        && vardiff_change_is_material(session.difficulty, next_diff)
                    {
                        let retarget_interval =
                            vardiff_retarget_interval_for_direction(&self.cfg(), false);
                        let can_retarget = session
                            .last_difficulty_adjustment
                            .is_none_or(|last| now.duration_since(last) >= retarget_interval);
                        if can_retarget && self.vardiff_downward_blocked(session, now) {
                            self.admission.note_vardiff_downward_held();
                        } else if can_retarget {
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
                hint_to_write = Some(VardiffHint {
                    address: session.address.clone(),
                    worker: session.worker.clone(),
                    difficulty: session.difficulty,
                });
            }
        }

        Some((session.difficulty, hint_to_write))
    }

    /// Persists a vardiff hint from `retarget_on_job`. Blocking store call.
    pub(crate) fn persist_vardiff_hint(&self, hint: VardiffHint) {
        if let Err(err) = self.store.upsert_vardiff_hint(
            &hint.address,
            &hint.worker,
            hint.difficulty,
            SystemTime::now(),
        ) {
            tracing::warn!(
                address = %hint.address,
                worker = %hint.worker,
                error = %err,
                "failed to persist vardiff hint on job tick"
            );
        }
    }

    pub(crate) fn session_supports_capability(&self, conn_id: &str, capability: &str) -> bool {
        self.sessions
            .lock()
            .get(conn_id)
            .is_some_and(|session| session.capabilities.contains(capability))
    }

    pub(crate) fn admission(&self) -> &AdmissionState {
        &self.admission
    }

    pub(crate) fn admission_handle(&self) -> Arc<AdmissionState> {
        Arc::clone(&self.admission)
    }

    /// Records a submit the pool answered without judging it: queue full,
    /// shed, rate limited, or no verification capacity. It is still evidence
    /// that the miner found a share, so it counts toward vardiff's share-rate
    /// estimate. A connection flooding a saturated pool therefore has its
    /// difficulty raised, which relieves the pool, instead of seeing no
    /// accepted shares and being lowered, which feeds the flood.
    ///
    /// Never lowers difficulty and never touches the store, so async Stratum
    /// tasks may call it directly. Returns the new difficulty if it went up;
    /// the caller must send the miner a job at that difficulty.
    pub(crate) fn record_unprocessed_submit(
        &self,
        conn_id: &str,
        reason: UnprocessedReason,
    ) -> Option<u64> {
        let now = Instant::now();
        self.admission.record(reason, now);
        if !self.cfg().enable_vardiff {
            return None;
        }
        let mut sessions = self.sessions.lock();
        let session = sessions.get_mut(conn_id)?;
        if self.vardiff_observe_share(session, now, true) {
            self.admission.note_vardiff_busy_raise();
            Some(session.difficulty)
        } else {
            None
        }
    }

    /// Pool-side reasons not to lower a session's difficulty right now: the
    /// regular submit lane was saturated recently, or this session's own
    /// submits were recently answered unprocessed. Either way the missing
    /// accepted shares say nothing about the miner's hashrate.
    fn vardiff_downward_blocked(&self, session: &MinerSession, now: Instant) -> bool {
        if self.admission.pressure_active(now) {
            return true;
        }
        let window = self
            .cfg()
            .vardiff_window_duration()
            .max(Duration::from_secs(30));
        session
            .last_unprocessed_at
            .is_some_and(|at| now.saturating_duration_since(at) < window)
    }

    /// Adds one share to the session's rate estimate and retargets. The share
    /// is either accepted or `unprocessed` (found, but the pool could not
    /// process it); unprocessed evidence may only raise difficulty. Returns
    /// whether the difficulty went up.
    fn vardiff_observe_share(
        &self,
        session: &mut MinerSession,
        now: Instant,
        unprocessed: bool,
    ) -> bool {
        let era = self.difficulty_era();
        let (min_diff, max_diff) = (era.min, era.max);
        session.difficulty = if era.faraday {
            era.admit(session.difficulty)
        } else {
            session.difficulty.clamp(min_diff, max_diff)
        };

        let window = self
            .cfg()
            .vardiff_window_duration()
            .max(Duration::from_secs(30));
        let target_interval = vardiff_target_interval_seconds(&self.cfg());
        let tolerance = self.cfg().vardiff_tolerance.clamp(0.01, 0.95);

        let last_share_age = session.last_share_at.map(|last| now.duration_since(last));
        session.last_share_at = Some(now);
        if unprocessed {
            session.last_unprocessed_at = Some(now);
        }
        session.share_times.push_back(now);
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while session.share_times.front().is_some_and(|ts| *ts < cutoff) {
            session.share_times.pop_front();
        }

        let mut next_diff = session.difficulty;
        let mut observed_interval = 0.0;
        let mut kind = "observed";
        if session.share_times.len() >= VARDIFF_MIN_SAMPLE_COUNT {
            if let Some(oldest) = session.share_times.front().copied() {
                let elapsed = now.duration_since(oldest).as_secs_f64();
                if elapsed >= 1.0 {
                    observed_interval = elapsed / ((session.share_times.len() - 1) as f64);
                    let lower = target_interval * (1.0 - tolerance);
                    let upper = target_interval * (1.0 + tolerance);
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
                }
            }
        } else if let Some(age) = last_share_age {
            // If the previous share fell out of the vardiff window, we can still
            // decay difficulty based on idle time before this share — but only
            // if the gap is within the window. Gaps longer than the window
            // indicate an intermittent session (e.g. dev fee), not a struggling
            // miner.
            observed_interval = age.as_secs_f64();
            let window_secs = window.as_secs_f64();
            let upper = target_interval * (1.0 + tolerance);
            if observed_interval <= window_secs && observed_interval > upper {
                let raw_ratio = observed_interval / target_interval;
                let ratio = raw_ratio
                    .powf(VARDIFF_RATIO_DAMPING_EXPONENT)
                    .clamp(VARDIFF_MIN_ADJUSTMENT_FACTOR, VARDIFF_MAX_ADJUSTMENT_FACTOR);
                next_diff = ((session.difficulty as f64) / ratio).floor().max(1.0) as u64;
                kind = "idle";
            }
        }

        next_diff = next_diff.clamp(min_diff, max_diff);
        if next_diff == session.difficulty
            || !vardiff_change_is_material(session.difficulty, next_diff)
        {
            return false;
        }
        let increasing = next_diff > session.difficulty;
        let retarget_interval = vardiff_retarget_interval_for_direction(&self.cfg(), increasing);
        let can_retarget = session
            .last_difficulty_adjustment
            .is_none_or(|last| now.duration_since(last) >= retarget_interval);
        if !can_retarget {
            return false;
        }
        if !increasing {
            if unprocessed {
                return false;
            }
            if self.vardiff_downward_blocked(session, now) {
                self.admission.note_vardiff_downward_held();
                return false;
            }
        }
        let miner = compact_address(&session.address);
        tracing::debug!(
            "vardiff {:>4} -> {:>4} ({} {:>5.1}s, target {:>4.0}s, miner {}{})",
            session.difficulty,
            next_diff,
            kind,
            observed_interval,
            target_interval,
            miner,
            if unprocessed {
                ", unprocessed submit"
            } else {
                ""
            }
        );
        session.difficulty = next_diff;
        session.last_difficulty_adjustment = Some(now);
        increasing
    }

    fn note_share_and_maybe_adjust_difficulty(&self, conn_id: &str) -> u64 {
        if !self.cfg().enable_vardiff {
            return self
                .sessions
                .lock()
                .get(conn_id)
                .map(|session| session.difficulty)
                .unwrap_or(self.cfg().initial_share_difficulty);
        }

        let mut hint_to_write: Option<(String, String, u64)> = None;
        let mut sessions = self.sessions.lock();
        let Some(session) = sessions.get_mut(conn_id) else {
            return self.cfg().initial_share_difficulty;
        };

        let now = Instant::now();
        self.vardiff_observe_share(session, now, false);

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
        Duration::from_secs(20)
    }
}

fn vardiff_change_is_material(current: u64, next: u64) -> bool {
    if current == 0 || current == next {
        return false;
    }
    let delta = current.abs_diff(next) as f64;
    let baseline = current.max(1) as f64;
    delta / baseline >= 0.08
}

fn compact_address(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.len() <= 12 {
        return trimmed.to_string();
    }
    format!("{}...{}", &trimmed[..6], &trimmed[trimmed.len() - 6..])
}

fn found_block_outbox_peers() -> Vec<PathBuf> {
    env::var(FOUND_BLOCK_OUTBOX_PEERS_ENV)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect()
}

/// With peers configured, locks this instance's outbox for its lifetime so
/// peers only replay it once this process has exited. Retries briefly in
/// case a peer is replaying it right now.
fn claim_found_block_outbox() -> Option<OutboxLock> {
    if found_block_outbox_peers().is_empty() {
        return None;
    }
    let path = found_block_outbox_path();
    for _ in 0..OUTBOX_LOCK_ATTEMPTS {
        match OutboxLock::try_acquire(&path) {
            Ok(Some(lock)) => return Some(lock),
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(err) => {
                tracing::error!(path = %path.display(), error = %err, "cannot lock found-block outbox");
                return None;
            }
        }
    }
    tracing::error!(
        path = %path.display(),
        "found-block outbox is locked by another live process; two instances share one outbox"
    );
    None
}

/// Exclusive `flock` on `<outbox>.lock`, released when dropped (or when the
/// process exits, however it exits).
struct OutboxLock {
    _file: std::fs::File,
}

impl OutboxLock {
    fn try_acquire(outbox: &Path) -> std::io::Result<Option<Self>> {
        use std::os::fd::AsRawFd;
        let mut lock_path = outbox.as_os_str().to_owned();
        lock_path.push(".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(PathBuf::from(lock_path))?;
        // Safety: flock on a descriptor this function owns.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            return Ok(Some(Self { _file: file }));
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            Ok(None)
        } else {
            Err(err)
        }
    }
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

#[cfg(test)]
#[derive(Default)]
pub(crate) struct InMemoryStore {
    seen: Mutex<HashSet<(String, u64)>>,
    shares: Mutex<Vec<ShareRecord>>,
    /// (miner_version, backend) recorded with each metadata-carrying insert.
    share_metadata: Mutex<Vec<(Option<String>, Option<String>)>>,
    blocks: Mutex<Vec<FoundBlockRecord>>,
    address_risk: Mutex<HashMap<String, AddressRiskState>>,
    vardiff_hints: Mutex<HashMap<(String, String), (u64, SystemTime)>>,
    /// Next share insert commits but reports an error, like a connection
    /// lost after COMMIT reached the server.
    ambiguous_commit_once: Mutex<bool>,
}

#[cfg(test)]
impl InMemoryStore {
    pub(crate) fn shares(&self) -> Vec<ShareRecord> {
        self.shares.lock().clone()
    }

    pub(crate) fn share_metadata(&self) -> Vec<(Option<String>, Option<String>)> {
        self.share_metadata.lock().clone()
    }

    pub(crate) fn fail_next_share_insert_after_commit(&self) {
        *self.ambiguous_commit_once.lock() = true;
    }

    pub(crate) fn blocks(&self) -> Vec<FoundBlockRecord> {
        self.blocks.lock().clone()
    }

    pub(crate) fn seed_vardiff_hint(
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

    pub(crate) fn vardiff_hint(&self, address: &str, worker: &str) -> Option<(u64, SystemTime)> {
        self.vardiff_hints
            .lock()
            .get(&(address.to_string(), worker.to_string()))
            .copied()
    }

    /// Test audit completion: applies only to still-provisional shares.
    pub(crate) fn complete_audit(&self, share_id: i64, status: &'static str) -> bool {
        let mut shares = self.shares.lock();
        let Some(share) = usize::try_from(share_id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .and_then(|idx| shares.get_mut(idx))
        else {
            return false;
        };
        if share.status != SHARE_STATUS_PROVISIONAL {
            return false;
        }
        share.status = status;
        share.was_sampled = true;
        true
    }

    fn clear_address_risk_history(&self, address: &str) -> Result<()> {
        self.address_risk.lock().remove(address);
        Ok(())
    }
}

#[cfg(test)]
impl ShareStore for InMemoryStore {
    fn try_claim_share(&self, _job_id: &str, share_key: &str, nonce: u64) -> Result<bool> {
        let key = (share_key.to_string(), nonce);
        let mut seen = self.seen.lock();
        if seen.contains(&key) {
            return Ok(false);
        }
        seen.insert(key);
        Ok(true)
    }

    fn release_share_claim(&self, share_key: &str, nonce: u64) -> Result<()> {
        self.seen.lock().remove(&(share_key.to_string(), nonce));
        Ok(())
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        self.shares.lock().push(share);
        Ok(())
    }

    fn add_share_with_replay(
        &self,
        share: ShareRecord,
        _replay: Option<ShareReplayData>,
    ) -> Result<()> {
        self.add_share(share)
    }

    fn add_share_with_replay_metadata(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
        metadata: &SubmitMetadata,
    ) -> Result<()> {
        self.share_metadata
            .lock()
            .push((metadata.miner_version.clone(), metadata.backend.clone()));
        self.add_share_with_replay(share, replay)
    }

    fn add_share_with_replay_and_id(
        &self,
        share: ShareRecord,
        _replay: Option<ShareReplayData>,
    ) -> Result<i64> {
        let mut shares = self.shares.lock();
        shares.push(share);
        if std::mem::take(&mut *self.ambiguous_commit_once.lock()) {
            return Err(anyhow!("connection closed after COMMIT"));
        }
        Ok(shares.len() as i64)
    }

    fn add_found_block(&self, block: FoundBlockRecord) -> Result<()> {
        self.blocks.lock().push(block);
        Ok(())
    }

    fn revoke_provisional_shares(&self, keys: &[ShareKey], reason: &str) -> Result<u64> {
        // Ids are 1-based insertion positions (see add_share_with_replay_and_id).
        let mut shares = self.shares.lock();
        let mut revoked = 0u64;
        for key in keys {
            let Some(share) = usize::try_from(key.id)
                .ok()
                .and_then(|id| id.checked_sub(1))
                .and_then(|idx| shares.get_mut(idx))
            else {
                continue;
            };
            if share.status == SHARE_STATUS_PROVISIONAL {
                share.status = SHARE_STATUS_REJECTED;
                share.reject_reason = Some(reason.to_string());
                revoked += 1;
            }
        }
        Ok(revoked)
    }

    fn address_risk_state(&self, address: &str) -> Result<Option<AddressRiskState>> {
        Ok(self.address_risk.lock().get(address).cloned())
    }

    fn should_force_verify_address(&self, address: &str) -> Result<bool> {
        let now = SystemTime::now();
        Ok(self.address_risk.lock().get(address).is_some_and(|state| {
            state.force_verify_until.is_some_and(|until| until > now)
                || state.quarantined_until.is_some_and(|until| until > now)
        }))
    }

    fn escalate_address_risk(&self, escalation: AddressRiskEscalation<'_>) -> Result<()> {
        let AddressRiskEscalation {
            address,
            reason,
            strike_window_duration,
            quarantine_threshold,
            quarantine_base,
            quarantine_max,
            force_verify_duration,
        } = escalation;
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
            let candidate = pool_common::db::quarantine_until_for_strikes(
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

#[cfg(test)]
#[derive(Default)]
pub(crate) struct InMemoryJobs {
    jobs: Mutex<HashMap<String, Job>>,
    assignments: Mutex<HashMap<String, InMemoryAssignment>>,
    current_height: Mutex<Option<u64>>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct InMemoryAssignment {
    template_job_id: String,
    share_difficulty: u64,
    assigned_miner: Option<String>,
    nonce_start: u64,
    nonce_end: u64,
}

#[cfg(test)]
impl InMemoryJobs {
    pub(crate) fn set_current_height(&self, height: u64) {
        *self.current_height.lock() = Some(height);
    }

    pub(crate) fn insert(&self, job: Job) {
        let id = job.id.clone();
        self.jobs.lock().insert(id.clone(), job);
    }

    pub(crate) fn insert_assignment(
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

#[cfg(test)]
impl JobRepository for InMemoryJobs {
    fn current_height(&self) -> Option<u64> {
        *self.current_height.lock()
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

#[cfg(test)]
#[derive(Default)]
pub(crate) struct InMemoryNode {
    pub(crate) submits: Mutex<Vec<(String, u64)>>,
    pub(crate) accepted: bool,
    /// Simulated daemon re-hash time per submission.
    pub(crate) delay: Duration,
}

#[cfg(test)]
impl NodeApi for InMemoryNode {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        self.submits.lock().push((job.id.clone(), nonce));
        std::thread::sleep(self.delay);
        Ok(BlockSubmitResponse {
            accepted: self.accepted,
            hash: Some(format!("{:x}", nonce)),
            height: Some(job.height),
            daemon_total_us: None,
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
    use pool_common::pow::{difficulty_to_target, DeterministicTestHasher, PowHasher};

    #[test]
    fn canonical_reject_reason_distinguishes_stale_blocks_from_stale_jobs() {
        assert_eq!(canonical_share_reject_reason("stale job"), "stale job");
        assert_eq!(
            canonical_share_reject_reason(
                "full-block fallback submit: /api/mining/submitblock 400: \
                 {\"error\":\"block rejected as stale\"}"
            ),
            "stale block"
        );
    }

    fn cfg() -> Config {
        Config {
            validation_mode: "probabilistic".to_string(),
            sample_rate: 0.0,
            warmup_shares: 0,
            min_sample_every: 0,
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
            template_id: "tmpl1".to_string(),
            prev_hash: None,
            template_created_at: Instant::now(),
            template_block: None,
        }
    }

    fn submit_hash_cap() -> Vec<String> {
        vec!["submit_claimed_hash".to_string()]
    }

    /// Always hashes to ff..ff, so "ff" claims are genuine regular shares.
    struct FfHasher;
    impl PowHasher for FfHasher {
        fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
            Ok([0xff; 32])
        }
    }

    /// Hashes to f0..|nonce: above job()'s network target, meets difficulty 1.
    struct NonceHasher;
    impl NonceHasher {
        fn hash_for(nonce: u64) -> [u8; 32] {
            let mut out = [0xf0; 32];
            out[24..].copy_from_slice(&nonce.to_be_bytes());
            out
        }
        fn honest(nonce: u64) -> Option<String> {
            Some(hex::encode(Self::hash_for(nonce)))
        }
    }
    impl PowHasher for NonceHasher {
        fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
            Ok(Self::hash_for(nonce))
        }
    }

    /// A claimed hash that meets difficulty 1 but is not the real hash.
    fn fake_claim() -> Option<String> {
        Some("ee".repeat(32))
    }

    /// A claimed hash below job()'s network target: forces a candidate check.
    fn fake_candidate_claim() -> Option<String> {
        Some("00".repeat(32))
    }

    fn source_test_engine(
        mut cfg: Config,
        hasher: Arc<dyn PowHasher>,
    ) -> (PoolEngine, Arc<InMemoryStore>, Arc<ValidationEngine>) {
        // No provisional-backlog / payout-coverage holds: tests control sampling.
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_provisional_cap_multiplier = 0.0;
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), hasher));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode {
                accepted: true,
                ..InMemoryNode::default()
            }),
        );
        (engine, store, validation)
    }

    fn statuses(store: &InMemoryStore, nonces: std::ops::RangeInclusive<u64>) -> Vec<&'static str> {
        let shares = store.shares();
        nonces
            .map(|nonce| {
                shares
                    .iter()
                    .find(|share| share.nonce == nonce)
                    .map(|share| share.status)
                    .unwrap_or("missing")
            })
            .collect()
    }

    #[test]
    fn faraday_shares_are_all_verified_inline_instead_of_sampled() {
        // Sample rate 0: before the fork these claims would be hot-accepted.
        let mut cfg = cfg();
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_provisional_cap_multiplier = 0.0;
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(NonceHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            height: faraday::FARADAY_HEIGHT,
            ..job()
        });
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode {
                accepted: true,
                ..InMemoryNode::default()
            }),
        );
        let conn = "203.0.113.1:4000";
        engine
            .login(conn, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("login");

        engine
            .submit(conn, "job1".to_string(), 1, NonceHasher::honest(1))
            .expect("honest share");
        assert!(engine
            .submit(conn, "job1".to_string(), 2, fake_claim())
            .is_err());

        assert_eq!(statuses(&store, 1..=1), vec![SHARE_STATUS_VERIFIED]);
        assert!(store
            .shares()
            .iter()
            .all(|share| share.status != SHARE_STATUS_PROVISIONAL));
        assert_eq!(validation.snapshot().hot_accepts, 0);
    }

    #[test]
    fn faraday_share_is_checked_with_blake2b_by_the_production_hasher() {
        use pool_common::pow::{blake2b_pow_hash, ConsensusPowHasher};

        let mut header_base = vec![0u8; faraday::FARADAY_HEADER_BASE_LEN];
        header_base[..4].copy_from_slice(&2u32.to_le_bytes());
        header_base[4..12].copy_from_slice(&faraday::FARADAY_HEIGHT.to_le_bytes());
        let difficulty = 1_000;
        let blake2b =
            |nonce| blake2b_pow_hash(&faraday::BLAKE2B_POW_PERSONAL_MAINNET, &header_base, nonce);
        let nonce = (0..)
            .find(|&nonce| check_target(blake2b(nonce), difficulty_to_target(difficulty)))
            .expect("a share within range");

        let mut cfg = cfg();
        cfg.initial_share_difficulty = difficulty;
        cfg.min_share_difficulty = difficulty;
        cfg.enable_vardiff = false;
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(ConsensusPowHasher::default()),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(Job {
            height: faraday::FARADAY_HEIGHT,
            header_base: header_base.clone(),
            network_target: [0; 32],
            ..job()
        });
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        let conn = "203.0.113.1:4000";
        engine
            .login(conn, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit(
                conn,
                "job1".to_string(),
                nonce,
                Some(hex::encode(blake2b(nonce))),
            )
            .expect("real BLAKE2b share");
        // The same claim on the next nonce does not match its BLAKE2b hash.
        assert!(engine
            .submit(
                conn,
                "job1".to_string(),
                nonce + 1,
                Some(hex::encode(blake2b(nonce)))
            )
            .is_err());
        assert_eq!(statuses(&store, nonce..=nonce), vec![SHARE_STATUS_VERIFIED]);
    }

    #[test]
    fn failed_sample_revokes_same_source_credit_but_not_other_sources_on_the_address() {
        let (engine, store, _validation) = source_test_engine(cfg(), Arc::new(NonceHasher));
        let victim = test_miner_address();
        let victim_conn = "203.0.113.1:4000";
        let attacker_conn = "198.51.100.9:5000";
        let attacker_reconnect = "198.51.100.9:5001";

        engine
            .login(victim_conn, victim.clone(), "rig", 2, submit_hash_cap())
            .expect("victim login");
        for nonce in 1..=3 {
            engine
                .submit(
                    victim_conn,
                    "job1".to_string(),
                    nonce,
                    NonceHasher::honest(nonce),
                )
                .expect("victim share");
        }

        // The attacker logs in as the victim's public address and farms
        // unverified credit across two sessions from one host.
        engine
            .login(attacker_conn, victim.clone(), "x", 2, submit_hash_cap())
            .expect("attacker login");
        engine
            .login(
                attacker_reconnect,
                victim.clone(),
                "y",
                2,
                submit_hash_cap(),
            )
            .expect("attacker second session");
        for nonce in 10..=12 {
            engine
                .submit(attacker_conn, "job1".to_string(), nonce, fake_claim())
                .expect("unsampled fake is provisionally accepted");
        }
        engine
            .submit(attacker_reconnect, "job1".to_string(), 13, fake_claim())
            .expect("unsampled fake on second session");

        // One sampled fake (a false block claim forces a full hash) proves it.
        let err = engine
            .submit(
                attacker_conn,
                "job1".to_string(),
                14,
                fake_candidate_claim(),
            )
            .expect_err("false block claim is rejected");
        assert!(err.to_string().contains("invalid share proof"));

        assert_eq!(
            statuses(&store, 10..=14),
            vec![SHARE_STATUS_REJECTED; 5],
            "every unverified share from the struck source is revoked"
        );
        assert_eq!(
            statuses(&store, 1..=3),
            vec![SHARE_STATUS_PROVISIONAL; 3],
            "the victim's own rig keeps its credit"
        );
        assert!(store
            .shares()
            .iter()
            .filter(|share| (10..=13).contains(&share.nonce))
            .all(|share| share.reject_reason.as_deref() == Some(SOURCE_REVOKED_REJECT_REASON)));

        assert!(engine.source_blocked(attacker_conn));
        assert!(engine.source_blocked(attacker_reconnect));
        assert!(!engine.source_blocked(victim_conn));
        engine
            .submit(victim_conn, "job1".to_string(), 4, NonceHasher::honest(4))
            .expect("victim keeps mining");
        assert!(engine
            .login(
                "198.51.100.9:5002",
                victim.clone(),
                "z",
                2,
                submit_hash_cap()
            )
            .is_err());
    }

    #[test]
    fn shadow_mode_logs_strikes_without_revoking_or_banning() {
        let mut cfg = cfg();
        cfg.source_penalties_enabled = false;
        let (engine, store, _validation) = source_test_engine(cfg, Arc::new(NonceHasher));
        let attacker = "198.51.100.9:5000";
        engine
            .login(attacker, test_miner_address(), "x", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit(attacker, "job1".to_string(), 10, fake_claim())
            .expect("unsampled fake");
        let err = engine
            .submit(attacker, "job1".to_string(), 11, fake_candidate_claim())
            .expect_err("false block claim still rejected");
        assert!(err.to_string().contains("invalid share proof"));
        assert!(!engine.source_blocked(attacker));
        assert_eq!(statuses(&store, 10..=10), vec![SHARE_STATUS_PROVISIONAL]);
    }

    #[test]
    fn pool_side_hash_failure_is_neither_a_pass_nor_a_strike() {
        struct BrokenHasher;
        impl PowHasher for BrokenHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Err(anyhow!("argon2 hash failure"))
            }
        }
        let mut cfg = cfg();
        cfg.sample_rate = 1.0;
        let (engine, store, validation) = source_test_engine(cfg, Arc::new(BrokenHasher));
        let rig = "203.0.113.1:4000";
        engine
            .login(rig, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit(rig, "job1".to_string(), 1, NonceHasher::honest(1))
            .expect("hot accept");
        let deadline = Instant::now() + Duration::from_secs(3);
        while validation.snapshot().audit_deferred == 0 {
            assert!(Instant::now() < deadline, "audit should run");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(statuses(&store, 1..=1), vec![SHARE_STATUS_PROVISIONAL]);
        assert!(!engine.source_blocked(rig));
        assert!(!engine.source_established(rig));
    }

    #[test]
    fn many_attacker_sources_cannot_push_a_public_address_into_forced_review() {
        let mut cfg = cfg();
        // Old behaviour: 5 invalid samples over a 10% ratio forced a 24h
        // review of the whole address (zeroing its unverified credit).
        cfg.invalid_sample_min = 1;
        cfg.invalid_sample_threshold = 0.1;
        let (engine, store, validation) = source_test_engine(cfg, Arc::new(NonceHasher));
        let victim = test_miner_address();

        for idx in 0..8u64 {
            let conn = format!("198.51.{idx}.9:6000");
            engine
                .login(&conn, victim.clone(), "x", 2, submit_hash_cap())
                .expect("attacker login");
            let err = engine
                .submit(&conn, "job1".to_string(), 100 + idx, fake_candidate_claim())
                .expect_err("fake rejected");
            assert!(err.to_string().contains("invalid share proof"));
        }

        let plan = validation.plan_regular_submit(&victim);
        assert!(
            !plan.sync_full_verify,
            "attributed failures must not start an address-level forced review"
        );
        assert!(!store.should_force_verify_address(&victim).unwrap());
        engine
            .login(
                "203.0.113.1:4000",
                victim.clone(),
                "rig",
                2,
                submit_hash_cap(),
            )
            .expect("victim login");
        let ack = engine
            .submit(
                "203.0.113.1:4000",
                "job1".to_string(),
                1,
                NonceHasher::honest(1),
            )
            .expect("victim share accepted");
        assert_eq!(ack.status, SHARE_STATUS_PROVISIONAL);
    }

    #[test]
    fn live_audit_failure_strikes_its_source_and_spares_the_address() {
        let mut cfg = cfg();
        cfg.sample_rate = 1.0;
        cfg.invalid_sample_min = 1;
        cfg.invalid_sample_threshold = 0.1;
        let (engine, store, validation) = source_test_engine(cfg, Arc::new(NonceHasher));
        let victim = test_miner_address();
        let attacker_conn = "198.51.100.9:5000";
        engine
            .login(attacker_conn, victim.clone(), "x", 2, submit_hash_cap())
            .expect("attacker login");
        engine
            .submit(attacker_conn, "job1".to_string(), 10, fake_claim())
            .expect("hot accept before the async audit");

        let deadline = Instant::now() + Duration::from_secs(3);
        while !engine.source_blocked(attacker_conn) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            engine.source_blocked(attacker_conn),
            "audit failure strikes the source"
        );
        assert_eq!(statuses(&store, 10..=10), vec![SHARE_STATUS_REJECTED]);
        assert!(!validation.plan_regular_submit(&victim).sync_full_verify);
        engine
            .login("203.0.113.1:4000", victim, "rig", 2, submit_hash_cap())
            .expect("other sources are unaffected");
    }

    #[test]
    fn established_source_block_bypasses_a_busy_candidate_verifier() {
        // Nonce >= 1000 hashes slowly (the flood); others are instant. Nonce 7
        // is a real block (all-zero hash).
        struct FloodHasher;
        impl PowHasher for FloodHasher {
            fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
                if nonce >= 1_000 {
                    std::thread::sleep(Duration::from_millis(600));
                }
                Ok(if nonce == 7 {
                    [0x00; 32]
                } else {
                    NonceHasher::hash_for(nonce)
                })
            }
        }
        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();
        let (engine, _store, _validation) = source_test_engine(cfg, Arc::new(FloodHasher));
        let engine = Arc::new(engine);
        let honest = "203.0.113.1:4000";
        engine
            .login(honest, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("login");
        for nonce in 1..=crate::source_risk::SOURCE_ESTABLISHED_MIN_VERIFIED {
            engine
                .submit(
                    honest,
                    "job1".to_string(),
                    nonce,
                    NonceHasher::honest(nonce),
                )
                .expect("verified share");
        }
        assert!(engine.source_established(honest));

        let attacker = "198.51.100.9:5000";
        engine
            .login(attacker, other_miner_address(), "x", 2, submit_hash_cap())
            .expect("attacker login");
        assert!(!engine.source_established(attacker));
        let flood = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                engine.submit(attacker, "job1".to_string(), 1_000, fake_candidate_claim())
            })
        };
        std::thread::sleep(Duration::from_millis(50));

        let started = Instant::now();
        let ack = engine
            .submit(honest, "job1".to_string(), 7, Some("00".repeat(32)))
            .expect("real block accepted");
        assert!(ack.block_accepted);
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "established block must not wait behind the false claim ({:?})",
            started.elapsed()
        );
        let flood_err = flood.join().unwrap().expect_err("false claim rejected");
        assert!(flood_err.to_string().contains("invalid share proof"));
        assert!(
            engine.source_blocked(attacker),
            "false block claim strikes the source"
        );
    }

    #[test]
    fn saturated_probation_accepts_unpaid_shares_and_queues_them_for_review() {
        let mut cfg = cfg();
        cfg.warmup_shares = 5;
        let (engine, store, validation) = source_test_engine(cfg, Arc::new(NonceHasher));
        // Fill the probation lane from other networks.
        for idx in 0..64 {
            let _ =
                validation.reserve_probation(&format!("filler-{idx}"), &format!("10.0.{idx}.1"));
        }
        let newbie = "203.0.113.1:4000";
        engine
            .login(newbie, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("login");
        // Seine never resubmits busy rejects, so the share is accepted; it has
        // no payout weight until the address is verified and is queued for a
        // per-source review instead of escaping verification.
        let ack = engine
            .submit(newbie, "job1".to_string(), 1, NonceHasher::honest(1))
            .expect("share accepted while the probation lane is full");
        assert_eq!(ack.status, SHARE_STATUS_PROVISIONAL);
        assert_eq!(store.shares().len(), 1);
        assert_eq!(validation.snapshot().probation_enqueued, 0);
        assert_eq!(
            engine
                .source_risk
                .take_degraded_review_batch(8, 1.0)
                .iter()
                .map(|key| key.id)
                .collect::<Vec<_>>(),
            vec![1]
        );

        validation.cancel_probation("filler-0", "10.0.0.1");
        engine
            .submit(newbie, "job1".to_string(), 3, NonceHasher::honest(3))
            .expect("share with a free probation slot");
        assert_eq!(validation.snapshot().probation_enqueued, 1);
        // Further shares while the probation audit is pending are accepted.
        engine
            .submit(newbie, "job1".to_string(), 2, NonceHasher::honest(2))
            .expect("second share while probation is pending");
    }

    #[test]
    fn probation_catches_a_never_verified_cheater() {
        let mut cfg = cfg();
        cfg.warmup_shares = 5;
        let (engine, store, _validation) = source_test_engine(cfg, Arc::new(NonceHasher));
        let cheater = "198.51.100.9:5000";
        engine
            .login(cheater, other_miner_address(), "x", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit(cheater, "job1".to_string(), 1, fake_claim())
            .expect("hot accept before probation runs");
        let deadline = Instant::now() + Duration::from_secs(3);
        while !engine.source_blocked(cheater) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(engine.source_blocked(cheater));
        assert_eq!(statuses(&store, 1..=1), vec![SHARE_STATUS_REJECTED]);
    }

    /// Validation state store that audits shares held by an InMemoryStore.
    struct InMemoryShareAuditStore {
        shares: Arc<InMemoryStore>,
        header_base: Vec<u8>,
        network_target: [u8; 32],
        /// Persisted address states loaded at startup.
        persisted: Vec<crate::validation::PersistedValidationAddressState>,
    }

    impl crate::validation::ValidationStateStore for InMemoryShareAuditStore {
        fn load_validation_state(
            &self,
            _state_cutoff: SystemTime,
            _provisional_cutoff: SystemTime,
            _accepted_window_cutoff: SystemTime,
            _now: SystemTime,
        ) -> Result<crate::validation::LoadedValidationState> {
            Ok(crate::validation::LoadedValidationState {
                states: self.persisted.clone(),
                ..Default::default()
            })
        }
        fn upsert_validation_state(
            &self,
            _state: &crate::validation::PersistedValidationAddressState,
        ) -> Result<()> {
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
        ) -> Result<Vec<crate::validation::ValidationClearEvent>> {
            Ok(Vec::new())
        }
        fn complete_validation_audit(
            &self,
            update: &pool_common::db::ShareReplayUpdate,
        ) -> Result<bool> {
            let status = if update.status == SHARE_STATUS_VERIFIED {
                SHARE_STATUS_VERIFIED
            } else {
                SHARE_STATUS_REJECTED
            };
            Ok(self.shares.complete_audit(update.share_id, status))
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
        fn load_provisional_audit_shares_by_keys(
            &self,
            keys: &[ShareKey],
        ) -> Result<Vec<PendingAuditShare>> {
            let shares = self.shares.shares();
            Ok(keys
                .iter()
                .filter_map(|key| {
                    let share = shares.get(usize::try_from(key.id).ok()?.checked_sub(1)?)?;
                    (share.status == SHARE_STATUS_PROVISIONAL).then(|| PendingAuditShare {
                        share_id: key.id,
                        job_id: share.job_id.clone(),
                        miner: share.miner.clone(),
                        worker: share.worker.clone(),
                        difficulty: share.difficulty,
                        nonce: share.nonce,
                        claimed_hash: share
                            .claimed_hash
                            .as_deref()
                            .and_then(|hash| parse_hash_hex(hash).ok()),
                        header_base: self.header_base.clone(),
                        network_target: self.network_target,
                        created_at: share.created_at,
                    })
                })
                .collect())
        }
    }

    /// Engine with live audits of every share and persisted address history,
    /// as after a Stratum restart: (address, sampled_shares, invalid_samples).
    fn restarted_engine_with_history(
        history: &[(String, u64, u64)],
    ) -> (PoolEngine, Arc<InMemoryStore>, Arc<ValidationEngine>) {
        let mut cfg = cfg();
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_provisional_cap_multiplier = 0.0;
        cfg.sample_rate = 1.0;
        cfg.regular_validation_queue = 64;
        let store = Arc::new(InMemoryStore::default());
        let persisted = history
            .iter()
            .map(
                |(address, sampled, invalid)| crate::validation::PersistedValidationAddressState {
                    address: address.clone(),
                    total_shares: sampled * 20,
                    sampled_shares: *sampled,
                    invalid_samples: *invalid,
                    risk_sampled_shares: 0,
                    risk_invalid_samples: 0,
                    forced_started_at: None,
                    forced_until: None,
                    forced_sampled_shares: 0,
                    forced_invalid_samples: 0,
                    resume_forced_at: None,
                    hold_cause: None,
                    last_seen_at: SystemTime::now(),
                },
            )
            .collect();
        let validation = Arc::new(ValidationEngine::new_with_state_store(
            cfg.clone(),
            Arc::new(NonceHasher),
            Arc::new(InMemoryShareAuditStore {
                shares: Arc::clone(&store),
                header_base: job().header_base,
                network_target: job().network_target,
                persisted,
            }),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        (engine, store, validation)
    }

    fn wait_for_audits(validation: &ValidationEngine, count: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = validation.snapshot();
            if snapshot.audit_verified + snapshot.audit_rejected >= count {
                return;
            }
            assert!(Instant::now() < deadline, "audits should complete");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn rare_error_from_long_standing_miner_is_a_soft_strike_after_restart() {
        // YCjTZ: 544249 good shares and 3 invalid proofs in 7 days at 5%.
        let address = test_miner_address();
        let (engine, store, validation) =
            restarted_engine_with_history(&[(address.clone(), 27_212 + 3, 3)]);
        let farm = "203.0.113.1:4000";
        engine
            .login(farm, address.clone(), "rig", 2, submit_hash_cap())
            .expect("login");
        for nonce in 1..=3 {
            engine
                .submit(farm, "job1".to_string(), nonce, NonceHasher::honest(nonce))
                .expect("honest share");
        }
        // A hardware/backend error produces one bad claimed hash.
        engine
            .submit(farm, "job1".to_string(), 4, fake_claim())
            .expect("hot accept");
        wait_for_audits(&validation, 4);

        assert!(!engine.source_blocked(farm), "no ban for a rare error");
        assert_eq!(
            statuses(&store, 1..=4),
            vec![
                SHARE_STATUS_VERIFIED,
                SHARE_STATUS_VERIFIED,
                SHARE_STATUS_VERIFIED,
                SHARE_STATUS_REJECTED
            ],
            "only the bad share is rejected"
        );
        assert!(store
            .shares()
            .iter()
            .all(|share| share.reject_reason.as_deref() != Some(SOURCE_REVOKED_REJECT_REASON)));
        // The source is audited more closely for a while.
        let source = engine.sessions.lock().get(farm).unwrap().source.clone();
        assert!(engine.source_risk.take_boost(&source));
        engine
            .login("203.0.113.1:4001", address, "rig2", 2, submit_hash_cap())
            .expect("the farm can keep connecting");
    }

    #[test]
    fn cheater_with_a_bad_address_record_is_struck_at_once_after_restart() {
        // 8BWg: 63001 good shares, 1791 invalid proofs.
        let address = test_miner_address();
        let (engine, store, validation) =
            restarted_engine_with_history(&[(address.clone(), 3_150 + 1_791, 1_791)]);
        let rig = "198.51.100.9:5000";
        engine
            .login(rig, address, "x", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit(rig, "job1".to_string(), 1, NonceHasher::honest(1))
            .expect("real share");
        engine
            .submit(rig, "job1".to_string(), 2, fake_claim())
            .expect("hot accept");
        wait_for_audits(&validation, 2);
        let deadline = Instant::now() + Duration::from_secs(3);
        while !engine.source_blocked(rig) {
            assert!(Instant::now() < deadline, "full strike expected");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(statuses(&store, 2..=2), vec![SHARE_STATUS_REJECTED]);
    }

    #[test]
    fn attacker_using_a_victims_address_cannot_hurt_the_victim_or_its_record() {
        let victim = test_miner_address();
        let (engine, store, validation) =
            restarted_engine_with_history(&[(victim.clone(), 27_215, 3)]);
        let victim_rig = "203.0.113.1:4000";
        engine
            .login(victim_rig, victim.clone(), "rig", 2, submit_hash_cap())
            .expect("victim login");
        engine
            .submit(victim_rig, "job1".to_string(), 1, NonceHasher::honest(1))
            .expect("victim share");

        // The attacker's first fake is shielded by the victim's history (it
        // only credits the victim); boosted audits then convict the source.
        let attacker = "198.51.100.9:5000";
        engine
            .login(attacker, victim.clone(), "x", 2, submit_hash_cap())
            .expect("attacker login");
        let mut accepted = 0;
        for nonce in 100..140 {
            match engine.submit(attacker, "job1".to_string(), nonce, fake_claim()) {
                Ok(_) => accepted += 1,
                Err(err) => {
                    assert!(err.to_string().starts_with("source banned"), "{err}");
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(accepted >= 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !engine.source_blocked(attacker) {
            assert!(Instant::now() < deadline, "attacker should be fully struck");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!engine.source_blocked(victim_rig));
        engine
            .submit(victim_rig, "job1".to_string(), 2, NonceHasher::honest(2))
            .expect("victim keeps mining");
        wait_for_audits(&validation, 2);
        assert!(store
            .shares()
            .iter()
            .filter(|share| share.nonce < 100)
            .all(|share| share.status != SHARE_STATUS_REJECTED));
        // The attacker had no verified history on the address, so its
        // failures do not inflate the victim's persisted invalid count.
        assert_eq!(validation.address_sample_counts(&victim).1, 3);
    }

    #[test]
    fn shares_accepted_during_emergency_are_reviewed_per_source_when_capacity_returns() {
        let mut cfg = cfg();
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_provisional_cap_multiplier = 0.0;
        cfg.sample_rate = 0.05;
        cfg.audit_verifiers = 1;
        let store = Arc::new(InMemoryStore::default());
        let validation = Arc::new(ValidationEngine::new_with_state_store(
            cfg.clone(),
            Arc::new(NonceHasher),
            Arc::new(InMemoryShareAuditStore {
                shares: Arc::clone(&store),
                header_base: job().header_base,
                network_target: job().network_target,
                persisted: Vec::new(),
            }),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );

        // Simulate a saturated submit lane: the pool enters Emergency, where
        // the sample rate is 0 and no share is verified at accept time.
        let pressure = Arc::new(QueueTracker::new(8));
        pressure.push(Instant::now() - Duration::from_secs(60));
        validation.attach_submit_regular_queue(Arc::clone(&pressure));
        assert_eq!(validation.snapshot().overload_mode, OverloadMode::Emergency);

        let honest = "203.0.113.1:4000";
        let attacker = "198.51.100.9:5000";
        engine
            .login(honest, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("honest login");
        engine
            .login(attacker, other_miner_address(), "x", 2, submit_hash_cap())
            .expect("attacker login");
        let mut fakes_accepted = 0;
        for nonce in 1..=3 {
            engine
                .submit(
                    honest,
                    "job1".to_string(),
                    nonce,
                    NonceHasher::honest(nonce),
                )
                .expect("honest share accepted unverified during emergency");
            // A maintenance tick may already review the first fake mid-loop.
            match engine.submit(attacker, "job1".to_string(), 100 + nonce, fake_claim()) {
                Ok(ack) => {
                    assert_eq!(ack.status, SHARE_STATUS_PROVISIONAL);
                    fakes_accepted += 1;
                }
                Err(err) => assert!(err.to_string().starts_with("source banned"), "{err}"),
            }
        }
        assert!(
            fakes_accepted >= 1,
            "fakes are accepted unverified during emergency"
        );

        // Reviews wait for capacity: nothing is audited while overloaded.
        std::thread::sleep(Duration::from_millis(1_500));
        assert!(!engine.source_blocked(attacker));

        // Capacity returns (the maintenance tick schedules reviews in Normal).
        validation.run_degraded_reviews_now();
        let deadline = Instant::now() + Duration::from_secs(6);
        while !engine.source_blocked(attacker) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            engine.source_blocked(attacker),
            "the degraded-window review catches the fake source afterwards"
        );
        assert!(
            store
                .shares()
                .iter()
                .filter(|share| share.miner == other_miner_address())
                .all(|share| share.status == SHARE_STATUS_REJECTED),
            "every fake accepted during the degraded window is revoked"
        );
        let honest_statuses = statuses(&store, 1..=3);
        assert!(
            honest_statuses
                .iter()
                .all(|status| *status != SHARE_STATUS_REJECTED),
            "honest degraded-window credit is kept: {honest_statuses:?}"
        );
        assert!(!engine.source_blocked(honest));
    }

    #[test]
    fn established_source_is_not_locked_out_by_an_address_quarantine() {
        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();
        let (engine, store, _validation) = source_test_engine(cfg, Arc::new(NonceHasher));
        let address = test_miner_address();
        let rig = "203.0.113.1:4000";
        engine
            .login(rig, address.clone(), "rig", 2, submit_hash_cap())
            .expect("login");
        for nonce in 1..=crate::source_risk::SOURCE_ESTABLISHED_MIN_VERIFIED {
            engine
                .submit(rig, "job1".to_string(), nonce, NonceHasher::honest(nonce))
                .expect("verified share");
        }
        store
            .escalate_address_risk(AddressRiskEscalation::new(
                &address,
                "bad share ratio during forced validation",
                Duration::from_secs(60),
                1,
                Duration::from_secs(600),
                Duration::from_secs(600),
                Duration::from_secs(0),
            ))
            .expect("quarantine address");

        engine
            .submit(rig, "job1".to_string(), 50, NonceHasher::honest(50))
            .expect("established source keeps mining through the quarantine");
        engine
            .login(
                "203.0.113.1:4001",
                address.clone(),
                "rig2",
                2,
                submit_hash_cap(),
            )
            .expect("established pair can reconnect");
        let err = engine
            .login("198.51.100.9:5000", address, "x", 2, submit_hash_cap())
            .expect_err("unknown sources still see the quarantine");
        assert!(err.to_string().starts_with("address quarantined"));
    }

    fn test_miner_address() -> String {
        "Kffdvk1q1YPCGKz8gzJoymedZucxqJtxsw5ZUMJrWrVUK4jCXChXmTBrMADRh9EzeMNNPunXrz3dyg9nMwdZPaNc7FB7v".to_string()
    }

    fn other_miner_address() -> String {
        "Eua26cFpFEd25h5G6basTRjVX61CVLwvM6SFaB5XxQKEftoAkGVWjkw2sp4GjfsBf8D44tr2SkVGn9CnFn2gP2NxBP6KV".to_string()
    }

    #[test]
    fn non_fraud_strikes_reset_after_window_expires() {
        let store = InMemoryStore::default();
        store
            .escalate_address_risk(AddressRiskEscalation::new(
                "addr1",
                "low difficulty share",
                Duration::from_secs(60),
                3,
                Duration::from_secs(15 * 60),
                Duration::from_secs(2 * 60 * 60),
                Duration::from_secs(2 * 60 * 60),
            ))
            .expect("first non-fraud event should persist");
        {
            let mut risk = store.address_risk.lock();
            let state = risk.get_mut("addr1").expect("risk state should exist");
            state.strikes = 2;
            state.strike_window_until = Some(SystemTime::now() - Duration::from_secs(1));
        }

        store
            .escalate_address_risk(AddressRiskEscalation::new(
                "addr1",
                "low difficulty share",
                Duration::from_secs(60),
                3,
                Duration::from_secs(15 * 60),
                Duration::from_secs(2 * 60 * 60),
                Duration::from_secs(2 * 60 * 60),
            ))
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
                .escalate_address_risk(AddressRiskEscalation::new(
                    "addr1",
                    "low difficulty share",
                    Duration::from_secs(6 * 60 * 60),
                    3,
                    Duration::from_secs(15 * 60),
                    Duration::from_secs(2 * 60 * 60),
                    Duration::from_secs(2 * 60 * 60),
                ))
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
                "rig01",
                2,
                vec!["submit_claimed_hash".to_string()],
            )
            .expect("login");

        assert_eq!(result.protocol_version, 2);
        assert!(engine.session_supports_capability("conn1", "submit_claimed_hash"));
    }

    #[test]
    fn prequeue_rejection_preserves_valid_claimed_hash() {
        let cfg = cfg();
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        jobs.insert_assignment(
            "assignment1",
            "job1",
            60,
            Some(test_miner_address()),
            0,
            u64::MAX,
        );
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        engine
            .login("conn1", test_miner_address(), "rig01", 2, submit_hash_cap())
            .expect("login");

        let claimed_hash = hex::encode([0x01; 32]);
        engine.record_prequeue_reject(
            "conn1",
            "assignment1",
            7,
            Some(&claimed_hash),
            Instant::now(),
            "candidate claim busy, retry",
            &SubmitMetadata::default(),
        );

        let shares = store.shares();
        assert_eq!(shares.len(), 1);
        assert_eq!(
            shares[0].claimed_hash.as_deref(),
            Some(claimed_hash.as_str())
        );
        assert_eq!(
            shares[0].reject_reason.as_deref(),
            Some("candidate claim busy")
        );
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
                "",
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
            .login("conn1", address, "rig01", 2, submit_hash_cap())
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
            .login("conn1", address, "rig01", 2, submit_hash_cap())
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
            .login("conn1", address, "rig01", 2, submit_hash_cap())
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
                "rig01",
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
                "rig02",
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
                "seine-devfee-1",
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
                "seine-devfee-1",
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
                "seine-devfee-new",
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
            .login("conn1", address.clone(), "rig01", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 1, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, vec![])
            .expect_err("should require submit_claimed_hash capability");
        assert!(err.to_string().contains("submit_claimed_hash"));
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
    fn backlog_hold_uses_background_audit_when_sync_slots_are_full() {
        temporary_hold_uses_background_audit(ValidationHoldCause::ProvisionalBacklog);
    }

    #[test]
    fn coverage_hold_uses_background_audit_when_sync_slots_are_full() {
        temporary_hold_uses_background_audit(ValidationHoldCause::PayoutCoverage);
    }

    fn temporary_hold_uses_background_audit(cause: ValidationHoldCause) {
        use crate::store::PoolStore;
        use crate::validation::{PersistedValidationAddressState, ValidationStateStore};

        let Some(store) = PoolStore::test_store() else {
            eprintln!(
                "skipping postgres test: set {}",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };
        let address = test_miner_address();
        let now = SystemTime::now();
        let mut config = cfg();
        config.sync_verify_slots = 1;
        config.payout_min_verified_shares = 1;
        config.payout_provisional_cap_multiplier = 1.0;
        config.provisional_share_delay = match cause {
            ValidationHoldCause::ProvisionalBacklog => "5m",
            _ => "0s",
        }
        .to_string();
        // A persisted provisional share keeps the temporary hold active after
        // restart, so the planner must actually request its background audit.
        ShareStore::add_share(
            store.as_ref(),
            ShareRecord {
                job_id: "seed".to_string(),
                miner: address.clone(),
                worker: "rig".to_string(),
                difficulty: 1,
                nonce: 0,
                status: SHARE_STATUS_PROVISIONAL,
                was_sampled: false,
                block_hash: None,
                claimed_hash: Some("ff".repeat(32)),
                reject_reason: None,
                created_at: now,
            },
        )
        .expect("seed provisional share");
        let mut hold = PersistedValidationAddressState {
            address: address.clone(),
            total_shares: 1,
            sampled_shares: 0,
            invalid_samples: 0,
            risk_sampled_shares: 0,
            risk_invalid_samples: 0,
            forced_started_at: None,
            forced_until: Some(now + Duration::from_secs(900)),
            forced_sampled_shares: 0,
            forced_invalid_samples: 0,
            resume_forced_at: None,
            hold_cause: Some(cause),
            last_seen_at: now,
        };
        ValidationStateStore::upsert_validation_state(store.as_ref(), &hold)
            .expect("seed validation hold");
        let validation = Arc::new(ValidationEngine::new_with_state_store(
            config.clone(),
            Arc::new(FfHasher),
            Arc::clone(&store) as Arc<dyn ValidationStateStore>,
        ));
        let plan = validation.plan_regular_submit(&address);
        assert!(plan.enqueue_audit && !plan.sync_full_verify);
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            config,
            Arc::clone(&validation),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        engine
            .login("conn1", address.clone(), "rig", 2, submit_hash_cap())
            .expect("login");
        // Hold the only permit explicitly: this exercises saturation without
        // racing a sleeping verifier or depending on machine speed.
        let busy = engine
            .admission()
            .sync_verify()
            .try_acquire()
            .expect("reserve slot");
        let ack = engine
            .submit("conn1", "job1".to_string(), 42, Some("ff".repeat(32)))
            .expect("temporary hold must not compete for synchronous verification");
        assert!(ack.accepted && !ack.verified);
        assert_eq!(ack.status, SHARE_STATUS_PROVISIONAL);
        assert_eq!(engine.admission().snapshot().sync_verify_busy_total, 0);
        wait_for_audits(&validation, 1);
        let snapshot = validation.snapshot();
        assert_eq!(snapshot.audit_enqueued, 1);
        assert_eq!(snapshot.audit_verified, 1);
        assert_eq!(snapshot.sync_full_verifies, 0);

        // A later fraud hold must immediately override the cached temporary
        // hold, even though the in-memory planner still knows the audit hold.
        hold.hold_cause = Some(ValidationHoldCause::InvalidSamples);
        hold.forced_started_at = Some(now);
        ValidationStateStore::upsert_validation_state(store.as_ref(), &hold).expect("fraud hold");
        let error = engine
            .submit("conn1", "job1".to_string(), 43, Some("ff".repeat(32)))
            .expect_err("fraud review must still require a synchronous slot");
        assert!(error.to_string().starts_with("server busy"));
        assert_eq!(engine.admission().snapshot().sync_verify_busy_total, 1);
        drop(busy);
        let reviewed = engine
            .submit("conn1", "job1".to_string(), 43, Some("ff".repeat(32)))
            .expect("fraud-review share can retry once a slot is free");
        assert!(reviewed.verified);
    }

    #[test]
    fn sync_verifies_beyond_their_slots_fail_fast_instead_of_holding_a_worker() {
        struct SlowMatchingHasher;
        impl PowHasher for SlowMatchingHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                std::thread::sleep(Duration::from_millis(400));
                Ok([0xff; 32])
            }
        }

        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();
        cfg.regular_verifiers = 2;
        cfg.sync_verify_slots = 1;
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(SlowMatchingHasher),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = Arc::new(PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        ));
        for conn in ["conn1", "conn2"] {
            engine
                .login(conn, test_miner_address(), conn, 2, submit_hash_cap())
                .expect("login");
        }

        let first = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                engine.submit("conn1", "job1".to_string(), 600, Some("ff".repeat(32)))
            })
        };
        while engine.admission().sync_verify().in_flight() == 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
        let started = Instant::now();
        let err = engine
            .submit("conn2", "job1".to_string(), 601, Some("ff".repeat(32)))
            .expect_err("no sync verify slot is free");
        assert!(err.to_string().starts_with("server busy"), "{err}");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "the second share must not wait for the first verify"
        );
        assert_eq!(engine.admission().snapshot().sync_verify_busy_total, 1);

        let first = first.join().expect("join").expect("first share verifies");
        assert!(first.verified);
        // The busy share released its claim, so the miner may resubmit it.
        let retry = engine
            .submit("conn2", "job1".to_string(), 601, Some("ff".repeat(32)))
            .expect("retried share verifies once a slot is free");
        assert!(retry.verified);
        assert_eq!(engine.admission().sync_verify().in_flight(), 0);
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", address.clone(), "rig01", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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

    /// Nonce 7 hashes to a block (all zeros) after `delay`; every other
    /// nonce is an instant ordinary share.
    struct SlowBlockHasher {
        delay: Duration,
    }
    impl PowHasher for SlowBlockHasher {
        fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
            if nonce == 7 {
                std::thread::sleep(self.delay);
                return Ok([0x00; 32]);
            }
            Ok(NonceHasher::hash_for(nonce))
        }
    }

    /// An engine with an established honest source on "203.0.113.1:4000", a
    /// daemon that takes `daemon_delay` per submit and a verifier that takes
    /// `verify_delay` for the block at nonce 7.
    fn early_submit_engine(
        mut cfg: Config,
        verify_delay: Duration,
        daemon_delay: Duration,
        daemon_accepts: bool,
    ) -> (PoolEngine, Arc<InMemoryStore>, Arc<InMemoryNode>) {
        cfg.validation_mode = "full".to_string();
        cfg.provisional_share_delay = "0s".to_string();
        cfg.payout_provisional_cap_multiplier = 0.0;
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(SlowBlockHasher {
                delay: verify_delay,
            }),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let store = Arc::new(InMemoryStore::default());
        let node = Arc::new(InMemoryNode {
            accepted: daemon_accepts,
            delay: daemon_delay,
            ..InMemoryNode::default()
        });
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::clone(&node) as Arc<dyn NodeApi>,
        );
        let honest = "203.0.113.1:4000";
        engine
            .login(honest, test_miner_address(), "rig", 2, submit_hash_cap())
            .expect("login");
        for nonce in 100..100 + crate::source_risk::SOURCE_ESTABLISHED_MIN_VERIFIED {
            engine
                .submit(
                    honest,
                    "job1".to_string(),
                    nonce,
                    NonceHasher::honest(nonce),
                )
                .expect("verified share");
        }
        assert!(engine.source_established(honest));
        (engine, store, node)
    }

    #[test]
    fn established_block_claim_overlaps_daemon_submit_with_verify() {
        let delay = Duration::from_millis(300);
        let (engine, store, node) = early_submit_engine(cfg(), delay, delay, true);
        let started = Instant::now();
        let ack = engine
            .submit(
                "203.0.113.1:4000",
                "job1".to_string(),
                7,
                Some("00".repeat(32)),
            )
            .expect("block accepted");
        let elapsed = started.elapsed();
        assert!(ack.block_accepted);
        assert!(
            elapsed < Duration::from_millis(550),
            "verify and daemon submit should overlap ({elapsed:?})"
        );
        assert_eq!(node.submits.lock().len(), 1);
        assert_eq!(store.blocks().len(), 1);
        assert!(engine
            .block_candidate_histograms()
            .contains_key("early_submit_wait"));
    }

    #[test]
    fn disabled_parallel_submit_verifies_before_submitting() {
        let mut cfg = cfg();
        cfg.parallel_candidate_submit = false;
        let delay = Duration::from_millis(200);
        let (engine, store, node) = early_submit_engine(cfg, delay, delay, true);
        let started = Instant::now();
        let ack = engine
            .submit(
                "203.0.113.1:4000",
                "job1".to_string(),
                7,
                Some("00".repeat(32)),
            )
            .expect("block accepted");
        assert!(ack.block_accepted);
        assert!(started.elapsed() >= Duration::from_millis(400));
        assert_eq!(node.submits.lock().len(), 1);
        assert_eq!(store.blocks().len(), 1);
        assert!(!engine
            .block_candidate_histograms()
            .contains_key("early_submit_wait"));
    }

    #[test]
    fn false_block_claim_from_established_source_is_rejected_and_struck() {
        let (engine, store, node) =
            early_submit_engine(cfg(), Duration::ZERO, Duration::ZERO, false);
        let err = engine
            .submit(
                "203.0.113.1:4000",
                "job1".to_string(),
                8,
                fake_candidate_claim(),
            )
            .expect_err("false claim rejected");
        assert!(err.to_string().contains("invalid share proof"));
        // The claim reached the daemon early, which rejected it; nothing is
        // recorded as found and the source is struck as before.
        assert_eq!(node.submits.lock().len(), 1);
        assert!(store.blocks().is_empty());
        assert!(engine.source_blocked("203.0.113.1:4000"));
    }

    #[test]
    fn daemon_accepted_early_submit_is_recorded_even_if_pool_verify_disagrees() {
        // The daemon is the consensus check: if it accepted the block, the
        // block is on chain and must be recorded whatever the pool hashed.
        let (engine, store, node) =
            early_submit_engine(cfg(), Duration::ZERO, Duration::ZERO, true);
        let _ = engine.submit(
            "203.0.113.1:4000",
            "job1".to_string(),
            8,
            fake_candidate_claim(),
        );
        assert_eq!(node.submits.lock().len(), 1);
        assert_eq!(store.blocks().len(), 1);
    }

    #[test]
    fn early_submits_are_rate_bounded() {
        let (engine, store, node) =
            early_submit_engine(cfg(), Duration::ZERO, Duration::ZERO, true);
        {
            let mut starts = engine.early_submit_starts.lock();
            for _ in 0..EARLY_SUBMIT_MAX_PER_WINDOW {
                starts.push_back(Instant::now());
            }
        }
        // Over the bound the block still goes through, verified first.
        let ack = engine
            .submit(
                "203.0.113.1:4000",
                "job1".to_string(),
                7,
                Some("00".repeat(32)),
            )
            .expect("block accepted");
        assert!(ack.block_accepted);
        assert_eq!(node.submits.lock().len(), 1);
        assert_eq!(store.blocks().len(), 1);
        assert!(!engine
            .block_candidate_histograms()
            .contains_key("early_submit_wait"));
    }

    #[test]
    fn vardiff_increases_difficulty_for_fast_submitters() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.vardiff_target_shares = 1;
        cfg.vardiff_window = "5m".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();
        cfg.validation_mode = "probabilistic".to_string();

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(FfHasher));
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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

        let validation = ValidationEngine::new(cfg.clone(), Arc::new(FfHasher));
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            session.share_times.clear();
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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

    fn faraday_vardiff_engine(
        cfg: Config,
        height: u64,
    ) -> (PoolEngine, Arc<InMemoryStore>, Arc<InMemoryJobs>) {
        struct ZeroHasher;
        impl PowHasher for ZeroHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0u8; 32])
            }
        }
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(ZeroHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        jobs.set_current_height(height);
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            Arc::clone(&jobs) as Arc<dyn JobRepository>,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        (engine, store, jobs)
    }

    #[test]
    fn faraday_login_replaces_argon2_sized_difficulty() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        let recent = SystemTime::now()
            .checked_sub(Duration::from_secs(30))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let address = test_miner_address();

        // Before the fork the configured difficulties apply unchanged.
        let (engine, store, _) = faraday_vardiff_engine(cfg.clone(), faraday::FARADAY_HEIGHT - 1);
        store.seed_vardiff_hint(&address, "rig01", 222, recent);
        engine
            .login("conn1", address.clone(), "rig01", 2, submit_hash_cap())
            .expect("login");
        assert_eq!(engine.session_difficulty("conn1"), Some(222));

        // From the fork an Argon2-sized stored hint or client hint restarts.
        let (engine, store, _) = faraday_vardiff_engine(cfg.clone(), faraday::FARADAY_HEIGHT);
        store.seed_vardiff_hint(&address, "rig01", 222, recent);
        engine
            .login("conn1", address.clone(), "rig01", 2, submit_hash_cap())
            .expect("login");
        assert_eq!(
            engine.session_difficulty("conn1"),
            Some(faraday::FARADAY_INITIAL_SHARE_DIFFICULTY)
        );
        engine
            .login_with_hint(
                "conn2",
                address.clone(),
                "rig02",
                2,
                submit_hash_cap(),
                Some(500),
            )
            .expect("login");
        assert_eq!(
            engine.session_difficulty("conn2"),
            Some(faraday::FARADAY_INITIAL_SHARE_DIFFICULTY)
        );

        // A hint saved after the fork is kept.
        let (engine, store, _) = faraday_vardiff_engine(cfg, faraday::FARADAY_HEIGHT);
        store.seed_vardiff_hint(&address, "rig01", 1 << 40, recent);
        engine
            .login("conn1", address, "rig01", 2, submit_hash_cap())
            .expect("login");
        assert_eq!(engine.session_difficulty("conn1"), Some(1 << 40));
    }

    #[test]
    fn sessions_cross_onto_faraday_share_difficulty() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.initial_share_difficulty = 60;
        let configured_max = cfg.max_share_difficulty;
        let (engine, _, jobs) = faraday_vardiff_engine(cfg, faraday::FARADAY_HEIGHT - 1);
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        engine
            .login("conn2", test_miner_address(), "rig2", 2, submit_hash_cap())
            .expect("login");
        assert_eq!(engine.retarget_on_job_if_needed("conn1"), Some(60));

        jobs.set_current_height(faraday::FARADAY_HEIGHT);
        // The job tick moves a live session to the fork's start.
        assert_eq!(
            engine.retarget_on_job_if_needed("conn1"),
            Some(faraday::FARADAY_INITIAL_SHARE_DIFFICULTY)
        );
        // So does the first share after the fork, before any job tick.
        engine
            .submit("conn2", "job1".to_string(), 400, Some("00".repeat(32)))
            .expect("share");
        assert_eq!(
            engine.session_difficulty("conn2"),
            Some(faraday::FARADAY_INITIAL_SHARE_DIFFICULTY)
        );

        // A reorg back below the fork clamps to the configured ceiling as before.
        jobs.set_current_height(faraday::FARADAY_HEIGHT - 1);
        assert_eq!(
            engine.retarget_on_job_if_needed("conn1"),
            Some(configured_max)
        );
    }

    #[test]
    fn faraday_vardiff_raises_fast_submitters_above_the_start() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.validation_mode = "full".to_string();
        cfg.vardiff_target_shares = 1;
        cfg.vardiff_window = "5m".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();
        let (engine, _, _) = faraday_vardiff_engine(cfg, faraday::FARADAY_HEIGHT);
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        let mut last = None;
        for nonce in 100..105 {
            last = Some(
                engine
                    .submit("conn1", "job1".to_string(), nonce, Some("00".repeat(32)))
                    .expect("share"),
            );
            std::thread::sleep(Duration::from_millis(300));
        }
        let ack = last.expect("ack");
        assert!(ack.next_difficulty > faraday::FARADAY_INITIAL_SHARE_DIFFICULTY);
        assert!(ack.next_difficulty <= faraday::FARADAY_MAX_SHARE_DIFFICULTY);
    }

    fn vardiff_test_engine(cfg: Config) -> PoolEngine {
        struct ZeroHasher;
        impl PowHasher for ZeroHasher {
            fn hash(&self, _header_base: &[u8], _nonce: u64) -> anyhow::Result<[u8; 32]> {
                Ok([0u8; 32])
            }
        }
        let validation = Arc::new(ValidationEngine::new(cfg.clone(), Arc::new(ZeroHasher)));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        )
    }

    /// One-second vardiff target (window clamps to 30s, 60 shares) so idle
    /// decay is reachable in a test.
    fn fast_vardiff_cfg() -> Config {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.validation_mode = "full".to_string();
        cfg.initial_share_difficulty = 100;
        cfg.vardiff_target_shares = 60;
        cfg.vardiff_window = "1s".to_string();
        cfg.vardiff_retarget_interval = "1s".to_string();
        cfg
    }

    #[test]
    fn unprocessed_submits_raise_difficulty_for_a_flooding_session() {
        let mut cfg = cfg();
        cfg.enable_vardiff = true;
        cfg.initial_share_difficulty = 100;
        cfg.vardiff_retarget_interval = "2s".to_string();
        let engine = vardiff_test_engine(cfg);
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");

        // Nothing but busy answers: no accepted share ever arrives.
        assert_eq!(
            engine.record_unprocessed_submit("conn1", UnprocessedReason::SubmitQueueFull),
            None,
            "one sample is not a rate"
        );
        std::thread::sleep(Duration::from_millis(1_100));
        assert_eq!(
            engine.record_unprocessed_submit("conn1", UnprocessedReason::SubmitQueueFull),
            Some(150),
            "a share every second against a 30s target must raise difficulty"
        );
        assert_eq!(engine.session_difficulty("conn1"), Some(150));
        assert_eq!(
            engine.record_unprocessed_submit("conn1", UnprocessedReason::RateLimited),
            None,
            "raises still respect the retarget interval"
        );
        let snapshot = engine.admission().snapshot();
        assert_eq!(snapshot.submit_queue_full_total, 2);
        assert_eq!(snapshot.rate_limited_total, 1);
        assert_eq!(snapshot.vardiff_busy_raises_total, 1);
    }

    #[test]
    fn unprocessed_submits_never_lower_difficulty() {
        let engine = vardiff_test_engine(fast_vardiff_cfg());
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        engine.record_unprocessed_submit("conn1", UnprocessedReason::ValidationQueueFull);
        std::thread::sleep(Duration::from_millis(2_200));
        // An idle gap followed by busy evidence would read as a slow miner.
        assert_eq!(
            engine.record_unprocessed_submit("conn1", UnprocessedReason::ValidationQueueFull),
            None
        );
        assert_eq!(engine.session_difficulty("conn1"), Some(100));
    }

    #[test]
    fn job_tick_holds_difficulty_while_the_submit_lane_is_saturated() {
        let engine = vardiff_test_engine(fast_vardiff_cfg());
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit("conn1", "job1".to_string(), 400, Some("00".repeat(32)))
            .expect("share 1");
        std::thread::sleep(Duration::from_millis(2_200));

        // Someone else's flood saturates the lane; this miner's silence is
        // the pool's doing, not a hashrate drop.
        engine.admission().mark_pressure(Instant::now());
        assert_eq!(engine.retarget_on_job_if_needed("conn1"), Some(100));
        assert_eq!(engine.admission().snapshot().vardiff_downward_held_total, 1);
    }

    #[test]
    fn job_tick_holds_difficulty_after_the_sessions_own_submits_went_unprocessed() {
        let engine = vardiff_test_engine(fast_vardiff_cfg());
        for conn in ["conn1", "conn2"] {
            engine
                .login(conn, test_miner_address(), conn, 2, submit_hash_cap())
                .expect("login");
        }
        engine
            .submit("conn1", "job1".to_string(), 400, Some("00".repeat(32)))
            .expect("share conn1");
        engine
            .submit("conn2", "job1".to_string(), 401, Some("00".repeat(32)))
            .expect("share conn2");
        // Rate limits are per connection: no global pressure.
        engine.record_unprocessed_submit("conn1", UnprocessedReason::RateLimited);
        assert!(!engine.admission().pressure_active(Instant::now()));
        std::thread::sleep(Duration::from_millis(2_200));

        assert_eq!(
            engine.retarget_on_job_if_needed("conn1"),
            Some(100),
            "a session whose submits were just turned away is not idle"
        );
        assert!(
            engine.retarget_on_job_if_needed("conn2").expect("conn2") < 100,
            "an idle session without unprocessed submits still decays"
        );
    }

    #[test]
    fn accepted_shares_do_not_lower_difficulty_under_submit_pressure() {
        let engine = vardiff_test_engine(fast_vardiff_cfg());
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit("conn1", "job1".to_string(), 400, Some("00".repeat(32)))
            .expect("share 1");
        std::thread::sleep(Duration::from_millis(2_200));
        engine.admission().mark_pressure(Instant::now());
        let ack = engine
            .submit("conn1", "job1".to_string(), 401, Some("00".repeat(32)))
            .expect("share 2");
        assert_eq!(ack.next_difficulty, 100);
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
                "",
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
            .escalate_address_risk(AddressRiskEscalation::new(
                &address,
                "bad share",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
            ))
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
    fn submit_without_claimed_hash_is_rejected() {
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");

        let err = engine
            .submit("conn1", "job1".to_string(), 3, None)
            .expect_err("submit without claimed hash should fail");
        assert!(err.to_string().contains("claimed hash required"));
    }

    #[test]
    fn proof_rejected_share_stays_claimed_for_its_template() {
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");

        let err = engine
            .submit("conn1", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("expected invalid share proof");
        assert!(!err.to_string().is_empty());
        assert!(engine
            .seen_in_memory
            .contains(&template_share_key(&job().header_base), 88));

        // The struck session is cut off; another source replaying the same
        // work is a duplicate.
        let banned = engine
            .submit("conn1", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("struck session is cut off");
        assert!(banned.to_string().contains("source banned"), "{banned}");
        engine
            .login("conn2", test_miner_address(), "", 2, submit_hash_cap())
            .expect("other source login");
        let second = engine
            .submit("conn2", "job1".to_string(), 88, Some("ff".repeat(32)))
            .expect_err("replay should be treated as duplicate");
        assert!(second.to_string().contains("duplicate"));
    }

    #[test]
    fn quarantine_error_message_includes_reason_and_expiry() {
        let now = SystemTime::now();
        let message = quarantine_error_message(
            Some(&AddressRiskState {
                address: "addr1".to_string(),
                last_reason: Some("invalid share proof".to_string()),
                quarantined_until: Some(now + Duration::from_secs(120)),
                strikes: 3,
                strike_window_until: Some(now + Duration::from_secs(120)),
                ..AddressRiskState::default()
            }),
            now,
        );
        assert!(message.contains("invalid share proof"));
        assert!(message.contains("risk strikes=3"));
        assert!(message.contains("remaining"));
    }

    #[test]
    fn invalid_proof_strikes_the_source_without_quarantining_the_address() {
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
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        let address = test_miner_address();

        engine
            .login(
                "198.51.100.9:1000",
                address.clone(),
                "",
                2,
                submit_hash_cap(),
            )
            .expect("initial login");
        let err = engine
            .submit(
                "198.51.100.9:1000",
                "job1".to_string(),
                100,
                Some("ff".repeat(32)),
            )
            .expect_err("invalid proof should be rejected");
        assert!(err.to_string().contains("invalid share proof"));
        assert!(engine.source_blocked("198.51.100.9:1000"));
        let again = engine
            .submit(
                "198.51.100.9:1000",
                "job1".to_string(),
                101,
                Some("ff".repeat(32)),
            )
            .expect_err("struck session is cut off");
        assert!(again.to_string().starts_with("source banned"));
        let relogin = engine
            .login(
                "198.51.100.9:1001",
                address.clone(),
                "",
                2,
                submit_hash_cap(),
            )
            .expect_err("the struck pair cannot simply reconnect");
        assert!(relogin.to_string().starts_with("source banned"));

        // The address itself is not quarantined: another source may mine it.
        engine
            .login(
                "203.0.113.5:1000",
                address.clone(),
                "",
                2,
                submit_hash_cap(),
            )
            .expect("other sources keep access to the address");
        assert!(store
            .address_risk_state(&address)
            .expect("risk state")
            .is_none());
    }

    #[test]
    fn accepted_share_stays_claimed_for_its_template() {
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit("conn1", "job1".to_string(), 89, Some("ff".repeat(32)))
            .expect("share should be accepted");

        assert!(engine
            .seen_in_memory
            .contains(&template_share_key(&job().header_base), 89));
    }

    #[test]
    fn same_template_rebind_cannot_recredit_an_accepted_nonce() {
        let cfg = cfg();
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let address = test_miner_address();
        // Two assignments over the same template and nonce window, as produced by
        // a vardiff rebind with same_template_rebind_v1 (PreserveCurrent).
        jobs.insert_assignment("assign-old", "job1", 1, Some(address.clone()), 1_000, 1_999);
        jobs.insert_assignment("assign-new", "job1", 1, Some(address.clone()), 1_000, 1_999);
        let store = Arc::new(InMemoryStore::default());
        let engine = PoolEngine::new(
            cfg,
            validation,
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        engine
            .login("conn1", address, "", 2, submit_hash_cap())
            .expect("login");

        engine
            .submit(
                "conn1",
                "assign-old".to_string(),
                1_234,
                Some("ff".repeat(32)),
            )
            .expect("original share accepted");
        let err = engine
            .submit(
                "conn1",
                "assign-new".to_string(),
                1_234,
                Some("ff".repeat(32)),
            )
            .expect_err("replay under the rebound assignment must be rejected");
        assert!(err.to_string().contains("duplicate share"));

        // A fresh nonce on the new assignment is still accepted.
        engine
            .submit(
                "conn1",
                "assign-new".to_string(),
                1_235,
                Some("ff".repeat(32)),
            )
            .expect("new work under the rebound assignment is accepted");

        let credited = store
            .shares()
            .into_iter()
            .filter(|share| share.status != SHARE_STATUS_REJECTED)
            .map(|share| share.nonce)
            .collect::<Vec<_>>();
        assert_eq!(credited, vec![1_234, 1_235]);
    }

    #[test]
    fn rejected_shares_record_the_submitting_miner_version() {
        let cfg = cfg();
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
        ));
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        let metadata = SubmitMetadata {
            miner_version: Some("0.2.44".to_string()),
            backend: Some("cpu#1".to_string()),
            ..SubmitMetadata::default()
        };
        engine
            .submit("conn1", "job1".to_string(), 77, Some("ff".repeat(32)))
            .expect("first share accepted");
        for job_id in ["job1", "no-such-job"] {
            engine
                .submit_with_received_at_and_metadata(
                    "conn1",
                    job_id.to_string(),
                    77,
                    Some("ff".repeat(32)),
                    Instant::now(),
                    metadata.clone(),
                )
                .expect_err("rejected");
        }

        let rejected = store
            .shares()
            .into_iter()
            .filter(|share| share.status == SHARE_STATUS_REJECTED)
            .count();
        assert_eq!(rejected, 2);
        let expected = (Some("0.2.44".to_string()), Some("cpu#1".to_string()));
        assert_eq!(
            store
                .share_metadata()
                .into_iter()
                .filter(|recorded| *recorded == expected)
                .count(),
            2
        );
    }

    #[test]
    fn same_header_under_another_template_job_is_still_a_duplicate() {
        let cfg = cfg();
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        // A forced same-tip rotation can install a new template job id whose
        // header is byte-identical to the previous one.
        jobs.insert(Job {
            id: "job1-rotated".to_string(),
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        engine
            .submit("conn1", "job1".to_string(), 77, Some("ff".repeat(32)))
            .expect("first share accepted");
        let err = engine
            .submit(
                "conn1",
                "job1-rotated".to_string(),
                77,
                Some("ff".repeat(32)),
            )
            .expect_err("same header and nonce is the same work");
        assert!(err.to_string().contains("duplicate share"));
    }

    #[test]
    fn share_whose_insert_may_have_committed_stays_claimed() {
        let store = Arc::new(InMemoryStore::default());
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = PoolEngine::new(
            cfg(),
            Arc::new(ValidationEngine::new(
                cfg(),
                Arc::new(DeterministicTestHasher),
            )),
            jobs,
            Arc::clone(&store) as Arc<dyn ShareStore>,
            Arc::new(InMemoryNode::default()),
        );
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");
        store.fail_next_share_insert_after_commit();
        engine
            .submit("conn1", "job1".to_string(), 700, Some("ff".repeat(32)))
            .expect_err("insert reported failure");
        let err = engine
            .submit("conn1", "job1".to_string(), 700, Some("ff".repeat(32)))
            .expect_err("the committed share must not be credited again");
        assert!(err.to_string().starts_with("duplicate share"), "{err}");
        let credited = store
            .shares()
            .into_iter()
            .filter(|share| share.nonce == 700 && share.status == SHARE_STATUS_PROVISIONAL)
            .count();
        assert_eq!(credited, 1);
    }

    #[test]
    fn outbox_lock_excludes_other_owners_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let outbox = dir.path().join("found-block-recovery-a.jsonl");
        let owner = OutboxLock::try_acquire(&outbox)
            .unwrap()
            .expect("first owner");
        assert!(OutboxLock::try_acquire(&outbox).unwrap().is_none());
        drop(owner);
        assert!(OutboxLock::try_acquire(&outbox).unwrap().is_some());
    }

    #[test]
    fn growth_mode_overrides_compose_with_reloads() {
        let mut cfg = cfg();
        cfg.vardiff_target_shares = 10;
        cfg.sample_rate = 0.05;
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
        ));
        let engine = PoolEngine::new(
            cfg.clone(),
            Arc::clone(&validation),
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );

        assert!(engine.set_growth_level(1));
        assert!(!engine.set_growth_level(1), "already active");
        assert_eq!(engine.cfg().vardiff_target_shares, 5);
        assert_eq!(engine.cfg().sample_rate, 0.08);
        assert_eq!(validation.snapshot().effective_sample_rate, 0.08);

        // A reload while growing changes the configured values; the growth
        // overrides stay in force.
        engine.reload_hot_config(&Config {
            sample_rate: 0.06,
            hold_audit_rate: 0.5,
            ..cfg.clone()
        });
        assert_eq!(engine.cfg().sample_rate, 0.08);
        assert_eq!(engine.cfg().hold_audit_rate, 0.5);
        assert_eq!(engine.base_cfg().sample_rate, 0.06);

        assert!(engine.set_growth_level(2));
        assert_eq!(engine.cfg().vardiff_target_shares, 3);
        assert_eq!(engine.cfg().sample_rate, 0.08);
        // Disabling the second level in config falls back to the first.
        engine.reload_hot_config(&Config {
            sample_rate: 0.06,
            growth2_submit_rate: 0.0,
            ..cfg.clone()
        });
        assert_eq!(engine.cfg().vardiff_target_shares, 5);
        engine.reload_hot_config(&Config {
            sample_rate: 0.06,
            ..cfg.clone()
        });

        assert!(engine.set_growth_level(0));
        assert_eq!(engine.cfg().vardiff_target_shares, 10);
        assert_eq!(engine.cfg().sample_rate, 0.06);
        assert_eq!(validation.snapshot().effective_sample_rate, 0.06);
    }

    #[test]
    fn config_file_reload_applies_hot_settings_and_rejects_invalid_files() {
        let cfg = cfg();
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(DeterministicTestHasher),
        ));
        let engine = PoolEngine::new(
            cfg.clone(),
            Arc::clone(&validation),
            Arc::new(InMemoryJobs::default()),
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");

        std::fs::write(
            &path,
            r#"{"hold_audit_rate": 0.0, "vardiff_target_shares": 3, "regular_verifiers": 99}"#,
        )
        .unwrap();
        let changed = crate::runtime::reload_hot_config(&path, &engine).expect("valid file");
        assert!(changed.contains(&"hold_audit_rate"));
        assert!(changed.contains(&"vardiff_target_shares"));
        assert_eq!(engine.cfg().hold_audit_rate, 0.0);
        assert_eq!(engine.cfg().vardiff_target_shares, 3);
        assert_eq!(engine.cfg().regular_verifiers, cfg.regular_verifiers);

        std::fs::write(
            &path,
            r#"{"hold_audit_rate": 2.0, "vardiff_target_shares": 7}"#,
        )
        .unwrap();
        assert!(crate::runtime::reload_hot_config(&path, &engine).is_err());
        assert_eq!(
            engine.cfg().hold_audit_rate,
            0.0,
            "invalid file changes nothing"
        );
        assert_eq!(engine.cfg().vardiff_target_shares, 3);
    }

    #[test]
    fn retryable_validation_failure_releases_template_claim() {
        // Nonces 10/11 block in the hasher until released, so the single
        // verifier and the single queue slot are deterministically occupied.
        struct GatedHasher(flume::Receiver<()>);
        impl PowHasher for GatedHasher {
            fn hash(&self, _header_base: &[u8], nonce: u64) -> anyhow::Result<[u8; 32]> {
                if nonce == 10 || nonce == 11 {
                    let _ = self.0.recv_timeout(Duration::from_secs(5));
                }
                Ok([0xff; 32])
            }
        }

        let mut cfg = cfg();
        cfg.validation_mode = "full".to_string();
        cfg.regular_verifiers = 1;
        cfg.regular_validation_queue = 1;
        let (release_tx, release_rx) = flume::unbounded();
        let validation = Arc::new(ValidationEngine::new(
            cfg.clone(),
            Arc::new(GatedHasher(release_rx)),
        ));
        let jobs = Arc::new(InMemoryJobs::default());
        jobs.insert(job());
        let engine = Arc::new(PoolEngine::new(
            cfg,
            Arc::clone(&validation),
            jobs,
            Arc::new(InMemoryStore::default()),
            Arc::new(InMemoryNode::default()),
        ));
        engine
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
            .expect("login");

        let wait_for =
            |what: &str, ready: &dyn Fn(&crate::validation::ValidationSnapshot) -> bool| {
                let deadline = Instant::now() + Duration::from_secs(3);
                while !ready(&validation.snapshot()) {
                    assert!(Instant::now() < deadline, "timed out waiting for {what}");
                    std::thread::sleep(Duration::from_millis(5));
                }
            };
        let spawn_submit = |nonce: u64| {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let _ = engine.submit("conn1", "job1".to_string(), nonce, Some("ff".repeat(32)));
            })
        };
        let first = spawn_submit(10);
        wait_for("verifier busy", &|snap| snap.in_flight == 1);
        let second = spawn_submit(11);
        wait_for("queue slot taken", &|snap| snap.sync_queue_depth == 1);
        let background = vec![first, second];
        let err = engine
            .submit("conn1", "job1".to_string(), 99, Some("ff".repeat(32)))
            .expect_err("saturated validation should return busy");
        assert!(err.to_string().contains("server busy"), "{err}");
        assert!(!engine
            .seen_in_memory
            .contains(&template_share_key(&job().header_base), 99));
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
        for handle in background {
            handle.join().expect("background submit");
        }

        engine
            .submit("conn1", "job1".to_string(), 99, Some("ff".repeat(32)))
            .expect("retry after busy must not be treated as duplicate");
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
            .login("conn1", test_miner_address(), "", 2, submit_hash_cap())
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
