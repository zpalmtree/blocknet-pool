use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{BlockSubmitResponse, Job, NodeApi};
use crate::telemetry::LatencyHistogramSnapshot;

const NODE_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// How long a found block keeps being resubmitted while the daemon is
/// unreachable (restart, proxy 502). The block stays winnable until the
/// network finds the next one, five minutes on average; once it is stale the
/// returning daemon rejects it and the retry stops.
const BLOCK_SUBMIT_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);
const BLOCK_SUBMIT_RETRY_DELAY_MIN: std::time::Duration = std::time::Duration::from_millis(250);
const BLOCK_SUBMIT_RETRY_DELAY_MAX: std::time::Duration = std::time::Duration::from_secs(2);
const WALLET_SEND_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const ORIGINAL_TEMPLATE_LEASE_SECONDS: u64 = 10 * 60;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlockTemplate {
    /// The full template block, kept as raw JSON: it can run to megabytes
    /// when the mempool is busy, and the pool only reads its header and
    /// resubmits it whole in the rare full-block fallback.
    pub block: Box<serde_json::value::RawValue>,
    pub target: String,
    pub header_base: String,
    pub template_id: String,
    #[serde(default)]
    pub template_expires_at_unix_ms: Option<i64>,
    #[serde(default)]
    pub mempool_generation: Option<u64>,
    /// The daemon left the transactions out (`compact=1`); the full block is
    /// fetched separately with `get_template_block`.
    #[serde(default)]
    pub compact: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RenewTemplateResponse {
    pub template_id: String,
    pub template_expires_at_unix_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub peers: i64,
    pub chain_height: u64,
    #[serde(default)]
    pub best_hash: String,
    #[serde(default)]
    pub mempool_size: u64,
    #[serde(default)]
    pub mempool_generation: Option<u64>,
    pub syncing: bool,
    pub current_process_block: Option<NodeCurrentProcessBlock>,
    pub last_process_block: Option<NodeLastProcessBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCurrentProcessBlock {
    pub height: u64,
    pub tx_count: u64,
    pub stage: String,
    pub started_at_unix_millis: i64,
    pub stage_started_at_unix_millis: i64,
    pub elapsed_millis: u64,
    pub stage_elapsed_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLastProcessBlock {
    pub height: u64,
    pub tx_count: u64,
    pub completed_at_unix_millis: i64,
    pub validate_millis: u64,
    pub commit_millis: u64,
    pub reorg_millis: u64,
    pub total_millis: u64,
    pub accepted: bool,
    pub main_chain: bool,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeBlock {
    pub hash: String,
    pub reward: u64,
    pub timestamp: i64,
    #[serde(default)]
    pub difficulty: u64,
    #[serde(default)]
    pub nonce: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WalletSendResponse {
    #[serde(default)]
    pub txid: String,
    pub fee: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WalletSendStatusResponse {
    pub state: String,
    #[serde(default)]
    pub original_status: u16,
    #[serde(default)]
    pub result: Option<WalletSendResponse>,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WalletSendsResponse {
    #[serde(default)]
    pub total: usize,
    #[serde(default)]
    pub sends: Vec<WalletSendHistoryEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WalletSendHistoryEntry {
    #[serde(default)]
    pub txid: String,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub fee: u64,
    #[serde(default)]
    pub total_amount: u64,
    #[serde(default)]
    pub recipients: Vec<WalletSendHistoryRecipient>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WalletSendHistoryRecipient {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub amount: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletAddressResponse {
    pub address: String,
    pub view_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletBalance {
    pub spendable: u64,
    pub pending: u64,
    pub pending_unconfirmed: u64,
    pub pending_unconfirmed_eta: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WalletOutputsResponse {
    pub outputs: Vec<WalletOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WalletOutput {
    pub txid: String,
    pub output_index: u32,
    pub amount: u64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WalletOutputRef {
    pub txid: String,
    pub output_index: u32,
}

impl From<&WalletOutput> for WalletOutputRef {
    fn from(value: &WalletOutput) -> Self {
        Self {
            txid: value.txid.clone(),
            output_index: value.output_index,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WalletRecipient {
    pub address: String,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize)]
struct WalletAdvancedSendRequest<'a> {
    recipients: &'a [WalletRecipient],
    inputs: &'a [WalletOutputRef],
    dry_run: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    change_split: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TxStatus {
    pub confirmations: u64,
    pub in_mempool: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SubmitBlockRawResponse {
    pub accepted: bool,
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub height: u64,
    /// Daemon stage timings (blocknet `timings_us`); absent on older daemons.
    #[serde(default)]
    pub timings_us: Option<SubmitBlockTimings>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SubmitBlockTimings {
    #[serde(default)]
    total: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct HttpError {
    pub path: String,
    pub status_code: u16,
    pub body: String,
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = self.path.split('?').next().unwrap_or(&self.path);
        write!(f, "{} {}: {}", path, self.status_code, self.body)
    }
}

impl std::error::Error for HttpError {}

pub(crate) fn is_http_status(err: &anyhow::Error, status: u16) -> bool {
    err.downcast_ref::<HttpError>()
        .is_some_and(|http_err| http_err.status_code == status)
}

pub(crate) fn http_error_body_contains(err: &anyhow::Error, status: u16, needle: &str) -> bool {
    let Some(http_err) = err.downcast_ref::<HttpError>() else {
        return false;
    };
    if http_err.status_code != status {
        return false;
    }
    let needle = needle.trim();
    if needle.is_empty() {
        return false;
    }
    http_err
        .body
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

pub(crate) fn daemon_instance_identity_for_cookie(cookie_path: &Path) -> Option<String> {
    if cookie_path.as_os_str().is_empty() {
        return None;
    }
    let resolved = fs::canonicalize(cookie_path).ok()?;
    let dir = resolved.parent()?;
    let identity = dir.to_string_lossy().trim().to_string();
    (!identity.is_empty()).then_some(identity)
}

fn is_unknown_or_expired_template_id_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<HttpError>().is_some_and(|http_err| {
        let body = http_err.body.to_ascii_lowercase();
        body.contains("unknown or expired template_id") || body.contains("mining_template_expired")
    })
}

/// Whether a failed block submit may be retried, and whether the daemon might
/// already have received it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockSubmitRetry {
    /// The daemon never got the block: refused connection, proxy 502/503,
    /// daemon busy (429).
    NotDelivered,
    /// The request may have reached the daemon (timeout, dropped response,
    /// proxy 504), so a later "rejected" can mean it was accepted.
    MaybeDelivered,
}

fn block_submit_retry_kind(err: &anyhow::Error) -> Option<BlockSubmitRetry> {
    for cause in err.chain() {
        if let Some(http_err) = cause.downcast_ref::<HttpError>() {
            return match http_err.status_code {
                429 | 502 | 503 => Some(BlockSubmitRetry::NotDelivered),
                504 => Some(BlockSubmitRetry::MaybeDelivered),
                _ => None,
            };
        }
        if let Some(req_err) = cause.downcast_ref::<reqwest::Error>() {
            return Some(if req_err.is_connect() {
                BlockSubmitRetry::NotDelivered
            } else {
                BlockSubmitRetry::MaybeDelivered
            });
        }
    }
    None
}

fn block_submit_retry_delay(attempt: u32) -> std::time::Duration {
    BLOCK_SUBMIT_RETRY_DELAY_MIN
        .saturating_mul(1u32 << attempt.saturating_sub(1).min(8))
        .min(BLOCK_SUBMIT_RETRY_DELAY_MAX)
}

fn is_stale_block_submit_error(err: &anyhow::Error) -> bool {
    http_error_body_contains(err, 400, "block rejected as stale")
}

/// Patch the solved nonce into a retained full template block. The daemon
/// serializes `BlockHeader` without JSON tags, so the live key is `Nonce`;
/// the lowercase variant is handled defensively in case that ever changes.
fn set_full_block_nonce(block: &mut Value, nonce: u64) -> Result<()> {
    let header = block
        .get_mut("header")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("retained template block is missing a header object"))?;
    let key = if header.contains_key("nonce") && !header.contains_key("Nonce") {
        "nonce"
    } else {
        "Nonce"
    };
    header.insert(key.to_string(), Value::from(nonce));
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TemplateLeaseTelemetrySnapshot {
    pub renewal_failures_total: u64,
    pub renewal_expired_responses_total: u64,
    pub submit_expired_responses_total: u64,
    pub block_submit_attempts_total: u64,
    pub block_submit_full_fallback_attempts_total: u64,
    pub block_submit_full_fallback_accepted_total: u64,
    pub block_submit_full_fallback_stale_total: u64,
    pub block_submit_full_fallback_errors_total: u64,
    pub block_submit_retries_total: u64,
    pub block_submit_recovered_total: u64,
    pub last_block_submit_template_age_seconds: u64,
    pub max_accepted_block_template_age_seconds: u64,
    pub accepted_blocks_after_ten_minutes_total: u64,
}

#[derive(Debug)]
pub struct NodeClient {
    base_url: String,
    client: Client,
    send_client: Client,
    events_client: Client,
    auth_token: Mutex<Option<String>>,
    auth_cookie_path: PathBuf,
    chain_height: AtomicU64,
    template_renewal_failures: AtomicU64,
    template_renewal_expired_responses: AtomicU64,
    template_submit_expired_responses: AtomicU64,
    block_submit_attempts: AtomicU64,
    block_submit_full_fallback_attempts: AtomicU64,
    block_submit_full_fallback_accepted: AtomicU64,
    block_submit_full_fallback_stale: AtomicU64,
    block_submit_full_fallback_errors: AtomicU64,
    block_submit_retries: AtomicU64,
    block_submit_recovered: AtomicU64,
    last_block_submit_template_age_seconds: AtomicU64,
    max_accepted_block_template_age_seconds: AtomicU64,
    accepted_blocks_after_ten_minutes: AtomicU64,
    block_submit_retry_window: std::time::Duration,
}

impl NodeClient {
    pub fn new(base_url: &str, daemon_cookie_path: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .default_headers(headers)
            .timeout(NODE_HTTP_TIMEOUT)
            .build()
            .context("build node http client")?;
        // Large wallet sends can take minutes when coin control selects many inputs.
        // Use a longer timeout for live sends so the pool records the txid instead of
        // treating a slow success as a transient transport failure.
        let send_client = Client::builder()
            .default_headers(HeaderMap::from_iter([(
                CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )]))
            .timeout(WALLET_SEND_HTTP_TIMEOUT)
            .build()
            .context("build node http client")?;
        let events_client = Client::builder()
            .default_headers(HeaderMap::new())
            .timeout(None::<std::time::Duration>)
            .build()
            .context("build node events http client")?;

        let auth_cookie_path = PathBuf::from(daemon_cookie_path.trim());
        let auth_cookie_configured = !auth_cookie_path.as_os_str().is_empty();
        if auth_cookie_configured {
            tracing::info!(path = %auth_cookie_path.display(), "daemon auth cookie source configured");
        }
        let resolved_token = if auth_cookie_configured && auth_cookie_path.exists() {
            Some(
                read_token_from_cookie_file(&auth_cookie_path)
                    .context("failed to load daemon token from configured cookie")?,
            )
        } else {
            None
        };
        if resolved_token.is_some() {
            tracing::info!("daemon auth token loaded");
        }

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            send_client,
            events_client,
            auth_token: Mutex::new(resolved_token),
            auth_cookie_path,
            chain_height: AtomicU64::new(0),
            template_renewal_failures: AtomicU64::new(0),
            template_renewal_expired_responses: AtomicU64::new(0),
            template_submit_expired_responses: AtomicU64::new(0),
            block_submit_attempts: AtomicU64::new(0),
            block_submit_full_fallback_attempts: AtomicU64::new(0),
            block_submit_full_fallback_accepted: AtomicU64::new(0),
            block_submit_full_fallback_stale: AtomicU64::new(0),
            block_submit_full_fallback_errors: AtomicU64::new(0),
            block_submit_retries: AtomicU64::new(0),
            block_submit_recovered: AtomicU64::new(0),
            last_block_submit_template_age_seconds: AtomicU64::new(0),
            max_accepted_block_template_age_seconds: AtomicU64::new(0),
            accepted_blocks_after_ten_minutes: AtomicU64::new(0),
            block_submit_retry_window: BLOCK_SUBMIT_RETRY_WINDOW,
        })
    }

    #[cfg(test)]
    fn with_block_submit_retry_window(mut self, window: std::time::Duration) -> Self {
        self.block_submit_retry_window = window;
        self
    }

    /// Identify the daemon instance behind the API: the data directory that
    /// holds the (possibly symlinked) auth cookie. Daemon cutovers switch
    /// that symlink, so the identity changes exactly when the pool starts
    /// talking to a different daemon, and stays stable across restarts of
    /// the same daemon (whose idempotency journal lives in that directory).
    pub(crate) fn daemon_instance_identity(&self) -> Option<String> {
        daemon_instance_identity_for_cookie(&self.auth_cookie_path)
    }

    pub fn get_status(&self) -> Result<NodeStatus> {
        let status: NodeStatus = self.get_json("/api/status")?;
        self.chain_height
            .store(status.chain_height, Ordering::Relaxed);
        Ok(status)
    }

    /// Daemon block-path latency histograms (`GET /api/perf/latency`), keyed
    /// by family then lane. Older daemons answer 404.
    pub fn get_perf_latency(
        &self,
    ) -> Result<BTreeMap<String, BTreeMap<String, LatencyHistogramSnapshot>>> {
        #[derive(Deserialize)]
        struct PerfLatency {
            #[serde(default)]
            families: BTreeMap<String, BTreeMap<String, LatencyHistogramSnapshot>>,
        }
        let resp: PerfLatency = self.get_json("/api/perf/latency")?;
        Ok(resp.families)
    }

    pub(crate) fn get_block_template(&self, reward_address: Option<&str>) -> Result<BlockTemplate> {
        // Daemons without compact templates ignore the parameter and send the
        // full block, which `BlockTemplate::compact` then reports as false.
        let mut path = "/api/mining/blocktemplate?compact=1".to_string();
        if let Some(address) = reward_address.filter(|v| !v.trim().is_empty()) {
            path.push_str("&address=");
            path.push_str(&urlencoding::encode(address));
        }
        self.get_json(&path)
    }

    /// The full block of a template fetched compact, while its lease lasts.
    pub(crate) fn get_template_block(
        &self,
        template_id: &str,
    ) -> Result<Box<serde_json::value::RawValue>> {
        self.get_json(&format!(
            "/api/mining/templateblock?template_id={}",
            urlencoding::encode(template_id)
        ))
    }

    pub(crate) fn renew_block_template(&self, template_id: &str) -> Result<RenewTemplateResponse> {
        let payload = serde_json::json!({ "template_id": template_id });
        // The job manager has the lease and parent needed to distinguish a
        // normal tip invalidation from a lost/expired current-tip template.
        self.post_json("/api/mining/renewtemplate", &payload)
    }

    pub(crate) fn get_template_recovery_status(&self) -> Result<NodeStatus> {
        // Diagnostic failure must not hold up replacement work for the normal
        // 30-second API timeout. An unavailable status preserves the alert.
        self.get_json_with_timeout("/api/status", std::time::Duration::from_secs(2))
    }

    pub(crate) fn record_template_renewal_error(&self, err: &anyhow::Error) {
        self.record_template_renewal_failure();
        if is_http_status(err, 404) && is_unknown_or_expired_template_id_error(err) {
            self.template_renewal_expired_responses
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_template_renewal_failure(&self) {
        self.template_renewal_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn template_lease_telemetry(&self) -> TemplateLeaseTelemetrySnapshot {
        TemplateLeaseTelemetrySnapshot {
            renewal_failures_total: self.template_renewal_failures.load(Ordering::Relaxed),
            renewal_expired_responses_total: self
                .template_renewal_expired_responses
                .load(Ordering::Relaxed),
            submit_expired_responses_total: self
                .template_submit_expired_responses
                .load(Ordering::Relaxed),
            block_submit_attempts_total: self.block_submit_attempts.load(Ordering::Relaxed),
            block_submit_full_fallback_attempts_total: self
                .block_submit_full_fallback_attempts
                .load(Ordering::Relaxed),
            block_submit_full_fallback_accepted_total: self
                .block_submit_full_fallback_accepted
                .load(Ordering::Relaxed),
            block_submit_full_fallback_stale_total: self
                .block_submit_full_fallback_stale
                .load(Ordering::Relaxed),
            block_submit_full_fallback_errors_total: self
                .block_submit_full_fallback_errors
                .load(Ordering::Relaxed),
            block_submit_retries_total: self.block_submit_retries.load(Ordering::Relaxed),
            block_submit_recovered_total: self.block_submit_recovered.load(Ordering::Relaxed),
            last_block_submit_template_age_seconds: self
                .last_block_submit_template_age_seconds
                .load(Ordering::Relaxed),
            max_accepted_block_template_age_seconds: self
                .max_accepted_block_template_age_seconds
                .load(Ordering::Relaxed),
            accepted_blocks_after_ten_minutes_total: self
                .accepted_blocks_after_ten_minutes
                .load(Ordering::Relaxed),
        }
    }

    fn record_accepted_block_template_age(&self, template_age_seconds: u64) {
        self.max_accepted_block_template_age_seconds
            .fetch_max(template_age_seconds, Ordering::Relaxed);
        if template_age_seconds >= ORIGINAL_TEMPLATE_LEASE_SECONDS {
            self.accepted_blocks_after_ten_minutes
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Last-resort submission path: the daemon no longer knows the compact
    /// template ID (typically a restart cleared its lease cache), so submit
    /// the retained full template block with the solved nonce patched in.
    /// Returns `Ok(None)` when no full block was retained for this job.
    fn submit_retained_full_block(
        &self,
        job: &Job,
        nonce: u64,
        template_age_seconds: u64,
        compact_err: &anyhow::Error,
    ) -> Result<Option<BlockSubmitResponse>> {
        let Some(template_block) = job.template_block.as_ref() else {
            return Ok(None);
        };
        let mut block: Value = serde_json::from_str(template_block)
            .context("parse retained full block for fallback submit")?;
        set_full_block_nonce(&mut block, nonce)
            .context("prepare retained full block for fallback submit")?;
        self.block_submit_full_fallback_attempts
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            job_id = %job.id,
            template_id = %job.template_id,
            height = job.height,
            template_age_seconds,
            compact_error = %compact_err,
            "compact template ID unknown or expired; submitting retained full block"
        );
        let resp: SubmitBlockRawResponse = match self.post_json("/api/mining/submitblock", &block) {
            Ok(resp) => resp,
            Err(err) => {
                if is_stale_block_submit_error(&err) {
                    self.block_submit_full_fallback_stale
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        job_id = %job.id,
                        template_id = %job.template_id,
                        height = job.height,
                        template_age_seconds,
                        "retained full block rejected as stale after chain tip advanced"
                    );
                } else {
                    self.block_submit_full_fallback_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                let context =
                    format!("full-block fallback submit after unknown/expired template ID: {err}");
                return Err(err).context(context);
            }
        };
        if resp.accepted {
            self.block_submit_full_fallback_accepted
                .fetch_add(1, Ordering::Relaxed);
            self.record_accepted_block_template_age(template_age_seconds);
        } else {
            self.block_submit_full_fallback_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(BlockSubmitResponse {
            accepted: resp.accepted,
            hash: if resp.hash.is_empty() {
                None
            } else {
                Some(resp.hash)
            },
            height: if resp.height == 0 {
                None
            } else {
                Some(resp.height)
            },
            daemon_total_us: resp.timings_us.and_then(|timings| timings.total),
        }))
    }

    pub fn get_block(&self, id: &str) -> Result<NodeBlock> {
        self.get_json(&format!("/api/block/{id}"))
    }

    pub fn get_block_by_height_optional(&self, height: u64) -> Result<Option<NodeBlock>> {
        match self.get_block(&height.to_string()) {
            Ok(block) => Ok(Some(block)),
            Err(err) if is_http_status(&err, 404) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Fetch canonical identities without transferring full blocks. The fallback
    /// keeps pool deployments compatible with daemons during a rolling upgrade.
    pub fn get_block_hashes(&self, heights: &[u64]) -> Result<HashMap<u64, String>> {
        #[derive(Deserialize)]
        struct Hashes {
            hashes: HashMap<u64, String>,
        }
        let mut result = HashMap::new();
        for chunk in heights.chunks(1024) {
            let query = chunk
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            match self.get_json::<Hashes>(&format!("/api/block-hashes?heights={query}")) {
                Ok(response) => result.extend(response.hashes),
                Err(err) if is_http_status(&err, 404) => {
                    std::thread::scope(|scope| -> Result<()> {
                        let tasks = chunk
                            .chunks(chunk.len().div_ceil(16).max(1))
                            .map(|part| {
                                scope.spawn(move || -> Result<Vec<(u64, String)>> {
                                    let mut rows = Vec::new();
                                    for height in part {
                                        if let Some(block) =
                                            self.get_block_by_height_optional(*height)?
                                        {
                                            rows.push((*height, block.hash));
                                        }
                                    }
                                    Ok(rows)
                                })
                            })
                            .collect::<Vec<_>>();
                        for task in tasks {
                            result.extend(
                                task.join()
                                    .map_err(|_| anyhow!("block lookup worker panicked"))??,
                            );
                        }
                        Ok(())
                    })?;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(result)
    }

    /// Errors abort the whole read; an unavailable daemon must never be treated
    /// as evidence that a payout transaction is missing.
    pub fn get_existing_transactions(&self, txids: &[String]) -> Result<HashSet<String>> {
        std::thread::scope(|scope| {
            let tasks = txids
                .chunks(txids.len().div_ceil(16).max(1))
                .map(|part| {
                    scope.spawn(move || -> Result<Vec<String>> {
                        let mut found = Vec::new();
                        for txid in part {
                            if self.get_tx_status_optional(txid)?.is_some() {
                                found.push(txid.clone());
                            }
                        }
                        Ok(found)
                    })
                })
                .collect::<Vec<_>>();
            let mut found = HashSet::new();
            for task in tasks {
                found.extend(
                    task.join()
                        .map_err(|_| anyhow!("transaction lookup worker panicked"))??,
                );
            }
            Ok(found)
        })
    }

    pub fn get_tx_status_optional(&self, txid: &str) -> Result<Option<TxStatus>> {
        match self.get_json(&format!("/api/tx/{txid}")) {
            Ok(status) => Ok(Some(status)),
            Err(err) if is_http_status(&err, 404) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn wallet_load(&self, password: &str) -> Result<()> {
        let payload = serde_json::json!({ "password": password });
        let _: Value = self.post_json("/api/wallet/load", &payload)?;
        Ok(())
    }

    pub(crate) fn wallet_unlock(&self, password: &str) -> Result<()> {
        let payload = serde_json::json!({ "password": password });
        let _: Value = self.post_json("/api/wallet/unlock", &payload)?;
        Ok(())
    }

    pub(crate) fn get_wallet_address(&self) -> Result<WalletAddressResponse> {
        self.get_json("/api/wallet/address")
    }

    pub fn get_wallet_balance(&self) -> Result<WalletBalance> {
        self.get_json("/api/wallet/balance")
    }

    pub(crate) fn get_wallet_outputs(&self) -> Result<WalletOutputsResponse> {
        self.get_json("/api/wallet/outputs?status=unspent")
    }

    pub(crate) fn wallet_send_advanced(
        &self,
        recipients: &[WalletRecipient],
        inputs: &[WalletOutputRef],
        change_split: u32,
        idempotency_key: &str,
        dry_run: bool,
    ) -> Result<WalletSendResponse> {
        let payload = WalletAdvancedSendRequest {
            recipients,
            inputs,
            dry_run,
            change_split: (change_split > 1).then_some(change_split),
        };
        let client = if dry_run {
            &self.client
        } else {
            &self.send_client
        };
        self.post_json_with_headers_using_client(
            client,
            "/api/wallet/send/advanced",
            &payload,
            (!dry_run && !idempotency_key.is_empty())
                .then_some(("Idempotency-Key", idempotency_key)),
        )
    }

    pub(crate) fn get_wallet_send_advanced_status(
        &self,
        idempotency_key: &str,
    ) -> Result<WalletSendStatusResponse> {
        let mut path = "/api/wallet/send/advanced/status?idempotency_key=".to_string();
        path.push_str(&urlencoding::encode(idempotency_key));
        self.get_json(&path)
    }

    pub(crate) fn get_wallet_sends(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<WalletSendsResponse> {
        let path = format!("/api/wallet/sends?limit={limit}&offset={offset}&order=desc");
        self.get_json(&path)
    }

    pub(crate) fn open_events_stream(&self) -> Result<Response> {
        let path = "/api/events";
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let req = self.apply_auth(self.events_client.get(&url));
            let resp = req.send().with_context(|| format!("GET {path}"))?;
            let status = resp.status();

            if status == reqwest::StatusCode::UNAUTHORIZED
                && self.refresh_after_unauthorized(&mut attempted_refresh)
            {
                continue;
            }

            if !status.is_success() {
                let body = resp.text().unwrap_or_default();
                return Err(anyhow!(HttpError {
                    path: path.to_string(),
                    status_code: status.as_u16(),
                    body,
                }));
            }

            return Ok(resp);
        }
    }

    pub fn chain_height(&self) -> u64 {
        self.chain_height.load(Ordering::Relaxed)
    }

    pub(crate) fn observe_chain_height(&self, height: u64) {
        self.chain_height.store(height, Ordering::Relaxed);
    }

    fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        self.get_json_with_timeout(path, NODE_HTTP_TIMEOUT)
    }

    fn get_json_with_timeout<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        timeout: std::time::Duration,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let req = self.apply_auth(self.client.get(&url).timeout(timeout));
            let resp = req.send().with_context(|| format!("GET {path}"))?;
            let status = resp.status();
            let body = resp.text().unwrap_or_default();

            if status == reqwest::StatusCode::UNAUTHORIZED
                && self.refresh_after_unauthorized(&mut attempted_refresh)
            {
                continue;
            }

            if !status.is_success() {
                return Err(anyhow!(HttpError {
                    path: path.to_string(),
                    status_code: status.as_u16(),
                    body,
                }));
            }
            return serde_json::from_str(&body)
                .with_context(|| format!("decode JSON response for GET {path}"));
        }
    }

    fn post_json<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<R> {
        self.post_json_with_headers_using_client(&self.client, path, payload, None)
    }

    fn post_json_with_headers_using_client<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        client: &Client,
        path: &str,
        payload: &T,
        header: Option<(&str, &str)>,
    ) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let mut req = client.post(&url).json(payload);
            if let Some((name, value)) = header {
                req = req.header(name, value);
            }
            let req = self.apply_auth(req);

            let resp = req.send().with_context(|| format!("POST {path}"))?;
            let status = resp.status();
            let body = resp.text().unwrap_or_default();

            if status == reqwest::StatusCode::UNAUTHORIZED
                && self.refresh_after_unauthorized(&mut attempted_refresh)
            {
                continue;
            }

            if !status.is_success() {
                return Err(anyhow!(HttpError {
                    path: path.to_string(),
                    status_code: status.as_u16(),
                    body,
                }));
            }
            return serde_json::from_str(&body)
                .with_context(|| format!("decode JSON response for POST {path}"));
        }
    }

    fn apply_auth(
        &self,
        request: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        if let Some(token) = self.auth_token.lock().clone() {
            request.bearer_auth(token)
        } else {
            request
        }
    }

    fn refresh_token_from_cookie(&self) -> Result<bool> {
        if self.auth_cookie_path.as_os_str().is_empty() || !self.auth_cookie_path.exists() {
            return Ok(false);
        }

        let token = read_token_from_cookie_file(&self.auth_cookie_path)
            .context("failed to load daemon token from configured cookie")?;
        let mut guard = self.auth_token.lock();
        if guard.as_deref() == Some(token.as_str()) {
            return Ok(false);
        }
        *guard = Some(token);
        tracing::info!(
            path = %self.auth_cookie_path.display(),
            "refreshed daemon API token from cookie"
        );
        Ok(true)
    }

    fn refresh_after_unauthorized(&self, attempted_refresh: &mut bool) -> bool {
        if *attempted_refresh {
            return false;
        }
        *attempted_refresh = true;
        match self.refresh_token_from_cookie() {
            Ok(refreshed) => refreshed,
            Err(err) => {
                tracing::warn!(error = %err, "failed to refresh daemon token from cookie");
                false
            }
        }
    }
}

fn read_token_from_cookie_file(cookie_path: &Path) -> Result<String> {
    let token = fs::read_to_string(cookie_path)
        .with_context(|| format!("failed to read cookie file at {}", cookie_path.display()))?;
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("cookie file is empty: {}", cookie_path.display()));
    }
    Ok(trimmed.to_string())
}

impl NodeClient {
    /// After an attempt that may have reached the daemon, a rejection can
    /// mean our block is already in the chain. It is ours when the block at
    /// the job height has our nonce.
    fn accepted_block_in_chain(&self, job: &Job, nonce: u64) -> Option<BlockSubmitResponse> {
        let block = self.get_block_by_height_optional(job.height).ok()??;
        (block.nonce == Some(nonce)).then(|| BlockSubmitResponse {
            accepted: true,
            hash: (!block.hash.is_empty()).then_some(block.hash),
            height: Some(job.height),
            daemon_total_us: None,
        })
    }

    fn submit_block_once(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        let template_id = job.template_id.trim();
        if template_id.is_empty() {
            return Err(anyhow!("missing template_id for compact block submit"));
        }
        let payload = serde_json::json!({
            "template_id": template_id,
            "nonce": nonce,
        });
        let template_age_seconds = job.template_created_at.elapsed().as_secs();
        self.block_submit_attempts.fetch_add(1, Ordering::Relaxed);
        self.last_block_submit_template_age_seconds
            .store(template_age_seconds, Ordering::Relaxed);
        let resp: SubmitBlockRawResponse = match self.post_json("/api/mining/submitblock", &payload)
        {
            Ok(resp) => resp,
            Err(err) => {
                if is_unknown_or_expired_template_id_error(&err) {
                    self.template_submit_expired_responses
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(resp) =
                        self.submit_retained_full_block(job, nonce, template_age_seconds, &err)?
                    {
                        return Ok(resp);
                    }
                }
                return Err(err);
            }
        };
        if resp.accepted {
            self.record_accepted_block_template_age(template_age_seconds);
        }
        Ok(BlockSubmitResponse {
            accepted: resp.accepted,
            hash: if resp.hash.is_empty() {
                None
            } else {
                Some(resp.hash)
            },
            height: if resp.height == 0 {
                None
            } else {
                Some(resp.height)
            },
            daemon_total_us: resp.timings_us.and_then(|timings| timings.total),
        })
    }
}

impl NodeApi for NodeClient {
    /// Submits a found block, resubmitting while the daemon is unreachable
    /// for up to `BLOCK_SUBMIT_RETRY_WINDOW`. A restarted daemon has
    /// forgotten the template ID, so a retry falls back to the full block.
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        let started_at = Instant::now();
        let mut attempt = 0u32;
        let mut maybe_delivered = false;
        loop {
            attempt += 1;
            let err = match self.submit_block_once(job, nonce) {
                Ok(resp) => {
                    if attempt > 1 && resp.accepted {
                        self.block_submit_recovered.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            height = job.height,
                            nonce,
                            attempts = attempt,
                            elapsed_ms = started_at.elapsed().as_millis() as u64,
                            "block submit succeeded after retrying an unreachable daemon"
                        );
                    }
                    return Ok(resp);
                }
                Err(err) => err,
            };
            let retry = block_submit_retry_kind(&err);
            if retry.is_none() && maybe_delivered {
                if let Some(resp) = self.accepted_block_in_chain(job, nonce) {
                    self.block_submit_recovered.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        height = job.height,
                        nonce,
                        attempts = attempt,
                        error = %err,
                        "block submit retry was rejected but an earlier attempt is in the chain"
                    );
                    return Ok(resp);
                }
            }
            let Some(retry) = retry else {
                return Err(err);
            };
            maybe_delivered |= retry == BlockSubmitRetry::MaybeDelivered;
            let delay = block_submit_retry_delay(attempt);
            if started_at.elapsed() + delay > self.block_submit_retry_window {
                if maybe_delivered {
                    if let Some(resp) = self.accepted_block_in_chain(job, nonce) {
                        self.block_submit_recovered.fetch_add(1, Ordering::Relaxed);
                        return Ok(resp);
                    }
                }
                tracing::error!(
                    height = job.height,
                    nonce,
                    attempts = attempt,
                    error = %err,
                    "giving up on block submit: daemon unreachable for the whole retry window"
                );
                return Err(err);
            }
            self.block_submit_retries.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                height = job.height,
                nonce,
                attempt,
                retry_in_ms = delay.as_millis() as u64,
                error = %format!("{err:#}"),
                "block submit failed: daemon unreachable; retrying"
            );
            std::thread::sleep(delay);
        }
    }

    fn current_chain_height(&self) -> Result<u64> {
        Ok(self.get_status()?.chain_height)
    }

    fn block_hash_at_height(&self, height: u64) -> Result<Option<String>> {
        Ok(self
            .get_block_by_height_optional(height)?
            .map(|block| block.hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wallet_send_timeout_exceeds_default_request_timeout() {
        assert!(WALLET_SEND_HTTP_TIMEOUT > NODE_HTTP_TIMEOUT);
    }

    #[test]
    fn block_template_lease_is_backward_compatible_and_renewal_response_parses() {
        let legacy: BlockTemplate = serde_json::from_value(serde_json::json!({
            "block": {},
            "target": "00",
            "header_base": "00",
            "template_id": "legacy"
        }))
        .expect("legacy response should parse");
        assert_eq!(legacy.template_expires_at_unix_ms, None);
        assert_eq!(legacy.mempool_generation, None);

        let leased: BlockTemplate = serde_json::from_value(serde_json::json!({
            "block": {},
            "target": "00",
            "header_base": "00",
            "template_id": "leased",
            "template_expires_at_unix_ms": 1_750_000_000_000_i64,
            "mempool_generation": 17
        }))
        .expect("leased response should parse");
        assert_eq!(
            leased.template_expires_at_unix_ms,
            Some(1_750_000_000_000_i64)
        );
        assert_eq!(leased.mempool_generation, Some(17));

        let renewed: RenewTemplateResponse = serde_json::from_value(serde_json::json!({
            "template_id": "leased",
            "template_expires_at_unix_ms": 1_750_000_600_000_i64
        }))
        .expect("renewal response should parse");
        assert_eq!(renewed.template_id, "leased");
        assert_eq!(renewed.template_expires_at_unix_ms, 1_750_000_600_000_i64);
    }

    #[test]
    fn node_status_mempool_generation_is_backward_compatible() {
        let legacy: NodeStatus = serde_json::from_value(serde_json::json!({
            "peers": 1,
            "chain_height": 9,
            "syncing": false,
            "current_process_block": null,
            "last_process_block": null
        }))
        .expect("legacy status should parse");
        assert_eq!(legacy.mempool_size, 0);
        assert_eq!(legacy.mempool_generation, None);

        let current: NodeStatus = serde_json::from_value(serde_json::json!({
            "peers": 1,
            "chain_height": 9,
            "mempool_size": 3,
            "mempool_generation": 17,
            "syncing": false,
            "current_process_block": null,
            "last_process_block": null
        }))
        .expect("generation-aware status should parse");
        assert_eq!(current.mempool_size, 3);
        assert_eq!(current.mempool_generation, Some(17));
    }

    #[test]
    fn submit_block_response_reads_daemon_total_timing() {
        let resp: SubmitBlockRawResponse = serde_json::from_str(
            r#"{"accepted":true,"hash":"ab","height":9,"timings_us":{"process":180000,"total":185100}}"#,
        )
        .expect("parse submit response");
        assert_eq!(
            resp.timings_us.and_then(|timings| timings.total),
            Some(185_100)
        );

        let legacy: SubmitBlockRawResponse =
            serde_json::from_str(r#"{"accepted":true,"hash":"ab","height":9}"#)
                .expect("parse legacy submit response");
        assert!(legacy.timings_us.is_none());
    }

    /// What the scripted daemon does with one HTTP request.
    enum Reply {
        Status(u16, &'static str),
        /// Read the request, then close without answering.
        Drop,
    }

    /// A daemon stand-in answering requests in order from `script`, keyed by
    /// whether the request line starts with each prefix. Returns its URL and
    /// the request lines it saw.
    fn scripted_daemon(
        script: Vec<(&'static str, Reply)>,
    ) -> (String, std::sync::Arc<Mutex<Vec<String>>>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind scripted daemon");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let seen_by_server = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut script = std::collections::VecDeque::from(script);
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                // Read headers, then the Content-Length body.
                let header_end = loop {
                    let n = stream.read(&mut buf).unwrap_or(0);
                    if n == 0 {
                        break None;
                    }
                    request.extend_from_slice(&buf[..n]);
                    if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break Some(pos + 4);
                    }
                };
                let Some(header_end) = header_end else {
                    continue;
                };
                let head = String::from_utf8_lossy(&request[..header_end]).to_string();
                let body_len = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                while request.len() < header_end + body_len {
                    let n = stream.read(&mut buf).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..n]);
                }
                let line = head.lines().next().unwrap_or_default().to_string();
                seen_by_server.lock().push(line.clone());
                let index = script
                    .iter()
                    .position(|(prefix, _)| line.starts_with(prefix));
                match index
                    .and_then(|index| script.remove(index))
                    .map(|(_, reply)| reply)
                {
                    Some(Reply::Status(code, body)) => {
                        let _ = write!(
                            stream,
                            "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                    }
                    Some(Reply::Drop) | None => {}
                }
            }
        });
        (url, seen)
    }

    fn retry_test_job(template_block: Option<Value>) -> Job {
        Job {
            id: "job1".to_string(),
            height: 7,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: "tmpl-1".to_string(),
            prev_hash: None,
            template_created_at: std::time::Instant::now(),
            template_block: template_block.map(|block| std::sync::Arc::from(block.to_string())),
        }
    }

    #[test]
    fn block_submit_retries_through_a_daemon_restart() {
        // Proxy 502 while the daemon is down, then the restarted daemon has
        // forgotten the template ID and takes the retained full block.
        let (url, seen) = scripted_daemon(vec![
            (
                "POST /api/mining/submitblock",
                Reply::Status(502, "<html>502 Bad Gateway</html>"),
            ),
            (
                "POST /api/mining/submitblock",
                Reply::Status(502, "<html>502 Bad Gateway</html>"),
            ),
            (
                "POST /api/mining/submitblock",
                Reply::Status(400, r#"{"error":"unknown or expired template_id"}"#),
            ),
            (
                "POST /api/mining/submitblock",
                Reply::Status(200, r#"{"accepted":true,"hash":"abc","height":7}"#),
            ),
        ]);
        let client = NodeClient::new(&url, "").expect("node client");
        let job = retry_test_job(Some(serde_json::json!({"header": {"nonce": 0}})));

        let resp = client
            .submit_block(&job, 42)
            .expect("block should be accepted");

        assert!(resp.accepted);
        assert_eq!(seen.lock().len(), 4);
        let telemetry = client.template_lease_telemetry();
        assert_eq!(telemetry.block_submit_retries_total, 2);
        assert_eq!(telemetry.block_submit_recovered_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_attempts_total, 1);
    }

    #[test]
    fn block_submit_gives_up_after_the_retry_window() {
        let unused = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", unused.local_addr().expect("addr"));
        drop(unused);
        let client = NodeClient::new(&url, "")
            .expect("node client")
            .with_block_submit_retry_window(std::time::Duration::from_millis(600));
        let started_at = std::time::Instant::now();

        client
            .submit_block(&retry_test_job(None), 42)
            .expect_err("refused connections should fail once the window ends");

        assert!(started_at.elapsed() < std::time::Duration::from_secs(5));
        assert!(client.template_lease_telemetry().block_submit_retries_total >= 1);
    }

    #[test]
    fn block_submit_recognizes_an_earlier_attempt_that_reached_the_chain() {
        // The first response is lost; the retry is rejected because our block
        // is already the tip, and the chain shows our nonce at that height.
        let (url, _) = scripted_daemon(vec![
            ("POST /api/mining/submitblock", Reply::Drop),
            (
                "POST /api/mining/submitblock",
                Reply::Status(400, r#"{"error":"block rejected as stale"}"#),
            ),
            (
                "GET /api/block/7",
                Reply::Status(
                    200,
                    r#"{"hash":"abc","reward":1,"timestamp":1,"difficulty":1,"nonce":42}"#,
                ),
            ),
        ]);
        let client = NodeClient::new(&url, "").expect("node client");

        let resp = client
            .submit_block(&retry_test_job(None), 42)
            .expect("earlier attempt should count as accepted");

        assert!(resp.accepted);
        assert_eq!(resp.hash.as_deref(), Some("abc"));
        assert_eq!(
            client
                .template_lease_telemetry()
                .block_submit_recovered_total,
            1
        );
    }

    #[test]
    fn block_submit_does_not_retry_a_stale_rejection() {
        let (url, seen) = scripted_daemon(vec![(
            "POST /api/mining/submitblock",
            Reply::Status(400, r#"{"error":"block rejected as stale"}"#),
        )]);
        let client = NodeClient::new(&url, "").expect("node client");

        client
            .submit_block(&retry_test_job(None), 42)
            .expect_err("a stale block is final");

        assert_eq!(seen.lock().len(), 1);
        assert_eq!(
            client.template_lease_telemetry().block_submit_retries_total,
            0
        );
    }

    #[test]
    fn submit_block_requires_compact_template_id() {
        let client = NodeClient::new("http://127.0.0.1:1", "").expect("node client");
        let job = Job {
            id: "job1".to_string(),
            height: 1,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 1,
            template_id: String::new(),
            prev_hash: None,
            template_created_at: std::time::Instant::now(),
            template_block: None,
        };

        let err = client
            .submit_block(&job, 42)
            .expect_err("missing template_id should fail before network submit");
        assert!(err.to_string().contains("missing template_id"));
    }

    #[test]
    fn unknown_or_expired_template_errors_are_classified() {
        let expired = anyhow!(HttpError {
            path: "/api/mining/submitblock".to_string(),
            status_code: 400,
            body: r#"{"error":"unknown or expired template_id"}"#.to_string(),
        });
        assert!(is_unknown_or_expired_template_id_error(&expired));

        let coded = anyhow!(HttpError {
            path: "/api/mining/submitblock".to_string(),
            status_code: 400,
            body: r#"{"code":"mining_template_expired","error":"template unavailable"}"#
                .to_string(),
        });
        assert!(is_unknown_or_expired_template_id_error(&coded));

        let stale = anyhow!(HttpError {
            path: "/api/mining/submitblock".to_string(),
            status_code: 400,
            body: r#"{"error":"block rejected as stale"}"#.to_string(),
        });
        assert!(!is_unknown_or_expired_template_id_error(&stale));
    }

    #[test]
    fn set_full_block_nonce_prefers_live_daemon_key_casing() {
        let mut capitalized = serde_json::json!({
            "header": {"Height": 5, "Difficulty": 9, "Nonce": 0}
        });
        set_full_block_nonce(&mut capitalized, 42).expect("patch capitalized nonce");
        assert_eq!(capitalized["header"]["Nonce"].as_u64(), Some(42));
        assert!(capitalized["header"].get("nonce").is_none());

        let mut lowercase = serde_json::json!({
            "header": {"height": 5, "nonce": 0}
        });
        set_full_block_nonce(&mut lowercase, 7).expect("patch lowercase nonce");
        assert_eq!(lowercase["header"]["nonce"].as_u64(), Some(7));
        assert!(lowercase["header"].get("Nonce").is_none());

        let mut missing_key = serde_json::json!({
            "header": {"Height": 5}
        });
        set_full_block_nonce(&mut missing_key, 9).expect("insert nonce key");
        assert_eq!(missing_key["header"]["Nonce"].as_u64(), Some(9));

        let mut no_header = serde_json::json!({"txns": []});
        let err = set_full_block_nonce(&mut no_header, 1).expect_err("missing header should fail");
        assert!(err.to_string().contains("missing a header object"));
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;

        let mut raw = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = stream.read(&mut buffer).expect("read daemon request");
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&buffer[..read]);
            let text = String::from_utf8_lossy(&raw);
            if let Some(header_end) = text.find("\r\n\r\n") {
                let content_length = text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if raw.len() >= header_end + 4 + content_length {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&raw).into_owned()
    }

    #[test]
    fn transaction_presence_distinguishes_missing_from_daemon_errors() {
        use std::io::Write;
        use std::net::TcpListener;
        for unavailable in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                for _ in 0..3 {
                    let (mut stream, _) = listener.accept().unwrap();
                    let request = read_http_request(&mut stream);
                    let (status, body) = if request.starts_with("GET /api/tx/found ") {
                        ("200 OK", r#"{"confirmations":1,"in_mempool":false}"#)
                    } else if request.starts_with("GET /api/tx/unknown ") && unavailable {
                        ("503 Service Unavailable", "{}")
                    } else {
                        ("404 Not Found", "{}")
                    };
                    write!(stream,"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
                }
            });
            let client = NodeClient::new(&format!("http://{address}"), "").unwrap();
            let result = client.get_existing_transactions(&[
                "found".into(),
                "missing".into(),
                "unknown".into(),
            ]);
            if unavailable {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), HashSet::from(["found".to_string()]));
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn block_hash_batch_and_legacy_fallback_preserve_missing_heights() {
        use std::io::Write;
        use std::net::TcpListener;
        for legacy in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                for _ in 0..if legacy { 3 } else { 1 } {
                    let (mut stream, _) = listener.accept().unwrap();
                    let request = read_http_request(&mut stream);
                    let (status, body) = if request.starts_with("GET /api/block-hashes?") {
                        if legacy {
                            ("404 Not Found", "{}")
                        } else {
                            ("200 OK", r#"{"chain_height":7,"hashes":{"7":"abcd"}}"#)
                        }
                    } else if request.starts_with("GET /api/block/7 ") {
                        ("200 OK", r#"{"hash":"abcd","reward":0,"timestamp":0}"#)
                    } else {
                        assert!(request.starts_with("GET /api/block/8 "));
                        ("404 Not Found", "{}")
                    };
                    write!(stream,"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
            });
            let client = NodeClient::new(&format!("http://{address}"), "").unwrap();
            assert!(client.get_block_hashes(&[]).unwrap().is_empty());
            let hashes = client.get_block_hashes(&[7, 8]).unwrap();
            assert_eq!(hashes, HashMap::from([(7, "abcd".to_string())]));
            server.join().unwrap();
        }
    }

    #[test]
    fn submit_block_falls_back_to_retained_full_block_when_template_id_unknown() {
        use std::io::Write;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind submit test server");
        let address = listener.local_addr().expect("submit server address");
        let (request_tx, request_rx) = std::sync::mpsc::channel::<String>();
        let server = std::thread::spawn(move || {
            let responses = [
                (
                    "400 Bad Request",
                    r#"{"error":"unknown or expired template_id"}"#.to_string(),
                ),
                (
                    "200 OK",
                    r#"{"accepted":true,"hash":"beef","height":11}"#.to_string(),
                ),
            ];
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().expect("accept daemon request");
                let request = read_http_request(&mut stream);
                request_tx.send(request).expect("record daemon request");
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write daemon response");
            }
        });

        let client = NodeClient::new(&format!("http://{address}"), "").expect("node client");
        let prev_hash = vec![0u8; 32];
        let template_block = serde_json::json!({
            "header": {"Height": 11, "Difficulty": 3, "Nonce": 0, "PrevHash": prev_hash},
            "txns": []
        });
        let job = Job {
            id: "job1".to_string(),
            height: 11,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 3,
            template_id: "tmpl-lost".to_string(),
            prev_hash: None,
            template_created_at: std::time::Instant::now(),
            template_block: Some(std::sync::Arc::from(template_block.to_string())),
        };

        let response = client
            .submit_block(&job, 42)
            .expect("fallback submit should succeed");
        assert!(response.accepted);
        assert_eq!(response.hash.as_deref(), Some("beef"));
        assert_eq!(response.height, Some(11));

        let compact_request = request_rx.recv().expect("compact request recorded");
        assert!(compact_request.contains("\"template_id\":\"tmpl-lost\""));
        let full_request = request_rx.recv().expect("full request recorded");
        let full_body = full_request
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or_default();
        let full_payload: serde_json::Value =
            serde_json::from_str(full_body).expect("full request body is JSON");
        assert!(full_payload.get("template_id").is_none());
        assert_eq!(full_payload["header"]["Nonce"].as_u64(), Some(42));
        assert_eq!(full_payload["header"]["Height"].as_u64(), Some(11));

        let telemetry = client.template_lease_telemetry();
        assert_eq!(telemetry.block_submit_attempts_total, 1);
        assert_eq!(telemetry.submit_expired_responses_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_attempts_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_accepted_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_stale_total, 0);
        assert_eq!(telemetry.block_submit_full_fallback_errors_total, 0);

        server.join().expect("submit test server thread");
    }

    #[test]
    fn submit_block_classifies_stale_full_block_fallback_as_tip_race() {
        use std::io::Write;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind submit test server");
        let address = listener.local_addr().expect("submit server address");
        let server = std::thread::spawn(move || {
            let responses = [
                (
                    "400 Bad Request",
                    r#"{"error":"unknown or expired template_id"}"#,
                ),
                ("400 Bad Request", r#"{"error":"block rejected as stale"}"#),
            ];
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().expect("accept daemon request");
                let _ = read_http_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write daemon response");
            }
        });

        let client = NodeClient::new(&format!("http://{address}"), "").expect("node client");
        let job = Job {
            id: "job-stale".to_string(),
            height: 11,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 3,
            template_id: "tmpl-stale".to_string(),
            prev_hash: None,
            template_created_at: std::time::Instant::now(),
            template_block: Some(std::sync::Arc::from(
                serde_json::json!({
                    "header": {"Height": 11, "Difficulty": 3, "Nonce": 0},
                    "txns": []
                })
                .to_string(),
            )),
        };

        let err = client
            .submit_block(&job, 42)
            .expect_err("stale fallback should remain a rejected candidate");
        assert!(err.to_string().contains("block rejected as stale"));

        let telemetry = client.template_lease_telemetry();
        assert_eq!(telemetry.submit_expired_responses_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_attempts_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_accepted_total, 0);
        assert_eq!(telemetry.block_submit_full_fallback_stale_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_errors_total, 0);

        server.join().expect("submit test server thread");
    }

    #[test]
    fn submit_block_counts_unexpected_full_block_fallback_error() {
        use std::io::Write;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind submit test server");
        let address = listener.local_addr().expect("submit server address");
        let server = std::thread::spawn(move || {
            let responses = [
                (
                    "400 Bad Request",
                    r#"{"error":"unknown or expired template_id"}"#,
                ),
                (
                    "500 Internal Server Error",
                    r#"{"error":"submit unavailable"}"#,
                ),
            ];
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().expect("accept daemon request");
                let _ = read_http_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write daemon response");
            }
        });

        let client = NodeClient::new(&format!("http://{address}"), "").expect("node client");
        let job = Job {
            id: "job-error".to_string(),
            height: 11,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 3,
            template_id: "tmpl-error".to_string(),
            prev_hash: None,
            template_created_at: std::time::Instant::now(),
            template_block: Some(std::sync::Arc::from(
                serde_json::json!({
                    "header": {"Height": 11, "Difficulty": 3, "Nonce": 0},
                    "txns": []
                })
                .to_string(),
            )),
        };

        let err = client
            .submit_block(&job, 42)
            .expect_err("unexpected fallback error should surface");
        assert!(err.to_string().contains("submit unavailable"));

        let telemetry = client.template_lease_telemetry();
        assert_eq!(telemetry.submit_expired_responses_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_attempts_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_accepted_total, 0);
        assert_eq!(telemetry.block_submit_full_fallback_stale_total, 0);
        assert_eq!(telemetry.block_submit_full_fallback_errors_total, 1);

        server.join().expect("submit test server thread");
    }

    #[test]
    fn submit_block_without_retained_block_surfaces_compact_error() {
        use std::io::Write;
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind submit test server");
        let address = listener.local_addr().expect("submit server address");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept daemon request");
            let _ = read_http_request(&mut stream);
            let body = r#"{"error":"unknown or expired template_id"}"#;
            let response = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write daemon response");
        });

        let client = NodeClient::new(&format!("http://{address}"), "").expect("node client");
        let job = Job {
            id: "job1".to_string(),
            height: 11,
            header_base: vec![0xAA; 92],
            network_target: [0xBB; 32],
            network_difficulty: 3,
            template_id: "tmpl-lost".to_string(),
            prev_hash: None,
            template_created_at: std::time::Instant::now(),
            template_block: None,
        };

        let err = client
            .submit_block(&job, 42)
            .expect_err("compact error should surface without retained block");
        assert!(is_unknown_or_expired_template_id_error(&err));

        let telemetry = client.template_lease_telemetry();
        assert_eq!(telemetry.submit_expired_responses_total, 1);
        assert_eq!(telemetry.block_submit_full_fallback_attempts_total, 0);

        server.join().expect("submit test server thread");
    }

    #[test]
    fn accepted_block_template_age_telemetry_crosses_original_lease_boundary() {
        let client = NodeClient::new("http://127.0.0.1:1", "").expect("node client");
        client.record_accepted_block_template_age(ORIGINAL_TEMPLATE_LEASE_SECONDS - 1);
        let before = client.template_lease_telemetry();
        assert_eq!(
            before.max_accepted_block_template_age_seconds,
            ORIGINAL_TEMPLATE_LEASE_SECONDS - 1
        );
        assert_eq!(before.accepted_blocks_after_ten_minutes_total, 0);

        client.record_accepted_block_template_age(ORIGINAL_TEMPLATE_LEASE_SECONDS);
        let after = client.template_lease_telemetry();
        assert_eq!(
            after.max_accepted_block_template_age_seconds,
            ORIGINAL_TEMPLATE_LEASE_SECONDS
        );
        assert_eq!(after.accepted_blocks_after_ten_minutes_total, 1);
    }
}
