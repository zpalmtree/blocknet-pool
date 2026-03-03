use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{BlockSubmitResponse, Job, NodeApi};

#[derive(Debug, Clone, Deserialize)]
pub struct BlockTemplate {
    pub block: Value,
    pub target: String,
    pub header_base: String,
    #[serde(default)]
    pub reward_address_used: String,
    #[serde(default)]
    pub template_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub peer_id: String,
    pub peers: i64,
    pub chain_height: u64,
    pub best_hash: String,
    pub total_work: u64,
    pub mempool_size: i64,
    pub mempool_bytes: i64,
    pub syncing: bool,
    pub identity_age: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeBlock {
    pub height: u64,
    pub hash: String,
    pub reward: u64,
    pub difficulty: u64,
    #[serde(default)]
    pub timestamp: i64,
    pub tx_count: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletSendResponse {
    pub txid: String,
    pub fee: u64,
    pub change: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletLoadResponse {
    pub loaded: bool,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalletUnlockResponse {
    pub locked: bool,
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
    pub total: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitBlockRawResponse {
    pub accepted: bool,
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub height: u64,
}

#[derive(Debug)]
pub struct HttpError {
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

pub fn is_http_status(err: &anyhow::Error, status: u16) -> bool {
    err.downcast_ref::<HttpError>()
        .is_some_and(|http_err| http_err.status_code == status)
}

pub fn http_error_body_contains(err: &anyhow::Error, status: u16, needle: &str) -> bool {
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

#[derive(Debug)]
pub struct NodeClient {
    base_url: String,
    client: Client,
    auth_token: Mutex<Option<String>>,
    auth_cookie_path: Option<PathBuf>,
    chain_height: AtomicU64,
    syncing: AtomicBool,
}

impl NodeClient {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        Self::new_with_daemon_auth(base_url, token, "", "")
    }

    pub fn new_with_daemon_auth(
        base_url: &str,
        token: &str,
        daemon_data_dir: &str,
        daemon_cookie_path: &str,
    ) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("build node http client")?;

        let (resolved_token, auth_cookie_path) =
            resolve_auth_token_and_cookie(token, daemon_data_dir, daemon_cookie_path);
        if let Some(path) = auth_cookie_path.as_ref() {
            tracing::info!(path = %path.display(), "daemon auth cookie source configured");
        } else if resolved_token.is_some() {
            tracing::warn!(
                "daemon auth cookie source not found; using static token only (auto-refresh disabled)"
            );
        }
        if resolved_token.is_some() {
            tracing::info!("daemon auth token loaded");
        }

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            auth_token: Mutex::new(resolved_token),
            auth_cookie_path,
            chain_height: AtomicU64::new(0),
            syncing: AtomicBool::new(false),
        })
    }

    pub fn get_status(&self) -> Result<NodeStatus> {
        let status: NodeStatus = self.get_json("/api/status")?;
        self.chain_height
            .store(status.chain_height, Ordering::Relaxed);
        self.syncing.store(status.syncing, Ordering::Relaxed);
        Ok(status)
    }

    pub fn get_block_template(&self, reward_address: Option<&str>) -> Result<BlockTemplate> {
        let mut path = "/api/mining/blocktemplate".to_string();
        if let Some(address) = reward_address.filter(|v| !v.trim().is_empty()) {
            path.push_str("?address=");
            path.push_str(&urlencoding::encode(address));
        }
        self.get_json(&path)
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

    pub fn wallet_send(
        &self,
        address: &str,
        amount: u64,
        idempotency_key: &str,
    ) -> Result<WalletSendResponse> {
        let mut extra = HashMap::<String, String>::new();
        if !idempotency_key.is_empty() {
            extra.insert("Idempotency-Key".to_string(), idempotency_key.to_string());
        }

        let payload = serde_json::json!({
            "address": address,
            "amount": amount,
        });
        self.post_json_with_headers("/api/wallet/send", &payload, &extra)
    }

    pub fn wallet_load(&self, password: &str) -> Result<WalletLoadResponse> {
        let payload = serde_json::json!({ "password": password });
        self.post_json("/api/wallet/load", &payload)
    }

    pub fn wallet_unlock(&self, password: &str) -> Result<WalletUnlockResponse> {
        let payload = serde_json::json!({ "password": password });
        self.post_json("/api/wallet/unlock", &payload)
    }

    pub fn get_wallet_address(&self) -> Result<WalletAddressResponse> {
        self.get_json("/api/wallet/address")
    }

    pub fn get_wallet_balance(&self) -> Result<WalletBalance> {
        self.get_json("/api/wallet/balance")
    }

    pub fn chain_height(&self) -> u64 {
        self.chain_height.load(Ordering::Relaxed)
    }

    pub fn syncing(&self) -> bool {
        self.syncing.load(Ordering::Relaxed)
    }

    fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let req = self.apply_auth(self.client.get(&url));
            let resp = req.send().with_context(|| format!("GET {path}"))?;
            let status = resp.status();
            let body = resp.text().unwrap_or_default();

            if status == reqwest::StatusCode::UNAUTHORIZED && !attempted_refresh {
                attempted_refresh = true;
                match self.refresh_token_from_cookie() {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to refresh daemon token from cookie");
                    }
                }
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
        self.post_json_with_headers(path, payload, &HashMap::new())
    }

    fn post_json_with_headers<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        payload: &T,
        headers: &HashMap<String, String>,
    ) -> Result<R> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let mut req = self.client.post(&url).json(payload);
            for (k, v) in headers {
                req = req.header(k, v);
            }
            let req = self.apply_auth(req);

            let resp = req.send().with_context(|| format!("POST {path}"))?;
            let status = resp.status();
            let body = resp.text().unwrap_or_default();

            if status == reqwest::StatusCode::UNAUTHORIZED && !attempted_refresh {
                attempted_refresh = true;
                match self.refresh_token_from_cookie() {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to refresh daemon token from cookie");
                    }
                }
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
        let mut candidates = Vec::<PathBuf>::new();
        if let Some(configured) = self.auth_cookie_path.as_ref() {
            candidates.push(configured.clone());
        }
        for candidate in cookie_candidates("", "") {
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }

        let mut last_read_error = None;
        for cookie_path in candidates {
            if !cookie_path.exists() {
                continue;
            }

            match read_token_from_cookie_file(&cookie_path) {
                Ok(token) => {
                    let mut guard = self.auth_token.lock();
                    if guard.as_deref() == Some(token.as_str()) {
                        return Ok(false);
                    }
                    *guard = Some(token);
                    if self.auth_cookie_path.is_some() {
                        tracing::info!(
                            path = %cookie_path.display(),
                            "refreshed daemon API token from cookie"
                        );
                    } else {
                        tracing::info!(
                            path = %cookie_path.display(),
                            "discovered daemon API cookie and refreshed token"
                        );
                    }
                    return Ok(true);
                }
                Err(err) => {
                    last_read_error = Some(err);
                }
            }
        }

        if let Some(err) = last_read_error {
            return Err(err.context("failed to load daemon token from discovered cookie"));
        }

        Ok(false)
    }
}

#[derive(Debug, Clone)]
struct DaemonContext {
    data_dir: PathBuf,
}

fn resolve_auth_token_and_cookie(
    explicit_token: &str,
    daemon_data_dir: &str,
    daemon_cookie_path: &str,
) -> (Option<String>, Option<PathBuf>) {
    let explicit_token = explicit_token.trim();
    let candidates = cookie_candidates(daemon_data_dir, daemon_cookie_path);
    let cookie_path = candidates.into_iter().find(|candidate| candidate.exists());

    if !explicit_token.is_empty() {
        return (Some(explicit_token.to_string()), cookie_path);
    }

    if let Some(path) = cookie_path.as_ref() {
        if let Ok(token) = read_token_from_cookie_file(path) {
            return (Some(token), cookie_path);
        }
        tracing::warn!(
            path = %path.display(),
            "daemon cookie was discovered but token could not be read"
        );
    }

    (None, cookie_path)
}

fn cookie_candidates(daemon_data_dir: &str, daemon_cookie_path: &str) -> Vec<PathBuf> {
    let mut out = Vec::<PathBuf>::new();

    let explicit_cookie = daemon_cookie_path.trim();
    if !explicit_cookie.is_empty() {
        out.push(PathBuf::from(explicit_cookie));
    }

    let daemon_data_dir = daemon_data_dir.trim();
    if !daemon_data_dir.is_empty() {
        out.push(PathBuf::from(daemon_data_dir).join("api.cookie"));
    }

    if let Some(context) = detect_daemon_context() {
        let candidate = context.data_dir.join("api.cookie");
        if !out.contains(&candidate) {
            out.push(candidate);
        }
    }

    if !out.iter().any(|p| p == Path::new("data/api.cookie")) {
        out.push(PathBuf::from("data/api.cookie"));
    }
    if !out
        .iter()
        .any(|p| p == Path::new("blocknet-data-mainnet/api.cookie"))
    {
        out.push(PathBuf::from("blocknet-data-mainnet/api.cookie"));
    }

    out
}

fn detect_daemon_context() -> Option<DaemonContext> {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::All,
        ProcessRefreshKind::new()
            .with_cmd(UpdateKind::Always)
            .with_cwd(UpdateKind::Always),
    );

    if let Some(process) = sys.processes_by_name(OsStr::new("blocknet")).next() {
        let cmd: Vec<String> = process
            .cmd()
            .iter()
            .filter_map(|s| s.to_str().map(String::from))
            .collect();

        let data_dir = daemon_data_dir_from_cmdline(&cmd)
            .unwrap_or_else(|| PathBuf::from("blocknet-data-mainnet"));

        if data_dir.is_relative() {
            if let Some(cwd) = process.cwd() {
                return Some(DaemonContext {
                    data_dir: cwd.join(data_dir),
                });
            }
            if let Some(exe) = process.exe() {
                if let Some(parent) = exe.parent() {
                    return Some(DaemonContext {
                        data_dir: parent.join(data_dir),
                    });
                }
            }
        }

        return Some(DaemonContext { data_dir });
    }

    None
}

fn daemon_data_dir_from_cmdline(cmd: &[String]) -> Option<PathBuf> {
    cmd_arg_value(cmd, &["--data", "--data-dir", "-datadir"]).map(PathBuf::from)
}

fn cmd_arg_value(cmd: &[String], flags: &[&str]) -> Option<String> {
    for (idx, arg) in cmd.iter().enumerate() {
        for flag in flags {
            if arg == flag {
                if let Some(value) = cmd.get(idx + 1) {
                    if !value.trim().is_empty() && !value.starts_with('-') {
                        return Some(value.clone());
                    }
                }
            }
            if let Some(value) = arg.strip_prefix(&format!("{flag}=")) {
                if !value.trim().is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
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

impl NodeApi for NodeClient {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        if let Some(template_id) = job.template_id.as_ref().filter(|v| !v.trim().is_empty()) {
            let payload = serde_json::json!({
                "template_id": template_id,
                "nonce": nonce,
            });
            if let Ok(resp) =
                self.post_json::<_, SubmitBlockRawResponse>("/api/mining/submitblock", &payload)
            {
                return Ok(BlockSubmitResponse {
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
                });
            }
        }

        let full_block = job
            .full_block
            .as_ref()
            .ok_or_else(|| anyhow!("missing full block payload for submit fallback"))?;

        let mut block = full_block.clone();
        if let Some(header) = block.get_mut("header").and_then(|v| v.as_object_mut()) {
            if header.contains_key("Nonce") {
                header.insert("Nonce".to_string(), serde_json::json!(nonce));
            } else {
                header.insert("nonce".to_string(), serde_json::json!(nonce));
            }
        }

        let resp: SubmitBlockRawResponse = self.post_json("/api/mining/submitblock", &block)?;
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
        })
    }
}
