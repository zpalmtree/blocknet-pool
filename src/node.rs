use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use reqwest::blocking::{Client, Response};
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
    #[serde(default)]
    pub pending_unconfirmed: u64,
    #[serde(default)]
    pub pending_unconfirmed_eta: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TxStatus {
    pub confirmations: u64,
    pub in_mempool: bool,
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
    events_client: Client,
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
        let events_client = Client::builder()
            .default_headers(HeaderMap::new())
            .timeout(None::<std::time::Duration>)
            .build()
            .context("build node events http client")?;

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
            events_client,
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

    pub fn get_tx_status_optional(&self, txid: &str) -> Result<Option<TxStatus>> {
        match self.get_json(&format!("/api/tx/{txid}")) {
            Ok(status) => Ok(Some(status)),
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

    pub fn open_events_stream(&self) -> Result<Response> {
        let path = "/api/events";
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let req = self.apply_auth(self.events_client.get(&url));
            let resp = req.send().with_context(|| format!("GET {path}"))?;
            let status = resp.status();

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DaemonNetwork {
    Mainnet,
    Testnet,
}

impl DaemonNetwork {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        }
    }

    fn legacy_data_dir(self) -> PathBuf {
        match self {
            Self::Mainnet => PathBuf::from("blocknet-data-mainnet"),
            Self::Testnet => PathBuf::from("blocknet-data-testnet"),
        }
    }
}

#[derive(Debug, Clone)]
struct DaemonContext {
    data_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct BntConfigContext {
    config_dir: PathBuf,
    config: BntConfigFile,
}

impl BntConfigContext {
    fn data_dir_for(&self, network: DaemonNetwork) -> PathBuf {
        self.config
            .cores
            .get(network.as_str())
            .and_then(|core| {
                let data_dir = core.data_dir.trim();
                (!data_dir.is_empty()).then(|| PathBuf::from(data_dir))
            })
            .unwrap_or_else(|| self.config_dir.join("data").join(network.as_str()))
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BntConfigFile {
    #[serde(default)]
    cores: HashMap<String, BntCoreConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BntCoreConfig {
    #[serde(default)]
    data_dir: String,
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
    let managed_config = load_bnt_config_context();
    let detected_context = detect_daemon_context();
    cookie_candidates_with_context(
        daemon_data_dir,
        daemon_cookie_path,
        managed_config.as_ref(),
        detected_context.as_ref(),
        bnt_config_dir().as_deref(),
    )
}

fn cookie_candidates_with_context(
    daemon_data_dir: &str,
    daemon_cookie_path: &str,
    managed_config: Option<&BntConfigContext>,
    detected_context: Option<&DaemonContext>,
    fallback_bnt_dir: Option<&Path>,
) -> Vec<PathBuf> {
    let mut out = Vec::<PathBuf>::new();

    let explicit_cookie = daemon_cookie_path.trim();
    if !explicit_cookie.is_empty() {
        push_cookie_candidate(&mut out, PathBuf::from(explicit_cookie));
    }

    let daemon_data_dir = daemon_data_dir.trim();
    if !daemon_data_dir.is_empty() {
        push_cookie_candidate(&mut out, PathBuf::from(daemon_data_dir).join("api.cookie"));
    }

    if let Some(context) = detected_context {
        push_cookie_candidate(&mut out, context.data_dir.join("api.cookie"));
    }

    let managed_dir = managed_config.map(|config| config.config_dir.as_path()).or(fallback_bnt_dir);
    if let Some(config_dir) = managed_dir {
        for network in [DaemonNetwork::Mainnet, DaemonNetwork::Testnet] {
            let pidfile = config_dir.join(format!("core.{}.pid", network.as_str()));
            if pidfile.exists() {
                let managed_cookie = managed_config
                    .map(|config| config.data_dir_for(network).join("api.cookie"))
                    .unwrap_or_else(|| config_dir.join("data").join(network.as_str()).join("api.cookie"));
                push_cookie_candidate(&mut out, managed_cookie);
            }
        }
    }

    if let Some(config) = managed_config {
        for network in [DaemonNetwork::Mainnet, DaemonNetwork::Testnet] {
            let managed_cookie = config.data_dir_for(network).join("api.cookie");
            push_cookie_candidate(&mut out, managed_cookie);
        }
    }

    if let Some(config_dir) = fallback_bnt_dir {
        for network in [DaemonNetwork::Mainnet, DaemonNetwork::Testnet] {
            push_cookie_candidate(
                &mut out,
                config_dir.join("data").join(network.as_str()).join("api.cookie"),
            );
        }
    }

    push_cookie_candidate(&mut out, PathBuf::from("data/api.cookie"));
    push_cookie_candidate(
        &mut out,
        DaemonNetwork::Mainnet.legacy_data_dir().join("api.cookie"),
    );
    push_cookie_candidate(
        &mut out,
        DaemonNetwork::Testnet.legacy_data_dir().join("api.cookie"),
    );

    out
}

fn push_cookie_candidate(out: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !out.contains(&candidate) {
        out.push(candidate);
    }
}

fn bnt_config_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("BNT_CONFIG_DIR").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(dir));
    }

    let home = std::env::var_os("HOME")?;
    Some(PathBuf::from(home).join(".config").join("bnt"))
}

fn load_bnt_config_context() -> Option<BntConfigContext> {
    let config_dir = bnt_config_dir()?;
    let config_path = config_dir.join("config.json");
    let data = fs::read(&config_path).ok()?;
    let stripped = strip_json_comments(&data);
    let config = serde_json::from_slice::<BntConfigFile>(&stripped)
        .inspect_err(|err| {
            tracing::warn!(
                path = %config_path.display(),
                error = %err,
                "failed to parse Blocknet wrapper config; falling back to defaults"
            );
        })
        .ok()?;
    Some(BntConfigContext { config_dir, config })
}

fn strip_json_comments(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len());
    let mut idx = 0;
    while idx < src.len() {
        let ch = src[idx];

        if ch == b'"' {
            out.push(ch);
            idx += 1;
            while idx < src.len() {
                let next = src[idx];
                out.push(next);
                idx += 1;
                if next == b'\\' && idx < src.len() {
                    out.push(src[idx]);
                    idx += 1;
                } else if next == b'"' {
                    break;
                }
            }
            continue;
        }

        if ch == b'/' && idx + 1 < src.len() && src[idx + 1] == b'/' {
            while idx < src.len() && src[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }

        if ch == b'#' {
            while idx < src.len() && src[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }

        out.push(ch);
        idx += 1;
    }
    out
}

fn detect_daemon_context() -> Option<DaemonContext> {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

    let managed_config = load_bnt_config_context();
    let managed_dir = managed_config.as_ref().map(|config| config.config_dir.as_path());
    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::All,
        ProcessRefreshKind::new()
            .with_cmd(UpdateKind::Always)
            .with_cwd(UpdateKind::Always),
    );

    for process in sys.processes().values() {
        let name = process.name().to_str();
        let cmd: Vec<String> = process
            .cmd()
            .iter()
            .filter_map(|s| s.to_str().map(String::from))
            .collect();
        if !looks_like_blocknet_daemon_process(name, &cmd) {
            continue;
        }

        let pid = process.pid().as_u32();
        let network = managed_dir
            .and_then(|config_dir| daemon_network_from_pidfiles(config_dir, pid))
            .or_else(|| daemon_network_from_cmdline(&cmd))
            .unwrap_or(DaemonNetwork::Mainnet);

        let data_dir = daemon_data_dir_from_cmdline(&cmd)
            .or_else(|| managed_config.as_ref().map(|config| config.data_dir_for(network)))
            .unwrap_or_else(|| network.legacy_data_dir());
        let data_dir = if data_dir.is_relative() {
            process
                .cwd()
                .map(|cwd| cwd.join(&data_dir))
                .unwrap_or(data_dir)
        } else {
            data_dir
        };

        return Some(DaemonContext { data_dir });
    }

    None
}

fn looks_like_blocknet_daemon_process(name: Option<&str>, cmd: &[String]) -> bool {
    name.is_some_and(binary_name_matches_blocknet)
        || cmd
            .first()
            .and_then(|arg0| Path::new(arg0).file_name())
            .and_then(|basename| basename.to_str())
            .is_some_and(binary_name_matches_blocknet)
}

fn binary_name_matches_blocknet(name: &str) -> bool {
    name == "blocknet" || name.starts_with("blocknet-core-")
}

fn daemon_network_from_pidfiles(config_dir: &Path, pid: u32) -> Option<DaemonNetwork> {
    for network in [DaemonNetwork::Mainnet, DaemonNetwork::Testnet] {
        let pidfile = config_dir.join(format!("core.{}.pid", network.as_str()));
        let Ok(raw) = fs::read_to_string(&pidfile) else {
            continue;
        };
        let Ok(found_pid) = raw.trim().parse::<u32>() else {
            continue;
        };
        if found_pid == pid {
            return Some(network);
        }
    }
    None
}

fn daemon_network_from_cmdline(cmd: &[String]) -> Option<DaemonNetwork> {
    cmd_bool_flag_is_truthy(cmd, &["--testnet", "-testnet"]).then_some(DaemonNetwork::Testnet)
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

fn cmd_bool_flag_is_truthy(cmd: &[String], flags: &[&str]) -> bool {
    for arg in cmd {
        for flag in flags {
            if arg == flag {
                return true;
            }
            if let Some(value) = arg.strip_prefix(&format!("{flag}=")) {
                let normalized = value.trim().to_ascii_lowercase();
                if matches!(normalized.as_str(), "1" | "true" | "t" | "yes" | "y" | "on") {
                    return true;
                }
            }
        }
    }
    false
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
            match self.post_json::<_, SubmitBlockRawResponse>("/api/mining/submitblock", &payload) {
                Ok(resp) => {
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
                Err(err) => {
                    tracing::warn!(
                        template_id = %template_id,
                        nonce,
                        error = %err,
                        "compact submit failed, falling back to full block payload"
                    );
                }
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
    fn cookie_candidates_prefer_explicit_over_managed_paths() {
        let config_dir = tempfile::tempdir().expect("tempdir");
        let managed = BntConfigContext {
            config_dir: config_dir.path().to_path_buf(),
            config: BntConfigFile {
                cores: HashMap::from([(
                    "mainnet".to_string(),
                    BntCoreConfig {
                        data_dir: "/managed/mainnet".to_string(),
                    },
                )]),
            },
        };

        let candidates = cookie_candidates_with_context(
            "/var/lib/blocknet/data",
            "/explicit/api.cookie",
            Some(&managed),
            Some(&DaemonContext {
                data_dir: PathBuf::from("/detected/mainnet"),
            }),
            Some(config_dir.path()),
        );

        assert_eq!(candidates[0], PathBuf::from("/explicit/api.cookie"));
        assert_eq!(candidates[1], PathBuf::from("/var/lib/blocknet/data/api.cookie"));
        assert_eq!(candidates[2], PathBuf::from("/detected/mainnet/api.cookie"));
        assert!(candidates.contains(&PathBuf::from("/managed/mainnet/api.cookie")));
    }

    #[test]
    fn cookie_candidates_include_wrapper_pidfile_network_first() {
        let config_dir = tempfile::tempdir().expect("tempdir");
        fs::write(config_dir.path().join("core.testnet.pid"), "1234\n").expect("pidfile");
        let managed = BntConfigContext {
            config_dir: config_dir.path().to_path_buf(),
            config: BntConfigFile {
                cores: HashMap::from([(
                    "testnet".to_string(),
                    BntCoreConfig {
                        data_dir: "/managed/testnet".to_string(),
                    },
                )]),
            },
        };

        let candidates =
            cookie_candidates_with_context("", "", Some(&managed), None, Some(config_dir.path()));

        assert_eq!(candidates[0], PathBuf::from("/managed/testnet/api.cookie"));
        assert!(candidates.contains(&PathBuf::from("/managed/testnet/api.cookie")));
        assert!(candidates.contains(&config_dir.path().join("data").join("mainnet").join("api.cookie")));
    }

    #[test]
    fn binary_name_match_includes_wrapper_managed_core_names() {
        assert!(binary_name_matches_blocknet("blocknet"));
        assert!(binary_name_matches_blocknet("blocknet-core-amd64-linux"));
        assert!(!binary_name_matches_blocknet("blocknet-wrapper"));
    }
}
