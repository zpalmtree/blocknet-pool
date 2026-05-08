use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{BlockSubmitResponse, Job, NodeApi};

const NODE_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const WALLET_SEND_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlockTemplate {
    pub block: Value,
    pub target: String,
    pub header_base: String,
    pub template_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeStatus {
    pub peers: i64,
    pub chain_height: u64,
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

#[derive(Debug)]
pub struct NodeClient {
    base_url: String,
    client: Client,
    send_client: Client,
    events_client: Client,
    auth_token: Mutex<Option<String>>,
    auth_cookie_path: PathBuf,
    chain_height: AtomicU64,
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
        })
    }

    pub fn get_status(&self) -> Result<NodeStatus> {
        let status: NodeStatus = self.get_json("/api/status")?;
        self.chain_height
            .store(status.chain_height, Ordering::Relaxed);
        Ok(status)
    }

    pub(crate) fn get_block_template(&self, reward_address: Option<&str>) -> Result<BlockTemplate> {
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
        self.get_json("/api/wallet/outputs")
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

    fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempted_refresh = false;
        loop {
            let req = self.apply_auth(self.client.get(&url));
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

impl NodeApi for NodeClient {
    fn submit_block(&self, job: &Job, nonce: u64) -> Result<BlockSubmitResponse> {
        let template_id = job.template_id.trim();
        if template_id.is_empty() {
            return Err(anyhow!("missing template_id for compact block submit"));
        }
        let payload = serde_json::json!({
            "template_id": template_id,
            "nonce": nonce,
        });
        let resp: SubmitBlockRawResponse = self.post_json("/api/mining/submitblock", &payload)?;
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
    fn wallet_send_timeout_exceeds_default_request_timeout() {
        assert!(WALLET_SEND_HTTP_TIMEOUT > NODE_HTTP_TIMEOUT);
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
        };

        let err = client
            .submit_block(&job, 42)
            .expect_err("missing template_id should fail before network submit");
        assert!(err.to_string().contains("missing template_id"));
    }
}
