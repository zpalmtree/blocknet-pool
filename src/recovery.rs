use std::collections::VecDeque;
use std::env;
use std::fs;
use std::future::Future;
use std::io::ErrorKind;
use std::os::fd::FromRawFd;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::node::{NodeStatus, WalletAddressResponse, WalletBalance};

const MAX_OPERATIONS: usize = 32;
const SYSTEMD_SOCKET_FD: i32 = 3;
const STATUS_WAIT_RETRY: Duration = Duration::from_secs(2);
const STATUS_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const STATUS_CACHE_TTL: Duration = Duration::from_secs(5);
const STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RecoveryConfig {
    pub enabled: bool,
    pub socket_path: String,
    pub state_path: String,
    pub secret_path: String,
    pub proxy_include_path: String,
    pub active_cookie_path: String,
    pub primary: RecoveryDaemonInstanceConfig,
    pub standby: RecoveryDaemonInstanceConfig,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            socket_path: "/run/blocknet-recoveryd.sock".to_string(),
            state_path: "/var/lib/blocknet-recovery/state.json".to_string(),
            secret_path: "/etc/blocknet/recovery/pool-wallet.json".to_string(),
            proxy_include_path: "/etc/nginx/blocknet-daemon-active-upstream.inc".to_string(),
            active_cookie_path: "/etc/blocknet/pool/daemon-active.api.cookie".to_string(),
            primary: RecoveryDaemonInstanceConfig {
                service: "blocknetd@primary.service".to_string(),
                api: "http://127.0.0.1:18331".to_string(),
                wallet_path: "/var/lib/blocknet/wallet.dat".to_string(),
                data_dir: "/var/lib/blocknet/data".to_string(),
                cookie_path: "/var/lib/blocknet/data/api.cookie".to_string(),
            },
            standby: RecoveryDaemonInstanceConfig {
                service: "blocknetd@standby.service".to_string(),
                api: "http://127.0.0.1:18332".to_string(),
                wallet_path: "/var/lib/blocknet-standby/wallet.dat".to_string(),
                data_dir: "/var/lib/blocknet-standby/data".to_string(),
                cookie_path: "/var/lib/blocknet-standby/data/api.cookie".to_string(),
            },
        }
    }
}

impl RecoveryConfig {
    pub fn normalize(&mut self) {
        if self.socket_path.trim().is_empty() {
            self.socket_path = Self::default().socket_path;
        }
        if self.state_path.trim().is_empty() {
            self.state_path = Self::default().state_path;
        }
        if self.secret_path.trim().is_empty() {
            self.secret_path = Self::default().secret_path;
        }
        if self.proxy_include_path.trim().is_empty() {
            self.proxy_include_path = Self::default().proxy_include_path;
        }
        if self.active_cookie_path.trim().is_empty() {
            self.active_cookie_path = Self::default().active_cookie_path;
        }
        self.primary.normalize(RecoveryInstanceId::Primary);
        self.standby.normalize(RecoveryInstanceId::Standby);
    }

    pub fn instance(&self, id: RecoveryInstanceId) -> &RecoveryDaemonInstanceConfig {
        match id {
            RecoveryInstanceId::Primary => &self.primary,
            RecoveryInstanceId::Standby => &self.standby,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RecoveryDaemonInstanceConfig {
    pub service: String,
    pub api: String,
    pub wallet_path: String,
    pub data_dir: String,
    pub cookie_path: String,
}

impl Default for RecoveryDaemonInstanceConfig {
    fn default() -> Self {
        Self {
            service: String::new(),
            api: String::new(),
            wallet_path: String::new(),
            data_dir: String::new(),
            cookie_path: String::new(),
        }
    }
}

impl RecoveryDaemonInstanceConfig {
    fn normalize(&mut self, id: RecoveryInstanceId) {
        let defaults = RecoveryConfig::default();
        let default = defaults.instance(id);
        if self.service.trim().is_empty() {
            self.service = default.service.clone();
        }
        if self.api.trim().is_empty() {
            self.api = default.api.clone();
        }
        if self.wallet_path.trim().is_empty() {
            self.wallet_path = default.wallet_path.clone();
        }
        if self.data_dir.trim().is_empty() {
            self.data_dir = default.data_dir.clone();
        }
        if self.cookie_path.trim().is_empty() {
            self.cookie_path = default.cookie_path.clone();
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryInstanceId {
    Primary,
    Standby,
}

impl RecoveryInstanceId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Standby => "standby",
        }
    }

    pub fn other(self) -> Self {
        match self {
            Self::Primary => Self::Standby,
            Self::Standby => Self::Primary,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryInstanceState {
    Stopped,
    Starting,
    Syncing,
    Ready,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOperationState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOperationKind {
    PausePayouts,
    ResumePayouts,
    StartStandbySync,
    RebuildStandbyWallet,
    Cutover,
    PurgeInactiveDaemon,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryWalletStatus {
    pub loaded: bool,
    pub address: Option<String>,
    pub spendable: Option<u64>,
    pub pending: Option<u64>,
    pub total: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryInstanceStatus {
    pub instance: RecoveryInstanceId,
    pub service: String,
    pub api: String,
    pub wallet_path: String,
    pub data_dir: String,
    pub cookie_path: String,
    pub service_state: String,
    pub state: RecoveryInstanceState,
    pub reachable: bool,
    pub cookie_present: bool,
    pub chain_height: Option<u64>,
    pub peers: Option<i64>,
    pub syncing: Option<bool>,
    pub best_hash: Option<String>,
    pub wallet: RecoveryWalletStatus,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryOperation {
    pub id: u64,
    pub kind: RecoveryOperationKind,
    pub target: Option<RecoveryInstanceId>,
    pub state: RecoveryOperationState,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryStatus {
    pub enabled: bool,
    pub payouts_paused: bool,
    pub payout_pause_file: String,
    pub secret_configured: bool,
    pub proxy_target: Option<RecoveryInstanceId>,
    pub active_cookie_target: Option<RecoveryInstanceId>,
    pub active_instance: Option<RecoveryInstanceId>,
    pub warning: Option<String>,
    pub instances: Vec<RecoveryInstanceStatus>,
    pub operations: Vec<RecoveryOperation>,
}

impl RecoveryStatus {
    pub fn disabled(cfg: &Config) -> Self {
        Self {
            enabled: false,
            payouts_paused: Path::new(cfg.payout_pause_file.trim()).exists(),
            payout_pause_file: cfg.payout_pause_file.clone(),
            secret_configured: false,
            proxy_target: None,
            active_cookie_target: None,
            active_instance: None,
            warning: Some("recovery controls are disabled in config".to_string()),
            instances: Vec::new(),
            operations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecoverySecret {
    mnemonic: String,
    password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct PersistedRecoveryState {
    next_operation_id: u64,
    operations: VecDeque<RecoveryOperation>,
}

impl Default for PersistedRecoveryState {
    fn default() -> Self {
        Self {
            next_operation_id: 1,
            operations: VecDeque::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum RecoveryAgentRequest {
    Status,
    PayoutsPause,
    PayoutsResume,
    StandbyStartSync,
    StandbyRebuildWallet,
    Cutover { target: RecoveryInstanceId },
    InactivePurgeResync,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum RecoveryAgentResult {
    Status { status: RecoveryStatus },
    Operation { operation: RecoveryOperation },
}

#[derive(Debug, Serialize, Deserialize)]
struct RecoveryAgentEnvelope {
    ok: bool,
    #[serde(default)]
    result: Option<RecoveryAgentResult>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RecoveryAgentClient {
    socket_path: PathBuf,
}

impl RecoveryAgentClient {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: path.into(),
        }
    }

    pub async fn status(&self) -> Result<RecoveryStatus> {
        match self.send(RecoveryAgentRequest::Status).await? {
            RecoveryAgentResult::Status { status } => Ok(status),
            _ => bail!("recovery agent returned unexpected response"),
        }
    }

    pub async fn pause_payouts(&self) -> Result<RecoveryOperation> {
        self.expect_operation(RecoveryAgentRequest::PayoutsPause)
            .await
    }

    pub async fn resume_payouts(&self) -> Result<RecoveryOperation> {
        self.expect_operation(RecoveryAgentRequest::PayoutsResume)
            .await
    }

    pub async fn start_inactive_sync(&self) -> Result<RecoveryOperation> {
        self.expect_operation(RecoveryAgentRequest::StandbyStartSync)
            .await
    }

    pub async fn rebuild_inactive_wallet(&self) -> Result<RecoveryOperation> {
        self.expect_operation(RecoveryAgentRequest::StandbyRebuildWallet)
            .await
    }

    pub async fn cutover(&self, target: RecoveryInstanceId) -> Result<RecoveryOperation> {
        self.expect_operation(RecoveryAgentRequest::Cutover { target })
            .await
    }

    pub async fn purge_inactive_daemon(&self) -> Result<RecoveryOperation> {
        self.expect_operation(RecoveryAgentRequest::InactivePurgeResync)
            .await
    }

    async fn expect_operation(&self, request: RecoveryAgentRequest) -> Result<RecoveryOperation> {
        match self.send(request).await? {
            RecoveryAgentResult::Operation { operation } => Ok(operation),
            _ => bail!("recovery agent returned unexpected response"),
        }
    }

    async fn send(&self, request: RecoveryAgentRequest) -> Result<RecoveryAgentResult> {
        if self.socket_path.as_os_str().is_empty() {
            bail!("recovery agent socket path is not configured");
        }
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| format!("connect {}", self.socket_path.display()))?;
        let payload = serde_json::to_vec(&request).context("serialize recovery agent request")?;
        stream
            .write_all(&payload)
            .await
            .context("write recovery request")?;
        stream
            .write_all(b"\n")
            .await
            .context("write recovery request newline")?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .context("read recovery response")?;
        if read == 0 {
            bail!("recovery agent closed connection without a response");
        }
        let envelope: RecoveryAgentEnvelope =
            serde_json::from_str(line.trim_end()).context("decode recovery response")?;
        if !envelope.ok {
            bail!(
                "{}",
                envelope
                    .error
                    .unwrap_or_else(|| "recovery agent request failed".to_string())
            );
        }
        envelope
            .result
            .ok_or_else(|| anyhow!("recovery agent returned an empty response"))
    }
}

pub struct RecoveryAgent {
    cfg: Config,
    http: reqwest::Client,
    state: Mutex<PersistedRecoveryState>,
    status_cache: Mutex<Option<CachedRecoveryStatus>>,
}

#[derive(Debug, Clone)]
struct CachedRecoveryStatus {
    status: RecoveryStatus,
    captured_at: Instant,
}

impl RecoveryAgent {
    pub async fn new(cfg: Config) -> Result<Arc<Self>> {
        let mut persisted = load_persisted_state(Path::new(cfg.recovery.state_path.trim()))?;
        let now = SystemTime::now();
        for operation in persisted.operations.iter_mut() {
            if matches!(
                operation.state,
                RecoveryOperationState::Queued | RecoveryOperationState::Running
            ) {
                operation.state = RecoveryOperationState::Failed;
                operation.finished_at = Some(now);
                operation.message =
                    Some("recovery agent restarted before operation completed".to_string());
            }
        }

        Ok(Arc::new(Self {
            cfg,
            http: reqwest::Client::builder()
                .build()
                .context("build recovery daemon http client")?,
            state: Mutex::new(persisted),
            status_cache: Mutex::new(None),
        }))
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = bind_recovery_listener(self.cfg.recovery.socket_path.trim())?;
        tracing::info!(
            socket = self.cfg.recovery.socket_path.trim(),
            "recovery agent listening"
        );

        let refresh_agent = Arc::clone(&self);
        tokio::spawn(async move {
            refresh_agent.status_refresh_loop().await;
        });

        loop {
            let (stream, _) = listener
                .accept()
                .await
                .context("accept recovery connection")?;
            let agent = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(err) = agent.handle_connection(stream).await {
                    tracing::warn!(error = %err, "recovery agent request failed");
                }
            });
        }
    }

    async fn handle_connection(self: Arc<Self>, stream: UnixStream) -> Result<()> {
        let (reader_half, mut writer_half) = stream.into_split();
        let mut reader = BufReader::new(reader_half);
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .context("read recovery request")?;
        if read == 0 {
            return Ok(());
        }

        let request: RecoveryAgentRequest =
            serde_json::from_str(line.trim_end()).context("decode recovery request")?;
        let response = match self.handle_request(request).await {
            Ok(result) => RecoveryAgentEnvelope {
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(err) => RecoveryAgentEnvelope {
                ok: false,
                result: None,
                error: Some(err.to_string()),
            },
        };
        let payload = serde_json::to_vec(&response).context("encode recovery response")?;
        writer_half
            .write_all(&payload)
            .await
            .context("write recovery response")?;
        writer_half
            .write_all(b"\n")
            .await
            .context("write recovery response newline")?;
        Ok(())
    }

    async fn handle_request(
        self: &Arc<Self>,
        request: RecoveryAgentRequest,
    ) -> Result<RecoveryAgentResult> {
        match request {
            RecoveryAgentRequest::Status => Ok(RecoveryAgentResult::Status {
                status: self.current_status().await?,
            }),
            RecoveryAgentRequest::PayoutsPause => {
                let agent = Arc::clone(self);
                Ok(RecoveryAgentResult::Operation {
                    operation: self
                        .spawn_operation(RecoveryOperationKind::PausePayouts, None, async move {
                            agent.pause_payouts_now().await?;
                            Ok("payouts paused".to_string())
                        })
                        .await?,
                })
            }
            RecoveryAgentRequest::PayoutsResume => {
                let agent = Arc::clone(self);
                Ok(RecoveryAgentResult::Operation {
                    operation: self
                        .spawn_operation(RecoveryOperationKind::ResumePayouts, None, async move {
                            agent.resume_payouts_now().await?;
                            Ok("payouts resumed".to_string())
                        })
                        .await?,
                })
            }
            RecoveryAgentRequest::StandbyStartSync => {
                let agent = Arc::clone(self);
                Ok(RecoveryAgentResult::Operation {
                    operation: self
                        .spawn_operation(
                            RecoveryOperationKind::StartStandbySync,
                            Some(self.inactive_instance()?),
                            async move {
                                let inactive = agent.inactive_instance().context(
                                    "cannot start the inactive daemon before daemon cutover plumbing is provisioned",
                                )?;
                                agent.start_instance(inactive).await?;
                                agent.wait_for_daemon_api(inactive).await?;
                                Ok(format!("started inactive {} daemon", inactive.as_str()))
                            },
                        )
                        .await?,
                })
            }
            RecoveryAgentRequest::StandbyRebuildWallet => {
                let agent = Arc::clone(self);
                Ok(RecoveryAgentResult::Operation {
                    operation: self
                        .spawn_operation(
                            RecoveryOperationKind::RebuildStandbyWallet,
                            Some(self.inactive_instance()?),
                            async move { agent.rebuild_inactive_wallet().await },
                        )
                        .await?,
                })
            }
            RecoveryAgentRequest::Cutover { target } => {
                let agent = Arc::clone(self);
                Ok(RecoveryAgentResult::Operation {
                    operation: self
                        .spawn_operation(RecoveryOperationKind::Cutover, Some(target), async move {
                            agent.cutover_to(target).await
                        })
                        .await?,
                })
            }
            RecoveryAgentRequest::InactivePurgeResync => {
                let agent = Arc::clone(self);
                Ok(RecoveryAgentResult::Operation {
                    operation: self
                        .spawn_operation(
                            RecoveryOperationKind::PurgeInactiveDaemon,
                            None,
                            async move { agent.purge_inactive_daemon().await },
                        )
                        .await?,
                })
            }
        }
    }

    async fn spawn_operation<Fut>(
        self: &Arc<Self>,
        kind: RecoveryOperationKind,
        target: Option<RecoveryInstanceId>,
        work: Fut,
    ) -> Result<RecoveryOperation>
    where
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        let mut state = self.state.lock().await;
        if state.operations.iter().any(|op| {
            matches!(
                op.state,
                RecoveryOperationState::Queued | RecoveryOperationState::Running
            )
        }) {
            bail!("another recovery operation is already running");
        }

        let operation = RecoveryOperation {
            id: state.next_operation_id.max(1),
            kind,
            target,
            state: RecoveryOperationState::Running,
            created_at: SystemTime::now(),
            started_at: Some(SystemTime::now()),
            finished_at: None,
            message: None,
        };
        state.next_operation_id = operation.id.saturating_add(1);
        state.operations.push_front(operation.clone());
        while state.operations.len() > MAX_OPERATIONS {
            state.operations.pop_back();
        }
        persist_state(Path::new(self.cfg.recovery.state_path.trim()), &state)?;
        drop(state);
        self.invalidate_status_cache().await;

        let agent = Arc::clone(self);
        tokio::spawn(async move {
            let result = work.await;
            agent.finish_operation(operation.id, result).await;
        });

        Ok(operation)
    }

    async fn finish_operation(&self, operation_id: u64, result: Result<String>) {
        let mut state = self.state.lock().await;
        if let Some(operation) = state.operations.iter_mut().find(|op| op.id == operation_id) {
            operation.finished_at = Some(SystemTime::now());
            match result {
                Ok(message) => {
                    operation.state = RecoveryOperationState::Succeeded;
                    operation.message = Some(message);
                }
                Err(err) => {
                    operation.state = RecoveryOperationState::Failed;
                    operation.message = Some(err.to_string());
                }
            }
        }
        if let Err(err) = persist_state(Path::new(self.cfg.recovery.state_path.trim()), &state) {
            tracing::warn!(error = %err, "failed persisting recovery state");
        }
        drop(state);
        if let Err(err) = self.refresh_status_cache().await {
            tracing::warn!(error = %err, "failed refreshing cached recovery status");
        }
    }

    async fn current_status(&self) -> Result<RecoveryStatus> {
        if let Some(status) = self.cached_status().await {
            return Ok(status);
        }
        self.refresh_status_cache().await
    }

    async fn cached_status(&self) -> Option<RecoveryStatus> {
        let cache = self.status_cache.lock().await;
        let cached = cache.as_ref()?;
        if cached.captured_at.elapsed() <= STATUS_CACHE_TTL {
            Some(cached.status.clone())
        } else {
            None
        }
    }

    async fn invalidate_status_cache(&self) {
        let mut cache = self.status_cache.lock().await;
        *cache = None;
    }

    async fn refresh_status_cache(&self) -> Result<RecoveryStatus> {
        let status = self.build_status_snapshot().await?;
        let mut cache = self.status_cache.lock().await;
        *cache = Some(CachedRecoveryStatus {
            status: status.clone(),
            captured_at: Instant::now(),
        });
        Ok(status)
    }

    async fn status_refresh_loop(self: Arc<Self>) {
        if let Err(err) = self.refresh_status_cache().await {
            tracing::warn!(error = %err, "failed warming recovery status cache");
        }
        let mut interval = tokio::time::interval(STATUS_REFRESH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(err) = self.refresh_status_cache().await {
                tracing::warn!(error = %err, "failed refreshing recovery status cache");
            }
        }
    }

    async fn build_status_snapshot(&self) -> Result<RecoveryStatus> {
        let proxy_target = self.detect_proxy_target();
        let active_cookie_target = self.detect_active_cookie_target();
        let active_instance = match (proxy_target, active_cookie_target) {
            (Some(a), Some(b)) if a == b => Some(a),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            _ => None,
        };

        let warning = match (proxy_target, active_cookie_target) {
            (Some(a), Some(b)) if a != b => Some(format!(
                "proxy points to {} while active cookie points to {}",
                a.as_str(),
                b.as_str()
            )),
            (None, None) => Some("active daemon routing is not provisioned yet".to_string()),
            _ => None,
        };

        let (primary, standby) = tokio::join!(
            self.inspect_instance(RecoveryInstanceId::Primary),
            self.inspect_instance(RecoveryInstanceId::Standby)
        );
        let instances = vec![primary, standby];

        let state = self.state.lock().await;
        let operations = state.operations.iter().cloned().collect();

        Ok(RecoveryStatus {
            enabled: self.cfg.recovery.enabled,
            payouts_paused: self.payouts_paused(),
            payout_pause_file: self.cfg.payout_pause_file.clone(),
            secret_configured: self.secret_is_configured(),
            proxy_target,
            active_cookie_target,
            active_instance,
            warning,
            instances,
            operations,
        })
    }

    async fn inspect_instance(&self, instance: RecoveryInstanceId) -> RecoveryInstanceStatus {
        let cfg = self.cfg.recovery.instance(instance);
        let service_state = match systemctl_state(cfg.service.trim()).await {
            Ok(state) => state,
            Err(err) => format!("unknown ({err})"),
        };
        let cookie_present = Path::new(cfg.cookie_path.trim()).exists();

        let mut reachable = false;
        let mut chain_height = None;
        let mut peers = None;
        let mut syncing = None;
        let mut best_hash = None;
        let mut wallet = RecoveryWalletStatus {
            loaded: false,
            address: None,
            spendable: None,
            pending: None,
            total: None,
        };
        let mut error = None;

        if matches!(
            service_state.as_str(),
            "active" | "activating" | "reloading"
        ) {
            match self.daemon_get_status(cfg).await {
                Ok(status) => {
                    reachable = true;
                    chain_height = Some(status.chain_height);
                    peers = Some(status.peers);
                    syncing = Some(status.syncing);
                    best_hash = Some(status.best_hash);
                    match self.daemon_get_wallet_address(cfg).await {
                        Ok(address) => {
                            wallet.loaded = true;
                            wallet.address = Some(address.address);
                            match self.daemon_get_wallet_balance(cfg).await {
                                Ok(balance) => {
                                    wallet.spendable = Some(balance.spendable);
                                    wallet.pending = Some(balance.pending);
                                    wallet.total = Some(balance.total);
                                }
                                Err(err) => {
                                    error = Some(format!(
                                        "wallet loaded but balance probe failed: {err}"
                                    ));
                                }
                            }
                        }
                        Err(err) => {
                            if !is_service_unavailable_wallet_error(&err) {
                                error = Some(err.to_string());
                            }
                        }
                    }
                }
                Err(err) => {
                    error = Some(err.to_string());
                }
            }
        }

        let state = match service_state.as_str() {
            "inactive" | "deactivating" => RecoveryInstanceState::Stopped,
            "activating" | "reloading" => RecoveryInstanceState::Starting,
            "failed" => RecoveryInstanceState::Failed,
            "active" => {
                if reachable {
                    if syncing.unwrap_or(false) {
                        RecoveryInstanceState::Syncing
                    } else if wallet.loaded {
                        RecoveryInstanceState::Ready
                    } else {
                        RecoveryInstanceState::Degraded
                    }
                } else {
                    RecoveryInstanceState::Degraded
                }
            }
            _ => RecoveryInstanceState::Degraded,
        };

        RecoveryInstanceStatus {
            instance,
            service: cfg.service.clone(),
            api: cfg.api.clone(),
            wallet_path: cfg.wallet_path.clone(),
            data_dir: cfg.data_dir.clone(),
            cookie_path: cfg.cookie_path.clone(),
            service_state,
            state,
            reachable,
            cookie_present,
            chain_height,
            peers,
            syncing,
            best_hash,
            wallet,
            error,
        }
    }

    async fn pause_payouts_now(&self) -> Result<()> {
        let path = Path::new(self.cfg.payout_pause_file.trim());
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
        }
        if !path.exists() {
            fs::write(path, b"paused\n").with_context(|| format!("write {}", path.display()))?;
        }
        Ok(())
    }

    async fn resume_payouts_now(&self) -> Result<()> {
        let path = Path::new(self.cfg.payout_pause_file.trim());
        if path.exists() {
            fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
        }
        Ok(())
    }

    async fn rebuild_inactive_wallet(&self) -> Result<String> {
        let inactive = self.inactive_instance().context(
            "cannot rebuild the inactive wallet before daemon cutover plumbing is provisioned",
        )?;
        let inactive_cfg = self.cfg.recovery.instance(inactive);
        let daemon_status = self.daemon_get_status(inactive_cfg).await?;
        if daemon_status.syncing {
            bail!(
                "{} daemon is still syncing; wait for it to finish before rebuilding the wallet",
                inactive.as_str()
            );
        }

        let secret = self.load_secret()?;
        validate_managed_path(inactive_cfg.wallet_path.trim())?;
        self.stop_instance(inactive).await?;

        let wallet_path = Path::new(inactive_cfg.wallet_path.trim());
        if wallet_path.exists() {
            fs::remove_file(wallet_path)
                .with_context(|| format!("remove {}", wallet_path.display()))?;
        }

        self.start_instance(inactive).await?;
        self.wait_for_daemon_api(inactive).await?;
        let imported = self
            .daemon_import_wallet(inactive_cfg, &secret.mnemonic, &secret.password)
            .await?;
        Ok(format!(
            "reimported inactive {} wallet for {}",
            inactive.as_str(),
            imported
        ))
    }

    async fn cutover_to(&self, target: RecoveryInstanceId) -> Result<String> {
        self.require_payouts_paused()?;
        let current = self
            .effective_active_instance()
            .context("cannot cut over before daemon routing is provisioned")?;
        if current == target {
            return Ok(format!("{} is already active", target.as_str()));
        }

        let target_cfg = self.cfg.recovery.instance(target);
        let status = self.inspect_instance(target).await;
        if !status.reachable {
            bail!("target daemon is not reachable");
        }
        if status.syncing.unwrap_or(false) {
            bail!("target daemon is still syncing");
        }
        if !status.wallet.loaded {
            bail!("target wallet is not loaded");
        }
        if !Path::new(target_cfg.cookie_path.trim()).exists() {
            bail!("target daemon cookie does not exist yet");
        }

        write_proxy_include(
            Path::new(self.cfg.recovery.proxy_include_path.trim()),
            target_cfg.api.trim(),
        )?;
        validate_nginx_config().await?;
        swap_symlink(
            Path::new(self.cfg.recovery.active_cookie_path.trim()),
            Path::new(target_cfg.cookie_path.trim()),
        )?;
        reload_nginx().await?;
        Ok(format!("cut over active daemon to {}", target.as_str()))
    }

    async fn purge_inactive_daemon(&self) -> Result<String> {
        self.require_payouts_paused()?;
        let active = self
            .effective_active_instance()
            .context("cannot purge the inactive daemon before daemon routing is provisioned")?;
        let inactive = active.other();
        let inactive_cfg = self.cfg.recovery.instance(inactive);
        validate_managed_path(inactive_cfg.data_dir.trim())?;

        self.stop_instance(inactive).await?;
        clear_directory(Path::new(inactive_cfg.data_dir.trim()))?;
        self.start_instance(inactive).await?;
        Ok(format!("purged and restarted {} daemon", inactive.as_str()))
    }

    async fn start_instance(&self, instance: RecoveryInstanceId) -> Result<()> {
        let service = self.cfg.recovery.instance(instance).service.trim();
        run_command("systemctl", &["start", service]).await?;
        Ok(())
    }

    async fn stop_instance(&self, instance: RecoveryInstanceId) -> Result<()> {
        let service = self.cfg.recovery.instance(instance).service.trim();
        run_command("systemctl", &["stop", service]).await?;
        Ok(())
    }

    async fn wait_for_daemon_api(&self, instance: RecoveryInstanceId) -> Result<()> {
        let cfg = self.cfg.recovery.instance(instance);
        let started = tokio::time::Instant::now();
        loop {
            match self.daemon_get_status(cfg).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if started.elapsed() >= STATUS_WAIT_TIMEOUT {
                        return Err(err.context("timed out waiting for daemon api"));
                    }
                    tracing::info!(
                        instance = instance.as_str(),
                        error = %err,
                        "waiting for daemon api"
                    );
                }
            }
            tokio::time::sleep(STATUS_WAIT_RETRY).await;
        }
    }

    fn load_secret(&self) -> Result<RecoverySecret> {
        let path = Path::new(self.cfg.recovery.secret_path.trim());
        let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
        let secret: RecoverySecret =
            serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))?;
        if secret.mnemonic.trim().is_empty() {
            bail!("recovery mnemonic is empty");
        }
        if secret.password.len() < 3 {
            bail!("recovery wallet password must be at least 3 characters");
        }
        Ok(secret)
    }

    fn secret_is_configured(&self) -> bool {
        self.load_secret().is_ok()
    }

    fn detect_proxy_target(&self) -> Option<RecoveryInstanceId> {
        let raw = fs::read_to_string(self.cfg.recovery.proxy_include_path.trim()).ok()?;
        let primary_api = self.cfg.recovery.primary.api.trim();
        let standby_api = self.cfg.recovery.standby.api.trim();
        if !primary_api.is_empty() && raw.contains(primary_api) {
            Some(RecoveryInstanceId::Primary)
        } else if !standby_api.is_empty() && raw.contains(standby_api) {
            Some(RecoveryInstanceId::Standby)
        } else {
            None
        }
    }

    fn detect_active_cookie_target(&self) -> Option<RecoveryInstanceId> {
        let link = Path::new(self.cfg.recovery.active_cookie_path.trim());
        let target = fs::read_link(link).ok()?;
        if path_matches(
            &target,
            Path::new(self.cfg.recovery.primary.cookie_path.trim()),
        ) {
            Some(RecoveryInstanceId::Primary)
        } else if path_matches(
            &target,
            Path::new(self.cfg.recovery.standby.cookie_path.trim()),
        ) {
            Some(RecoveryInstanceId::Standby)
        } else {
            None
        }
    }

    fn effective_active_instance(&self) -> Option<RecoveryInstanceId> {
        match (
            self.detect_proxy_target(),
            self.detect_active_cookie_target(),
        ) {
            (Some(a), Some(b)) if a == b => Some(a),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            _ => None,
        }
    }

    fn inactive_instance(&self) -> Result<RecoveryInstanceId> {
        self.effective_active_instance()
            .map(RecoveryInstanceId::other)
            .ok_or_else(|| anyhow!("active daemon routing is not provisioned yet"))
    }

    fn payouts_paused(&self) -> bool {
        let path = self.cfg.payout_pause_file.trim();
        !path.is_empty() && Path::new(path).exists()
    }

    fn require_payouts_paused(&self) -> Result<()> {
        if !self.payouts_paused() {
            bail!("pause payouts before running recovery actions");
        }
        Ok(())
    }

    async fn daemon_get_status(&self, cfg: &RecoveryDaemonInstanceConfig) -> Result<NodeStatus> {
        self.daemon_get_json(cfg, "/api/status").await
    }

    async fn daemon_get_wallet_address(
        &self,
        cfg: &RecoveryDaemonInstanceConfig,
    ) -> Result<WalletAddressResponse> {
        self.daemon_get_json(cfg, "/api/wallet/address").await
    }

    async fn daemon_get_wallet_balance(
        &self,
        cfg: &RecoveryDaemonInstanceConfig,
    ) -> Result<WalletBalance> {
        self.daemon_get_json(cfg, "/api/wallet/balance").await
    }

    async fn daemon_import_wallet(
        &self,
        cfg: &RecoveryDaemonInstanceConfig,
        mnemonic: &str,
        password: &str,
    ) -> Result<String> {
        #[derive(Debug, Deserialize)]
        struct ImportResponse {
            address: String,
        }

        let response: ImportResponse = self
            .daemon_post_json(
                cfg,
                "/api/wallet/import",
                &serde_json::json!({
                    "mnemonic": mnemonic,
                    "password": password,
                }),
            )
            .await?;
        Ok(response.address)
    }

    async fn daemon_get_json<T>(&self, cfg: &RecoveryDaemonInstanceConfig, path: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let token = read_token_from_cookie(Path::new(cfg.cookie_path.trim()))?;
        let url = format!("{}{}", cfg.api.trim_end_matches('/'), path);
        let response = self
            .http
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        decode_daemon_response(response, &format!("GET {path}")).await
    }

    async fn daemon_post_json<T, R>(
        &self,
        cfg: &RecoveryDaemonInstanceConfig,
        path: &str,
        payload: &T,
    ) -> Result<R>
    where
        T: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let token = read_token_from_cookie(Path::new(cfg.cookie_path.trim()))?;
        let url = format!("{}{}", cfg.api.trim_end_matches('/'), path);
        let response = self
            .http
            .post(&url)
            .bearer_auth(token)
            .json(payload)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        decode_daemon_response(response, &format!("POST {path}")).await
    }
}

fn load_persisted_state(path: &Path) -> Result<PersistedRecoveryState> {
    if !path.exists() {
        return Ok(PersistedRecoveryState::default());
    }
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let mut state: PersistedRecoveryState =
        serde_json::from_slice(&data).with_context(|| format!("parse {}", path.display()))?;
    while state.operations.len() > MAX_OPERATIONS {
        state.operations.pop_back();
    }
    Ok(state)
}

fn persist_state(path: &Path, state: &PersistedRecoveryState) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
    }
    let data = serde_json::to_vec_pretty(state).context("serialize recovery state")?;
    write_atomic(path, &data)
}

fn bind_recovery_listener(socket_path: &str) -> Result<UnixListener> {
    if let Some(listener) = systemd_listener()? {
        return Ok(listener);
    }

    let path = Path::new(socket_path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
    }
    match fs::remove_file(path) {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("remove {}", path.display())),
    }

    let listener = UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))
        .with_context(|| format!("chmod {}", path.display()))?;
    Ok(listener)
}

fn systemd_listener() -> Result<Option<UnixListener>> {
    let listen_pid = env::var("LISTEN_PID")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok());
    let listen_fds = env::var("LISTEN_FDS")
        .ok()
        .and_then(|raw| raw.parse::<i32>().ok())
        .unwrap_or(0);
    if listen_pid != Some(std::process::id()) || listen_fds < 1 {
        return Ok(None);
    }

    let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(SYSTEMD_SOCKET_FD) };
    std_listener
        .set_nonblocking(true)
        .context("set recovery socket nonblocking")?;
    Ok(Some(
        UnixListener::from_std(std_listener).context("adopt systemd recovery socket")?,
    ))
}

async fn decode_daemon_response<T>(response: reqwest::Response, action: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!(
            "{action} failed with HTTP {}: {}",
            status.as_u16(),
            body.trim()
        );
    }
    serde_json::from_str(&body).with_context(|| format!("decode response for {action}"))
}

async fn run_command(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("run {program} {}", args.join(" ")))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        let detail = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit {}", output.status)
        };
        bail!("{program} {} failed: {detail}", args.join(" "));
    }
    Ok(stdout)
}

async fn systemctl_state(service: &str) -> Result<String> {
    let output = Command::new("systemctl")
        .arg("is-active")
        .arg(service)
        .output()
        .await
        .with_context(|| format!("run systemctl is-active {service}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stdout.is_empty() {
        return Ok(stdout);
    }
    if !stderr.is_empty() {
        return Ok(stderr);
    }
    if output.status.success() {
        Ok("active".to_string())
    } else {
        Ok("inactive".to_string())
    }
}

async fn validate_nginx_config() -> Result<()> {
    run_command("nginx", &["-t"]).await.map(|_| ())
}

async fn reload_nginx() -> Result<()> {
    run_command("systemctl", &["reload", "nginx.service"])
        .await
        .map(|_| ())
}

fn write_proxy_include(path: &Path, upstream: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
    }
    let content = format!("proxy_pass {};\n", upstream.trim());
    write_atomic(path, content.as_bytes())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn swap_symlink(link: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = link.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
    }
    let tmp = link.with_extension(format!("tmp.{}", std::process::id()));
    match fs::remove_file(&tmp) {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("remove {}", tmp.display())),
    }
    symlink(target, &tmp)
        .with_context(|| format!("symlink {} -> {}", tmp.display(), target.display()))?;
    fs::rename(&tmp, link)
        .with_context(|| format!("rename {} -> {}", tmp.display(), link.display()))?;
    Ok(())
}

fn clear_directory(path: &Path) -> Result<()> {
    validate_managed_path(path)?;
    if !path.exists() {
        fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
        return Ok(());
    }
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry.with_context(|| format!("read {}", path.display()))?;
        let entry_path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("stat {}", entry_path.display()))?;
        if metadata.is_dir() {
            fs::remove_dir_all(&entry_path)
                .with_context(|| format!("remove {}", entry_path.display()))?;
        } else {
            fs::remove_file(&entry_path)
                .with_context(|| format!("remove {}", entry_path.display()))?;
        }
    }
    Ok(())
}

fn validate_managed_path(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let path_str = path
        .to_str()
        .ok_or_else(|| anyhow!("managed path is not valid utf-8"))?;
    if !path_str.starts_with("/var/lib/blocknet") {
        bail!("refusing to modify unmanaged path {path_str}");
    }
    if path_str == "/var/lib/blocknet" {
        bail!("refusing to modify top-level daemon storage path");
    }
    Ok(())
}

fn read_token_from_cookie(path: &Path) -> Result<String> {
    let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let token = raw.trim();
    if token.is_empty() {
        bail!("daemon cookie {} is empty", path.display());
    }
    Ok(token.to_string())
}

fn path_matches(actual: &Path, expected: &Path) -> bool {
    if actual == expected {
        return true;
    }
    match (fs::canonicalize(actual), fs::canonicalize(expected)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

fn is_service_unavailable_wallet_error(err: &anyhow::Error) -> bool {
    err.to_string().contains(&format!(
        "HTTP {}",
        StatusCode::SERVICE_UNAVAILABLE.as_u16()
    )) && err
        .to_string()
        .to_ascii_lowercase()
        .contains("no wallet loaded")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn recovery_defaults_are_stable() {
        let cfg = RecoveryConfig::default();
        assert_eq!(cfg.socket_path, "/run/blocknet-recoveryd.sock");
        assert_eq!(cfg.primary.api, "http://127.0.0.1:18331");
        assert_eq!(cfg.standby.api, "http://127.0.0.1:18332");
    }

    #[test]
    fn managed_path_guard_rejects_root_path() {
        let err = validate_managed_path(Path::new("/var/lib/blocknet"))
            .expect_err("guard should reject root");
        assert!(err.to_string().contains("top-level"));
    }

    #[test]
    fn managed_path_guard_accepts_instance_path() {
        validate_managed_path(Path::new("/var/lib/blocknet-standby/data")).expect("managed path");
    }

    #[test]
    fn secret_is_not_configured_for_placeholder_file() {
        let dir = tempdir().expect("tempdir");
        let secret_path = dir.path().join("pool-wallet.json");
        fs::write(&secret_path, r#"{"mnemonic":"","password":""}"#).expect("write secret");

        let mut cfg = Config::default();
        cfg.recovery.secret_path = secret_path.display().to_string();

        let agent = RecoveryAgent {
            cfg,
            http: reqwest::Client::new(),
            state: Mutex::new(PersistedRecoveryState::default()),
            status_cache: Mutex::new(None),
        };
        assert!(!agent.secret_is_configured());
    }

    #[test]
    fn secret_is_configured_for_valid_secret_file() {
        let dir = tempdir().expect("tempdir");
        let secret_path = dir.path().join("pool-wallet.json");
        fs::write(
            &secret_path,
            r#"{"mnemonic":"alpha beta gamma","password":"hunter2"}"#,
        )
        .expect("write secret");

        let mut cfg = Config::default();
        cfg.recovery.secret_path = secret_path.display().to_string();

        let agent = RecoveryAgent {
            cfg,
            http: reqwest::Client::new(),
            state: Mutex::new(PersistedRecoveryState::default()),
            status_cache: Mutex::new(None),
        };
        assert!(agent.secret_is_configured());
    }
}
