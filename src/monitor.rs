use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use parking_lot::{Mutex, RwLock};
use reqwest::blocking::Client as BlockingClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::db::{MonitorHeartbeatUpsert, MonitorIncidentUpsert};
use crate::node::{NodeClient, NodeCurrentProcessBlock, NodeLastProcessBlock, NodeStatus};
use crate::pool_activity::{assess_pool_activity, POOL_ACTIVITY_SNAPSHOT_STALE_AFTER};
use crate::service_state::{PersistedRuntimeSnapshot, LIVE_RUNTIME_SNAPSHOT_META_KEY};
use crate::store::PoolStore;
use pool_runtime::telemetry::{ApiPerformanceSnapshot, TimedOperationSummary};

const LOCAL_MONITOR_SOURCE: &str = "local";
const LOCAL_API_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL_DAEMON_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const LOCAL_STRATUM_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const LOCAL_WALLET_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const EXTERNAL_REFERENCE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_HEARTBEAT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_INCIDENT_RETENTION: Duration = Duration::from_secs(180 * 24 * 60 * 60);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60 * 60);
const STRATUM_SNAPSHOT_STALE_AFTER: Duration = POOL_ACTIVITY_SNAPSHOT_STALE_AFTER;
const TEMPLATE_REFRESH_WARN_AFTER: Duration = Duration::from_secs(45);
const TEMPLATE_REFRESH_CRITICAL_AFTER: Duration = Duration::from_secs(120);
const DAEMON_SLOW_BLOCK_WARN_AFTER: Duration = Duration::from_secs(5);
const DAEMON_SLOW_BLOCK_CRITICAL_AFTER: Duration = Duration::from_secs(15);
const DAEMON_SLOW_BLOCK_RECENT_AFTER: Duration = Duration::from_secs(60);
const POOL_ACTIVITY_LOSS_MIN_THRESHOLD: Duration = Duration::from_secs(60);
const POOL_ACTIVITY_LOSS_THRESHOLD_SAMPLES: u32 = 6;

#[derive(Debug, Clone, Default, Serialize)]
pub struct MonitorSnapshot {
    pub updated_at: Option<SystemTime>,
    pub summary_state: String,
    pub api_up: Option<bool>,
    pub stratum_up: Option<bool>,
    pub db_up: Option<bool>,
    pub daemon_up: Option<bool>,
    pub daemon_syncing: Option<bool>,
    pub daemon_current_process_block: Option<NodeCurrentProcessBlock>,
    pub daemon_last_process_block: Option<NodeLastProcessBlock>,
    pub chain_height: Option<u64>,
    pub template_age_seconds: Option<u64>,
    pub last_refresh_millis: Option<u64>,
    pub stratum_snapshot_age_seconds: Option<u64>,
    pub connected_miners: Option<u64>,
    pub connected_workers: Option<u64>,
    pub estimated_hashrate: Option<f64>,
    pub wallet_up: Option<bool>,
    pub last_accepted_share_at: Option<SystemTime>,
    pub last_accepted_share_age_seconds: Option<u64>,
    pub payout_pending_count: Option<u64>,
    pub payout_pending_amount: Option<u64>,
    pub oldest_pending_payout_at: Option<SystemTime>,
    pub oldest_pending_payout_age_seconds: Option<u64>,
    pub oldest_pending_send_started_at: Option<SystemTime>,
    pub oldest_pending_send_age_seconds: Option<u64>,
    pub validation_candidate_queue_depth: Option<u64>,
    pub validation_regular_queue_depth: Option<u64>,
    pub reference_height_spread: Option<u64>,
    pub reference_height_min: Option<u64>,
    pub reference_height_max: Option<u64>,
    #[serde(default)]
    pub reference_heights: BTreeMap<String, u64>,
    #[serde(default)]
    pub reference_source_up: BTreeMap<String, bool>,
    #[serde(default)]
    pub api_performance: ApiPerformanceSnapshot,
    #[serde(default)]
    pub stratum_runtime_tasks: BTreeMap<String, TimedOperationSummary>,
    #[serde(default)]
    pub process_metrics: BTreeMap<String, ProcessMetrics>,
    pub open_public_incidents: u64,
    pub open_private_incidents: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProcessMetrics {
    pub user_cpu_seconds: Option<f64>,
    pub system_cpu_seconds: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub thread_count: Option<u64>,
}

#[derive(Debug, Clone)]
struct ReferenceHeightSample {
    source: String,
    height: Option<u64>,
    error: Option<String>,
}

impl ReferenceHeightSample {
    fn ok(source: String, height: u64) -> Self {
        Self {
            source,
            height: Some(height),
            error: None,
        }
    }

    fn err(source: String, error: impl Into<String>) -> Self {
        Self {
            source,
            height: None,
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PublicChainStatusResponse {
    chain_height: u64,
}

#[derive(Debug, Deserialize)]
struct PoolStatusResponse {
    daemon: PoolStatusDaemon,
}

#[derive(Debug, Deserialize)]
struct PoolStatusDaemon {
    chain_height: Option<u64>,
}

#[derive(Debug, Clone)]
struct DiscordNotifier {
    bot_token: String,
    channel_id: String,
    mention_user_id: Option<String>,
}

impl DiscordNotifier {
    fn from_env() -> Option<Self> {
        let bot_token = std::env::var("DISCORD_BOT_TOKEN").ok()?.trim().to_string();
        let channel_id = std::env::var("DISCORD_CHAIN_DIVERGENCE_CHANNEL_ID")
            .ok()
            .or_else(|| std::env::var("DISCORD_ALERT_CHANNEL_ID").ok())?
            .trim()
            .to_string();
        if bot_token.is_empty() || channel_id.is_empty() {
            return None;
        }
        let mention_user_id = std::env::var("DISCORD_CHAIN_DIVERGENCE_MENTION_USER_ID")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        Some(Self {
            bot_token,
            channel_id,
            mention_user_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainDivergenceTransition {
    Opened,
    Resolved,
}

#[derive(Debug, Clone)]
struct ProbeOutcome<T> {
    value: Option<T>,
    error: Option<String>,
}

impl<T> Default for ProbeOutcome<T> {
    fn default() -> Self {
        Self {
            value: None,
            error: None,
        }
    }
}

impl<T> ProbeOutcome<T> {
    fn ok(value: T) -> Self {
        Self {
            value: Some(value),
            error: None,
        }
    }

    fn err(err: impl Into<String>) -> Self {
        Self {
            value: None,
            error: Some(err.into()),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ConditionState {
    failing_since: Option<SystemTime>,
}

#[derive(Debug, Clone, Default)]
struct IncidentState {
    api_down: ConditionState,
    db_down: ConditionState,
    stratum_down: ConditionState,
    pool_activity_lost: ConditionState,
    daemon_down: ConditionState,
    daemon_syncing: ConditionState,
    daemon_slow_block_processing: ConditionState,
    template_stale: ConditionState,
    validation_backlog: ConditionState,
    payout_queue_stalled: ConditionState,
    wallet_unavailable: ConditionState,
    share_ingest_stalled: ConditionState,
    reference_height_divergence: ConditionState,
}

#[derive(Debug, Clone, Default)]
struct PendingPayoutSummary {
    count: u64,
    amount: u64,
    oldest_pending_at: Option<SystemTime>,
    oldest_send_started_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
struct MonitorSample {
    sampled_at: SystemTime,
    api_probe: ProbeOutcome<()>,
    stratum_probe: ProbeOutcome<()>,
    db_probe: ProbeOutcome<()>,
    daemon_status: ProbeOutcome<NodeStatus>,
    wallet_probe: ProbeOutcome<()>,
    runtime_snapshot: Option<PersistedRuntimeSnapshot>,
    api_performance: Option<ApiPerformanceSnapshot>,
    process_metrics: BTreeMap<String, ProcessMetrics>,
    payout_summary: Option<PendingPayoutSummary>,
    reference_heights: Vec<ReferenceHeightSample>,
    summary_state: String,
}

#[derive(Debug)]
enum IncidentStoreAction {
    Upsert(MonitorIncidentUpsert),
    Resolve {
        dedupe_key: String,
        ended_at: SystemTime,
    },
}

#[derive(Clone)]
struct MonitorHttpState {
    snapshot: Arc<RwLock<MonitorSnapshot>>,
    interval: Duration,
}

pub async fn run_monitor(config_path: &Path) -> Result<()> {
    load_dotenv(config_path);
    let cfg = Config::load(config_path)?;
    let cfg_for_store = cfg.clone();
    let store = tokio::task::spawn_blocking(move || {
        PoolStore::open(
            &cfg_for_store.database_url,
            cfg_for_store.database_pool_size,
        )
    })
    .await
    .context("join monitor store initialization task")??;
    let daemon_api = cfg.daemon_api.clone();
    let daemon_token = cfg.daemon_token.clone();
    let daemon_data_dir = cfg.daemon_data_dir.clone();
    let daemon_cookie_path = cfg.daemon_cookie_path.clone();
    let node = tokio::task::spawn_blocking(move || {
        NodeClient::new_with_daemon_auth(
            &daemon_api,
            &daemon_token,
            &daemon_data_dir,
            &daemon_cookie_path,
        )
    })
    .await
    .context("join monitor node initialization task")??;
    let node = Arc::new(node);
    let api_client = build_http_client(LOCAL_API_PROBE_TIMEOUT, true)?;
    let external_client = build_http_client(EXTERNAL_REFERENCE_PROBE_TIMEOUT, false)?;
    let monitor = Arc::new(MonitorRuntime {
        cfg: cfg.clone(),
        store,
        node,
        api_client,
        external_client,
        discord_notifier: DiscordNotifier::from_env(),
        metrics: Arc::new(RwLock::new(MonitorSnapshot {
            summary_state: "starting".to_string(),
            ..MonitorSnapshot::default()
        })),
        incidents: Arc::new(Mutex::new(IncidentState::default())),
        spool_path: PathBuf::from(cfg.monitor_spool_path.clone()),
        interval: cfg.monitor_interval_duration(),
        last_housekeeping_at: Arc::new(Mutex::new(None)),
    });

    let loop_monitor = Arc::clone(&monitor);
    tokio::spawn(async move {
        if let Err(err) = loop_monitor.monitor_loop().await {
            tracing::error!(error = %err, "monitor loop exited");
        }
    });

    let state = MonitorHttpState {
        snapshot: Arc::clone(&monitor.metrics),
        interval: monitor.interval,
    };
    let addr = monitor.listen_addr()?;
    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/healthz", get(handle_healthz))
        .route("/snapshot", get(handle_snapshot))
        .with_state(state);

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind monitor listen address {addr}"))?;
    axum::serve(listener, app)
        .await
        .context("serve monitor http endpoint")
}

struct MonitorRuntime {
    cfg: Config,
    store: Arc<PoolStore>,
    node: Arc<NodeClient>,
    api_client: BlockingClient,
    external_client: BlockingClient,
    discord_notifier: Option<DiscordNotifier>,
    metrics: Arc<RwLock<MonitorSnapshot>>,
    incidents: Arc<Mutex<IncidentState>>,
    spool_path: PathBuf,
    interval: Duration,
    last_housekeeping_at: Arc<Mutex<Option<SystemTime>>>,
}

impl MonitorRuntime {
    fn listen_addr(&self) -> Result<SocketAddr> {
        format!("{}:{}", self.cfg.monitor_host, self.cfg.monitor_port)
            .parse()
            .with_context(|| {
                format!(
                    "invalid monitor listen address {}:{}",
                    self.cfg.monitor_host, self.cfg.monitor_port
                )
            })
    }

    async fn run_store_task<R, F>(&self, label: &'static str, op: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(Arc<PoolStore>) -> Result<R> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || op(store))
            .await
            .with_context(|| format!("join {label}"))?
    }

    async fn monitor_loop(self: Arc<Self>) -> Result<()> {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            ticker.tick().await;
            let sampled_at = SystemTime::now();
            let sample = self.collect_sample(sampled_at).await;
            if let Err(err) = self.persist_sample(&sample).await {
                tracing::warn!(error = %err, "failed persisting monitor sample");
                self.update_metrics(&sample, Some(err.to_string())).await;
            } else if let Err(err) = self.sync_incidents(&sample).await {
                tracing::warn!(error = %err, "failed syncing monitor incidents");
                self.update_metrics(&sample, Some(err.to_string())).await;
            } else {
                self.update_metrics(&sample, None).await;
            }
        }
    }

    async fn collect_sample(&self, sampled_at: SystemTime) -> MonitorSample {
        let api = self.probe_api().await;
        let stratum = self.probe_stratum().await;
        let db = self.probe_database().await;
        let daemon = self.probe_daemon().await;
        let wallet = if daemon.error.is_none() {
            self.probe_wallet().await
        } else {
            ProbeOutcome::default()
        };
        let reference_heights = self.probe_reference_heights().await;

        let mut runtime_snapshot = None;
        let mut payout_summary = None;
        if db.error.is_none() {
            runtime_snapshot = self.load_runtime_snapshot().await;
            payout_summary = self.load_pending_payouts().await;
        }
        let api_performance = if api.error.is_none() {
            self.load_api_performance().await
        } else {
            None
        };
        let process_metrics = self.load_process_metrics().await;

        let template_refresh_millis = runtime_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.jobs.last_refresh_millis);
        let stratum_snapshot_fresh = runtime_snapshot_age(sampled_at, runtime_snapshot.as_ref())
            .is_some_and(|age| age <= STRATUM_SNAPSHOT_STALE_AFTER);
        let validation_backlog = validation_backlog_state(
            runtime_snapshot.as_ref(),
            self.cfg.max_validation_queue.max(1) as u64,
        );
        let payout_queue_stalled =
            payout_queue_state(&self.cfg, sampled_at, payout_summary.as_ref());
        let share_ingest_stalled = share_progress_state(sampled_at, runtime_snapshot.as_ref());
        let pool_activity_lost = pool_activity_loss_state(
            sampled_at,
            runtime_snapshot.as_ref(),
            STRATUM_SNAPSHOT_STALE_AFTER,
        );
        let daemon_slow_block = daemon_slow_block_state(sampled_at, daemon.value.as_ref());
        let reference_height_divergence = reference_height_divergence_state(
            &reference_heights,
            self.cfg.height_divergence_threshold,
        );
        let summary_state = summarize_state(
            api.error.is_none(),
            stratum.error.is_none() && stratum_snapshot_fresh,
            db.error.is_none(),
            daemon.value.as_ref().is_some_and(|status| !status.syncing),
            template_refresh_millis,
            daemon_slow_block.as_ref().map(|(severity, _)| *severity),
            pool_activity_lost.as_ref().map(|(severity, _)| *severity),
            validation_backlog.as_ref().map(|(severity, _)| *severity),
            payout_queue_stalled.as_ref().map(|(severity, _)| *severity),
            share_ingest_stalled.as_ref().map(|(severity, _)| *severity),
            reference_height_divergence
                .as_ref()
                .map(|(severity, _, _)| *severity),
            wallet.error.is_none(),
        );

        MonitorSample {
            sampled_at,
            api_probe: api,
            stratum_probe: stratum,
            db_probe: db,
            daemon_status: daemon,
            wallet_probe: wallet,
            runtime_snapshot,
            api_performance,
            process_metrics,
            payout_summary,
            reference_heights,
            summary_state,
        }
    }

    async fn persist_sample(&self, sample: &MonitorSample) -> Result<()> {
        if sample.db_probe.error.is_some() {
            append_spool_entry(&self.spool_path, &self.sample_to_heartbeat(sample, false)?)?;
            return Ok(());
        }

        self.flush_spool().await?;
        self.backfill_local_gap(sample.sampled_at).await?;
        let heartbeat = self.sample_to_heartbeat(sample, false)?;
        self.run_store_task("monitor heartbeat upsert task", move |store| {
            store.upsert_monitor_heartbeat(&heartbeat)
        })
        .await?;
        self.prune_old_heartbeats(sample.sampled_at).await?;
        Ok(())
    }

    async fn sync_incidents(&self, sample: &MonitorSample) -> Result<()> {
        if sample.db_probe.error.is_some() {
            return Ok(());
        }

        let mut incidents = { self.incidents.lock().clone() };
        let mut actions = Vec::new();
        let now = sample.sampled_at;
        let daemon_syncing = sample
            .daemon_status
            .value
            .as_ref()
            .is_some_and(|status| status.syncing);
        let template_refresh_millis = sample
            .runtime_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.jobs.last_refresh_millis);
        let snapshot_stale = runtime_snapshot_age(now, sample.runtime_snapshot.as_ref())
            .is_some_and(|age| age > STRATUM_SNAPSHOT_STALE_AFTER);
        let validation_backlog = validation_backlog_state(
            sample.runtime_snapshot.as_ref(),
            self.cfg.max_validation_queue.max(1) as u64,
        );
        let payout_queue_stalled =
            payout_queue_state(&self.cfg, now, sample.payout_summary.as_ref());
        let share_ingest_stalled = share_progress_state(now, sample.runtime_snapshot.as_ref());
        let pool_activity_lost = pool_activity_loss_state(
            now,
            sample.runtime_snapshot.as_ref(),
            STRATUM_SNAPSHOT_STALE_AFTER,
        );
        let daemon_slow_block = daemon_slow_block_state(now, sample.daemon_status.value.as_ref());
        let reference_height_divergence = reference_height_divergence_state(
            &sample.reference_heights,
            self.cfg.height_divergence_threshold,
        );
        let reference_height_divergence_was_active = incidents
            .reference_height_divergence
            .failing_since
            .is_some();

        self.sync_condition(
            &mut incidents.api_down,
            "local_api_down",
            "api_down",
            "critical",
            "public",
            sample.api_probe.error.is_some(),
            Duration::from_secs(20),
            now,
            sample.api_probe.error.clone(),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.db_down,
            "local_database_down",
            "pool_database_down",
            "critical",
            "public",
            sample.db_probe.error.is_some(),
            Duration::from_secs(20),
            now,
            sample.db_probe.error.clone(),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.stratum_down,
            "local_stratum_down",
            "stratum_down",
            "critical",
            "public",
            sample.stratum_probe.error.is_some() || snapshot_stale,
            STRATUM_SNAPSHOT_STALE_AFTER,
            now,
            sample
                .stratum_probe
                .error
                .clone()
                .or_else(|| Some("persisted runtime snapshot is stale".to_string())),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.pool_activity_lost,
            "local_pool_activity_lost",
            "pool_activity_lost",
            pool_activity_lost
                .as_ref()
                .map(|(severity, _)| *severity)
                .unwrap_or("critical"),
            "private",
            pool_activity_lost.is_some(),
            pool_activity_loss_threshold(self.interval),
            now,
            pool_activity_lost.map(|(_, detail)| detail),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.daemon_down,
            "local_daemon_down",
            "daemon_down",
            "critical",
            "public",
            sample.daemon_status.error.is_some(),
            Duration::from_secs(30),
            now,
            sample.daemon_status.error.clone(),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.daemon_syncing,
            "local_daemon_syncing",
            "daemon_syncing",
            "warn",
            "public",
            daemon_syncing,
            Duration::from_secs(0),
            now,
            Some("daemon reported syncing".to_string()),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.daemon_slow_block_processing,
            "local_daemon_slow_block_processing",
            "daemon_slow_block_processing",
            daemon_slow_block
                .as_ref()
                .map(|(severity, _)| *severity)
                .unwrap_or("warn"),
            "public",
            daemon_slow_block.is_some(),
            Duration::from_secs(0),
            now,
            daemon_slow_block.map(|(_, detail)| detail),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.template_stale,
            "local_template_stale",
            "template_stale",
            if template_refresh_millis
                .is_some_and(|lag| lag >= TEMPLATE_REFRESH_CRITICAL_AFTER.as_millis() as u64)
            {
                "critical"
            } else {
                "warn"
            },
            "public",
            template_refresh_millis
                .is_some_and(|lag| lag >= TEMPLATE_REFRESH_WARN_AFTER.as_millis() as u64),
            Duration::from_secs(0),
            now,
            template_refresh_millis
                .map(|lag| format!("latest template refresh lag is {}s", lag / 1000)),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.validation_backlog,
            "local_validation_backlog",
            "validation_backlog",
            validation_backlog
                .as_ref()
                .map(|(severity, _)| *severity)
                .unwrap_or("warn"),
            "private",
            validation_backlog.is_some(),
            Duration::from_secs(60),
            now,
            validation_backlog.map(|(_, detail)| detail),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.payout_queue_stalled,
            "local_payout_queue_stalled",
            "payout_queue_stalled",
            payout_queue_stalled
                .as_ref()
                .map(|(severity, _)| *severity)
                .unwrap_or("warn"),
            "private",
            payout_queue_stalled.is_some(),
            Duration::from_secs(0),
            now,
            payout_queue_stalled.map(|(_, detail)| detail),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.wallet_unavailable,
            "local_wallet_unavailable",
            "wallet_unavailable",
            "warn",
            "private",
            sample.wallet_probe.error.is_some(),
            Duration::from_secs(10 * 60),
            now,
            sample.wallet_probe.error.clone(),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.share_ingest_stalled,
            "local_share_ingest_stalled",
            "share_ingest_stalled",
            share_ingest_stalled
                .as_ref()
                .map(|(severity, _)| *severity)
                .unwrap_or("warn"),
            "private",
            share_ingest_stalled.is_some(),
            Duration::from_secs(0),
            now,
            share_ingest_stalled.map(|(_, detail)| detail),
            &mut actions,
        )?;
        self.sync_condition(
            &mut incidents.reference_height_divergence,
            "external_chain_height_divergence",
            "chain_height_divergence",
            "critical",
            "public",
            reference_height_divergence.is_some(),
            Duration::from_secs(0),
            now,
            reference_height_divergence
                .as_ref()
                .map(|(_, _, detail)| detail.clone()),
            &mut actions,
        )?;
        let reference_height_divergence_is_active = incidents
            .reference_height_divergence
            .failing_since
            .is_some();
        let reference_height_divergence_transition = match (
            reference_height_divergence_was_active,
            reference_height_divergence_is_active,
        ) {
            (false, true) => Some(ChainDivergenceTransition::Opened),
            (true, false) => Some(ChainDivergenceTransition::Resolved),
            _ => None,
        };

        *self.incidents.lock() = incidents;
        if !actions.is_empty() {
            self.run_store_task("apply monitor incident updates", move |store| {
                for action in actions {
                    match action {
                        IncidentStoreAction::Upsert(incident) => {
                            store.upsert_monitor_incident(&incident)?;
                        }
                        IncidentStoreAction::Resolve {
                            dedupe_key,
                            ended_at,
                        } => {
                            store.resolve_monitor_incident(&dedupe_key, ended_at)?;
                        }
                    }
                }
                Ok(())
            })
            .await?;
        }
        if let Some(transition) = reference_height_divergence_transition {
            let detail = reference_height_divergence
                .as_ref()
                .map(|(_, _, detail)| detail.clone())
                .unwrap_or_else(|| {
                    format!(
                        "height spread returned within {} blocks",
                        self.cfg.height_divergence_threshold
                    )
                });
            self.notify_chain_divergence(transition, detail).await;
        }

        Ok(())
    }

    fn sync_condition(
        &self,
        state: &mut ConditionState,
        dedupe_key: &str,
        kind: &str,
        severity: &str,
        visibility: &str,
        active: bool,
        threshold: Duration,
        now: SystemTime,
        detail: Option<String>,
        actions: &mut Vec<IncidentStoreAction>,
    ) -> Result<()> {
        if active {
            let started_at = *state.failing_since.get_or_insert(now);
            if now.duration_since(started_at).unwrap_or_default() >= threshold {
                actions.push(IncidentStoreAction::Upsert(MonitorIncidentUpsert {
                    dedupe_key: dedupe_key.to_string(),
                    kind: kind.to_string(),
                    severity: severity.to_string(),
                    visibility: visibility.to_string(),
                    source: LOCAL_MONITOR_SOURCE.to_string(),
                    summary: detail.clone().unwrap_or_else(|| kind.replace('_', " ")),
                    detail,
                    started_at,
                    updated_at: now,
                }));
            }
            return Ok(());
        }

        let dedupe_key_owned = dedupe_key.to_string();
        if let Some(started_at) = state.failing_since.take() {
            if now.duration_since(started_at).unwrap_or_default() >= threshold {
                actions.push(IncidentStoreAction::Upsert(MonitorIncidentUpsert {
                    dedupe_key: dedupe_key_owned.clone(),
                    kind: kind.to_string(),
                    severity: severity.to_string(),
                    visibility: visibility.to_string(),
                    source: LOCAL_MONITOR_SOURCE.to_string(),
                    summary: detail
                        .clone()
                        .unwrap_or_else(|| format!("{kind} recovered")),
                    detail,
                    started_at,
                    updated_at: now,
                }));
                actions.push(IncidentStoreAction::Resolve {
                    dedupe_key: dedupe_key_owned,
                    ended_at: now,
                });
            }
        } else {
            actions.push(IncidentStoreAction::Resolve {
                dedupe_key: dedupe_key_owned,
                ended_at: now,
            });
        }

        Ok(())
    }

    async fn update_metrics(&self, sample: &MonitorSample, error: Option<String>) {
        let open_public = if sample.db_probe.error.is_none() {
            self.run_store_task("load open public monitor incidents", move |store| {
                store.get_open_monitor_incidents(Some("public"))
            })
            .await
            .map(|items| items.len() as u64)
            .unwrap_or(0)
        } else {
            0
        };
        let open_private = if sample.db_probe.error.is_none() {
            self.run_store_task("load open private monitor incidents", move |store| {
                store.get_open_monitor_incidents(Some("private"))
            })
            .await
            .map(|items| items.len() as u64)
            .unwrap_or(0)
        } else {
            0
        };
        let daemon = sample.daemon_status.value.as_ref();
        let runtime = sample.runtime_snapshot.as_ref();
        let mut metrics = self.metrics.write();
        metrics.updated_at = Some(sample.sampled_at);
        metrics.summary_state = sample.summary_state.clone();
        metrics.api_up = Some(sample.api_probe.error.is_none());
        metrics.stratum_up = Some(sample.stratum_probe.error.is_none());
        metrics.db_up = Some(sample.db_probe.error.is_none());
        metrics.daemon_up = Some(sample.daemon_status.error.is_none());
        metrics.daemon_syncing = daemon.map(|status| status.syncing);
        metrics.daemon_current_process_block =
            daemon.and_then(|status| status.current_process_block.clone());
        metrics.daemon_last_process_block =
            daemon.and_then(|status| status.last_process_block.clone());
        metrics.chain_height = daemon.map(|status| status.chain_height);
        metrics.template_age_seconds =
            runtime.and_then(|snapshot| snapshot.jobs.template_age_seconds);
        metrics.last_refresh_millis =
            runtime.and_then(|snapshot| snapshot.jobs.last_refresh_millis);
        metrics.stratum_snapshot_age_seconds = runtime.and_then(|snapshot| {
            sample
                .sampled_at
                .duration_since(snapshot.sampled_at)
                .ok()
                .map(|age| age.as_secs())
        });
        metrics.connected_miners = runtime.map(|snapshot| snapshot.connected_miners as u64);
        metrics.connected_workers = runtime.map(|snapshot| snapshot.connected_workers as u64);
        metrics.estimated_hashrate = runtime.map(|snapshot| snapshot.estimated_hashrate);
        metrics.wallet_up = Some(sample.wallet_probe.error.is_none());
        metrics.last_accepted_share_at = runtime.and_then(|snapshot| snapshot.last_share_at);
        metrics.last_accepted_share_age_seconds = runtime.and_then(|snapshot| {
            snapshot
                .last_share_at
                .and_then(|last| sample.sampled_at.duration_since(last).ok())
                .map(|age| age.as_secs())
        });
        metrics.payout_pending_count = sample.payout_summary.as_ref().map(|summary| summary.count);
        metrics.payout_pending_amount =
            sample.payout_summary.as_ref().map(|summary| summary.amount);
        metrics.oldest_pending_payout_at = sample
            .payout_summary
            .as_ref()
            .and_then(|summary| summary.oldest_pending_at);
        metrics.oldest_pending_payout_age_seconds =
            sample.payout_summary.as_ref().and_then(|summary| {
                summary
                    .oldest_pending_at
                    .and_then(|ts| sample.sampled_at.duration_since(ts).ok())
                    .map(|age| age.as_secs())
            });
        metrics.oldest_pending_send_started_at = sample
            .payout_summary
            .as_ref()
            .and_then(|summary| summary.oldest_send_started_at);
        metrics.oldest_pending_send_age_seconds =
            sample.payout_summary.as_ref().and_then(|summary| {
                summary
                    .oldest_send_started_at
                    .and_then(|ts| sample.sampled_at.duration_since(ts).ok())
                    .map(|age| age.as_secs())
            });
        metrics.validation_candidate_queue_depth =
            runtime.map(|snapshot| snapshot.validation.candidate_queue_depth as u64);
        metrics.validation_regular_queue_depth =
            runtime.map(|snapshot| snapshot.validation.regular_queue_depth as u64);
        let observed_reference_heights = sample
            .reference_heights
            .iter()
            .filter_map(|item| item.height)
            .collect::<Vec<_>>();
        metrics.reference_height_spread = match (
            observed_reference_heights.iter().min(),
            observed_reference_heights.iter().max(),
        ) {
            (Some(min_height), Some(max_height)) => Some(max_height.saturating_sub(*min_height)),
            _ => None,
        };
        metrics.reference_height_min = observed_reference_heights.iter().min().copied();
        metrics.reference_height_max = observed_reference_heights.iter().max().copied();
        metrics.reference_heights = sample
            .reference_heights
            .iter()
            .filter_map(|item| item.height.map(|height| (item.source.clone(), height)))
            .collect();
        metrics.reference_source_up = sample
            .reference_heights
            .iter()
            .map(|item| (item.source.clone(), item.error.is_none()))
            .collect();
        metrics.api_performance = sample.api_performance.clone().unwrap_or_default();
        metrics.stratum_runtime_tasks = runtime
            .map(|snapshot| snapshot.runtime_tasks.clone())
            .unwrap_or_default();
        metrics.process_metrics = sample.process_metrics.clone();
        metrics.open_public_incidents = open_public;
        metrics.open_private_incidents = open_private;
        metrics.last_error = error;
    }

    fn sample_to_heartbeat(
        &self,
        sample: &MonitorSample,
        synthetic: bool,
    ) -> Result<MonitorHeartbeatUpsert> {
        let daemon = sample.daemon_status.value.as_ref();
        let runtime = sample.runtime_snapshot.as_ref();
        let details = json!({
            "api_error": sample.api_probe.error,
            "stratum_error": sample.stratum_probe.error,
            "db_error": sample.db_probe.error,
            "daemon_error": sample.daemon_status.error,
            "wallet_error": sample.wallet_probe.error,
            "daemon_current_process_block": daemon.and_then(|status| status.current_process_block.as_ref()),
            "daemon_last_process_block": daemon.and_then(|status| status.last_process_block.as_ref()),
            "reference_heights": sample.reference_heights.iter().map(|item| json!({
                "source": item.source,
                "height": item.height,
                "error": item.error,
            })).collect::<Vec<_>>(),
            "reference_height_spread": reference_height_spread(&sample.reference_heights),
        });
        Ok(MonitorHeartbeatUpsert {
            sampled_at: sample.sampled_at,
            source: LOCAL_MONITOR_SOURCE.to_string(),
            synthetic,
            api_up: Some(sample.api_probe.error.is_none()),
            stratum_up: Some(sample.stratum_probe.error.is_none()),
            db_up: sample.db_probe.error.is_none(),
            daemon_up: Some(sample.daemon_status.error.is_none()),
            public_http_up: None,
            daemon_syncing: daemon.map(|status| status.syncing),
            chain_height: daemon.map(|status| status.chain_height),
            template_age_seconds: runtime.and_then(|snapshot| snapshot.jobs.template_age_seconds),
            last_refresh_millis: runtime.and_then(|snapshot| snapshot.jobs.last_refresh_millis),
            stratum_snapshot_age_seconds: runtime.and_then(|snapshot| {
                sample
                    .sampled_at
                    .duration_since(snapshot.sampled_at)
                    .ok()
                    .map(|age| age.as_secs())
            }),
            connected_miners: runtime.map(|snapshot| snapshot.connected_miners as u64),
            connected_workers: runtime.map(|snapshot| snapshot.connected_workers as u64),
            estimated_hashrate: runtime.map(|snapshot| snapshot.estimated_hashrate),
            wallet_up: Some(sample.wallet_probe.error.is_none()),
            last_accepted_share_at: runtime.and_then(|snapshot| snapshot.last_share_at),
            last_accepted_share_age_seconds: runtime.and_then(|snapshot| {
                snapshot
                    .last_share_at
                    .and_then(|last| sample.sampled_at.duration_since(last).ok())
                    .map(|age| age.as_secs())
            }),
            payout_pending_count: sample.payout_summary.as_ref().map(|summary| summary.count),
            payout_pending_amount: sample.payout_summary.as_ref().map(|summary| summary.amount),
            oldest_pending_payout_at: sample
                .payout_summary
                .as_ref()
                .and_then(|summary| summary.oldest_pending_at),
            oldest_pending_payout_age_seconds: sample.payout_summary.as_ref().and_then(|summary| {
                summary
                    .oldest_pending_at
                    .and_then(|ts| sample.sampled_at.duration_since(ts).ok())
                    .map(|age| age.as_secs())
            }),
            oldest_pending_send_started_at: sample
                .payout_summary
                .as_ref()
                .and_then(|summary| summary.oldest_send_started_at),
            oldest_pending_send_age_seconds: sample.payout_summary.as_ref().and_then(|summary| {
                summary
                    .oldest_send_started_at
                    .and_then(|ts| sample.sampled_at.duration_since(ts).ok())
                    .map(|age| age.as_secs())
            }),
            validation_candidate_queue_depth: runtime
                .map(|snapshot| snapshot.validation.candidate_queue_depth as u64),
            validation_regular_queue_depth: runtime
                .map(|snapshot| snapshot.validation.regular_queue_depth as u64),
            summary_state: sample.summary_state.clone(),
            details_json: Some(details.to_string()),
        })
    }

    async fn backfill_local_gap(&self, sampled_at: SystemTime) -> Result<()> {
        let Some(previous) = self
            .run_store_task("load latest local monitor heartbeat", move |store| {
                store.get_latest_monitor_heartbeat(Some(LOCAL_MONITOR_SOURCE))
            })
            .await?
        else {
            return Ok(());
        };
        let gap = sampled_at
            .duration_since(previous.sampled_at)
            .unwrap_or_default();
        if gap <= self.interval.saturating_mul(2) {
            return Ok(());
        }

        let mut ts = previous
            .sampled_at
            .checked_add(self.interval)
            .unwrap_or(sampled_at);
        let max_rows = 10_000usize;
        let mut written = 0usize;
        let mut heartbeats = Vec::new();
        while ts < sampled_at && written < max_rows {
            heartbeats.push(MonitorHeartbeatUpsert {
                sampled_at: ts,
                source: LOCAL_MONITOR_SOURCE.to_string(),
                synthetic: true,
                api_up: Some(false),
                stratum_up: Some(false),
                db_up: false,
                daemon_up: Some(false),
                public_http_up: None,
                daemon_syncing: None,
                chain_height: None,
                template_age_seconds: None,
                last_refresh_millis: None,
                stratum_snapshot_age_seconds: None,
                connected_miners: None,
                connected_workers: None,
                estimated_hashrate: None,
                wallet_up: None,
                last_accepted_share_at: None,
                last_accepted_share_age_seconds: None,
                payout_pending_count: None,
                payout_pending_amount: None,
                oldest_pending_payout_at: None,
                oldest_pending_payout_age_seconds: None,
                oldest_pending_send_started_at: None,
                oldest_pending_send_age_seconds: None,
                validation_candidate_queue_depth: None,
                validation_regular_queue_depth: None,
                summary_state: "down".to_string(),
                details_json: Some(
                    json!({"reason":"inferred_missing_heartbeat","synthetic":true}).to_string(),
                ),
            });
            ts = ts.checked_add(self.interval).unwrap_or(sampled_at);
            written = written.saturating_add(1);
        }
        if heartbeats.is_empty() {
            return Ok(());
        }
        self.run_store_task("backfill local monitor heartbeat gap", move |store| {
            for heartbeat in &heartbeats {
                store.upsert_monitor_heartbeat(heartbeat)?;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn flush_spool(&self) -> Result<()> {
        let path = self.spool_path.clone();
        if !path.exists() {
            return Ok(());
        }

        let file = fs::File::open(&path)
            .with_context(|| format!("open monitor spool {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut heartbeats = Vec::new();
        for line in reader.lines() {
            let line = line.with_context(|| format!("read monitor spool {}", path.display()))?;
            if line.trim().is_empty() {
                continue;
            }
            let heartbeat: MonitorHeartbeatUpsert =
                serde_json::from_str(&line).context("decode monitor spool entry")?;
            heartbeats.push(heartbeat);
        }
        if !heartbeats.is_empty() {
            self.run_store_task("flush monitor spool heartbeats", move |store| {
                for heartbeat in &heartbeats {
                    store.upsert_monitor_heartbeat(heartbeat)?;
                }
                Ok(())
            })
            .await?;
        }
        fs::remove_file(&path)
            .with_context(|| format!("remove monitor spool {}", path.display()))?;
        Ok(())
    }

    async fn prune_old_heartbeats(&self, now: SystemTime) -> Result<()> {
        {
            let mut last = self.last_housekeeping_at.lock();
            if last.as_ref().is_some_and(|prev| {
                now.duration_since(*prev).unwrap_or_default() < HOUSEKEEPING_INTERVAL
            }) {
                return Ok(());
            }
            *last = Some(now);
        }
        let cutoff = now
            .checked_sub(DEFAULT_HEARTBEAT_RETENTION)
            .unwrap_or(UNIX_EPOCH);
        let incident_cutoff = now
            .checked_sub(DEFAULT_INCIDENT_RETENTION)
            .unwrap_or(UNIX_EPOCH);
        self.run_store_task("prune old monitor heartbeats and incidents", move |store| {
            store.delete_monitor_heartbeats_before(cutoff)?;
            store.delete_resolved_monitor_incidents_before(incident_cutoff)?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn probe_api(&self) -> ProbeOutcome<()> {
        let url = self.local_api_url("/api/info");
        let client = self.api_client.clone();
        match tokio::task::spawn_blocking(move || -> Result<()> {
            let response = client
                .get(&url)
                .send()
                .with_context(|| format!("GET {url}"))?;
            if !response.status().is_success() {
                return Err(anyhow!("GET {url} returned HTTP {}", response.status()));
            }
            Ok(())
        })
        .await
        {
            Ok(Ok(())) => ProbeOutcome::ok(()),
            Ok(Err(err)) => ProbeOutcome::err(err.to_string()),
            Err(err) => ProbeOutcome::err(format!("join error: {err}")),
        }
    }

    async fn load_api_performance(&self) -> Option<ApiPerformanceSnapshot> {
        let url = self.local_api_url("/api/admin/perf");
        let api_key = self.cfg.api_key.trim().to_string();
        let client = self.api_client.clone();
        match tokio::task::spawn_blocking(move || -> Result<Option<ApiPerformanceSnapshot>> {
            let mut request = client.get(&url);
            if !api_key.is_empty() {
                request = request.header("x-api-key", api_key);
            }
            let response = request.send().with_context(|| format!("GET {url}"))?;
            let status = response.status();
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || status == reqwest::StatusCode::UNAUTHORIZED
            {
                return Ok(None);
            }
            if !status.is_success() {
                return Err(anyhow!("GET {url} returned HTTP {status}"));
            }
            Ok(Some(response.json()?))
        })
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "failed loading api performance snapshot");
                None
            }
            Err(err) => {
                tracing::warn!(error = %err, "api performance snapshot task join failed");
                None
            }
        }
    }

    async fn probe_stratum(&self) -> ProbeOutcome<()> {
        let target = format!("{}:{}", self.cfg.stratum_host, self.cfg.stratum_port);
        match tokio::time::timeout(
            LOCAL_STRATUM_PROBE_TIMEOUT,
            tokio::net::TcpStream::connect(&target),
        )
        .await
        {
            Ok(Ok(_)) => ProbeOutcome::ok(()),
            Ok(Err(err)) => ProbeOutcome::err(format!("{target}: {err}")),
            Err(_) => ProbeOutcome::err(format!("{target}: timed out")),
        }
    }

    async fn probe_database(&self) -> ProbeOutcome<()> {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || store.get_meta(LIVE_RUNTIME_SNAPSHOT_META_KEY))
            .await
        {
            Ok(Ok(_)) => ProbeOutcome::ok(()),
            Ok(Err(err)) => ProbeOutcome::err(err.to_string()),
            Err(err) => ProbeOutcome::err(format!("join error: {err}")),
        }
    }

    async fn probe_daemon(&self) -> ProbeOutcome<NodeStatus> {
        let node = Arc::clone(&self.node);
        match tokio::time::timeout(
            LOCAL_DAEMON_PROBE_TIMEOUT,
            tokio::task::spawn_blocking(move || node.get_status()),
        )
        .await
        {
            Ok(Ok(Ok(status))) => ProbeOutcome::ok(status),
            Ok(Ok(Err(err))) => ProbeOutcome::err(err.to_string()),
            Ok(Err(err)) => ProbeOutcome::err(format!("join error: {err}")),
            Err(_) => ProbeOutcome::err("daemon probe timed out"),
        }
    }

    async fn probe_wallet(&self) -> ProbeOutcome<()> {
        let node = Arc::clone(&self.node);
        match tokio::time::timeout(
            LOCAL_WALLET_PROBE_TIMEOUT,
            tokio::task::spawn_blocking(move || node.get_wallet_balance()),
        )
        .await
        {
            Ok(Ok(Ok(_))) => ProbeOutcome::ok(()),
            Ok(Ok(Err(err))) => ProbeOutcome::err(err.to_string()),
            Ok(Err(err)) => ProbeOutcome::err(format!("join error: {err}")),
            Err(_) => ProbeOutcome::err("wallet probe timed out"),
        }
    }

    async fn probe_reference_heights(&self) -> Vec<ReferenceHeightSample> {
        let mut tasks = Vec::new();
        for url in &self.cfg.seed_status_urls {
            let client = self.external_client.clone();
            let url = url.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                fetch_public_chain_height(client, format!("seed:{}", source_host_label(&url)), url)
            }));
        }

        {
            let client = self.external_client.clone();
            let url = self.cfg.pool_status_url.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                fetch_pool_chain_height(client, format!("pool:{}", source_host_label(&url)), url)
            }));
        }
        {
            let client = self.external_client.clone();
            let url = self.cfg.explorer_status_url.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                fetch_public_chain_height(
                    client,
                    format!("explorer:{}", source_host_label(&url)),
                    url,
                )
            }));
        }

        let mut out = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(sample) => out.push(sample),
                Err(err) => out.push(ReferenceHeightSample::err(
                    "reference:join".to_string(),
                    format!("join error: {err}"),
                )),
            }
        }
        out.sort_by(|a, b| a.source.cmp(&b.source));
        out
    }

    async fn notify_chain_divergence(&self, transition: ChainDivergenceTransition, detail: String) {
        let Some(notifier) = self.discord_notifier.clone() else {
            return;
        };
        let client = self.external_client.clone();
        let threshold = self.cfg.height_divergence_threshold;
        let send_result = tokio::task::spawn_blocking(move || {
            post_chain_divergence_discord(client, notifier, transition, threshold, &detail)
        })
        .await;
        match send_result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::warn!(error = %err, "failed sending discord divergence alert"),
            Err(err) => tracing::warn!(error = %err, "discord divergence alert task join failed"),
        }
    }

    async fn load_runtime_snapshot(&self) -> Option<PersistedRuntimeSnapshot> {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || -> Result<Option<PersistedRuntimeSnapshot>> {
            let Some(raw) = store.get_meta(LIVE_RUNTIME_SNAPSHOT_META_KEY)? else {
                return Ok(None);
            };
            Ok(Some(serde_json::from_slice(&raw)?))
        })
        .await
        {
            Ok(Ok(value)) => value,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "failed loading persisted runtime snapshot in monitor");
                None
            }
            Err(err) => {
                tracing::warn!(error = %err, "runtime snapshot monitor task join failed");
                None
            }
        }
    }

    async fn load_process_metrics(&self) -> BTreeMap<String, ProcessMetrics> {
        tokio::task::spawn_blocking(collect_process_metrics)
            .await
            .unwrap_or_default()
    }

    async fn load_pending_payouts(&self) -> Option<PendingPayoutSummary> {
        let store = Arc::clone(&self.store);
        match tokio::task::spawn_blocking(move || -> Result<PendingPayoutSummary> {
            let pending = store.get_pending_payouts()?;
            let oldest_pending_at = pending.iter().map(|payout| payout.initiated_at).min();
            let oldest_send_started_at = pending
                .iter()
                .filter_map(|payout| payout.send_started_at)
                .min();
            Ok(PendingPayoutSummary {
                count: pending.len() as u64,
                amount: pending
                    .iter()
                    .fold(0u64, |acc, payout| acc.saturating_add(payout.amount)),
                oldest_pending_at,
                oldest_send_started_at,
            })
        })
        .await
        {
            Ok(Ok(value)) => Some(value),
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "failed loading pending payouts in monitor");
                None
            }
            Err(err) => {
                tracing::warn!(error = %err, "pending payout monitor task join failed");
                None
            }
        }
    }

    fn local_api_url(&self, path: &str) -> String {
        let scheme = if self.cfg.api_tls_cert_path.trim().is_empty()
            || self.cfg.api_tls_key_path.trim().is_empty()
        {
            "http"
        } else {
            "https"
        };
        format!(
            "{scheme}://{}:{}{path}",
            self.cfg.api_host, self.cfg.api_port
        )
    }
}

async fn handle_metrics(State(state): State<MonitorHttpState>) -> impl IntoResponse {
    let snapshot = state.snapshot.read().clone();
    let body = render_metrics(&snapshot);
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

async fn handle_healthz(State(state): State<MonitorHttpState>) -> impl IntoResponse {
    let snapshot = state.snapshot.read().clone();
    let is_fresh = snapshot.updated_at.is_some_and(|updated| {
        SystemTime::now()
            .duration_since(updated)
            .unwrap_or_default()
            <= state.interval.saturating_mul(2)
    });
    let status = if is_fresh {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "updated_at": snapshot.updated_at,
            "summary_state": snapshot.summary_state,
            "fresh": is_fresh,
        })),
    )
}

async fn handle_snapshot(State(state): State<MonitorHttpState>) -> impl IntoResponse {
    Json(state.snapshot.read().clone())
}

fn build_http_client(timeout: Duration, accept_invalid_certs: bool) -> Result<BlockingClient> {
    BlockingClient::builder()
        .timeout(timeout)
        .danger_accept_invalid_certs(accept_invalid_certs)
        .build()
        .context("build monitor http client")
}

fn fetch_public_chain_height(
    client: BlockingClient,
    source: String,
    url: String,
) -> ReferenceHeightSample {
    let request_url = url.clone();
    let response = (|| -> Result<ReferenceHeightSample> {
        let response = client
            .get(&request_url)
            .send()
            .with_context(|| format!("GET {request_url}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("GET {request_url} returned HTTP {status}"));
        }
        let payload: PublicChainStatusResponse = response.json()?;
        Ok(ReferenceHeightSample::ok(
            source.clone(),
            payload.chain_height,
        ))
    })();
    response.unwrap_or_else(|err| ReferenceHeightSample::err(source, err.to_string()))
}

fn fetch_pool_chain_height(
    client: BlockingClient,
    source: String,
    url: String,
) -> ReferenceHeightSample {
    let request_url = url.clone();
    let response = (|| -> Result<ReferenceHeightSample> {
        let response = client
            .get(&request_url)
            .send()
            .with_context(|| format!("GET {request_url}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("GET {request_url} returned HTTP {status}"));
        }
        let payload: PoolStatusResponse = response.json()?;
        let height = payload
            .daemon
            .chain_height
            .ok_or_else(|| anyhow!("GET {request_url} returned no daemon chain height"))?;
        Ok(ReferenceHeightSample::ok(source.clone(), height))
    })();
    response.unwrap_or_else(|err| ReferenceHeightSample::err(source, err.to_string()))
}

fn source_host_label(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_string))
        .unwrap_or_else(|| url.to_string())
}

fn reference_height_spread(samples: &[ReferenceHeightSample]) -> Option<u64> {
    let min_height = samples.iter().filter_map(|item| item.height).min()?;
    let max_height = samples.iter().filter_map(|item| item.height).max()?;
    Some(max_height.saturating_sub(min_height))
}

fn reference_height_divergence_state(
    samples: &[ReferenceHeightSample],
    threshold: u64,
) -> Option<(&'static str, u64, String)> {
    let spread = reference_height_spread(samples)?;
    if spread <= threshold {
        return None;
    }
    Some((
        "critical",
        spread,
        format_reference_height_detail(samples, spread, threshold),
    ))
}

fn format_reference_height_detail(
    samples: &[ReferenceHeightSample],
    spread: u64,
    threshold: u64,
) -> String {
    let mut observed = samples
        .iter()
        .filter_map(|item| {
            item.height
                .map(|height| format!("{}={height}", item.source))
        })
        .collect::<Vec<_>>();
    observed.sort();
    let mut detail = format!(
        "height spread is {spread} blocks (threshold {threshold}): {}",
        observed.join(", ")
    );
    let unavailable = samples
        .iter()
        .filter_map(|item| {
            item.error
                .as_ref()
                .map(|error| format!("{} ({error})", item.source))
        })
        .collect::<Vec<_>>();
    if !unavailable.is_empty() {
        detail.push_str("; unavailable: ");
        detail.push_str(&unavailable.join(", "));
    }
    detail
}

fn post_chain_divergence_discord(
    client: BlockingClient,
    notifier: DiscordNotifier,
    transition: ChainDivergenceTransition,
    threshold: u64,
    detail: &str,
) -> Result<()> {
    let mention = if matches!(transition, ChainDivergenceTransition::Opened) {
        notifier
            .mention_user_id
            .as_ref()
            .map(|user_id| format!("<@{user_id}> "))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let headline = match transition {
        ChainDivergenceTransition::Opened => {
            format!("{mention}chain height divergence exceeded {threshold} blocks")
        }
        ChainDivergenceTransition::Resolved => {
            format!("chain height divergence recovered within {threshold} blocks")
        }
    };
    let content = format!("{headline}\n{detail}");
    let mut allowed_mentions = json!({ "parse": [] });
    if matches!(transition, ChainDivergenceTransition::Opened) {
        if let Some(user_id) = notifier.mention_user_id.as_ref() {
            allowed_mentions = json!({ "users": [user_id] });
        }
    }
    let response = client
        .post(format!(
            "https://discord.com/api/v10/channels/{}/messages",
            notifier.channel_id
        ))
        .header("Authorization", format!("Bot {}", notifier.bot_token))
        .header(
            reqwest::header::USER_AGENT,
            "blocknet-pool-monitor-chain-divergence/1",
        )
        .json(&json!({
            "content": content,
            "allowed_mentions": allowed_mentions,
        }))
        .send()
        .context("POST discord message")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "POST discord message returned HTTP {}",
            response.status()
        ));
    }
    Ok(())
}

fn collect_process_metrics() -> BTreeMap<String, ProcessMetrics> {
    let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if clock_ticks <= 0 || page_size <= 0 {
        return BTreeMap::new();
    }

    [
        ("api", "blocknet-pool-api"),
        ("stratum", "blocknet-pool-stratum"),
        ("monitor", "blocknet-pool-monitor"),
        ("recoveryd", "blocknet-pool-recoveryd"),
    ]
    .into_iter()
    .filter_map(|(service, needle)| {
        read_process_metrics(needle, clock_ticks as f64, page_size as u64)
            .map(|metrics| (service.to_string(), metrics))
    })
    .collect()
}

fn read_process_metrics(
    command_needle: &str,
    clock_ticks_per_second: f64,
    page_size: u64,
) -> Option<ProcessMetrics> {
    let entries = fs::read_dir("/proc").ok()?;
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        if file_name
            .to_str()
            .is_none_or(|name| !name.bytes().all(|ch| ch.is_ascii_digit()))
        {
            continue;
        }

        let proc_path = entry.path();
        let cmdline_raw = fs::read(proc_path.join("cmdline")).ok()?;
        if cmdline_raw.is_empty() {
            continue;
        }
        let cmdline = cmdline_raw
            .split(|byte| *byte == 0)
            .filter(|segment| !segment.is_empty())
            .map(String::from_utf8_lossy)
            .collect::<Vec<_>>()
            .join(" ");
        if !cmdline.contains(command_needle) {
            continue;
        }

        let stat_raw = fs::read_to_string(proc_path.join("stat")).ok()?;
        let (user_cpu_seconds, system_cpu_seconds, thread_count, rss_bytes) =
            parse_proc_stat_metrics(&stat_raw, clock_ticks_per_second, page_size)?;
        return Some(ProcessMetrics {
            user_cpu_seconds: Some(user_cpu_seconds),
            system_cpu_seconds: Some(system_cpu_seconds),
            rss_bytes: Some(rss_bytes),
            thread_count: Some(thread_count),
        });
    }
    None
}

fn parse_proc_stat_metrics(
    stat_raw: &str,
    clock_ticks_per_second: f64,
    page_size: u64,
) -> Option<(f64, f64, u64, u64)> {
    let close_paren = stat_raw.rfind(')')?;
    let fields = stat_raw
        .get(close_paren + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    if fields.len() <= 21 {
        return None;
    }

    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    let thread_count = fields.get(17)?.parse::<u64>().ok()?;
    let rss_pages = fields.get(21)?.parse::<i64>().ok()?.max(0) as u64;
    Some((
        user_ticks as f64 / clock_ticks_per_second,
        system_ticks as f64 / clock_ticks_per_second,
        thread_count,
        rss_pages.saturating_mul(page_size),
    ))
}

fn summarize_state(
    api_up: bool,
    stratum_up: bool,
    db_up: bool,
    daemon_ready: bool,
    template_refresh_millis: Option<u64>,
    daemon_slow_block_severity: Option<&'static str>,
    pool_activity_loss_severity: Option<&'static str>,
    validation_backlog_severity: Option<&'static str>,
    payout_queue_severity: Option<&'static str>,
    share_ingest_severity: Option<&'static str>,
    reference_height_divergence_severity: Option<&'static str>,
    wallet_ready: bool,
) -> String {
    if !api_up || !stratum_up || !db_up || !daemon_ready {
        return "down".to_string();
    }
    if !wallet_ready {
        return "degraded".to_string();
    }
    if template_refresh_millis
        .is_some_and(|lag| lag >= TEMPLATE_REFRESH_WARN_AFTER.as_millis() as u64)
        || daemon_slow_block_severity.is_some()
        || pool_activity_loss_severity.is_some()
        || validation_backlog_severity.is_some()
        || payout_queue_severity.is_some()
        || share_ingest_severity.is_some()
        || reference_height_divergence_severity.is_some()
    {
        return "degraded".to_string();
    }
    "healthy".to_string()
}

fn format_millis(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", ms as f64 / 1_000.0)
    }
}

fn system_time_from_unix_millis(unix_millis: i64) -> Option<SystemTime> {
    if unix_millis <= 0 {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::from_millis(unix_millis as u64))
}

fn daemon_slow_block_state(
    now: SystemTime,
    status: Option<&NodeStatus>,
) -> Option<(&'static str, String)> {
    let status = status?;

    if let Some(current) = status.current_process_block.as_ref() {
        if current.elapsed_millis >= DAEMON_SLOW_BLOCK_WARN_AFTER.as_millis() as u64 {
            let severity =
                if current.elapsed_millis >= DAEMON_SLOW_BLOCK_CRITICAL_AFTER.as_millis() as u64 {
                    "critical"
                } else {
                    "warn"
                };
            return Some((
                severity,
                format!(
                    "daemon processing block height={} stage={} txs={} elapsed={} stage_elapsed={}",
                    current.height,
                    current.stage,
                    current.tx_count,
                    format_millis(current.elapsed_millis),
                    format_millis(current.stage_elapsed_millis),
                ),
            ));
        }
    }

    let last = status.last_process_block.as_ref()?;
    if last.total_millis < DAEMON_SLOW_BLOCK_WARN_AFTER.as_millis() as u64 {
        return None;
    }
    let completed_at = system_time_from_unix_millis(last.completed_at_unix_millis)?;
    if now.duration_since(completed_at).unwrap_or_default() > DAEMON_SLOW_BLOCK_RECENT_AFTER {
        return None;
    }

    let severity = if last.total_millis >= DAEMON_SLOW_BLOCK_CRITICAL_AFTER.as_millis() as u64 {
        "critical"
    } else {
        "warn"
    };
    let mut detail = format!(
        "daemon processed block height={} txs={} total={} validate={} commit={} reorg={}",
        last.height,
        last.tx_count,
        format_millis(last.total_millis),
        format_millis(last.validate_millis),
        format_millis(last.commit_millis),
        format_millis(last.reorg_millis),
    );
    if !last.error.trim().is_empty() {
        detail.push_str(&format!(" error={}", last.error.trim()));
    }
    Some((severity, detail))
}

fn runtime_snapshot_age(
    sampled_at: SystemTime,
    snapshot: Option<&PersistedRuntimeSnapshot>,
) -> Option<Duration> {
    snapshot.and_then(|snapshot| sampled_at.duration_since(snapshot.sampled_at).ok())
}

fn pool_activity_loss_threshold(interval: Duration) -> Duration {
    interval
        .saturating_mul(POOL_ACTIVITY_LOSS_THRESHOLD_SAMPLES)
        .max(POOL_ACTIVITY_LOSS_MIN_THRESHOLD)
}

fn validation_backlog_state(
    snapshot: Option<&PersistedRuntimeSnapshot>,
    max_queue: u64,
) -> Option<(&'static str, String)> {
    let snapshot = snapshot?;
    let candidate = snapshot.validation.candidate_queue_depth as u64;
    let regular = snapshot.validation.regular_queue_depth as u64;
    let cap = max_queue.max(1);
    let max_depth = candidate.max(regular);
    let combined = candidate.saturating_add(regular);
    if max_depth < (cap * 7) / 10 && combined < cap {
        return None;
    }
    let severity = if max_depth >= (cap * 9) / 10 || combined >= cap.saturating_mul(3) / 2 {
        "critical"
    } else {
        "warn"
    };
    Some((
        severity,
        format!(
            "validation backlog candidate={} regular={} cap={}",
            candidate, regular, cap
        ),
    ))
}

fn payout_queue_state(
    cfg: &Config,
    now: SystemTime,
    summary: Option<&PendingPayoutSummary>,
) -> Option<(&'static str, String)> {
    let summary = summary?;
    if summary.count == 0 {
        return None;
    }

    let queue_warn = cfg
        .payout_interval_duration()
        .saturating_mul(2)
        .max(Duration::from_secs(2 * 60 * 60));
    let queue_critical = cfg
        .payout_interval_duration()
        .saturating_mul(6)
        .max(Duration::from_secs(6 * 60 * 60));
    let send_warn = Duration::from_secs(30 * 60);
    let send_critical = Duration::from_secs(2 * 60 * 60);

    let oldest_pending_age = summary
        .oldest_pending_at
        .and_then(|ts| now.duration_since(ts).ok());
    let oldest_send_age = summary
        .oldest_send_started_at
        .and_then(|ts| now.duration_since(ts).ok());
    let active = oldest_pending_age.is_some_and(|age| age >= queue_warn)
        || oldest_send_age.is_some_and(|age| age >= send_warn);
    if !active {
        return None;
    }

    let severity = if oldest_pending_age.is_some_and(|age| age >= queue_critical)
        || oldest_send_age.is_some_and(|age| age >= send_critical)
    {
        "critical"
    } else {
        "warn"
    };
    Some((
        severity,
        format!(
            "payout queue pending_count={} pending_amount={} oldest_pending_age_seconds={} oldest_send_age_seconds={}",
            summary.count,
            summary.amount,
            oldest_pending_age.map(|age| age.as_secs()).unwrap_or_default(),
            oldest_send_age.map(|age| age.as_secs()).unwrap_or_default(),
        ),
    ))
}

fn share_progress_state(
    now: SystemTime,
    snapshot: Option<&PersistedRuntimeSnapshot>,
) -> Option<(&'static str, String)> {
    let snapshot = snapshot?;
    if snapshot.connected_miners == 0 || snapshot.estimated_hashrate <= 0.0 {
        return None;
    }
    let last_share_age = snapshot
        .last_share_at
        .and_then(|last| now.duration_since(last).ok())?;
    let warn_after = Duration::from_secs(15 * 60);
    let critical_after = Duration::from_secs(30 * 60);
    if last_share_age < warn_after {
        return None;
    }
    let severity = if last_share_age >= critical_after {
        "critical"
    } else {
        "warn"
    };
    Some((
        severity,
        format!(
            "miners connected but no accepted share progress for {}s (miners={}, workers={})",
            last_share_age.as_secs(),
            snapshot.connected_miners,
            snapshot.connected_workers,
        ),
    ))
}

fn pool_activity_loss_state(
    now: SystemTime,
    snapshot: Option<&PersistedRuntimeSnapshot>,
    snapshot_stale_after: Duration,
) -> Option<(&'static str, String)> {
    let assessment = assess_pool_activity(now, snapshot, snapshot_stale_after);
    if assessment.state != "collapsed" {
        return None;
    }
    Some(("critical", assessment.detail))
}

fn load_dotenv(config_path: &Path) {
    let candidates = [
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".env"),
        PathBuf::from(".env"),
    ];

    for candidate in candidates {
        if dotenvy::from_path(&candidate).is_ok() {
            tracing::info!(path = %candidate.display(), "loaded environment");
            return;
        }
    }
}

fn append_spool_entry(path: &Path, heartbeat: &MonitorHeartbeatUpsert) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create monitor spool dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open monitor spool {}", path.display()))?;
    serde_json::to_writer(&mut file, heartbeat).context("encode monitor spool entry")?;
    file.write_all(b"\n")
        .with_context(|| format!("write monitor spool {}", path.display()))?;
    Ok(())
}

fn render_metrics(snapshot: &MonitorSnapshot) -> String {
    let mut out = String::new();
    metric_line(
        &mut out,
        "blocknet_pool_monitor_api_up",
        bool_gauge(snapshot.api_up.unwrap_or(false)),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_stratum_up",
        bool_gauge(snapshot.stratum_up.unwrap_or(false)),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_db_up",
        bool_gauge(snapshot.db_up.unwrap_or(false)),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_up",
        bool_gauge(snapshot.daemon_up.unwrap_or(false)),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_syncing",
        bool_gauge(snapshot.daemon_syncing.unwrap_or(false)),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_process_block_active",
        bool_gauge(snapshot.daemon_current_process_block.is_some()),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_process_block_elapsed_millis",
        snapshot
            .daemon_current_process_block
            .as_ref()
            .map(|block| block.elapsed_millis)
            .unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_process_block_stage_elapsed_millis",
        snapshot
            .daemon_current_process_block
            .as_ref()
            .map(|block| block.stage_elapsed_millis)
            .unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_last_process_block_total_millis",
        snapshot
            .daemon_last_process_block
            .as_ref()
            .map(|block| block.total_millis)
            .unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_last_process_block_validate_millis",
        snapshot
            .daemon_last_process_block
            .as_ref()
            .map(|block| block.validate_millis)
            .unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_last_process_block_commit_millis",
        snapshot
            .daemon_last_process_block
            .as_ref()
            .map(|block| block.commit_millis)
            .unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_daemon_last_process_block_reorg_millis",
        snapshot
            .daemon_last_process_block
            .as_ref()
            .map(|block| block.reorg_millis)
            .unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_wallet_up",
        bool_gauge(snapshot.wallet_up.unwrap_or(false)),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_open_public_incidents",
        snapshot.open_public_incidents,
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_open_private_incidents",
        snapshot.open_private_incidents,
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_chain_height",
        snapshot.chain_height.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_reference_height_spread_blocks",
        snapshot.reference_height_spread.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_reference_height_min",
        snapshot.reference_height_min.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_reference_height_max",
        snapshot.reference_height_max.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_template_age_seconds",
        snapshot.template_age_seconds.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_last_refresh_millis",
        snapshot.last_refresh_millis.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_stratum_snapshot_age_seconds",
        snapshot.stratum_snapshot_age_seconds.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_connected_miners",
        snapshot.connected_miners.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_connected_workers",
        snapshot.connected_workers.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_last_accepted_share_age_seconds",
        snapshot.last_accepted_share_age_seconds.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_payout_pending_count",
        snapshot.payout_pending_count.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_payout_pending_amount",
        snapshot.payout_pending_amount.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_oldest_pending_payout_age_seconds",
        snapshot.oldest_pending_payout_age_seconds.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_oldest_pending_send_age_seconds",
        snapshot.oldest_pending_send_age_seconds.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_validation_candidate_queue_depth",
        snapshot.validation_candidate_queue_depth.unwrap_or(0),
    );
    metric_line(
        &mut out,
        "blocknet_pool_monitor_validation_regular_queue_depth",
        snapshot.validation_regular_queue_depth.unwrap_or(0),
    );
    for (service, metrics) in &snapshot.process_metrics {
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_process_user_cpu_seconds",
            &[("service", service)],
            metrics.user_cpu_seconds.unwrap_or(0.0),
        );
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_process_system_cpu_seconds",
            &[("service", service)],
            metrics.system_cpu_seconds.unwrap_or(0.0),
        );
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_process_rss_bytes",
            &[("service", service)],
            metrics.rss_bytes.unwrap_or(0),
        );
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_process_threads",
            &[("service", service)],
            metrics.thread_count.unwrap_or(0),
        );
    }
    for (source, height) in &snapshot.reference_heights {
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_reference_height",
            &[("source", source)],
            height,
        );
    }
    for (source, up) in &snapshot.reference_source_up {
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_reference_source_up",
            &[("source", source)],
            bool_gauge(*up),
        );
    }
    render_timed_operation_metrics(
        &mut out,
        "blocknet_pool_monitor_api_route",
        "route",
        &snapshot.api_performance.routes,
    );
    render_timed_operation_metrics(
        &mut out,
        "blocknet_pool_monitor_api_operation",
        "operation",
        &snapshot.api_performance.operations,
    );
    render_timed_operation_metrics(
        &mut out,
        "blocknet_pool_monitor_api_task",
        "task",
        &snapshot.api_performance.tasks,
    );
    for (cache, stats) in &snapshot.api_performance.caches {
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_api_cache_hits",
            &[("cache", cache)],
            stats.hits,
        );
        metric_line_labeled(
            &mut out,
            "blocknet_pool_monitor_api_cache_misses",
            &[("cache", cache)],
            stats.misses,
        );
    }
    render_timed_operation_metrics(
        &mut out,
        "blocknet_pool_monitor_stratum_task",
        "task",
        &snapshot.stratum_runtime_tasks,
    );
    if let Some(hashrate) = snapshot.estimated_hashrate {
        out.push_str(&format!(
            "blocknet_pool_monitor_estimated_hashrate {}\n",
            hashrate
        ));
    }
    if let Some(updated_at) = snapshot.updated_at {
        let updated = updated_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        metric_line(
            &mut out,
            "blocknet_pool_monitor_last_sample_timestamp_seconds",
            updated,
        );
    }
    let state_value = match snapshot.summary_state.as_str() {
        "healthy" => 0,
        "degraded" => 1,
        "down" => 2,
        _ => 3,
    };
    metric_line(&mut out, "blocknet_pool_monitor_summary_state", state_value);
    out
}

fn bool_gauge(value: bool) -> u8 {
    u8::from(value)
}

fn metric_line<T>(buf: &mut String, name: &str, value: T)
where
    T: std::fmt::Display,
{
    buf.push_str(name);
    buf.push(' ');
    buf.push_str(&value.to_string());
    buf.push('\n');
}

fn metric_line_labeled<T>(buf: &mut String, name: &str, labels: &[(&str, &str)], value: T)
where
    T: std::fmt::Display,
{
    buf.push_str(name);
    if !labels.is_empty() {
        buf.push('{');
        for (idx, (key, value)) in labels.iter().enumerate() {
            if idx > 0 {
                buf.push(',');
            }
            buf.push_str(key);
            buf.push_str("=\"");
            buf.push_str(&escape_metric_label_value(value));
            buf.push('"');
        }
        buf.push('}');
    }
    buf.push(' ');
    buf.push_str(&value.to_string());
    buf.push('\n');
}

fn render_timed_operation_metrics(
    out: &mut String,
    prefix: &str,
    label_name: &str,
    stats_by_name: &BTreeMap<String, TimedOperationSummary>,
) {
    for (name, stats) in stats_by_name {
        let labels: [(&str, &str); 1] = [(label_name, name.as_str())];
        metric_line_labeled(out, &format!("{prefix}_count"), &labels, stats.count);
        metric_line_labeled(
            out,
            &format!("{prefix}_error_count"),
            &labels,
            stats.error_count,
        );
        metric_line_labeled(
            out,
            &format!("{prefix}_slow_count"),
            &labels,
            stats.slow_count,
        );
        metric_line_labeled(
            out,
            &format!("{prefix}_total_millis"),
            &labels,
            stats.total_millis,
        );
        metric_line_labeled(
            out,
            &format!("{prefix}_max_millis"),
            &labels,
            stats.max_millis,
        );
        metric_line_labeled(
            out,
            &format!("{prefix}_p50_millis"),
            &labels,
            stats.duration.p50_millis.unwrap_or(0),
        );
        metric_line_labeled(
            out,
            &format!("{prefix}_p95_millis"),
            &labels,
            stats.duration.p95_millis.unwrap_or(0),
        );
    }
}

fn escape_metric_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::{
        bool_gauge, daemon_slow_block_state, parse_proc_stat_metrics, payout_queue_state,
        pool_activity_loss_state, reference_height_divergence_state, render_metrics,
        share_progress_state, summarize_state, validation_backlog_state, MonitorSnapshot,
        PendingPayoutSummary, ProcessMetrics, ReferenceHeightSample, STRATUM_SNAPSHOT_STALE_AFTER,
    };
    use crate::config::Config;
    use crate::node::{NodeCurrentProcessBlock, NodeLastProcessBlock, NodeStatus};
    use crate::pool_activity::assess_pool_activity;
    use crate::service_state::{
        PersistedRuntimeSnapshot, PersistedSubmitSummary, PersistedValidationSummary,
    };
    use pool_runtime::jobs::JobRuntimeSnapshot;
    use pool_runtime::telemetry::{
        ApiPerformanceSnapshot, CacheCounterSummary, TimedOperationSummary,
    };
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};

    fn sample_runtime_snapshot() -> PersistedRuntimeSnapshot {
        PersistedRuntimeSnapshot {
            sampled_at: SystemTime::now(),
            total_shares_accepted: 42,
            connected_miners: 2,
            connected_workers: 3,
            estimated_hashrate: 12.5,
            last_share_at: Some(SystemTime::now()),
            jobs: JobRuntimeSnapshot::default(),
            payouts: Default::default(),
            submit: PersistedSubmitSummary::default(),
            validation: PersistedValidationSummary::default(),
            runtime_tasks: BTreeMap::new(),
        }
    }

    #[test]
    fn bool_gauge_uses_numeric_values() {
        assert_eq!(bool_gauge(false), 0);
        assert_eq!(bool_gauge(true), 1);
    }

    #[test]
    fn render_metrics_outputs_numeric_booleans() {
        let snapshot = MonitorSnapshot {
            summary_state: "healthy".to_string(),
            api_up: Some(true),
            wallet_up: Some(false),
            daemon_current_process_block: Some(NodeCurrentProcessBlock {
                height: 5,
                tx_count: 1,
                stage: "validate".to_string(),
                started_at_unix_millis: 1,
                stage_started_at_unix_millis: 1,
                elapsed_millis: 6_000,
                stage_elapsed_millis: 6_000,
            }),
            daemon_last_process_block: Some(NodeLastProcessBlock {
                height: 4,
                tx_count: 1,
                completed_at_unix_millis: 1,
                validate_millis: 4_000,
                commit_millis: 500,
                reorg_millis: 250,
                total_millis: 4_750,
                accepted: true,
                main_chain: true,
                error: String::new(),
            }),
            ..MonitorSnapshot::default()
        };
        let rendered = render_metrics(&snapshot);
        assert!(rendered.contains("blocknet_pool_monitor_api_up 1"));
        assert!(rendered.contains("blocknet_pool_monitor_wallet_up 0"));
        assert!(rendered.contains("blocknet_pool_monitor_daemon_process_block_active 1"));
        assert!(
            rendered.contains("blocknet_pool_monitor_daemon_last_process_block_total_millis 4750")
        );
        assert!(!rendered.contains("true"));
        assert!(!rendered.contains("false"));
    }

    #[test]
    fn render_metrics_outputs_labeled_perf_metrics() {
        let mut snapshot = MonitorSnapshot {
            summary_state: "healthy".to_string(),
            ..MonitorSnapshot::default()
        };
        snapshot.process_metrics.insert(
            "api".to_string(),
            ProcessMetrics {
                user_cpu_seconds: Some(12.5),
                system_cpu_seconds: Some(4.25),
                rss_bytes: Some(9_000),
                thread_count: Some(16),
            },
        );
        snapshot.api_performance = ApiPerformanceSnapshot {
            sampled_at: None,
            routes: BTreeMap::from([(
                "stats".to_string(),
                TimedOperationSummary {
                    count: 5,
                    error_count: 1,
                    slow_count: 2,
                    total_millis: 600,
                    max_millis: 250,
                    duration: Default::default(),
                },
            )]),
            operations: BTreeMap::new(),
            tasks: BTreeMap::new(),
            caches: BTreeMap::from([(
                "stats_response".to_string(),
                CacheCounterSummary { hits: 7, misses: 3 },
            )]),
        };
        snapshot.stratum_runtime_tasks.insert(
            "runtime_snapshot_persist".to_string(),
            TimedOperationSummary {
                count: 4,
                error_count: 0,
                slow_count: 1,
                total_millis: 240,
                max_millis: 120,
                duration: Default::default(),
            },
        );

        let rendered = render_metrics(&snapshot);
        assert!(rendered
            .contains("blocknet_pool_monitor_process_user_cpu_seconds{service=\"api\"} 12.5"));
        assert!(rendered.contains("blocknet_pool_monitor_api_route_count{route=\"stats\"} 5"));
        assert!(
            rendered.contains("blocknet_pool_monitor_api_cache_hits{cache=\"stats_response\"} 7")
        );
        assert!(rendered.contains(
            "blocknet_pool_monitor_stratum_task_count{task=\"runtime_snapshot_persist\"} 4"
        ));
    }

    #[test]
    fn render_metrics_outputs_reference_height_metrics() {
        let mut snapshot = MonitorSnapshot {
            summary_state: "degraded".to_string(),
            reference_height_spread: Some(3),
            reference_height_min: Some(14050),
            reference_height_max: Some(14053),
            ..MonitorSnapshot::default()
        };
        snapshot
            .reference_heights
            .insert("seed:bnt-0.blocknetcrypto.com".to_string(), 14050);
        snapshot
            .reference_source_up
            .insert("seed:bnt-0.blocknetcrypto.com".to_string(), true);

        let rendered = render_metrics(&snapshot);
        assert!(rendered.contains("blocknet_pool_monitor_reference_height_spread_blocks 3"));
        assert!(rendered.contains(
            "blocknet_pool_monitor_reference_height{source=\"seed:bnt-0.blocknetcrypto.com\"} 14050"
        ));
        assert!(rendered.contains(
            "blocknet_pool_monitor_reference_source_up{source=\"seed:bnt-0.blocknetcrypto.com\"} 1"
        ));
    }

    #[test]
    fn reference_height_divergence_detects_spread_above_threshold() {
        let samples = vec![
            ReferenceHeightSample::ok("seed:bnt-0".to_string(), 14050),
            ReferenceHeightSample::ok("pool:bntpool.com".to_string(), 14053),
            ReferenceHeightSample::ok("explorer:explorer.blocknetcrypto.com".to_string(), 14051),
        ];

        let state = reference_height_divergence_state(&samples, 2).expect("divergence");
        assert_eq!(state.0, "critical");
        assert_eq!(state.1, 3);
        assert!(state.2.contains("seed:bnt-0=14050"));
        assert!(state.2.contains("pool:bntpool.com=14053"));
    }

    #[test]
    fn parse_proc_stat_metrics_reads_expected_fields() {
        let stat_raw =
            "4321 (blocknet-pool-api) S 1 2 3 4 5 6 7 8 9 10 200 50 0 0 20 0 4 0 100 4096 32";
        let metrics = parse_proc_stat_metrics(stat_raw, 100.0, 4096).expect("proc stat parse");
        assert_eq!(metrics.0, 2.0);
        assert_eq!(metrics.1, 0.5);
        assert_eq!(metrics.2, 4);
        assert_eq!(metrics.3, 32 * 4096);
    }

    #[test]
    fn validation_backlog_detects_high_depths() {
        let mut snapshot = sample_runtime_snapshot();
        snapshot.validation.candidate_queue_depth = 1_600;
        let state = validation_backlog_state(Some(&snapshot), 2_048).expect("backlog");
        assert_eq!(state.0, "warn");

        snapshot.validation.candidate_queue_depth = 1_900;
        let state = validation_backlog_state(Some(&snapshot), 2_048).expect("critical backlog");
        assert_eq!(state.0, "critical");
    }

    #[test]
    fn share_progress_detects_stalled_shares() {
        let mut snapshot = sample_runtime_snapshot();
        snapshot.last_share_at = Some(SystemTime::now() - Duration::from_secs(20 * 60));
        let state = share_progress_state(SystemTime::now(), Some(&snapshot)).expect("share stall");
        assert_eq!(state.0, "warn");
    }

    #[test]
    fn pool_activity_loss_detects_recent_zeroed_pool() {
        let now = SystemTime::now();
        let mut snapshot = sample_runtime_snapshot();
        snapshot.connected_miners = 0;
        snapshot.connected_workers = 0;
        snapshot.estimated_hashrate = 0.0;
        snapshot.last_share_at = Some(now - Duration::from_secs(30));
        let state = pool_activity_loss_state(now, Some(&snapshot), STRATUM_SNAPSHOT_STALE_AFTER)
            .expect("pool activity loss");
        assert_eq!(state.0, "critical");
        assert!(state.1.contains("miners=0"));
        assert!(state.1.contains("hashrate=0.00"));
    }

    #[test]
    fn pool_activity_loss_ignores_long_idle_pool() {
        let now = SystemTime::now();
        let mut snapshot = sample_runtime_snapshot();
        snapshot.connected_miners = 0;
        snapshot.connected_workers = 0;
        snapshot.estimated_hashrate = 0.0;
        snapshot.last_share_at = Some(now - Duration::from_secs(20 * 60));
        assert!(
            pool_activity_loss_state(now, Some(&snapshot), STRATUM_SNAPSHOT_STALE_AFTER).is_none()
        );
    }

    #[test]
    fn pool_activity_loss_ignores_partial_drop() {
        let now = SystemTime::now();
        let mut snapshot = sample_runtime_snapshot();
        snapshot.connected_miners = 0;
        snapshot.connected_workers = 3;
        snapshot.estimated_hashrate = 12.5;
        snapshot.last_share_at = Some(now - Duration::from_secs(30));
        assert!(
            pool_activity_loss_state(now, Some(&snapshot), STRATUM_SNAPSHOT_STALE_AFTER).is_none()
        );
    }

    #[test]
    fn pool_activity_loss_ignores_stale_snapshot() {
        let now = SystemTime::now();
        let mut snapshot = sample_runtime_snapshot();
        snapshot.sampled_at = now - Duration::from_secs(45);
        snapshot.connected_miners = 0;
        snapshot.connected_workers = 0;
        snapshot.estimated_hashrate = 0.0;
        snapshot.last_share_at = Some(now - Duration::from_secs(10));
        assert!(
            pool_activity_loss_state(now, Some(&snapshot), STRATUM_SNAPSHOT_STALE_AFTER).is_none()
        );
    }

    #[test]
    fn payout_queue_stall_degrades_summary_state() {
        let state = summarize_state(
            true,
            true,
            true,
            true,
            None,
            None,
            None,
            None,
            Some("critical"),
            None,
            None,
            true,
        );
        assert_eq!(state, "degraded");
    }

    #[test]
    fn pool_activity_loss_marks_summary_degraded() {
        let state = summarize_state(
            true,
            true,
            true,
            true,
            None,
            None,
            Some("critical"),
            None,
            None,
            None,
            None,
            true,
        );
        assert_eq!(state, "degraded");
    }

    #[test]
    fn assess_pool_activity_reports_stale_snapshot() {
        let now = SystemTime::now();
        let mut snapshot = sample_runtime_snapshot();
        snapshot.sampled_at = now - Duration::from_secs(45);
        let assessment = assess_pool_activity(now, Some(&snapshot), STRATUM_SNAPSHOT_STALE_AFTER);
        assert_eq!(assessment.state, "stale");
        assert_eq!(assessment.snapshot_age_seconds, Some(45));
    }

    #[test]
    fn daemon_slow_block_state_prefers_active_block() {
        let now = SystemTime::now();
        let status = NodeStatus {
            peer_id: "peer".to_string(),
            peers: 2,
            chain_height: 42,
            best_hash: "abcd".to_string(),
            total_work: 100,
            mempool_size: 0,
            mempool_bytes: 0,
            syncing: false,
            identity_age: "1m".to_string(),
            current_process_block: Some(NodeCurrentProcessBlock {
                height: 43,
                tx_count: 1,
                stage: "validate".to_string(),
                started_at_unix_millis: 1,
                stage_started_at_unix_millis: 1,
                elapsed_millis: 18_000,
                stage_elapsed_millis: 18_000,
            }),
            last_process_block: None,
        };

        let state = daemon_slow_block_state(now, Some(&status)).expect("slow block");
        assert_eq!(state.0, "critical");
        assert!(state.1.contains("stage=validate"));
        assert!(state.1.contains("height=43"));
    }

    #[test]
    fn daemon_slow_block_state_reports_recent_completed_block() {
        let now = SystemTime::now();
        let completed_at = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let status = NodeStatus {
            peer_id: "peer".to_string(),
            peers: 2,
            chain_height: 42,
            best_hash: "abcd".to_string(),
            total_work: 100,
            mempool_size: 0,
            mempool_bytes: 0,
            syncing: false,
            identity_age: "1m".to_string(),
            current_process_block: None,
            last_process_block: Some(NodeLastProcessBlock {
                height: 43,
                tx_count: 2,
                completed_at_unix_millis: completed_at,
                validate_millis: 8_000,
                commit_millis: 900,
                reorg_millis: 100,
                total_millis: 9_000,
                accepted: true,
                main_chain: true,
                error: String::new(),
            }),
        };

        let state = daemon_slow_block_state(now, Some(&status)).expect("recent slow completion");
        assert_eq!(state.0, "warn");
        assert!(state.1.contains("validate=8.0s"));
        assert!(state.1.contains("commit=900ms"));
    }

    #[test]
    fn payout_queue_state_activates_after_cadence_threshold() {
        let cfg = Config::default();
        let now = SystemTime::now();
        let summary = PendingPayoutSummary {
            count: 2,
            amount: 123,
            oldest_pending_at: Some(now - Duration::from_secs(3 * 60 * 60)),
            oldest_send_started_at: None,
        };
        let state = payout_queue_state(&cfg, now, Some(&summary)).expect("queue stall");
        assert_eq!(state.0, "warn");
    }
}
