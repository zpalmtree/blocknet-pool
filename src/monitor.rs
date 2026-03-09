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
use serde::Serialize;
use serde_json::json;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::db::{MonitorHeartbeatUpsert, MonitorIncidentUpsert};
use crate::node::{NodeClient, NodeStatus};
use crate::service_state::{PersistedRuntimeSnapshot, LIVE_RUNTIME_SNAPSHOT_META_KEY};
use crate::store::PoolStore;

const LOCAL_MONITOR_SOURCE: &str = "local";
const LOCAL_API_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const LOCAL_DAEMON_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const LOCAL_STRATUM_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const LOCAL_WALLET_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const DEFAULT_HEARTBEAT_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_INCIDENT_RETENTION: Duration = Duration::from_secs(180 * 24 * 60 * 60);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60 * 60);
const STRATUM_SNAPSHOT_STALE_AFTER: Duration = Duration::from_secs(30);
const TEMPLATE_REFRESH_WARN_AFTER: Duration = Duration::from_secs(45);
const TEMPLATE_REFRESH_CRITICAL_AFTER: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Default, Serialize)]
pub struct MonitorSnapshot {
    pub updated_at: Option<SystemTime>,
    pub summary_state: String,
    pub api_up: Option<bool>,
    pub stratum_up: Option<bool>,
    pub db_up: Option<bool>,
    pub daemon_up: Option<bool>,
    pub daemon_syncing: Option<bool>,
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
    pub open_public_incidents: u64,
    pub open_private_incidents: u64,
    pub last_error: Option<String>,
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
    daemon_down: ConditionState,
    daemon_syncing: ConditionState,
    template_stale: ConditionState,
    validation_backlog: ConditionState,
    payout_queue_stalled: ConditionState,
    wallet_unavailable: ConditionState,
    share_ingest_stalled: ConditionState,
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
    payout_summary: Option<PendingPayoutSummary>,
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
    let store = tokio::task::spawn_blocking(move || PoolStore::open_from_config(&cfg_for_store))
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
    let api_client = build_local_api_client()?;
    let monitor = Arc::new(MonitorRuntime {
        cfg: cfg.clone(),
        store,
        node,
        api_client,
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

        let mut runtime_snapshot = None;
        let mut payout_summary = None;
        if db.error.is_none() {
            runtime_snapshot = self.load_runtime_snapshot().await;
            payout_summary = self.load_pending_payouts().await;
        }

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
        let summary_state = summarize_state(
            api.error.is_none(),
            stratum.error.is_none() && stratum_snapshot_fresh,
            db.error.is_none(),
            daemon.value.as_ref().is_some_and(|status| !status.syncing),
            template_refresh_millis,
            validation_backlog.as_ref().map(|(severity, _)| *severity),
            payout_queue_stalled.as_ref().map(|(severity, _)| *severity),
            share_ingest_stalled.as_ref().map(|(severity, _)| *severity),
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
            payout_summary,
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
        let scheme = if self.cfg.api_tls_cert_path.trim().is_empty()
            || self.cfg.api_tls_key_path.trim().is_empty()
        {
            "http"
        } else {
            "https"
        };
        let url = format!(
            "{scheme}://{}:{}/api/info",
            self.cfg.api_host, self.cfg.api_port
        );
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

fn build_local_api_client() -> Result<BlockingClient> {
    BlockingClient::builder()
        .timeout(LOCAL_API_PROBE_TIMEOUT)
        .danger_accept_invalid_certs(true)
        .build()
        .context("build local monitor http client")
}

fn summarize_state(
    api_up: bool,
    stratum_up: bool,
    db_up: bool,
    daemon_ready: bool,
    template_refresh_millis: Option<u64>,
    validation_backlog_severity: Option<&'static str>,
    payout_queue_severity: Option<&'static str>,
    share_ingest_severity: Option<&'static str>,
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
        || validation_backlog_severity.is_some()
        || payout_queue_severity.is_some()
        || share_ingest_severity.is_some()
    {
        return "degraded".to_string();
    }
    "healthy".to_string()
}

fn runtime_snapshot_age(
    sampled_at: SystemTime,
    snapshot: Option<&PersistedRuntimeSnapshot>,
) -> Option<Duration> {
    snapshot.and_then(|snapshot| sampled_at.duration_since(snapshot.sampled_at).ok())
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

#[cfg(test)]
mod tests {
    use super::{
        bool_gauge, payout_queue_state, render_metrics, share_progress_state, summarize_state,
        validation_backlog_state, MonitorSnapshot, PendingPayoutSummary,
    };
    use crate::config::Config;
    use crate::jobs::JobRuntimeSnapshot;
    use crate::service_state::{PersistedRuntimeSnapshot, PersistedValidationSummary};
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
            validation: PersistedValidationSummary::default(),
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
            ..MonitorSnapshot::default()
        };
        let rendered = render_metrics(&snapshot);
        assert!(rendered.contains("blocknet_pool_monitor_api_up 1"));
        assert!(rendered.contains("blocknet_pool_monitor_wallet_up 0"));
        assert!(!rendered.contains("true"));
        assert!(!rendered.contains("false"));
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
    fn payout_queue_stall_degrades_summary_state() {
        let state = summarize_state(
            true,
            true,
            true,
            true,
            None,
            None,
            Some("critical"),
            None,
            true,
        );
        assert_eq!(state, "degraded");
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
