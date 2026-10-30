use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::config::Config;
use crate::engine::{JobRepository, NodeApi, PoolEngine, ShareStore};
use crate::hashrate::from_stats_with_warmup as hashrate_from_stats_with_warmup;
use crate::jobs::JobManager;
use crate::node::NodeClient;
use crate::payout::PayoutProcessor;
use crate::service_state::{EngineRole, PersistedRuntimeSnapshot, LIVE_RUNTIME_SNAPSHOT_META_KEY};
use crate::stats::PoolStats;
use crate::store::PoolStore;
use crate::stratum::StratumServer;
use crate::telemetry::NamedTimedOperationTracker;
use crate::validation::{ValidationEngine, ValidationStateStore, CANDIDATE_VALIDATION_WORKERS};
use pool_common::pow::ConsensusPowHasher;
use pool_common::protocol::{address_network, validate_miner_address, AddressNetwork};

const HASHRATE_WARMUP_WINDOW: Duration = Duration::from_secs(5 * 60);
const RUNTIME_TASK_SLOW_LOG_AFTER: Duration = Duration::from_millis(250);
const SEEN_SHARE_GC_INTERVAL: Duration = Duration::from_secs(10 * 60);
const RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
const SHARE_PARTITION_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60 * 60);
const GROWTH_MODE_META_KEY: &str = "growth_mode_v1";
const GROWTH_MODE_TICK: Duration = Duration::from_secs(30);
const HEAP_TRIM_INTERVAL: Duration = Duration::from_secs(60);
const SHARES_RETENTION: Duration = Duration::from_secs(90 * 24 * 60 * 60);
const PAYOUTS_RETENTION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

pub struct SharedRuntime {
    pub cfg: Config,
    pub(crate) store: Arc<PoolStore>,
    pub(crate) node: Arc<NodeClient>,
    pub(crate) expected_address_network: Option<AddressNetwork>,
    pub(crate) jobs: Arc<JobManager>,
    pub(crate) validation: Arc<ValidationEngine>,
    pub(crate) stats: Arc<PoolStats>,
    /// Behind the gateway this starts non-primary until the gateway says so.
    pub role: Arc<EngineRole>,
}

pub struct ApiRuntime {
    pub store: Arc<PoolStore>,
    pub node: Arc<NodeClient>,
    pub jobs: Arc<JobManager>,
}

struct RuntimeCore {
    store: Arc<PoolStore>,
    node: Arc<NodeClient>,
    jobs: Arc<JobManager>,
}

pub async fn bootstrap_shared_runtime(config_path: &Path) -> Result<SharedRuntime> {
    load_dotenv(config_path);
    let cfg = Config::load(config_path)?;

    validate_pool_fee_destination_config(&cfg)?;
    info!(
        "vardiff init={} min={} max={} target_shares={} retarget={}",
        cfg.initial_share_difficulty,
        cfg.min_share_difficulty,
        cfg.max_share_difficulty,
        cfg.vardiff_target_shares,
        cfg.vardiff_retarget_interval
    );
    if cfg.gateway_socket.trim().is_empty() && !is_local_bind_host(&cfg.stratum_host) {
        warn!(
            host = %cfg.stratum_host,
            port = cfg.stratum_port,
            "stratum is bound on a non-local host and transport is plaintext; place stratum behind a tls terminator when exposed publicly"
        );
    }
    warn_on_validation_visibility_config(&cfg);

    // Only the Stratum process runs this bootstrap; its submit workers and
    // background tasks share one pool, sized separately from the API's.
    let core = bootstrap_runtime_core(&cfg, cfg.stratum_database_pool_size()).await?;
    core.store.set_async_share_commit(cfg.share_async_commit);
    let expected_address_network =
        resolve_expected_address_network(&cfg, Arc::clone(&core.node)).await;
    let validation_cfg = cfg.clone();
    let validation_store = Arc::clone(&core.store) as Arc<dyn ValidationStateStore>;
    // One arena per dedicated verifier thread (candidate, regular, audit).
    // With the default four, fluctuating verifier concurrency mapped and
    // unmapped a 2 GiB arena ~20 times a minute; each munmap takes the
    // process-wide mmap lock and stalls page faults on every thread, which
    // pushed hot-accept p99 from ~50 ms to ~200 ms in production.
    let retained_arenas =
        CANDIDATE_VALIDATION_WORKERS + cfg.regular_verifier_count() + cfg.audit_verifier_count();
    let validation = tokio::task::spawn_blocking(move || {
        Arc::new(ValidationEngine::new_with_state_store(
            validation_cfg,
            Arc::new(ConsensusPowHasher::with_arena_limit(retained_arenas)),
            validation_store,
        ))
    })
    .await
    .context("join validation engine init task")?;

    let role = Arc::new(EngineRole::new(cfg.gateway_socket.trim().is_empty()));
    Ok(SharedRuntime {
        cfg,
        store: core.store,
        node: core.node,
        expected_address_network,
        jobs: core.jobs,
        validation,
        stats: Arc::new(PoolStats::default()),
        role,
    })
}

pub async fn bootstrap_api_runtime_from_config(cfg: Config) -> Result<ApiRuntime> {
    validate_pool_fee_destination_config(&cfg)?;
    let core = bootstrap_runtime_core(&cfg, cfg.database_pool_size).await?;
    Ok(ApiRuntime {
        store: core.store,
        node: core.node,
        jobs: core.jobs,
    })
}

async fn bootstrap_runtime_core(cfg: &Config, database_pool_size: i32) -> Result<RuntimeCore> {
    let database_url = cfg.database_url.clone();
    let store =
        tokio::task::spawn_blocking(move || PoolStore::open(&database_url, database_pool_size))
            .await
            .context("join store initialization task")??;
    let daemon_api = cfg.daemon_api.clone();
    let daemon_cookie_path = cfg.daemon_cookie_path.clone();
    let node =
        tokio::task::spawn_blocking(move || NodeClient::new(&daemon_api, &daemon_cookie_path))
            .await
            .context("join node client init task")??;
    let node = Arc::new(node);

    let node_for_probe = Arc::clone(&node);
    if let Err(err) = tokio::task::spawn_blocking(move || node_for_probe.get_status())
        .await
        .context("join node startup probe task")?
    {
        warn!(error = %err, "cannot reach daemon on startup; continuing");
    }
    let jobs = JobManager::new(Arc::clone(&node), cfg.clone());
    jobs.start();

    Ok(RuntimeCore { store, node, jobs })
}

pub async fn build_engine(shared: &SharedRuntime) -> Result<Arc<PoolEngine>> {
    let cfg = shared.cfg.clone();
    let expected_address_network = shared.expected_address_network;
    let validation = Arc::clone(&shared.validation);
    let jobs = Arc::clone(&shared.jobs) as Arc<dyn JobRepository>;
    let store = Arc::clone(&shared.store) as Arc<dyn ShareStore>;
    let node = Arc::clone(&shared.node) as Arc<dyn NodeApi>;

    tokio::task::spawn_blocking(move || {
        Arc::new(PoolEngine::new_with_expected_address_network(
            cfg,
            expected_address_network,
            validation,
            jobs,
            store,
            node,
        ))
    })
    .await
    .context("join engine init task")
}

pub fn build_stratum_server(
    shared: &SharedRuntime,
    engine: Arc<PoolEngine>,
) -> Result<Arc<StratumServer>> {
    let addr = stratum_listen_addr(&shared.cfg)?;
    Ok(StratumServer::new(
        addr,
        engine,
        Arc::clone(&shared.jobs),
        Arc::clone(&shared.stats),
        shared.cfg.clone(),
    ))
}

fn stratum_listen_addr(cfg: &Config) -> Result<SocketAddr> {
    format!("{}:{}", cfg.stratum_host, cfg.stratum_port)
        .parse()
        .with_context(|| {
            format!(
                "invalid stratum listen address {}:{}",
                cfg.stratum_host, cfg.stratum_port
            )
        })
}

/// Re-reads `config_path` on SIGHUP (`systemctl reload`) and applies its hot
/// settings (see `Config::with_hot_settings_from`) to the running engine,
/// without disconnecting miners. An invalid file changes nothing; settings
/// that need a restart are reported, not applied. Also replaces SIGHUP's
/// default action, which would terminate Stratum.
pub fn start_config_reload_on_hangup(config_path: PathBuf, engine: Arc<PoolEngine>) {
    use tokio::signal::unix::{signal, SignalKind};
    tokio::spawn(async move {
        let mut hangups = match signal(SignalKind::hangup()) {
            Ok(hangups) => hangups,
            Err(err) => {
                warn!(error = %err, "cannot listen for SIGHUP; config reload disabled");
                return;
            }
        };
        while hangups.recv().await.is_some() {
            let path = config_path.clone();
            let engine = Arc::clone(&engine);
            let reloaded =
                tokio::task::spawn_blocking(move || reload_hot_config(&path, &engine)).await;
            if let Ok(Err(err)) = reloaded {
                warn!(error = %format!("{err:#}"), "config reload rejected; running settings unchanged");
            }
        }
    });
}

/// Loads and validates `config_path`, then applies its hot settings to
/// `engine`. Returns the names of the settings that changed.
pub fn reload_hot_config(config_path: &Path, engine: &PoolEngine) -> Result<Vec<&'static str>> {
    let next = Config::load(config_path)?;
    let changed = engine.reload_hot_config(&next);
    if changed.is_empty() {
        info!("config reloaded; no hot settings changed");
    } else {
        info!(changed = ?changed, "config reloaded; hot settings applied");
    }
    if engine.base_cfg() != next {
        warn!("config file also changes settings that only apply after a Stratum restart");
    }
    Ok(changed)
}

/// Handle to Stratum's background work, for a clean exit.
pub struct StratumBackground {
    role: Arc<EngineRole>,
    payout: Arc<PayoutProcessor>,
}

impl StratumBackground {
    /// Stops singleton work and waits (bounded) for an in-flight payout pass,
    /// so the process never exits in the middle of sending a payout.
    pub async fn quiesce(&self, max_wait: Duration) {
        self.role.set_primary(false);
        let deadline = Instant::now() + max_wait;
        while self.payout.tick_running() {
            if Instant::now() >= deadline {
                warn!("exiting with a payout pass still running");
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

pub fn start_stratum_background_tasks(
    shared: &SharedRuntime,
    engine: Arc<PoolEngine>,
    stratum: Arc<StratumServer>,
) -> StratumBackground {
    let validation = Arc::clone(&shared.validation);
    let role = Arc::clone(&shared.role);
    let task_metrics = Arc::new(NamedTimedOperationTracker::default());
    start_growth_mode(
        Arc::clone(&engine),
        Arc::clone(&stratum),
        Arc::clone(&shared.store),
        Arc::clone(&role),
    );
    start_found_block_recovery(engine, Arc::clone(&task_metrics));
    start_heap_trim(Arc::clone(&task_metrics));
    let payout = PayoutProcessor::new_with_task_metrics(
        shared.cfg.clone(),
        Arc::clone(&shared.store),
        Arc::clone(&shared.node),
        Some(Arc::clone(&task_metrics)),
    );
    payout.start(Arc::clone(&role));
    start_seen_share_gc(
        Arc::clone(&shared.store),
        Arc::clone(&role),
        Arc::clone(&task_metrics),
    );
    start_stat_snapshots(
        Arc::clone(&shared.stats),
        Arc::clone(&shared.store),
        Arc::clone(&role),
        Arc::clone(&task_metrics),
    );
    start_retention_maintenance(
        Arc::clone(&shared.store),
        Arc::clone(&role),
        Arc::clone(&task_metrics),
    );
    start_share_partition_maintenance(
        Arc::clone(&shared.store),
        Arc::clone(&role),
        Arc::clone(&task_metrics),
    );
    start_live_runtime_snapshot_persist(
        Arc::clone(&shared.jobs),
        Arc::clone(&payout),
        Arc::clone(&shared.stats),
        stratum,
        validation,
        Arc::clone(&shared.store),
        Arc::clone(&role),
        task_metrics,
    );
    StratumBackground { role, payout }
}

/// Resumes the persisted growth mode, then lets the primary engine switch it
/// from the measured share rate (see `crate::growth`).
fn start_growth_mode(
    engine: Arc<PoolEngine>,
    stratum: Arc<StratumServer>,
    store: Arc<PoolStore>,
    role: Arc<EngineRole>,
) {
    tokio::spawn(async move {
        let load_store = Arc::clone(&store);
        let persisted =
            tokio::task::spawn_blocking(move || load_store.get_meta(GROWTH_MODE_META_KEY)).await;
        // Stored as "0", "1" or "2" (engines before level 2 read only "1").
        let level = match persisted {
            Ok(Ok(Some(value))) => match value.as_slice() {
                b"1" => 1,
                b"2" => 2,
                _ => 0,
            },
            Ok(Ok(None)) => 0,
            Ok(Err(err)) => {
                warn!(error = %err, "failed loading growth mode; starting without it");
                0
            }
            Err(err) => {
                warn!(error = %err, "growth mode load task failed");
                0
            }
        };
        if level > 0 {
            engine.set_growth_level(level);
        }
        let mut controller = crate::growth::GrowthController::new(level, Instant::now());
        let mut ticker = tokio::time::interval(GROWTH_MODE_TICK);
        loop {
            ticker.tick().await;
            if !role.is_primary() {
                continue;
            }
            let total = stratum.submit_snapshot().submits_received_total;
            let switched = controller.observe(Instant::now(), total, &engine.base_cfg());
            if let Some(rate) = controller.normal_size_rate() {
                engine.record_growth_rate(rate);
            }
            let Some(level) = switched else {
                continue;
            };
            engine.set_growth_level(level);
            let store = Arc::clone(&store);
            let value: &'static [u8] = match level {
                0 => b"0",
                1 => b"1",
                _ => b"2",
            };
            match tokio::task::spawn_blocking(move || store.set_meta(GROWTH_MODE_META_KEY, value))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!(error = %err, "failed persisting growth mode"),
                Err(err) => warn!(error = %err, "growth mode persist task failed"),
            }
        }
    });
}

/// Hands freed heap memory back to the OS every `HEAP_TRIM_INTERVAL` (see
/// `stratum::trim_heap`).
fn start_heap_trim(task_metrics: Arc<NamedTimedOperationTracker>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(HEAP_TRIM_INTERVAL);
        loop {
            ticker.tick().await;
            let started_at = Instant::now();
            let failed = tokio::task::spawn_blocking(crate::stratum::trim_heap)
                .await
                .is_err();
            record_runtime_task_observation(
                &task_metrics,
                "heap_trim",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

fn start_seen_share_gc(
    store: Arc<PoolStore>,
    role: Arc<EngineRole>,
    task_metrics: Arc<NamedTimedOperationTracker>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SEEN_SHARE_GC_INTERVAL);
        loop {
            ticker.tick().await;
            if !role.is_primary() {
                continue;
            }
            let started_at = Instant::now();
            let store = Arc::clone(&store);
            let failed = match tokio::task::spawn_blocking(move || {
                store.clean_expired_seen_shares()
            })
            .await
            {
                Ok(Ok(removed)) if removed > 0 => {
                    tracing::debug!(removed, "cleaned expired seen-share entries");
                    false
                }
                Ok(Ok(_)) => false,
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "seen-share cleanup failed");
                    true
                }
                Err(err) => {
                    tracing::warn!(error = %err, "seen-share cleanup task join failed");
                    true
                }
            };
            record_runtime_task_observation(
                &task_metrics,
                "seen_share_gc",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

fn start_stat_snapshots(
    stats: Arc<PoolStats>,
    store: Arc<PoolStore>,
    role: Arc<EngineRole>,
    task_metrics: Arc<NamedTimedOperationTracker>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5 * 60));
        let retain = Duration::from_secs(60 * 24 * 60 * 60);
        let hr_window = Duration::from_secs(60 * 60);
        loop {
            ticker.tick().await;
            if !role.is_primary() {
                continue;
            }
            let started_at = Instant::now();
            let snap = stats.snapshot();
            let store = Arc::clone(&store);
            let failed = match tokio::task::spawn_blocking(move || {
                let hashrate = db_pool_hashrate(&store, hr_window);
                store.add_stat_snapshot(
                    SystemTime::now(),
                    hashrate,
                    snap.connected_miners as i32,
                    snap.connected_workers as i32,
                )?;
                store.clean_old_snapshots(retain)?;
                Ok::<_, anyhow::Error>(())
            })
            .await
            {
                Ok(Ok(())) => false,
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "stat snapshot failed");
                    true
                }
                Err(err) => {
                    tracing::warn!(error = %err, "stat snapshot task join failed");
                    true
                }
            };
            record_runtime_task_observation(
                &task_metrics,
                "stat_snapshot",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

fn start_retention_maintenance(
    store: Arc<PoolStore>,
    role: Arc<EngineRole>,
    task_metrics: Arc<NamedTimedOperationTracker>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + RETENTION_INTERVAL,
            RETENTION_INTERVAL,
        );
        loop {
            ticker.tick().await;
            if !role.is_primary() {
                continue;
            }
            let now = SystemTime::now();
            let shares_before = now.checked_sub(SHARES_RETENTION);
            let payouts_before = now.checked_sub(PAYOUTS_RETENTION);

            let started_at = Instant::now();
            let store = Arc::clone(&store);
            let failed = match tokio::task::spawn_blocking(move || {
                store.rollup_and_prune_retention(shares_before, payouts_before)
            })
            .await
            {
                Ok(Ok(report)) => {
                    if report.shares_pruned > 0 || report.payouts_pruned > 0 {
                        tracing::info!(
                            shares_pruned = report.shares_pruned,
                            payouts_pruned = report.payouts_pruned,
                            "completed retention rollup/prune cycle"
                        );
                    }
                    false
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        error = %err,
                        error_chain = %format!("{err:#}"),
                        "retention rollup/prune failed"
                    );
                    true
                }
                Err(err) => {
                    tracing::warn!(error = %err, "retention task join failed");
                    true
                }
            };
            record_runtime_task_observation(
                &task_metrics,
                "retention_maintenance",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

/// Keeps the coming weeks' share partitions created. A failure, or rows in the
/// DEFAULT partition, counts as a failed `share_partition_maintenance` task.
fn start_share_partition_maintenance(
    store: Arc<PoolStore>,
    role: Arc<EngineRole>,
    task_metrics: Arc<NamedTimedOperationTracker>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SHARE_PARTITION_MAINTENANCE_INTERVAL);
        loop {
            ticker.tick().await;
            if !role.is_primary() {
                continue;
            }
            let started_at = Instant::now();
            let store = Arc::clone(&store);
            let failed = match tokio::task::spawn_blocking(move || {
                store.maintain_share_partitions()
            })
            .await
            {
                Ok(Ok(report)) => report.default_rows > 0,
                Ok(Err(err)) => {
                    tracing::warn!(
                        error = %err,
                        error_chain = %format!("{err:#}"),
                        "share partition maintenance failed"
                    );
                    true
                }
                Err(err) => {
                    tracing::warn!(error = %err, "share partition maintenance task join failed");
                    true
                }
            };
            record_runtime_task_observation(
                &task_metrics,
                "share_partition_maintenance",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

fn start_found_block_recovery(
    engine: Arc<PoolEngine>,
    task_metrics: Arc<NamedTimedOperationTracker>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            let started_at = Instant::now();
            let engine = Arc::clone(&engine);
            let failed = match tokio::task::spawn_blocking(move || {
                engine.recover_found_block_outbox()
            })
            .await
            {
                Ok(()) => false,
                Err(err) => {
                    tracing::warn!(error = %err, "found-block recovery task join failed");
                    true
                }
            };
            record_runtime_task_observation(
                &task_metrics,
                "found_block_recovery",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn start_live_runtime_snapshot_persist(
    jobs: Arc<JobManager>,
    payouts: Arc<PayoutProcessor>,
    stats: Arc<PoolStats>,
    stratum: Arc<StratumServer>,
    validation: Arc<ValidationEngine>,
    store: Arc<PoolStore>,
    role: Arc<EngineRole>,
    task_metrics: Arc<NamedTimedOperationTracker>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;
            // The API and monitor read one shared snapshot; only the primary
            // engine writes it.
            if !role.is_primary() {
                continue;
            }
            let started_at = Instant::now();
            let payload = PersistedRuntimeSnapshot::from_live(
                stats.snapshot(),
                stratum.submit_snapshot(),
                validation.snapshot(),
                jobs.runtime_snapshot(),
                payouts.runtime_snapshot(),
                task_metrics.snapshot(),
                stratum
                    .latency_histograms()
                    .into_iter()
                    .chain(validation.latency_histograms())
                    .collect(),
            );
            let store = Arc::clone(&store);
            let failed = match tokio::task::spawn_blocking(move || -> Result<()> {
                let bytes = serde_json::to_vec(&payload)?;
                store.set_meta(LIVE_RUNTIME_SNAPSHOT_META_KEY, &bytes)?;
                Ok(())
            })
            .await
            {
                Ok(Ok(())) => false,
                Ok(Err(err)) => {
                    tracing::warn!(error = %err, "failed persisting live runtime snapshot");
                    true
                }
                Err(err) => {
                    tracing::warn!(error = %err, "live runtime snapshot task join failed");
                    true
                }
            };
            record_runtime_task_observation(
                &task_metrics,
                "runtime_snapshot_persist",
                started_at.elapsed(),
                failed,
            );
        }
    });
}

fn record_runtime_task_observation(
    task_metrics: &NamedTimedOperationTracker,
    task: &str,
    duration: Duration,
    failed: bool,
) {
    let slow = duration >= RUNTIME_TASK_SLOW_LOG_AFTER;
    task_metrics.record(task, duration, failed, slow);
    if failed {
        tracing::warn!(
            component = "runtime_perf",
            operation = task,
            duration_ms = duration.as_millis() as u64,
            "stratum runtime task failed"
        );
    } else if slow {
        tracing::info!(
            component = "runtime_perf",
            operation = task,
            duration_ms = duration.as_millis() as u64,
            "stratum runtime task observed"
        );
    }
}

fn db_pool_hashrate(store: &PoolStore, window: Duration) -> f64 {
    let since = SystemTime::now()
        .checked_sub(window)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let Ok((total_diff, count, oldest, newest)) = store.hashrate_stats_pool(since) else {
        return 0.0;
    };
    hashrate_from_stats_with_warmup(
        total_diff,
        count,
        oldest,
        newest,
        window,
        HASHRATE_WARMUP_WINDOW,
    )
}

pub fn load_dotenv(config_path: &Path) {
    let candidates = [
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(".env"),
        std::path::PathBuf::from(".env"),
    ];

    for candidate in candidates {
        if dotenvy::from_path(&candidate).is_ok() {
            info!(path = %candidate.display(), "loaded environment");
            return;
        }
    }
}

fn warn_on_validation_visibility_config(cfg: &Config) {
    if cfg.validation_mode == "full" {
        return;
    }

    let periodic_floor = if cfg.min_sample_every > 0 {
        1.0 / cfg.min_sample_every as f64
    } else {
        0.0
    };
    let typical_verified_ratio = cfg.sample_rate.max(periodic_floor).clamp(0.0, 1.0);

    if cfg.payout_provisional_cap_multiplier > 0.0 {
        let full_credit_verified_ratio =
            1.0 / (1.0 + cfg.payout_provisional_cap_multiplier.max(0.0));
        if full_credit_verified_ratio > typical_verified_ratio + f64::EPSILON {
            warn!(
                sample_rate = cfg.sample_rate,
                min_sample_every = cfg.min_sample_every,
                warmup_shares = cfg.warmup_shares,
                payout_provisional_cap_multiplier = cfg.payout_provisional_cap_multiplier,
                full_credit_verified_ratio,
                typical_verified_ratio,
                "sampler coverage is below the provisional cap's full-credit target; honest miners may see reduced payout weight until more shares are fully verified"
            );
        }
    }
}

fn is_local_bind_host(host: &str) -> bool {
    let trimmed = host.trim().to_ascii_lowercase();
    matches!(trimmed.as_str(), "127.0.0.1" | "::1" | "localhost")
}

fn validate_pool_fee_destination_config(cfg: &Config) -> Result<()> {
    if cfg.pool_fee_pct <= 0.0 {
        return Ok(());
    }

    let destination = cfg.pool_fee_wallet_address.trim();
    if destination.is_empty() {
        anyhow::bail!("pool_fee_wallet_address must be set when pool_fee_pct is non-zero");
    }

    validate_miner_address(destination)
        .map_err(|err| anyhow::anyhow!("pool_fee_wallet_address is invalid: {err}"))?;
    Ok(())
}

async fn resolve_expected_address_network(
    cfg: &Config,
    node: Arc<NodeClient>,
) -> Option<AddressNetwork> {
    match cfg.pool_fee_wallet_address_network() {
        Ok(Some(network)) => return Some(network),
        Ok(None) => {}
        Err(err) => {
            tracing::warn!(
                error = %err,
                "pool_fee_wallet_address could not be parsed for pool network detection"
            );
        }
    }

    let node_for_wallet = Arc::clone(&node);
    let wallet_address = match tokio::task::spawn_blocking(move || {
        node_for_wallet.get_wallet_address()
    })
    .await
    {
        Ok(Ok(wallet)) => wallet.address,
        Ok(Err(err)) => {
            tracing::debug!(error = %err, "wallet address unavailable for pool network detection");
            return None;
        }
        Err(err) => {
            tracing::warn!(error = %err, "wallet address network detection task join failed");
            return None;
        }
    };

    match address_network(wallet_address.trim()) {
        Ok(network) => network,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "daemon wallet address could not be parsed for pool network detection"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate_pool_fee_destination_config;
    use crate::config::Config;

    #[test]
    fn pool_fee_destination_is_optional_when_fee_is_disabled() {
        let cfg = Config::default();
        assert!(validate_pool_fee_destination_config(&cfg).is_ok());
    }

    #[test]
    fn pool_fee_destination_is_required_when_fee_is_enabled() {
        let cfg = Config {
            pool_fee_pct: 1.0,
            ..Config::default()
        };
        let err = validate_pool_fee_destination_config(&cfg).expect_err("missing destination");
        assert!(err
            .to_string()
            .contains("pool_fee_wallet_address must be set"));
    }

    #[test]
    fn pool_fee_destination_must_be_a_valid_address() {
        let cfg = Config {
            pool_fee_pct: 1.0,
            pool_fee_wallet_address: "not-an-address".to_string(),
            ..Config::default()
        };
        let err = validate_pool_fee_destination_config(&cfg).expect_err("invalid destination");
        assert!(err
            .to_string()
            .contains("pool_fee_wallet_address is invalid"));
    }

    #[test]
    fn valid_pool_fee_destination_is_accepted() {
        let cfg = Config {
            pool_fee_pct: 1.0,
            pool_fee_wallet_address: "Kffdvk1q1YPCGKz8gzJoymedZucxqJtxsw5ZUMJrWrVUK4jCXChXmTBrMADRh9EzeMNNPunXrz3dyg9nMwdZPaNc7FB7v".to_string(),
            ..Config::default()
        };
        assert!(validate_pool_fee_destination_config(&cfg).is_ok());
    }
}
