use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use pool_recovery::RecoveryAgentClient;

use crate::api::{
    load_persisted_status_history, ApiState, DaemonHealthCache, DbTotalsCache, InsightsCache,
    MinerBalanceResponseCache, MinerDetailResponseCache, NetworkHashrateCache,
    PendingEstimateSnapshotCache, PoolHealthCache, PublicTelemetryRateLimiter,
    RejectionAnalyticsCache, StatsResponseCache, StatusHistory, DEFAULT_MAX_SSE_SUBSCRIBERS,
};
use crate::config::Config;

pub use pool_runtime::runtime::SharedRuntime;

pub async fn bootstrap_api_runtime(config_path: &Path) -> Result<(Config, SharedRuntime)> {
    pool_runtime::runtime::ensure_runtime_files(config_path)?;
    pool_runtime::runtime::load_dotenv(config_path);
    let cfg = Config::load(config_path)?;
    warn_api_config(&cfg);
    let shared = pool_runtime::runtime::bootstrap_shared_runtime_without_validation_from_config(
        cfg.to_runtime_config(),
    )
    .await?;
    Ok((cfg, shared))
}

pub async fn build_api_state(cfg: &Config, shared: &SharedRuntime) -> Result<ApiState> {
    let persisted_status_history = {
        let store = Arc::clone(&shared.store);
        match tokio::task::spawn_blocking(move || load_persisted_status_history(store.as_ref()))
            .await
            .context("join status history load task")?
        {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(error = %err, "failed loading persisted status history; starting fresh");
                StatusHistory::default()
            }
        }
    };

    Ok(ApiState {
        config: cfg.clone(),
        store: Arc::clone(&shared.store),
        stats: Arc::clone(&shared.stats),
        jobs: Arc::clone(&shared.jobs),
        node: Arc::clone(&shared.node),
        validation: shared.validation.clone(),
        db_totals_cache: Arc::new(Mutex::new(DbTotalsCache::default())),
        daemon_health_cache: Arc::new(Mutex::new(DaemonHealthCache::default())),
        pool_health_cache: Arc::new(Mutex::new(PoolHealthCache::default())),
        network_hashrate_cache: Arc::new(Mutex::new(NetworkHashrateCache::default())),
        insights_cache: Arc::new(Mutex::new(InsightsCache::default())),
        rejection_analytics_cache: Arc::new(Mutex::new(RejectionAnalyticsCache::default())),
        stats_response_cache: Arc::new(Mutex::new(StatsResponseCache::default())),
        pending_estimate_snapshot_cache: Arc::new(Mutex::new(
            PendingEstimateSnapshotCache::default(),
        )),
        pending_estimate_snapshot_notify: Arc::new(tokio::sync::Notify::new()),
        miner_balance_response_cache: Arc::new(Mutex::new(MinerBalanceResponseCache::default())),
        miner_detail_response_cache: Arc::new(Mutex::new(MinerDetailResponseCache::default())),
        public_telemetry_rate_limiter: Arc::new(Mutex::new(PublicTelemetryRateLimiter::default())),
        performance: Arc::new(crate::api::ApiPerformanceTracker::default()),
        recovery: Arc::new(RecoveryAgentClient::new(cfg.recovery.socket_path.clone())),
        live_runtime_snapshot_cache: Arc::new(Mutex::new(
            crate::api::LiveRuntimeSnapshotCache::default(),
        )),
        status_history: Arc::new(Mutex::new(persisted_status_history)),
        sse_subscriber_limiter: Arc::new(tokio::sync::Semaphore::new(DEFAULT_MAX_SSE_SUBSCRIBERS)),
        api_key: cfg.api_key.clone(),
        pool_name: cfg.pool_name.clone(),
        pool_url: cfg.pool_url.clone(),
        stratum_port: cfg.stratum_port,
        pool_fee_pct: cfg.pool_fee_pct,
        pool_fee_flat: cfg.pool_fee_flat,
        min_payout_amount: cfg.min_payout_amount,
        blocks_before_payout: cfg.blocks_before_payout,
        payout_scheme: cfg.payout_scheme.clone(),
        started_at: std::time::Instant::now(),
        started_at_system: SystemTime::now(),
    })
}

pub fn api_listen_addr(cfg: &Config) -> Result<SocketAddr> {
    format!("{}:{}", cfg.api_host, cfg.api_port)
        .parse()
        .with_context(|| {
            format!(
                "invalid api listen address {}:{}",
                cfg.api_host, cfg.api_port
            )
        })
}

pub fn start_api_background_tasks(api_state: ApiState) {
    tokio::spawn(async move {
        api_state.sample_status().await;

        let mut ticker = tokio::time::interval(api_state.config.monitor_interval_duration());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            ticker.tick().await;
            api_state.sample_status().await;
        }
    });
}

fn warn_api_config(cfg: &Config) {
    if cfg.api_key.trim().is_empty() {
        tracing::warn!(
            host = %cfg.api_host,
            port = cfg.api_port,
            "api_key is empty; protected routes remain disabled"
        );
    }
    if !cfg.api_tls_cert_path.trim().is_empty() ^ !cfg.api_tls_key_path.trim().is_empty() {
        tracing::warn!(
            cert_path = %cfg.api_tls_cert_path,
            key_path = %cfg.api_tls_key_path,
            "api tls is partially configured; set both api_tls_cert_path and api_tls_key_path to enable tls"
        );
    }
}
