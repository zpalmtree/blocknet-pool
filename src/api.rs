use axum::body::Body;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use parking_lot::Mutex;
use serde::Serialize;

use crate::db::PoolFeeEvent;
use crate::engine::JobRepository;
use crate::jobs::JobManager;
use crate::node::NodeClient;
use crate::stats::PoolStats;
use crate::store::PoolStore;
use crate::validation::ValidationEngine;

const DB_TOTALS_CACHE_TTL: Duration = Duration::from_secs(2);
const EXPLORER_HASHRATE_SAMPLE_COUNT: usize = 10;
const NETWORK_HASHRATE_CACHE_RETRY_TTL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<PoolStore>,
    pub stats: Arc<PoolStats>,
    pub jobs: Arc<JobManager>,
    pub node: Arc<NodeClient>,
    pub validation: Arc<ValidationEngine>,
    pub db_totals_cache: Arc<Mutex<DbTotalsCache>>,
    pub network_hashrate_cache: Arc<Mutex<NetworkHashrateCache>>,
    pub api_key: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct DbTotals {
    total_shares: u64,
    total_blocks: u64,
    pool_fees_collected: u64,
}

#[derive(Debug, Default)]
pub struct DbTotalsCache {
    updated_at: Option<Instant>,
    totals: DbTotals,
}

#[derive(Debug, Default)]
pub struct NetworkHashrateCache {
    updated_at: Option<Instant>,
    chain_height: Option<u64>,
    difficulty: Option<u64>,
    hashrate_hps: Option<f64>,
}

pub async fn run_api(addr: SocketAddr, state: ApiState) -> anyhow::Result<()> {
    let app_state = state.clone();
    let protected = Router::new()
        .route("/api/miners", get(handle_miners))
        .route("/api/blocks", get(handle_blocks))
        .route("/api/payouts", get(handle_payouts))
        .route("/api/fees", get(handle_fees))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            require_api_key,
        ));

    let app = Router::new()
        .route("/", get(handle_ui))
        .route("/ui", get(handle_ui))
        .route("/api/stats", get(handle_stats))
        .route("/api/miner/:address", get(handle_miner))
        .merge(protected)
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

const UI_INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Blocknet Pool Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@400;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-a: #0e1c2f;
      --bg-b: #113b4f;
      --panel: rgba(7, 17, 34, 0.78);
      --ink: #e8f1ff;
      --muted: #a7bad8;
      --ok: #4dd68a;
      --warn: #ffbc5b;
      --line: rgba(123, 153, 201, 0.33);
      --accent: #4fb7ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      background: radial-gradient(circle at 20% -10%, #1f6f8f 0%, transparent 50%),
                  radial-gradient(circle at 85% 120%, #215d77 0%, transparent 40%),
                  linear-gradient(140deg, var(--bg-a), var(--bg-b));
      min-height: 100vh;
    }
    .wrap {
      width: min(1100px, 92vw);
      margin: 26px auto 42px;
      animation: rise .45s ease-out;
    }
    @keyframes rise {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
    .hero {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 16px;
    }
    h1 {
      margin: 0;
      letter-spacing: .02em;
      font-size: clamp(1.4rem, 2.8vw, 2.2rem);
    }
    .hint {
      color: var(--muted);
      font-family: "JetBrains Mono", monospace;
      font-size: .86rem;
      text-align: right;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    @media (max-width: 900px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    @media (max-width: 560px) {
      .hero { flex-direction: column; align-items: flex-start; }
      .hint { text-align: left; }
      .grid { grid-template-columns: 1fr; }
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px 16px;
      backdrop-filter: blur(4px);
      box-shadow: 0 12px 30px rgba(0,0,0,.23);
    }
    .label {
      color: var(--muted);
      font-size: .78rem;
      text-transform: uppercase;
      letter-spacing: .12em;
      margin-bottom: 6px;
    }
    .value {
      font-size: clamp(1.05rem, 2.5vw, 1.45rem);
      font-weight: 700;
      font-variant-numeric: tabular-nums;
    }
    .mono { font-family: "JetBrains Mono", monospace; }
    .good { color: var(--ok); }
    .warn { color: var(--warn); }
    .pulse {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 999px;
      margin-right: 8px;
      background: var(--ok);
      animation: pulse 1.2s infinite;
      vertical-align: middle;
    }
    @keyframes pulse {
      0% { box-shadow: 0 0 0 0 rgba(77,214,138,.55); }
      100% { box-shadow: 0 0 0 12px rgba(77,214,138,0); }
    }
    .footer {
      margin-top: 16px;
      color: var(--muted);
      font-size: .84rem;
    }
    .err {
      margin-top: 14px;
      color: #ff8f8f;
      font-family: "JetBrains Mono", monospace;
      font-size: .84rem;
      white-space: pre-wrap;
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1><span class="pulse"></span>Blocknet Pool Dashboard</h1>
      <div class="hint">
        live source: <span class="mono">/api/stats</span><br>
        refresh: <span class="mono">2s</span>
      </div>
    </section>

    <section class="grid">
      <article class="card"><div class="label">Connected Miners</div><div id="miners" class="value mono">-</div></article>
      <article class="card"><div class="label">Connected Workers</div><div id="workers" class="value mono">-</div></article>
      <article class="card"><div class="label">Pool Hashrate</div><div id="hashrate" class="value mono">-</div></article>
      <article class="card"><div class="label">Network Hashrate</div><div id="network" class="value mono">-</div></article>
      <article class="card"><div class="label">Shares Accepted</div><div id="accepted" class="value mono good">-</div></article>
      <article class="card"><div class="label">Shares Rejected</div><div id="rejected" class="value mono warn">-</div></article>
      <article class="card"><div class="label">Blocks Found</div><div id="blocks" class="value mono">-</div></article>
      <article class="card"><div class="label">Current Job Height</div><div id="height" class="value mono">-</div></article>
      <article class="card"><div class="label">Validation In Flight</div><div id="inflight" class="value mono">-</div></article>
      <article class="card"><div class="label">Candidate Queue</div><div id="queuec" class="value mono">-</div></article>
      <article class="card"><div class="label">Regular Queue</div><div id="queuer" class="value mono">-</div></article>
      <article class="card"><div class="label">Fraud Detections</div><div id="fraud" class="value mono">-</div></article>
    </section>

    <section class="footer">
      Last updated: <span id="updated" class="mono">never</span>
    </section>
    <section id="err" class="err"></section>
  </main>

  <script>
    const byId = (id) => document.getElementById(id);
    const fmt = new Intl.NumberFormat("en-US");

    function humanRate(value) {
      if (value === null || value === undefined || !Number.isFinite(value)) return "-";
      const units = ["H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s"];
      let v = value;
      let i = 0;
      while (v >= 1000 && i < units.length - 1) {
        v /= 1000;
        i += 1;
      }
      return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
    }

    function setValue(id, value) {
      byId(id).textContent = value;
    }

    async function refresh() {
      try {
        const res = await fetch("/api/stats", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const stats = await res.json();

        setValue("miners", fmt.format(stats.pool.miners || 0));
        setValue("workers", fmt.format(stats.pool.workers || 0));
        setValue("hashrate", humanRate(stats.pool.hashrate));
        setValue("network", humanRate(stats.chain.network_hashrate));
        setValue("accepted", fmt.format(stats.pool.shares_accepted || 0));
        setValue("rejected", fmt.format(stats.pool.shares_rejected || 0));
        setValue("blocks", fmt.format(stats.pool.blocks_found || 0));
        setValue("height", stats.chain.current_job_height ?? "-");
        setValue("inflight", fmt.format(stats.validation.in_flight || 0));
        setValue("queuec", fmt.format(stats.validation.candidate_queue_depth || 0));
        setValue("queuer", fmt.format(stats.validation.regular_queue_depth || 0));
        setValue("fraud", fmt.format(stats.validation.fraud_detections || 0));
        setValue("updated", new Date().toLocaleTimeString());
        byId("err").textContent = "";
      } catch (err) {
        byId("err").textContent = `fetch failed: ${String(err)}`;
      }
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"#;

async fn handle_ui() -> Html<&'static str> {
    Html(UI_INDEX_HTML)
}

#[derive(Serialize)]
struct StatsResponse {
    pool: PoolSummary,
    chain: ChainSummary,
    validation: ValidationSummary,
}

#[derive(Serialize)]
struct PoolSummary {
    miners: usize,
    workers: usize,
    hashrate: f64,
    shares_accepted: u64,
    shares_rejected: u64,
    blocks_found: u64,
    total_shares: u64,
    total_blocks: u64,
    pool_fees_collected: u64,
}

#[derive(Serialize)]
struct ChainSummary {
    current_job_height: Option<u64>,
    network_hashrate: Option<f64>,
}

#[derive(Serialize)]
struct ValidationSummary {
    in_flight: i64,
    candidate_queue_depth: usize,
    regular_queue_depth: usize,
    tracked_addresses: usize,
    forced_verify_addresses: usize,
    total_shares: u64,
    sampled_shares: u64,
    invalid_samples: u64,
    pending_provisional: u64,
    fraud_detections: u64,
}

#[derive(Serialize)]
struct FeesResponse {
    total_collected: u64,
    recent: Vec<PoolFeeEvent>,
}

async fn handle_stats(State(state): State<ApiState>) -> impl IntoResponse {
    let snap = state.stats.snapshot();
    let validation = state.validation.snapshot();
    let totals = match state.db_totals().await {
        Ok(v) => v,
        Err(err) => return internal_error("failed loading pool stats", err).into_response(),
    };
    let current_job = state.jobs.current_job();
    let current_job_height = current_job.as_ref().map(|j| j.height);
    let network_hashrate = state.network_hashrate_for_job(current_job.as_ref()).await;

    let response = StatsResponse {
        pool: PoolSummary {
            miners: snap.connected_miners,
            workers: snap.connected_workers,
            hashrate: snap.estimated_hashrate,
            shares_accepted: snap.total_shares_accepted,
            shares_rejected: snap.total_shares_rejected,
            blocks_found: snap.total_blocks_found,
            total_shares: totals.total_shares,
            total_blocks: totals.total_blocks,
            pool_fees_collected: totals.pool_fees_collected,
        },
        chain: ChainSummary {
            current_job_height,
            network_hashrate,
        },
        validation: ValidationSummary {
            in_flight: validation.in_flight,
            candidate_queue_depth: validation.candidate_queue_depth,
            regular_queue_depth: validation.regular_queue_depth,
            tracked_addresses: validation.tracked_addresses,
            forced_verify_addresses: validation.forced_verify_addresses,
            total_shares: validation.total_shares,
            sampled_shares: validation.sampled_shares,
            invalid_samples: validation.invalid_samples,
            pending_provisional: validation.pending_provisional,
            fraud_detections: validation.fraud_detections,
        },
    };

    Json(response).into_response()
}

async fn handle_miners(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.stats.all_miner_stats())
}

async fn handle_miner(
    Path(address): Path<String>,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    let stats = state.stats.get_miner_stats(&address);
    let store = Arc::clone(&state.store);
    let address_for_query = address.clone();
    let (shares, balance, pending_payout) = match tokio::task::spawn_blocking(move || {
        Ok::<_, anyhow::Error>((
            store.get_shares_for_miner(&address_for_query, 100)?,
            store.get_balance(&address_for_query)?,
            store.get_pending_payout(&address_for_query)?,
        ))
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading miner shares", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading miner shares",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };

    let hashrate = state.stats.estimate_miner_hashrate(&address);
    let balance_json = serde_json::json!({
        "pending": balance.pending,
        "paid": balance.paid,
    });
    let has_activity = miner_has_activity(
        shares.len(),
        balance.pending,
        balance.paid,
        pending_payout.is_some(),
    );

    match stats {
        Some(miner_stats) => Json(serde_json::json!({
            "stats": miner_stats,
            "shares": shares,
            "hashrate": hashrate,
            "balance": balance_json,
            "pending_payout": pending_payout,
        }))
        .into_response(),
        None if has_activity => Json(serde_json::json!({
            "stats": serde_json::Value::Null,
            "shares": shares,
            "hashrate": hashrate,
            "balance": balance_json,
            "pending_payout": pending_payout,
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error":"miner not found",
                "hashrate": hashrate,
                "balance": balance_json,
                "pending_payout": pending_payout,
            })),
        )
            .into_response(),
    }
}

async fn handle_blocks(State(state): State<ApiState>) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let blocks = match tokio::task::spawn_blocking(move || store.get_recent_blocks(100)).await {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading blocks", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading blocks",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    Json(blocks).into_response()
}

async fn handle_payouts(State(state): State<ApiState>) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let payouts = match tokio::task::spawn_blocking(move || store.get_recent_payouts(100)).await {
        Ok(Ok(v)) => v,
        Ok(Err(err)) => return internal_error("failed loading payouts", err).into_response(),
        Err(err) => {
            return internal_error(
                "failed loading payouts",
                anyhow::anyhow!("join error: {err}"),
            )
            .into_response()
        }
    };
    Json(payouts).into_response()
}

async fn handle_fees(State(state): State<ApiState>) -> impl IntoResponse {
    let store = Arc::clone(&state.store);
    let (total_collected, recent) =
        match tokio::task::spawn_blocking(move || -> anyhow::Result<(u64, Vec<PoolFeeEvent>)> {
            Ok((
                store.get_total_pool_fees()?,
                store.get_recent_pool_fees(100)?,
            ))
        })
        .await
        {
            Ok(Ok(v)) => v,
            Ok(Err(err)) => return internal_error("failed loading fees", err).into_response(),
            Err(err) => {
                return internal_error("failed loading fees", anyhow::anyhow!("join error: {err}"))
                    .into_response()
            }
        };

    Json(FeesResponse {
        total_collected,
        recent,
    })
    .into_response()
}

impl ApiState {
    async fn db_totals(&self) -> anyhow::Result<DbTotals> {
        {
            let cache = self.db_totals_cache.lock();
            if cache
                .updated_at
                .is_some_and(|updated| updated.elapsed() < DB_TOTALS_CACHE_TTL)
            {
                return Ok(cache.totals);
            }
        }

        let store = Arc::clone(&self.store);
        let totals = tokio::task::spawn_blocking(move || -> anyhow::Result<DbTotals> {
            Ok(DbTotals {
                total_shares: store.get_total_share_count()?,
                total_blocks: store.get_block_count()?,
                pool_fees_collected: store.get_total_pool_fees()?,
            })
        })
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))??;

        let mut cache = self.db_totals_cache.lock();
        cache.totals = totals;
        cache.updated_at = Some(Instant::now());
        Ok(totals)
    }

    async fn network_hashrate_for_job(&self, job: Option<&crate::engine::Job>) -> Option<f64> {
        let job = job?;
        let chain_height = job.height.checked_sub(1)?;
        let difficulty = job.network_difficulty.max(1);

        {
            let cache = self.network_hashrate_cache.lock();
            let same_key =
                cache.chain_height == Some(chain_height) && cache.difficulty == Some(difficulty);
            if same_key {
                if let Some(value) = cache.hashrate_hps {
                    return Some(value);
                }
                if cache
                    .updated_at
                    .is_some_and(|updated| updated.elapsed() < NETWORK_HASHRATE_CACHE_RETRY_TTL)
                {
                    return None;
                }
            }
        }

        let node = Arc::clone(&self.node);
        let sampled = tokio::task::spawn_blocking(move || {
            estimate_explorer_network_hashrate_hps(node.as_ref(), chain_height, difficulty)
        })
        .await
        .ok()
        .and_then(Result::ok)
        .filter(|value| value.is_finite() && *value >= 0.0);

        let mut cache = self.network_hashrate_cache.lock();
        cache.updated_at = Some(Instant::now());
        cache.chain_height = Some(chain_height);
        cache.difficulty = Some(difficulty);
        cache.hashrate_hps = sampled;
        sampled
    }
}

fn estimate_explorer_network_hashrate_hps(
    node: &NodeClient,
    chain_height: u64,
    difficulty: u64,
) -> anyhow::Result<f64> {
    // Match explorer.go: hashrate = NextDifficulty / avg(last 10 positive block-time deltas).
    if chain_height < 2 {
        return Ok(0.0);
    }

    let mut total_time = 0i64;
    let mut count = 0usize;
    let mut current_ts = node
        .get_block_by_height_optional(chain_height)?
        .map(|block| block.timestamp);
    let mut height = chain_height;

    while height > 0 && count < EXPLORER_HASHRATE_SAMPLE_COUNT {
        let prev_ts = node
            .get_block_by_height_optional(height - 1)?
            .map(|block| block.timestamp);
        if let (Some(block_ts), Some(prev_block_ts)) = (current_ts, prev_ts) {
            let block_time = block_ts - prev_block_ts;
            if block_time > 0 {
                total_time += block_time;
                count += 1;
            }
        }
        current_ts = prev_ts;
        height -= 1;
    }

    if count > 0 && total_time > 0 {
        let avg_block_time = total_time as f64 / count as f64;
        return Ok(difficulty as f64 / avg_block_time);
    }

    Ok(0.0)
}

async fn require_api_key(
    State(state): State<ApiState>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    let expected = state.api_key.trim();
    if expected.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"api key not configured"})),
        )
            .into_response();
    }

    let api_key = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let bearer = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|v| !v.is_empty());

    if api_key == Some(expected) || bearer == Some(expected) {
        return next.run(req).await.into_response();
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"error":"unauthorized"})),
    )
        .into_response()
}

fn miner_has_activity(
    shares_len: usize,
    balance_pending: u64,
    balance_paid: u64,
    has_pending_payout: bool,
) -> bool {
    shares_len > 0 || balance_pending > 0 || balance_paid > 0 || has_pending_payout
}

fn internal_error(msg: &str, err: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    tracing::warn!(error = %err, "{msg}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"error": msg})),
    )
}

#[cfg(test)]
mod tests {
    use super::miner_has_activity;

    #[test]
    fn miner_activity_detects_share_history() {
        assert!(miner_has_activity(1, 0, 0, false));
    }

    #[test]
    fn miner_activity_detects_balance_and_pending() {
        assert!(miner_has_activity(0, 1, 0, false));
        assert!(miner_has_activity(0, 0, 1, false));
        assert!(miner_has_activity(0, 0, 0, true));
        assert!(!miner_has_activity(0, 0, 0, false));
    }
}
