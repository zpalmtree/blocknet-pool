use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use parking_lot::{Mutex, MutexGuard};
use postgres::{types::ToSql, Client, Config as PostgresConfig, NoTls, Row, Transaction};
use serde::Deserialize;
use tracing::warn;

use crate::engine::{ShareRecord, SubmitRisk};
use crate::share_dedupe::TEMPLATE_SHARE_CLAIM_RETENTION;
use crate::stats::RejectionReasonCount;
use crate::validation::{
    LoadedValidationState, PersistedValidationAcceptedShare, PersistedValidationAddressState,
    PersistedValidationProvisional, ValidationClearEvent,
};
use pool_common::db::{
    active_window_strikes, quarantine_until_for_strikes, ActiveVerificationHold,
    AddressRiskEscalation, AddressRiskState, Balance, BlockCreditEvent, DbBlock, DbLuckRound,
    DbShare, MonitorHeartbeat, MonitorHeartbeatUpsert, MonitorIncident, MonitorIncidentUpsert,
    Payout, PendingPayout, PendingPayoutBatchMember, PoolFeeEvent, PoolFeeRecord,
    PublicPayoutBatch, ShareReplayData, ShareReplayUpdate, ValidationHoldCause,
    ValidationHoldState,
};

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

static READ_PERFORMANCE: OnceLock<crate::telemetry::NamedTimedOperationTracker> = OnceLock::new();

thread_local! {
    /// Statement timeout requested by the current thread (see
    /// [`with_statement_timeout`]). `None` keeps the server/session default.
    static STATEMENT_TIMEOUT_SCOPE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Run `op` with a Postgres `statement_timeout` applied to every pooled
/// connection it acquires on this thread. Connections are switched lazily (one
/// extra `SET` only when a connection's current setting differs), so callers
/// outside a scope, such as admin and background work, keep the server default.
pub fn with_statement_timeout<T>(timeout: Option<Duration>, op: impl FnOnce() -> T) -> T {
    struct Restore(Option<u64>);
    impl Drop for Restore {
        fn drop(&mut self) {
            STATEMENT_TIMEOUT_SCOPE.with(|scope| scope.set(self.0));
        }
    }
    let millis = timeout
        .map(|value| u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
        .filter(|value| *value > 0);
    let _restore = Restore(STATEMENT_TIMEOUT_SCOPE.with(|scope| scope.replace(millis)));
    op()
}

#[track_caller]
fn observe_read<T>(operation: impl FnOnce() -> Result<T>) -> Result<T> {
    // Source locations are bounded by the compiled queries. Never record SQL
    // parameters, addresses, credentials, or dynamically constructed SQL.
    let caller = std::panic::Location::caller();
    let started = Instant::now();
    let result = operation();
    let duration = started.elapsed();
    READ_PERFORMANCE.get_or_init(Default::default).record(
        &format!("{}:{}", caller.file(), caller.line()),
        duration,
        result.is_err(),
        duration >= Duration::from_millis(200),
    );
    result
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShareWindowAddressPreview {
    pub address: String,
    pub seen_shares: u64,
    pub verified_shares: u64,
    pub verified_difficulty: u64,
    pub provisional_shares_ready: u64,
    pub provisional_difficulty_ready: u64,
    pub provisional_shares_delayed: u64,
}

pub type WorkerStats = Vec<(String, u64, u64, u64, i64)>;
pub type WorkerHashrateStats = Vec<(String, u64, u64, Option<SystemTime>, Option<SystemTime>)>;
pub type WorkerVersions = Vec<(String, String, Option<String>)>;
pub struct MinerWorkerSnapshot {
    pub totals: WorkerStats,
    pub hashrate: WorkerHashrateStats,
    pub versions: WorkerVersions,
}
pub type MinerLifetimeCounts = HashMap<String, (u64, u64, u64, Option<i64>)>;

pub struct MinerWindowStats {
    pub worker_count: usize,
    pub total_difficulty: u64,
    pub accepted_count: u64,
    pub oldest: Option<SystemTime>,
    pub newest: Option<SystemTime>,
}

#[derive(Debug, Clone, Default)]
pub struct MonitorUptimeSummary {
    pub sample_count: u64,
    pub api_total: u64,
    pub api_up: u64,
    pub stratum_total: u64,
    pub stratum_up: u64,
    pub pool_total: u64,
    pub pool_up: u64,
    pub daemon_total: u64,
    pub daemon_up: u64,
    pub database_total: u64,
    pub database_up: u64,
    pub public_http_total: u64,
    pub public_http_up: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OrphanBlockReconciliation {
    pub orphaned: bool,
    pub reversed_credit_events: u64,
    pub reversed_credit_amount: u64,
    pub reversed_fee_amount: u64,
    pub canceled_pending_payouts: u64,
    pub manual_reconciliation_required: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExistingOrphanBlockReconciliationReport {
    pub blocks_scanned: u64,
    pub blocks_reconciled: u64,
    pub blocks_requiring_manual_reconciliation: u64,
    pub reversed_credit_events: u64,
    pub reversed_credit_amount: u64,
    pub reversed_fee_amount: u64,
    pub canceled_pending_payouts: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CompletedPayoutReconciliation {
    pub reverted_payout_rows: u64,
    pub restored_pending_amount: u64,
    pub dropped_orphaned_amount: u64,
    pub manual_reconciliation_required: bool,
}

#[derive(Debug, Clone)]
pub struct ConfirmedPayoutImportRecipient {
    pub address: String,
    pub amount: u64,
    pub fee: u64,
}

#[derive(Debug, Clone)]
pub struct ConfirmedPayoutImportTx {
    pub tx_hash: String,
    pub timestamp: SystemTime,
    pub recipients: Vec<ConfirmedPayoutImportRecipient>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConfirmedPayoutImportReport {
    pub imported_txs: u64,
    pub imported_payout_rows: u64,
    pub imported_amount: u64,
    pub imported_fee: u64,
    pub canceled_pending_payouts: u64,
    pub recorded_manual_offset_amount: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ManualPayoutOffsetApplication {
    pub address: String,
    pub applied_amount: u64,
    pub remaining_offset_amount: u64,
    pub remaining_balance_pending: u64,
    pub remaining_canonical_pending: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ManualPayoutOffsetApplyReport {
    pub scanned_offset_addresses: u64,
    pub offset_amount_before: u64,
    pub applied_address_count: u64,
    pub applied_amount: u64,
    pub remaining_offset_amount: u64,
    pub applications: Vec<ManualPayoutOffsetApplication>,
}

#[derive(Debug, Clone, Default)]
pub struct OrphanedBlockCreditIssue {
    pub height: u64,
    pub hash: String,
    pub credit_event_count: u64,
    pub credited_address_count: u64,
    pub remaining_credit_amount: u64,
    pub paid_credit_amount: u64,
    pub remaining_fee_amount: u64,
    pub paid_fee_amount: u64,
    pub pending_payout_count: u64,
    pub broadcast_pending_payout_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BalanceSourceSummary {
    pub address: String,
    pub canonical_pending: u64,
    pub orphan_pending: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LiveReconciliationBlockers {
    pub orphaned_block_issue_count: u64,
    pub orphaned_unpaid_amount: u64,
    pub unresolved_completed_payout_rows: u64,
}

#[derive(Debug, Clone)]
pub struct UnreconciledCompletedPayoutRow {
    pub payout_id: i64,
    pub address: String,
    pub amount: u64,
    pub fee: u64,
    pub tx_hash: String,
    pub timestamp: SystemTime,
    pub linked_amount: u64,
    pub orphaned_linked_amount: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManualCompletedPayoutResolutionKind {
    RestorePending,
    DropPaid,
}

impl ManualCompletedPayoutResolutionKind {
    fn revert_reason(self) -> &'static str {
        match self {
            Self::RestorePending => "admin_restore_pending_missing_from_chain",
            Self::DropPaid => "admin_drop_paid_missing_from_chain",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BalanceCreditSourceKind {
    Block,
    Fee,
}

impl BalanceCreditSourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Fee => "fee",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "block" => Ok(Self::Block),
            "fee" => Ok(Self::Fee),
            _ => Err(anyhow!("unknown payout credit source kind {value:?}")),
        }
    }
}

#[derive(Debug, Clone)]
struct BalanceCreditSourceState {
    kind: BalanceCreditSourceKind,
    block_height: u64,
    amount: u64,
    paid_amount: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LivePendingSourceSummary {
    canonical_pending: u64,
}

#[derive(Debug, Clone)]
struct PayoutCreditAllocation {
    source_kind: BalanceCreditSourceKind,
    block_height: u64,
    block_hash: Option<String>,
    address: String,
    amount: u64,
}

struct ManagedClient {
    client: Client,
    url: String,
    schema: Option<String>,
    label: String,
    /// Statement timeout last applied to this session (`None` = default).
    statement_timeout_ms: Option<u64>,
}

impl ManagedClient {
    fn new(client: Client, url: &str, schema: Option<&str>, label: &str) -> Self {
        Self {
            client,
            url: url.to_string(),
            schema: schema.map(str::to_string),
            label: label.to_string(),
            statement_timeout_ms: None,
        }
    }

    fn reconnect(&mut self) -> Result<()> {
        self.client =
            PostgresStore::connect_client(&self.url, self.schema.as_deref(), &self.label)?;
        self.statement_timeout_ms = None;
        Ok(())
    }

    fn apply_statement_timeout_scope(&mut self) {
        let desired = STATEMENT_TIMEOUT_SCOPE.with(|scope| scope.get());
        if desired == self.statement_timeout_ms || self.client.is_closed() {
            return;
        }
        let sql = match desired {
            Some(millis) => format!("SET statement_timeout = {millis}"),
            None => "RESET statement_timeout".to_string(),
        };
        match self.client.batch_execute(&sql) {
            Ok(()) => self.statement_timeout_ms = desired,
            Err(err) => warn!(label = %self.label, error = %err, "failed applying statement_timeout"),
        }
    }

    fn ensure_connected(&mut self) -> Result<()> {
        if self.client.is_closed() {
            warn!(label = %self.label, "postgres client closed; reconnecting");
            self.reconnect()?;
        }
        Ok(())
    }

    fn should_retry(err: &anyhow::Error, client: &Client) -> bool {
        client.is_closed() || err.to_string().contains("connection closed")
    }

    fn with_retry<T>(
        &mut self,
        operation: &'static str,
        mut op: impl FnMut(&mut Client) -> Result<T>,
    ) -> Result<T> {
        self.ensure_connected()?;
        match op(&mut self.client) {
            Ok(value) => Ok(value),
            Err(err) if Self::should_retry(&err, &self.client) => {
                warn!(
                    label = %self.label,
                    operation,
                    error = %err,
                    "postgres operation failed on a closed client; reconnecting and retrying"
                );
                self.reconnect()?;
                op(&mut self.client)
            }
            Err(err) => Err(err),
        }
    }

    fn execute(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        self.with_retry("execute", |client| Ok(client.execute(query, params)?))
    }

    #[track_caller]
    fn query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        observe_read(|| self.with_retry("query", |client| Ok(client.query(query, params)?)))
    }

    #[track_caller]
    fn query_one(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row> {
        observe_read(|| self.with_retry("query_one", |client| Ok(client.query_one(query, params)?)))
    }

    #[track_caller]
    fn query_opt(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        observe_read(|| self.with_retry("query_opt", |client| Ok(client.query_opt(query, params)?)))
    }

    fn transaction(&mut self) -> Result<Transaction<'_>> {
        self.ensure_connected()?;
        Ok(self.client.transaction()?)
    }
}

pub struct PostgresStore {
    conns: Vec<Mutex<ManagedClient>>,
    next_conn: AtomicUsize,
    url: String,
    schema: Option<String>,
    /// Dedicated session for the payout sweep advisory lock. Session locks
    /// belong to one backend, so this connection never serves other queries.
    payout_sweep_lock_client: Mutex<Option<Client>>,
    /// Commit share claims and provisional share inserts without waiting for
    /// the WAL flush (`share_async_commit`).
    async_share_commit: std::sync::atomic::AtomicBool,
}

/// Advisory lock key shared by every process that runs payout sweeps or
/// moves the daemon a payout send may be in flight on. The schema is part of
/// the key so isolated test schemas in one database do not contend.
const PAYOUT_SWEEP_LOCK_KEY_SQL: &str =
    "hashtextextended('blocknet-pool:payout-sweep:' || current_schema(), 0)";

/// Try to take the payout sweep session lock on `client` without waiting.
pub fn try_acquire_payout_sweep_lock(client: &mut Client) -> Result<bool> {
    let row = client
        .query_one(
            &format!("SELECT pg_try_advisory_lock({PAYOUT_SWEEP_LOCK_KEY_SQL})"),
            &[],
        )
        .context("try payout sweep advisory lock")?;
    Ok(row.get::<_, bool>(0))
}

/// Release the payout sweep session lock held by `client`.
pub fn release_payout_sweep_lock(client: &mut Client) -> Result<bool> {
    let row = client
        .query_one(
            &format!("SELECT pg_advisory_unlock({PAYOUT_SWEEP_LOCK_KEY_SQL})"),
            &[],
        )
        .context("release payout sweep advisory lock")?;
    Ok(row.get::<_, bool>(0))
}

/// Payout sends whose outcome is not final yet: frozen before or during the
/// wallet send (`unresolved_*`), or broadcast and awaiting confirmation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InflightPayoutSends {
    pub unresolved_rows: u64,
    pub unresolved_batches: Vec<String>,
    pub broadcast_rows: u64,
    pub broadcast_tx_hashes: Vec<String>,
}

impl InflightPayoutSends {
    pub fn is_empty(&self) -> bool {
        self.unresolved_rows == 0 && self.broadcast_rows == 0
    }
}

/// List payout sends that are in flight, using any client (no schema setup).
pub fn load_inflight_payout_sends(client: &mut Client) -> Result<InflightPayoutSends> {
    let rows = client
        .query(
            "SELECT COALESCE(batch_id, address), NULLIF(BTRIM(COALESCE(tx_hash, '')), '')
             FROM pending_payouts
             WHERE send_started_at IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(tx_hash, '')), '') IS NOT NULL
             ORDER BY 1",
            &[],
        )
        .context("load in-flight payout sends")?;
    let mut inflight = InflightPayoutSends::default();
    for row in rows {
        let batch = row.get::<_, String>(0);
        match row.get::<_, Option<String>>(1) {
            Some(tx_hash) => {
                inflight.broadcast_rows += 1;
                if !inflight.broadcast_tx_hashes.contains(&tx_hash) {
                    inflight.broadcast_tx_hashes.push(tx_hash);
                }
            }
            None => {
                inflight.unresolved_rows += 1;
                if !inflight.unresolved_batches.contains(&batch) {
                    inflight.unresolved_batches.push(batch);
                }
            }
        }
    }
    Ok(inflight)
}

/// Open a plain client for lock/inspection queries without running the
/// store's schema setup (used by out-of-process tools such as recoveryd).
pub fn connect_payout_inspection_client(url: &str, label: &str) -> Result<Client> {
    PostgresStore::connect_client(url, None, label)
}

impl PostgresStore {
    pub fn read_performance(
        &self,
    ) -> std::collections::BTreeMap<String, crate::telemetry::TimedOperationSummary> {
        READ_PERFORMANCE.get_or_init(Default::default).snapshot()
    }

    fn conn(&self) -> MutexGuard<'_, ManagedClient> {
        let pool_len = self.conns.len();
        assert!(pool_len > 0, "postgres store connection unavailable");
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % pool_len;
        // A cheap balance/health read must not queue behind a history scan
        // while another connection is idle. Acquire the selected connection
        // atomically rather than racing an is_locked() check.
        for offset in 0..pool_len {
            if let Some(mut conn) = self.conns[(idx + offset) % pool_len].try_lock() {
                conn.apply_statement_timeout_scope();
                return conn;
            }
        }
        // At full capacity retain bounded, blocking backpressure.
        let mut conn = self.conns[idx].lock();
        conn.apply_statement_timeout_scope();
        conn
    }

    fn connect_client(url: &str, schema: Option<&str>, label: &str) -> Result<Client> {
        let mut cfg =
            PostgresConfig::from_str(url).with_context(|| format!("parse postgres {url}"))?;
        if let Some(schema) = schema {
            let options = format!("-c search_path={schema}");
            cfg.options(&options);
        }
        cfg.connect(NoTls)
            .with_context(|| format!("connect postgres {url} ({label})"))
    }

    pub fn connect(url: &str, pool_size: i32) -> Result<Arc<Self>> {
        Self::connect_with_schema(url, pool_size, None)
    }

    pub(crate) fn connect_with_schema(
        url: &str,
        pool_size: i32,
        schema: Option<&str>,
    ) -> Result<Arc<Self>> {
        let pool_size = pool_size.max(1) as usize;
        let mut conn = Self::connect_client(url, schema, "primary")?;
        conn.batch_execute(
            r#"
CREATE TABLE IF NOT EXISTS shares (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    miner TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    nonce BIGINT NOT NULL,
    status TEXT NOT NULL,
    was_sampled BOOLEAN NOT NULL,
    block_hash TEXT,
    claimed_hash TEXT,
    miner_version TEXT,
    backend TEXT,
    reject_reason TEXT,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_created ON shares(miner, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_status_created ON shares(miner, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_job_nonce ON shares(job_id, nonce);
CREATE INDEX IF NOT EXISTS idx_shares_block_hash_created_at ON shares(block_hash, created_at DESC);

CREATE TABLE IF NOT EXISTS share_job_replays (
    job_id TEXT PRIMARY KEY,
    header_base BYTEA NOT NULL,
    network_target BYTEA NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_share_job_replays_created_at
    ON share_job_replays(created_at DESC);

CREATE TABLE IF NOT EXISTS blocks (
    height BIGINT PRIMARY KEY,
    hash TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    finder TEXT NOT NULL,
    finder_worker TEXT NOT NULL,
    reward BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    confirmed BOOLEAN NOT NULL,
    orphaned BOOLEAN NOT NULL,
    paid_out BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_finder ON blocks(finder);
CREATE INDEX IF NOT EXISTS idx_blocks_finder_height ON blocks(finder, height DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_unconfirmed ON blocks(confirmed, orphaned, height ASC);
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp_height ON blocks(timestamp DESC, height DESC);

CREATE TABLE IF NOT EXISTS balances (
    address TEXT PRIMARY KEY,
    pending BIGINT NOT NULL,
    paid BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS payouts (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    fee BIGINT NOT NULL DEFAULT 0,
    tx_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    reverted_at BIGINT,
    revert_reason TEXT,
    reversible BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_payouts_timestamp ON payouts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_address_timestamp ON payouts(address, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_tx_hash ON payouts(tx_hash);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS seen_shares (
    job_id TEXT NOT NULL,
    nonce BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    PRIMARY KEY (job_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_seen_shares_expiry ON seen_shares(expires_at);

CREATE TABLE IF NOT EXISTS pending_payouts (
    address TEXT PRIMARY KEY,
    amount BIGINT NOT NULL,
    initiated_at BIGINT NOT NULL,
    send_started_at BIGINT,
    tx_hash TEXT,
    fee BIGINT,
    sent_at BIGINT
);

CREATE TABLE IF NOT EXISTS manual_payout_offsets (
    address TEXT PRIMARY KEY,
    amount BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_fee_events (
    id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL UNIQUE,
    amount BIGINT NOT NULL,
    fee_address TEXT NOT NULL,
    timestamp BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_timestamp ON pool_fee_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_fee_address ON pool_fee_events(fee_address);

CREATE TABLE IF NOT EXISTS pool_fee_balance_credits (
    block_height BIGINT PRIMARY KEY,
    fee_address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL DEFAULT 0,
    timestamp BIGINT NOT NULL,
    credited_at BIGINT NOT NULL,
    reversible BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_balance_credits_fee_address
    ON pool_fee_balance_credits(fee_address);

CREATE TABLE IF NOT EXISTS block_credit_events (
    id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL DEFAULT 0,
    reversible BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (block_height, address)
);
CREATE INDEX IF NOT EXISTS idx_block_credit_events_block_height
    ON block_credit_events(block_height, amount DESC, address ASC);
CREATE INDEX IF NOT EXISTS idx_block_credit_events_address
    ON block_credit_events(address, block_height DESC);

CREATE TABLE IF NOT EXISTS payout_credit_allocations (
    payout_id BIGINT NOT NULL REFERENCES payouts(id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    block_hash TEXT,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    PRIMARY KEY (payout_id, source_kind, block_height, address)
);
CREATE INDEX IF NOT EXISTS idx_payout_credit_allocations_payout_id
    ON payout_credit_allocations(payout_id);
CREATE INDEX IF NOT EXISTS idx_payout_credit_allocations_source
    ON payout_credit_allocations(source_kind, block_height, address);

CREATE TABLE IF NOT EXISTS archived_blocks (
    height BIGINT NOT NULL,
    hash TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    finder TEXT NOT NULL,
    finder_worker TEXT NOT NULL,
    reward BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    confirmed BOOLEAN NOT NULL,
    orphaned BOOLEAN NOT NULL,
    paid_out BOOLEAN NOT NULL,
    effort_pct DOUBLE PRECISION,
    archived_at BIGINT NOT NULL,
    PRIMARY KEY (height, hash)
);
CREATE INDEX IF NOT EXISTS idx_archived_blocks_height
    ON archived_blocks(height DESC, archived_at DESC);

CREATE TABLE IF NOT EXISTS archived_pool_fee_events (
    block_height BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    amount BIGINT NOT NULL,
    fee_address TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    archived_at BIGINT NOT NULL,
    PRIMARY KEY (block_height, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_archived_pool_fee_events_fee_address
    ON archived_pool_fee_events(fee_address, block_height DESC);

CREATE TABLE IF NOT EXISTS archived_pool_fee_balance_credits (
    block_height BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    fee_address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL DEFAULT 0,
    timestamp BIGINT NOT NULL,
    credited_at BIGINT NOT NULL,
    reversible BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at BIGINT NOT NULL,
    PRIMARY KEY (block_height, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_archived_pool_fee_balance_credits_fee_address
    ON archived_pool_fee_balance_credits(fee_address, block_height DESC);

CREATE TABLE IF NOT EXISTS archived_block_credit_events (
    block_height BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    paid_amount BIGINT NOT NULL DEFAULT 0,
    reversible BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at BIGINT NOT NULL,
    PRIMARY KEY (block_height, block_hash, address)
);
CREATE INDEX IF NOT EXISTS idx_archived_block_credit_events_height
    ON archived_block_credit_events(block_height DESC, amount DESC, address ASC);
CREATE INDEX IF NOT EXISTS idx_archived_block_credit_events_address
    ON archived_block_credit_events(address, block_height DESC);

CREATE TABLE IF NOT EXISTS address_risk (
    address TEXT PRIMARY KEY,
    strikes BIGINT NOT NULL,
    strike_window_until BIGINT,
    last_reason TEXT,
    last_event_at BIGINT,
    quarantined_until BIGINT,
    force_verify_until BIGINT
);

CREATE TABLE IF NOT EXISTS vardiff_hints (
    address TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (address, worker)
);
CREATE INDEX IF NOT EXISTS idx_vardiff_hints_updated_at ON vardiff_hints(updated_at DESC);

CREATE TABLE IF NOT EXISTS stat_snapshots (
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    hashrate DOUBLE PRECISION NOT NULL,
    miners INTEGER NOT NULL,
    workers INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stat_snapshots_timestamp ON stat_snapshots(timestamp DESC);

CREATE TABLE IF NOT EXISTS monitor_heartbeats (
    id BIGSERIAL PRIMARY KEY,
    sampled_at BIGINT NOT NULL,
    source TEXT NOT NULL,
    synthetic BOOLEAN NOT NULL DEFAULT FALSE,
    api_up BOOLEAN,
    stratum_up BOOLEAN,
    db_up BOOLEAN NOT NULL,
    daemon_up BOOLEAN,
    public_http_up BOOLEAN,
    daemon_syncing BOOLEAN,
    chain_height BIGINT,
    template_age_seconds BIGINT,
    last_refresh_millis BIGINT,
    stratum_snapshot_age_seconds BIGINT,
    connected_miners BIGINT,
    connected_workers BIGINT,
    estimated_hashrate DOUBLE PRECISION,
    wallet_up BOOLEAN,
    last_accepted_share_at BIGINT,
    last_accepted_share_age_seconds BIGINT,
    payout_pending_count BIGINT,
    payout_pending_amount BIGINT,
    oldest_pending_payout_at BIGINT,
    oldest_pending_payout_age_seconds BIGINT,
    oldest_pending_send_started_at BIGINT,
    oldest_pending_send_age_seconds BIGINT,
    validation_candidate_queue_depth BIGINT,
    validation_regular_queue_depth BIGINT,
    summary_state TEXT NOT NULL,
    details_json TEXT,
    UNIQUE (source, sampled_at)
);
CREATE INDEX IF NOT EXISTS idx_monitor_heartbeats_sampled_at
    ON monitor_heartbeats(sampled_at DESC);
CREATE INDEX IF NOT EXISTS idx_monitor_heartbeats_source_sampled_at
    ON monitor_heartbeats(source, sampled_at DESC);

CREATE TABLE IF NOT EXISTS monitor_incidents (
    id BIGSERIAL PRIMARY KEY,
    dedupe_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    visibility TEXT NOT NULL,
    source TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail TEXT,
    started_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    ended_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_monitor_incidents_started_at
    ON monitor_incidents(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_monitor_incidents_visibility_started_at
    ON monitor_incidents(visibility, started_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_monitor_incidents_open_dedupe
    ON monitor_incidents(dedupe_key)
    WHERE ended_at IS NULL;

CREATE TABLE IF NOT EXISTS validation_address_states (
    address TEXT PRIMARY KEY,
    total_shares BIGINT NOT NULL,
    sampled_shares BIGINT NOT NULL,
    invalid_samples BIGINT NOT NULL,
    risk_sampled_shares BIGINT NOT NULL DEFAULT 0,
    risk_invalid_samples BIGINT NOT NULL DEFAULT 0,
    forced_started_at BIGINT,
    forced_until BIGINT,
    forced_sampled_shares BIGINT NOT NULL DEFAULT 0,
    forced_invalid_samples BIGINT NOT NULL DEFAULT 0,
    resume_forced_at BIGINT,
    hold_cause TEXT,
    last_seen_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_address_states_last_seen
    ON validation_address_states(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_address_states_forced_until
    ON validation_address_states(forced_until DESC);

CREATE TABLE IF NOT EXISTS validation_clear_events (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    cleared_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_clear_events_cleared_at
    ON validation_clear_events(cleared_at DESC);

CREATE TABLE IF NOT EXISTS validation_provisionals (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    share_id BIGINT,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_provisionals_created_at
    ON validation_provisionals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_provisionals_address_created_at
    ON validation_provisionals(address, created_at DESC);

CREATE TABLE IF NOT EXISTS share_daily_summaries (
    day_start BIGINT PRIMARY KEY,
    accepted_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    accepted_difficulty BIGINT NOT NULL,
    unique_miners BIGINT NOT NULL,
    unique_workers BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_share_daily_summaries_day_start
    ON share_daily_summaries(day_start DESC);

CREATE TABLE IF NOT EXISTS share_rejection_reason_daily_summaries (
    day_start BIGINT NOT NULL,
    reason TEXT NOT NULL,
    rejected_count BIGINT NOT NULL,
    PRIMARY KEY (day_start, reason)
);
CREATE INDEX IF NOT EXISTS idx_share_rejection_reason_daily_summaries_day_start
    ON share_rejection_reason_daily_summaries(day_start DESC);

CREATE TABLE IF NOT EXISTS payout_daily_summaries (
    day_start BIGINT PRIMARY KEY,
    payout_count BIGINT NOT NULL,
    total_amount BIGINT NOT NULL,
    total_fee BIGINT NOT NULL,
    unique_recipients BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payout_daily_summaries_day_start
    ON payout_daily_summaries(day_start DESC);
"#,
        )
        .context("init postgres schema")?;
        conn.batch_execute(
            "ALTER TABLE payouts ADD COLUMN IF NOT EXISTS fee BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure payouts.fee column")?;
        conn.batch_execute(
            "ALTER TABLE pending_payouts ADD COLUMN IF NOT EXISTS send_started_at BIGINT",
        )
        .context("ensure pending_payouts.send_started_at column")?;
        conn.batch_execute("ALTER TABLE pending_payouts ADD COLUMN IF NOT EXISTS tx_hash TEXT")
            .context("ensure pending_payouts.tx_hash column")?;
        conn.batch_execute("ALTER TABLE pending_payouts ADD COLUMN IF NOT EXISTS fee BIGINT")
            .context("ensure pending_payouts.fee column")?;
        conn.batch_execute("ALTER TABLE pending_payouts ADD COLUMN IF NOT EXISTS sent_at BIGINT")
            .context("ensure pending_payouts.sent_at column")?;
        conn.batch_execute("ALTER TABLE payouts ADD COLUMN IF NOT EXISTS batch_id TEXT")
            .context("ensure payouts.batch_id column")?;
        conn.batch_execute("ALTER TABLE pending_payouts ADD COLUMN IF NOT EXISTS batch_id TEXT")
            .context("ensure pending_payouts.batch_id column")?;
        conn.batch_execute("ALTER TABLE pending_payouts ADD COLUMN IF NOT EXISTS send_daemon TEXT")
            .context("ensure pending_payouts.send_daemon column")?;
        conn.batch_execute(
            "UPDATE payouts SET batch_id = CONCAT('legacy:', (timestamp / 300)::text)
             WHERE COALESCE(BTRIM(batch_id), '') = '';
             UPDATE pending_payouts SET batch_id = CONCAT('legacy:', (COALESCE(sent_at, send_started_at, initiated_at) / 300)::text)
             WHERE tx_hash IS NOT NULL AND BTRIM(tx_hash) <> ''
               AND COALESCE(BTRIM(batch_id), '') = ''
               AND COALESCE(sent_at, send_started_at, initiated_at) IS NOT NULL",
        )
        .context("backfill legacy payout batch ids")?;
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_payouts_batch_id
             ON payouts(batch_id)",
        )
        .context("ensure payouts.batch_id index")?;
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_payouts_batch_id
             ON pending_payouts(batch_id)",
        )
        .context("ensure pending_payouts.batch_id index")?;
        conn.batch_execute("ALTER TABLE payouts ADD COLUMN IF NOT EXISTS reverted_at BIGINT")
            .context("ensure payouts.reverted_at column")?;
        conn.batch_execute("ALTER TABLE payouts ADD COLUMN IF NOT EXISTS revert_reason TEXT")
            .context("ensure payouts.revert_reason column")?;
        conn.batch_execute(
            "ALTER TABLE payouts ADD COLUMN IF NOT EXISTS reversible BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .context("ensure payouts.reversible column")?;
        // Existing databases must first apply scripts/index_read_latency.sql
        // concurrently. These declarations also provision empty test/new pools.
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_payouts_visible_batch_timestamp
             ON payouts(batch_id, timestamp DESC) WHERE reverted_at IS NULL;
             CREATE INDEX IF NOT EXISTS idx_shares_accepted_time_difficulty
             ON shares(created_at) INCLUDE (difficulty)
             WHERE status IN ('verified', 'provisional')",
        )
        .context("ensure API read indexes")?;
        conn.batch_execute(include_str!("../sql/share_read_counts.sql"))
            .context("ensure exact miner share read counts")?;
        conn.batch_execute(include_str!("../sql/share_read_minutes.sql"))
            .context("ensure exact minute share read summaries")?;
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_shares_worker_window
             ON shares(created_at) INCLUDE (miner,worker,status,difficulty)",
        )
        .context("ensure share preview boundary index")?;
        // Install concurrently with scripts/index_remaining_read_windows.sql first.
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_block_credit_unpaid
             ON block_credit_events(block_height,address) INCLUDE(amount,paid_amount)
             WHERE amount > paid_amount;
             CREATE INDEX IF NOT EXISTS idx_pool_fee_credit_unpaid
             ON pool_fee_balance_credits(block_height,fee_address) INCLUDE(amount,paid_amount)
             WHERE amount > paid_amount",
        )
        .context("ensure unpaid balance source indexes")?;
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS reject_reason TEXT")
            .context("ensure shares.reject_reason column")?;
        // Apply scripts/index_share_rejections.sql concurrently before deploying
        // to an existing production database to avoid a startup index build.
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_shares_rejected_created_reason
             ON shares(created_at DESC) INCLUDE (reject_reason)
             WHERE status NOT IN ('verified', 'provisional')",
        )
        .context("ensure shares rejection analytics index")?;
        conn.batch_execute(
            "UPDATE shares SET reject_reason = 'legacy / unknown'
             WHERE status NOT IN ('verified','provisional')
               AND COALESCE(BTRIM(reject_reason), '') = ''",
        )
        .context("backfill legacy share rejection reasons")?;
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS claimed_hash TEXT")
            .context("ensure shares.claimed_hash column")?;
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS miner_version TEXT")
            .context("ensure shares.miner_version column")?;
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS backend TEXT")
            .context("ensure shares.backend column")?;
        // Apply scripts/index_worker_reads.sql concurrently before upgrading an
        // existing database. Keep worker history reads out of the share heap.
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_shares_miner_read_window
             ON shares(miner, created_at DESC)
             INCLUDE (worker, status, difficulty, miner_version, backend)",
        )
        .context("ensure covering miner read index")?;
        conn.batch_execute(
            "ALTER TABLE blocks ADD COLUMN IF NOT EXISTS effort_pct DOUBLE PRECISION",
        )
        .context("ensure blocks.effort_pct column")?;
        conn.batch_execute(
            "ALTER TABLE address_risk ADD COLUMN IF NOT EXISTS strike_window_until BIGINT",
        )
        .context("ensure address_risk.strike_window_until column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS risk_sampled_shares BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure validation_address_states.risk_sampled_shares column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS risk_invalid_samples BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure validation_address_states.risk_invalid_samples column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS forced_started_at BIGINT",
        )
        .context("ensure validation_address_states.forced_started_at column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS forced_sampled_shares BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure validation_address_states.forced_sampled_shares column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS forced_invalid_samples BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure validation_address_states.forced_invalid_samples column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS resume_forced_at BIGINT",
        )
        .context("ensure validation_address_states.resume_forced_at column")?;
        conn.batch_execute(
            "ALTER TABLE validation_address_states ADD COLUMN IF NOT EXISTS hold_cause TEXT",
        )
        .context("ensure validation_address_states.hold_cause column")?;
        conn.batch_execute(
            "ALTER TABLE validation_provisionals ADD COLUMN IF NOT EXISTS share_id BIGINT",
        )
        .context("ensure validation_provisionals.share_id column")?;
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_validation_provisionals_share_id
             ON validation_provisionals(share_id)",
        )
        .context("ensure validation_provisionals.share_id index")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS wallet_up BOOLEAN",
        )
        .context("ensure monitor_heartbeats.wallet_up column")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS last_accepted_share_at BIGINT",
        )
        .context("ensure monitor_heartbeats.last_accepted_share_at column")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS last_accepted_share_age_seconds BIGINT",
        )
        .context("ensure monitor_heartbeats.last_accepted_share_age_seconds column")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS oldest_pending_payout_at BIGINT",
        )
        .context("ensure monitor_heartbeats.oldest_pending_payout_at column")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS oldest_pending_payout_age_seconds BIGINT",
        )
        .context("ensure monitor_heartbeats.oldest_pending_payout_age_seconds column")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS oldest_pending_send_started_at BIGINT",
        )
        .context("ensure monitor_heartbeats.oldest_pending_send_started_at column")?;
        conn.batch_execute(
            "ALTER TABLE monitor_heartbeats ADD COLUMN IF NOT EXISTS oldest_pending_send_age_seconds BIGINT",
        )
        .context("ensure monitor_heartbeats.oldest_pending_send_age_seconds column")?;
        conn.batch_execute(
            "ALTER TABLE block_credit_events ADD COLUMN IF NOT EXISTS paid_amount BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure block_credit_events.paid_amount column")?;
        conn.batch_execute(
            "ALTER TABLE block_credit_events ADD COLUMN IF NOT EXISTS reversible BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .context("ensure block_credit_events.reversible column")?;
        conn.batch_execute(
            "ALTER TABLE pool_fee_balance_credits ADD COLUMN IF NOT EXISTS paid_amount BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure pool_fee_balance_credits.paid_amount column")?;
        conn.batch_execute(
            "ALTER TABLE pool_fee_balance_credits ADD COLUMN IF NOT EXISTS reversible BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .context("ensure pool_fee_balance_credits.reversible column")?;
        conn.batch_execute(
            "ALTER TABLE payout_credit_allocations ADD COLUMN IF NOT EXISTS block_hash TEXT",
        )
        .context("ensure payout_credit_allocations.block_hash column")?;
        conn.batch_execute(
            "CREATE INDEX IF NOT EXISTS idx_payout_credit_allocations_source_hash
             ON payout_credit_allocations(source_kind, block_height, block_hash, address)",
        )
        .context("ensure payout_credit_allocations source_hash index")?;

        let mut conns = Vec::with_capacity(pool_size);
        conns.push(Mutex::new(ManagedClient::new(conn, url, schema, "primary")));
        for idx in 1..pool_size {
            let label = format!("pool conn {idx}");
            let extra = Self::connect_client(url, schema, &label)?;
            conns.push(Mutex::new(ManagedClient::new(extra, url, schema, &label)));
        }

        Ok(Arc::new(Self {
            conns,
            next_conn: AtomicUsize::new(0),
            url: url.to_string(),
            schema: schema.map(str::to_string),
            payout_sweep_lock_client: Mutex::new(None),
            async_share_commit: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    /// A separate session on this store's database/schema, for tests that
    /// simulate another payout process.
    #[cfg(test)]
    pub(crate) fn open_independent_session(&self) -> Result<Client> {
        Self::connect_client(
            &self.url,
            self.schema.as_deref(),
            "test independent session",
        )
    }

    /// Run `work` while holding the cross-process payout sweep advisory lock.
    ///
    /// Returns `Ok(None)` without running `work` when another session (a
    /// second payout process, or recoveryd mid-cutover) holds the lock. The
    /// lock lives on a dedicated connection; if releasing it fails the
    /// connection is dropped, which ends the session and frees the lock.
    pub(crate) fn with_payout_sweep_lock<T>(&self, work: impl FnOnce() -> T) -> Result<Option<T>> {
        let mut slot = self.payout_sweep_lock_client.lock();
        let mut client = match slot.take() {
            Some(client) if !client.is_closed() => client,
            _ => Self::connect_client(&self.url, self.schema.as_deref(), "payout sweep lock")?,
        };
        if !try_acquire_payout_sweep_lock(&mut client)? {
            *slot = Some(client);
            return Ok(None);
        }
        // Panics in `work` drop `client`, closing the session and the lock.
        let result = work();
        match release_payout_sweep_lock(&mut client) {
            Ok(true) => *slot = Some(client),
            Ok(false) => warn!("payout sweep advisory lock was not held at release"),
            Err(err) => warn!(
                error = %err,
                "failed releasing payout sweep advisory lock; dropping its session"
            ),
        }
        Ok(Some(result))
    }

    /// See `Config::share_async_commit`.
    pub fn set_async_share_commit(&self, enabled: bool) {
        self.async_share_commit.store(enabled, Ordering::Relaxed);
    }

    fn async_share_commit(&self) -> bool {
        self.async_share_commit.load(Ordering::Relaxed)
    }

    pub fn add_share(&self, share: ShareRecord) -> Result<()> {
        self.add_share_with_replay(share, None)
    }

    pub fn add_share_with_replay(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<()> {
        self.add_share_with_replay_and_id(share, replay).map(|_| ())
    }

    pub(crate) fn add_share_with_replay_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<i64> {
        self.add_share_with_replay_metadata_and_id(share, replay, None, None)
    }

    pub(crate) fn add_share_with_replay_metadata_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
        miner_version: Option<&str>,
        backend: Option<&str>,
    ) -> Result<i64> {
        let created = to_unix(share.created_at);
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        // Verified shares (block candidates among them) stay durable.
        if share.status == "provisional" && self.async_share_commit() {
            tx.batch_execute("SET LOCAL synchronous_commit = off")?;
        }

        if let Some(replay) = replay {
            let network_target = replay.network_target.to_vec();
            tx.execute(
                "INSERT INTO share_job_replays (job_id, header_base, network_target, created_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(job_id) DO UPDATE SET
                     header_base = EXCLUDED.header_base,
                     network_target = EXCLUDED.network_target,
                     created_at = LEAST(share_job_replays.created_at, EXCLUDED.created_at)",
                &[
                    &replay.job_id,
                    &replay.header_base,
                    &network_target,
                    &to_unix(replay.created_at),
                ],
            )?;
        }

        let row = tx.query_one(
            "INSERT INTO shares (job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, reject_reason, created_at, miner_version, backend)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             RETURNING id",
            &[
                &share.job_id,
                &share.miner,
                &share.worker,
                &u64_to_i64(share.difficulty)?,
                &u64_to_i64(share.nonce)?,
                &share.status,
                &share.was_sampled,
                &share.block_hash,
                &share.claimed_hash,
                &share.reject_reason,
                &created,
                &miner_version,
                &backend,
            ],
        )?;
        tx.commit()?;
        Ok(row.get::<_, i64>(0))
    }

    pub(crate) fn get_share_replays_for_job_ids(
        &self,
        job_ids: &[String],
    ) -> Result<HashMap<String, ShareReplayData>> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = self.conn().query(
            "SELECT job_id, header_base, network_target, created_at
             FROM share_job_replays
             WHERE job_id = ANY($1)",
            &[&job_ids],
        )?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let replay = row_to_share_replay(row)?;
            out.insert(replay.job_id.clone(), replay);
        }
        Ok(out)
    }

    pub(crate) fn apply_share_replay_updates(&self, updates: &[ShareReplayUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        for update in updates {
            tx.execute(
                "UPDATE shares
                 SET status = $2,
                     was_sampled = $3,
                     reject_reason = $4
                 WHERE id = $1",
                &[
                    &update.share_id,
                    &update.status,
                    &update.was_sampled,
                    &update.reject_reason,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Applies an audit verdict only while the share is still provisional, so
    /// a late passing audit cannot resurrect a share revoked meanwhile.
    pub(crate) fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<bool> {
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let updated = tx.execute(
            "UPDATE shares
             SET status = $2,
                 was_sampled = $3,
                 reject_reason = $4
             WHERE id = $1 AND status = 'provisional'",
            &[
                &update.share_id,
                &update.status,
                &update.was_sampled,
                &update.reject_reason,
            ],
        )?;
        tx.execute(
            "DELETE FROM validation_provisionals WHERE share_id = $1",
            &[&update.share_id],
        )?;
        tx.commit()?;
        Ok(updated > 0)
    }

    /// Marks still-provisional shares as rejected (not creditable). Used when
    /// a source is struck for a proven fake share. Returns rows changed.
    pub(crate) fn revoke_provisional_shares(&self, share_ids: &[i64], reason: &str) -> Result<u64> {
        let mut total = 0u64;
        for chunk in share_ids.chunks(5_000) {
            let ids = chunk.to_vec();
            let mut conn = self.conn();
            let mut tx = conn.transaction()?;
            let updated = tx.execute(
                "UPDATE shares
                 SET status = 'rejected',
                     reject_reason = $2
                 WHERE id = ANY($1) AND status = 'provisional'",
                &[&ids, &reason],
            )?;
            tx.execute(
                "DELETE FROM validation_provisionals WHERE share_id = ANY($1)",
                &[&ids],
            )?;
            tx.commit()?;
            total = total.saturating_add(updated);
        }
        Ok(total)
    }

    #[cfg(test)]
    pub(crate) fn get_recent_shares(&self, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares ORDER BY created_at DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares WHERE miner = $1 ORDER BY created_at DESC LIMIT $2",
            &[&address, &limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub(crate) fn get_provisional_shares_by_ids(&self, ids: &[i64]) -> Result<Vec<DbShare>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids.to_vec();
        let rows = self.conn().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares
             WHERE id = ANY($1)
               AND status = 'provisional'
             ORDER BY id ASC",
            &[&ids],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub(crate) fn get_provisional_shares_for_miner_since(
        &self,
        address: &str,
        cutoff: SystemTime,
        limit: i64,
    ) -> Result<Vec<DbShare>> {
        let rows = self.conn().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares
             WHERE miner = $1
               AND status = 'provisional'
               AND was_sampled = FALSE
               AND created_at > $2
             ORDER BY created_at ASC, id ASC
             LIMIT $3",
            &[&address, &to_unix(cutoff), &limit.max(1)],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn first_share_at_for_miner(&self, address: &str) -> Result<Option<SystemTime>> {
        let row = self.conn().query_one(
            "SELECT MIN(created_at) FROM shares WHERE miner = $1",
            &[&address],
        )?;
        Ok(row.get::<_, Option<i64>>(0).map(from_unix))
    }

    pub fn get_shares_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<DbShare>> {
        let start_ts = to_unix(start);
        let end_ts = to_unix(end);
        let rows = self.conn().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares WHERE created_at >= $1 AND created_at <= $2",
            &[&start_ts, &end_ts],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    /// Summarize overlapping PPLNS windows without materializing raw shares or
    /// rescanning their overlap. Split at every window/delay boundary, then add
    /// each exact segment to the windows containing it.
    pub fn summarize_share_windows(
        &self,
        windows: &[(SystemTime, SystemTime)],
        provisional_ready_cutoff: Option<SystemTime>,
    ) -> Result<Vec<Vec<ShareWindowAddressPreview>>> {
        if windows.is_empty() {
            return Ok(Vec::new());
        }
        let starts = windows
            .iter()
            .map(|(start, _)| {
                let seconds = to_unix(*start);
                if start
                    .duration_since(UNIX_EPOCH)
                    .is_ok_and(|value| value.subsec_nanos() > 0)
                {
                    seconds
                        .checked_add(1)
                        .context("share window start overflow")
                } else {
                    Ok(seconds)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let ends = windows
            .iter()
            .map(|(_, end)| {
                to_unix(*end)
                    .checked_add(1)
                    .context("share window end overflow")
            })
            .collect::<Result<Vec<_>>>()?;
        let cutoff = provisional_ready_cutoff.map(to_unix);
        let rows = self.conn().query(
            "WITH windows AS (
                SELECT (ordinality-1)::bigint AS idx,start_at,end_at
                FROM unnest($1::bigint[],$2::bigint[]) WITH ORDINALITY AS w(start_at,end_at,ordinality)
             ), boundaries AS (
                SELECT start_at AS t FROM windows UNION SELECT end_at FROM windows
                UNION SELECT $3::bigint+1 WHERE $3::bigint IS NOT NULL
                    AND $3::bigint >= (SELECT MIN(start_at) FROM windows)
                    AND $3::bigint < (SELECT MAX(end_at) FROM windows)-1
             ), segments AS (
                SELECT t AS start_at,LEAD(t) OVER (ORDER BY t) AS end_at FROM boundaries
             ), summaries AS MATERIALIZED (
                SELECT s.start_at,s.end_at,p.* FROM segments s
                CROSS JOIN LATERAL share_read_address_window(s.start_at,s.end_at-1) p
                WHERE s.end_at > s.start_at AND EXISTS (
                    SELECT 1 FROM windows w WHERE s.start_at >= w.start_at AND s.end_at <= w.end_at
                )
             )
             SELECT w.idx,p.address,SUM(p.share_count)::bigint,
                    COALESCE(SUM(p.share_count) FILTER (WHERE p.share_status IN ('','verified')),0)::bigint,
                    COALESCE(SUM(p.share_work) FILTER (WHERE p.share_status IN ('','verified')),0)::bigint,
                    COALESCE(SUM(p.share_count) FILTER (WHERE p.share_status='provisional' AND p.end_at-1 <= $3),0)::bigint,
                    COALESCE(SUM(p.share_work) FILTER (WHERE p.share_status='provisional' AND p.end_at-1 <= $3),0)::bigint,
                    COALESCE(SUM(p.share_count) FILTER (WHERE p.share_status='provisional' AND ($3 IS NULL OR p.start_at > $3)),0)::bigint
             FROM windows w JOIN summaries p ON p.start_at >= w.start_at AND p.end_at <= w.end_at
             GROUP BY w.idx,p.address ORDER BY w.idx,p.address",
            &[&starts,&ends,&cutoff],
        )?;
        let mut results = vec![Vec::new(); windows.len()];
        for row in rows {
            results[row.get::<_, i64>(0) as usize].push(ShareWindowAddressPreview {
                address: row.get(1),
                seen_shares: row.get::<_, i64>(2).max(0) as u64,
                verified_shares: row.get::<_, i64>(3).max(0) as u64,
                verified_difficulty: row.get::<_, i64>(4).max(0) as u64,
                provisional_shares_ready: row.get::<_, i64>(5).max(0) as u64,
                provisional_difficulty_ready: row.get::<_, i64>(6).max(0) as u64,
                provisional_shares_delayed: row.get::<_, i64>(7).max(0) as u64,
            });
        }
        Ok(results)
    }

    #[cfg(test)]
    fn summarize_shares_between(
        &self,
        start: SystemTime,
        end: SystemTime,
        provisional_ready_cutoff: Option<SystemTime>,
    ) -> Result<Vec<ShareWindowAddressPreview>> {
        let start_ts = to_unix(start);
        let end_ts = to_unix(end);
        let ready_enabled = provisional_ready_cutoff.is_some();
        let ready_cutoff = provisional_ready_cutoff.map(to_unix).unwrap_or_default();
        let rows = self.conn().query(
            "SELECT
                miner,
                COUNT(*)::bigint AS seen_shares,
                COUNT(*) FILTER (WHERE status = '' OR status = 'verified')::bigint AS verified_shares,
                COALESCE(SUM(difficulty) FILTER (WHERE status = '' OR status = 'verified'), 0)::bigint AS verified_difficulty,
                COUNT(*) FILTER (
                    WHERE status = 'provisional'
                      AND $3
                      AND created_at <= $4
                )::bigint AS provisional_shares_ready,
                COALESCE(SUM(difficulty) FILTER (
                    WHERE status = 'provisional'
                      AND $3
                      AND created_at <= $4
                ), 0)::bigint AS provisional_difficulty_ready,
                COUNT(*) FILTER (
                    WHERE status = 'provisional'
                      AND (
                        NOT $3
                        OR created_at > $4
                      )
                )::bigint AS provisional_shares_delayed
             FROM shares
             WHERE created_at >= $1 AND created_at <= $2
             GROUP BY miner
             ORDER BY miner ASC",
            &[&start_ts, &end_ts, &ready_enabled, &ready_cutoff],
        )?;
        Ok(rows
            .into_iter()
            .map(row_to_share_window_preview)
            .collect::<Vec<_>>())
    }

    pub(crate) fn latest_share_timestamp_for_block_hash(
        &self,
        hash: &str,
    ) -> Result<Option<SystemTime>> {
        let row = self.conn().query_one(
            "SELECT MAX(created_at) FROM shares WHERE block_hash = $1",
            &[&hash],
        )?;
        Ok(row.get::<_, Option<i64>>(0).map(from_unix))
    }

    pub fn latest_share_timestamps_for_block_hashes(
        &self,
        hashes: &[String],
    ) -> Result<HashMap<String, SystemTime>> {
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = self.conn().query(
            "SELECT block_hash, MAX(created_at)::bigint
             FROM shares
             WHERE block_hash = ANY($1)
             GROUP BY block_hash",
            &[&hashes],
        )?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let hash = row.get::<_, Option<String>>(0)?;
                let created_at = row.get::<_, Option<i64>>(1)?;
                Some((hash, from_unix(created_at)))
            })
            .collect())
    }

    pub fn hashrate_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<(u64, u64, Option<SystemTime>, Option<SystemTime>)> {
        let since_ts = to_unix(since);
        let row = self.conn().query_one(
            "SELECT COALESCE(SUM(difficulty),0)::bigint, COUNT(*)::bigint, MIN(created_at), MAX(created_at)
             FROM shares
             WHERE miner = $1 AND created_at >= $2
               AND status IN ('verified','provisional')",
            &[&address, &since_ts],
        )?;
        let total_diff: i64 = row.get(0);
        let count: i64 = row.get(1);
        let oldest: Option<i64> = row.get(2);
        let newest: Option<i64> = row.get(3);
        Ok((
            total_diff.max(0) as u64,
            count.max(0) as u64,
            oldest.map(from_unix),
            newest.map(from_unix),
        ))
    }

    pub fn hashrate_stats_pool(
        &self,
        since: SystemTime,
    ) -> Result<(u64, u64, Option<SystemTime>, Option<SystemTime>)> {
        let since_ts = to_unix(since);
        let row = self.conn().query_one(
            "SELECT w.work,w.accepted,
                (SELECT MIN(created_at) FROM shares WHERE created_at >= $1 AND status IN ('verified','provisional')),
                (SELECT MAX(created_at) FROM shares WHERE created_at >= $1 AND status IN ('verified','provisional'))
             FROM share_read_window($1,9223372036854775807) w",
            &[&since_ts],
        )?;
        let total_diff: i64 = row.get(0);
        let count: i64 = row.get(1);
        let oldest: Option<i64> = row.get(2);
        let newest: Option<i64> = row.get(3);
        Ok((
            total_diff.max(0) as u64,
            count.max(0) as u64,
            oldest.map(from_unix),
            newest.map(from_unix),
        ))
    }

    pub fn accepted_work_between(&self, start: SystemTime, end: SystemTime) -> Result<u64> {
        let row = self.conn().query_one(
            "SELECT share_read_work($1,$2)",
            &[&to_unix(start), &to_unix(end)],
        )?;
        Ok(row.get::<_, i64>(0).max(0) as u64)
    }

    pub fn share_outcome_counts_since(&self, since: SystemTime) -> Result<(u64, u64)> {
        let since_ts = to_unix(since);
        let row = self.conn().query_one(
            "SELECT accepted,rejected FROM share_read_window($1,9223372036854775807)",
            &[&since_ts],
        )?;
        let accepted: i64 = row.get(0);
        let rejected: i64 = row.get(1);
        Ok((accepted.max(0) as u64, rejected.max(0) as u64))
    }

    pub fn rejection_reason_counts_since(
        &self,
        since: SystemTime,
    ) -> Result<Vec<RejectionReasonCount>> {
        let since_ts = to_unix(since);
        let rows = self.conn().query(
            "SELECT
                reject_reason AS reason,
                COUNT(*)::bigint AS rejected_count
             FROM shares
             WHERE created_at >= $1
               AND status NOT IN ('verified','provisional')
             GROUP BY reason
             ORDER BY rejected_count DESC, reason ASC",
            &[&since_ts],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| RejectionReasonCount {
                reason: row.get::<_, String>(0),
                count: row.get::<_, i64>(1).max(0) as u64,
            })
            .collect())
    }

    pub fn total_rejection_reason_counts(&self) -> Result<Vec<RejectionReasonCount>> {
        let live_rows = self.conn().query(
            "SELECT
                reject_reason AS reason,
                COUNT(*)::bigint AS rejected_count
             FROM shares
             WHERE status NOT IN ('verified','provisional')
             GROUP BY reason",
            &[],
        )?;
        let summary_rows = self.conn().query(
            "SELECT reason, COALESCE(SUM(rejected_count), 0)::bigint
             FROM share_rejection_reason_daily_summaries
             GROUP BY reason",
            &[],
        )?;

        let live = live_rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1).max(0) as u64));
        let summarized = summary_rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1).max(0) as u64));
        Ok(sort_reason_counts(
            live.chain(summarized).collect::<Vec<(String, u64)>>(),
        ))
    }

    pub fn add_block(&self, block: &DbBlock) -> Result<()> {
        self.conn().execute(
            "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT(height) DO UPDATE SET
                 hash=EXCLUDED.hash,
                 difficulty=EXCLUDED.difficulty,
                 finder=EXCLUDED.finder,
                 finder_worker=EXCLUDED.finder_worker,
                 reward=EXCLUDED.reward,
                 timestamp=EXCLUDED.timestamp,
                 confirmed=EXCLUDED.confirmed,
                 orphaned=EXCLUDED.orphaned,
                 paid_out=EXCLUDED.paid_out,
                 effort_pct=EXCLUDED.effort_pct",
            &[
                &u64_to_i64(block.height)?,
                &block.hash,
                &u64_to_i64(block.difficulty)?,
                &block.finder,
                &block.finder_worker,
                &u64_to_i64(block.reward)?,
                &to_unix(block.timestamp),
                &block.confirmed,
                &block.orphaned,
                &block.paid_out,
                &block.effort_pct,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn insert_block_if_absent(&self, block: &DbBlock) -> Result<bool> {
        let inserted = self.conn().execute(
            "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT(height) DO NOTHING",
            &[
                &u64_to_i64(block.height)?,
                &block.hash,
                &u64_to_i64(block.difficulty)?,
                &block.finder,
                &block.finder_worker,
                &u64_to_i64(block.reward)?,
                &to_unix(block.timestamp),
                &block.confirmed,
                &block.orphaned,
                &block.paid_out,
                &block.effort_pct,
            ],
        )?;
        Ok(inserted > 0)
    }

    pub(crate) fn archive_conflicting_block_and_replace(&self, block: &DbBlock) -> Result<bool> {
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let existing_row = tx.query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE height = $1
             FOR UPDATE",
            &[&u64_to_i64(block.height)?],
        )?;
        let Some(existing_row) = existing_row else {
            tx.execute(
                "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    &u64_to_i64(block.height)?,
                    &block.hash,
                    &u64_to_i64(block.difficulty)?,
                    &block.finder,
                    &block.finder_worker,
                    &u64_to_i64(block.reward)?,
                    &to_unix(block.timestamp),
                    &block.confirmed,
                    &block.orphaned,
                    &block.paid_out,
                    &block.effort_pct,
                ],
            )?;
            tx.commit()?;
            return Ok(true);
        };

        let existing = row_to_block(&existing_row);
        if existing.hash == block.hash {
            return Ok(false);
        }

        let height_i64 = u64_to_i64(block.height)?;
        Self::archive_block_credit_state(&mut tx, &existing, now_unix())?;
        tx.execute(
            "DELETE FROM block_credit_events WHERE block_height = $1",
            &[&height_i64],
        )?;
        tx.execute(
            "DELETE FROM pool_fee_balance_credits WHERE block_height = $1",
            &[&height_i64],
        )?;
        tx.execute(
            "DELETE FROM pool_fee_events WHERE block_height = $1",
            &[&height_i64],
        )?;
        tx.execute(
            "UPDATE blocks
             SET hash = $2,
                 difficulty = $3,
                 finder = $4,
                 finder_worker = $5,
                 reward = $6,
                 timestamp = $7,
                 confirmed = $8,
                 orphaned = $9,
                 paid_out = $10,
                 effort_pct = $11
             WHERE height = $1",
            &[
                &height_i64,
                &block.hash,
                &u64_to_i64(block.difficulty)?,
                &block.finder,
                &block.finder_worker,
                &u64_to_i64(block.reward)?,
                &to_unix(block.timestamp),
                &block.confirmed,
                &block.orphaned,
                &block.paid_out,
                &block.effort_pct,
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    fn archive_block_credit_state(
        tx: &mut Transaction<'_>,
        block: &DbBlock,
        archived_at: i64,
    ) -> Result<()> {
        let height_i64 = u64_to_i64(block.height)?;
        tx.execute(
            "UPDATE payout_credit_allocations
             SET block_hash = $2
             WHERE block_height = $1
               AND block_hash IS NULL",
            &[&height_i64, &block.hash],
        )?;
        tx.execute(
            "INSERT INTO archived_blocks (
                height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed,
                orphaned, paid_out, effort_pct, archived_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (height, hash) DO UPDATE SET
                 difficulty = EXCLUDED.difficulty,
                 finder = EXCLUDED.finder,
                 finder_worker = EXCLUDED.finder_worker,
                 reward = EXCLUDED.reward,
                 timestamp = EXCLUDED.timestamp,
                 confirmed = EXCLUDED.confirmed,
                 orphaned = EXCLUDED.orphaned,
                 paid_out = EXCLUDED.paid_out,
                 effort_pct = EXCLUDED.effort_pct,
                 archived_at = EXCLUDED.archived_at",
            &[
                &height_i64,
                &block.hash,
                &u64_to_i64(block.difficulty)?,
                &block.finder,
                &block.finder_worker,
                &u64_to_i64(block.reward)?,
                &to_unix(block.timestamp),
                &block.confirmed,
                &block.orphaned,
                &block.paid_out,
                &block.effort_pct,
                &archived_at,
            ],
        )?;
        tx.execute(
            "INSERT INTO archived_block_credit_events (
                block_height, block_hash, address, amount, paid_amount, reversible, archived_at
             )
             SELECT
                 block_height,
                 $2,
                 address,
                 amount,
                 paid_amount,
                 reversible,
                 $3
             FROM block_credit_events
             WHERE block_height = $1
             ON CONFLICT (block_height, block_hash, address) DO UPDATE SET
                 amount = EXCLUDED.amount,
                 paid_amount = EXCLUDED.paid_amount,
                 reversible = EXCLUDED.reversible,
                 archived_at = EXCLUDED.archived_at",
            &[&height_i64, &block.hash, &archived_at],
        )?;
        tx.execute(
            "INSERT INTO archived_pool_fee_balance_credits (
                block_height, block_hash, fee_address, amount, paid_amount, timestamp,
                credited_at, reversible, archived_at
             )
             SELECT
                 block_height,
                 $2,
                 fee_address,
                 amount,
                 paid_amount,
                 timestamp,
                 credited_at,
                 reversible,
                 $3
             FROM pool_fee_balance_credits
             WHERE block_height = $1
             ON CONFLICT (block_height, block_hash) DO UPDATE SET
                 fee_address = EXCLUDED.fee_address,
                 amount = EXCLUDED.amount,
                 paid_amount = EXCLUDED.paid_amount,
                 timestamp = EXCLUDED.timestamp,
                 credited_at = EXCLUDED.credited_at,
                 reversible = EXCLUDED.reversible,
                 archived_at = EXCLUDED.archived_at",
            &[&height_i64, &block.hash, &archived_at],
        )?;
        tx.execute(
            "INSERT INTO archived_pool_fee_events (
                block_height, block_hash, amount, fee_address, timestamp, archived_at
             )
             SELECT
                 block_height,
                 $2,
                 amount,
                 fee_address,
                 timestamp,
                 $3
             FROM pool_fee_events
             WHERE block_height = $1
             ON CONFLICT (block_height, block_hash) DO UPDATE SET
                 amount = EXCLUDED.amount,
                 fee_address = EXCLUDED.fee_address,
                 timestamp = EXCLUDED.timestamp,
                 archived_at = EXCLUDED.archived_at",
            &[&height_i64, &block.hash, &archived_at],
        )?;
        Ok(())
    }

    pub fn get_block(&self, height: u64) -> Result<Option<DbBlock>> {
        let row = self.conn().query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE height = $1",
            &[&u64_to_i64(height)?],
        )?;
        Ok(row.map(|v| row_to_block(&v)))
    }

    pub(crate) fn update_block(&self, block: &DbBlock) -> Result<()> {
        self.add_block(block)
    }

    pub fn get_recent_blocks(&self, limit: i64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks ORDER BY height DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_recent_blocks_up_to(&self, limit: i64, max_height: u64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE height <= $1
             ORDER BY height DESC
             LIMIT $2",
            &[&u64_to_i64(max_height)?, &limit],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_all_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks ORDER BY height DESC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_luck_rounds_page(&self, limit: i64, offset: i64) -> Result<(Vec<DbLuckRound>, u64)> {
        self.get_luck_rounds_page_up_to(limit, offset, i64::MAX as u64)
    }

    pub fn get_luck_rounds_page_up_to(
        &self,
        limit: i64,
        offset: i64,
        max_height: u64,
    ) -> Result<(Vec<DbLuckRound>, u64)> {
        let mut conn = self.conn();
        let max_height = u64_to_i64(max_height)?;
        let total: i64 = conn
            .query_one(
                "SELECT GREATEST(COUNT(*) - 1, 0)::bigint FROM blocks WHERE height <= $1",
                &[&max_height],
            )?
            .get(0);
        let rows = conn.query(
            "WITH paged_blocks AS MATERIALIZED (
                SELECT height, hash, difficulty, timestamp, confirmed, orphaned
                FROM blocks
                WHERE height <= $3
                  AND (timestamp, height) > (
                    SELECT timestamp, height FROM blocks WHERE height <= $3
                    ORDER BY timestamp, height LIMIT 1
                  )
                ORDER BY height DESC LIMIT $1 OFFSET $2
             )
             SELECT b.height, b.hash, b.difficulty, b.timestamp, b.confirmed, b.orphaned,
                    COALESCE(sw.round_work, 0)::bigint,
                    GREATEST(b.timestamp - prev.timestamp, 0)::bigint
             FROM paged_blocks b
             JOIN LATERAL (
                SELECT timestamp FROM blocks
                WHERE (timestamp, height) < (b.timestamp, b.height) AND height <= $3
                ORDER BY timestamp DESC, height DESC LIMIT 1
             ) prev ON TRUE
             LEFT JOIN LATERAL (
                SELECT share_read_work(prev.timestamp,b.timestamp) AS round_work
             ) sw ON TRUE
             ORDER BY b.height DESC",
            &[&limit.max(1), &offset.max(0), &max_height],
        )?;
        Ok((
            rows.iter().map(row_to_luck_round).collect(),
            total.max(0) as u64,
        ))
    }

    pub fn get_luck_rounds_for_hashes(
        &self,
        hashes: &[String],
    ) -> Result<HashMap<String, DbLuckRound>> {
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = self.conn().query(
            "SELECT b.height, b.hash, b.difficulty, b.timestamp, b.confirmed, b.orphaned,
                    COALESCE(sw.round_work, 0)::bigint,
                    GREATEST(b.timestamp - prev.timestamp, 0)::bigint
             FROM blocks b
             JOIN LATERAL (
                SELECT timestamp FROM blocks
                WHERE (timestamp, height) < (b.timestamp, b.height)
                ORDER BY timestamp DESC, height DESC LIMIT 1
             ) prev ON TRUE
             LEFT JOIN LATERAL (
                SELECT share_read_work(prev.timestamp,b.timestamp) AS round_work
             ) sw ON TRUE
             WHERE b.hash = ANY($1)",
            &[&hashes],
        )?;
        Ok(rows
            .iter()
            .map(|row| {
                let round = row_to_luck_round(row);
                (round.block_hash.clone(), round)
            })
            .collect())
    }

    pub fn avg_effort_pct_up_to(&self, max_height: u64) -> Result<Option<f64>> {
        let row = self.conn().query_one(
            "SELECT AVG(effort_pct)
             FROM blocks
             WHERE orphaned = FALSE
               AND effort_pct IS NOT NULL
               AND height <= $1",
            &[&u64_to_i64(max_height)?],
        )?;
        Ok(row.get::<_, Option<f64>>(0))
    }

    pub fn avg_effort_pct_since(&self, since: SystemTime) -> Result<(Option<f64>, u64)> {
        let since_ts = to_unix(since);
        let row = self.conn().query_one(
            "SELECT AVG(effort_pct), COUNT(*)::bigint
             FROM blocks
             WHERE orphaned = FALSE
               AND effort_pct IS NOT NULL
               AND timestamp >= $1",
            &[&since_ts],
        )?;
        let count: i64 = row.get(1);
        Ok((row.get::<_, Option<f64>>(0), count.max(0) as u64))
    }

    pub fn get_blocks_page_up_to(
        &self,
        max_height: u64,
        status: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<DbBlock>, u64)> {
        let max_height = u64_to_i64(max_height)?;

        let mut conn = self.conn();
        let total_row = conn.query_one(
            "SELECT COUNT(*)
             FROM blocks
             WHERE height <= $1
               AND (
                   $2::text IS NULL
                   OR ($2 = 'confirmed' AND confirmed = TRUE AND orphaned = FALSE)
                   OR ($2 = 'orphaned' AND orphaned = TRUE)
                   OR ($2 = 'pending' AND confirmed = FALSE AND orphaned = FALSE)
               )",
            &[&max_height, &status],
        )?;
        let total: i64 = total_row.get(0);

        let rows = conn.query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE height <= $1
               AND (
                   $2::text IS NULL
                   OR ($2 = 'confirmed' AND confirmed = TRUE AND orphaned = FALSE)
                   OR ($2 = 'orphaned' AND orphaned = TRUE)
                   OR ($2 = 'pending' AND confirmed = FALSE AND orphaned = FALSE)
               )
             ORDER BY height DESC
             LIMIT $3 OFFSET $4",
            &[&max_height, &status, &limit.max(0), &offset.max(0)],
        )?;
        let items = rows.into_iter().map(|row| row_to_block(&row)).collect();

        Ok((items, total.max(0) as u64))
    }

    pub fn get_unconfirmed_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE confirmed = FALSE AND orphaned = FALSE ORDER BY height ASC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub(crate) fn get_unpaid_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE confirmed = TRUE AND orphaned = FALSE AND paid_out = FALSE ORDER BY height ASC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_block_count_up_to(&self, max_height: u64) -> Result<u64> {
        let row = self.conn().query_one(
            "SELECT COUNT(*) FROM blocks WHERE height <= $1",
            &[&u64_to_i64(max_height)?],
        )?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn get_block_counts_since(&self, since_epoch: i64) -> Result<(u64, u64)> {
        let row = self.conn().query_one(
            "SELECT
                 COUNT(*)::BIGINT,
                 COALESCE(SUM(CASE WHEN orphaned THEN 1 ELSE 0 END), 0)::BIGINT
             FROM blocks
             WHERE \"timestamp\" >= $1",
            &[&since_epoch],
        )?;
        let total: i64 = row.get(0);
        let orphaned: i64 = row.get(1);
        Ok((total.max(0) as u64, orphaned.max(0) as u64))
    }

    pub fn get_unique_block_identity_counts(&self) -> Result<(u64, u64)> {
        let row = self.conn().query_one(
            "SELECT
                 COUNT(*)::BIGINT,
                 COALESCE(SUM(CASE WHEN orphaned THEN 1 ELSE 0 END), 0)::BIGINT
             FROM (
                 SELECT
                     height,
                     hash,
                     BOOL_OR(orphaned) AS orphaned
                 FROM (
                     SELECT height, hash, orphaned FROM blocks
                     UNION ALL
                     SELECT height, hash, orphaned FROM archived_blocks
                 ) combined
                 GROUP BY height, hash
             ) unique_blocks",
            &[],
        )?;
        let total: i64 = row.get(0);
        let orphaned: i64 = row.get(1);
        Ok((total.max(0) as u64, orphaned.max(0) as u64))
    }

    /// Returns (confirmed_non_orphaned, orphaned, pending_non_orphaned).
    pub fn get_block_status_counts(&self) -> Result<(u64, u64, u64)> {
        let row = self.conn().query_one(
            "SELECT
                 COALESCE(SUM(CASE WHEN confirmed = TRUE AND orphaned = FALSE THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN orphaned = TRUE THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN confirmed = FALSE AND orphaned = FALSE THEN 1 ELSE 0 END), 0)
             FROM blocks",
            &[],
        )?;
        let confirmed: i64 = row.get(0);
        let orphaned: i64 = row.get(1);
        let pending: i64 = row.get(2);
        Ok((
            confirmed.max(0) as u64,
            orphaned.max(0) as u64,
            pending.max(0) as u64,
        ))
    }

    pub fn get_balance(&self, address: &str) -> Result<Balance> {
        let row = self.conn().query_opt(
            "SELECT address, pending, paid FROM balances WHERE address = $1",
            &[&address],
        )?;
        if let Some(row) = row {
            return Ok(Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            });
        }
        Ok(Balance {
            address: address.to_string(),
            pending: 0,
            paid: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn update_balance(&self, bal: &Balance) -> Result<()> {
        self.conn().execute(
            "INSERT INTO balances (address, pending, paid) VALUES ($1, $2, $3)
             ON CONFLICT(address) DO UPDATE SET pending = EXCLUDED.pending, paid = EXCLUDED.paid",
            &[
                &bal.address,
                &u64_to_i64(bal.pending)?,
                &u64_to_i64(bal.paid)?,
            ],
        )?;
        Ok(())
    }

    fn credit_pending_balance(
        tx: &mut Transaction<'_>,
        destination: &str,
        amount: u64,
    ) -> Result<u64> {
        if destination.is_empty() || amount == 0 {
            return Ok(0);
        }

        let offset_applied = Self::apply_manual_payout_offset(tx, destination, amount)?;
        let amount_to_pending = amount.saturating_sub(offset_applied);
        if amount_to_pending == 0 {
            return Ok(offset_applied);
        }

        let mut bal = Self::load_balance(tx, destination)?;
        bal.pending = bal
            .pending
            .checked_add(amount_to_pending)
            .ok_or_else(|| anyhow!("balance overflow"))?;

        Self::upsert_balance(tx, &bal)?;
        Ok(offset_applied)
    }

    fn debit_pending_balance(
        tx: &mut Transaction<'_>,
        destination: &str,
        amount: u64,
    ) -> Result<()> {
        if destination.is_empty() || amount == 0 {
            return Ok(());
        }

        let mut bal = Self::load_balance(tx, destination)?;
        if bal.pending < amount {
            return Err(anyhow!(
                "insufficient pending balance for {}: have {}, need {}",
                destination,
                bal.pending,
                amount
            ));
        }
        bal.pending -= amount;
        Self::upsert_balance(tx, &bal)?;
        Ok(())
    }

    /// Load a balance for read-modify-write inside `tx`, holding its row lock
    /// until the transaction ends.
    ///
    /// Every balance write (`upsert_balance`) stores absolute values computed
    /// from this read, so the lock is what keeps concurrent writers (payout
    /// completion in Stratum, admin imports/offsets/reverts in the API) from
    /// losing each other's updates. A missing row is created as zero first so
    /// two transactions crediting a new address also serialize on its lock.
    fn load_balance(tx: &mut Transaction<'_>, destination: &str) -> Result<Balance> {
        const SELECT_FOR_UPDATE: &str =
            "SELECT address, pending, paid FROM balances WHERE address = $1 FOR UPDATE";
        let mut row = tx.query_opt(SELECT_FOR_UPDATE, &[&destination])?;
        if row.is_none() {
            tx.execute(
                "INSERT INTO balances (address, pending, paid) VALUES ($1, 0, 0)
                 ON CONFLICT(address) DO NOTHING",
                &[&destination],
            )?;
            row = tx.query_opt(SELECT_FOR_UPDATE, &[&destination])?;
        }
        Ok(if let Some(row) = row {
            Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            }
        } else {
            Balance {
                address: destination.to_string(),
                pending: 0,
                paid: 0,
            }
        })
    }

    fn load_manual_payout_offset(tx: &mut Transaction<'_>, address: &str) -> Result<u64> {
        let row = tx.query_opt(
            "SELECT amount
             FROM manual_payout_offsets
             WHERE address = $1
             FOR UPDATE",
            &[&address],
        )?;
        Ok(row
            .map(|row| row.get::<_, i64>(0).max(0) as u64)
            .unwrap_or(0))
    }

    fn upsert_manual_payout_offset(
        tx: &mut Transaction<'_>,
        address: &str,
        amount: u64,
    ) -> Result<()> {
        if amount == 0 {
            tx.execute(
                "DELETE FROM manual_payout_offsets WHERE address = $1",
                &[&address],
            )?;
            return Ok(());
        }

        tx.execute(
            "INSERT INTO manual_payout_offsets (address, amount)
             VALUES ($1, $2)
             ON CONFLICT(address) DO UPDATE SET amount = EXCLUDED.amount",
            &[&address, &u64_to_i64(amount)?],
        )?;
        Ok(())
    }

    fn add_manual_payout_offset(
        tx: &mut Transaction<'_>,
        address: &str,
        amount: u64,
    ) -> Result<()> {
        if address.trim().is_empty() || amount == 0 {
            return Ok(());
        }
        // Relative upsert: a concurrent first offset for the same address
        // must add to, not overwrite, this one.
        tx.execute(
            "INSERT INTO manual_payout_offsets (address, amount)
             VALUES ($1, $2)
             ON CONFLICT(address) DO UPDATE
             SET amount = manual_payout_offsets.amount + EXCLUDED.amount",
            &[&address, &u64_to_i64(amount)?],
        )
        .with_context(|| format!("add manual payout offset for {address}"))?;
        Ok(())
    }

    fn apply_manual_payout_offset(
        tx: &mut Transaction<'_>,
        address: &str,
        amount: u64,
    ) -> Result<u64> {
        if address.trim().is_empty() || amount == 0 {
            return Ok(0);
        }
        let existing = Self::load_manual_payout_offset(tx, address)?;
        if existing == 0 {
            return Ok(0);
        }
        let applied = existing.min(amount);
        Self::upsert_manual_payout_offset(tx, address, existing.saturating_sub(applied))?;
        Ok(applied)
    }

    fn upsert_balance(tx: &mut Transaction<'_>, bal: &Balance) -> Result<()> {
        tx.execute(
            "INSERT INTO balances (address, pending, paid) VALUES ($1, $2, $3)
             ON CONFLICT(address) DO UPDATE SET pending = EXCLUDED.pending, paid = EXCLUDED.paid",
            &[
                &bal.address,
                &u64_to_i64(bal.pending)?,
                &u64_to_i64(bal.paid)?,
            ],
        )?;
        Ok(())
    }

    fn apply_pool_fee_balance_credit(
        tx: &mut Transaction<'_>,
        block_height: u64,
        destination: &str,
        amount: u64,
        timestamp: SystemTime,
        reversible: bool,
    ) -> Result<bool> {
        if amount == 0 {
            return Ok(false);
        }

        let destination = destination.trim();
        if destination.is_empty() {
            return Err(anyhow!("fee address is required"));
        }

        let inserted = tx.execute(
            "INSERT INTO pool_fee_balance_credits (block_height, fee_address, amount, paid_amount, timestamp, credited_at, reversible)
             VALUES ($1, $2, $3, 0, $4, $5, $6)
             ON CONFLICT(block_height) DO NOTHING",
            &[
                &u64_to_i64(block_height)?,
                &destination,
                &u64_to_i64(amount)?,
                &to_unix(timestamp),
                &now_unix(),
                &reversible,
            ],
        )?;
        if inserted == 0 {
            return Ok(false);
        }

        let offset_applied = Self::credit_pending_balance(tx, destination, amount)?;
        if offset_applied > 0 {
            tx.execute(
                "UPDATE pool_fee_balance_credits
                 SET paid_amount = $2
                 WHERE block_height = $1",
                &[&u64_to_i64(block_height)?, &u64_to_i64(offset_applied)?],
            )?;
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn apply_block_credits_and_mark_paid(
        &self,
        block_height: u64,
        credits: &[(String, u64)],
    ) -> Result<bool> {
        self.apply_block_credits_and_mark_paid_with_fee(block_height, credits, None)
    }

    pub fn apply_block_credits_and_mark_paid_with_fee(
        &self,
        block_height: u64,
        credits: &[(String, u64)],
        fee_record: Option<&PoolFeeRecord>,
    ) -> Result<bool> {
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;

        let block_state = tx.query_opt(
            "SELECT confirmed, orphaned, paid_out FROM blocks WHERE height = $1 FOR UPDATE",
            &[&u64_to_i64(block_height)?],
        )?;
        let Some(row) = block_state else {
            return Err(anyhow!("block {block_height} not found"));
        };
        let confirmed: bool = row.get(0);
        let orphaned: bool = row.get(1);
        let paid_out: bool = row.get(2);
        if paid_out {
            return Ok(false);
        }
        if !confirmed || orphaned {
            return Err(anyhow!("block {block_height} is not eligible for payout"));
        }

        for (address, amount) in credits {
            let destination = address.trim();
            if destination.is_empty() || *amount == 0 {
                continue;
            }

            tx.execute(
                "INSERT INTO block_credit_events (block_height, address, amount, paid_amount, reversible)
                 VALUES ($1, $2, $3, 0, TRUE)
                 ON CONFLICT(block_height, address) DO NOTHING",
                &[
                    &u64_to_i64(block_height)?,
                    &destination,
                    &u64_to_i64(*amount)?,
                ],
            )?;
            let offset_applied = Self::credit_pending_balance(&mut tx, destination, *amount)?;
            if offset_applied > 0 {
                tx.execute(
                    "UPDATE block_credit_events
                     SET paid_amount = $3
                     WHERE block_height = $1 AND address = $2",
                    &[
                        &u64_to_i64(block_height)?,
                        &destination,
                        &u64_to_i64(offset_applied)?,
                    ],
                )?;
            }
        }

        if let Some(fee) = fee_record {
            if fee.amount > 0 {
                let destination = fee.fee_address.trim();
                if destination.is_empty() {
                    return Err(anyhow!("fee address is required"));
                }

                let _ = Self::apply_pool_fee_balance_credit(
                    &mut tx,
                    block_height,
                    destination,
                    fee.amount,
                    fee.timestamp,
                    true,
                )?;

                tx.execute(
                    "INSERT INTO pool_fee_events (block_height, amount, fee_address, timestamp)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT(block_height) DO NOTHING",
                    &[
                        &u64_to_i64(block_height)?,
                        &u64_to_i64(fee.amount)?,
                        &destination,
                        &to_unix(fee.timestamp),
                    ],
                )?;
            }
        }

        let updated = tx.execute(
            "UPDATE blocks SET paid_out = TRUE WHERE height = $1 AND paid_out = FALSE",
            &[&u64_to_i64(block_height)?],
        )?;
        if updated == 0 {
            return Ok(false);
        }

        tx.commit()?;
        Ok(true)
    }

    pub fn get_all_balances(&self) -> Result<Vec<Balance>> {
        let rows = self.conn().query(
            "SELECT address, pending, paid FROM balances ORDER BY pending DESC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            })
            .collect())
    }

    pub fn list_balance_source_summaries(&self) -> Result<Vec<BalanceSourceSummary>> {
        self.balance_source_summaries(false)
    }

    /// Only unsettled credit sources are needed by the balance screens.
    pub fn list_pending_balance_source_summaries(&self) -> Result<Vec<BalanceSourceSummary>> {
        self.balance_source_summaries(true)
    }

    fn balance_source_summaries(&self, pending_only: bool) -> Result<Vec<BalanceSourceSummary>> {
        let sql = "WITH miner_sources AS (
                 SELECT
                     e.address AS address,
                     SUM(CASE WHEN b.orphaned THEN GREATEST(e.amount - e.paid_amount, 0) ELSE 0 END)::BIGINT AS orphan_pending,
                     SUM(CASE WHEN NOT b.orphaned THEN GREATEST(e.amount - e.paid_amount, 0) ELSE 0 END)::BIGINT AS canonical_pending
                 FROM block_credit_events e
                 JOIN blocks b ON b.height = e.block_height
                 {miner_filter}
                 GROUP BY e.address
             ),
             fee_sources AS (
                 SELECT
                     f.fee_address AS address,
                     SUM(CASE WHEN b.orphaned THEN GREATEST(f.amount - f.paid_amount, 0) ELSE 0 END)::BIGINT AS orphan_pending,
                     SUM(CASE WHEN NOT b.orphaned THEN GREATEST(f.amount - f.paid_amount, 0) ELSE 0 END)::BIGINT AS canonical_pending
                 FROM pool_fee_balance_credits f
                 JOIN blocks b ON b.height = f.block_height
                 {fee_filter}
                 GROUP BY f.fee_address
             )
             SELECT
                 address,
                 COALESCE(SUM(canonical_pending)::BIGINT, 0) AS canonical_pending,
                 COALESCE(SUM(orphan_pending)::BIGINT, 0) AS orphan_pending
             FROM (
                 SELECT address, canonical_pending, orphan_pending FROM miner_sources
                 UNION ALL
                 SELECT address, canonical_pending, orphan_pending FROM fee_sources
             ) sources
             GROUP BY address
             ORDER BY address ASC"
            .replace("{miner_filter}", if pending_only { "WHERE e.amount > e.paid_amount" } else { "" })
            .replace("{fee_filter}", if pending_only { "WHERE f.amount > f.paid_amount" } else { "" });
        let rows = self.conn().query(&sql, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| BalanceSourceSummary {
                address: row.get::<_, String>(0),
                canonical_pending: row.get::<_, i64>(1).max(0) as u64,
                orphan_pending: row.get::<_, i64>(2).max(0) as u64,
            })
            .collect())
    }

    pub(crate) fn live_reconciliation_blockers(&self) -> Result<LiveReconciliationBlockers> {
        let row = self.conn().query_one(
            "SELECT
                 COALESCE((
                     SELECT COUNT(*)
                     FROM blocks b
                     WHERE b.orphaned = TRUE
                       AND (
                           EXISTS (SELECT 1 FROM block_credit_events e WHERE e.block_height = b.height)
                           OR EXISTS (SELECT 1 FROM pool_fee_balance_credits f WHERE f.block_height = b.height)
                       )
                 ), 0)::BIGINT AS orphaned_block_issue_count,
                 COALESCE((
                     SELECT
                         COALESCE(SUM(block_unpaid + fee_unpaid)::BIGINT, 0)
                     FROM (
                         SELECT
                             COALESCE((
                                 SELECT SUM(GREATEST(e.amount - e.paid_amount, 0))::BIGINT
                                 FROM block_credit_events e
                                 WHERE e.block_height = b.height
                             ), 0) AS block_unpaid,
                             COALESCE((
                                 SELECT SUM(GREATEST(f.amount - f.paid_amount, 0))::BIGINT
                                 FROM pool_fee_balance_credits f
                                 WHERE f.block_height = b.height
                             ), 0) AS fee_unpaid
                         FROM blocks b
                         WHERE b.orphaned = TRUE
                           AND (
                               EXISTS (SELECT 1 FROM block_credit_events e WHERE e.block_height = b.height)
                               OR EXISTS (SELECT 1 FROM pool_fee_balance_credits f WHERE f.block_height = b.height)
                           )
                     ) orphaned
                 ), 0)::BIGINT AS orphaned_unpaid_amount,
                 COALESCE((
                     SELECT COUNT(*)
                     FROM payouts p
                     WHERE p.reverted_at IS NULL
                       AND p.reversible = FALSE
                       AND COALESCE(p.tx_hash, '') <> ''
                 ), 0)::BIGINT AS unresolved_completed_payout_rows",
            &[],
        )?;
        Ok(LiveReconciliationBlockers {
            orphaned_block_issue_count: row.get::<_, i64>(0).max(0) as u64,
            orphaned_unpaid_amount: row.get::<_, i64>(1).max(0) as u64,
            unresolved_completed_payout_rows: row.get::<_, i64>(2).max(0) as u64,
        })
    }

    #[cfg(test)]
    fn add_payout_with_batch(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
        batch_id: Option<&str>,
    ) -> Result<()> {
        self.conn().execute(
            "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp, batch_id, reverted_at, revert_reason, reversible)
             VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, FALSE)",
            &[
                &address,
                &u64_to_i64(amount)?,
                &u64_to_i64(fee)?,
                &tx_hash,
                &now_unix(),
                &batch_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_recent_payouts(&self, limit: i64) -> Result<Vec<Payout>> {
        let rows = self.conn().query(
            "SELECT address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
             FROM payouts
             WHERE reverted_at IS NULL
             ORDER BY id DESC
             LIMIT $1",
            &[&limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    #[cfg(test)]
    pub(crate) fn get_recent_payouts_for_address(
        &self,
        address: &str,
        limit: i64,
    ) -> Result<Vec<Payout>> {
        let rows = self.conn().query(
            "SELECT address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
             FROM payouts
             WHERE address = $1
               AND reverted_at IS NULL
             ORDER BY id DESC
             LIMIT $2",
            &[&address, &limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    #[cfg(test)]
    pub(crate) fn get_active_payouts(&self) -> Result<Vec<Payout>> {
        let rows = self.conn().query(
            "SELECT address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
             FROM payouts
             WHERE reverted_at IS NULL
             ORDER BY timestamp DESC, id DESC",
            &[],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub(crate) fn get_active_payout_tx_hash_batch(
        &self,
        after_payout_id: i64,
        limit: i64,
    ) -> Result<Vec<(i64, String)>> {
        let rows = self.conn().query(
            "SELECT first_payout_id, tx_hash
             FROM (
                 SELECT MIN(id) AS first_payout_id, tx_hash
                 FROM payouts
                 WHERE reverted_at IS NULL
                   AND BTRIM(tx_hash) <> ''
                 GROUP BY tx_hash
             ) active
             WHERE first_payout_id > $1
             ORDER BY first_payout_id ASC, tx_hash ASC
             LIMIT $2",
            &[&after_payout_id.max(0), &limit.max(1)],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
            .collect())
    }

    pub fn get_recent_visible_payouts_for_address(
        &self,
        address: &str,
        limit: i64,
    ) -> Result<Vec<Payout>> {
        let rows = self.conn().query(
            "SELECT address, amount, fee, tx_hash, timestamp, confirmed, batch_id
             FROM (
                 SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
                 FROM payouts
                 WHERE address = $1
                   AND reverted_at IS NULL
                 UNION ALL
                SELECT
                    0 AS id,
                    address,
                    amount,
                    COALESCE(fee, 0) AS fee,
                    COALESCE(tx_hash, '') AS tx_hash,
                    COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                    0 AS confirmed,
                    batch_id
                FROM pending_payouts
                WHERE address = $1
             ) visible
             ORDER BY timestamp DESC, confirmed ASC, id DESC
             LIMIT $2",
            &[&address, &limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_public_payout_batches_page(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<PublicPayoutBatch>, u64)> {
        let mut conn = self.conn();
        let rows = conn.query(
            "WITH batches AS MATERIALIZED (
                SELECT batch_key, MAX(timestamp) AS batch_ts
                FROM (
                    SELECT batch_id AS batch_key, timestamp FROM payouts
                    WHERE reverted_at IS NULL
                    UNION ALL
                    SELECT batch_id AS batch_key,
                           COALESCE(sent_at, send_started_at, initiated_at) AS timestamp
                    FROM pending_payouts
                    WHERE tx_hash IS NOT NULL AND BTRIM(tx_hash) <> ''
                ) visible_keys
                GROUP BY batch_key
             ), page_batches AS MATERIALIZED (
                SELECT batch_key,batch_ts FROM batches
                ORDER BY batch_ts DESC,batch_key LIMIT $1 OFFSET $2
             ), visible AS (
                SELECT
                    amount,
                    fee,
                    tx_hash,
                    timestamp,
                    1 AS confirmed,
                    batch_id AS batch_key
                FROM page_batches page JOIN payouts ON payouts.batch_id = page.batch_key
                WHERE reverted_at IS NULL
                UNION ALL
                SELECT amount,fee,tx_hash,timestamp,1 AS confirmed,batch_id AS batch_key
                FROM payouts
                WHERE reverted_at IS NULL AND batch_id IS NULL
                  AND EXISTS (SELECT 1 FROM page_batches WHERE batch_key IS NULL)
                UNION ALL
                SELECT
                    amount,
                    COALESCE(fee, 0)::bigint AS fee,
                    tx_hash,
                    COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                    0 AS confirmed,
                    batch_id AS batch_key
                FROM pending_payouts
                WHERE tx_hash IS NOT NULL
                  AND BTRIM(tx_hash) <> ''
                  AND (batch_id IN (SELECT batch_key FROM page_batches)
                       OR (batch_id IS NULL AND EXISTS (
                           SELECT 1 FROM page_batches WHERE batch_key IS NULL
                       )))
             ),
             grouped AS (
                SELECT
                    batch_key,
                    SUM(amount)::bigint AS total_amount,
                    SUM(fee)::bigint AS total_fee,
                    COUNT(*)::bigint AS recipient_count,
                    STRING_AGG(DISTINCT tx_hash, ',') AS tx_hashes,
                    MAX(timestamp)::bigint AS batch_ts,
                    MIN(confirmed)::bigint AS confirmed
                FROM visible
                GROUP BY batch_key
             )
             SELECT total_amount, total_fee, recipient_count, tx_hashes, batch_ts, confirmed, totals.n
             FROM (SELECT COUNT(*) AS n FROM batches) totals
             LEFT JOIN grouped ON TRUE
             ORDER BY batch_ts DESC, batch_key",
            &[&limit.max(0), &offset.max(0)],
        )?;
        let total: i64 = rows.first().map(|row| row.get(6)).unwrap_or(0);
        let items = rows
            .into_iter()
            .filter(|row| row.get::<_, Option<i64>>(2).is_some())
            .map(row_to_public_payout_batch)
            .collect::<Result<Vec<_>>>()?;

        Ok((items, total.max(0) as u64))
    }

    pub fn get_total_pool_fees(&self) -> Result<u64> {
        let row = self.conn().query_one(
            "SELECT COALESCE(SUM(e.amount)::BIGINT, 0)
             FROM pool_fee_events e
             LEFT JOIN blocks b ON b.height = e.block_height
             WHERE b.height IS NULL OR b.orphaned = FALSE",
            &[],
        )?;
        let total: i64 = row.get(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_total_confirmed_block_rewards(&self) -> Result<u64> {
        let row = self.conn().query_one(
            "SELECT COALESCE(SUM(reward)::BIGINT, 0)
             FROM blocks
             WHERE confirmed = TRUE AND orphaned = FALSE",
            &[],
        )?;
        let total: i64 = row.get(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_total_paid_to_miners(&self) -> Result<u64> {
        let row = self
            .conn()
            .query_one("SELECT COALESCE(SUM(paid)::BIGINT, 0) FROM balances", &[])?;
        let total: i64 = row.get(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_block_pool_fee_event(&self, block_height: u64) -> Result<Option<PoolFeeEvent>> {
        let row = self.conn().query_opt(
            "SELECT e.id, e.amount, e.fee_address
             FROM pool_fee_events e
             LEFT JOIN blocks b ON b.height = e.block_height
             WHERE e.block_height = $1
               AND (b.height IS NULL OR b.orphaned = FALSE)",
            &[&u64_to_i64(block_height)?],
        )?;
        Ok(row.map(|row| PoolFeeEvent {
            id: row.get::<_, i64>(0),
            amount: row.get::<_, i64>(1).max(0) as u64,
            fee_address: row.get::<_, String>(2),
        }))
    }

    pub fn get_block_credit_events(&self, block_height: u64) -> Result<Vec<BlockCreditEvent>> {
        let rows = self.conn().query(
            "SELECT address, amount, paid_amount, reversible
             FROM block_credit_events
             WHERE block_height = $1
             ORDER BY amount DESC, address ASC",
            &[&u64_to_i64(block_height)?],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| BlockCreditEvent {
                address: row.get::<_, String>(0),
                amount: row.get::<_, i64>(1).max(0) as u64,
                paid_amount: row.get::<_, i64>(2).max(0) as u64,
                reversible: row.get::<_, bool>(3),
            })
            .collect())
    }

    pub(crate) fn get_latest_non_orphan_block_up_to(
        &self,
        max_height: u64,
    ) -> Result<Option<DbBlock>> {
        let row = self.conn().query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE orphaned = FALSE AND height <= $1
             ORDER BY height DESC
             LIMIT 1",
            &[&u64_to_i64(max_height)?],
        )?;
        Ok(row.map(|row| row_to_block(&row)))
    }

    pub(crate) fn get_non_orphan_blocks_up_to(&self, max_height: u64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE orphaned = FALSE AND height <= $1
             ORDER BY height DESC",
            &[&u64_to_i64(max_height)?],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub(crate) fn get_non_orphan_blocks_from_height(
        &self,
        min_height: u64,
    ) -> Result<Vec<DbBlock>> {
        let rows = self.conn().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE orphaned = FALSE AND height >= $1
             ORDER BY height ASC",
            &[&u64_to_i64(min_height)?],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn list_orphaned_block_credit_issues(&self) -> Result<Vec<OrphanedBlockCreditIssue>> {
        let rows = self.conn().query(
            "SELECT
                 issue.height,
                 issue.hash,
                 issue.credit_event_count,
                 issue.credited_address_count,
                 issue.remaining_credit_amount,
                 issue.paid_credit_amount,
                 issue.remaining_fee_amount,
                 issue.paid_fee_amount,
                 issue.pending_payout_count,
                 issue.broadcast_pending_payout_count
             FROM (
                 SELECT
                     b.height,
                     b.hash,
                     COALESCE((SELECT COUNT(*) FROM block_credit_events e WHERE e.block_height = b.height), 0) AS credit_event_count,
                     COALESCE((SELECT COUNT(DISTINCT e.address) FROM block_credit_events e WHERE e.block_height = b.height), 0) AS credited_address_count,
                     COALESCE((
                         SELECT SUM(GREATEST(e.amount - e.paid_amount, 0))::BIGINT
                         FROM block_credit_events e
                         WHERE e.block_height = b.height
                     ), 0) AS remaining_credit_amount,
                     COALESCE((SELECT SUM(e.paid_amount)::BIGINT FROM block_credit_events e WHERE e.block_height = b.height), 0) AS paid_credit_amount,
                     COALESCE((
                         SELECT GREATEST(f.amount - f.paid_amount, 0)::BIGINT
                         FROM pool_fee_balance_credits f
                         WHERE f.block_height = b.height
                     ), 0) AS remaining_fee_amount,
                     COALESCE((SELECT f.paid_amount::BIGINT FROM pool_fee_balance_credits f WHERE f.block_height = b.height), 0) AS paid_fee_amount,
                     COALESCE((
                         SELECT COUNT(*)
                         FROM pending_payouts pp
                         WHERE pp.address IN (
                             SELECT e.address FROM block_credit_events e WHERE e.block_height = b.height
                             UNION
                             SELECT f.fee_address FROM pool_fee_balance_credits f WHERE f.block_height = b.height
                         )
                     ), 0) AS pending_payout_count,
                     COALESCE((
                         SELECT COUNT(*)
                         FROM pending_payouts pp
                         WHERE COALESCE(pp.tx_hash, '') <> ''
                           AND pp.address IN (
                               SELECT e.address FROM block_credit_events e WHERE e.block_height = b.height
                               UNION
                               SELECT f.fee_address FROM pool_fee_balance_credits f WHERE f.block_height = b.height
                           )
                     ), 0) AS broadcast_pending_payout_count
                 FROM blocks b
                 WHERE b.orphaned = TRUE
                   AND (
                       EXISTS (SELECT 1 FROM block_credit_events e WHERE e.block_height = b.height)
                       OR EXISTS (SELECT 1 FROM pool_fee_balance_credits f WHERE f.block_height = b.height)
                   )
             ) issue
             ORDER BY issue.height DESC, issue.hash DESC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| OrphanedBlockCreditIssue {
                height: row.get::<_, i64>(0).max(0) as u64,
                hash: row.get::<_, String>(1),
                credit_event_count: row.get::<_, i64>(2).max(0) as u64,
                credited_address_count: row.get::<_, i64>(3).max(0) as u64,
                remaining_credit_amount: row.get::<_, i64>(4).max(0) as u64,
                paid_credit_amount: row.get::<_, i64>(5).max(0) as u64,
                remaining_fee_amount: row.get::<_, i64>(6).max(0) as u64,
                paid_fee_amount: row.get::<_, i64>(7).max(0) as u64,
                pending_payout_count: row.get::<_, i64>(8).max(0) as u64,
                broadcast_pending_payout_count: row.get::<_, i64>(9).max(0) as u64,
            })
            .collect())
    }

    pub fn list_unreconciled_completed_payout_rows(
        &self,
    ) -> Result<Vec<UnreconciledCompletedPayoutRow>> {
        let rows = self.conn().query(
            "SELECT
                 p.id,
                 p.address,
                 p.amount,
                 p.fee,
                 p.tx_hash,
                 p.timestamp,
                 COALESCE((
                     SELECT SUM(a.amount)::BIGINT
                     FROM payout_credit_allocations a
                     WHERE a.payout_id = p.id
                 ), 0),
                 COALESCE((
                     SELECT SUM(a.amount)::BIGINT
                     FROM payout_credit_allocations a
                     LEFT JOIN blocks b
                       ON b.height = a.block_height
                      AND (a.block_hash IS NULL OR b.hash = a.block_hash)
                     LEFT JOIN archived_blocks archived
                       ON archived.height = a.block_height
                      AND a.block_hash IS NOT NULL
                      AND archived.hash = a.block_hash
                     WHERE a.payout_id = p.id
                       AND COALESCE(b.orphaned, archived.orphaned, FALSE) = TRUE
                 ), 0)
             FROM payouts p
             WHERE p.reverted_at IS NULL
               AND p.reversible = FALSE
               AND COALESCE(p.tx_hash, '') <> ''
             ORDER BY p.timestamp DESC, p.id DESC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| UnreconciledCompletedPayoutRow {
                payout_id: row.get::<_, i64>(0),
                address: row.get::<_, String>(1),
                amount: row.get::<_, i64>(2).max(0) as u64,
                fee: row.get::<_, i64>(3).max(0) as u64,
                tx_hash: row.get::<_, String>(4),
                timestamp: from_unix(row.get::<_, i64>(5)),
                linked_amount: row.get::<_, i64>(6).max(0) as u64,
                orphaned_linked_amount: row.get::<_, i64>(7).max(0) as u64,
            })
            .collect())
    }

    pub(crate) fn orphan_block_and_reverse_unpaid_credits(
        &self,
        block_height: u64,
    ) -> Result<OrphanBlockReconciliation> {
        self.reconcile_block_credit_state(block_height, false)
    }

    pub fn reconcile_existing_orphaned_block_credits(
        &self,
        block_height: u64,
    ) -> Result<OrphanBlockReconciliation> {
        self.reconcile_block_credit_state(block_height, true)
    }

    pub(crate) fn reconcile_all_existing_orphaned_block_credits(
        &self,
    ) -> Result<ExistingOrphanBlockReconciliationReport> {
        let rows = self.conn().query(
            "SELECT height
             FROM blocks b
             WHERE b.orphaned = TRUE
               AND (
                   EXISTS (SELECT 1 FROM block_credit_events e WHERE e.block_height = b.height)
                   OR EXISTS (SELECT 1 FROM pool_fee_balance_credits f WHERE f.block_height = b.height)
               )
             ORDER BY b.height ASC",
            &[],
        )?;

        let mut report = ExistingOrphanBlockReconciliationReport::default();
        for row in rows {
            let height = row.get::<_, i64>(0).max(0) as u64;
            let result = self
                .reconcile_existing_orphaned_block_credits(height)
                .with_context(|| format!("reconciling orphaned block height {height}"))?;
            report.blocks_scanned = report.blocks_scanned.saturating_add(1);
            if result.manual_reconciliation_required {
                report.blocks_requiring_manual_reconciliation = report
                    .blocks_requiring_manual_reconciliation
                    .saturating_add(1);
                continue;
            }
            if result.orphaned {
                report.blocks_reconciled = report.blocks_reconciled.saturating_add(1);
            }
            report.reversed_credit_events = report
                .reversed_credit_events
                .saturating_add(result.reversed_credit_events);
            report.reversed_credit_amount = report
                .reversed_credit_amount
                .saturating_add(result.reversed_credit_amount);
            report.reversed_fee_amount = report
                .reversed_fee_amount
                .saturating_add(result.reversed_fee_amount);
            report.canceled_pending_payouts = report
                .canceled_pending_payouts
                .saturating_add(result.canceled_pending_payouts);
        }

        Ok(report)
    }

    fn reconcile_block_credit_state(
        &self,
        block_height: u64,
        allow_existing_orphan: bool,
    ) -> Result<OrphanBlockReconciliation> {
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let Some(block_row) = tx.query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp,
                    confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE height = $1
             FOR UPDATE",
            &[&u64_to_i64(block_height)?],
        )?
        else {
            return Err(anyhow!("block {block_height} not found"));
        };
        let block = row_to_block(&block_row);
        let block_already_orphaned = block.orphaned;
        if block_already_orphaned && !allow_existing_orphan {
            return Ok(OrphanBlockReconciliation::default());
        }

        let credit_rows = tx.query(
            "SELECT address, amount, paid_amount, reversible
             FROM block_credit_events
             WHERE block_height = $1
             ORDER BY address ASC",
            &[&u64_to_i64(block_height)?],
        )?;
        let mut credit_events = Vec::with_capacity(credit_rows.len());
        let mut affected_addresses = Vec::<String>::with_capacity(credit_rows.len() + 1);
        let mut has_paid_sources = false;
        let mut can_auto_reconcile = true;
        let mut orphan_only_pending_addresses = HashMap::<String, bool>::new();
        for row in credit_rows {
            let address = row.get::<_, String>(0);
            let amount = row.get::<_, i64>(1).max(0) as u64;
            let paid_amount = row.get::<_, i64>(2).max(0) as u64;
            let reversible = row.get::<_, bool>(3);
            if amount > 0 && !affected_addresses.contains(&address) {
                affected_addresses.push(address.clone());
            }
            if paid_amount > 0 {
                has_paid_sources = true;
            }
            if !reversible && paid_amount > 0 {
                can_auto_reconcile = false;
            }
            credit_events.push((address, amount, paid_amount));
        }

        let fee_credit = tx.query_opt(
            "SELECT fee_address, amount, paid_amount, reversible
             FROM pool_fee_balance_credits
             WHERE block_height = $1",
            &[&u64_to_i64(block_height)?],
        )?;
        let fee_credit = fee_credit.map(|row| {
            let fee_address = row.get::<_, String>(0);
            let amount = row.get::<_, i64>(1).max(0) as u64;
            let paid_amount = row.get::<_, i64>(2).max(0) as u64;
            let reversible = row.get::<_, bool>(3);
            (fee_address, amount, paid_amount, reversible)
        });
        if let Some((fee_address, _, paid_amount, reversible)) = fee_credit.as_ref() {
            if !fee_address.is_empty() && !affected_addresses.contains(fee_address) {
                affected_addresses.push(fee_address.clone());
            }
            if *paid_amount > 0 {
                has_paid_sources = true;
            }
            if !*reversible && *paid_amount > 0 {
                can_auto_reconcile = false;
            }
        }

        let mut cancelable_pending_addresses = Vec::<String>::new();
        if !affected_addresses.is_empty() {
            for address in &affected_addresses {
                if address.trim().is_empty() {
                    continue;
                }
                if let Some(pending_row) = tx.query_opt(
                    "SELECT tx_hash FROM pending_payouts WHERE address = $1",
                    &[&address],
                )? {
                    let tx_hash = pending_row.get::<_, Option<String>>(0).unwrap_or_default();
                    if !tx_hash.trim().is_empty() {
                        can_auto_reconcile = false;
                    } else {
                        cancelable_pending_addresses.push(address.clone());
                    }
                }
            }

            if can_auto_reconcile {
                for (address, amount, paid_amount) in &credit_events {
                    let unpaid_amount = amount.saturating_sub(*paid_amount);
                    if unpaid_amount == 0 || address.trim().is_empty() {
                        continue;
                    }
                    let balance = Self::load_balance(&mut tx, address)?;
                    let allow_orphan_only_pending = if let Some(value) =
                        orphan_only_pending_addresses.get(address)
                    {
                        *value
                    } else {
                        let summary = Self::load_live_pending_source_summary(
                                &mut tx,
                                address,
                                Some(block_height),
                            )
                            .with_context(|| {
                                format!(
                                    "loading live pending source summary for {address} while reconciling block {block_height}"
                                )
                            })?;
                        let allow = summary.canonical_pending == 0;
                        orphan_only_pending_addresses.insert(address.clone(), allow);
                        allow
                    };
                    if balance.pending < unpaid_amount && !allow_orphan_only_pending {
                        can_auto_reconcile = false;
                        break;
                    }
                }
            }

            if can_auto_reconcile {
                if let Some((fee_address, amount, paid_amount, _)) = fee_credit.as_ref() {
                    let unpaid_amount = amount.saturating_sub(*paid_amount);
                    if unpaid_amount > 0 && !fee_address.trim().is_empty() {
                        let balance = Self::load_balance(&mut tx, fee_address)?;
                        let allow_orphan_only_pending = if let Some(value) =
                            orphan_only_pending_addresses.get(fee_address)
                        {
                            *value
                        } else {
                            let summary = Self::load_live_pending_source_summary(
                                    &mut tx,
                                    fee_address,
                                    Some(block_height),
                                )
                                .with_context(|| {
                                    format!(
                                        "loading live pending source summary for {fee_address} while reconciling block {block_height}"
                                    )
                                })?;
                            let allow = summary.canonical_pending == 0;
                            orphan_only_pending_addresses.insert(fee_address.clone(), allow);
                            allow
                        };
                        if balance.pending < unpaid_amount && !allow_orphan_only_pending {
                            can_auto_reconcile = false;
                        }
                    }
                }
            }
        }

        let mut result = OrphanBlockReconciliation {
            orphaned: true,
            manual_reconciliation_required: !can_auto_reconcile
                && (!credit_events.is_empty() || fee_credit.is_some()),
            ..OrphanBlockReconciliation::default()
        };

        if can_auto_reconcile {
            let archived_at = now_unix();
            if has_paid_sources {
                let archived_block = DbBlock {
                    confirmed: false,
                    orphaned: true,
                    ..block.clone()
                };
                Self::archive_block_credit_state(&mut tx, &archived_block, archived_at)
                    .with_context(|| {
                        format!("archiving paid orphan credit state for block {block_height}")
                    })?;
            }

            for address in &cancelable_pending_addresses {
                result.canceled_pending_payouts =
                    result.canceled_pending_payouts.saturating_add(tx.execute(
                        "DELETE FROM pending_payouts WHERE address = $1",
                        &[&address],
                    )?);
            }

            for (address, amount, paid_amount) in &credit_events {
                let unpaid_amount = amount.saturating_sub(*paid_amount);
                if unpaid_amount == 0 || address.trim().is_empty() {
                    continue;
                }
                let balance = Self::load_balance(&mut tx, address)?;
                let debit_amount = if orphan_only_pending_addresses
                    .get(address)
                    .copied()
                    .unwrap_or(false)
                {
                    balance.pending.min(unpaid_amount)
                } else {
                    unpaid_amount
                };
                if debit_amount > 0 {
                    Self::debit_pending_balance(&mut tx, address, debit_amount)?;
                    result.reversed_credit_amount =
                        result.reversed_credit_amount.saturating_add(debit_amount);
                }
                result.reversed_credit_events = result.reversed_credit_events.saturating_add(1);
            }
            if !credit_events.is_empty() {
                tx.execute(
                    "DELETE FROM block_credit_events WHERE block_height = $1",
                    &[&u64_to_i64(block_height)?],
                )?;
            }

            if let Some((fee_address, amount, paid_amount, _)) = fee_credit.as_ref() {
                let unpaid_amount = amount.saturating_sub(*paid_amount);
                if unpaid_amount > 0 && !fee_address.trim().is_empty() {
                    let balance = Self::load_balance(&mut tx, fee_address)?;
                    let debit_amount = if orphan_only_pending_addresses
                        .get(fee_address)
                        .copied()
                        .unwrap_or(false)
                    {
                        balance.pending.min(unpaid_amount)
                    } else {
                        unpaid_amount
                    };
                    if debit_amount > 0 {
                        Self::debit_pending_balance(&mut tx, fee_address, debit_amount)?;
                        result.reversed_fee_amount = debit_amount;
                    }
                }
                tx.execute(
                    "DELETE FROM pool_fee_balance_credits WHERE block_height = $1",
                    &[&u64_to_i64(block_height)?],
                )?;
            }
            if has_paid_sources {
                tx.execute(
                    "DELETE FROM pool_fee_events WHERE block_height = $1",
                    &[&u64_to_i64(block_height)?],
                )?;
            }
        }

        tx.execute(
            "UPDATE blocks
             SET orphaned = TRUE,
                 confirmed = FALSE,
                 paid_out = CASE WHEN $2 THEN FALSE ELSE paid_out END
             WHERE height = $1",
            &[&u64_to_i64(block_height)?, &(!has_paid_sources)],
        )?;
        tx.commit()?;
        Ok(result)
    }

    fn load_live_pending_source_summary(
        tx: &mut Transaction<'_>,
        address: &str,
        exclude_block_height: Option<u64>,
    ) -> Result<LivePendingSourceSummary> {
        let row = if let Some(excluded_height) = exclude_block_height {
            let excluded_height = u64_to_i64(excluded_height)?;
            tx.query_one(
                "SELECT COALESCE(SUM(pending_amount), 0)::BIGINT
                 FROM (
                     SELECT GREATEST(e.amount - e.paid_amount, 0)::BIGINT AS pending_amount
                     FROM block_credit_events e
                     JOIN blocks b ON b.height = e.block_height
                     WHERE e.address = $1
                       AND e.block_height <> $2
                       AND NOT b.orphaned
                     UNION ALL
                     SELECT GREATEST(f.amount - f.paid_amount, 0)::BIGINT AS pending_amount
                     FROM pool_fee_balance_credits f
                     JOIN blocks b ON b.height = f.block_height
                     WHERE f.fee_address = $1
                       AND f.block_height <> $2
                       AND NOT b.orphaned
                 ) sources",
                &[&address, &excluded_height],
            )?
        } else {
            tx.query_one(
                "SELECT COALESCE(SUM(pending_amount), 0)::BIGINT
                 FROM (
                     SELECT GREATEST(e.amount - e.paid_amount, 0)::BIGINT AS pending_amount
                     FROM block_credit_events e
                     JOIN blocks b ON b.height = e.block_height
                     WHERE e.address = $1
                       AND NOT b.orphaned
                     UNION ALL
                     SELECT GREATEST(f.amount - f.paid_amount, 0)::BIGINT AS pending_amount
                     FROM pool_fee_balance_credits f
                     JOIN blocks b ON b.height = f.block_height
                     WHERE f.fee_address = $1
                       AND NOT b.orphaned
                 ) sources",
                &[&address],
            )?
        };
        Ok(LivePendingSourceSummary {
            canonical_pending: row.get::<_, i64>(0).max(0) as u64,
        })
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        self.conn().execute(
            "INSERT INTO meta (key, value) VALUES ($1, $2)
             ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
            &[&key, &value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row = self
            .conn()
            .query_opt("SELECT value FROM meta WHERE key = $1", &[&key])?;
        Ok(row.map(|v| v.get::<_, Vec<u8>>(0)))
    }

    pub fn upsert_monitor_heartbeat(&self, heartbeat: &MonitorHeartbeatUpsert) -> Result<()> {
        let mut conn = self.conn();
        conn.execute(
            "INSERT INTO monitor_heartbeats (
                sampled_at, source, synthetic, api_up, stratum_up, db_up, daemon_up, public_http_up,
                daemon_syncing, chain_height, template_age_seconds, last_refresh_millis,
                stratum_snapshot_age_seconds, connected_miners, connected_workers,
                estimated_hashrate, wallet_up, last_accepted_share_at, last_accepted_share_age_seconds,
                payout_pending_count, payout_pending_amount,
                oldest_pending_payout_at, oldest_pending_payout_age_seconds,
                oldest_pending_send_started_at, oldest_pending_send_age_seconds,
                validation_candidate_queue_depth, validation_regular_queue_depth,
                summary_state, details_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12,
                $13, $14, $15,
                $16, $17, $18, $19,
                $20, $21,
                $22, $23,
                $24, $25,
                $26, $27,
                $28, $29
            )
            ON CONFLICT (source, sampled_at) DO UPDATE SET
                synthetic = EXCLUDED.synthetic,
                api_up = EXCLUDED.api_up,
                stratum_up = EXCLUDED.stratum_up,
                db_up = EXCLUDED.db_up,
                daemon_up = EXCLUDED.daemon_up,
                public_http_up = EXCLUDED.public_http_up,
                daemon_syncing = EXCLUDED.daemon_syncing,
                chain_height = EXCLUDED.chain_height,
                template_age_seconds = EXCLUDED.template_age_seconds,
                last_refresh_millis = EXCLUDED.last_refresh_millis,
                stratum_snapshot_age_seconds = EXCLUDED.stratum_snapshot_age_seconds,
                connected_miners = EXCLUDED.connected_miners,
                connected_workers = EXCLUDED.connected_workers,
                estimated_hashrate = EXCLUDED.estimated_hashrate,
                wallet_up = EXCLUDED.wallet_up,
                last_accepted_share_at = EXCLUDED.last_accepted_share_at,
                last_accepted_share_age_seconds = EXCLUDED.last_accepted_share_age_seconds,
                payout_pending_count = EXCLUDED.payout_pending_count,
                payout_pending_amount = EXCLUDED.payout_pending_amount,
                oldest_pending_payout_at = EXCLUDED.oldest_pending_payout_at,
                oldest_pending_payout_age_seconds = EXCLUDED.oldest_pending_payout_age_seconds,
                oldest_pending_send_started_at = EXCLUDED.oldest_pending_send_started_at,
                oldest_pending_send_age_seconds = EXCLUDED.oldest_pending_send_age_seconds,
                validation_candidate_queue_depth = EXCLUDED.validation_candidate_queue_depth,
                validation_regular_queue_depth = EXCLUDED.validation_regular_queue_depth,
                summary_state = EXCLUDED.summary_state,
                details_json = EXCLUDED.details_json",
            &[
                &to_unix(heartbeat.sampled_at),
                &heartbeat.source,
                &heartbeat.synthetic,
                &heartbeat.api_up,
                &heartbeat.stratum_up,
                &heartbeat.db_up,
                &heartbeat.daemon_up,
                &heartbeat.public_http_up,
                &heartbeat.daemon_syncing,
                &opt_u64_to_i64(heartbeat.chain_height)?,
                &opt_u64_to_i64(heartbeat.template_age_seconds)?,
                &opt_u64_to_i64(heartbeat.last_refresh_millis)?,
                &opt_u64_to_i64(heartbeat.stratum_snapshot_age_seconds)?,
                &opt_u64_to_i64(heartbeat.connected_miners)?,
                &opt_u64_to_i64(heartbeat.connected_workers)?,
                &heartbeat.estimated_hashrate,
                &heartbeat.wallet_up,
                &heartbeat.last_accepted_share_at.map(to_unix),
                &opt_u64_to_i64(heartbeat.last_accepted_share_age_seconds)?,
                &opt_u64_to_i64(heartbeat.payout_pending_count)?,
                &opt_u64_to_i64(heartbeat.payout_pending_amount)?,
                &heartbeat.oldest_pending_payout_at.map(to_unix),
                &opt_u64_to_i64(heartbeat.oldest_pending_payout_age_seconds)?,
                &heartbeat.oldest_pending_send_started_at.map(to_unix),
                &opt_u64_to_i64(heartbeat.oldest_pending_send_age_seconds)?,
                &opt_u64_to_i64(heartbeat.validation_candidate_queue_depth)?,
                &opt_u64_to_i64(heartbeat.validation_regular_queue_depth)?,
                &heartbeat.summary_state,
                &heartbeat.details_json,
            ],
        )?;
        Ok(())
    }

    pub fn get_latest_monitor_heartbeat(
        &self,
        source: Option<&str>,
    ) -> Result<Option<MonitorHeartbeat>> {
        let row = self.conn().query_opt(
            "SELECT sampled_at, api_up, stratum_up, db_up, daemon_up,
                    public_http_up, daemon_syncing, chain_height, template_age_seconds,
                    last_refresh_millis, summary_state
             FROM monitor_heartbeats
             WHERE ($1::text IS NULL OR source = $1)
             ORDER BY sampled_at DESC, id DESC
             LIMIT 1",
            &[&source],
        )?;
        row.map(row_to_monitor_heartbeat).transpose()
    }

    pub fn get_monitor_uptime_summary(
        &self,
        since: SystemTime,
        source: Option<&str>,
    ) -> Result<MonitorUptimeSummary> {
        let row = self.conn().query_one(
            "SELECT
                COUNT(*)::bigint AS sample_count,
                COUNT(api_up)::bigint AS api_total,
                COUNT(*) FILTER (WHERE api_up = TRUE)::bigint AS api_up,
                COUNT(stratum_up)::bigint AS stratum_total,
                COUNT(*) FILTER (WHERE stratum_up = TRUE)::bigint AS stratum_up,
                COUNT(*)::bigint AS pool_total,
                COUNT(*) FILTER (
                    WHERE COALESCE(api_up, FALSE) = TRUE
                      AND COALESCE(stratum_up, FALSE) = TRUE
                      AND db_up = TRUE
                      AND COALESCE(daemon_up, FALSE) = TRUE
                )::bigint AS pool_up,
                COUNT(daemon_up)::bigint AS daemon_total,
                COUNT(*) FILTER (WHERE daemon_up = TRUE)::bigint AS daemon_up,
                COUNT(*)::bigint AS database_total,
                COUNT(*) FILTER (WHERE db_up = TRUE)::bigint AS database_up,
                COUNT(public_http_up)::bigint AS public_http_total,
                COUNT(*) FILTER (WHERE public_http_up = TRUE)::bigint AS public_http_up
             FROM monitor_heartbeats
             WHERE sampled_at >= $1
               AND ($2::text IS NULL OR source = $2)",
            &[&to_unix(since), &source],
        )?;
        Ok(MonitorUptimeSummary {
            sample_count: row.get::<_, i64>(0).max(0) as u64,
            api_total: row.get::<_, i64>(1).max(0) as u64,
            api_up: row.get::<_, i64>(2).max(0) as u64,
            stratum_total: row.get::<_, i64>(3).max(0) as u64,
            stratum_up: row.get::<_, i64>(4).max(0) as u64,
            pool_total: row.get::<_, i64>(5).max(0) as u64,
            pool_up: row.get::<_, i64>(6).max(0) as u64,
            daemon_total: row.get::<_, i64>(7).max(0) as u64,
            daemon_up: row.get::<_, i64>(8).max(0) as u64,
            database_total: row.get::<_, i64>(9).max(0) as u64,
            database_up: row.get::<_, i64>(10).max(0) as u64,
            public_http_total: row.get::<_, i64>(11).max(0) as u64,
            public_http_up: row.get::<_, i64>(12).max(0) as u64,
        })
    }

    pub fn upsert_monitor_incident(&self, incident: &MonitorIncidentUpsert) -> Result<()> {
        self.conn().execute(
            "INSERT INTO monitor_incidents (
                dedupe_key, kind, severity, visibility, source, summary, detail, started_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (dedupe_key) WHERE ended_at IS NULL DO UPDATE SET
                kind = EXCLUDED.kind,
                severity = EXCLUDED.severity,
                visibility = EXCLUDED.visibility,
                source = EXCLUDED.source,
                summary = EXCLUDED.summary,
                detail = EXCLUDED.detail,
                updated_at = EXCLUDED.updated_at",
            &[
                &incident.dedupe_key,
                &incident.kind,
                &incident.severity,
                &incident.visibility,
                &incident.source,
                &incident.summary,
                &incident.detail,
                &to_unix(incident.started_at),
                &to_unix(incident.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn resolve_monitor_incident(&self, dedupe_key: &str, ended_at: SystemTime) -> Result<u64> {
        let updated = self.conn().execute(
            "UPDATE monitor_incidents
             SET ended_at = $2, updated_at = GREATEST(updated_at, $2)
             WHERE dedupe_key = $1
               AND ended_at IS NULL",
            &[&dedupe_key, &to_unix(ended_at)],
        )?;
        Ok(updated)
    }

    pub fn get_open_monitor_incidents(
        &self,
        visibility: Option<&str>,
    ) -> Result<Vec<MonitorIncident>> {
        let rows = self.conn().query(
            "SELECT id, kind, severity, summary, started_at, ended_at
             FROM monitor_incidents
             WHERE ended_at IS NULL
               AND ($1::text IS NULL OR visibility = $1)
             ORDER BY started_at DESC, id DESC",
            &[&visibility],
        )?;
        rows.into_iter().map(row_to_monitor_incident).collect()
    }

    pub fn get_recent_monitor_incidents(
        &self,
        limit: i64,
        visibility: Option<&str>,
    ) -> Result<Vec<MonitorIncident>> {
        let rows = self.conn().query(
            "SELECT id, kind, severity, summary, started_at, ended_at
             FROM monitor_incidents
             WHERE ($1::text IS NULL OR visibility = $1)
             ORDER BY COALESCE(ended_at, updated_at) DESC, id DESC
             LIMIT $2",
            &[&visibility, &limit.max(0)],
        )?;
        rows.into_iter().map(row_to_monitor_incident).collect()
    }

    pub fn delete_monitor_heartbeats_before(&self, before: SystemTime) -> Result<u64> {
        let deleted = self.conn().execute(
            "DELETE FROM monitor_heartbeats
             WHERE sampled_at < $1",
            &[&to_unix(before)],
        )?;
        Ok(deleted)
    }

    pub fn delete_resolved_monitor_incidents_before(&self, before: SystemTime) -> Result<u64> {
        let deleted = self.conn().execute(
            "DELETE FROM monitor_incidents
             WHERE ended_at IS NOT NULL
               AND ended_at < $1",
            &[&to_unix(before)],
        )?;
        Ok(deleted)
    }

    /// Claims `(share_key, nonce)` in `seen_shares`, where `share_key` is the
    /// template-scoped key (see `share_dedupe::template_share_key`). The claim
    /// is kept for [`TEMPLATE_SHARE_CLAIM_RETENTION`] after the share is
    /// persisted, so it doubles as the durable header-scoped dedupe record; it
    /// is deleted only after a retryable failure that wrote no share row.
    ///
    /// This used to look up `shares` by (assignment id, nonce) first. That is
    /// redundant: an assignment belongs to one template and lives minutes,
    /// templates are in-memory only, and every path that writes a settling
    /// row keeps this claim for hours. The lookup cost a random read on the
    /// largest table for every submit.
    pub(crate) fn try_claim_share(
        &self,
        _job_id: &str,
        share_key: &str,
        nonce: u64,
    ) -> Result<bool> {
        let now = now_unix();
        let expires_at = now + TEMPLATE_SHARE_CLAIM_RETENTION.as_secs() as i64;
        const CLAIM_SQL: &str =
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = EXCLUDED.expires_at
             WHERE seen_shares.expires_at <= $4";
        let nonce = u64_to_i64(nonce)?;
        let claimed = if self.async_share_commit() {
            let mut conn = self.conn();
            let mut tx = conn.transaction()?;
            tx.batch_execute("SET LOCAL synchronous_commit = off")?;
            let claimed = tx.execute(CLAIM_SQL, &[&share_key, &nonce, &expires_at, &now])?;
            tx.commit()?;
            claimed
        } else {
            self.conn()
                .execute(CLAIM_SQL, &[&share_key, &nonce, &expires_at, &now])?
        };
        Ok(claimed > 0)
    }

    pub(crate) fn release_share_claim(&self, share_key: &str, nonce: u64) -> Result<()> {
        self.conn().execute(
            "DELETE FROM seen_shares WHERE job_id = $1 AND nonce = $2",
            &[&share_key, &u64_to_i64(nonce)?],
        )?;
        Ok(())
    }

    pub(crate) fn clean_expired_seen_shares(&self) -> Result<u64> {
        let affected = self.conn().execute(
            "DELETE FROM seen_shares WHERE expires_at <= $1",
            &[&now_unix()],
        )?;
        Ok(affected)
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        let row = self.conn().query_opt(
            "SELECT address, strikes, strike_window_until, last_reason, last_event_at,
                    quarantined_until, force_verify_until
             FROM address_risk WHERE address = $1",
            &[&address],
        )?;
        Ok(row.map(|row| AddressRiskState {
            address: row.get::<_, String>(0),
            strikes: row.get::<_, i64>(1).max(0) as u64,
            strike_window_until: row.get::<_, Option<i64>>(2).map(from_unix),
            last_reason: row.get::<_, Option<String>>(3),
            last_event_at: row.get::<_, Option<i64>>(4).map(from_unix),
            quarantined_until: row.get::<_, Option<i64>>(5).map(from_unix),
            force_verify_until: row.get::<_, Option<i64>>(6).map(from_unix),
        }))
    }

    /// The address's risk row and validation hold deadline/cause in one round
    /// trip. Deadlines are returned unfiltered; submit admission distinguishes
    /// background-audit holds from mandatory synchronous verification.
    pub(crate) fn get_submit_risk(&self, address: &str) -> Result<SubmitRisk> {
        let row = self.conn().query_one(
            "SELECT r.address, r.strikes, r.strike_window_until, r.last_reason, r.last_event_at,
                    r.quarantined_until, r.force_verify_until, v.forced_until, v.hold_cause
             FROM (SELECT $1::text AS address) AS q
             LEFT JOIN address_risk r ON r.address = q.address
             LEFT JOIN validation_address_states v ON v.address = q.address",
            &[&address],
        )?;
        let state = row
            .get::<_, Option<String>>(0)
            .map(|address| AddressRiskState {
                address,
                strikes: row.get::<_, Option<i64>>(1).unwrap_or(0).max(0) as u64,
                strike_window_until: row.get::<_, Option<i64>>(2).map(from_unix),
                last_reason: row.get::<_, Option<String>>(3),
                last_event_at: row.get::<_, Option<i64>>(4).map(from_unix),
                quarantined_until: row.get::<_, Option<i64>>(5).map(from_unix),
                force_verify_until: row.get::<_, Option<i64>>(6).map(from_unix),
            });
        Ok(SubmitRisk {
            state,
            validation_forced_until: row.get::<_, Option<i64>>(7).map(from_unix),
            validation_hold_cause: validation_hold_cause_from_db(
                row.get::<_, Option<String>>(8).as_deref(),
            )?,
            force_verify: false,
        })
    }

    pub fn should_force_verify_address(
        &self,
        address: &str,
    ) -> Result<(bool, Option<AddressRiskState>)> {
        let state = self.get_address_risk(address)?;
        let now = SystemTime::now();
        let validation_forced_until = self.validation_forced_until_after(address, now)?;
        let force = state.as_ref().is_some_and(|s| {
            s.force_verify_until.is_some_and(|until| until > now)
                || s.quarantined_until.is_some_and(|until| until > now)
        }) || validation_forced_until.is_some();
        Ok((force, state))
    }

    pub fn validation_forced_until(&self, address: &str) -> Result<Option<SystemTime>> {
        self.validation_forced_until_after(address, SystemTime::now())
    }

    pub fn active_force_verify_addresses(
        &self,
        addresses: &[String],
        now: SystemTime,
    ) -> Result<HashSet<String>> {
        if addresses.is_empty() {
            return Ok(HashSet::new());
        }

        let rows = self.conn().query(
            "SELECT address
             FROM (
                 SELECT address
                 FROM address_risk
                 WHERE address = ANY($1)
                   AND (
                        (force_verify_until IS NOT NULL AND force_verify_until > $2)
                     OR (quarantined_until IS NOT NULL AND quarantined_until > $2)
                   )
                 UNION
                 SELECT address
                 FROM validation_address_states
                 WHERE address = ANY($1)
                   AND forced_until IS NOT NULL
                   AND forced_until > $2
             ) AS active_force_verify",
            &[&addresses, &to_unix(now)],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    }

    pub fn validation_hold_state(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
    ) -> Result<Option<ValidationHoldState>> {
        let now = SystemTime::now();
        let row = self.conn().query_opt(
            "SELECT s.forced_started_at,
                    s.forced_until,
                    s.hold_cause,
                    COALESCE((
                        SELECT COUNT(*)::bigint
                        FROM validation_provisionals vp
                        WHERE vp.address = s.address
                          AND vp.created_at > $2
                    ), 0)::bigint,
                    COALESCE((
                        SELECT SUM(sh.difficulty)::bigint
                        FROM shares sh
                        WHERE sh.miner = s.address
                          AND sh.created_at > $2
                          AND sh.status = 'verified'
                    ), 0)::bigint,
                    COALESCE((
                        SELECT SUM(sh.difficulty)::bigint
                        FROM shares sh
                        WHERE sh.miner = s.address
                          AND sh.created_at > $2
                          AND sh.status = 'provisional'
                    ), 0)::bigint
             FROM validation_address_states s
             WHERE s.address = $1",
            &[&address, &to_unix(provisional_cutoff)],
        )?;
        row.map(|row| {
            Ok(ValidationHoldState {
                forced_started_at: row.get::<_, Option<i64>>(0).map(from_unix),
                forced_until: row
                    .get::<_, Option<i64>>(1)
                    .map(from_unix)
                    .filter(|until| *until > now),
                hold_cause: validation_hold_cause_from_db(
                    row.get::<_, Option<String>>(2).as_deref(),
                )?,
                pending_provisional: row.get::<_, i64>(3).max(0) as u64,
                recent_verified_difficulty: row.get::<_, i64>(4).max(0) as u64,
                recent_provisional_difficulty: row.get::<_, i64>(5).max(0) as u64,
            })
        })
        .transpose()
    }

    fn validation_forced_until_after(
        &self,
        address: &str,
        now: SystemTime,
    ) -> Result<Option<SystemTime>> {
        let row = self.conn().query_opt(
            "SELECT forced_until
             FROM validation_address_states
             WHERE address = $1
               AND forced_until IS NOT NULL
               AND forced_until > $2",
            &[&address, &to_unix(now)],
        )?;
        Ok(row
            .and_then(|row| row.get::<_, Option<i64>>(0))
            .map(from_unix))
    }

    pub fn clear_address_risk_history(&self, address: &str) -> Result<()> {
        let now = now_unix();
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO validation_clear_events (address, cleared_at) VALUES ($1, $2)",
            &[&address, &now],
        )?;
        tx.execute("DELETE FROM address_risk WHERE address = $1", &[&address])?;
        tx.execute(
            "DELETE FROM validation_provisionals WHERE address = $1",
            &[&address],
        )?;
        tx.execute(
            "DELETE FROM validation_address_states WHERE address = $1",
            &[&address],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn list_active_verification_holds(
        &self,
        provisional_cutoff: SystemTime,
    ) -> Result<Vec<ActiveVerificationHold>> {
        let now = SystemTime::now();
        let now_ts = now_unix();
        let rows = self.conn().query(
            "SELECT COALESCE(r.address, v.address) AS address,
                    r.strikes,
                    r.last_reason,
                    r.last_event_at,
                    r.quarantined_until,
                    r.force_verify_until,
                    v.forced_until,
                    v.forced_started_at,
                    v.hold_cause,
                    COALESCE(vp.pending_provisional, 0)::bigint,
                    COALESCE(vs.recent_verified_difficulty, 0)::bigint,
                    COALESCE(vs.recent_provisional_difficulty, 0)::bigint
             FROM (
                 SELECT address, strikes, last_reason, last_event_at,
                        quarantined_until, force_verify_until
                 FROM address_risk
                 WHERE (quarantined_until IS NOT NULL AND quarantined_until > $1)
                    OR (force_verify_until IS NOT NULL AND force_verify_until > $1)
             ) r
             FULL OUTER JOIN (
                 SELECT address, forced_until, forced_started_at, hold_cause
                 FROM validation_address_states
                 WHERE forced_until IS NOT NULL AND forced_until > $1
             ) v
             ON r.address = v.address
             LEFT JOIN (
                 SELECT address, COUNT(*)::bigint AS pending_provisional
                 FROM validation_provisionals
                 WHERE created_at > $2
                 GROUP BY address
             ) vp
             ON COALESCE(r.address, v.address) = vp.address
             LEFT JOIN (
                 SELECT miner AS address,
                        COALESCE(SUM(CASE WHEN status = 'verified' THEN difficulty ELSE 0 END), 0)::bigint
                            AS recent_verified_difficulty,
                        COALESCE(SUM(CASE WHEN status = 'provisional' THEN difficulty ELSE 0 END), 0)::bigint
                            AS recent_provisional_difficulty
                 FROM shares
                 WHERE created_at > $2
                   AND status IN ('verified', 'provisional')
                 GROUP BY miner
             ) vs
             ON COALESCE(r.address, v.address) = vs.address
             ORDER BY address ASC",
            &[&now_ts, &to_unix(provisional_cutoff)],
        )?;
        rows.iter()
            .map(|row| {
                let last_reason = row.get::<_, Option<String>>(2);
                let validation_hold_cause =
                    validation_hold_cause_from_db(row.get::<_, Option<String>>(8).as_deref())?;
                let validation_pending_provisional = row.get::<_, i64>(9).max(0) as u64;
                let validation_recent_verified_difficulty = row.get::<_, i64>(10).max(0) as u64;
                let validation_recent_provisional_difficulty = row.get::<_, i64>(11).max(0) as u64;
                let reason = last_reason.clone().or_else(|| {
                    validation_hold_cause.map(|cause| {
                        cause.hold_reason(
                            validation_pending_provisional,
                            validation_recent_verified_difficulty,
                            validation_recent_provisional_difficulty,
                        )
                    })
                });
                Ok(ActiveVerificationHold {
                    address: row.get::<_, String>(0),
                    strikes: row.get::<_, Option<i64>>(1).unwrap_or_default().max(0) as u64,
                    last_reason,
                    reason,
                    last_event_at: row.get::<_, Option<i64>>(3).map(from_unix),
                    quarantined_until: row
                        .get::<_, Option<i64>>(4)
                        .map(from_unix)
                        .filter(|until| *until > now),
                    force_verify_until: row
                        .get::<_, Option<i64>>(5)
                        .map(from_unix)
                        .filter(|until| *until > now),
                    validation_forced_until: row
                        .get::<_, Option<i64>>(6)
                        .map(from_unix)
                        .filter(|until| *until > now),
                    validation_hold_cause,
                    validation_pending_provisional,
                    validation_recent_verified_difficulty,
                    validation_recent_provisional_difficulty,
                })
            })
            .collect()
    }

    pub fn escalate_address_risk(
        &self,
        escalation: AddressRiskEscalation<'_>,
    ) -> Result<AddressRiskState> {
        let AddressRiskEscalation {
            address,
            reason,
            strike_window_duration,
            quarantine_threshold,
            quarantine_base,
            quarantine_max,
            force_verify_duration,
        } = escalation;
        if address.trim().is_empty() {
            return Err(anyhow!("address is required"));
        }

        let now = SystemTime::now();
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO address_risk (address, strikes, strike_window_until, last_reason, last_event_at, quarantined_until, force_verify_until)
             VALUES ($1, 0, NULL, NULL, NULL, NULL, NULL)
             ON CONFLICT(address) DO NOTHING",
            &[&address],
        )?;
        let row = tx.query_one(
            "SELECT strikes, strike_window_until, quarantined_until, force_verify_until
             FROM address_risk WHERE address = $1 FOR UPDATE",
            &[&address],
        )?;
        let existing_strikes = row.get::<_, i64>(0).max(0) as u64;
        let existing_window = row.get::<_, Option<i64>>(1).map(from_unix);
        let existing_quarantine = row.get::<_, Option<i64>>(2).map(from_unix);
        let existing_force = row.get::<_, Option<i64>>(3).map(from_unix);

        let active_strikes = active_window_strikes(existing_strikes, existing_window, now);
        let strikes = active_strikes.saturating_add(1);
        let strike_window_until = merge_optional_later(
            existing_window.filter(|until| *until > now),
            if strike_window_duration.is_zero() {
                None
            } else {
                Some(now + strike_window_duration)
            },
        );

        let force_verify_until = merge_optional_later(
            existing_force,
            if force_verify_duration.is_zero() {
                None
            } else {
                Some(now + force_verify_duration)
            },
        );
        let quarantined_until = if quarantine_threshold > 0
            && strikes >= quarantine_threshold
            && !quarantine_base.is_zero()
        {
            let quarantine_strikes = strikes
                .saturating_sub(quarantine_threshold)
                .saturating_add(1);
            merge_optional_later(
                existing_quarantine,
                Some(quarantine_until_for_strikes(
                    now,
                    quarantine_strikes,
                    quarantine_base,
                    quarantine_max,
                )),
            )
        } else {
            existing_quarantine
        };

        tx.execute(
            "UPDATE address_risk
             SET strikes = $2,
                 strike_window_until = $3,
                 last_reason = $4,
                 last_event_at = $5,
                 quarantined_until = $6,
                 force_verify_until = $7
             WHERE address = $1",
            &[
                &address,
                &u64_to_i64(strikes)?,
                &strike_window_until.map(to_unix),
                &reason,
                &to_unix(now),
                &quarantined_until.map(to_unix),
                &force_verify_until.map(to_unix),
            ],
        )?;
        tx.commit()?;

        Ok(AddressRiskState {
            address: address.to_string(),
            strikes,
            strike_window_until,
            last_reason: Some(reason.to_string()),
            last_event_at: Some(now),
            quarantined_until,
            force_verify_until,
        })
    }

    pub(crate) fn create_pending_payout(&self, address: &str, amount: u64) -> Result<()> {
        self.conn().execute(
            "INSERT INTO pending_payouts (
                address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id
             ) VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, NULL)
             ON CONFLICT(address) DO UPDATE
             SET amount = EXCLUDED.amount,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL,
                 batch_id = NULL,
                 send_daemon = NULL
             WHERE pending_payouts.send_started_at IS NULL",
            &[&address, &u64_to_i64(amount)?, &now_unix()],
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn mark_pending_payout_send_started(
        &self,
        address: &str,
    ) -> Result<Option<PendingPayout>> {
        let Some(pending) = self.get_pending_payout(address)? else {
            return Ok(None);
        };
        let mut rows = self.mark_pending_payouts_send_started_batch(
            &[(address.to_string(), pending.amount)],
            &format!("test-batch:{address}"),
            None,
        )?;
        Ok(rows.pop())
    }

    /// Atomically freeze every planned batch member before the wallet send.
    ///
    /// Each row must still be unclaimed (no send in flight, no broadcast) and
    /// hold exactly the planned amount; otherwise nothing is frozen and an
    /// error is returned, so a concurrent payout process or a changed amount
    /// can never be sent under this batch id.
    pub(crate) fn mark_pending_payouts_send_started_batch(
        &self,
        members: &[(String, u64)],
        batch_id: &str,
        send_daemon: Option<&str>,
    ) -> Result<Vec<PendingPayout>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let batch_id = batch_id.trim();
        if batch_id.is_empty() {
            return Err(anyhow!("payout batch id is required to freeze a send"));
        }
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let started_at = now_unix();
        let mut rows = Vec::with_capacity(members.len());
        for (address, amount) in members {
            let row = tx.query_opt(
                "UPDATE pending_payouts
                 SET send_started_at = $2,
                     batch_id = $3,
                     send_daemon = $5
                 WHERE address = $1
                   AND amount = $4
                   AND send_started_at IS NULL
                   AND tx_hash IS NULL
                 RETURNING address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id, send_daemon",
                &[
                    &address,
                    &started_at,
                    &batch_id,
                    &u64_to_i64(*amount)?,
                    &send_daemon,
                ],
            )?;
            let Some(row) = row else {
                // Dropping the transaction rolls back members frozen so far.
                return Err(anyhow!(
                    "pending payout for {address} (amount {amount}) is missing, changed, or already claimed by another send; refusing to freeze batch {batch_id}"
                ));
            };
            rows.push(row_to_pending_payout(&row));
        }
        tx.commit()?;
        Ok(rows)
    }

    #[cfg(test)]
    pub(crate) fn record_pending_payout_broadcast(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
    ) -> Result<PendingPayout> {
        let mut recorded = self.record_pending_payout_batch_broadcast(
            &[PendingPayoutBatchMember {
                address: address.to_string(),
                amount,
                fee,
            }],
            &format!("test-batch:{address}"),
            tx_hash,
        )?;
        recorded
            .pop()
            .ok_or_else(|| anyhow!("no pending payout for {address}"))
    }

    pub(crate) fn record_pending_payout_batch_broadcast(
        &self,
        members: &[PendingPayoutBatchMember],
        batch_id: &str,
        tx_hash: &str,
    ) -> Result<Vec<PendingPayout>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let sent_at = now_unix();
        let batch_id = batch_id.trim();
        let explicit_batch_id = (!batch_id.is_empty()).then_some(batch_id);
        let mut recorded = Vec::with_capacity(members.len());
        for member in members {
            let pending = tx.query_opt(
                "SELECT amount, tx_hash, fee, batch_id FROM pending_payouts WHERE address = $1",
                &[&member.address],
            )?;
            let Some(pending_row) = pending else {
                return Err(anyhow!("no pending payout for {}", member.address));
            };
            let pending_amount = pending_row.get::<_, i64>(0).max(0) as u64;
            if pending_amount != member.amount {
                return Err(anyhow!(
                    "pending payout amount mismatch for {}: expected={}, requested={}",
                    member.address,
                    pending_amount,
                    member.amount
                ));
            }
            let existing_tx_hash = pending_row.get::<_, Option<String>>(1);
            if let Some(existing_tx_hash) = existing_tx_hash.as_deref() {
                if existing_tx_hash != tx_hash {
                    return Err(anyhow!(
                        "pending payout tx mismatch for {}: expected={}, requested={}",
                        member.address,
                        existing_tx_hash,
                        tx_hash
                    ));
                }
            }
            let existing_fee_raw = pending_row.get::<_, Option<i64>>(2);
            if let Some(existing_fee_raw) = existing_fee_raw {
                let existing_fee = existing_fee_raw.max(0) as u64;
                if existing_fee != member.fee {
                    return Err(anyhow!(
                        "pending payout fee mismatch for {}: expected={}, requested={}",
                        member.address,
                        existing_fee,
                        member.fee
                    ));
                }
            }
            let existing_batch_id = pending_row.get::<_, Option<String>>(3);
            if let Some(existing_batch_id) = existing_batch_id.as_deref() {
                if explicit_batch_id
                    .filter(|candidate| *candidate != existing_batch_id)
                    .is_some()
                {
                    return Err(anyhow!(
                        "pending payout batch mismatch for {}: expected={}, requested={}",
                        member.address,
                        existing_batch_id,
                        batch_id
                    ));
                }
            }

            tx.execute(
                "UPDATE pending_payouts
                 SET send_started_at = COALESCE(send_started_at, $2),
                     tx_hash = $3,
                     fee = $4,
                     sent_at = COALESCE(sent_at, $2),
                     batch_id = COALESCE(batch_id, $5)
                 WHERE address = $1",
                &[
                    &member.address,
                    &sent_at,
                    &tx_hash,
                    &u64_to_i64(member.fee)?,
                    &explicit_batch_id,
                ],
            )?;

            let row = tx.query_one(
                "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id, send_daemon
                 FROM pending_payouts
                 WHERE address = $1",
                &[&member.address],
            )?;
            recorded.push(row_to_pending_payout(&row));
        }
        tx.commit()?;
        Ok(recorded)
    }

    #[cfg(test)]
    pub(crate) fn reset_pending_payout_send_state(&self, address: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE pending_payouts
             SET send_started_at = NULL,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL,
                 batch_id = NULL,
                 send_daemon = NULL
             WHERE address = $1",
            &[&address],
        )?;
        Ok(())
    }

    /// Forget which daemon a batch went to (the daemon routing changed while
    /// the send was in flight), so recovery holds it instead of resetting.
    pub(crate) fn clear_pending_payout_batch_send_daemon(&self, batch_id: &str) -> Result<u64> {
        if batch_id.trim().is_empty() {
            return Ok(0);
        }
        let updated = self.conn().execute(
            "UPDATE pending_payouts SET send_daemon = NULL WHERE batch_id = $1",
            &[&batch_id],
        )?;
        Ok(updated)
    }

    pub(crate) fn reset_pending_payout_batch_send_state(&self, batch_id: &str) -> Result<u64> {
        if batch_id.trim().is_empty() {
            return Ok(0);
        }
        let updated = self.conn().execute(
            "UPDATE pending_payouts
             SET send_started_at = NULL,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL,
                 batch_id = NULL,
                 send_daemon = NULL
             WHERE batch_id = $1",
            &[&batch_id],
        )?;
        Ok(updated)
    }

    fn insert_completed_payout_row(
        tx: &mut Transaction<'_>,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
        timestamp: SystemTime,
        batch_id: Option<&str>,
    ) -> Result<i64> {
        let row = tx.query_one(
            "INSERT INTO payouts (
                address, amount, fee, tx_hash, timestamp, batch_id, reverted_at, revert_reason, reversible
             )
             VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, FALSE)
             RETURNING id",
            &[
                &address,
                &u64_to_i64(amount)?,
                &u64_to_i64(fee)?,
                &tx_hash,
                &to_unix(timestamp),
                &batch_id,
            ],
        )?;
        Ok(row.get::<_, i64>(0))
    }

    fn insert_payout_credit_allocations(
        tx: &mut Transaction<'_>,
        payout_id: i64,
        allocations: &[PayoutCreditAllocation],
    ) -> Result<()> {
        for allocation in allocations {
            tx.execute(
                "INSERT INTO payout_credit_allocations (
                    payout_id, source_kind, block_height, block_hash, address, amount
                 ) VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT(payout_id, source_kind, block_height, address) DO UPDATE
                 SET block_hash = COALESCE(EXCLUDED.block_hash, payout_credit_allocations.block_hash),
                     amount = EXCLUDED.amount",
                &[
                    &payout_id,
                    &allocation.source_kind.as_str(),
                    &u64_to_i64(allocation.block_height)?,
                    &allocation.block_hash,
                    &allocation.address,
                    &u64_to_i64(allocation.amount)?,
                ],
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn complete_pending_payout(
        &self,
        address: &str,
        _amount: u64,
        _fee: u64,
        tx_hash: &str,
    ) -> Result<()> {
        let batch_id = self
            .get_pending_payout(address)?
            .and_then(|pending| pending.batch_id)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("pending payout missing batch_id for {address}"))?;
        self.complete_pending_payout_batch(&batch_id, tx_hash)
    }

    pub(crate) fn complete_pending_payout_batch(
        &self,
        batch_id: &str,
        tx_hash: &str,
    ) -> Result<()> {
        if batch_id.trim().is_empty() {
            return Err(anyhow!("batch_id is required"));
        }
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;

        let rows = tx.query(
            "SELECT address, amount, initiated_at, send_started_at, sent_at, tx_hash, fee, batch_id
             FROM pending_payouts
             WHERE batch_id = $1
             ORDER BY address ASC
             FOR UPDATE",
            &[&batch_id],
        )?;
        if rows.is_empty() {
            return Err(anyhow!("no pending payouts for batch {batch_id}"));
        }

        for row in rows {
            let address = row.get::<_, String>(0);
            let amount = row.get::<_, i64>(1).max(0) as u64;
            let initiated_at = from_unix(row.get::<_, i64>(2));
            let send_started_at = row.get::<_, Option<i64>>(3).map(from_unix);
            let sent_at = row.get::<_, Option<i64>>(4).map(from_unix);
            if let Some(pending_tx_hash) = row.get::<_, Option<String>>(5).as_deref() {
                if pending_tx_hash != tx_hash {
                    return Err(anyhow!(
                        "pending payout tx mismatch for {}: expected={}, requested={}",
                        address,
                        pending_tx_hash,
                        tx_hash
                    ));
                }
            }
            let fee = row
                .get::<_, Option<i64>>(6)
                .map(|value| value.max(0) as u64)
                .unwrap_or_default();

            let mut balance = Self::load_balance(&mut tx, &address)?;
            if balance.pending < amount {
                return Err(anyhow!("insufficient balance for {}", address));
            }
            balance.pending -= amount;
            balance.paid = balance
                .paid
                .checked_add(amount)
                .ok_or_else(|| anyhow!("paid overflow"))?;

            Self::upsert_balance(&mut tx, &balance)?;
            let payout_timestamp = sent_at.or(send_started_at).unwrap_or(initiated_at);
            let payout_id = Self::insert_completed_payout_row(
                &mut tx,
                &address,
                amount,
                fee,
                tx_hash,
                payout_timestamp,
                Some(batch_id),
            )?;
            let allocations = Self::mark_balance_credit_sources_paid(&mut tx, &address, amount)?;
            let allocated_amount = allocations.iter().fold(0u64, |acc, allocation| {
                acc.saturating_add(allocation.amount)
            });
            Self::insert_payout_credit_allocations(&mut tx, payout_id, &allocations)?;
            tx.execute(
                "UPDATE payouts SET reversible = $2 WHERE id = $1",
                &[&payout_id, &(allocated_amount == amount)],
            )?;
        }

        tx.execute(
            "DELETE FROM pending_payouts WHERE batch_id = $1",
            &[&batch_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn import_confirmed_payout_txs(
        &self,
        payout_txs: &[ConfirmedPayoutImportTx],
    ) -> Result<ConfirmedPayoutImportReport> {
        if payout_txs.is_empty() {
            return Ok(ConfirmedPayoutImportReport::default());
        }

        let mut requested_tx_hashes = Vec::with_capacity(payout_txs.len());
        let mut seen_tx_hashes = HashSet::<String>::new();
        for payout_tx in payout_txs {
            let tx_hash = payout_tx.tx_hash.trim();
            if tx_hash.is_empty() {
                return Err(anyhow!("tx hash is required"));
            }
            if !seen_tx_hashes.insert(tx_hash.to_string()) {
                return Err(anyhow!("duplicate payout tx hash {tx_hash}"));
            }
            if payout_tx.recipients.is_empty() {
                return Err(anyhow!("no payout recipients provided for tx {tx_hash}"));
            }
            requested_tx_hashes.push(tx_hash.to_string());
        }

        let mut conn = self.conn();
        let mut tx = conn.transaction()?;

        let existing_rows = tx.query(
            "SELECT DISTINCT tx_hash
             FROM payouts
             WHERE tx_hash = ANY($1)",
            &[&requested_tx_hashes],
        )?;
        if let Some(existing_tx_hash) = existing_rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .next()
        {
            return Err(anyhow!(
                "payout tx {} already exists in the ledger",
                existing_tx_hash
            ));
        }

        let mut report = ConfirmedPayoutImportReport::default();
        for payout_tx in payout_txs {
            let tx_hash = payout_tx.tx_hash.trim();
            let mut imported_addresses = Vec::<String>::with_capacity(payout_tx.recipients.len());

            for recipient in &payout_tx.recipients {
                let address = recipient.address.trim();
                if address.is_empty() {
                    return Err(anyhow!(
                        "payout recipient address is required for tx {tx_hash}"
                    ));
                }
                if recipient.amount == 0 {
                    return Err(anyhow!(
                        "payout recipient amount must be positive for {} in tx {}",
                        address,
                        tx_hash
                    ));
                }

                let mut balance = Self::load_balance(&mut tx, address)?;
                let available_pending = balance.pending.min(recipient.amount);
                let manual_offset_amount = recipient.amount.saturating_sub(available_pending);
                balance.pending = balance.pending.saturating_sub(available_pending);
                balance.paid = balance
                    .paid
                    .checked_add(recipient.amount)
                    .ok_or_else(|| anyhow!("paid overflow while importing tx {}", tx_hash))?;

                if manual_offset_amount > 0 {
                    Self::add_manual_payout_offset(&mut tx, address, manual_offset_amount)?;
                    report.recorded_manual_offset_amount = report
                        .recorded_manual_offset_amount
                        .saturating_add(manual_offset_amount);
                }
                Self::upsert_balance(&mut tx, &balance)?;
                let payout_id = Self::insert_completed_payout_row(
                    &mut tx,
                    address,
                    recipient.amount,
                    recipient.fee,
                    tx_hash,
                    payout_tx.timestamp,
                    None,
                )?;
                let allocations =
                    Self::mark_balance_credit_sources_paid(&mut tx, address, available_pending)?;
                let allocated_amount = allocations.iter().fold(0u64, |acc, allocation| {
                    acc.saturating_add(allocation.amount)
                });
                Self::insert_payout_credit_allocations(&mut tx, payout_id, &allocations)?;
                tx.execute(
                    "UPDATE payouts SET reversible = $2 WHERE id = $1",
                    &[&payout_id, &(allocated_amount == recipient.amount)],
                )?;

                imported_addresses.push(address.to_string());
                report.imported_payout_rows = report.imported_payout_rows.saturating_add(1);
                report.imported_amount = report.imported_amount.saturating_add(recipient.amount);
                report.imported_fee = report.imported_fee.saturating_add(recipient.fee);
            }

            imported_addresses.sort();
            imported_addresses.dedup();
            report.canceled_pending_payouts =
                report.canceled_pending_payouts.saturating_add(tx.execute(
                    "DELETE FROM pending_payouts WHERE address = ANY($1)",
                    &[&imported_addresses],
                )?);
            report.imported_txs = report.imported_txs.saturating_add(1);
        }

        tx.commit()?;
        Ok(report)
    }

    pub fn apply_manual_payout_offsets_to_live_pending(
        &self,
    ) -> Result<ManualPayoutOffsetApplyReport> {
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let offset_rows = tx.query(
            "SELECT address, amount
             FROM manual_payout_offsets
             WHERE amount > 0
             ORDER BY address ASC
             FOR UPDATE",
            &[],
        )?;
        if offset_rows.is_empty() {
            return Ok(ManualPayoutOffsetApplyReport::default());
        }

        let mut report = ManualPayoutOffsetApplyReport::default();
        for row in offset_rows {
            let address = row.get::<_, String>(0);
            let offset_amount = row.get::<_, i64>(1).max(0) as u64;
            if address.trim().is_empty() || offset_amount == 0 {
                continue;
            }

            report.scanned_offset_addresses = report.scanned_offset_addresses.saturating_add(1);
            report.offset_amount_before = report.offset_amount_before.saturating_add(offset_amount);

            let mut balance = Self::load_balance(&mut tx, &address)?;
            let source_summary = Self::load_live_pending_source_summary(&mut tx, &address, None)?;
            let applied_amount = offset_amount
                .min(balance.pending)
                .min(source_summary.canonical_pending);
            if applied_amount == 0 {
                report.remaining_offset_amount =
                    report.remaining_offset_amount.saturating_add(offset_amount);
                continue;
            }

            let allocations =
                Self::mark_balance_credit_sources_paid(&mut tx, &address, applied_amount)?;
            let allocated_amount = allocations.iter().fold(0u64, |acc, allocation| {
                acc.saturating_add(allocation.amount)
            });
            if allocated_amount != applied_amount {
                return Err(anyhow!(
                    "manual payout offset application for {} allocated {} of {}",
                    address,
                    allocated_amount,
                    applied_amount
                ));
            }

            balance.pending = balance.pending.saturating_sub(applied_amount);
            Self::upsert_balance(&mut tx, &balance)?;

            let remaining_offset_amount = offset_amount.saturating_sub(applied_amount);
            Self::upsert_manual_payout_offset(&mut tx, &address, remaining_offset_amount)?;

            let remaining_sources =
                Self::load_live_pending_source_summary(&mut tx, &address, None)?;

            report.applied_address_count = report.applied_address_count.saturating_add(1);
            report.applied_amount = report.applied_amount.saturating_add(applied_amount);
            report.remaining_offset_amount = report
                .remaining_offset_amount
                .saturating_add(remaining_offset_amount);
            report.applications.push(ManualPayoutOffsetApplication {
                address,
                applied_amount,
                remaining_offset_amount,
                remaining_balance_pending: balance.pending,
                remaining_canonical_pending: remaining_sources.canonical_pending,
            });
        }

        tx.commit()?;
        Ok(report)
    }

    fn mark_balance_credit_sources_paid(
        tx: &mut Transaction<'_>,
        address: &str,
        amount: u64,
    ) -> Result<Vec<PayoutCreditAllocation>> {
        if address.trim().is_empty() || amount == 0 {
            return Ok(Vec::new());
        }

        let rows = tx.query(
            "SELECT source_kind, block_height, block_hash, amount, paid_amount
             FROM (
                 SELECT
                     'block'::text AS source_kind,
                     e.block_height,
                     b.hash AS block_hash,
                     e.amount,
                     e.paid_amount,
                     e.reversible
                 FROM block_credit_events e
                 JOIN blocks b ON b.height = e.block_height
                 WHERE address = $1
                   AND e.paid_amount < e.amount
                 UNION ALL
                 SELECT
                     'fee'::text AS source_kind,
                     f.block_height,
                     b.hash AS block_hash,
                     f.amount,
                     f.paid_amount,
                     f.reversible
                 FROM pool_fee_balance_credits f
                 JOIN blocks b ON b.height = f.block_height
                 WHERE fee_address = $1
                   AND f.paid_amount < f.amount
             ) sources
             ORDER BY reversible ASC, block_height ASC, source_kind ASC",
            &[&address],
        )?;

        let mut remaining = amount;
        let mut allocations = Vec::<PayoutCreditAllocation>::new();
        for row in rows {
            if remaining == 0 {
                break;
            }
            let kind = BalanceCreditSourceKind::from_db(&row.get::<_, String>(0))?;
            let block_height = row.get::<_, i64>(1).max(0) as u64;
            let block_hash = row.get::<_, String>(2);
            let source = BalanceCreditSourceState {
                kind,
                block_height,
                amount: row.get::<_, i64>(3).max(0) as u64,
                paid_amount: row.get::<_, i64>(4).max(0) as u64,
            };
            let available = source.amount.saturating_sub(source.paid_amount);
            if available == 0 {
                continue;
            }
            let consume = remaining.min(available);
            match source.kind {
                BalanceCreditSourceKind::Block => {
                    tx.execute(
                        "UPDATE block_credit_events
                         SET paid_amount = LEAST(amount, paid_amount + $3)
                         WHERE block_height = $1 AND address = $2",
                        &[
                            &u64_to_i64(source.block_height)?,
                            &address,
                            &u64_to_i64(consume)?,
                        ],
                    )?;
                }
                BalanceCreditSourceKind::Fee => {
                    tx.execute(
                        "UPDATE pool_fee_balance_credits
                         SET paid_amount = LEAST(amount, paid_amount + $2)
                         WHERE block_height = $1 AND fee_address = $3",
                        &[
                            &u64_to_i64(source.block_height)?,
                            &u64_to_i64(consume)?,
                            &address,
                        ],
                    )?;
                }
            }
            allocations.push(PayoutCreditAllocation {
                source_kind: source.kind,
                block_height: source.block_height,
                block_hash: Some(block_hash),
                address: address.to_string(),
                amount: consume,
            });
            remaining -= consume;
        }

        Ok(allocations)
    }

    pub(crate) fn revert_completed_payout_tx(
        &self,
        tx_hash: &str,
        reason: &str,
    ) -> Result<CompletedPayoutReconciliation> {
        let tx_hash = tx_hash.trim();
        if tx_hash.is_empty() {
            return Ok(CompletedPayoutReconciliation::default());
        }

        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let payout_rows = tx.query(
            "SELECT id, address, amount, reversible
             FROM payouts
             WHERE tx_hash = $1
               AND reverted_at IS NULL
             ORDER BY id ASC
             FOR UPDATE",
            &[&tx_hash],
        )?;
        if payout_rows.is_empty() {
            return Ok(CompletedPayoutReconciliation::default());
        }

        let mut row_allocations = Vec::with_capacity(payout_rows.len());
        let mut manual_required = false;
        for row in &payout_rows {
            let payout_id = row.get::<_, i64>(0);
            let amount = row.get::<_, i64>(2).max(0) as u64;
            let reversible = row.get::<_, bool>(3);
            let allocation_rows = tx.query(
                "SELECT source_kind, block_height, block_hash, address, amount
                 FROM payout_credit_allocations
                 WHERE payout_id = $1
                 ORDER BY block_height ASC, source_kind ASC, address ASC",
                &[&payout_id],
            )?;
            let allocations = allocation_rows
                .into_iter()
                .map(|allocation| {
                    Ok(PayoutCreditAllocation {
                        source_kind: BalanceCreditSourceKind::from_db(
                            &allocation.get::<_, String>(0),
                        )?,
                        block_height: allocation.get::<_, i64>(1).max(0) as u64,
                        block_hash: allocation.get::<_, Option<String>>(2),
                        address: allocation.get::<_, String>(3),
                        amount: allocation.get::<_, i64>(4).max(0) as u64,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let allocated_amount = allocations.iter().fold(0u64, |acc, allocation| {
                acc.saturating_add(allocation.amount)
            });
            if !reversible || allocated_amount != amount {
                manual_required = true;
            }
            row_allocations.push((payout_id, allocations));
        }
        if manual_required {
            return Ok(CompletedPayoutReconciliation {
                manual_reconciliation_required: true,
                ..CompletedPayoutReconciliation::default()
            });
        }

        let mut result = CompletedPayoutReconciliation::default();
        for row in payout_rows {
            let payout_id = row.get::<_, i64>(0);
            let address = row.get::<_, String>(1);
            let amount = row.get::<_, i64>(2).max(0) as u64;
            let allocations = row_allocations
                .iter()
                .find(|(id, _)| *id == payout_id)
                .map(|(_, allocations)| allocations.as_slice())
                .unwrap_or(&[]);

            let mut balance = Self::load_balance(&mut tx, &address)?;
            if balance.paid < amount {
                return Err(anyhow!(
                    "insufficient paid balance for {} while reverting payout {}",
                    address,
                    payout_id
                ));
            }
            balance.paid -= amount;
            let mut restore_pending = 0u64;
            let mut drop_amount = 0u64;

            for allocation in allocations {
                let valid = Self::source_credit_is_currently_valid(
                    &mut tx,
                    allocation.source_kind,
                    allocation.block_height,
                    allocation.block_hash.as_deref(),
                )?;
                let updated = Self::decrement_paid_amount_for_credit_source(
                    &mut tx,
                    allocation.source_kind,
                    allocation.block_height,
                    allocation.block_hash.as_deref(),
                    &allocation.address,
                    allocation.amount,
                    true,
                )?;
                if !updated {
                    return Err(anyhow!(
                        "failed to unwind payout source for payout {}",
                        payout_id
                    ));
                }

                if valid {
                    restore_pending = restore_pending.saturating_add(allocation.amount);
                } else {
                    drop_amount = drop_amount.saturating_add(allocation.amount);
                }
            }

            balance.pending = balance
                .pending
                .checked_add(restore_pending)
                .ok_or_else(|| anyhow!("pending overflow while reverting payout"))?;
            Self::upsert_balance(&mut tx, &balance)?;
            tx.execute(
                "UPDATE payouts
                 SET reverted_at = $2,
                     revert_reason = $3,
                     reversible = FALSE
                 WHERE id = $1",
                &[&payout_id, &now_unix(), &reason],
            )?;

            result.reverted_payout_rows = result.reverted_payout_rows.saturating_add(1);
            result.restored_pending_amount = result
                .restored_pending_amount
                .saturating_add(restore_pending);
            result.dropped_orphaned_amount =
                result.dropped_orphaned_amount.saturating_add(drop_amount);
        }

        tx.commit()?;
        Ok(result)
    }

    pub fn resolve_completed_payout_tx_override(
        &self,
        tx_hash: &str,
        resolution: ManualCompletedPayoutResolutionKind,
    ) -> Result<CompletedPayoutReconciliation> {
        let tx_hash = tx_hash.trim();
        if tx_hash.is_empty() {
            return Err(anyhow!("tx hash is required"));
        }

        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        let payout_rows = tx.query(
            "SELECT id, address, amount
             FROM payouts
             WHERE tx_hash = $1
               AND reverted_at IS NULL
               AND reversible = FALSE
             ORDER BY id ASC
             FOR UPDATE",
            &[&tx_hash],
        )?;
        if payout_rows.is_empty() {
            return Err(anyhow!(
                "no unresolved completed payout rows found for tx {}",
                tx_hash
            ));
        }

        let mut result = CompletedPayoutReconciliation::default();
        for row in payout_rows {
            let payout_id = row.get::<_, i64>(0);
            let address = row.get::<_, String>(1);
            let amount = row.get::<_, i64>(2).max(0) as u64;
            let allocation_rows = tx.query(
                "SELECT source_kind, block_height, block_hash, address, amount
                 FROM payout_credit_allocations
                 WHERE payout_id = $1
                 ORDER BY block_height ASC, source_kind ASC, address ASC",
                &[&payout_id],
            )?;

            let mut balance = Self::load_balance(&mut tx, &address)?;
            if balance.paid < amount {
                return Err(anyhow!(
                    "insufficient paid balance for {} while resolving payout {}",
                    address,
                    payout_id
                ));
            }
            balance.paid -= amount;
            match resolution {
                ManualCompletedPayoutResolutionKind::RestorePending => {
                    balance.pending = balance
                        .pending
                        .checked_add(amount)
                        .ok_or_else(|| anyhow!("pending overflow while restoring payout"))?;
                    result.restored_pending_amount =
                        result.restored_pending_amount.saturating_add(amount);
                }
                ManualCompletedPayoutResolutionKind::DropPaid => {
                    result.dropped_orphaned_amount =
                        result.dropped_orphaned_amount.saturating_add(amount);
                }
            }
            Self::upsert_balance(&mut tx, &balance)?;

            for allocation in allocation_rows {
                let source_kind =
                    BalanceCreditSourceKind::from_db(&allocation.get::<_, String>(0))?;
                let block_height = allocation.get::<_, i64>(1).max(0) as u64;
                let block_hash = allocation.get::<_, Option<String>>(2);
                let source_address = allocation.get::<_, String>(3);
                let allocation_amount = allocation.get::<_, i64>(4).max(0) as u64;
                if allocation_amount == 0 {
                    continue;
                }
                let _ = Self::decrement_paid_amount_for_credit_source(
                    &mut tx,
                    source_kind,
                    block_height,
                    block_hash.as_deref(),
                    &source_address,
                    allocation_amount,
                    false,
                )?;
            }

            tx.execute(
                "UPDATE payouts
                 SET reverted_at = $2,
                     revert_reason = $3,
                     reversible = FALSE
                 WHERE id = $1",
                &[&payout_id, &now_unix(), &resolution.revert_reason()],
            )?;

            result.reverted_payout_rows = result.reverted_payout_rows.saturating_add(1);
        }

        tx.commit()?;
        Ok(result)
    }

    fn decrement_paid_amount_for_credit_source(
        tx: &mut Transaction<'_>,
        kind: BalanceCreditSourceKind,
        block_height: u64,
        block_hash: Option<&str>,
        address: &str,
        amount: u64,
        require_available_paid_amount: bool,
    ) -> Result<bool> {
        if amount == 0 {
            return Ok(true);
        }

        let height_i64 = u64_to_i64(block_height)?;
        let amount_i64 = u64_to_i64(amount)?;
        let block_hash = block_hash.map(str::trim).filter(|value| !value.is_empty());
        let live_matches_hash = if let Some(block_hash) = block_hash {
            tx.query_opt(
                "SELECT 1
                 FROM blocks
                 WHERE height = $1 AND hash = $2",
                &[&height_i64, &block_hash],
            )?
            .is_some()
        } else {
            tx.query_opt("SELECT 1 FROM blocks WHERE height = $1", &[&height_i64])?
                .is_some()
        };

        let live_source_exists = if live_matches_hash {
            match kind {
                BalanceCreditSourceKind::Block => tx
                    .query_opt(
                        "SELECT 1
                         FROM block_credit_events
                         WHERE block_height = $1 AND address = $2",
                        &[&height_i64, &address],
                    )?
                    .is_some(),
                BalanceCreditSourceKind::Fee => tx
                    .query_opt(
                        "SELECT 1
                         FROM pool_fee_balance_credits
                         WHERE block_height = $1 AND fee_address = $2",
                        &[&height_i64, &address],
                    )?
                    .is_some(),
            }
        } else {
            false
        };

        let updated = match (kind, live_matches_hash && live_source_exists) {
            (BalanceCreditSourceKind::Block, true) => tx.execute(
                if require_available_paid_amount {
                    "UPDATE block_credit_events
                     SET paid_amount = GREATEST(0, paid_amount - $3)
                     WHERE block_height = $1 AND address = $2 AND paid_amount >= $3"
                } else {
                    "UPDATE block_credit_events
                     SET paid_amount = GREATEST(0, paid_amount - $3)
                     WHERE block_height = $1 AND address = $2"
                },
                &[&height_i64, &address, &amount_i64],
            )?,
            (BalanceCreditSourceKind::Fee, true) => tx.execute(
                if require_available_paid_amount {
                    "UPDATE pool_fee_balance_credits
                     SET paid_amount = GREATEST(0, paid_amount - $2)
                     WHERE block_height = $1 AND fee_address = $3 AND paid_amount >= $2"
                } else {
                    "UPDATE pool_fee_balance_credits
                     SET paid_amount = GREATEST(0, paid_amount - $2)
                     WHERE block_height = $1 AND fee_address = $3"
                },
                &[&height_i64, &amount_i64, &address],
            )?,
            (BalanceCreditSourceKind::Block, false) => tx.execute(
                if require_available_paid_amount {
                    "UPDATE archived_block_credit_events
                     SET paid_amount = GREATEST(0, paid_amount - $4)
                     WHERE block_height = $1
                       AND block_hash = $2
                       AND address = $3
                       AND paid_amount >= $4"
                } else {
                    "UPDATE archived_block_credit_events
                     SET paid_amount = GREATEST(0, paid_amount - $4)
                     WHERE block_height = $1
                       AND block_hash = $2
                       AND address = $3"
                },
                &[
                    &height_i64,
                    &block_hash.unwrap_or_default(),
                    &address,
                    &amount_i64,
                ],
            )?,
            (BalanceCreditSourceKind::Fee, false) => tx.execute(
                if require_available_paid_amount {
                    "UPDATE archived_pool_fee_balance_credits
                     SET paid_amount = GREATEST(0, paid_amount - $3)
                     WHERE block_height = $1
                       AND block_hash = $2
                       AND fee_address = $4
                       AND paid_amount >= $3"
                } else {
                    "UPDATE archived_pool_fee_balance_credits
                     SET paid_amount = GREATEST(0, paid_amount - $3)
                     WHERE block_height = $1
                       AND block_hash = $2
                       AND fee_address = $4"
                },
                &[
                    &height_i64,
                    &block_hash.unwrap_or_default(),
                    &amount_i64,
                    &address,
                ],
            )?,
        };

        Ok(updated > 0)
    }

    fn source_credit_is_currently_valid(
        tx: &mut Transaction<'_>,
        _kind: BalanceCreditSourceKind,
        block_height: u64,
        block_hash: Option<&str>,
    ) -> Result<bool> {
        let height_i64 = u64_to_i64(block_height)?;
        let block_hash = block_hash.map(str::trim).filter(|value| !value.is_empty());
        let row = if let Some(block_hash) = block_hash {
            tx.query_opt(
                "SELECT orphaned
                 FROM (
                     SELECT orphaned, 0 AS priority
                     FROM blocks
                     WHERE height = $1 AND hash = $2
                     UNION ALL
                     SELECT orphaned, 1 AS priority
                     FROM archived_blocks
                     WHERE height = $1 AND hash = $2
                 ) source
                 ORDER BY priority ASC
                 LIMIT 1",
                &[&height_i64, &block_hash],
            )?
        } else {
            tx.query_opt(
                "SELECT orphaned FROM blocks WHERE height = $1",
                &[&height_i64],
            )?
        };
        Ok(row.map(|row| !row.get::<_, bool>(0)).unwrap_or(false))
    }

    pub(crate) fn cancel_pending_payout(&self, address: &str) -> Result<()> {
        self.conn().execute(
            "DELETE FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        Ok(())
    }

    /// True when the ledger already attributes `tx_hash` to a completed or
    /// in-flight payout, so a wallet send with that txid must not be adopted
    /// for a different pending batch.
    pub(crate) fn is_payout_tx_hash_recorded(&self, tx_hash: &str) -> Result<bool> {
        let tx_hash = tx_hash.trim();
        if tx_hash.is_empty() {
            return Ok(false);
        }
        let row = self.conn().query_one(
            "SELECT EXISTS(SELECT 1 FROM payouts WHERE tx_hash = $1)
                 OR EXISTS(SELECT 1 FROM pending_payouts WHERE tx_hash = $1)",
            &[&tx_hash],
        )?;
        Ok(row.get::<_, bool>(0))
    }

    pub fn get_pending_payouts(&self) -> Result<Vec<PendingPayout>> {
        let rows = self.conn().query(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id, send_daemon
             FROM pending_payouts
             ORDER BY initiated_at ASC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| row_to_pending_payout(&row))
            .collect())
    }

    pub fn get_pending_payout(&self, address: &str) -> Result<Option<PendingPayout>> {
        let row = self.conn().query_opt(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id, send_daemon
             FROM pending_payouts
             WHERE address = $1",
            &[&address],
        )?;
        Ok(row.map(|row| row_to_pending_payout(&row)))
    }

    pub(crate) fn get_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
    ) -> Result<Option<(u64, SystemTime)>> {
        let row = self.conn().query_opt(
            "SELECT difficulty, updated_at FROM vardiff_hints WHERE address = $1 AND worker = $2",
            &[&address, &worker],
        )?;
        Ok(row.map(|row| {
            (
                row.get::<_, i64>(0).max(1) as u64,
                from_unix(row.get::<_, i64>(1)),
            )
        }))
    }

    pub(crate) fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        self.conn().execute(
            "INSERT INTO vardiff_hints (address, worker, difficulty, updated_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT(address, worker) DO UPDATE SET difficulty = EXCLUDED.difficulty, updated_at = EXCLUDED.updated_at",
            &[&address, &worker, &u64_to_i64(difficulty.max(1))?, &to_unix(updated_at)],
        )?;
        Ok(())
    }

    pub(crate) fn get_vardiff_hints_for_address(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<(u64, SystemTime)>> {
        let limit = (limit.max(1)).min(i64::MAX as usize) as i64;
        let rows = self.conn().query(
            "SELECT difficulty, updated_at
             FROM vardiff_hints
             WHERE address = $1
             ORDER BY updated_at DESC
             LIMIT $2",
            &[&address, &limit],
        )?;
        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, i64>(0).max(1) as u64,
                    from_unix(row.get::<_, i64>(1)),
                )
            })
            .collect())
    }

    pub(crate) fn add_stat_snapshot(
        &self,
        timestamp: SystemTime,
        hashrate: f64,
        miners: i32,
        workers: i32,
    ) -> Result<()> {
        self.conn().execute(
            "INSERT INTO stat_snapshots (timestamp, hashrate, miners, workers) VALUES ($1, $2, $3, $4)",
            &[&to_unix(timestamp), &hashrate, &miners, &workers],
        )?;
        Ok(())
    }

    pub(crate) fn load_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        accepted_window_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState> {
        let rows = self.conn().query(
            "SELECT address, total_shares, sampled_shares, invalid_samples,
                    risk_sampled_shares, risk_invalid_samples,
                    forced_started_at, forced_until,
                    forced_sampled_shares, forced_invalid_samples,
                    resume_forced_at, hold_cause, last_seen_at
             FROM validation_address_states
             WHERE last_seen_at >= $1
                OR (forced_until IS NOT NULL AND forced_until > $2)
                OR (resume_forced_at IS NOT NULL AND resume_forced_at > $2)",
            &[&to_unix(state_cutoff), &to_unix(now)],
        )?;
        let states = rows
            .iter()
            .map(|row| {
                Ok(PersistedValidationAddressState {
                    address: row.get::<_, String>(0),
                    total_shares: row.get::<_, i64>(1).max(0) as u64,
                    sampled_shares: row.get::<_, i64>(2).max(0) as u64,
                    invalid_samples: row.get::<_, i64>(3).max(0) as u64,
                    risk_sampled_shares: row.get::<_, i64>(4).max(0) as u64,
                    risk_invalid_samples: row.get::<_, i64>(5).max(0) as u64,
                    forced_started_at: row.get::<_, Option<i64>>(6).map(from_unix),
                    forced_until: row.get::<_, Option<i64>>(7).map(from_unix),
                    forced_sampled_shares: row.get::<_, i64>(8).max(0) as u64,
                    forced_invalid_samples: row.get::<_, i64>(9).max(0) as u64,
                    resume_forced_at: row.get::<_, Option<i64>>(10).map(from_unix),
                    hold_cause: validation_hold_cause_from_db(
                        row.get::<_, Option<String>>(11).as_deref(),
                    )?,
                    last_seen_at: from_unix(row.get::<_, i64>(12)),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let provisional_rows = self.conn().query(
            "SELECT address, share_id, created_at
             FROM validation_provisionals
             WHERE created_at > $1
             ORDER BY address ASC, created_at ASC",
            &[&to_unix(provisional_cutoff)],
        )?;
        let provisionals = provisional_rows
            .iter()
            .map(|row| PersistedValidationProvisional {
                address: row.get::<_, String>(0),
                share_id: row.get::<_, Option<i64>>(1),
                created_at: from_unix(row.get::<_, i64>(2)),
            })
            .collect::<Vec<_>>();

        let accepted_rows = self.conn().query(
            "SELECT id, miner, created_at, difficulty, status
             FROM shares
             WHERE created_at > $1
               AND status IN ('verified', 'provisional')
             ORDER BY miner ASC, created_at ASC, id ASC",
            &[&to_unix(accepted_window_cutoff)],
        )?;
        let accepted_window = accepted_rows
            .iter()
            .map(|row| PersistedValidationAcceptedShare {
                share_id: row.get::<_, i64>(0),
                address: row.get::<_, String>(1),
                created_at: from_unix(row.get::<_, i64>(2)),
                difficulty: row.get::<_, i64>(3).max(0) as u64,
                verified: row
                    .get::<_, String>(4)
                    .trim()
                    .eq_ignore_ascii_case("verified"),
            })
            .collect::<Vec<_>>();

        Ok(LoadedValidationState {
            states,
            provisionals,
            accepted_window,
        })
    }

    pub(crate) fn latest_validation_clear_event_id(&self) -> Result<i64> {
        let row = self.conn().query_one(
            "SELECT COALESCE(MAX(id), 0) FROM validation_clear_events",
            &[],
        )?;
        Ok(row.get::<_, i64>(0).max(0))
    }

    pub(crate) fn load_validation_clear_events_since(
        &self,
        cursor: i64,
    ) -> Result<Vec<ValidationClearEvent>> {
        let rows = self.conn().query(
            "SELECT id, address, cleared_at
             FROM validation_clear_events
             WHERE id > $1
             ORDER BY id ASC",
            &[&cursor],
        )?;
        Ok(rows
            .iter()
            .map(|row| ValidationClearEvent {
                id: row.get::<_, i64>(0),
                address: row.get::<_, String>(1),
                cleared_at: from_unix(row.get::<_, i64>(2)),
            })
            .collect())
    }

    pub fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()> {
        self.conn().execute(
            "INSERT INTO validation_address_states (
                address, total_shares, sampled_shares, invalid_samples,
                risk_sampled_shares, risk_invalid_samples,
                forced_started_at, forced_until,
                forced_sampled_shares, forced_invalid_samples,
                resume_forced_at, hold_cause, last_seen_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT(address) DO UPDATE SET
                total_shares = EXCLUDED.total_shares,
                sampled_shares = EXCLUDED.sampled_shares,
                invalid_samples = EXCLUDED.invalid_samples,
                risk_sampled_shares = EXCLUDED.risk_sampled_shares,
                risk_invalid_samples = EXCLUDED.risk_invalid_samples,
                forced_started_at = EXCLUDED.forced_started_at,
                forced_until = EXCLUDED.forced_until,
                forced_sampled_shares = EXCLUDED.forced_sampled_shares,
                forced_invalid_samples = EXCLUDED.forced_invalid_samples,
                resume_forced_at = EXCLUDED.resume_forced_at,
                hold_cause = EXCLUDED.hold_cause,
                last_seen_at = EXCLUDED.last_seen_at",
            &[
                &state.address,
                &u64_to_i64(state.total_shares)?,
                &u64_to_i64(state.sampled_shares)?,
                &u64_to_i64(state.invalid_samples)?,
                &u64_to_i64(state.risk_sampled_shares)?,
                &u64_to_i64(state.risk_invalid_samples)?,
                &state.forced_started_at.map(to_unix),
                &state.forced_until.map(to_unix),
                &u64_to_i64(state.forced_sampled_shares)?,
                &u64_to_i64(state.forced_invalid_samples)?,
                &state.resume_forced_at.map(to_unix),
                &validation_hold_cause_to_db(state.hold_cause),
                &to_unix(state.last_seen_at),
            ],
        )?;
        Ok(())
    }

    pub fn add_validation_provisional(
        &self,
        address: &str,
        share_id: Option<i64>,
        created_at: SystemTime,
    ) -> Result<()> {
        self.conn().execute(
            "INSERT INTO validation_provisionals (address, share_id, created_at) VALUES ($1, $2, $3)",
            &[&address, &share_id, &to_unix(created_at)],
        )?;
        Ok(())
    }

    pub(crate) fn clean_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<()> {
        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM validation_provisionals WHERE created_at <= $1",
            &[&to_unix(provisional_cutoff)],
        )?;
        tx.execute(
            "DELETE FROM validation_address_states
             WHERE last_seen_at < $1
               AND (forced_until IS NULL OR forced_until <= $2)
               AND (resume_forced_at IS NULL OR resume_forced_at <= $2)
               AND NOT EXISTS (
                   SELECT 1 FROM validation_provisionals vp
                   WHERE vp.address = validation_address_states.address
               )",
            &[&to_unix(state_cutoff), &to_unix(now)],
        )?;
        tx.execute(
            "DELETE FROM validation_clear_events WHERE cleared_at <= $1",
            &[&to_unix(state_cutoff)],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_stat_snapshots(
        &self,
        since: SystemTime,
    ) -> Result<Vec<pool_common::db::StatSnapshot>> {
        let mut conn = self.conn();
        let rows = conn.query(
            "SELECT timestamp, hashrate FROM stat_snapshots WHERE timestamp >= $1 ORDER BY timestamp ASC",
            &[&to_unix(since)],
        )?;
        Ok(rows
            .iter()
            .map(|row| pool_common::db::StatSnapshot {
                timestamp: from_unix(row.get::<_, i64>(0)),
                hashrate: row.get::<_, f64>(1),
            })
            .collect())
    }

    pub(crate) fn clean_old_snapshots(&self, retain_duration: Duration) -> Result<u64> {
        let cutoff = SystemTime::now()
            .checked_sub(retain_duration)
            .unwrap_or(UNIX_EPOCH);
        let removed = self.conn().execute(
            "DELETE FROM stat_snapshots WHERE timestamp < $1",
            &[&to_unix(cutoff)],
        )?;
        Ok(removed)
    }

    pub(crate) fn rollup_and_prune_shares_before(&self, before: SystemTime) -> Result<u64> {
        let cutoff = floor_to_day_start_unix(to_unix(before));
        if cutoff <= 0 {
            return Ok(0);
        }

        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO share_daily_summaries
                (day_start, accepted_count, rejected_count, accepted_difficulty, unique_miners, unique_workers)
             SELECT
                (created_at / $2) * $2 AS day_start,
                SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint AS accepted_count,
                SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint AS rejected_count,
                SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END)::bigint AS accepted_difficulty,
                COUNT(DISTINCT miner)::bigint AS unique_miners,
                COUNT(DISTINCT worker)::bigint AS unique_workers
             FROM shares
             WHERE created_at < $1
             GROUP BY day_start
             ON CONFLICT(day_start) DO UPDATE SET
                accepted_count = share_daily_summaries.accepted_count + EXCLUDED.accepted_count,
                rejected_count = share_daily_summaries.rejected_count + EXCLUDED.rejected_count,
                accepted_difficulty = share_daily_summaries.accepted_difficulty + EXCLUDED.accepted_difficulty,
                unique_miners = share_daily_summaries.unique_miners + EXCLUDED.unique_miners,
                unique_workers = share_daily_summaries.unique_workers + EXCLUDED.unique_workers",
            &[&cutoff, &SECONDS_PER_DAY],
        )
        .context("roll up retained shares into share_daily_summaries")?;
        tx.execute(
            "INSERT INTO share_rejection_reason_daily_summaries
                (day_start, reason, rejected_count)
             SELECT
                (created_at / $2) * $2 AS day_start,
                reject_reason AS reason,
                COUNT(*)::bigint AS rejected_count
             FROM shares
             WHERE created_at < $1
               AND status NOT IN ('verified','provisional')
             GROUP BY day_start, reason
             ON CONFLICT(day_start, reason) DO UPDATE SET
                rejected_count = share_rejection_reason_daily_summaries.rejected_count + EXCLUDED.rejected_count",
            &[&cutoff, &SECONDS_PER_DAY],
        )
        .context(
            "roll up retained rejected shares into share_rejection_reason_daily_summaries",
        )?;
        let removed = tx
            .execute("DELETE FROM shares WHERE created_at < $1", &[&cutoff])
            .context("prune retained shares")?;
        tx.execute(
            "DELETE FROM share_job_replays replay
             WHERE NOT EXISTS (
                 SELECT 1 FROM shares share
                 WHERE share.job_id = replay.job_id
             )",
            &[],
        )
        .context("prune orphaned share_job_replays after share retention")?;
        tx.commit().context("commit share retention transaction")?;
        Ok(removed)
    }

    pub(crate) fn rollup_and_prune_payouts_before(&self, before: SystemTime) -> Result<u64> {
        let cutoff = floor_to_day_start_unix(to_unix(before));
        if cutoff <= 0 {
            return Ok(0);
        }

        let mut conn = self.conn();
        let mut tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO payout_daily_summaries
                (day_start, payout_count, total_amount, total_fee, unique_recipients)
             SELECT
                (timestamp / $2) * $2 AS day_start,
                COUNT(*)::bigint AS payout_count,
                COALESCE(SUM(amount), 0)::bigint AS total_amount,
                COALESCE(SUM(fee), 0)::bigint AS total_fee,
                COUNT(DISTINCT address)::bigint AS unique_recipients
             FROM payouts
             WHERE timestamp < $1
               AND reverted_at IS NULL
             GROUP BY day_start
             ON CONFLICT(day_start) DO UPDATE SET
                payout_count = payout_daily_summaries.payout_count + EXCLUDED.payout_count,
                total_amount = payout_daily_summaries.total_amount + EXCLUDED.total_amount,
                total_fee = payout_daily_summaries.total_fee + EXCLUDED.total_fee,
                unique_recipients = payout_daily_summaries.unique_recipients + EXCLUDED.unique_recipients",
            &[&cutoff, &SECONDS_PER_DAY],
        )
        .context("roll up retained payouts into payout_daily_summaries")?;
        let removed = tx
            .execute("DELETE FROM payouts WHERE timestamp < $1", &[&cutoff])
            .context("prune retained payouts")?;
        tx.commit().context("commit payout retention transaction")?;
        Ok(removed)
    }

    /// Returns bucketed hashrate data for a miner: Vec<(bucket_ts, total_diff, count)>
    pub fn hashrate_history_for_miner(
        &self,
        address: &str,
        since: SystemTime,
        bucket_secs: i64,
    ) -> Result<Vec<(i64, u64, u64)>> {
        let mut conn = self.conn();
        let ready: bool = conn
            .query_one("SELECT ready FROM share_read_state WHERE singleton", &[])?
            .get(0);
        let since_ts = to_unix(since);
        let rows = if ready && bucket_secs > 0 && bucket_secs % 60 == 0 && since_ts < i64::MAX - 60
        {
            let full_start = since_ts / 60 * 60 + if since_ts % 60 > 0 { 60 } else { 0 };
            conn.query(
                "SELECT bucket,SUM(work)::bigint,SUM(n)::bigint FROM (
                    SELECT (minute/$3)*$3 AS bucket,work,shares AS n FROM share_read_miners
                    WHERE miner=$1 AND minute >= $4 AND status IN ('verified','provisional')
                    UNION ALL
                    SELECT (minute/$3)*$3,work,shares FROM share_read_deltas
                    WHERE miner=$1 AND minute >= $4 AND status IN ('verified','provisional')
                    UNION ALL
                    SELECT (created_at/$3)*$3,difficulty,1 FROM shares
                    WHERE miner=$1 AND created_at >= $2 AND created_at < $4
                      AND status IN ('verified','provisional')
                 ) samples GROUP BY bucket HAVING SUM(n)>0 ORDER BY bucket",
                &[&address, &since_ts, &bucket_secs, &full_start],
            )?
        } else {
            conn.query(
                "SELECT (created_at / $3) * $3 AS bucket, SUM(difficulty)::bigint, COUNT(*)::bigint
             FROM shares
             WHERE miner = $1 AND created_at >= $2
               AND status IN ('verified','provisional')
             GROUP BY bucket ORDER BY bucket",
                &[&address, &since_ts, &bucket_secs],
            )?
        };
        Ok(rows
            .iter()
            .map(|row| {
                let bucket: i64 = row.get(0);
                let total_diff: i64 = row.get(1);
                let count: i64 = row.get(2);
                (bucket, total_diff.max(0) as u64, count.max(0) as u64)
            })
            .collect())
    }

    /// Read the detail page's worker windows and latest reported versions in a
    /// single covering scan. Aggregate timestamps before looking up versions;
    /// DISTINCT ON would sort every share (and spill for large miners).
    pub fn miner_worker_snapshot(
        &self,
        address: &str,
        since: SystemTime,
        hashrate_since: SystemTime,
    ) -> Result<MinerWorkerSnapshot> {
        let rows = self.conn().query(
            "WITH groups AS (
                SELECT worker,status,created_at >= $3 AS recent,
                    COUNT(*)::bigint AS n,SUM(difficulty)::bigint AS work,
                    MIN(created_at) AS first_share,MAX(created_at) AS last_share,
                    MAX(created_at) FILTER (WHERE miner_version IS NOT NULL) AS version_at
                FROM shares WHERE miner = $1 AND created_at >= $2
                GROUP BY worker,status,recent
             ), workers AS (
                SELECT worker,
                    COALESCE(SUM(n) FILTER (WHERE status IN ('verified','provisional')),0)::bigint AS accepted,
                    COALESCE(SUM(n) FILTER (WHERE status NOT IN ('verified','provisional')),0)::bigint AS rejected,
                    COALESCE(SUM(work) FILTER (WHERE status IN ('verified','provisional')),0)::bigint AS work,
                    MAX(last_share) AS last_share,
                    COALESCE(SUM(work) FILTER (WHERE status IN ('verified','provisional') AND recent),0)::bigint AS hr_work,
                    COALESCE(SUM(n) FILTER (WHERE status IN ('verified','provisional') AND recent),0)::bigint AS hr_count,
                    MIN(first_share) FILTER (WHERE status IN ('verified','provisional') AND recent) AS hr_oldest,
                    MAX(last_share) FILTER (WHERE status IN ('verified','provisional') AND recent) AS hr_newest,
                    MAX(version_at) AS version_at
                FROM groups GROUP BY worker
             )
             SELECT w.worker,w.accepted,w.rejected,w.work,w.last_share,
                    w.hr_work,w.hr_count,w.hr_oldest,w.hr_newest,v.miner_version,v.backend
             FROM workers w
             LEFT JOIN LATERAL (
                SELECT miner_version,backend FROM shares
                WHERE miner = $1 AND created_at = w.version_at AND worker = w.worker
                  AND miner_version IS NOT NULL LIMIT 1
             ) v ON TRUE ORDER BY w.worker",
            &[&address, &to_unix(since), &to_unix(hashrate_since)],
        )?;
        let mut snapshot = MinerWorkerSnapshot {
            totals: Vec::with_capacity(rows.len()),
            hashrate: Vec::with_capacity(rows.len()),
            versions: Vec::with_capacity(rows.len()),
        };
        for row in rows {
            let worker: String = row.get(0);
            snapshot.totals.push((
                worker.clone(),
                row.get::<_, i64>(1).max(0) as u64,
                row.get::<_, i64>(2).max(0) as u64,
                row.get::<_, i64>(3).max(0) as u64,
                row.get(4),
            ));
            if row.get::<_, i64>(4) >= to_unix(hashrate_since) {
                snapshot.hashrate.push((
                    worker.clone(),
                    row.get::<_, i64>(5).max(0) as u64,
                    row.get::<_, i64>(6).max(0) as u64,
                    row.get::<_, Option<i64>>(7).map(from_unix),
                    row.get::<_, Option<i64>>(8).map(from_unix),
                ));
            }
            if let Some(version) = row.get::<_, Option<String>>(9) {
                snapshot.versions.push((worker, version, row.get(10)));
            }
        }
        Ok(snapshot)
    }

    #[cfg(test)]
    /// Returns per-worker stats: Vec<(worker, accepted, rejected, total_diff, last_share_at)>
    pub fn worker_stats_for_miner(&self, address: &str, since: SystemTime) -> Result<WorkerStats> {
        let rows = self.conn().query(
            "SELECT worker,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END)::bigint,
                    MAX(created_at)::bigint
             FROM shares
             WHERE miner = $1 AND created_at >= $2
             GROUP BY worker ORDER BY worker",
            &[&address, &to_unix(since)],
        )?;
        Ok(rows
            .iter()
            .map(|row| {
                let worker: String = row.get(0);
                let accepted: i64 = row.get(1);
                let rejected: i64 = row.get(2);
                let total_diff: i64 = row.get(3);
                let last_share: i64 = row.get(4);
                (
                    worker,
                    accepted.max(0) as u64,
                    rejected.max(0) as u64,
                    total_diff.max(0) as u64,
                    last_share,
                )
            })
            .collect())
    }

    /// Returns per-worker hashrate stats:
    /// Vec<(worker, total_diff, accepted_count, oldest_accepted_ts, newest_accepted_ts)>
    #[cfg(test)]
    pub fn worker_hashrate_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<WorkerHashrateStats> {
        let rows = self.conn().query(
            "SELECT worker,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END)::bigint,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    MIN(CASE WHEN status IN ('verified','provisional') THEN created_at END)::bigint,
                    MAX(CASE WHEN status IN ('verified','provisional') THEN created_at END)::bigint
             FROM shares
             WHERE miner = $1 AND created_at >= $2
             GROUP BY worker ORDER BY worker",
            &[&address, &to_unix(since)],
        )?;
        Ok(rows
            .iter()
            .map(|row| {
                let worker: String = row.get(0);
                let total_diff: i64 = row.get(1);
                let accepted: i64 = row.get(2);
                let oldest: Option<i64> = row.get(3);
                let newest: Option<i64> = row.get(4);
                (
                    worker,
                    total_diff.max(0) as u64,
                    accepted.max(0) as u64,
                    oldest.map(from_unix),
                    newest.map(from_unix),
                )
            })
            .collect())
    }

    /// Returns the most recent reported miner version per worker:
    /// Vec<(worker, miner_version, backend)>. Workers whose shares in the
    /// window never reported a version are omitted. The scan is bounded by
    /// `since` and served by idx_shares_miner_created (miner, created_at DESC).
    #[cfg(test)]
    pub fn worker_versions_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<WorkerVersions> {
        let rows = self.conn().query(
            "SELECT DISTINCT ON (worker) worker, miner_version, backend
             FROM shares
             WHERE miner = $1 AND created_at >= $2 AND miner_version IS NOT NULL
             ORDER BY worker, created_at DESC",
            &[&address, &to_unix(since)],
        )?;
        Ok(rows
            .iter()
            .map(|row| {
                let worker: String = row.get(0);
                let miner_version: String = row.get(1);
                let backend: Option<String> = row.get(2);
                (worker, miner_version, backend)
            })
            .collect())
    }

    pub fn get_block_count_for_miner(&self, address: &str) -> Result<u64> {
        let row = self.conn().query_one(
            "SELECT COUNT(*)::bigint FROM blocks WHERE finder = $1",
            &[&address],
        )?;
        Ok(row.get::<_, i64>(0).max(0) as u64)
    }

    pub fn miner_has_any_activity(&self, address: &str) -> Result<bool> {
        let row = self.conn().query_one(
            "SELECT (
                EXISTS(SELECT 1 FROM shares WHERE miner = $1)
                OR EXISTS(SELECT 1 FROM blocks WHERE finder = $1)
                OR EXISTS(SELECT 1 FROM payouts WHERE address = $1)
                OR EXISTS(SELECT 1 FROM pending_payouts WHERE address = $1)
                OR EXISTS(
                    SELECT 1
                    FROM balances
                    WHERE address = $1
                      AND (pending <> 0 OR paid <> 0)
                )
            )",
            &[&address],
        )?;
        Ok(row.get::<_, bool>(0))
    }

    /// Bulk per-miner lifetime counts from the DB: (accepted, rejected, blocks_found, last_share_unix).
    pub fn miner_lifetime_counts(&self) -> Result<MinerLifetimeCounts> {
        let mut conn = self.conn();
        let ready: bool = conn
            .query_one(
                "SELECT ready FROM miner_share_count_state WHERE singleton",
                &[],
            )?
            .get(0);
        let share_rows = if ready {
            conn.query(
                "WITH counts AS (
                    SELECT miner, SUM(accepted)::bigint AS accepted, SUM(rejected)::bigint AS rejected
                    FROM (
                        SELECT miner,accepted,rejected FROM miner_share_counts
                        UNION ALL
                        SELECT miner,accepted,rejected FROM miner_share_count_deltas
                    ) combined GROUP BY miner
                )
                SELECT c.miner,c.accepted,c.rejected,s.created_at
                FROM counts c
                LEFT JOIN LATERAL (
                    SELECT created_at FROM shares WHERE miner = c.miner
                    ORDER BY created_at DESC LIMIT 1
                ) s ON TRUE
                WHERE c.accepted + c.rejected > 0", &[]
            )?
        } else {
            // Existing databases remain correct until the online backfill runs.
            conn.query(
            "SELECT miner,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    MAX(created_at)::bigint
             FROM shares GROUP BY miner",
            &[],
            )?
        };
        let mut map = std::collections::HashMap::new();
        for row in &share_rows {
            let miner: String = row.get(0);
            let accepted: i64 = row.get(1);
            let rejected: i64 = row.get(2);
            let last_share: Option<i64> = row.get(3);
            map.insert(
                miner,
                (
                    accepted.max(0) as u64,
                    rejected.max(0) as u64,
                    0u64,
                    last_share,
                ),
            );
        }
        let block_rows = conn.query(
            "SELECT finder, COUNT(*)::bigint FROM blocks GROUP BY finder",
            &[],
        )?;
        for row in &block_rows {
            let finder: String = row.get(0);
            let count: i64 = row.get(1);
            map.entry(finder)
                .and_modify(|e| e.2 = count.max(0) as u64)
                .or_insert((0, 0, count.max(0) as u64, None));
        }
        Ok(map)
    }

    /// Fold a bounded batch atomically. Concurrent readers see either the old
    /// counters plus these deltas, or the new counters without these deltas.
    pub fn compact_miner_share_count_deltas(&self) -> Result<u64> {
        self.conn().execute(
            "WITH taken AS (
                DELETE FROM miner_share_count_deltas WHERE id IN (
                    SELECT id FROM miner_share_count_deltas
                    WHERE EXISTS (SELECT 1 FROM miner_share_count_state WHERE singleton AND ready)
                    ORDER BY id LIMIT 5000 FOR UPDATE SKIP LOCKED
                ) RETURNING miner,accepted,rejected
             ), grouped AS (
                SELECT miner,SUM(accepted)::bigint AS accepted,SUM(rejected)::bigint AS rejected
                FROM taken GROUP BY miner
             )
             INSERT INTO miner_share_counts(miner,accepted,rejected)
             SELECT miner,accepted,rejected FROM grouped ORDER BY miner
             ON CONFLICT (miner) DO UPDATE SET
                accepted = miner_share_counts.accepted + EXCLUDED.accepted,
                rejected = miner_share_counts.rejected + EXCLUDED.rejected",
            &[],
        )
    }

    pub fn compact_share_read_minutes(&self) -> Result<u64> {
        let updated = self.conn().execute(
            "WITH taken AS (
                DELETE FROM share_read_deltas WHERE id IN (
                    SELECT id FROM share_read_deltas
                    WHERE EXISTS (SELECT 1 FROM share_read_state WHERE singleton AND ready)
                    ORDER BY id LIMIT 5000 FOR UPDATE SKIP LOCKED
                ) RETURNING minute,miner,status,shares,work
             ), by_miner AS (
                SELECT minute,miner,status,SUM(shares)::bigint AS shares,SUM(work)::bigint AS work
                FROM taken GROUP BY minute,miner,status
             ), applied AS (
                INSERT INTO share_read_miners(minute,miner,status,shares,work)
                SELECT minute,miner,status,shares,work FROM by_miner ORDER BY minute,miner,status
                ON CONFLICT (minute,miner,status) DO UPDATE SET
                    shares=share_read_miners.shares+EXCLUDED.shares,
                    work=share_read_miners.work+EXCLUDED.work
                RETURNING 1
             )
             INSERT INTO share_read_minutes(minute,accepted,rejected,work)
             SELECT minute,
                    COALESCE(SUM(shares) FILTER (WHERE status IN ('verified','provisional')),0)::bigint,
                    COALESCE(SUM(shares) FILTER (WHERE status NOT IN ('verified','provisional')),0)::bigint,
                    COALESCE(SUM(work) FILTER (WHERE status IN ('verified','provisional')),0)::bigint
             FROM taken WHERE (SELECT COUNT(*) FROM applied) >= 0
             GROUP BY minute ORDER BY minute
             ON CONFLICT (minute) DO UPDATE SET
                accepted=share_read_minutes.accepted+EXCLUDED.accepted,
                rejected=share_read_minutes.rejected+EXCLUDED.rejected,
                work=share_read_minutes.work+EXCLUDED.work", &[]
        )?;
        self.conn().execute(
            "DELETE FROM share_read_miners WHERE (minute,miner,status) IN (
                SELECT minute,miner,status FROM share_read_miners WHERE shares=0 AND work=0 LIMIT 5000
             ) AND shares=0 AND work=0", &[])?;
        self.conn().execute(
            "DELETE FROM share_read_minutes WHERE minute IN (
                SELECT minute FROM share_read_minutes WHERE accepted=0 AND rejected=0 AND work=0 LIMIT 5000
             ) AND accepted=0 AND rejected=0 AND work=0", &[])?;
        Ok(updated)
    }

    pub fn miner_window_stats_since(
        &self,
        since: SystemTime,
    ) -> Result<HashMap<String, MinerWindowStats>> {
        let mut conn = self.conn();
        let rows = conn.query(
            "WITH workers AS (
                SELECT miner,worker,
                    COALESCE(SUM(difficulty) FILTER (WHERE status IN ('verified','provisional')),0)::bigint AS work,
                    COUNT(*) FILTER (WHERE status IN ('verified','provisional')) AS n,
                    MIN(created_at) FILTER (WHERE status IN ('verified','provisional')) AS first_share,
                    MAX(created_at) FILTER (WHERE status IN ('verified','provisional')) AS last_share
                FROM shares WHERE created_at >= $1 GROUP BY miner,worker
             )
             SELECT miner,COUNT(*)::bigint,SUM(work)::bigint,SUM(n)::bigint,MIN(first_share),MAX(last_share)
             FROM workers GROUP BY miner",
            &[&to_unix(since)],
        )?;
        let mut map = std::collections::HashMap::new();
        for row in &rows {
            let miner: String = row.get(0);
            let worker_count: i64 = row.get(1);
            map.insert(
                miner,
                MinerWindowStats {
                    worker_count: worker_count.max(0) as usize,
                    total_difficulty: row.get::<_, i64>(2).max(0) as u64,
                    accepted_count: row.get::<_, i64>(3).max(0) as u64,
                    oldest: row.get::<_, Option<i64>>(4).map(from_unix),
                    newest: row.get::<_, Option<i64>>(5).map(from_unix),
                },
            );
        }
        Ok(map)
    }
}

fn row_to_share(row: postgres::Row) -> DbShare {
    DbShare {
        id: row.get::<_, i64>(0),
        job_id: row.get::<_, String>(1),
        miner: row.get::<_, String>(2),
        worker: row.get::<_, String>(3),
        difficulty: row.get::<_, i64>(4).max(0) as u64,
        nonce: row.get::<_, i64>(5).max(0) as u64,
        status: row.get::<_, String>(6),
        was_sampled: row.get::<_, bool>(7),
        block_hash: row.get::<_, Option<String>>(8),
        claimed_hash: row.get::<_, Option<String>>(9),
        created_at: from_unix(row.get::<_, i64>(10)),
    }
}

#[cfg(test)]
fn row_to_share_window_preview(row: postgres::Row) -> ShareWindowAddressPreview {
    ShareWindowAddressPreview {
        address: row.get::<_, String>(0),
        seen_shares: row.get::<_, i64>(1).max(0) as u64,
        verified_shares: row.get::<_, i64>(2).max(0) as u64,
        verified_difficulty: row.get::<_, i64>(3).max(0) as u64,
        provisional_shares_ready: row.get::<_, i64>(4).max(0) as u64,
        provisional_difficulty_ready: row.get::<_, i64>(5).max(0) as u64,
        provisional_shares_delayed: row.get::<_, i64>(6).max(0) as u64,
    }
}

fn row_to_share_replay(row: postgres::Row) -> Result<ShareReplayData> {
    let network_target = row.get::<_, Vec<u8>>(2);
    if network_target.len() != 32 {
        return Err(anyhow!(
            "invalid replay network target length for {}: expected 32 bytes, got {}",
            row.get::<_, String>(0),
            network_target.len()
        ));
    }
    let mut network_target_array = [0u8; 32];
    network_target_array.copy_from_slice(&network_target);
    Ok(ShareReplayData {
        job_id: row.get::<_, String>(0),
        header_base: row.get::<_, Vec<u8>>(1),
        network_target: network_target_array,
        created_at: from_unix(row.get::<_, i64>(3)),
    })
}

fn row_get_boolish(row: &postgres::Row, idx: usize) -> Result<bool> {
    if let Ok(value) = row.try_get::<_, bool>(idx) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<_, i64>(idx) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<_, i32>(idx) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<_, i16>(idx) {
        return Ok(value != 0);
    }
    Err(anyhow!(
        "unsupported boolean column type at index {idx}; expected bool or integer"
    ))
}

fn row_to_payout(row: postgres::Row) -> Result<Payout> {
    Ok(Payout {
        address: row.get::<_, String>(0),
        amount: row.get::<_, i64>(1).max(0) as u64,
        fee: row.get::<_, i64>(2).max(0) as u64,
        tx_hash: row.get::<_, String>(3),
        timestamp: from_unix(row.get::<_, i64>(4)),
        confirmed: row_get_boolish(&row, 5)?,
        batch_id: row.get::<_, Option<String>>(6),
    })
}

fn row_to_pending_payout(row: &postgres::Row) -> PendingPayout {
    PendingPayout {
        address: row.get::<_, String>(0),
        amount: row.get::<_, i64>(1).max(0) as u64,
        initiated_at: from_unix(row.get::<_, i64>(2)),
        send_started_at: row.get::<_, Option<i64>>(3).map(from_unix),
        tx_hash: row.get(4),
        fee: row
            .get::<_, Option<i64>>(5)
            .map(|value| value.max(0) as u64),
        sent_at: row.get::<_, Option<i64>>(6).map(from_unix),
        batch_id: row.get::<_, Option<String>>(7),
        send_daemon: row
            .get::<_, Option<String>>(8)
            .filter(|value| !value.trim().is_empty()),
    }
}

fn row_to_public_payout_batch(row: postgres::Row) -> Result<PublicPayoutBatch> {
    let tx_hashes = row.get::<_, Option<String>>(3).unwrap_or_default();
    Ok(PublicPayoutBatch {
        total_amount: row.get::<_, i64>(0).max(0) as u64,
        total_fee: row.get::<_, i64>(1).max(0) as u64,
        recipient_count: row.get::<_, i64>(2).max(0) as usize,
        tx_hashes: tx_hashes
            .split(',')
            .filter(|v| !v.trim().is_empty())
            .map(|v| v.to_string())
            .collect(),
        timestamp: from_unix(row.get::<_, i64>(4)),
        confirmed: row_get_boolish(&row, 5)?,
    })
}

fn sort_reason_counts(entries: Vec<(String, u64)>) -> Vec<RejectionReasonCount> {
    let mut counts = HashMap::<String, u64>::new();
    for (reason, count) in entries {
        let entry = counts.entry(reason).or_insert(0);
        *entry = entry.saturating_add(count);
    }

    let mut items = counts
        .into_iter()
        .map(|(reason, count)| RejectionReasonCount { reason, count })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));
    items
}

fn row_to_monitor_heartbeat(row: postgres::Row) -> Result<MonitorHeartbeat> {
    Ok(MonitorHeartbeat {
        sampled_at: from_unix(row.get::<_, i64>(0)),
        api_up: row.get::<_, Option<bool>>(1),
        stratum_up: row.get::<_, Option<bool>>(2),
        db_up: row.get::<_, bool>(3),
        daemon_up: row.get::<_, Option<bool>>(4),
        public_http_up: row.get::<_, Option<bool>>(5),
        daemon_syncing: row.get::<_, Option<bool>>(6),
        chain_height: opt_i64_to_u64(row.get::<_, Option<i64>>(7)),
        template_age_seconds: opt_i64_to_u64(row.get::<_, Option<i64>>(8)),
        last_refresh_millis: opt_i64_to_u64(row.get::<_, Option<i64>>(9)),
        summary_state: row.get::<_, String>(10),
    })
}

fn row_to_monitor_incident(row: postgres::Row) -> Result<MonitorIncident> {
    Ok(MonitorIncident {
        id: row.get::<_, i64>(0),
        kind: row.get::<_, String>(1),
        severity: row.get::<_, String>(2),
        summary: row.get::<_, String>(3),
        started_at: from_unix(row.get::<_, i64>(4)),
        ended_at: row.get::<_, Option<i64>>(5).map(from_unix),
    })
}

fn row_to_block(row: &postgres::Row) -> DbBlock {
    DbBlock {
        height: row.get::<_, i64>(0).max(0) as u64,
        hash: row.get::<_, String>(1),
        difficulty: row.get::<_, i64>(2).max(0) as u64,
        finder: row.get::<_, String>(3),
        finder_worker: row.get::<_, String>(4),
        reward: row.get::<_, i64>(5).max(0) as u64,
        timestamp: from_unix(row.get::<_, i64>(6)),
        confirmed: row.get::<_, bool>(7),
        orphaned: row.get::<_, bool>(8),
        paid_out: row.get::<_, bool>(9),
        effort_pct: row.get::<_, Option<f64>>(10),
    }
}

fn row_to_luck_round(row: &postgres::Row) -> DbLuckRound {
    DbLuckRound {
        block_height: row.get::<_, i64>(0).max(0) as u64,
        block_hash: row.get::<_, String>(1),
        difficulty: row.get::<_, i64>(2).max(0) as u64,
        timestamp: from_unix(row.get::<_, i64>(3)),
        confirmed: row.get::<_, bool>(4),
        orphaned: row.get::<_, bool>(5),
        round_work: row.get::<_, i64>(6).max(0) as u64,
        duration_seconds: row.get::<_, i64>(7).max(0) as u64,
    }
}

fn now_unix() -> i64 {
    to_unix(SystemTime::now())
}

fn floor_to_day_start_unix(ts: i64) -> i64 {
    if ts <= 0 {
        return 0;
    }
    ts - (ts % SECONDS_PER_DAY)
}

fn to_unix(ts: SystemTime) -> i64 {
    i64::try_from(ts.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()).unwrap_or(i64::MAX)
}

fn opt_u64_to_i64(value: Option<u64>) -> Result<Option<i64>> {
    value.map(u64_to_i64).transpose()
}

fn opt_i64_to_u64(value: Option<i64>) -> Option<u64> {
    value.map(|inner| inner.max(0) as u64)
}

fn u64_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("value does not fit in i64"))
}

fn from_unix(ts: i64) -> SystemTime {
    if ts <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_secs(ts as u64)
}

fn validation_hold_cause_from_db(value: Option<&str>) -> Result<Option<ValidationHoldCause>> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some("invalid_samples") => Ok(Some(ValidationHoldCause::InvalidSamples)),
        Some("provisional_backlog") => Ok(Some(ValidationHoldCause::ProvisionalBacklog)),
        Some("payout_coverage") => Ok(Some(ValidationHoldCause::PayoutCoverage)),
        Some(value) => Err(anyhow!("unknown validation hold cause {value:?}")),
        None => Ok(None),
    }
}

fn validation_hold_cause_to_db(value: Option<ValidationHoldCause>) -> Option<&'static str> {
    value.map(ValidationHoldCause::as_str)
}

fn merge_optional_later(
    existing: Option<SystemTime>,
    candidate: Option<SystemTime>,
) -> Option<SystemTime> {
    match (existing, candidate) {
        (Some(a), Some(b)) => Some(if b > a { b } else { a }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POSTGRES_TEST_URL_ENV: &str = "BLOCKNET_POOL_TEST_POSTGRES_URL";

    fn test_store() -> Option<Arc<PostgresStore>> {
        let url = std::env::var(POSTGRES_TEST_URL_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())?;
        Some(PostgresStore::connect(&url, 2).expect("connect postgres test store"))
    }

    fn unique_suffix() -> String {
        format!("{}-{}", std::process::id(), rand::random::<u64>())
    }

    fn isolated_test_store() -> Option<Arc<PostgresStore>> {
        let url = std::env::var(POSTGRES_TEST_URL_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())?;
        let schema = format!(
            "blocknet_pool_pgdb_test_{}_{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let mut admin = Client::connect(&url, NoTls).expect("connect postgres admin client");
        admin
            .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .expect("create isolated test schema");
        Some(
            PostgresStore::connect_with_schema(&url, 2, Some(&schema))
                .expect("connect isolated postgres test store"),
        )
    }

    fn lock_session(store: &PostgresStore) -> Client {
        store
            .open_independent_session()
            .expect("connect lock session")
    }

    #[test]
    fn payout_sweep_lock_skips_work_while_another_session_holds_it_postgres() {
        let Some(store) = isolated_test_store() else {
            return;
        };
        let mut other = lock_session(&store);
        assert!(try_acquire_payout_sweep_lock(&mut other).expect("lock in other session"));

        let mut ran = false;
        let skipped = store
            .with_payout_sweep_lock(|| ran = true)
            .expect("try sweep lock");
        assert!(
            skipped.is_none(),
            "sweep must be skipped while the lock is held"
        );
        assert!(!ran);

        assert!(release_payout_sweep_lock(&mut other).expect("release other session"));
        let ran_now = store.with_payout_sweep_lock(|| 7).expect("take sweep lock");
        assert_eq!(ran_now, Some(7));

        // The sweep released its lock: another session can take it again.
        assert!(try_acquire_payout_sweep_lock(&mut other).expect("relock other session"));
        assert!(store.with_payout_sweep_lock(|| ()).unwrap().is_none());
        assert!(release_payout_sweep_lock(&mut other).unwrap());
    }

    #[test]
    fn payout_sweep_lock_is_held_for_the_whole_sweep_postgres() {
        let Some(store) = isolated_test_store() else {
            return;
        };
        let mut other = lock_session(&store);
        let observed = store
            .with_payout_sweep_lock(|| try_acquire_payout_sweep_lock(&mut other).unwrap())
            .expect("take sweep lock");
        assert_eq!(
            observed,
            Some(false),
            "a second session must not get the lock mid-sweep"
        );
        assert!(try_acquire_payout_sweep_lock(&mut other).unwrap());
        release_payout_sweep_lock(&mut other).unwrap();
    }

    fn concurrent_balance_credits_do_not_lose_updates(seed_existing: bool) {
        let Some(store) = isolated_test_store() else {
            return;
        };
        let address = format!("lost-update-{}", unique_suffix());
        if seed_existing {
            store
                .update_balance(&Balance {
                    address: address.clone(),
                    pending: 1_000,
                    paid: 0,
                })
                .unwrap();
        }

        // Session A: read-modify-write the balance and keep the tx open.
        let mut session_a = store.open_independent_session().unwrap();
        let mut tx_a = session_a.transaction().unwrap();
        let mut balance = PostgresStore::load_balance(&mut tx_a, &address).unwrap();
        balance.pending += 50;
        PostgresStore::upsert_balance(&mut tx_a, &balance).unwrap();

        // Session B (e.g. an API admin action) credits the same address.
        let mut session_b = store.open_independent_session().unwrap();
        let address_b = address.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let mut tx_b = session_b.transaction().unwrap();
            PostgresStore::credit_pending_balance(&mut tx_b, &address_b, 30).unwrap();
            tx_b.commit().unwrap();
            done_tx.send(()).unwrap();
        });

        std::thread::sleep(Duration::from_millis(300));
        assert!(
            done_rx.try_recv().is_err(),
            "second writer must wait for the first writer's row lock"
        );
        tx_a.commit().unwrap();
        writer.join().unwrap();

        let expected = if seed_existing { 1_080 } else { 80 };
        assert_eq!(store.get_balance(&address).unwrap().pending, expected);
    }

    #[test]
    fn concurrent_balance_writes_serialize_on_existing_rows_postgres() {
        concurrent_balance_credits_do_not_lose_updates(true);
    }

    #[test]
    fn concurrent_balance_writes_serialize_on_new_rows_postgres() {
        concurrent_balance_credits_do_not_lose_updates(false);
    }

    #[test]
    fn concurrent_manual_offsets_accumulate_postgres() {
        let Some(store) = isolated_test_store() else {
            return;
        };
        let address = format!("offset-race-{}", unique_suffix());
        let mut session_a = store.open_independent_session().unwrap();
        let mut tx_a = session_a.transaction().unwrap();
        PostgresStore::add_manual_payout_offset(&mut tx_a, &address, 40).unwrap();

        let mut session_b = store.open_independent_session().unwrap();
        let address_b = address.clone();
        let writer = std::thread::spawn(move || {
            let mut tx_b = session_b.transaction().unwrap();
            PostgresStore::add_manual_payout_offset(&mut tx_b, &address_b, 2).unwrap();
            tx_b.commit().unwrap();
        });
        std::thread::sleep(Duration::from_millis(200));
        tx_a.commit().unwrap();
        writer.join().unwrap();

        let row = store
            .conn()
            .query_one(
                "SELECT amount FROM manual_payout_offsets WHERE address = $1",
                &[&address],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 42);
    }

    #[test]
    fn mark_send_started_requires_every_member_unclaimed_with_planned_amount_postgres() {
        let Some(store) = isolated_test_store() else {
            return;
        };
        store.create_pending_payout("mark-a", 100).unwrap();
        store.create_pending_payout("mark-b", 200).unwrap();

        // Wrong planned amount: nothing is frozen.
        assert!(store
            .mark_pending_payouts_send_started_batch(&[("mark-a".to_string(), 99)], "batch-x", None)
            .is_err());
        assert!(store
            .get_pending_payout("mark-a")
            .unwrap()
            .unwrap()
            .send_started_at
            .is_none());

        let frozen = store
            .mark_pending_payouts_send_started_batch(
                &[("mark-b".to_string(), 200)],
                "batch-1",
                None,
            )
            .expect("freeze unclaimed member");
        assert_eq!(frozen.len(), 1);
        assert_eq!(frozen[0].batch_id.as_deref(), Some("batch-1"));

        // A second process planning the same rows cannot claim them, and the
        // already-claimed member makes the whole batch fail atomically.
        let err = store
            .mark_pending_payouts_send_started_batch(
                &[("mark-a".to_string(), 100), ("mark-b".to_string(), 200)],
                "batch-2",
                None,
            )
            .expect_err("claimed member must abort the freeze");
        assert!(err.to_string().contains("mark-b"), "{err:#}");
        let a = store.get_pending_payout("mark-a").unwrap().unwrap();
        assert!(a.send_started_at.is_none(), "earlier members roll back");
        assert!(a.batch_id.is_none());
        let b = store.get_pending_payout("mark-b").unwrap().unwrap();
        assert_eq!(b.batch_id.as_deref(), Some("batch-1"));

        // Missing rows and empty batch ids are refused.
        assert!(store
            .mark_pending_payouts_send_started_batch(
                &[("mark-missing".to_string(), 1)],
                "batch-3",
                None
            )
            .is_err());
        assert!(store
            .mark_pending_payouts_send_started_batch(&[("mark-a".to_string(), 100)], " ", None)
            .is_err());
    }

    #[test]
    fn inflight_payout_sends_lists_unresolved_and_broadcast_batches_postgres() {
        let Some(store) = isolated_test_store() else {
            return;
        };
        let mut client = lock_session(&store);
        assert!(load_inflight_payout_sends(&mut client).unwrap().is_empty());

        store.create_pending_payout("inflight-queued", 10).unwrap();
        store.create_pending_payout("inflight-a", 20).unwrap();
        store.create_pending_payout("inflight-b", 30).unwrap();
        store
            .mark_pending_payouts_send_started_batch(
                &[
                    ("inflight-a".to_string(), 20),
                    ("inflight-b".to_string(), 30),
                ],
                "batch-unresolved",
                None,
            )
            .unwrap();
        store.create_pending_payout("inflight-c", 40).unwrap();
        store
            .mark_pending_payouts_send_started_batch(
                &[("inflight-c".to_string(), 40)],
                "batch-sent",
                None,
            )
            .unwrap();
        store
            .record_pending_payout_batch_broadcast(
                &[PendingPayoutBatchMember {
                    address: "inflight-c".to_string(),
                    amount: 40,
                    fee: 1,
                }],
                "batch-sent",
                "tx-sent",
            )
            .unwrap();

        let inflight = load_inflight_payout_sends(&mut client).unwrap();
        assert_eq!(inflight.unresolved_rows, 2);
        assert_eq!(
            inflight.unresolved_batches,
            vec!["batch-unresolved".to_string()]
        );
        assert_eq!(inflight.broadcast_rows, 1);
        assert_eq!(inflight.broadcast_tx_hashes, vec!["tx-sent".to_string()]);
    }

    #[test]
    fn payout_page_selects_complete_mixed_batches_before_aggregation_postgres() {
        let Some(store) = test_store() else { return };
        // Run in an isolated database: this tests global pagination and counts.
        store.conn().client.batch_execute(
            "INSERT INTO payouts(address,amount,fee,tx_hash,timestamp,batch_id,reverted_at) VALUES
                ('page-a1',100,10,'tx-a',100,'page-a',NULL),
                ('page-a2',200,20,'tx-a',101,'page-a',NULL),
                ('page-b',300,30,'tx-b',200,'page-b',NULL),
                ('page-reverted',1000,100,'tx-old',500,'page-old',1),
                ('page-null',40,4,'tx-null',400,NULL,NULL);
             INSERT INTO pending_payouts(address,amount,fee,tx_hash,initiated_at,sent_at,batch_id) VALUES
                ('page-a3',50,5,'tx-pending-a',250,300,'page-a'),
                ('page-d',70,7,'tx-pending-d',150,150,'page-d'),
                ('page-unsent',999,9,'',999,NULL,'page-unsent');"
        ).unwrap();
        let (rows, total) = store.get_public_payout_batches_page(2, 1).unwrap();
        assert_eq!(total, 4);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].total_amount, 350);
        assert_eq!(rows[0].total_fee, 35);
        assert_eq!(rows[0].recipient_count, 3);
        assert!(!rows[0].confirmed);
        assert_eq!(rows[0].tx_hashes.len(), 2);
        assert_eq!(rows[1].total_amount, 300);
        assert!(rows[1].confirmed);
        let (first, _) = store.get_public_payout_batches_page(1, 0).unwrap();
        assert_eq!(first[0].total_amount, 40);
        for (limit, offset) in [(0, 0), (200, 100)] {
            let (empty, total) = store.get_public_payout_batches_page(limit, offset).unwrap();
            assert!(empty.is_empty());
            assert_eq!(total, 4);
        }
    }

    #[test]
    fn pending_balance_sources_match_complete_accounting_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO blocks(height,hash,difficulty,finder,finder_worker,reward,timestamp,confirmed,orphaned,paid_out)
             VALUES (9912001,'pending-canonical',1,'finder','rig',100,1,TRUE,FALSE,TRUE),
                    (9912002,'pending-orphan',1,'finder','rig',100,2,TRUE,TRUE,TRUE);
             INSERT INTO block_credit_events(block_height,address,amount,paid_amount) VALUES
                (9912001,'partial',100,30),(9912001,'paid',10,10),(9912001,'overpaid',10,15),
                (9912002,'partial',50,10),(9912002,'orphan-only',90,0),
                (9912003,'missing-block',20,0);
             INSERT INTO pool_fee_balance_credits(block_height,fee_address,amount,paid_amount,timestamp,credited_at)
             VALUES (9912001,'partial',10,2,1,1),(9912002,'fee-paid',10,10,2,2);"
        ).unwrap();
        let full = store
            .list_balance_source_summaries()
            .unwrap()
            .into_iter()
            .filter(|v| v.canonical_pending > 0 || v.orphan_pending > 0)
            .map(|v| (v.address, v.canonical_pending, v.orphan_pending))
            .collect::<Vec<_>>();
        let pending = store
            .list_pending_balance_source_summaries()
            .unwrap()
            .into_iter()
            .map(|v| (v.address, v.canonical_pending, v.orphan_pending))
            .collect::<Vec<_>>();
        assert_eq!(pending, full);
        assert_eq!(
            pending,
            vec![("orphan-only".into(), 0, 90), ("partial".into(), 78, 40)]
        );
    }

    #[test]
    fn projected_miner_history_matches_raw_partial_buckets_and_updates_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             SELECT 'history','history-miner','rig',n+1,n,
                    CASE n % 3 WHEN 0 THEN 'verified' WHEN 1 THEN 'provisional' ELSE 'rejected' END,
                    FALSE,n FROM generate_series(0,600) n;"
        ).unwrap();
        for phase in 0..3 {
            if phase == 1 {
                store.compact_share_read_minutes().unwrap();
            }
            if phase == 2 {
                store.conn().client.batch_execute("UPDATE shares SET status='invalid' WHERE nonce % 7=0; DELETE FROM shares WHERE created_at<120;").unwrap();
            }
            for since in [0i64, 59, 60, 61, 119, 121, 700] {
                for bucket in [7i64, 120, 600, 3600] {
                    let raw = store.conn().query(
                        "SELECT (created_at/$2)*$2 AS bucket,SUM(difficulty)::bigint,COUNT(*)::bigint
                         FROM shares WHERE miner='history-miner' AND created_at >= $1
                           AND status IN ('verified','provisional') GROUP BY bucket ORDER BY bucket",
                        &[&since,&bucket]
                    ).unwrap().into_iter().map(|row|(row.get::<_,i64>(0),row.get::<_,i64>(1) as u64,row.get::<_,i64>(2) as u64)).collect::<Vec<_>>();
                    assert_eq!(
                        store
                            .hashrate_history_for_miner("history-miner", from_unix(since), bucket)
                            .unwrap(),
                        raw,
                        "phase {phase}, since {since}, bucket {bucket}"
                    );
                }
            }
        }
    }

    #[test]
    fn batched_share_windows_match_raw_previews_and_legacy_status_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             SELECT 'preview','miner-'||(n % 3),'rig',n+1,n,
                    CASE n % 4 WHEN 0 THEN '' WHEN 1 THEN 'verified' WHEN 2 THEN 'provisional' ELSE 'invalid' END,
                    FALSE,n FROM generate_series(0,600) n;"
        ).unwrap();
        let windows = [
            (0, 120),
            (60, 300),
            (120, 180),
            (119, 121),
            (300, 299),
            (600, 600),
            (60, 300),
        ]
        .map(|(start, end)| (from_unix(start), from_unix(end)));
        for phase in 0..3 {
            if phase == 1 {
                store.compact_share_read_minutes().unwrap();
            }
            if phase == 2 {
                store.conn().client.batch_execute(
                    "UPDATE shares SET status='rejected',difficulty=difficulty+5 WHERE nonce % 7=0;
                     UPDATE shares SET miner='changed',created_at=created_at+60 WHERE nonce % 11=0;
                     DELETE FROM shares WHERE created_at < 10;"
                ).unwrap();
            }
            for cutoff in [
                None,
                Some(from_unix(0)),
                Some(from_unix(119)),
                Some(from_unix(120)),
                Some(from_unix(999)),
            ] {
                let summaries = store.summarize_share_windows(&windows, cutoff).unwrap();
                for (index, (start, end)) in windows.iter().enumerate() {
                    assert_eq!(
                        summaries[index],
                        store
                            .summarize_shares_between(*start, *end, cutoff)
                            .unwrap(),
                        "phase {phase}, window {index}, cutoff {cutoff:?}"
                    );
                }
            }
        }
        assert!(store.summarize_share_windows(&[], None).unwrap().is_empty());
        let fractional = store
            .summarize_share_windows(
                &[(from_unix(59) + Duration::from_millis(500), from_unix(60))],
                Some(from_unix(60)),
            )
            .unwrap();
        // Share 60 is legacy verified; mutation moved rejected share 0 to 60.
        // The fractional lower bound excludes share 59.
        assert_eq!(fractional[0].iter().map(|p| p.seen_shares).sum::<u64>(), 2);
        assert_eq!(
            fractional[0]
                .iter()
                .map(|p| p.verified_difficulty)
                .sum::<u64>(),
            61
        );
    }

    #[test]
    fn minute_read_windows_match_raw_shares_before_and_after_mutations_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             SELECT 'minutes','minute-miner','rig',n+1,n,
                    CASE n % 3 WHEN 0 THEN 'verified' WHEN 1 THEN 'provisional' ELSE 'invalid' END,
                    FALSE,n FROM generate_series(0,300) n;"
        ).unwrap();
        let check = || {
            for start in [-1i64, 0, 1, 59, 60, 61, 119, 120, 121, 300, 301] {
                for end in [
                    start - 1,
                    start,
                    start + 1,
                    start + 59,
                    start + 60,
                    start + 180,
                    400,
                ] {
                    let row = store.conn().query_one(
                        "SELECT w.accepted,w.rejected,w.work,r.a,r.r,r.w
                         FROM share_read_window($1,$2) w CROSS JOIN LATERAL (
                            SELECT COUNT(*) FILTER (WHERE status IN ('verified','provisional')) AS a,
                                COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional')) AS r,
                                COALESCE(SUM(difficulty) FILTER (WHERE status IN ('verified','provisional')),0)::bigint AS w
                            FROM shares WHERE created_at >= $1 AND created_at <= $2
                         ) r", &[&start,&end]
                    ).unwrap();
                    let work: i64 = store
                        .conn()
                        .query_one("SELECT share_read_work($1,$2)", &[&start, &end])
                        .unwrap()
                        .get(0);
                    assert_eq!(work, row.get::<_, i64>(5), "accepted work {start}..{end}");
                    for column in 0..3 {
                        assert_eq!(
                            row.get::<_, i64>(column),
                            row.get::<_, i64>(column + 3),
                            "window {start}..{end}, column {column}"
                        );
                    }
                }
            }
        };
        check();
        store
            .conn()
            .client
            .batch_execute("UPDATE share_read_state SET ready=FALSE")
            .unwrap();
        check();
        store
            .conn()
            .client
            .batch_execute("UPDATE share_read_state SET ready=TRUE")
            .unwrap();
        store.compact_share_read_minutes().unwrap();
        check();
        store
            .conn()
            .client
            .batch_execute(
                "UPDATE shares SET status='rejected',difficulty=difficulty+10 WHERE nonce % 5=0;
             UPDATE shares SET miner='moved',created_at=created_at+60 WHERE nonce % 7=0;
             DELETE FROM shares WHERE created_at < 60;",
            )
            .unwrap();
        check();
        store.compact_share_read_minutes().unwrap();
        store.compact_share_read_minutes().unwrap();
        check();
        store
            .conn()
            .client
            .batch_execute("BEGIN; UPDATE shares SET status='verified'; ROLLBACK;")
            .unwrap();
        check();
        store
            .conn()
            .client
            .batch_execute("TRUNCATE shares")
            .unwrap();
        check();
    }

    #[test]
    fn minute_read_backfill_preserves_late_commits_postgres() {
        let Some(store) = test_store() else { return };
        store
            .conn()
            .client
            .batch_execute(
                "UPDATE share_read_state SET ready=FALSE;
             INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             VALUES ('base','base','rig',10,1,'verified',FALSE,120);",
            )
            .unwrap();
        {
            let mut writer = store.conns[0].lock();
            let mut write_tx = writer.transaction().unwrap();
            write_tx.batch_execute(
                "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
                 VALUES ('late','late','rig',20,2,'provisional',FALSE,130);"
            ).unwrap();
            let mut reader = store.conns[1].lock();
            let mut tx = reader.transaction().unwrap();
            tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT ready FROM share_read_state WHERE singleton FOR UPDATE;").unwrap();
            let migration = include_str!("../../../scripts/backfill_share_read_minutes.sql");
            let backfill = migration
                .split("\\else\n")
                .nth(1)
                .unwrap()
                .split("-- The same snapshot")
                .next()
                .unwrap();
            tx.batch_execute(backfill).unwrap();
            write_tx.commit().unwrap();
            tx.batch_execute("DELETE FROM share_read_deltas; UPDATE share_read_state SET ready=TRUE WHERE singleton;").unwrap();
            tx.commit().unwrap();
        }
        for _ in 0..2 {
            let row = store
                .conn()
                .query_one("SELECT accepted,work FROM share_read_window(120,179)", &[])
                .unwrap();
            assert_eq!(row.get::<_, i64>(0), 2);
            assert_eq!(row.get::<_, i64>(1), 30);
            store.compact_share_read_minutes().unwrap();
        }
    }

    #[test]
    fn bulk_miner_window_matches_individual_hashrate_and_worker_semantics_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             VALUES ('window','window-a','old',1000,1,'verified',FALSE,99),
                    ('window','window-a','one',10,2,'verified',FALSE,100),
                    ('window','window-a','one',20,3,'provisional',FALSE,150),
                    ('window','window-a','two',999,4,'rejected',FALSE,200),
                    ('window','window-b','rejected-only',999,5,'rejected',FALSE,200);"
        ).unwrap();
        let windows = store.miner_window_stats_since(from_unix(100)).unwrap();
        let a = &windows["window-a"];
        assert_eq!(a.worker_count, 2);
        assert_eq!(
            (a.total_difficulty, a.accepted_count, a.oldest, a.newest),
            store
                .hashrate_stats_for_miner("window-a", from_unix(100))
                .unwrap()
        );
        let b = &windows["window-b"];
        assert_eq!(b.worker_count, 1);
        assert_eq!(
            (b.total_difficulty, b.accepted_count, b.oldest, b.newest),
            (0, 0, None, None)
        );
        for miner in ["window-a", "window-b"] {
            let combined = store
                .miner_worker_snapshot(miner, from_unix(100), from_unix(150))
                .unwrap();
            assert_eq!(
                combined.totals,
                store.worker_stats_for_miner(miner, from_unix(100)).unwrap()
            );
            assert_eq!(
                combined.hashrate,
                store
                    .worker_hashrate_stats_for_miner(miner, from_unix(150))
                    .unwrap()
            );
            assert!(combined.versions.is_empty());
        }
    }

    #[test]
    fn miner_counts_include_uncompacted_updates_deletes_and_rollback_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             VALUES ('counts','count-a','rig',10,1,'verified',FALSE,100),
                    ('counts','count-a','rig',20,2,'provisional',FALSE,200),
                    ('counts','count-b','rig',99,3,'rejected',FALSE,300);"
        ).unwrap();
        let initial = store.miner_lifetime_counts().unwrap();
        assert_eq!(initial["count-a"], (2, 0, 0, Some(200)));
        assert_eq!(initial["count-b"], (0, 1, 0, Some(300)));
        store.compact_miner_share_count_deltas().unwrap();
        assert_eq!(store.miner_lifetime_counts().unwrap(), initial);
        store
            .conn()
            .client
            .batch_execute(
                "UPDATE shares SET status='invalid' WHERE miner='count-a' AND nonce=2;
             UPDATE shares SET miner='count-a',status='verified' WHERE miner='count-b';
             DELETE FROM shares WHERE miner='count-a' AND nonce=3;",
            )
            .unwrap();
        store
            .conn()
            .client
            .batch_execute("BEGIN; DELETE FROM shares; ROLLBACK;")
            .unwrap();
        let changed = store.miner_lifetime_counts().unwrap();
        assert_eq!(changed["count-a"], (1, 1, 0, Some(200)));
        assert!(!changed.contains_key("count-b"));
        store.compact_miner_share_count_deltas().unwrap();
        store.compact_miner_share_count_deltas().unwrap();
        assert_eq!(store.miner_lifetime_counts().unwrap(), changed);
        store
            .conn()
            .client
            .batch_execute("DELETE FROM shares")
            .unwrap();
        assert!(store.miner_lifetime_counts().unwrap().is_empty());
        store.compact_miner_share_count_deltas().unwrap();
        assert!(store.miner_lifetime_counts().unwrap().is_empty());
    }

    #[test]
    fn miner_count_backfill_keeps_transactions_committed_after_snapshot_postgres() {
        let Some(store) = test_store() else { return };
        store
            .conn()
            .client
            .batch_execute(
                "UPDATE miner_share_count_state SET ready=FALSE;
             INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             VALUES ('base','base-miner','rig',10,1,'verified',FALSE,100);",
            )
            .unwrap();
        {
            let mut writer = store.conns[0].lock();
            let mut write_tx = writer.transaction().unwrap();
            write_tx.batch_execute(
                "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
                 VALUES ('late','late-miner','rig',10,2,'rejected',FALSE,200);"
            ).unwrap();
            let mut backfill = store.conns[1].lock();
            let mut tx = backfill.transaction().unwrap();
            tx.batch_execute(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
                 SELECT ready FROM miner_share_count_state WHERE singleton FOR UPDATE;
                 INSERT INTO miner_share_counts(miner,accepted,rejected)
                 SELECT miner,COUNT(*) FILTER (WHERE status IN ('verified','provisional')),
                     COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional'))
                 FROM shares GROUP BY miner;",
            )
            .unwrap();
            write_tx.commit().unwrap();
            tx.batch_execute(
                "DELETE FROM miner_share_count_deltas;
                 UPDATE miner_share_count_state SET ready=TRUE WHERE singleton;",
            )
            .unwrap();
            tx.commit().unwrap();
        }
        let counts = store.miner_lifetime_counts().unwrap();
        assert_eq!(counts["base-miner"], (1, 0, 0, Some(100)));
        assert_eq!(counts["late-miner"], (0, 1, 0, Some(200)));
        store.compact_miner_share_count_deltas().unwrap();
        assert_eq!(store.miner_lifetime_counts().unwrap(), counts);
        store
            .conn()
            .client
            .batch_execute("TRUNCATE shares")
            .unwrap();
        assert!(store.miner_lifetime_counts().unwrap().is_empty());
    }

    #[test]
    fn miner_count_compaction_is_bounded_without_losing_unapplied_rows_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             SELECT 'batch','batch-miner','rig',1,n,'verified',FALSE,n FROM generate_series(1,5001) n;"
        ).unwrap();
        store.compact_miner_share_count_deltas().unwrap();
        let remaining: i64 = store
            .conn()
            .query_one("SELECT COUNT(*) FROM miner_share_count_deltas", &[])
            .unwrap()
            .get(0);
        assert_eq!(remaining, 1);
        assert_eq!(
            store.miner_lifetime_counts().unwrap()["batch-miner"],
            (5001, 0, 0, Some(5001))
        );
        store.compact_miner_share_count_deltas().unwrap();
        assert_eq!(
            store.miner_lifetime_counts().unwrap()["batch-miner"],
            (5001, 0, 0, Some(5001))
        );
    }

    #[test]
    fn outcome_counts_include_boundary_and_all_rejection_states_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             VALUES ('outcomes','test','rig',10,1,'verified',FALSE,100),
                    ('outcomes','test','rig',20,2,'provisional',FALSE,200),
                    ('outcomes','test','rig',99,3,'rejected',FALSE,200),
                    ('outcomes','test','rig',30,4,'invalid',FALSE,300);"
        ).unwrap();
        assert_eq!(
            store.share_outcome_counts_since(from_unix(200)).unwrap(),
            (1, 2)
        );
        assert_eq!(
            store.share_outcome_counts_since(from_unix(301)).unwrap(),
            (0, 0)
        );
        let performance = store.read_performance();
        assert!(performance
            .iter()
            .any(|(name, timing)| name.contains("pgdb.rs:") && timing.count >= 2));
    }

    #[test]
    fn luck_page_respects_tip_and_inclusive_share_boundaries_postgres() {
        let Some(store) = test_store() else { return };
        store.conn().client.batch_execute(
            "INSERT INTO blocks(height,hash,difficulty,finder,finder_worker,reward,timestamp,confirmed,orphaned,paid_out)
             VALUES (1,'luck-start',100,'test','rig',1,100,TRUE,FALSE,FALSE),
                    (2,'luck-middle',100,'test','rig',1,200,TRUE,FALSE,FALSE),
                    (3,'luck-end',100,'test','rig',1,300,TRUE,FALSE,FALSE);
             INSERT INTO shares(job_id,miner,worker,difficulty,nonce,status,was_sampled,created_at)
             VALUES ('luck-test','test','rig',10,1,'verified',FALSE,100),
                    ('luck-test','test','rig',20,2,'provisional',FALSE,200),
                    ('luck-test','test','rig',99,3,'rejected',FALSE,150),
                    ('luck-test','test','rig',30,4,'verified',FALSE,300);"
        ).unwrap();
        let (rows, total) = store.get_luck_rounds_page_up_to(25, 0, 2).unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_height, 2);
        assert_eq!(rows[0].round_work, 30);
        assert_eq!(rows[0].duration_seconds, 100);
        let (page, total) = store.get_luck_rounds_page_up_to(1, 1, 3).unwrap();
        assert_eq!(total, 2);
        assert_eq!(page[0].block_height, 2);
        let hashes = store
            .get_luck_rounds_for_hashes(&["luck-start".into(), "luck-end".into()])
            .unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes["luck-end"].round_work, 50);
    }

    #[test]
    fn connection_acquisition_skips_busy_connection_postgres() {
        let Some(store) = test_store() else { return };
        let busy = store.conns[0].lock();
        store.next_conn.store(0, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::channel();
        let reader_store = Arc::clone(&store);
        let reader = std::thread::spawn(move || {
            let result = reader_store
                .conn()
                .query_one("SELECT 1::bigint", &[])
                .map(|row| row.get::<_, i64>(0));
            tx.send(result).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(2));
        drop(busy);
        reader.join().unwrap();
        assert_eq!(
            result
                .expect("idle connection must be used without waiting")
                .unwrap(),
            1
        );
    }

    #[test]
    fn statement_timeout_scope_applies_only_inside_scope_postgres() {
        let Some(store) = test_store() else { return };
        let show = |store: &PostgresStore| -> String {
            store
                .conn()
                .query_one("SHOW statement_timeout", &[])
                .unwrap()
                .get(0)
        };
        let default = show(&store);
        let scoped = with_statement_timeout(Some(Duration::from_millis(1500)), || show(&store));
        assert_eq!(scoped, "1500ms");
        assert_eq!(show(&store), default, "scope must not leak to later reads");
        let err = with_statement_timeout(Some(Duration::from_millis(50)), || {
            store
                .conn()
                .query_one("SELECT pg_sleep(2)::text", &[])
                .map(|_| ())
        })
        .unwrap_err();
        assert!(format!("{err:#}").contains("statement timeout"), "{err:#}");
        assert_eq!(show(&store), default, "connection stays usable after a timeout");
    }

    #[test]
    fn connection_acquisition_waits_when_pool_is_full_postgres() {
        let Some(store) = test_store() else { return };
        let first = store.conns[0].lock();
        let second = store.conns[1].lock();
        store.next_conn.store(0, Ordering::Relaxed);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let reader_store = Arc::clone(&store);
        let reader = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _conn = reader_store.conn();
            done_tx.send(()).unwrap();
        });
        started_rx.recv().unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(25));
        drop(first);
        drop(second);
        reader.join().unwrap();
        assert!(matches!(
            early,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn db_enum_parsers_reject_unknown_values() {
        assert_eq!(
            BalanceCreditSourceKind::from_db("block").unwrap(),
            BalanceCreditSourceKind::Block
        );
        assert!(BalanceCreditSourceKind::from_db("legacy").is_err());
        assert_eq!(
            validation_hold_cause_from_db(Some("payout_coverage")).unwrap(),
            Some(ValidationHoldCause::PayoutCoverage)
        );
        assert!(validation_hold_cause_from_db(Some("legacy")).is_err());
    }

    #[test]
    fn async_share_commit_claims_and_inserts_provisional_shares_postgres() {
        let Some(store) = test_store() else { return };
        store.set_async_share_commit(true);
        let suffix = unique_suffix();
        let key = format!("tpl:async-{suffix}");
        assert!(store.try_claim_share("unused", &key, 3).unwrap());
        assert!(
            !store.try_claim_share("unused", &key, 3).unwrap(),
            "still exclusive"
        );
        let id = store
            .add_share_with_replay_and_id(
                ShareRecord {
                    job_id: format!("async-{suffix}"),
                    miner: format!("miner-{suffix}"),
                    worker: "rig".to_string(),
                    difficulty: 1,
                    nonce: 3,
                    status: "provisional",
                    was_sampled: false,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at: SystemTime::now(),
                },
                None,
            )
            .unwrap();
        assert!(id > 0);
        let mut conn = store.conn();
        let row = conn
            .query_one("SHOW synchronous_commit", &[])
            .expect("show synchronous_commit");
        assert_eq!(
            row.get::<_, String>(0),
            "on",
            "SET LOCAL must not leak into the pooled session"
        );
    }

    #[test]
    fn template_share_claims_outlive_persistence_and_ignore_retryable_rejects_postgres() {
        let Some(store) = test_store() else { return };
        let suffix = unique_suffix();
        let key = format!("tpl:{suffix}");
        let assignment_old = format!("assign-old-{suffix}");
        let assignment_new = format!("assign-new-{suffix}");

        assert!(store.try_claim_share(&assignment_old, &key, 7).unwrap());
        store
            .add_share(ShareRecord {
                job_id: assignment_old.clone(),
                miner: format!("miner-{suffix}"),
                worker: "rig".to_string(),
                difficulty: 1,
                nonce: 7,
                status: "provisional",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: SystemTime::now(),
            })
            .unwrap();
        // The header-scoped claim is not released after persistence, so the same
        // nonce under a rebound assignment id is still a duplicate.
        assert!(!store.try_claim_share(&assignment_new, &key, 7).unwrap());
        // Releasing (retryable failure path) frees it again.
        store.release_share_claim(&key, 7).unwrap();
        assert!(store.try_claim_share(&assignment_new, &key, 7).unwrap());

        // A persisted "server busy" rejection never blocks the retry.
        let busy_assignment = format!("assign-busy-{suffix}");
        store
            .add_share(ShareRecord {
                job_id: busy_assignment.clone(),
                miner: format!("miner-{suffix}"),
                worker: "rig".to_string(),
                difficulty: 1,
                nonce: 8,
                status: "rejected",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("server busy".to_string()),
                created_at: SystemTime::now(),
            })
            .unwrap();
        assert!(store.try_claim_share(&busy_assignment, &key, 8).unwrap());
        // Nor does a "duplicate share" row on its own.
        store
            .add_share(ShareRecord {
                job_id: busy_assignment.clone(),
                miner: format!("miner-{suffix}"),
                worker: "rig".to_string(),
                difficulty: 1,
                nonce: 10,
                status: "rejected",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("duplicate share".to_string()),
                created_at: SystemTime::now(),
            })
            .unwrap();
        assert!(store.try_claim_share(&busy_assignment, &key, 10).unwrap());
        // Claims are keyed by template header only; persisted rows never
        // stand in for a claim (an assignment id maps to a single header).
    }

    #[test]
    fn revoked_provisional_shares_cannot_be_resurrected_by_late_audit_postgres() {
        let Some(store) = test_store() else { return };
        let suffix = unique_suffix();
        let share = |nonce: u64, status: &'static str| ShareRecord {
            job_id: format!("revoke-{suffix}"),
            miner: format!("miner-{suffix}"),
            worker: "rig".to_string(),
            difficulty: 1,
            nonce,
            status,
            was_sampled: false,
            block_hash: None,
            claimed_hash: None,
            reject_reason: None,
            created_at: SystemTime::now(),
        };
        let provisional = store
            .add_share_with_replay_and_id(share(1, "provisional"), None)
            .unwrap();
        let verified = store
            .add_share_with_replay_and_id(share(2, "verified"), None)
            .unwrap();
        store
            .add_validation_provisional(
                &format!("miner-{suffix}"),
                Some(provisional),
                SystemTime::now(),
            )
            .unwrap();

        let revoked = store
            .revoke_provisional_shares(&[provisional, verified], "source revoked")
            .unwrap();
        assert_eq!(revoked, 1, "verified shares are never revoked");
        let rows = store
            .get_shares_for_miner(&format!("miner-{suffix}"), 10)
            .unwrap();
        let status_of = |id: i64| rows.iter().find(|row| row.id == id).unwrap().status.clone();
        assert_eq!(status_of(provisional), "rejected");
        assert_eq!(status_of(verified), "verified");

        let applied = store
            .complete_validation_audit(&ShareReplayUpdate {
                share_id: provisional,
                status: "verified".to_string(),
                was_sampled: true,
                reject_reason: None,
            })
            .unwrap();
        assert!(
            !applied,
            "a passing late audit must not resurrect a revoked share"
        );
        let rows = store
            .get_shares_for_miner(&format!("miner-{suffix}"), 10)
            .unwrap();
        assert_eq!(
            rows.iter()
                .find(|row| row.id == provisional)
                .unwrap()
                .status,
            "rejected"
        );
    }

    #[test]
    fn summarize_shares_between_groups_preview_stats_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let miner_a = format!("miner-a-{suffix}");
        let miner_b = format!("miner-b-{suffix}");
        let base = UNIX_EPOCH + Duration::from_secs(20_000_000);

        for share in [
            ShareRecord {
                job_id: format!("job-a-verified-{suffix}"),
                miner: miner_a.clone(),
                worker: "rig-a".to_string(),
                difficulty: 10,
                nonce: 1,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base,
            },
            ShareRecord {
                job_id: format!("job-a-ready-{suffix}"),
                miner: miner_a.clone(),
                worker: "rig-a".to_string(),
                difficulty: 20,
                nonce: 2,
                status: "provisional",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(10),
            },
            ShareRecord {
                job_id: format!("job-a-delayed-{suffix}"),
                miner: miner_a.clone(),
                worker: "rig-a".to_string(),
                difficulty: 30,
                nonce: 3,
                status: "provisional",
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(50),
            },
            ShareRecord {
                job_id: format!("job-b-reject-{suffix}"),
                miner: miner_b.clone(),
                worker: "rig-b".to_string(),
                difficulty: 40,
                nonce: 4,
                status: "rejected",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: Some("stale".to_string()),
                created_at: base + Duration::from_secs(20),
            },
            ShareRecord {
                job_id: format!("job-b-verified-{suffix}"),
                miner: miner_b.clone(),
                worker: "rig-b".to_string(),
                difficulty: 50,
                nonce: 5,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: base + Duration::from_secs(30),
            },
        ] {
            store.add_share(share).expect("add share");
        }

        let summary = store
            .summarize_shares_between(
                base,
                base + Duration::from_secs(60),
                Some(base + Duration::from_secs(40)),
            )
            .expect("summarize shares between");
        assert_eq!(summary.len(), 2);

        let miner_a_summary = summary
            .iter()
            .find(|row| row.address == miner_a)
            .expect("miner a summary");
        assert_eq!(miner_a_summary.seen_shares, 3);
        assert_eq!(miner_a_summary.verified_shares, 1);
        assert_eq!(miner_a_summary.verified_difficulty, 10);
        assert_eq!(miner_a_summary.provisional_shares_ready, 1);
        assert_eq!(miner_a_summary.provisional_difficulty_ready, 20);
        assert_eq!(miner_a_summary.provisional_shares_delayed, 1);

        let miner_b_summary = summary
            .iter()
            .find(|row| row.address == miner_b)
            .expect("miner b summary");
        assert_eq!(miner_b_summary.seen_shares, 2);
        assert_eq!(miner_b_summary.verified_shares, 1);
        assert_eq!(miner_b_summary.verified_difficulty, 50);
        assert_eq!(miner_b_summary.provisional_shares_ready, 0);
        assert_eq!(miner_b_summary.provisional_difficulty_ready, 0);
        assert_eq!(miner_b_summary.provisional_shares_delayed, 0);
    }

    #[test]
    fn worker_versions_for_miner_returns_latest_reported_version_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let miner = format!("miner-versions-{suffix}");
        let base = UNIX_EPOCH + Duration::from_secs(20_000_000);

        let share = |job: &str, worker: &str, nonce: u64, offset: u64| ShareRecord {
            job_id: format!("{job}-{suffix}"),
            miner: miner.clone(),
            worker: worker.to_string(),
            difficulty: 10,
            nonce,
            status: "verified",
            was_sampled: true,
            block_hash: None,
            claimed_hash: None,
            reject_reason: None,
            created_at: base + Duration::from_secs(offset),
        };

        // rig-new reports versions; the newest share's version should win.
        store
            .add_share_with_replay_metadata_and_id(
                share("job-v1", "rig-new", 1, 0),
                None,
                Some("0.2.14"),
                Some("cpu"),
            )
            .expect("add versioned share");
        store
            .add_share_with_replay_metadata_and_id(
                share("job-v2", "rig-new", 2, 10),
                None,
                Some("0.2.15"),
                Some("gpu"),
            )
            .expect("add newer versioned share");
        // rig-old never reports a version and must be omitted.
        store
            .add_share(share("job-v3", "rig-old", 3, 20))
            .expect("add unversioned share");
        // Versions reported before the window must not leak in.
        store
            .add_share_with_replay_metadata_and_id(
                share("job-v4", "rig-stale", 4, 0),
                None,
                Some("0.2.13"),
                None,
            )
            .expect("add stale versioned share");

        let versions = store
            .worker_versions_for_miner(&miner, base + Duration::from_secs(5))
            .expect("worker versions for miner");

        assert_eq!(
            versions,
            vec![(
                "rig-new".to_string(),
                "0.2.15".to_string(),
                Some("gpu".to_string())
            )]
        );
        let combined = store
            .miner_worker_snapshot(
                &miner,
                base + Duration::from_secs(5),
                base + Duration::from_secs(15),
            )
            .unwrap();
        assert_eq!(combined.versions, versions);
        assert_eq!(
            combined.totals,
            store
                .worker_stats_for_miner(&miner, base + Duration::from_secs(5))
                .unwrap()
        );
        assert_eq!(
            combined.hashrate,
            store
                .worker_hashrate_stats_for_miner(&miner, base + Duration::from_secs(15))
                .unwrap()
        );
    }

    #[test]
    fn summarize_shares_between_reduces_materialized_rows_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let base = UNIX_EPOCH + Duration::from_secs(21_000_000);
        let miner_count = 16u64;
        let shares_per_miner = 80u64;

        for miner_idx in 0..miner_count {
            let miner = format!("miner-{miner_idx}-{suffix}");
            for share_idx in 0..shares_per_miner {
                let status = if share_idx % 3 == 0 {
                    "verified"
                } else {
                    "provisional"
                };
                store
                    .add_share(ShareRecord {
                        job_id: format!("job-{suffix}-{miner_idx}-{share_idx}"),
                        miner: miner.clone(),
                        worker: format!("rig-{miner_idx}"),
                        difficulty: 1 + (share_idx % 5),
                        nonce: share_idx + 1,
                        status,
                        was_sampled: status == "verified",
                        block_hash: None,
                        claimed_hash: None,
                        reject_reason: None,
                        created_at: base + Duration::from_secs(miner_idx * 5 + share_idx),
                    })
                    .expect("add perf share");
            }
        }

        let start = base;
        let end = base + Duration::from_secs(miner_count * 5 + shares_per_miner + 5);
        let raw = store
            .get_shares_between(start, end)
            .expect("load raw shares between");
        let summary = store
            .summarize_shares_between(start, end, Some(end))
            .expect("summarize shares between");

        assert_eq!(raw.len(), (miner_count * shares_per_miner) as usize);
        assert_eq!(
            summary.iter().map(|row| row.seen_shares).sum::<u64>(),
            raw.len() as u64
        );
        assert!(summary.len() <= miner_count as usize);
        assert!(summary.len() * 10 < raw.len());
    }

    #[test]
    fn apply_block_credits_with_fee_records_fee_atomically_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_000_000u64 + (rand::random::<u16>() as u64);
        let addr1 = format!("addr1-{suffix}");
        let addr2 = format!("addr2-{suffix}");
        let fee_address = format!("pool-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("fee-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");

        let credits = vec![(addr1.clone(), 60), (addr2, 40)];
        let fee = PoolFeeRecord {
            amount: 5,
            fee_address: fee_address.clone(),
            timestamp: SystemTime::now(),
        };
        assert!(store
            .apply_block_credits_and_mark_paid_with_fee(height, &credits, Some(&fee))
            .expect("apply with fee"));
        assert_eq!(store.get_balance(&addr1).expect("bal1").pending, 60);
        assert_eq!(store.get_balance(&fee_address).expect("fee bal").pending, 5);

        let fee_event = store
            .get_block_pool_fee_event(height)
            .expect("fee event")
            .expect("fee event exists");
        assert_eq!(fee_event.amount, 5);
        assert_eq!(fee_event.fee_address, fee_address);

        assert!(!store
            .apply_block_credits_and_mark_paid_with_fee(height, &credits, Some(&fee))
            .expect("second apply no-op"));
        assert_eq!(
            store
                .get_block_pool_fee_event(height)
                .expect("fee event after no-op")
                .expect("fee event exists after no-op")
                .id,
            fee_event.id
        );
    }

    #[test]
    fn apply_block_credits_with_invalid_fee_address_rolls_back_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_100_000u64 + (rand::random::<u16>() as u64);
        let addr1 = format!("addr1-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("rollback-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");

        let credits = vec![(addr1.clone(), 100)];
        let fee = PoolFeeRecord {
            amount: 10,
            fee_address: "   ".to_string(),
            timestamp: SystemTime::now(),
        };
        let err = store
            .apply_block_credits_and_mark_paid_with_fee(height, &credits, Some(&fee))
            .expect_err("empty fee address should fail");
        assert!(err.to_string().contains("fee address"));

        assert!(store
            .get_block_pool_fee_event(height)
            .expect("fee event")
            .is_none());
        assert_eq!(store.get_balance(&addr1).expect("bal").pending, 0);
        assert!(
            !store
                .get_block(height)
                .expect("block query")
                .expect("block exists")
                .paid_out
        );
    }

    #[test]
    fn complete_pending_payout_marks_reversible_block_credit_sources_as_paid_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_350_000u64 + (rand::random::<u16>() as u64);
        let addr = format!("credit-paid-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("credit-paid-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(addr.clone(), 100)])
            .expect("apply credits"));

        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&addr, 100, 3, "paid-source-tx")
            .expect("record broadcast");
        store
            .complete_pending_payout(&addr, 100, 3, "paid-source-tx")
            .expect("complete payout");

        let events = store
            .get_block_credit_events(height)
            .expect("credit events after payout");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].amount, 100);
        assert_eq!(events[0].paid_amount, 100);
        assert!(events[0].reversible);
    }

    #[test]
    fn revert_completed_payout_tx_restores_pending_for_valid_sources_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_356_000u64 + (rand::random::<u16>() as u64);
        let addr = format!("valid-revert-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("valid-revert-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(addr.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&addr, 100, 3, "valid-revert-tx")
            .expect("record broadcast");
        store
            .complete_pending_payout(&addr, 100, 3, "valid-revert-tx")
            .expect("complete payout");

        let reverted = store
            .revert_completed_payout_tx("valid-revert-tx", "missing after recovery")
            .expect("revert completed payout");
        assert_eq!(reverted.reverted_payout_rows, 1);
        assert_eq!(reverted.restored_pending_amount, 100);
        assert_eq!(reverted.dropped_orphaned_amount, 0);
        assert!(!reverted.manual_reconciliation_required);
        let balance = store.get_balance(&addr).expect("balance after revert");
        assert_eq!(balance.pending, 100);
        assert_eq!(balance.paid, 0);
        let events = store
            .get_block_credit_events(height)
            .expect("credit events after revert");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].paid_amount, 0);
        assert!(store
            .get_recent_payouts(10)
            .expect("recent payouts")
            .is_empty());
    }

    #[test]
    fn revert_completed_payout_tx_drops_orphaned_sources_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_357_000u64 + (rand::random::<u16>() as u64);
        let addr = format!("orphan-revert-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("orphan-revert-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(addr.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&addr, 100, 2, "orphan-revert-tx")
            .expect("record broadcast");
        store
            .complete_pending_payout(&addr, 100, 2, "orphan-revert-tx")
            .expect("complete payout");

        let orphaned = store
            .orphan_block_and_reverse_unpaid_credits(height)
            .expect("mark block orphaned");
        assert!(!orphaned.manual_reconciliation_required);
        assert!(store
            .get_block_credit_events(height)
            .expect("credit events after orphan reconcile")
            .is_empty());

        let reverted = store
            .revert_completed_payout_tx("orphan-revert-tx", "missing after recovery")
            .expect("revert orphaned payout");
        assert_eq!(reverted.reverted_payout_rows, 1);
        assert_eq!(reverted.restored_pending_amount, 0);
        assert_eq!(reverted.dropped_orphaned_amount, 100);
        assert!(!reverted.manual_reconciliation_required);
        let balance = store.get_balance(&addr).expect("balance after revert");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 0);
        assert!(store
            .get_block_credit_events(height)
            .expect("credit events after orphan revert")
            .is_empty());
        assert!(store
            .get_recent_payouts(10)
            .expect("recent payouts")
            .is_empty());
    }

    #[test]
    fn resolve_completed_payout_tx_override_restores_pending_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_358_000u64 + (rand::random::<u16>() as u64);
        let addr = format!("override-restore-{suffix}");
        let tx_hash = "override-restore-tx";
        store
            .add_block(&DbBlock {
                height,
                hash: format!("override-restore-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(addr.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&addr, 100, 2, tx_hash)
            .expect("record broadcast");
        store
            .complete_pending_payout(&addr, 100, 2, tx_hash)
            .expect("complete payout");
        store
            .conn()
            .execute(
                "UPDATE payouts SET reversible = FALSE WHERE tx_hash = $1",
                &[&tx_hash],
            )
            .expect("mark payout unresolved");

        let resolved = store
            .resolve_completed_payout_tx_override(
                tx_hash,
                ManualCompletedPayoutResolutionKind::RestorePending,
            )
            .expect("resolve payout override");
        assert_eq!(resolved.reverted_payout_rows, 1);
        assert_eq!(resolved.restored_pending_amount, 100);
        assert_eq!(resolved.dropped_orphaned_amount, 0);

        let balance = store.get_balance(&addr).expect("balance after resolve");
        assert_eq!(balance.pending, 100);
        assert_eq!(balance.paid, 0);
        let events = store
            .get_block_credit_events(height)
            .expect("credit events after resolve");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].paid_amount, 0);
        assert!(store
            .get_recent_payouts(10)
            .expect("recent payouts")
            .is_empty());
    }

    #[test]
    fn resolve_completed_payout_tx_override_drops_paid_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_359_000u64 + (rand::random::<u16>() as u64);
        let addr = format!("override-drop-{suffix}");
        let tx_hash = "override-drop-tx";
        store
            .add_block(&DbBlock {
                height,
                hash: format!("override-drop-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(addr.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&addr, 100, 2, tx_hash)
            .expect("record broadcast");
        store
            .complete_pending_payout(&addr, 100, 2, tx_hash)
            .expect("complete payout");
        store
            .conn()
            .execute(
                "UPDATE payouts SET reversible = FALSE WHERE tx_hash = $1",
                &[&tx_hash],
            )
            .expect("mark payout unresolved");

        let resolved = store
            .resolve_completed_payout_tx_override(
                tx_hash,
                ManualCompletedPayoutResolutionKind::DropPaid,
            )
            .expect("drop unresolved payout");
        assert_eq!(resolved.reverted_payout_rows, 1);
        assert_eq!(resolved.restored_pending_amount, 0);
        assert_eq!(resolved.dropped_orphaned_amount, 100);

        let balance = store.get_balance(&addr).expect("balance after drop");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 0);
        let events = store
            .get_block_credit_events(height)
            .expect("credit events after drop");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].paid_amount, 0);
        assert!(store
            .get_recent_payouts(10)
            .expect("recent payouts")
            .is_empty());
    }

    #[test]
    fn orphan_block_reverses_unpaid_credits_and_cancels_unbroadcast_pending_payouts_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_360_000u64 + (rand::random::<u16>() as u64);
        let miner = format!("orphan-miner-{suffix}");
        let fee_address = format!("orphan-fee-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("orphan-credit-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");

        let fee = PoolFeeRecord {
            amount: 7,
            fee_address: fee_address.clone(),
            timestamp: SystemTime::now(),
        };
        assert!(store
            .apply_block_credits_and_mark_paid_with_fee(height, &[(miner.clone(), 93)], Some(&fee),)
            .expect("apply credits with fee"));
        store
            .create_pending_payout(&miner, 93)
            .expect("create pending payout");

        let result = store
            .orphan_block_and_reverse_unpaid_credits(height)
            .expect("reconcile orphan");
        assert!(result.orphaned);
        assert_eq!(result.reversed_credit_events, 1);
        assert_eq!(result.reversed_credit_amount, 93);
        assert_eq!(result.reversed_fee_amount, 7);
        assert_eq!(result.canceled_pending_payouts, 1);
        assert!(!result.manual_reconciliation_required);

        let block = store
            .get_block(height)
            .expect("query block")
            .expect("block exists");
        assert!(block.orphaned);
        assert!(!block.confirmed);
        assert!(!block.paid_out);
        assert_eq!(store.get_balance(&miner).expect("miner balance").pending, 0);
        assert_eq!(
            store
                .get_balance(&fee_address)
                .expect("fee balance")
                .pending,
            0
        );
        assert!(store
            .get_pending_payout(&miner)
            .expect("pending payout lookup")
            .is_none());
        assert!(store
            .get_block_credit_events(height)
            .expect("credit events after orphan")
            .is_empty());
        assert_eq!(store.get_total_pool_fees().expect("total pool fees"), 0);
        assert!(store
            .get_block_pool_fee_event(height)
            .expect("fee event after orphan")
            .is_none());
    }

    #[test]
    fn orphan_block_archives_paid_credits_and_clears_live_blockers_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_370_000u64 + (rand::random::<u16>() as u64);
        let miner = format!("paid-orphan-miner-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("paid-orphan-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(miner.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&miner, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&miner)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&miner, 100, 2, "already-paid-orphan")
            .expect("record broadcast");
        store
            .complete_pending_payout(&miner, 100, 2, "already-paid-orphan")
            .expect("complete payout");

        let result = store
            .orphan_block_and_reverse_unpaid_credits(height)
            .expect("reconcile orphan");
        assert!(result.orphaned);
        assert!(!result.manual_reconciliation_required);
        assert_eq!(result.reversed_credit_amount, 0);

        let block = store
            .get_block(height)
            .expect("query block")
            .expect("block exists");
        assert!(block.orphaned);
        assert!(!block.confirmed);
        assert!(block.paid_out);

        assert!(store
            .get_block_credit_events(height)
            .expect("events after orphan reconcile")
            .is_empty());
        assert_eq!(store.get_balance(&miner).expect("miner balance").paid, 100);

        let issues = store
            .list_orphaned_block_credit_issues()
            .expect("load orphaned issues");
        assert!(!issues.iter().any(|issue| issue.height == height));

        let blockers = store
            .live_reconciliation_blockers()
            .expect("load live reconciliation blockers");
        assert_eq!(blockers.orphaned_block_issue_count, 0);
        assert_eq!(blockers.orphaned_unpaid_amount, 0);
    }

    #[test]
    fn orphan_block_debits_unpaid_remainder_after_partial_payout_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_371_000u64 + (rand::random::<u16>() as u64);
        let miner = format!("partial-paid-orphan-miner-{suffix}");
        let tx_hash = format!("partial-paid-orphan-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("partial-paid-orphan-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(miner.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&miner, 40)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&miner)
            .expect("mark send started")
            .expect("pending exists");
        store
            .record_pending_payout_broadcast(&miner, 40, 2, &tx_hash)
            .expect("record broadcast");
        store
            .complete_pending_payout(&miner, 40, 2, &tx_hash)
            .expect("complete payout");

        let result = store
            .orphan_block_and_reverse_unpaid_credits(height)
            .expect("reconcile orphan");
        assert!(result.orphaned);
        assert!(!result.manual_reconciliation_required);
        assert_eq!(result.reversed_credit_events, 1);
        assert_eq!(result.reversed_credit_amount, 60);
        assert!(store
            .get_block_credit_events(height)
            .expect("events after orphan cleanup")
            .is_empty());

        let balance = store.get_balance(&miner).expect("balance after orphan");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 40);

        let reverted = store
            .revert_completed_payout_tx(&tx_hash, "missing after recovery")
            .expect("revert partial orphan payout");
        assert_eq!(reverted.reverted_payout_rows, 1);
        assert_eq!(reverted.restored_pending_amount, 0);
        assert_eq!(reverted.dropped_orphaned_amount, 40);

        let balance = store.get_balance(&miner).expect("balance after revert");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 0);
    }

    #[test]
    fn orphaned_block_issue_reports_only_live_unpaid_remainder_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_371_500u64 + (rand::random::<u16>() as u64);
        let miner = format!("live-orphan-issue-miner-{suffix}");
        let fee_address = format!("live-orphan-issue-fee-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("live-orphan-issue-block-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        let fee = PoolFeeRecord {
            amount: 25,
            fee_address: fee_address.clone(),
            timestamp: SystemTime::now(),
        };
        assert!(
            store
                .apply_block_credits_and_mark_paid_with_fee(
                    height,
                    &[(miner.clone(), 100)],
                    Some(&fee),
                )
                .expect("apply credits with fee")
        );

        {
            let mut conn = store.conn();
            conn.execute(
                "UPDATE blocks
                 SET confirmed = FALSE, orphaned = TRUE, paid_out = FALSE
                 WHERE height = $1",
                &[&u64_to_i64(height).expect("height to i64")],
            )
            .expect("mark block orphaned");
            conn.execute(
                "UPDATE block_credit_events
                 SET paid_amount = 40
                 WHERE block_height = $1 AND address = $2",
                &[&u64_to_i64(height).expect("height to i64"), &miner],
            )
            .expect("seed partial paid miner credit");
            conn.execute(
                "UPDATE pool_fee_balance_credits
                 SET paid_amount = 5
                 WHERE block_height = $1 AND fee_address = $2",
                &[&u64_to_i64(height).expect("height to i64"), &fee_address],
            )
            .expect("seed partial paid fee credit");
        }

        let issues = store
            .list_orphaned_block_credit_issues()
            .expect("load orphaned issues");
        let issue = issues
            .iter()
            .find(|issue| issue.height == height)
            .expect("live orphan issue should remain actionable");
        assert_eq!(issue.remaining_credit_amount, 60);
        assert_eq!(issue.paid_credit_amount, 40);
        assert_eq!(issue.remaining_fee_amount, 20);
        assert_eq!(issue.paid_fee_amount, 5);
    }

    #[test]
    fn unique_block_identity_counts_dedup_live_and_archived_rows_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let ts = SystemTime::now();
        let canonical_height = 8_372_000u64 + (rand::random::<u16>() as u64);
        let duplicated_orphan_height = canonical_height + 1;
        let archived_only_orphan_height = canonical_height + 2;
        let miner = format!("unique-count-miner-{suffix}");
        let duplicate_payout_tx = format!("dup-orphan-payout-{suffix}");

        store
            .add_block(&DbBlock {
                height: canonical_height,
                hash: format!("canonical-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: ts,
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert canonical block");

        store
            .add_block(&DbBlock {
                height: duplicated_orphan_height,
                hash: format!("duplicated-orphan-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: ts,
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert duplicate-prone orphan block");
        assert!(store
            .apply_block_credits_and_mark_paid(duplicated_orphan_height, &[(miner.clone(), 100)])
            .expect("apply duplicate-prone orphan credits"));
        store
            .create_pending_payout(&miner, 100)
            .expect("create duplicate-prone orphan payout");
        store
            .mark_pending_payout_send_started(&miner)
            .expect("mark duplicate-prone payout send started")
            .expect("duplicate-prone pending payout exists");
        store
            .record_pending_payout_broadcast(&miner, 100, 2, &duplicate_payout_tx)
            .expect("record duplicate-prone payout broadcast");
        store
            .complete_pending_payout(&miner, 100, 2, &duplicate_payout_tx)
            .expect("complete duplicate-prone payout");
        store
            .orphan_block_and_reverse_unpaid_credits(duplicated_orphan_height)
            .expect("reconcile duplicate-prone orphan");

        store
            .add_block(&DbBlock {
                height: archived_only_orphan_height,
                hash: format!("archived-only-orphan-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: ts,
                confirmed: false,
                orphaned: true,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert archived-only orphan block");
        assert!(store
            .archive_conflicting_block_and_replace(&DbBlock {
                height: archived_only_orphan_height,
                hash: format!("replacement-{suffix}"),
                difficulty: 1,
                finder: format!("finder-{suffix}"),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: ts,
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("replace archived-only orphan"));

        let (total_blocks, orphaned_blocks) = store
            .get_unique_block_identity_counts()
            .expect("load unique block identity counts");

        assert_eq!(total_blocks, 5);
        assert_eq!(orphaned_blocks, 2);
    }

    #[test]
    fn list_active_verification_holds_omits_expired_quarantine_timestamp_when_force_verify_remains()
    {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let address = format!("risk-{}", unique_suffix());
        store
            .escalate_address_risk(AddressRiskEscalation::new(
                &address,
                "low difficulty share",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(24 * 60 * 60),
            ))
            .expect("escalate address risk");

        let expired = SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .expect("expired timestamp");
        store
            .conn()
            .with_retry("expire quarantined_until", |conn| {
                conn.execute(
                    "UPDATE address_risk SET quarantined_until = $2 WHERE address = $1",
                    &[&address, &to_unix(expired)],
                )?;
                Ok(())
            })
            .expect("expire quarantine");

        let holds = store
            .list_active_verification_holds(
                SystemTime::now()
                    .checked_sub(Duration::from_secs(60))
                    .expect("provisional cutoff"),
            )
            .expect("list active verification holds");
        let hold = holds
            .iter()
            .find(|hold| hold.address == address)
            .expect("hold row");
        assert!(hold.force_verify_until.is_some());
        assert!(hold.quarantined_until.is_none());
    }

    #[test]
    fn clear_address_risk_history_removes_risk_and_validation_rows() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let address = format!("risk-clear-{}", unique_suffix());
        store
            .escalate_address_risk(AddressRiskEscalation::new(
                &address,
                "invalid share proof",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
            ))
            .expect("seed address risk");
        store
            .upsert_validation_state(&PersistedValidationAddressState {
                address: address.clone(),
                total_shares: 10,
                sampled_shares: 2,
                invalid_samples: 1,
                risk_sampled_shares: 2,
                risk_invalid_samples: 1,
                forced_started_at: None,
                forced_until: Some(SystemTime::now() + Duration::from_secs(60)),
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: Some(ValidationHoldCause::ProvisionalBacklog),
                last_seen_at: SystemTime::now(),
            })
            .expect("seed validation state");
        store
            .add_validation_provisional(&address, None, SystemTime::now())
            .expect("seed validation provisional");

        store
            .clear_address_risk_history(&address)
            .expect("clear risk history");

        assert!(store
            .get_address_risk(&address)
            .expect("read cleared risk")
            .is_none());

        let loaded = store
            .load_validation_state(
                SystemTime::now()
                    .checked_sub(Duration::from_secs(60))
                    .expect("state cutoff"),
                SystemTime::now()
                    .checked_sub(Duration::from_secs(60))
                    .expect("provisional cutoff"),
                SystemTime::now()
                    .checked_sub(Duration::from_secs(60))
                    .expect("accepted cutoff"),
                SystemTime::now(),
            )
            .expect("load validation state");
        assert!(!loaded.states.iter().any(|state| state.address == address));
        assert!(!loaded
            .provisionals
            .iter()
            .any(|provisional| provisional.address == address));
        let clear_events = store
            .load_validation_clear_events_since(0)
            .expect("load validation clear events");
        assert!(clear_events.iter().any(|event| event.address == address));
    }

    #[test]
    fn pending_payout_broadcast_roundtrip_and_reset_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let addr = format!("addr-{suffix}");
        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark started");

        let broadcast = store
            .record_pending_payout_broadcast(&addr, 100, 42, "tx-1")
            .expect("record broadcast");
        assert_eq!(broadcast.tx_hash.as_deref(), Some("tx-1"));
        assert_eq!(broadcast.fee, Some(42));
        assert!(broadcast.sent_at.is_some());

        store
            .reset_pending_payout_send_state(&addr)
            .expect("reset send state");
        let reset = store
            .get_pending_payout(&addr)
            .expect("get pending")
            .expect("pending");
        assert!(reset.send_started_at.is_none());
        assert!(reset.tx_hash.is_none());
        assert!(reset.fee.is_none());
        assert!(reset.sent_at.is_none());
    }

    #[test]
    fn visible_payouts_for_address_include_pending_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let addr = format!("addr-{suffix}");
        store
            .conn()
            .execute(
                "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES ($1, $2, $3, $4, $5)",
                &[&addr, &10i64, &1i64, &format!("tx-confirmed-{suffix}"), &100i64],
            )
            .expect("insert confirmed payout");
        store
            .create_pending_payout(&addr, 25)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark started");
        let pending_tx = format!("tx-pending-{suffix}");
        let broadcast = store
            .record_pending_payout_broadcast(&addr, 25, 2, &pending_tx)
            .expect("record broadcast");
        store
            .create_pending_payout(&addr, 30)
            .expect("create second pending");

        let payouts = store
            .get_recent_visible_payouts_for_address(&addr, 10)
            .expect("visible payouts");
        assert_eq!(payouts.len(), 3);
        assert_eq!(payouts[0].tx_hash, "");
        assert_eq!(payouts[0].amount, 30);
        assert!(!payouts[0].confirmed);
        assert_eq!(payouts[1].tx_hash, pending_tx);
        assert_eq!(payouts[1].timestamp, broadcast.sent_at.expect("sent at"));
        assert!(!payouts[1].confirmed);
        assert!(payouts[2].confirmed);
    }

    #[test]
    fn get_recent_payouts_for_address_filters_and_orders_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let addr1 = format!("addr1-{suffix}");
        let addr2 = format!("addr2-{suffix}");

        store
            .add_payout_with_batch(&addr1, 10, 1, &format!("tx-a-{suffix}"), None)
            .expect("add payout a");
        store
            .add_payout_with_batch(&addr2, 20, 2, &format!("tx-b-{suffix}"), None)
            .expect("add payout b");
        store
            .add_payout_with_batch(&addr1, 30, 3, &format!("tx-c-{suffix}"), None)
            .expect("add payout c");

        let addr1_payouts = store
            .get_recent_payouts_for_address(&addr1, 10)
            .expect("addr1 payouts");
        assert_eq!(addr1_payouts.len(), 2);
        assert!(addr1_payouts[0].tx_hash.contains("tx-c-"));
        assert!(addr1_payouts[1].tx_hash.contains("tx-a-"));

        let addr2_payouts = store
            .get_recent_payouts_for_address(&addr2, 10)
            .expect("addr2 payouts");
        assert_eq!(addr2_payouts.len(), 1);
        assert!(addr2_payouts[0].tx_hash.contains("tx-b-"));
    }

    #[test]
    fn public_payout_batches_include_broadcast_pending_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let confirmed_addr = format!("addr-confirmed-{suffix}");
        let pending_addr = format!("addr-pending-{suffix}");
        let confirmed_tx = format!("tx-confirmed-{suffix}");
        let pending_tx = format!("tx-pending-{suffix}");

        store
            .add_payout_with_batch(
                &confirmed_addr,
                10,
                1,
                &confirmed_tx,
                Some(&format!("batch-confirmed-{suffix}")),
            )
            .expect("insert confirmed payout");
        store
            .create_pending_payout(&pending_addr, 25)
            .expect("create pending");
        store
            .mark_pending_payouts_send_started_batch(
                &[(pending_addr.clone(), 25)],
                &format!("batch-pending-{suffix}"),
                None,
            )
            .expect("mark started");
        store
            .record_pending_payout_batch_broadcast(
                &[PendingPayoutBatchMember {
                    address: pending_addr.clone(),
                    amount: 25,
                    fee: 2,
                }],
                &format!("batch-pending-{suffix}"),
                &pending_tx,
            )
            .expect("record broadcast");

        let (items, total) = store
            .get_public_payout_batches_page(10, 0)
            .expect("public payout batches");
        assert!(total >= 2);
        let pending = items
            .iter()
            .find(|item| item.tx_hashes.iter().any(|tx| tx == &pending_tx))
            .expect("pending payout batch");
        assert!(!pending.confirmed);
        let confirmed = items
            .iter()
            .find(|item| item.tx_hashes.iter().any(|tx| tx == &confirmed_tx))
            .expect("confirmed payout batch");
        assert!(confirmed.confirmed);
    }

    #[test]
    fn complete_pending_payout_preserves_broadcast_timestamp_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let addr = format!("addr-{suffix}");
        let txid = format!("tx-{suffix}");
        store
            .update_balance(&Balance {
                address: addr.clone(),
                pending: 100,
                paid: 0,
            })
            .expect("seed balance");
        store
            .create_pending_payout(&addr, 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&addr)
            .expect("mark started");
        let broadcast = store
            .record_pending_payout_broadcast(&addr, 100, 42, &txid)
            .expect("record broadcast");

        store
            .complete_pending_payout(&addr, 100, 42, &txid)
            .expect("complete payout");

        let payouts = store.get_recent_payouts(1).expect("recent payouts");
        let payout = payouts
            .into_iter()
            .find(|payout| payout.address == addr && payout.tx_hash == txid)
            .expect("matching payout");
        assert_eq!(payout.timestamp, broadcast.sent_at.expect("sent at"));
        assert!(payout.confirmed);
    }

    #[test]
    fn import_confirmed_payout_txs_moves_pending_balance_to_paid_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let miner = format!("miner-{suffix}");
        let fee_address = format!("pool-{suffix}");
        let tx_hash = format!("imported-{suffix}");
        let height = 9_500_000u64 + (rand::random::<u16>() as u64);
        let timestamp = UNIX_EPOCH + Duration::from_secs(33_000_000);

        store
            .add_block(&DbBlock {
                height,
                hash: format!("blk-{suffix}"),
                difficulty: 1,
                finder: miner.clone(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp,
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        store
            .apply_block_credits_and_mark_paid_with_fee(
                height,
                &[(miner.clone(), 80)],
                Some(&PoolFeeRecord {
                    amount: 20,
                    fee_address: fee_address.clone(),
                    timestamp,
                }),
            )
            .expect("credit balances");
        store
            .create_pending_payout(&miner, 80)
            .expect("create miner pending");
        store
            .create_pending_payout(&fee_address, 20)
            .expect("create fee pending");

        let report = store
            .import_confirmed_payout_txs(&[ConfirmedPayoutImportTx {
                tx_hash: tx_hash.clone(),
                timestamp,
                recipients: vec![
                    ConfirmedPayoutImportRecipient {
                        address: fee_address.clone(),
                        amount: 20,
                        fee: 2,
                    },
                    ConfirmedPayoutImportRecipient {
                        address: miner.clone(),
                        amount: 80,
                        fee: 7,
                    },
                ],
            }])
            .expect("import confirmed payout tx");

        assert_eq!(report.imported_txs, 1);
        assert_eq!(report.imported_payout_rows, 2);
        assert_eq!(report.imported_amount, 100);
        assert_eq!(report.imported_fee, 9);
        assert_eq!(report.canceled_pending_payouts, 2);
        assert_eq!(report.recorded_manual_offset_amount, 0);

        let miner_balance = store.get_balance(&miner).expect("miner balance");
        assert_eq!(miner_balance.pending, 0);
        assert_eq!(miner_balance.paid, 80);
        let fee_balance = store.get_balance(&fee_address).expect("fee balance");
        assert_eq!(fee_balance.pending, 0);
        assert_eq!(fee_balance.paid, 20);
        assert!(store
            .get_pending_payout(&miner)
            .expect("miner pending lookup")
            .is_none());
        assert!(store
            .get_pending_payout(&fee_address)
            .expect("fee pending lookup")
            .is_none());

        let payouts = store.get_recent_payouts(10).expect("recent payouts");
        assert!(payouts.iter().any(|payout| {
            payout.tx_hash == tx_hash
                && payout.address == miner
                && payout.amount == 80
                && payout.fee == 7
        }));
        assert!(payouts.iter().any(|payout| {
            payout.tx_hash == tx_hash
                && payout.address == fee_address
                && payout.amount == 20
                && payout.fee == 2
        }));

        let sources = store
            .list_balance_source_summaries()
            .expect("balance source summaries");
        let miner_sources = sources
            .iter()
            .find(|source| source.address == miner)
            .expect("miner source summary");
        assert_eq!(miner_sources.canonical_pending, 0);
        let fee_sources = sources
            .iter()
            .find(|source| source.address == fee_address)
            .expect("fee source summary");
        assert_eq!(fee_sources.canonical_pending, 0);
    }

    #[test]
    fn import_confirmed_payout_txs_tracks_manual_offset_for_overpay_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let miner = format!("miner-{suffix}");
        let tx_hash = format!("advance-{suffix}");
        let base_height = 9_600_000u64 + (rand::random::<u16>() as u64);
        let timestamp = UNIX_EPOCH + Duration::from_secs(34_000_000);

        store
            .add_block(&DbBlock {
                height: base_height,
                hash: format!("seed-{suffix}"),
                difficulty: 1,
                finder: miner.clone(),
                finder_worker: "rig".to_string(),
                reward: 30,
                timestamp,
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert seed block");
        store
            .apply_block_credits_and_mark_paid(base_height, &[(miner.clone(), 30)])
            .expect("seed credits");

        let report = store
            .import_confirmed_payout_txs(&[ConfirmedPayoutImportTx {
                tx_hash: tx_hash.clone(),
                timestamp,
                recipients: vec![ConfirmedPayoutImportRecipient {
                    address: miner.clone(),
                    amount: 50,
                    fee: 1,
                }],
            }])
            .expect("import advanced payout");
        assert_eq!(report.recorded_manual_offset_amount, 20);

        let offset_after_import = store
            .conn()
            .query_one(
                "SELECT amount FROM manual_payout_offsets WHERE address = $1",
                &[&miner],
            )
            .expect("offset row after import")
            .get::<_, i64>(0);
        assert_eq!(offset_after_import, 20);

        let miner_balance = store.get_balance(&miner).expect("balance after import");
        assert_eq!(miner_balance.pending, 0);
        assert_eq!(miner_balance.paid, 50);

        store
            .add_block(&DbBlock {
                height: base_height + 1,
                hash: format!("consume-{suffix}"),
                difficulty: 1,
                finder: miner.clone(),
                finder_worker: "rig".to_string(),
                reward: 20,
                timestamp: timestamp + Duration::from_secs(300),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert consume block");
        store
            .apply_block_credits_and_mark_paid(base_height + 1, &[(miner.clone(), 20)])
            .expect("apply consume credits");

        let miner_balance = store.get_balance(&miner).expect("balance after consume");
        assert_eq!(miner_balance.pending, 0);
        assert_eq!(miner_balance.paid, 50);
        let source_summary = store
            .list_balance_source_summaries()
            .expect("source summaries")
            .into_iter()
            .find(|source| source.address == miner)
            .expect("miner source summary after consume");
        assert_eq!(source_summary.canonical_pending, 0);
        let offset_after_consume = store
            .conn()
            .query_opt(
                "SELECT amount FROM manual_payout_offsets WHERE address = $1",
                &[&miner],
            )
            .expect("offset lookup after consume");
        assert!(offset_after_consume.is_none());

        store
            .add_block(&DbBlock {
                height: base_height + 2,
                hash: format!("fresh-{suffix}"),
                difficulty: 1,
                finder: miner.clone(),
                finder_worker: "rig".to_string(),
                reward: 5,
                timestamp: timestamp + Duration::from_secs(600),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert fresh block");
        store
            .apply_block_credits_and_mark_paid(base_height + 2, &[(miner.clone(), 5)])
            .expect("apply fresh credits");

        let miner_balance = store
            .get_balance(&miner)
            .expect("balance after fresh credit");
        assert_eq!(miner_balance.pending, 5);
        let source_summary = store
            .list_balance_source_summaries()
            .expect("source summaries after fresh credit")
            .into_iter()
            .find(|source| source.address == miner)
            .expect("miner source summary after fresh credit");
        assert_eq!(source_summary.canonical_pending, 5);
    }

    #[test]
    fn apply_manual_payout_offsets_to_live_pending_consumes_current_canonical_pending_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let miner = format!("offset-apply-miner-{suffix}");
        let tx_hash = format!("offset-apply-{suffix}");
        let base_height = 9_620_000u64 + (rand::random::<u16>() as u64);
        let pending_height = base_height + 1;
        let timestamp = UNIX_EPOCH + Duration::from_secs(34_500_000);

        store
            .add_block(&DbBlock {
                height: base_height,
                hash: format!("seed-{suffix}"),
                difficulty: 1,
                finder: miner.clone(),
                finder_worker: "rig".to_string(),
                reward: 30,
                timestamp,
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert seed block");
        store
            .apply_block_credits_and_mark_paid(base_height, &[(miner.clone(), 30)])
            .expect("seed credits");
        store
            .import_confirmed_payout_txs(&[ConfirmedPayoutImportTx {
                tx_hash,
                timestamp,
                recipients: vec![ConfirmedPayoutImportRecipient {
                    address: miner.clone(),
                    amount: 50,
                    fee: 1,
                }],
            }])
            .expect("import advanced payout");

        store
            .add_block(&DbBlock {
                height: pending_height,
                hash: format!("pending-{suffix}"),
                difficulty: 1,
                finder: miner.clone(),
                finder_worker: "rig".to_string(),
                reward: 7,
                timestamp: timestamp + Duration::from_secs(300),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert pending block");

        {
            let mut conn = store.conn();
            conn.execute(
                "INSERT INTO block_credit_events (block_height, address, amount, paid_amount, reversible)
                 VALUES ($1, $2, $3, 0, TRUE)",
                &[
                    &u64_to_i64(pending_height).expect("pending height to i64"),
                    &miner,
                    &7i64,
                ],
            )
            .expect("insert current canonical pending credit");
            conn.execute(
                "UPDATE balances
                 SET pending = pending + 7
                 WHERE address = $1",
                &[&miner],
            )
            .expect("seed live pending balance");
        }

        let report = store
            .apply_manual_payout_offsets_to_live_pending()
            .expect("apply manual offsets");
        assert_eq!(report.scanned_offset_addresses, 1);
        assert_eq!(report.offset_amount_before, 20);
        assert_eq!(report.applied_address_count, 1);
        assert_eq!(report.applied_amount, 7);
        assert_eq!(report.remaining_offset_amount, 13);
        assert_eq!(report.applications.len(), 1);

        let application = &report.applications[0];
        assert_eq!(application.address, miner);
        assert_eq!(application.applied_amount, 7);
        assert_eq!(application.remaining_offset_amount, 13);
        assert_eq!(application.remaining_balance_pending, 0);
        assert_eq!(application.remaining_canonical_pending, 0);

        let balance = store
            .get_balance(&application.address)
            .expect("balance after apply");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 50);

        let offset_row = store
            .conn()
            .query_one(
                "SELECT amount FROM manual_payout_offsets WHERE address = $1",
                &[&application.address],
            )
            .expect("remaining offset row");
        assert_eq!(offset_row.get::<_, i64>(0), 13);

        let source_row = store
            .conn()
            .query_one(
                "SELECT paid_amount
                 FROM block_credit_events
                 WHERE block_height = $1 AND address = $2",
                &[
                    &u64_to_i64(pending_height).expect("pending height to i64"),
                    &application.address,
                ],
            )
            .expect("updated source row");
        assert_eq!(source_row.get::<_, i64>(0), 7);
    }
}
