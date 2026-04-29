use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use postgres::{types::ToSql, Client, Config as PostgresConfig, NoTls, Row, Transaction};
use tracing::warn;

use crate::db::{
    ActiveVerificationHold, AddressRiskState, Balance, BlockCreditEvent, DbBlock, DbLuckRound,
    DbShare, MonitorHeartbeat, MonitorHeartbeatUpsert, MonitorIncident, MonitorIncidentUpsert,
    Payout, PendingPayout, PendingPayoutBatchMember, PoolFeeEvent, PoolFeeRecord,
    PublicPayoutBatch, ShareReplayData, ShareReplayUpdate, ValidationHoldCause,
    ValidationHoldState,
};
use crate::engine::{ShareRecord, ShareStore};
use crate::stats::RejectionReasonCount;
use crate::validation::{
    LoadedValidationAddressActivity, LoadedValidationState, PersistedValidationAcceptedShare,
    PersistedValidationAddressState, PersistedValidationProvisional, ValidationClearEvent,
};

const SHARE_CLAIM_EXPIRY_SECS: i64 = 2 * 60;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, Default)]
pub struct MinerShareWindowStats {
    pub accepted_difficulty: u64,
    pub rejected_difficulty: u64,
    pub accepted_count: u64,
    pub rejected_count: u64,
    pub stale_rejected_difficulty: u64,
    pub stale_rejected_count: u64,
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

#[derive(Debug, Clone, Default)]
pub struct VardiffHintSummary {
    pub total_workers: u64,
    pub below_floor_workers: u64,
    pub at_floor_workers: u64,
    pub above_floor_workers: u64,
    pub min_difficulty: Option<u64>,
    pub median_difficulty: Option<u64>,
    pub max_difficulty: Option<u64>,
    pub latest_updated_at: Option<SystemTime>,
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

#[derive(Debug, Clone)]
pub struct VardiffHintDiagnostic {
    pub worker: String,
    pub difficulty: u64,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PoolFeeCreditBackfillReport {
    pub credited_events: u64,
    pub credited_amount: u64,
    pub reconciled_finder_fallback_events: u64,
    pub reconciled_finder_fallback_amount: u64,
    pub skipped_mismatched_destination: u64,
}

#[derive(Debug, Clone)]
pub struct HistoricalMissingMinerCreditsBlock {
    pub block: DbBlock,
    pub recorded_fee_amount: u64,
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
pub struct ExistingOrphanBlockReconciliationReport {
    pub blocks_scanned: u64,
    pub blocks_reconciled: u64,
    pub blocks_requiring_manual_reconciliation: u64,
    pub reversed_credit_events: u64,
    pub reversed_credit_amount: u64,
    pub reversed_fee_amount: u64,
    pub canceled_pending_payouts: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PayoutReconciliationBackfillReport {
    pub payouts_linked: u64,
    pub payouts_unreversible: u64,
    pub source_credits_marked_reversible: u64,
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
pub struct LiveReconciliationBlockers {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    fn from_db(value: &str) -> Self {
        match value {
            "fee" => Self::Fee,
            _ => Self::Block,
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
    _orphan_pending: u64,
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
}

impl ManagedClient {
    fn new(client: Client, url: &str, schema: Option<&str>, label: &str) -> Self {
        Self {
            client,
            url: url.to_string(),
            schema: schema.map(str::to_string),
            label: label.to_string(),
        }
    }

    fn reconnect(&mut self) -> Result<()> {
        self.client =
            PostgresStore::connect_client(&self.url, self.schema.as_deref(), &self.label)?;
        Ok(())
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

    fn query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        self.with_retry("query", |client| Ok(client.query(query, params)?))
    }

    fn query_one(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Row> {
        self.with_retry("query_one", |client| Ok(client.query_one(query, params)?))
    }

    fn query_opt(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        self.with_retry("query_opt", |client| Ok(client.query_opt(query, params)?))
    }

    fn transaction(&mut self) -> Result<Transaction<'_>> {
        self.ensure_connected()?;
        Ok(self.client.transaction()?)
    }
}

pub struct PostgresStore {
    conns: Vec<Mutex<ManagedClient>>,
    next_conn: AtomicUsize,
}

impl PostgresStore {
    fn conn(&self) -> &Mutex<ManagedClient> {
        let pool_len = self.conns.len().max(1);
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % pool_len;
        self.conns
            .get(idx)
            .expect("postgres store connection unavailable")
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
    force_verify_until BIGINT,
    suspected_fraud_strikes BIGINT NOT NULL DEFAULT 0,
    suspected_fraud_window_until BIGINT
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
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS reject_reason TEXT")
            .context("ensure shares.reject_reason column")?;
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS claimed_hash TEXT")
            .context("ensure shares.claimed_hash column")?;
        conn.batch_execute(
            "ALTER TABLE blocks ADD COLUMN IF NOT EXISTS effort_pct DOUBLE PRECISION",
        )
        .context("ensure blocks.effort_pct column")?;
        conn.batch_execute(
            "ALTER TABLE address_risk ADD COLUMN IF NOT EXISTS strike_window_until BIGINT",
        )
        .context("ensure address_risk.strike_window_until column")?;
        conn.batch_execute(
            "ALTER TABLE address_risk ADD COLUMN IF NOT EXISTS suspected_fraud_strikes BIGINT NOT NULL DEFAULT 0",
        )
        .context("ensure address_risk.suspected_fraud_strikes column")?;
        conn.batch_execute(
            "ALTER TABLE address_risk ADD COLUMN IF NOT EXISTS suspected_fraud_window_until BIGINT",
        )
        .context("ensure address_risk.suspected_fraud_window_until column")?;
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
        conn.execute(
            "UPDATE payout_credit_allocations allocation
             SET block_hash = blocks.hash
             FROM blocks
             WHERE allocation.block_hash IS NULL
               AND allocation.block_height = blocks.height",
            &[],
        )
        .context("backfill payout_credit_allocations.block_hash from live blocks")?;

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
        }))
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

    pub fn add_share_with_id(&self, share: ShareRecord) -> Result<i64> {
        self.add_share_with_replay_and_id(share, None)
    }

    pub fn add_share_with_replay_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<i64> {
        let created = to_unix(share.created_at);
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;

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
            "INSERT INTO shares (job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, reject_reason, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
            ],
        )?;
        tx.commit()?;
        Ok(row.get::<_, i64>(0))
    }

    pub fn get_share_replay(&self, job_id: &str) -> Result<Option<ShareReplayData>> {
        let row = self.conn().lock().query_opt(
            "SELECT job_id, header_base, network_target, created_at
             FROM share_job_replays
             WHERE job_id = $1",
            &[&job_id],
        )?;
        row.map(row_to_share_replay).transpose()
    }

    pub fn get_share_replays_for_job_ids(
        &self,
        job_ids: &[String],
    ) -> Result<HashMap<String, ShareReplayData>> {
        if job_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = self.conn().lock().query(
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

    pub fn apply_share_replay_updates(&self, updates: &[ShareReplayUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut conn = self.conn().lock();
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

    pub fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<()> {
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;
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
        tx.execute(
            "DELETE FROM validation_provisionals WHERE share_id = $1",
            &[&update.share_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_recent_shares(&self, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares ORDER BY created_at DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares WHERE miner = $1 ORDER BY created_at DESC LIMIT $2",
            &[&address, &limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_provisional_shares_for_miner_since(
        &self,
        address: &str,
        cutoff: SystemTime,
        limit: i64,
    ) -> Result<Vec<DbShare>> {
        let rows = self.conn().lock().query(
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
        let row = self.conn().lock().query_one(
            "SELECT MIN(created_at) FROM shares WHERE miner = $1",
            &[&address],
        )?;
        Ok(row.get::<_, Option<i64>>(0).map(from_unix))
    }

    pub fn get_shares_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<DbShare>> {
        let start_ts = to_unix(start);
        let end_ts = to_unix(end);
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares WHERE created_at >= $1 AND created_at <= $2",
            &[&start_ts, &end_ts],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_last_n_shares_before(&self, before: SystemTime, n: i64) -> Result<Vec<DbShare>> {
        let before_ts = to_unix(before);
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, claimed_hash, created_at
             FROM shares WHERE created_at <= $1 ORDER BY created_at DESC LIMIT $2",
            &[&before_ts, &n],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn summarize_shares_between(
        &self,
        start: SystemTime,
        end: SystemTime,
        provisional_ready_cutoff: Option<SystemTime>,
    ) -> Result<Vec<ShareWindowAddressPreview>> {
        let start_ts = to_unix(start);
        let end_ts = to_unix(end);
        let ready_enabled = provisional_ready_cutoff.is_some();
        let ready_cutoff = provisional_ready_cutoff.map(to_unix).unwrap_or_default();
        let rows = self.conn().lock().query(
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

    pub fn summarize_last_n_shares_before(
        &self,
        before: SystemTime,
        n: i64,
        provisional_ready_cutoff: Option<SystemTime>,
    ) -> Result<Vec<ShareWindowAddressPreview>> {
        let before_ts = to_unix(before);
        let ready_enabled = provisional_ready_cutoff.is_some();
        let ready_cutoff = provisional_ready_cutoff.map(to_unix).unwrap_or_default();
        let rows = self.conn().lock().query(
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
             FROM (
                SELECT miner, difficulty, status, created_at
                FROM shares
                WHERE created_at <= $1
                ORDER BY created_at DESC
                LIMIT $2
             ) AS recent_shares
             GROUP BY miner
             ORDER BY miner ASC",
            &[&before_ts, &n.max(1), &ready_enabled, &ready_cutoff],
        )?;
        Ok(rows
            .into_iter()
            .map(row_to_share_window_preview)
            .collect::<Vec<_>>())
    }

    pub fn latest_share_timestamp_for_block_hash(&self, hash: &str) -> Result<Option<SystemTime>> {
        let row = self.conn().lock().query_one(
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

        let rows = self.conn().lock().query(
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
        let row = self.conn().lock().query_one(
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
        let row = self.conn().lock().query_one(
            "SELECT COALESCE(SUM(difficulty),0)::bigint, COUNT(*)::bigint, MIN(created_at), MAX(created_at)
             FROM shares
             WHERE created_at >= $1
               AND status IN ('verified','provisional')",
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

    pub fn get_total_share_count(&self) -> Result<u64> {
        let mut conn = self.conn().lock();
        let live_row = conn.query_one("SELECT COUNT(*)::bigint FROM shares", &[])?;
        let live_count: i64 = live_row.get(0);
        let summarized_row = conn.query_one(
            "SELECT COALESCE(SUM(accepted_count + rejected_count), 0)::bigint FROM share_daily_summaries",
            &[],
        )?;
        let summarized_count: i64 = summarized_row.get(0);
        Ok((live_count.max(0) as u64).saturating_add(summarized_count.max(0) as u64))
    }

    pub fn share_outcome_counts_since(&self, since: SystemTime) -> Result<(u64, u64)> {
        let since_ts = to_unix(since);
        let row = self.conn().lock().query_one(
            "SELECT
                COALESCE(SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END), 0)::bigint,
                COALESCE(SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END), 0)::bigint
             FROM shares
             WHERE created_at >= $1",
            &[&since_ts],
        )?;
        let accepted: i64 = row.get(0);
        let rejected: i64 = row.get(1);
        Ok((accepted.max(0) as u64, rejected.max(0) as u64))
    }

    pub fn miner_share_window_stats_since(
        &self,
        miner: &str,
        since: SystemTime,
    ) -> Result<MinerShareWindowStats> {
        let since_ts = to_unix(since);
        let row = self.conn().lock().query_one(
            "SELECT
                COALESCE(SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END), 0)::bigint,
                COALESCE(SUM(CASE WHEN status NOT IN ('verified','provisional') THEN difficulty ELSE 0 END), 0)::bigint,
                COUNT(*) FILTER (WHERE status IN ('verified','provisional'))::bigint,
                COUNT(*) FILTER (WHERE status NOT IN ('verified','provisional'))::bigint,
                COALESCE(SUM(CASE
                    WHEN status NOT IN ('verified','provisional')
                     AND COALESCE(NULLIF(BTRIM(reject_reason), ''), 'legacy / unknown') = 'stale job'
                    THEN difficulty
                    ELSE 0
                END), 0)::bigint,
                COUNT(*) FILTER (
                    WHERE status NOT IN ('verified','provisional')
                      AND COALESCE(NULLIF(BTRIM(reject_reason), ''), 'legacy / unknown') = 'stale job'
                )::bigint
             FROM shares
             WHERE miner = $1
               AND created_at >= $2",
            &[&miner, &since_ts],
        )?;
        Ok(MinerShareWindowStats {
            accepted_difficulty: row.get::<_, i64>(0).max(0) as u64,
            rejected_difficulty: row.get::<_, i64>(1).max(0) as u64,
            accepted_count: row.get::<_, i64>(2).max(0) as u64,
            rejected_count: row.get::<_, i64>(3).max(0) as u64,
            stale_rejected_difficulty: row.get::<_, i64>(4).max(0) as u64,
            stale_rejected_count: row.get::<_, i64>(5).max(0) as u64,
        })
    }

    pub fn total_rejected_share_count(&self) -> Result<u64> {
        let mut conn = self.conn().lock();
        let live_row = conn.query_one(
            "SELECT COUNT(*)::bigint FROM shares WHERE status NOT IN ('verified','provisional')",
            &[],
        )?;
        let live_count: i64 = live_row.get(0);
        let summarized_row = conn.query_one(
            "SELECT COALESCE(SUM(rejected_count), 0)::bigint FROM share_daily_summaries",
            &[],
        )?;
        let summarized_count: i64 = summarized_row.get(0);
        Ok((live_count.max(0) as u64).saturating_add(summarized_count.max(0) as u64))
    }

    pub fn rejection_reason_counts_since(
        &self,
        since: SystemTime,
    ) -> Result<Vec<RejectionReasonCount>> {
        let since_ts = to_unix(since);
        let rows = self.conn().lock().query(
            "SELECT
                COALESCE(NULLIF(BTRIM(reject_reason), ''), 'legacy / unknown') AS reason,
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
        let live_rows = self.conn().lock().query(
            "SELECT
                COALESCE(NULLIF(BTRIM(reject_reason), ''), 'legacy / unknown') AS reason,
                COUNT(*)::bigint AS rejected_count
             FROM shares
             WHERE status NOT IN ('verified','provisional')
             GROUP BY reason",
            &[],
        )?;
        let summary_rows = self.conn().lock().query(
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
        self.conn().lock().execute(
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

    pub fn insert_block_if_absent(&self, block: &DbBlock) -> Result<bool> {
        let inserted = self.conn().lock().execute(
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

    pub fn archive_conflicting_block_and_replace(&self, block: &DbBlock) -> Result<bool> {
        let mut conn = self.conn().lock();
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
        let row = self.conn().lock().query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE height = $1",
            &[&u64_to_i64(height)?],
        )?;
        Ok(row.map(|v| row_to_block(&v)))
    }

    pub fn update_block(&self, block: &DbBlock) -> Result<()> {
        self.add_block(block)
    }

    pub fn get_recent_blocks(&self, limit: i64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks ORDER BY height DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_recent_blocks_up_to(&self, limit: i64, max_height: u64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
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
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks ORDER BY height DESC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_luck_rounds_page(&self, limit: i64, offset: i64) -> Result<(Vec<DbLuckRound>, u64)> {
        let mut conn = self.conn().lock();
        let total_row =
            conn.query_one("SELECT GREATEST(COUNT(*) - 1, 0)::bigint FROM blocks", &[])?;
        let total: i64 = total_row.get(0);
        let rows = conn.query(
            "WITH ordered_blocks AS (
                SELECT
                    height,
                    hash,
                    difficulty,
                    timestamp,
                    confirmed,
                    orphaned,
                    lag(timestamp) OVER (ORDER BY timestamp ASC) AS prev_timestamp
                FROM blocks
             ),
             paged_blocks AS (
                SELECT *
                FROM ordered_blocks
                WHERE prev_timestamp IS NOT NULL
                ORDER BY height DESC
                LIMIT $1 OFFSET $2
             )
             SELECT
                pb.height,
                pb.hash,
                pb.difficulty,
                pb.timestamp,
                pb.confirmed,
                pb.orphaned,
                COALESCE(sw.round_work, 0)::bigint AS round_work,
                GREATEST(pb.timestamp - pb.prev_timestamp, 0)::bigint AS duration_seconds
             FROM paged_blocks pb
             LEFT JOIN LATERAL (
                SELECT COALESCE(SUM(difficulty), 0)::bigint AS round_work
                FROM shares
                WHERE created_at >= pb.prev_timestamp
                  AND created_at <= pb.timestamp
                  AND status IN ('verified', 'provisional')
             ) sw ON TRUE
             ORDER BY pb.height DESC",
            &[&limit.max(1), &offset.max(0)],
        )?;
        Ok((
            rows.into_iter()
                .map(|row| row_to_luck_round(&row))
                .collect(),
            total.max(0) as u64,
        ))
    }

    pub fn get_recent_luck_rounds(&self, limit: i64) -> Result<Vec<DbLuckRound>> {
        Ok(self.get_luck_rounds_page(limit, 0)?.0)
    }

    pub fn get_luck_rounds_for_hashes(
        &self,
        hashes: &[String],
    ) -> Result<HashMap<String, DbLuckRound>> {
        if hashes.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = self.conn().lock().query(
            "WITH ordered_blocks AS (
                SELECT
                    height,
                    hash,
                    difficulty,
                    timestamp,
                    confirmed,
                    orphaned,
                    lag(timestamp) OVER (ORDER BY timestamp ASC) AS prev_timestamp
                FROM blocks
             )
             SELECT
                ob.height,
                ob.hash,
                ob.difficulty,
                ob.timestamp,
                ob.confirmed,
                ob.orphaned,
                COALESCE(sw.round_work, 0)::bigint AS round_work,
                GREATEST(ob.timestamp - ob.prev_timestamp, 0)::bigint AS duration_seconds
             FROM ordered_blocks ob
             LEFT JOIN LATERAL (
                SELECT COALESCE(SUM(difficulty), 0)::bigint AS round_work
                FROM shares
                WHERE created_at >= ob.prev_timestamp
                  AND created_at <= ob.timestamp
                  AND status IN ('verified', 'provisional')
             ) sw ON TRUE
             WHERE ob.prev_timestamp IS NOT NULL
               AND ob.hash = ANY($1)",
            &[&hashes],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let round = row_to_luck_round(&row);
                (round.block_hash.clone(), round)
            })
            .collect())
    }

    pub fn avg_effort_pct(&self) -> Result<Option<f64>> {
        let row = self.conn().lock().query_one(
            "SELECT AVG(effort_pct) FROM blocks WHERE orphaned = false AND effort_pct IS NOT NULL",
            &[],
        )?;
        Ok(row.get::<_, Option<f64>>(0))
    }

    pub fn avg_effort_pct_up_to(&self, max_height: u64) -> Result<Option<f64>> {
        let row = self.conn().lock().query_one(
            "SELECT AVG(effort_pct)
             FROM blocks
             WHERE orphaned = FALSE
               AND effort_pct IS NOT NULL
               AND height <= $1",
            &[&u64_to_i64(max_height)?],
        )?;
        Ok(row.get::<_, Option<f64>>(0))
    }

    pub fn get_blocks_page(
        &self,
        finder: Option<&str>,
        status: Option<&str>,
        sort: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<DbBlock>, u64)> {
        let finder_pattern = finder
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| format!("%{}%", v.to_ascii_lowercase()));
        let status_filter = normalize_block_status_filter(status);

        let order_clause = match sort {
            "height_asc" => "height ASC",
            "height_desc" => "height DESC",
            "reward_desc" => "reward DESC, height DESC",
            "reward_asc" => "reward ASC, height DESC",
            "time_desc" => "timestamp DESC, height DESC",
            "time_asc" => "timestamp ASC, height ASC",
            _ => "height DESC",
        };

        let mut conn = self.conn().lock();
        let total_row = conn.query_one(
            "SELECT COUNT(*)
             FROM blocks
             WHERE ($1::text IS NULL OR LOWER(finder) LIKE $1)
               AND (
                   $2::text IS NULL
                   OR ($2 = 'confirmed' AND confirmed = TRUE AND orphaned = FALSE)
                   OR ($2 = 'orphaned' AND orphaned = TRUE)
                   OR ($2 = 'pending' AND confirmed = FALSE AND orphaned = FALSE)
                   OR ($2 = 'paid' AND paid_out = TRUE)
                   OR ($2 = 'unpaid' AND paid_out = FALSE)
               )",
            &[&finder_pattern, &status_filter],
        )?;
        let total: i64 = total_row.get(0);

        let sql = format!(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE ($1::text IS NULL OR LOWER(finder) LIKE $1)
               AND (
                   $2::text IS NULL
                   OR ($2 = 'confirmed' AND confirmed = TRUE AND orphaned = FALSE)
                   OR ($2 = 'orphaned' AND orphaned = TRUE)
                   OR ($2 = 'pending' AND confirmed = FALSE AND orphaned = FALSE)
                   OR ($2 = 'paid' AND paid_out = TRUE)
                   OR ($2 = 'unpaid' AND paid_out = FALSE)
               )
             ORDER BY {order_clause}
             LIMIT $3 OFFSET $4"
        );
        let rows = conn.query(
            &sql,
            &[
                &finder_pattern,
                &status_filter,
                &limit.max(0),
                &offset.max(0),
            ],
        )?;
        let items = rows.into_iter().map(|row| row_to_block(&row)).collect();

        Ok((items, total.max(0) as u64))
    }

    pub fn get_blocks_page_up_to(
        &self,
        max_height: u64,
        finder: Option<&str>,
        status: Option<&str>,
        sort: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<DbBlock>, u64)> {
        let finder_pattern = finder
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| format!("%{}%", v.to_ascii_lowercase()));
        let status_filter = normalize_block_status_filter(status);
        let max_height = u64_to_i64(max_height)?;

        let order_clause = match sort {
            "height_asc" => "height ASC",
            "height_desc" => "height DESC",
            "reward_desc" => "reward DESC, height DESC",
            "reward_asc" => "reward ASC, height DESC",
            "time_desc" => "timestamp DESC, height DESC",
            "time_asc" => "timestamp ASC, height ASC",
            _ => "height DESC",
        };

        let mut conn = self.conn().lock();
        let total_row = conn.query_one(
            "SELECT COUNT(*)
             FROM blocks
             WHERE height <= $1
               AND ($2::text IS NULL OR LOWER(finder) LIKE $2)
               AND (
                   $3::text IS NULL
                   OR ($3 = 'confirmed' AND confirmed = TRUE AND orphaned = FALSE)
                   OR ($3 = 'orphaned' AND orphaned = TRUE)
                   OR ($3 = 'pending' AND confirmed = FALSE AND orphaned = FALSE)
                   OR ($3 = 'paid' AND paid_out = TRUE)
                   OR ($3 = 'unpaid' AND paid_out = FALSE)
               )",
            &[&max_height, &finder_pattern, &status_filter],
        )?;
        let total: i64 = total_row.get(0);

        let sql = format!(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE height <= $1
               AND ($2::text IS NULL OR LOWER(finder) LIKE $2)
               AND (
                   $3::text IS NULL
                   OR ($3 = 'confirmed' AND confirmed = TRUE AND orphaned = FALSE)
                   OR ($3 = 'orphaned' AND orphaned = TRUE)
                   OR ($3 = 'pending' AND confirmed = FALSE AND orphaned = FALSE)
                   OR ($3 = 'paid' AND paid_out = TRUE)
                   OR ($3 = 'unpaid' AND paid_out = FALSE)
               )
             ORDER BY {order_clause}
             LIMIT $4 OFFSET $5"
        );
        let rows = conn.query(
            &sql,
            &[
                &max_height,
                &finder_pattern,
                &status_filter,
                &limit.max(0),
                &offset.max(0),
            ],
        )?;
        let items = rows.into_iter().map(|row| row_to_block(&row)).collect();

        Ok((items, total.max(0) as u64))
    }

    pub fn get_unconfirmed_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE confirmed = FALSE AND orphaned = FALSE ORDER BY height ASC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_unpaid_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE confirmed = TRUE AND orphaned = FALSE AND paid_out = FALSE ORDER BY height ASC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_block_count(&self) -> Result<u64> {
        let row = self
            .conn()
            .lock()
            .query_one("SELECT COUNT(*) FROM blocks", &[])?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn get_block_count_up_to(&self, max_height: u64) -> Result<u64> {
        let row = self.conn().lock().query_one(
            "SELECT COUNT(*) FROM blocks WHERE height <= $1",
            &[&u64_to_i64(max_height)?],
        )?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn get_archived_block_count(&self) -> Result<u64> {
        let row = self
            .conn()
            .lock()
            .query_one("SELECT COUNT(*) FROM archived_blocks", &[])?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn get_archived_orphaned_block_count(&self) -> Result<u64> {
        let row = self.conn().lock().query_one(
            "SELECT COUNT(*) FROM archived_blocks WHERE orphaned = TRUE",
            &[],
        )?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn get_unique_block_identity_counts(&self) -> Result<(u64, u64)> {
        let row = self.conn().lock().query_one(
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
        let row = self.conn().lock().query_one(
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

    /// Returns (confirmed_non_orphaned, orphaned, pending_non_orphaned) up to max_height.
    pub fn get_block_status_counts_up_to(&self, max_height: u64) -> Result<(u64, u64, u64)> {
        let row = self.conn().lock().query_one(
            "SELECT
                 COALESCE(SUM(CASE WHEN confirmed = TRUE AND orphaned = FALSE THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN orphaned = TRUE THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN confirmed = FALSE AND orphaned = FALSE THEN 1 ELSE 0 END), 0)
             FROM blocks
             WHERE height <= $1",
            &[&u64_to_i64(max_height)?],
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
        let row = self.conn().lock().query_opt(
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

    pub fn update_balance(&self, bal: &Balance) -> Result<()> {
        self.conn().lock().execute(
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

    fn load_balance(tx: &mut Transaction<'_>, destination: &str) -> Result<Balance> {
        let row = tx.query_opt(
            "SELECT address, pending, paid FROM balances WHERE address = $1",
            &[&destination],
        )?;
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
        let existing = Self::load_manual_payout_offset(tx, address)?;
        let updated = existing
            .checked_add(amount)
            .ok_or_else(|| anyhow!("manual payout offset overflow for {address}"))?;
        Self::upsert_manual_payout_offset(tx, address, updated)
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

    pub fn apply_block_credits_and_mark_paid(
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
        let mut conn = self.conn().lock();
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

    pub fn backfill_uncredited_pool_fee_balance_credits(
        &self,
        expected_fee_address: Option<&str>,
    ) -> Result<PoolFeeCreditBackfillReport> {
        let expected_fee_address = expected_fee_address
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let rows = self.conn().lock().query(
            "SELECT e.block_height, e.amount, e.fee_address, e.timestamp, COALESCE(b.finder, '')
             FROM pool_fee_events e
             LEFT JOIN pool_fee_balance_credits c ON c.block_height = e.block_height
             LEFT JOIN blocks b ON b.height = e.block_height
             WHERE c.block_height IS NULL
             ORDER BY e.block_height ASC",
            &[],
        )?;

        let mut report = PoolFeeCreditBackfillReport::default();
        for row in rows {
            let block_height = row.get::<_, i64>(0).max(0) as u64;
            let amount = row.get::<_, i64>(1).max(0) as u64;
            let fee_address = row.get::<_, String>(2);
            let timestamp = from_unix(row.get::<_, i64>(3));
            let finder = row.get::<_, String>(4);
            let recorded_fee_address = fee_address.trim();
            let finder = finder.trim();
            let mut credit_destination = recorded_fee_address.to_string();
            let mut reconciled_legacy_finder_fallback = false;

            if expected_fee_address
                .as_deref()
                .is_some_and(|expected| recorded_fee_address != expected)
            {
                if !finder.is_empty() && recorded_fee_address == finder {
                    credit_destination = expected_fee_address
                        .as_deref()
                        .expect("checked Some above")
                        .to_string();
                    reconciled_legacy_finder_fallback = true;
                } else {
                    report.skipped_mismatched_destination =
                        report.skipped_mismatched_destination.saturating_add(1);
                    continue;
                }
            }

            let mut conn = self.conn().lock();
            let mut tx = conn.transaction()?;
            if reconciled_legacy_finder_fallback {
                tx.execute(
                    "UPDATE pool_fee_events
                     SET fee_address = $2
                     WHERE block_height = $1 AND fee_address = $3",
                    &[
                        &u64_to_i64(block_height)?,
                        &credit_destination,
                        &recorded_fee_address,
                    ],
                )?;
            }
            if Self::apply_pool_fee_balance_credit(
                &mut tx,
                block_height,
                &credit_destination,
                amount,
                timestamp,
                false,
            )? {
                report.credited_events = report.credited_events.saturating_add(1);
                report.credited_amount = report.credited_amount.saturating_add(amount);
                if reconciled_legacy_finder_fallback {
                    report.reconciled_finder_fallback_events =
                        report.reconciled_finder_fallback_events.saturating_add(1);
                    report.reconciled_finder_fallback_amount = report
                        .reconciled_finder_fallback_amount
                        .saturating_add(amount);
                }
            }
            tx.commit()?;
        }

        Ok(report)
    }

    pub fn list_confirmed_paid_blocks_missing_miner_credits(
        &self,
    ) -> Result<Vec<HistoricalMissingMinerCreditsBlock>> {
        let rows = self.conn().lock().query(
            "SELECT
                 b.height,
                 b.hash,
                 b.difficulty,
                 b.finder,
                 b.finder_worker,
                 b.reward,
                 b.timestamp,
                 b.confirmed,
                 b.orphaned,
                 b.paid_out,
                 b.effort_pct,
                 COALESCE(c.amount, e.amount, 0)::BIGINT AS recorded_fee_amount
             FROM blocks b
             LEFT JOIN pool_fee_balance_credits c ON c.block_height = b.height
             LEFT JOIN pool_fee_events e ON e.block_height = b.height
             WHERE b.confirmed = TRUE
               AND b.orphaned = FALSE
               AND b.paid_out = TRUE
               AND NOT EXISTS (
                   SELECT 1
                   FROM block_credit_events credits
                   WHERE credits.block_height = b.height
               )
             ORDER BY b.height ASC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| HistoricalMissingMinerCreditsBlock {
                block: row_to_block(&row),
                recorded_fee_amount: row.get::<_, i64>(11).max(0) as u64,
            })
            .collect())
    }

    pub fn backfill_missing_block_credit_events(
        &self,
        block_height: u64,
        credits: &[(String, u64)],
    ) -> Result<bool> {
        let mut conn = self.conn().lock();
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
        if !confirmed || orphaned {
            return Err(anyhow!(
                "block {block_height} is not eligible for historical credit backfill"
            ));
        }
        if !paid_out {
            return Ok(false);
        }
        let existing = tx.query_one(
            "SELECT COUNT(*)::BIGINT FROM block_credit_events WHERE block_height = $1",
            &[&u64_to_i64(block_height)?],
        )?;
        if existing.get::<_, i64>(0) > 0 {
            return Ok(false);
        }

        for (address, amount) in credits {
            let destination = address.trim();
            if destination.is_empty() || *amount == 0 {
                continue;
            }

            tx.execute(
                "INSERT INTO block_credit_events (block_height, address, amount, paid_amount, reversible)
                 VALUES ($1, $2, $3, 0, TRUE)",
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

        tx.commit()?;
        Ok(true)
    }

    pub fn get_all_balances(&self) -> Result<Vec<Balance>> {
        let rows = self.conn().lock().query(
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
        let rows = self.conn().lock().query(
            "WITH miner_sources AS (
                 SELECT
                     e.address AS address,
                     SUM(CASE WHEN b.orphaned THEN GREATEST(e.amount - e.paid_amount, 0) ELSE 0 END)::BIGINT AS orphan_pending,
                     SUM(CASE WHEN NOT b.orphaned THEN GREATEST(e.amount - e.paid_amount, 0) ELSE 0 END)::BIGINT AS canonical_pending
                 FROM block_credit_events e
                 JOIN blocks b ON b.height = e.block_height
                 GROUP BY e.address
             ),
             fee_sources AS (
                 SELECT
                     f.fee_address AS address,
                     SUM(CASE WHEN b.orphaned THEN GREATEST(f.amount - f.paid_amount, 0) ELSE 0 END)::BIGINT AS orphan_pending,
                     SUM(CASE WHEN NOT b.orphaned THEN GREATEST(f.amount - f.paid_amount, 0) ELSE 0 END)::BIGINT AS canonical_pending
                 FROM pool_fee_balance_credits f
                 JOIN blocks b ON b.height = f.block_height
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
             ORDER BY address ASC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| BalanceSourceSummary {
                address: row.get::<_, String>(0),
                canonical_pending: row.get::<_, i64>(1).max(0) as u64,
                orphan_pending: row.get::<_, i64>(2).max(0) as u64,
            })
            .collect())
    }

    pub fn live_reconciliation_blockers(&self) -> Result<LiveReconciliationBlockers> {
        let row = self.conn().lock().query_one(
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

    pub fn add_payout(&self, address: &str, amount: u64, fee: u64, tx_hash: &str) -> Result<()> {
        self.add_payout_with_batch(address, amount, fee, tx_hash, None)
    }

    pub fn add_payout_with_batch(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
        batch_id: Option<&str>,
    ) -> Result<()> {
        self.conn().lock().execute(
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
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
             FROM payouts
             WHERE reverted_at IS NULL
             ORDER BY id DESC
             LIMIT $1",
            &[&limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_recent_payouts_for_address(&self, address: &str, limit: i64) -> Result<Vec<Payout>> {
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
             FROM payouts
             WHERE address = $1
               AND reverted_at IS NULL
             ORDER BY id DESC
             LIMIT $2",
            &[&address, &limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_active_payouts(&self) -> Result<Vec<Payout>> {
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
             FROM payouts
             WHERE reverted_at IS NULL
             ORDER BY timestamp DESC, id DESC",
            &[],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_active_payout_tx_hash_batch(
        &self,
        after_payout_id: i64,
        limit: i64,
    ) -> Result<Vec<(i64, String)>> {
        let rows = self.conn().lock().query(
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
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, confirmed, batch_id
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
                     tx_hash,
                     COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                     0 AS confirmed,
                     batch_id
                 FROM pending_payouts
                 WHERE address = $1
                   AND tx_hash IS NOT NULL
                   AND BTRIM(tx_hash) <> ''
             ) visible
             ORDER BY timestamp DESC, confirmed ASC, id DESC
             LIMIT $2",
            &[&address, &limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_payouts_page(
        &self,
        address: Option<&str>,
        tx_hash: Option<&str>,
        sort: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<Payout>, u64)> {
        let address_pattern = address
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| format!("%{}%", v.to_ascii_lowercase()));
        let tx_hash_pattern = tx_hash
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| format!("%{}%", v.to_ascii_lowercase()));

        let order_clause = match sort {
            "time_asc" => "timestamp ASC, confirmed ASC, id ASC",
            "amount_desc" => "amount DESC, confirmed ASC, id DESC",
            "amount_asc" => "amount ASC, confirmed ASC, id DESC",
            _ => "timestamp DESC, confirmed ASC, id DESC",
        };

        let mut conn = self.conn().lock();
        let total_row = conn.query_one(
            "SELECT COUNT(*)
             FROM (
                 SELECT address, tx_hash
                 FROM payouts
                 WHERE reverted_at IS NULL
                 UNION ALL
                 SELECT address, tx_hash
                 FROM pending_payouts
                 WHERE tx_hash IS NOT NULL
                   AND BTRIM(tx_hash) <> ''
             ) visible
             WHERE ($1::text IS NULL OR LOWER(address) LIKE $1)
               AND ($2::text IS NULL OR LOWER(tx_hash) LIKE $2)",
            &[&address_pattern, &tx_hash_pattern],
        )?;
        let total: i64 = total_row.get(0);

        let sql = format!(
            "SELECT id, address, amount, fee, tx_hash, timestamp, confirmed, batch_id
             FROM (
                 SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed, batch_id
                 FROM payouts
                 WHERE reverted_at IS NULL
                 UNION ALL
                 SELECT
                     0 AS id,
                     address,
                     amount,
                     COALESCE(fee, 0)::bigint AS fee,
                     tx_hash,
                     COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                     0 AS confirmed,
                     batch_id
                 FROM pending_payouts
                 WHERE tx_hash IS NOT NULL
                   AND BTRIM(tx_hash) <> ''
             ) visible
             WHERE ($1::text IS NULL OR LOWER(address) LIKE $1)
               AND ($2::text IS NULL OR LOWER(tx_hash) LIKE $2)
             ORDER BY {order_clause}
             LIMIT $3 OFFSET $4"
        );
        let rows = conn.query(
            &sql,
            &[
                &address_pattern,
                &tx_hash_pattern,
                &limit.max(0),
                &offset.max(0),
            ],
        )?;
        let items = rows
            .into_iter()
            .map(row_to_payout)
            .collect::<Result<Vec<_>>>()?;

        Ok((items, total.max(0) as u64))
    }

    pub fn get_public_payout_batches_page(
        &self,
        sort: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<PublicPayoutBatch>, u64)> {
        let order_clause = match sort {
            "time_asc" => "batch_ts ASC",
            "amount_desc" => "total_amount DESC, batch_ts DESC",
            "amount_asc" => "total_amount ASC, batch_ts DESC",
            _ => "batch_ts DESC",
        };

        let mut conn = self.conn().lock();
        let total_row = conn.query_one(
            "SELECT COUNT(*) FROM (
                SELECT batch_key
                FROM (
                    SELECT
                        COALESCE(NULLIF(batch_id, ''), CONCAT('legacy:', (timestamp / 300)::text)) AS batch_key
                    FROM payouts
                    WHERE reverted_at IS NULL
                    UNION ALL
                    SELECT
                        COALESCE(
                            NULLIF(batch_id, ''),
                            CONCAT('legacy:', (COALESCE(sent_at, send_started_at, initiated_at) / 300)::text)
                        ) AS batch_key
                    FROM pending_payouts
                    WHERE tx_hash IS NOT NULL
                      AND BTRIM(tx_hash) <> ''
                ) visible
                GROUP BY batch_key
            ) grouped",
            &[],
        )?;
        let total: i64 = total_row.get(0);

        let sql = format!(
            "WITH visible AS (
                SELECT
                    amount,
                    fee,
                    tx_hash,
                    timestamp,
                    1 AS confirmed,
                    COALESCE(NULLIF(batch_id, ''), CONCAT('legacy:', (timestamp / 300)::text)) AS batch_key
                FROM payouts
                WHERE reverted_at IS NULL
                UNION ALL
                SELECT
                    amount,
                    COALESCE(fee, 0)::bigint AS fee,
                    tx_hash,
                    COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                    0 AS confirmed,
                    COALESCE(
                        NULLIF(batch_id, ''),
                        CONCAT('legacy:', (COALESCE(sent_at, send_started_at, initiated_at) / 300)::text)
                    ) AS batch_key
                FROM pending_payouts
                WHERE tx_hash IS NOT NULL
                  AND BTRIM(tx_hash) <> ''
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
             SELECT total_amount, total_fee, recipient_count, tx_hashes, batch_ts, confirmed
             FROM grouped
             ORDER BY {order_clause}
             LIMIT $1 OFFSET $2"
        );
        let rows = conn.query(&sql, &[&limit.max(0), &offset.max(0)])?;
        let items = rows
            .into_iter()
            .map(row_to_public_payout_batch)
            .collect::<Result<Vec<_>>>()?;

        Ok((items, total.max(0) as u64))
    }

    pub fn record_pool_fee(
        &self,
        block_height: u64,
        amount: u64,
        fee_address: &str,
        timestamp: SystemTime,
    ) -> Result<bool> {
        if amount == 0 {
            return Ok(false);
        }

        let destination = fee_address.trim();
        if destination.is_empty() {
            return Err(anyhow!("fee address is required"));
        }

        let inserted = self.conn().lock().execute(
            "INSERT INTO pool_fee_events (block_height, amount, fee_address, timestamp)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(block_height) DO NOTHING",
            &[
                &u64_to_i64(block_height)?,
                &u64_to_i64(amount)?,
                &destination,
                &to_unix(timestamp),
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn get_total_pool_fees(&self) -> Result<u64> {
        let row = self.conn().lock().query_one(
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
        let row = self.conn().lock().query_one(
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
            .lock()
            .query_one("SELECT COALESCE(SUM(paid)::BIGINT, 0) FROM balances", &[])?;
        let total: i64 = row.get(0);
        Ok(total.max(0) as u64)
    }

    pub fn find_existing_payout_tx_hashes(&self, tx_hashes: &[String]) -> Result<HashSet<String>> {
        if tx_hashes.is_empty() {
            return Ok(HashSet::new());
        }

        let rows = self.conn().lock().query(
            "SELECT DISTINCT tx_hash
             FROM payouts
             WHERE tx_hash = ANY($1)
               AND reverted_at IS NULL",
            &[&tx_hashes],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect())
    }

    pub fn get_recent_pool_fees(&self, limit: i64) -> Result<Vec<PoolFeeEvent>> {
        let rows = self.conn().lock().query(
            "SELECT e.id, e.block_height, e.amount, e.fee_address, e.timestamp
             FROM pool_fee_events e
             LEFT JOIN blocks b ON b.height = e.block_height
             WHERE b.height IS NULL OR b.orphaned = FALSE
             ORDER BY e.id DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| PoolFeeEvent {
                id: row.get::<_, i64>(0),
                block_height: row.get::<_, i64>(1).max(0) as u64,
                amount: row.get::<_, i64>(2).max(0) as u64,
                fee_address: row.get::<_, String>(3),
                timestamp: from_unix(row.get::<_, i64>(4)),
            })
            .collect())
    }

    pub fn get_block_pool_fee_event(&self, block_height: u64) -> Result<Option<PoolFeeEvent>> {
        let row = self.conn().lock().query_opt(
            "SELECT e.id, e.block_height, e.amount, e.fee_address, e.timestamp
             FROM pool_fee_events e
             LEFT JOIN blocks b ON b.height = e.block_height
             WHERE e.block_height = $1
               AND (b.height IS NULL OR b.orphaned = FALSE)",
            &[&u64_to_i64(block_height)?],
        )?;
        Ok(row.map(|row| PoolFeeEvent {
            id: row.get::<_, i64>(0),
            block_height: row.get::<_, i64>(1).max(0) as u64,
            amount: row.get::<_, i64>(2).max(0) as u64,
            fee_address: row.get::<_, String>(3),
            timestamp: from_unix(row.get::<_, i64>(4)),
        }))
    }

    pub fn get_all_pool_fees(&self) -> Result<Vec<PoolFeeEvent>> {
        let rows = self.conn().lock().query(
            "SELECT e.id, e.block_height, e.amount, e.fee_address, e.timestamp
             FROM pool_fee_events e
             LEFT JOIN blocks b ON b.height = e.block_height
             WHERE b.height IS NULL OR b.orphaned = FALSE
             ORDER BY e.id DESC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| PoolFeeEvent {
                id: row.get::<_, i64>(0),
                block_height: row.get::<_, i64>(1).max(0) as u64,
                amount: row.get::<_, i64>(2).max(0) as u64,
                fee_address: row.get::<_, String>(3),
                timestamp: from_unix(row.get::<_, i64>(4)),
            })
            .collect())
    }

    pub fn get_block_credit_events(&self, block_height: u64) -> Result<Vec<BlockCreditEvent>> {
        let rows = self.conn().lock().query(
            "SELECT id, block_height, address, amount, paid_amount, reversible
             FROM block_credit_events
             WHERE block_height = $1
             ORDER BY amount DESC, address ASC",
            &[&u64_to_i64(block_height)?],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| BlockCreditEvent {
                id: row.get::<_, i64>(0),
                block_height: row.get::<_, i64>(1).max(0) as u64,
                address: row.get::<_, String>(2),
                amount: row.get::<_, i64>(3).max(0) as u64,
                paid_amount: row.get::<_, i64>(4).max(0) as u64,
                reversible: row.get::<_, bool>(5),
            })
            .collect())
    }

    pub fn get_latest_non_orphan_block_up_to(&self, max_height: u64) -> Result<Option<DbBlock>> {
        let row = self.conn().lock().query_opt(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE orphaned = FALSE AND height <= $1
             ORDER BY height DESC
             LIMIT 1",
            &[&u64_to_i64(max_height)?],
        )?;
        Ok(row.map(|row| row_to_block(&row)))
    }

    pub fn get_non_orphan_blocks_up_to(&self, max_height: u64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE orphaned = FALSE AND height <= $1
             ORDER BY height DESC",
            &[&u64_to_i64(max_height)?],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn get_non_orphan_blocks_from_height(&self, min_height: u64) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks
             WHERE orphaned = FALSE AND height >= $1
             ORDER BY height ASC",
            &[&u64_to_i64(min_height)?],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn list_orphaned_block_credit_issues(&self) -> Result<Vec<OrphanedBlockCreditIssue>> {
        let rows = self.conn().lock().query(
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
        let rows = self.conn().lock().query(
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

    pub fn orphan_block_and_reverse_unpaid_credits(
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

    pub fn reconcile_all_existing_orphaned_block_credits(
        &self,
    ) -> Result<ExistingOrphanBlockReconciliationReport> {
        let rows = self.conn().lock().query(
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
        let mut conn = self.conn().lock();
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
                    )? as u64);
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
                "SELECT
                     COALESCE(SUM(CASE WHEN orphaned THEN 0 ELSE pending_amount END), 0)::BIGINT,
                     COALESCE(SUM(CASE WHEN orphaned THEN pending_amount ELSE 0 END), 0)::BIGINT
                 FROM (
                     SELECT
                         b.orphaned,
                         GREATEST(e.amount - e.paid_amount, 0)::BIGINT AS pending_amount
                     FROM block_credit_events e
                     JOIN blocks b ON b.height = e.block_height
                     WHERE e.address = $1
                       AND e.block_height <> $2
                     UNION ALL
                     SELECT
                         b.orphaned,
                         GREATEST(f.amount - f.paid_amount, 0)::BIGINT AS pending_amount
                     FROM pool_fee_balance_credits f
                     JOIN blocks b ON b.height = f.block_height
                     WHERE f.fee_address = $1
                       AND f.block_height <> $2
                 ) sources",
                &[&address, &excluded_height],
            )?
        } else {
            tx.query_one(
                "SELECT
                     COALESCE(SUM(CASE WHEN orphaned THEN 0 ELSE pending_amount END), 0)::BIGINT,
                     COALESCE(SUM(CASE WHEN orphaned THEN pending_amount ELSE 0 END), 0)::BIGINT
                 FROM (
                     SELECT
                         b.orphaned,
                         GREATEST(e.amount - e.paid_amount, 0)::BIGINT AS pending_amount
                     FROM block_credit_events e
                     JOIN blocks b ON b.height = e.block_height
                     WHERE e.address = $1
                     UNION ALL
                     SELECT
                         b.orphaned,
                         GREATEST(f.amount - f.paid_amount, 0)::BIGINT AS pending_amount
                     FROM pool_fee_balance_credits f
                     JOIN blocks b ON b.height = f.block_height
                     WHERE f.fee_address = $1
                 ) sources",
                &[&address],
            )?
        };
        Ok(LivePendingSourceSummary {
            canonical_pending: row.get::<_, i64>(0).max(0) as u64,
            _orphan_pending: row.get::<_, i64>(1).max(0) as u64,
        })
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO meta (key, value) VALUES ($1, $2)
             ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
            &[&key, &value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row = self
            .conn()
            .lock()
            .query_opt("SELECT value FROM meta WHERE key = $1", &[&key])?;
        Ok(row.map(|v| v.get::<_, Vec<u8>>(0)))
    }

    pub fn upsert_monitor_heartbeat(&self, heartbeat: &MonitorHeartbeatUpsert) -> Result<()> {
        let mut conn = self.conn().lock();
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

    pub fn get_monitor_heartbeats_since(
        &self,
        since: SystemTime,
        source: Option<&str>,
    ) -> Result<Vec<MonitorHeartbeat>> {
        let rows = self.conn().lock().query(
            "SELECT id, sampled_at, source, synthetic, api_up, stratum_up, db_up, daemon_up,
                    public_http_up, daemon_syncing, chain_height, template_age_seconds,
                    last_refresh_millis, stratum_snapshot_age_seconds, connected_miners,
                    connected_workers, estimated_hashrate, wallet_up,
                    last_accepted_share_at, last_accepted_share_age_seconds,
                    payout_pending_count, payout_pending_amount,
                    oldest_pending_payout_at, oldest_pending_payout_age_seconds,
                    oldest_pending_send_started_at, oldest_pending_send_age_seconds,
                    validation_candidate_queue_depth, validation_regular_queue_depth,
                    summary_state, details_json
             FROM monitor_heartbeats
             WHERE sampled_at >= $1
               AND ($2::text IS NULL OR source = $2)
             ORDER BY sampled_at ASC, id ASC",
            &[&to_unix(since), &source],
        )?;
        rows.into_iter().map(row_to_monitor_heartbeat).collect()
    }

    pub fn get_latest_monitor_heartbeat(
        &self,
        source: Option<&str>,
    ) -> Result<Option<MonitorHeartbeat>> {
        let row = self.conn().lock().query_opt(
            "SELECT id, sampled_at, source, synthetic, api_up, stratum_up, db_up, daemon_up,
                    public_http_up, daemon_syncing, chain_height, template_age_seconds,
                    last_refresh_millis, stratum_snapshot_age_seconds, connected_miners,
                    connected_workers, estimated_hashrate, wallet_up,
                    last_accepted_share_at, last_accepted_share_age_seconds,
                    payout_pending_count, payout_pending_amount,
                    oldest_pending_payout_at, oldest_pending_payout_age_seconds,
                    oldest_pending_send_started_at, oldest_pending_send_age_seconds,
                    validation_candidate_queue_depth, validation_regular_queue_depth,
                    summary_state, details_json
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
        let row = self.conn().lock().query_one(
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
        self.conn().lock().execute(
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
        let updated = self.conn().lock().execute(
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
        let rows = self.conn().lock().query(
            "SELECT id, dedupe_key, kind, severity, visibility, source, summary, detail,
                    started_at, updated_at, ended_at
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
        let rows = self.conn().lock().query(
            "SELECT id, dedupe_key, kind, severity, visibility, source, summary, detail,
                    started_at, updated_at, ended_at
             FROM monitor_incidents
             WHERE ($1::text IS NULL OR visibility = $1)
             ORDER BY COALESCE(ended_at, updated_at) DESC, id DESC
             LIMIT $2",
            &[&visibility, &limit.max(0)],
        )?;
        rows.into_iter().map(row_to_monitor_incident).collect()
    }

    pub fn delete_monitor_heartbeats_before(&self, before: SystemTime) -> Result<u64> {
        let deleted = self.conn().lock().execute(
            "DELETE FROM monitor_heartbeats
             WHERE sampled_at < $1",
            &[&to_unix(before)],
        )?;
        Ok(deleted)
    }

    pub fn delete_resolved_monitor_incidents_before(&self, before: SystemTime) -> Result<u64> {
        let deleted = self.conn().lock().execute(
            "DELETE FROM monitor_incidents
             WHERE ended_at IS NOT NULL
               AND ended_at < $1",
            &[&to_unix(before)],
        )?;
        Ok(deleted)
    }

    pub fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        let expires_at = now_unix() + SHARE_CLAIM_EXPIRY_SECS;
        self.conn().lock().execute(
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = EXCLUDED.expires_at",
            &[&job_id, &u64_to_i64(nonce)?, &expires_at],
        )?;
        Ok(())
    }

    pub fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        if self.has_persisted_share(job_id, nonce)? {
            return Ok(false);
        }
        let now = now_unix();
        let expires_at = now + SHARE_CLAIM_EXPIRY_SECS;
        let claimed = self.conn().lock().execute(
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES ($1, $2, $3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = EXCLUDED.expires_at
             WHERE seen_shares.expires_at <= $4",
            &[&job_id, &u64_to_i64(nonce)?, &expires_at, &now],
        )?;
        Ok(claimed > 0)
    }

    pub fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.conn().lock().execute(
            "DELETE FROM seen_shares WHERE job_id = $1 AND nonce = $2",
            &[&job_id, &u64_to_i64(nonce)?],
        )?;
        Ok(())
    }

    pub fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        if self.has_persisted_share(job_id, nonce)? {
            return Ok(true);
        }
        let row = self.conn().lock().query_opt(
            "SELECT expires_at FROM seen_shares WHERE job_id = $1 AND nonce = $2",
            &[&job_id, &u64_to_i64(nonce)?],
        )?;
        if let Some(row) = row {
            let expiry: i64 = row.get(0);
            return Ok(expiry > now_unix());
        }
        Ok(false)
    }

    fn has_persisted_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        let row = self.conn().lock().query_opt(
            "SELECT 1 FROM shares WHERE job_id = $1 AND nonce = $2 LIMIT 1",
            &[&job_id, &u64_to_i64(nonce)?],
        )?;
        Ok(row.is_some())
    }

    pub fn clean_expired_seen_shares(&self) -> Result<u64> {
        let affected = self.conn().lock().execute(
            "DELETE FROM seen_shares WHERE expires_at <= $1",
            &[&now_unix()],
        )?;
        Ok(affected as u64)
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        let row = self.conn().lock().query_opt(
            "SELECT address, strikes, strike_window_until, last_reason, last_event_at,
                    quarantined_until, force_verify_until, suspected_fraud_strikes,
                    suspected_fraud_window_until
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
            suspected_fraud_strikes: row.get::<_, i64>(7).max(0) as u64,
            suspected_fraud_window_until: row.get::<_, Option<i64>>(8).map(from_unix),
        }))
    }

    pub fn is_address_quarantined(
        &self,
        address: &str,
    ) -> Result<(bool, Option<AddressRiskState>)> {
        let state = self.get_address_risk(address)?;
        let now = SystemTime::now();
        let quarantined = state
            .as_ref()
            .and_then(|s| s.quarantined_until)
            .is_some_and(|until| until > now);
        Ok((quarantined, state))
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

        let rows = self.conn().lock().query(
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
        let row = self.conn().lock().query_opt(
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
        Ok(row.map(|row| ValidationHoldState {
            forced_started_at: row.get::<_, Option<i64>>(0).map(from_unix),
            forced_until: row
                .get::<_, Option<i64>>(1)
                .map(from_unix)
                .filter(|until| *until > now),
            hold_cause: validation_hold_cause_from_db(row.get::<_, Option<String>>(2).as_deref())
                .or_else(|| {
                    infer_validation_hold_cause_from_row(
                        row.get::<_, Option<i64>>(0).map(from_unix),
                        row.get::<_, Option<i64>>(1).map(from_unix),
                    )
                }),
            pending_provisional: row.get::<_, i64>(3).max(0) as u64,
            recent_verified_difficulty: row.get::<_, i64>(4).max(0) as u64,
            recent_provisional_difficulty: row.get::<_, i64>(5).max(0) as u64,
        }))
    }

    fn validation_forced_until_after(
        &self,
        address: &str,
        now: SystemTime,
    ) -> Result<Option<SystemTime>> {
        let row = self.conn().lock().query_opt(
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
        let mut conn = self.conn().lock();
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
        let rows = self.conn().lock().query(
            "SELECT COALESCE(r.address, v.address) AS address,
                    r.strikes,
                    r.suspected_fraud_strikes,
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
                 SELECT address, strikes, suspected_fraud_strikes, last_reason, last_event_at,
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
        Ok(rows
            .iter()
            .map(|row| {
                let last_reason = row.get::<_, Option<String>>(3);
                let validation_hold_cause =
                    validation_hold_cause_from_db(row.get::<_, Option<String>>(9).as_deref())
                        .or_else(|| {
                            infer_validation_hold_cause_from_row(
                                row.get::<_, Option<i64>>(8).map(from_unix),
                                row.get::<_, Option<i64>>(7).map(from_unix),
                            )
                        });
                let validation_pending_provisional = row.get::<_, i64>(10).max(0) as u64;
                let validation_recent_verified_difficulty = row.get::<_, i64>(11).max(0) as u64;
                let validation_recent_provisional_difficulty = row.get::<_, i64>(12).max(0) as u64;
                let reason = last_reason.clone().or_else(|| {
                    validation_hold_reason(
                        validation_hold_cause,
                        validation_pending_provisional,
                        validation_recent_verified_difficulty,
                        validation_recent_provisional_difficulty,
                    )
                });
                ActiveVerificationHold {
                    address: row.get::<_, String>(0),
                    strikes: row.get::<_, Option<i64>>(1).unwrap_or_default().max(0) as u64,
                    suspected_fraud_strikes: row.get::<_, Option<i64>>(2).unwrap_or_default().max(0)
                        as u64,
                    last_reason,
                    reason,
                    last_event_at: row.get::<_, Option<i64>>(4).map(from_unix),
                    quarantined_until: row
                        .get::<_, Option<i64>>(5)
                        .map(from_unix)
                        .filter(|until| *until > now),
                    force_verify_until: row
                        .get::<_, Option<i64>>(6)
                        .map(from_unix)
                        .filter(|until| *until > now),
                    validation_forced_until: row
                        .get::<_, Option<i64>>(7)
                        .map(from_unix)
                        .filter(|until| *until > now),
                    validation_hold_cause,
                    validation_pending_provisional,
                    validation_recent_verified_difficulty,
                    validation_recent_provisional_difficulty,
                }
            })
            .collect())
    }

    pub fn escalate_address_risk(
        &self,
        address: &str,
        reason: &str,
        strike_window_duration: Duration,
        quarantine_threshold: u64,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
    ) -> Result<AddressRiskState> {
        if address.trim().is_empty() {
            return Err(anyhow!("address is required"));
        }

        let now = SystemTime::now();
        let mut conn = self.conn().lock();
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
            suspected_fraud_strikes: self
                .get_address_risk(address)?
                .map(|state| state.suspected_fraud_strikes)
                .unwrap_or_default(),
            suspected_fraud_window_until: self
                .get_address_risk(address)?
                .and_then(|state| state.suspected_fraud_window_until),
        })
    }

    pub fn record_suspected_fraud(
        &self,
        address: &str,
        reason: &str,
        quarantine_threshold: u64,
        strike_window_duration: Duration,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
    ) -> Result<AddressRiskState> {
        if address.trim().is_empty() {
            return Err(anyhow!("address is required"));
        }

        let now = SystemTime::now();
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO address_risk (
                 address, strikes, strike_window_until, last_reason, last_event_at, quarantined_until, force_verify_until,
                 suspected_fraud_strikes, suspected_fraud_window_until
             )
             VALUES ($1, 0, NULL, NULL, NULL, NULL, NULL, 0, NULL)
             ON CONFLICT(address) DO NOTHING",
            &[&address],
        )?;

        let row = tx.query_one(
            "SELECT strikes, quarantined_until, force_verify_until,
                    suspected_fraud_strikes, suspected_fraud_window_until
             FROM address_risk WHERE address = $1 FOR UPDATE",
            &[&address],
        )?;
        let strikes = row.get::<_, i64>(0).max(0) as u64;
        let existing_quarantine = row.get::<_, Option<i64>>(1).map(from_unix);
        let existing_force = row.get::<_, Option<i64>>(2).map(from_unix);
        let existing_fraud_strikes = row.get::<_, i64>(3).max(0) as u64;
        let existing_fraud_window = row.get::<_, Option<i64>>(4).map(from_unix);

        let active_fraud_strikes = existing_fraud_window
            .filter(|until| *until > now)
            .map(|_| existing_fraud_strikes)
            .unwrap_or_default();
        let suspected_fraud_strikes = active_fraud_strikes.saturating_add(1);
        let force_verify_until = merge_optional_later(
            existing_force,
            if force_verify_duration.is_zero() {
                None
            } else {
                Some(now + force_verify_duration)
            },
        );
        let suspected_fraud_window_until = merge_optional_later(
            existing_fraud_window.filter(|until| *until > now),
            if strike_window_duration.is_zero() {
                None
            } else {
                Some(now + strike_window_duration)
            },
        );
        let quarantined_until = if quarantine_threshold > 0
            && suspected_fraud_strikes >= quarantine_threshold
            && !quarantine_base.is_zero()
        {
            let quarantine_strikes = suspected_fraud_strikes
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
             SET last_reason = $2,
                 last_event_at = $3,
                 quarantined_until = $4,
                 force_verify_until = $5,
                 suspected_fraud_strikes = $6,
                 suspected_fraud_window_until = $7
             WHERE address = $1",
            &[
                &address,
                &reason,
                &to_unix(now),
                &quarantined_until.map(to_unix),
                &force_verify_until.map(to_unix),
                &u64_to_i64(suspected_fraud_strikes)?,
                &suspected_fraud_window_until.map(to_unix),
            ],
        )?;
        tx.commit()?;

        Ok(AddressRiskState {
            address: address.to_string(),
            strikes,
            strike_window_until: self
                .get_address_risk(address)?
                .and_then(|state| state.strike_window_until),
            last_reason: Some(reason.to_string()),
            last_event_at: Some(now),
            quarantined_until,
            force_verify_until,
            suspected_fraud_strikes,
            suspected_fraud_window_until,
        })
    }

    pub fn create_pending_payout(&self, address: &str, amount: u64) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO pending_payouts (
                address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id
             ) VALUES ($1, $2, $3, NULL, NULL, NULL, NULL, NULL)
             ON CONFLICT(address) DO UPDATE
             SET amount = EXCLUDED.amount,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL,
                 batch_id = NULL
             WHERE pending_payouts.send_started_at IS NULL",
            &[&address, &u64_to_i64(amount)?, &now_unix()],
        )?;
        Ok(())
    }

    pub fn mark_pending_payout_send_started(&self, address: &str) -> Result<Option<PendingPayout>> {
        let mut rows = self.mark_pending_payouts_send_started_batch(&[address.to_string()], "")?;
        Ok(rows.pop())
    }

    pub fn mark_pending_payouts_send_started_batch(
        &self,
        addresses: &[String],
        batch_id: &str,
    ) -> Result<Vec<PendingPayout>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;
        let started_at = now_unix();
        let batch_id = batch_id.trim();
        let batch_id_param = (!batch_id.is_empty()).then_some(batch_id);
        let mut rows = Vec::with_capacity(addresses.len());
        for address in addresses {
            tx.execute(
                "UPDATE pending_payouts
                 SET send_started_at = COALESCE(send_started_at, $2),
                     batch_id = COALESCE(batch_id, $3)
                 WHERE address = $1",
                &[&address, &started_at, &batch_id_param],
            )?;
            if let Some(row) = tx.query_opt(
                "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id
                 FROM pending_payouts
                 WHERE address = $1",
                &[&address],
            )? {
                rows.push(row_to_pending_payout(&row));
            }
        }
        tx.commit()?;
        Ok(rows)
    }

    pub fn record_pending_payout_broadcast(
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
            "",
            tx_hash,
        )?;
        recorded
            .pop()
            .ok_or_else(|| anyhow!("no pending payout for {address}"))
    }

    pub fn record_pending_payout_batch_broadcast(
        &self,
        members: &[PendingPayoutBatchMember],
        batch_id: &str,
        tx_hash: &str,
    ) -> Result<Vec<PendingPayout>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.conn().lock();
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
                "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id
                 FROM pending_payouts
                 WHERE address = $1",
                &[&member.address],
            )?;
            recorded.push(row_to_pending_payout(&row));
        }
        tx.commit()?;
        Ok(recorded)
    }

    pub fn reset_pending_payout_send_state(&self, address: &str) -> Result<()> {
        self.conn().lock().execute(
            "UPDATE pending_payouts
             SET send_started_at = NULL,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL,
                 batch_id = NULL
             WHERE address = $1",
            &[&address],
        )?;
        Ok(())
    }

    pub fn reset_pending_payout_batch_send_state(&self, batch_id: &str) -> Result<u64> {
        if batch_id.trim().is_empty() {
            return Ok(0);
        }
        let updated = self.conn().lock().execute(
            "UPDATE pending_payouts
             SET send_started_at = NULL,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL,
                 batch_id = NULL
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

    pub fn backfill_payout_reconciliation_state(
        &self,
    ) -> Result<PayoutReconciliationBackfillReport> {
        const META_KEY: &str = "payout_reconciliation_backfill_v1";

        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;
        if tx
            .query_opt("SELECT value FROM meta WHERE key = $1", &[&META_KEY])?
            .is_some()
        {
            return Ok(PayoutReconciliationBackfillReport::default());
        }

        let source_block_updates = tx.execute(
            "UPDATE block_credit_events
             SET paid_amount = 0,
                 reversible = TRUE
             WHERE paid_amount <> 0 OR reversible = FALSE",
            &[],
        )?;
        let source_fee_updates = tx.execute(
            "UPDATE pool_fee_balance_credits
             SET paid_amount = 0,
                 reversible = TRUE
             WHERE paid_amount <> 0 OR reversible = FALSE",
            &[],
        )?;
        tx.execute("DELETE FROM payout_credit_allocations", &[])?;

        let payout_rows = tx.query(
            "SELECT id, address, amount
             FROM payouts
             WHERE reverted_at IS NULL
             ORDER BY timestamp ASC, id ASC",
            &[],
        )?;

        let mut report = PayoutReconciliationBackfillReport {
            source_credits_marked_reversible: source_block_updates as u64
                + source_fee_updates as u64,
            ..PayoutReconciliationBackfillReport::default()
        };
        for row in payout_rows {
            let payout_id = row.get::<_, i64>(0);
            let address = row.get::<_, String>(1);
            let amount = row.get::<_, i64>(2).max(0) as u64;
            let allocations = Self::mark_balance_credit_sources_paid(&mut tx, &address, amount)?;
            let linked_amount = allocations.iter().fold(0u64, |acc, allocation| {
                acc.saturating_add(allocation.amount)
            });
            Self::insert_payout_credit_allocations(&mut tx, payout_id, &allocations)?;
            let reversible = linked_amount == amount;
            tx.execute(
                "UPDATE payouts SET reversible = $2 WHERE id = $1",
                &[&payout_id, &reversible],
            )?;
            if reversible {
                report.payouts_linked = report.payouts_linked.saturating_add(1);
            } else {
                report.payouts_unreversible = report.payouts_unreversible.saturating_add(1);
            }
        }

        tx.execute(
            "INSERT INTO meta (key, value) VALUES ($1, $2)
             ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
            &[&META_KEY, &b"done".as_slice()],
        )?;
        tx.commit()?;
        Ok(report)
    }

    pub fn complete_pending_payout(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
    ) -> Result<()> {
        let batch_id = self
            .get_pending_payout(address)?
            .and_then(|pending| pending.batch_id);
        if let Some(batch_id) = batch_id.filter(|value| !value.trim().is_empty()) {
            return self.complete_pending_payout_batch(&batch_id, tx_hash);
        }
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;

        let pending = tx.query_opt(
            "SELECT amount, initiated_at, send_started_at, sent_at, tx_hash, fee, batch_id
             FROM pending_payouts
             WHERE address = $1",
            &[&address],
        )?;
        let Some(pending_row) = pending else {
            return Err(anyhow!("no pending payout for {address}"));
        };
        let pending_amount = pending_row.get::<_, i64>(0).max(0) as u64;
        let initiated_at = from_unix(pending_row.get::<_, i64>(1));
        let send_started_at = pending_row.get::<_, Option<i64>>(2).map(from_unix);
        let sent_at = pending_row.get::<_, Option<i64>>(3).map(from_unix);
        if pending_amount != amount {
            return Err(anyhow!(
                "pending payout amount mismatch: expected={}, requested={}",
                pending_amount,
                amount
            ));
        }
        if let Some(pending_tx_hash) = pending_row.get::<_, Option<String>>(4).as_deref() {
            if pending_tx_hash != tx_hash {
                return Err(anyhow!(
                    "pending payout tx mismatch: expected={}, requested={}",
                    pending_tx_hash,
                    tx_hash
                ));
            }
        }
        if let Some(pending_fee_raw) = pending_row.get::<_, Option<i64>>(5) {
            let pending_fee = pending_fee_raw.max(0) as u64;
            if pending_fee != fee {
                return Err(anyhow!(
                    "pending payout fee mismatch: expected={}, requested={}",
                    pending_fee,
                    fee
                ));
            }
        }
        let pending_batch_id = pending_row.get::<_, Option<String>>(6);

        let mut bal = Self::load_balance(&mut tx, address)?;
        if bal.pending < amount {
            return Err(anyhow!("insufficient balance"));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid overflow"))?;

        Self::upsert_balance(&mut tx, &bal)?;
        let payout_timestamp = sent_at.or(send_started_at).unwrap_or(initiated_at);
        let payout_id = Self::insert_completed_payout_row(
            &mut tx,
            address,
            amount,
            fee,
            tx_hash,
            payout_timestamp,
            pending_batch_id.as_deref(),
        )?;
        let allocations = Self::mark_balance_credit_sources_paid(&mut tx, address, amount)?;
        let allocated_amount = allocations.iter().fold(0u64, |acc, allocation| {
            acc.saturating_add(allocation.amount)
        });
        Self::insert_payout_credit_allocations(&mut tx, payout_id, &allocations)?;
        tx.execute(
            "UPDATE payouts SET reversible = $2 WHERE id = $1",
            &[&payout_id, &(allocated_amount == amount)],
        )?;

        tx.execute(
            "DELETE FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn complete_pending_payout_batch(&self, batch_id: &str, tx_hash: &str) -> Result<()> {
        if batch_id.trim().is_empty() {
            return Err(anyhow!("batch_id is required"));
        }
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;

        let rows = tx.query(
            "SELECT address, amount, initiated_at, send_started_at, sent_at, tx_hash, fee, batch_id
             FROM pending_payouts
             WHERE batch_id = $1
             ORDER BY address ASC",
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

        let mut conn = self.conn().lock();
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
        let mut conn = self.conn().lock();
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
            let kind = BalanceCreditSourceKind::from_db(&row.get::<_, String>(0));
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

    pub fn revert_completed_payout_tx(
        &self,
        tx_hash: &str,
        reason: &str,
    ) -> Result<CompletedPayoutReconciliation> {
        let tx_hash = tx_hash.trim();
        if tx_hash.is_empty() {
            return Ok(CompletedPayoutReconciliation::default());
        }

        let mut conn = self.conn().lock();
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
                .map(|allocation| PayoutCreditAllocation {
                    source_kind: BalanceCreditSourceKind::from_db(&allocation.get::<_, String>(0)),
                    block_height: allocation.get::<_, i64>(1).max(0) as u64,
                    block_hash: allocation.get::<_, Option<String>>(2),
                    address: allocation.get::<_, String>(3),
                    amount: allocation.get::<_, i64>(4).max(0) as u64,
                })
                .collect::<Vec<_>>();
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

        let mut conn = self.conn().lock();
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
                let source_kind = BalanceCreditSourceKind::from_db(&allocation.get::<_, String>(0));
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

    pub fn cancel_pending_payout(&self, address: &str) -> Result<()> {
        self.conn().lock().execute(
            "DELETE FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        Ok(())
    }

    pub fn get_pending_payouts(&self) -> Result<Vec<PendingPayout>> {
        let rows = self.conn().lock().query(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id
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
        let row = self.conn().lock().query_opt(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at, batch_id
             FROM pending_payouts
             WHERE address = $1",
            &[&address],
        )?;
        Ok(row.map(|row| row_to_pending_payout(&row)))
    }

    pub fn get_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
    ) -> Result<Option<(u64, SystemTime)>> {
        let row = self.conn().lock().query_opt(
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

    pub fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO vardiff_hints (address, worker, difficulty, updated_at) VALUES ($1, $2, $3, $4)
             ON CONFLICT(address, worker) DO UPDATE SET difficulty = EXCLUDED.difficulty, updated_at = EXCLUDED.updated_at",
            &[&address, &worker, &u64_to_i64(difficulty.max(1))?, &to_unix(updated_at)],
        )?;
        Ok(())
    }

    pub fn get_vardiff_hints_for_address(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<(u64, SystemTime)>> {
        let limit = (limit.max(1)).min(i64::MAX as usize) as i64;
        let rows = self.conn().lock().query(
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

    pub fn vardiff_hint_summary(&self, address: &str, floor: u64) -> Result<VardiffHintSummary> {
        let row = self.conn().lock().query_one(
            "SELECT
                COUNT(*)::bigint,
                COUNT(*) FILTER (WHERE difficulty < $2)::bigint,
                COUNT(*) FILTER (WHERE difficulty = $2)::bigint,
                COUNT(*) FILTER (WHERE difficulty > $2)::bigint,
                MIN(difficulty),
                PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY difficulty),
                MAX(difficulty),
                MAX(updated_at)
             FROM vardiff_hints
             WHERE address = $1",
            &[&address, &u64_to_i64(floor.max(1))?],
        )?;
        Ok(VardiffHintSummary {
            total_workers: row.get::<_, i64>(0).max(0) as u64,
            below_floor_workers: row.get::<_, i64>(1).max(0) as u64,
            at_floor_workers: row.get::<_, i64>(2).max(0) as u64,
            above_floor_workers: row.get::<_, i64>(3).max(0) as u64,
            min_difficulty: row
                .get::<_, Option<i64>>(4)
                .map(|value| value.max(1) as u64),
            median_difficulty: row
                .get::<_, Option<i64>>(5)
                .map(|value| value.max(1) as u64),
            max_difficulty: row
                .get::<_, Option<i64>>(6)
                .map(|value| value.max(1) as u64),
            latest_updated_at: row.get::<_, Option<i64>>(7).map(from_unix),
        })
    }

    pub fn recent_vardiff_hint_diagnostics(
        &self,
        address: &str,
        limit: i64,
    ) -> Result<Vec<VardiffHintDiagnostic>> {
        let rows = self.conn().lock().query(
            "SELECT worker, difficulty, updated_at
             FROM vardiff_hints
             WHERE address = $1
             ORDER BY updated_at DESC, worker ASC
             LIMIT $2",
            &[&address, &limit.max(1)],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| VardiffHintDiagnostic {
                worker: row.get::<_, String>(0),
                difficulty: row.get::<_, i64>(1).max(1) as u64,
                updated_at: from_unix(row.get::<_, i64>(2)),
            })
            .collect())
    }

    pub fn add_stat_snapshot(
        &self,
        timestamp: SystemTime,
        hashrate: f64,
        miners: i32,
        workers: i32,
    ) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO stat_snapshots (timestamp, hashrate, miners, workers) VALUES ($1, $2, $3, $4)",
            &[&to_unix(timestamp), &hashrate, &miners, &workers],
        )?;
        Ok(())
    }

    pub fn load_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        accepted_window_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState> {
        let rows = self.conn().lock().query(
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
            .map(|row| PersistedValidationAddressState {
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
                ),
                last_seen_at: from_unix(row.get::<_, i64>(12)),
            })
            .collect::<Vec<_>>();

        let provisional_rows = self.conn().lock().query(
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

        let accepted_rows = self.conn().lock().query(
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

    pub fn latest_validation_clear_event_id(&self) -> Result<i64> {
        let row = self.conn().lock().query_one(
            "SELECT COALESCE(MAX(id), 0) FROM validation_clear_events",
            &[],
        )?;
        Ok(row.get::<_, i64>(0).max(0))
    }

    pub fn load_validation_address_activity(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
        accepted_window_cutoff: SystemTime,
    ) -> Result<LoadedValidationAddressActivity> {
        let provisional_rows = self.conn().lock().query(
            "SELECT address, share_id, created_at
             FROM validation_provisionals
             WHERE address = $1
               AND created_at > $2
             ORDER BY created_at ASC",
            &[&address, &to_unix(provisional_cutoff)],
        )?;
        let provisionals = provisional_rows
            .iter()
            .map(|row| PersistedValidationProvisional {
                address: row.get::<_, String>(0),
                share_id: row.get::<_, Option<i64>>(1),
                created_at: from_unix(row.get::<_, i64>(2)),
            })
            .collect::<Vec<_>>();

        let accepted_rows = self.conn().lock().query(
            "SELECT id, miner, created_at, difficulty, status
             FROM shares
             WHERE miner = $1
               AND created_at > $2
               AND status IN ('verified', 'provisional')
             ORDER BY created_at ASC, id ASC",
            &[&address, &to_unix(accepted_window_cutoff)],
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

        Ok(LoadedValidationAddressActivity {
            provisionals,
            accepted_window,
        })
    }

    pub fn load_validation_clear_events_since(
        &self,
        cursor: i64,
    ) -> Result<Vec<ValidationClearEvent>> {
        let rows = self.conn().lock().query(
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
        self.conn().lock().execute(
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
        self.conn().lock().execute(
            "INSERT INTO validation_provisionals (address, share_id, created_at) VALUES ($1, $2, $3)",
            &[&address, &share_id, &to_unix(created_at)],
        )?;
        Ok(())
    }

    pub fn clean_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<()> {
        let mut conn = self.conn().lock();
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

    pub fn get_stat_snapshots(&self, since: SystemTime) -> Result<Vec<crate::db::StatSnapshot>> {
        let mut conn = self.conn().lock();
        let rows = conn.query(
            "SELECT id, timestamp, hashrate, miners, workers FROM stat_snapshots WHERE timestamp >= $1 ORDER BY timestamp ASC",
            &[&to_unix(since)],
        )?;
        Ok(rows
            .iter()
            .map(|row| crate::db::StatSnapshot {
                id: row.get::<_, i64>(0),
                timestamp: from_unix(row.get::<_, i64>(1)),
                hashrate: row.get::<_, f64>(2),
                miners: row.get::<_, i32>(3),
                workers: row.get::<_, i32>(4),
            })
            .collect())
    }

    pub fn clean_old_snapshots(&self, retain_duration: Duration) -> Result<u64> {
        let cutoff = SystemTime::now()
            .checked_sub(retain_duration)
            .unwrap_or(UNIX_EPOCH);
        let removed = self.conn().lock().execute(
            "DELETE FROM stat_snapshots WHERE timestamp < $1",
            &[&to_unix(cutoff)],
        )?;
        Ok(removed)
    }

    pub fn rollup_and_prune_shares_before(&self, before: SystemTime) -> Result<u64> {
        let cutoff = floor_to_day_start_unix(to_unix(before));
        if cutoff <= 0 {
            return Ok(0);
        }

        let mut conn = self.conn().lock();
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
                COALESCE(NULLIF(BTRIM(reject_reason), ''), 'legacy / unknown') AS reason,
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

    pub fn rollup_and_prune_payouts_before(&self, before: SystemTime) -> Result<u64> {
        let cutoff = floor_to_day_start_unix(to_unix(before));
        if cutoff <= 0 {
            return Ok(0);
        }

        let mut conn = self.conn().lock();
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
        let rows = self.conn().lock().query(
            "SELECT (created_at / $3) * $3 AS bucket, SUM(difficulty)::bigint, COUNT(*)::bigint
             FROM shares
             WHERE miner = $1 AND created_at >= $2
               AND status IN ('verified','provisional')
             GROUP BY bucket ORDER BY bucket",
            &[&address, &to_unix(since), &bucket_secs],
        )?;
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

    /// Returns per-worker stats: Vec<(worker, accepted, rejected, total_diff, last_share_at)>
    pub fn worker_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<Vec<(String, u64, u64, u64, i64)>> {
        let rows = self.conn().lock().query(
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
    pub fn worker_hashrate_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<Vec<(String, u64, u64, Option<SystemTime>, Option<SystemTime>)>> {
        let rows = self.conn().lock().query(
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

    pub fn get_blocks_for_miner(&self, address: &str) -> Result<Vec<crate::db::DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks WHERE finder = $1 ORDER BY height DESC",
            &[&address],
        )?;
        Ok(rows.iter().map(|row| row_to_block(row)).collect())
    }

    pub fn miner_has_any_activity(&self, address: &str) -> Result<bool> {
        let row = self.conn().lock().query_one(
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
    pub fn miner_lifetime_counts(
        &self,
    ) -> Result<std::collections::HashMap<String, (u64, u64, u64, Option<i64>)>> {
        let mut conn = self.conn().lock();
        let share_rows = conn.query(
            "SELECT miner,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END)::bigint,
                    MAX(created_at)::bigint
             FROM shares GROUP BY miner",
            &[],
        )?;
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

    pub fn miner_worker_counts_since(
        &self,
        since: SystemTime,
    ) -> Result<std::collections::HashMap<String, usize>> {
        let mut conn = self.conn().lock();
        let rows = conn.query(
            "SELECT miner, COUNT(DISTINCT worker)::bigint
             FROM shares
             WHERE created_at >= $1
             GROUP BY miner",
            &[&to_unix(since)],
        )?;
        let mut map = std::collections::HashMap::new();
        for row in &rows {
            let miner: String = row.get(0);
            let worker_count: i64 = row.get(1);
            map.insert(miner, worker_count.max(0) as usize);
        }
        Ok(map)
    }
}

impl ShareStore for PostgresStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        PostgresStore::is_share_seen(self, job_id, nonce)
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        PostgresStore::mark_share_seen(self, job_id, nonce)
    }

    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        PostgresStore::try_claim_share(self, job_id, nonce)
    }

    fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        PostgresStore::release_share_claim(self, job_id, nonce)
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        PostgresStore::add_share(self, share)
    }

    fn address_risk_strikes(&self, address: &str) -> Result<u64> {
        let now = SystemTime::now();
        Ok(PostgresStore::get_address_risk(self, address)?
            .map(|v| active_window_strikes(v.strikes, v.strike_window_until, now))
            .unwrap_or(0))
    }

    fn clear_address_risk_history(&self, address: &str) -> Result<()> {
        PostgresStore::clear_address_risk_history(self, address)
    }

    fn get_vardiff_hint(&self, address: &str, worker: &str) -> Result<Option<(u64, SystemTime)>> {
        PostgresStore::get_vardiff_hint(self, address, worker)
    }

    fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        PostgresStore::upsert_vardiff_hint(self, address, worker, difficulty, updated_at)
    }

    fn get_vardiff_hints_for_address(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<(u64, SystemTime)>> {
        PostgresStore::get_vardiff_hints_for_address(self, address, limit)
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
        id: row.get::<_, i64>(0),
        address: row.get::<_, String>(1),
        amount: row.get::<_, i64>(2).max(0) as u64,
        fee: row.get::<_, i64>(3).max(0) as u64,
        tx_hash: row.get::<_, String>(4),
        timestamp: from_unix(row.get::<_, i64>(5)),
        confirmed: row_get_boolish(&row, 6)?,
        batch_id: row.get::<_, Option<String>>(7),
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
        id: row.get::<_, i64>(0),
        sampled_at: from_unix(row.get::<_, i64>(1)),
        source: row.get::<_, String>(2),
        synthetic: row.get::<_, bool>(3),
        api_up: row.get::<_, Option<bool>>(4),
        stratum_up: row.get::<_, Option<bool>>(5),
        db_up: row.get::<_, bool>(6),
        daemon_up: row.get::<_, Option<bool>>(7),
        public_http_up: row.get::<_, Option<bool>>(8),
        daemon_syncing: row.get::<_, Option<bool>>(9),
        chain_height: opt_i64_to_u64(row.get::<_, Option<i64>>(10)),
        template_age_seconds: opt_i64_to_u64(row.get::<_, Option<i64>>(11)),
        last_refresh_millis: opt_i64_to_u64(row.get::<_, Option<i64>>(12)),
        stratum_snapshot_age_seconds: opt_i64_to_u64(row.get::<_, Option<i64>>(13)),
        connected_miners: opt_i64_to_u64(row.get::<_, Option<i64>>(14)),
        connected_workers: opt_i64_to_u64(row.get::<_, Option<i64>>(15)),
        estimated_hashrate: row.get::<_, Option<f64>>(16),
        wallet_up: row.get::<_, Option<bool>>(17),
        last_accepted_share_at: row.get::<_, Option<i64>>(18).map(from_unix),
        last_accepted_share_age_seconds: opt_i64_to_u64(row.get::<_, Option<i64>>(19)),
        payout_pending_count: opt_i64_to_u64(row.get::<_, Option<i64>>(20)),
        payout_pending_amount: opt_i64_to_u64(row.get::<_, Option<i64>>(21)),
        oldest_pending_payout_at: row.get::<_, Option<i64>>(22).map(from_unix),
        oldest_pending_payout_age_seconds: opt_i64_to_u64(row.get::<_, Option<i64>>(23)),
        oldest_pending_send_started_at: row.get::<_, Option<i64>>(24).map(from_unix),
        oldest_pending_send_age_seconds: opt_i64_to_u64(row.get::<_, Option<i64>>(25)),
        validation_candidate_queue_depth: opt_i64_to_u64(row.get::<_, Option<i64>>(26)),
        validation_regular_queue_depth: opt_i64_to_u64(row.get::<_, Option<i64>>(27)),
        summary_state: row.get::<_, String>(28),
        details_json: row.get::<_, Option<String>>(29),
    })
}

fn row_to_monitor_incident(row: postgres::Row) -> Result<MonitorIncident> {
    Ok(MonitorIncident {
        id: row.get::<_, i64>(0),
        dedupe_key: row.get::<_, String>(1),
        kind: row.get::<_, String>(2),
        severity: row.get::<_, String>(3),
        visibility: row.get::<_, String>(4),
        source: row.get::<_, String>(5),
        summary: row.get::<_, String>(6),
        detail: row.get::<_, Option<String>>(7),
        started_at: from_unix(row.get::<_, i64>(8)),
        updated_at: from_unix(row.get::<_, i64>(9)),
        ended_at: row.get::<_, Option<i64>>(10).map(from_unix),
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

fn normalize_block_status_filter(status: Option<&str>) -> Option<&'static str> {
    match status.map(str::trim) {
        Some(v) if v.eq_ignore_ascii_case("confirmed") => Some("confirmed"),
        Some(v) if v.eq_ignore_ascii_case("orphaned") => Some("orphaned"),
        Some(v) if v.eq_ignore_ascii_case("pending") => Some("pending"),
        Some(v) if v.eq_ignore_ascii_case("paid") => Some("paid"),
        Some(v) if v.eq_ignore_ascii_case("unpaid") => Some("unpaid"),
        _ => None,
    }
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

fn validation_hold_cause_from_db(value: Option<&str>) -> Option<ValidationHoldCause> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        Some("invalid_samples") => Some(ValidationHoldCause::InvalidSamples),
        Some("provisional_backlog") => Some(ValidationHoldCause::ProvisionalBacklog),
        Some("payout_coverage") => Some(ValidationHoldCause::PayoutCoverage),
        _ => None,
    }
}

fn validation_hold_cause_to_db(value: Option<ValidationHoldCause>) -> Option<&'static str> {
    value.map(ValidationHoldCause::as_str)
}

fn infer_validation_hold_cause_from_row(
    forced_started_at: Option<SystemTime>,
    forced_until: Option<SystemTime>,
) -> Option<ValidationHoldCause> {
    if forced_started_at.is_some() {
        Some(ValidationHoldCause::InvalidSamples)
    } else if forced_until.is_some() {
        Some(ValidationHoldCause::ProvisionalBacklog)
    } else {
        None
    }
}

fn validation_hold_reason(
    cause: Option<ValidationHoldCause>,
    pending_provisional: u64,
    recent_verified_difficulty: u64,
    recent_provisional_difficulty: u64,
) -> Option<String> {
    match cause {
        Some(ValidationHoldCause::InvalidSamples) => {
            Some("recent invalid sampled shares are under review".to_string())
        }
        Some(ValidationHoldCause::ProvisionalBacklog) => Some(
            if recent_provisional_difficulty > 0 || recent_verified_difficulty > 0 {
                match recent_verified_difficulty {
                    0 => format!(
                        "recent provisional diff {} has no recent verified diff yet",
                        recent_provisional_difficulty
                    ),
                    verified => format!(
                        "recent provisional diff {} vs {} verified",
                        recent_provisional_difficulty, verified
                    ),
                }
            } else if pending_provisional > 0 {
                format!(
                    "{pending_provisional} provisional share{} waiting for full verification",
                    if pending_provisional == 1 { "" } else { "s" }
                )
            } else {
                "recent provisional backlog is draining".to_string()
            },
        ),
        Some(ValidationHoldCause::PayoutCoverage) => {
            Some("boosting verified-share coverage so payout weight stays proportional".to_string())
        }
        None => None,
    }
}

fn quarantine_until_for_strikes(
    now: SystemTime,
    strikes: u64,
    quarantine_base: Duration,
    quarantine_max: Duration,
) -> SystemTime {
    if strikes == 0 || quarantine_base.is_zero() {
        return now;
    }

    let mut duration = quarantine_base;
    if !quarantine_max.is_zero() && duration > quarantine_max {
        duration = quarantine_max;
    }
    for _ in 1..strikes {
        duration = duration.saturating_mul(2);
        if !quarantine_max.is_zero() && duration >= quarantine_max {
            duration = quarantine_max;
            break;
        }
    }
    now + duration
}

fn active_window_strikes(strikes: u64, window_until: Option<SystemTime>, now: SystemTime) -> u64 {
    if window_until.is_some_and(|until| until > now) {
        strikes
    } else {
        0
    }
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

        let fees = store.get_recent_pool_fees(500).expect("recent fees");
        let matching = fees
            .iter()
            .filter(|event| event.block_height == height)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].amount, 5);
        assert_eq!(matching[0].fee_address, fee_address);

        assert!(!store
            .apply_block_credits_and_mark_paid_with_fee(height, &credits, Some(&fee))
            .expect("second apply no-op"));
        let fees_after = store.get_recent_pool_fees(500).expect("recent fees again");
        let matching_after = fees_after
            .iter()
            .filter(|event| event.block_height == height)
            .count();
        assert_eq!(matching_after, 1);
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

        let fees = store.get_recent_pool_fees(500).expect("recent fees");
        assert!(!fees.iter().any(|event| event.block_height == height));
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
    fn backfill_uncredited_pool_fee_balance_credits_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_200_000u64 + (rand::random::<u16>() as u64);
        let fee_address = format!("pool-{suffix}");
        store
            .record_pool_fee(height, 25, &fee_address, SystemTime::now())
            .expect("record legacy fee");

        let report = store
            .backfill_uncredited_pool_fee_balance_credits(Some(&fee_address))
            .expect("backfill");
        assert_eq!(report.credited_events, 1);
        assert_eq!(report.credited_amount, 25);
        assert_eq!(report.reconciled_finder_fallback_events, 0);
        assert_eq!(report.reconciled_finder_fallback_amount, 0);
        assert_eq!(report.skipped_mismatched_destination, 0);
        assert_eq!(
            store
                .get_balance(&fee_address)
                .expect("fee balance")
                .pending,
            25
        );

        let second = store
            .backfill_uncredited_pool_fee_balance_credits(Some(&fee_address))
            .expect("second backfill");
        assert_eq!(second.credited_events, 0);
        assert_eq!(second.credited_amount, 0);
    }

    #[test]
    fn backfill_uncredited_pool_fee_balance_credits_reconciles_finder_fallback_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_300_000u64 + (rand::random::<u16>() as u64);
        let recorded = format!("finder-{suffix}");
        let expected = format!("expected-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("legacy-fee-block-{suffix}"),
                difficulty: 1,
                finder: recorded.clone(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert block");
        store
            .record_pool_fee(height, 30, &recorded, SystemTime::now())
            .expect("record legacy fee");

        let report = store
            .backfill_uncredited_pool_fee_balance_credits(Some(&expected))
            .expect("backfill");
        assert_eq!(report.credited_events, 1);
        assert_eq!(report.credited_amount, 30);
        assert_eq!(report.reconciled_finder_fallback_events, 1);
        assert_eq!(report.reconciled_finder_fallback_amount, 30);
        assert_eq!(report.skipped_mismatched_destination, 0);
        assert_eq!(
            store
                .get_balance(&recorded)
                .expect("recorded balance")
                .pending,
            0
        );
        assert_eq!(
            store
                .get_balance(&expected)
                .expect("expected balance")
                .pending,
            30
        );
        assert_eq!(
            store
                .get_block_pool_fee_event(height)
                .expect("fee event query")
                .expect("fee event")
                .fee_address,
            expected
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
    fn backfill_payout_reconciliation_state_reconstructs_legacy_completed_payouts_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_355_000u64 + (rand::random::<u16>() as u64);
        let addr = format!("legacy-paid-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("legacy-paid-block-{suffix}"),
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
            .update_balance(&Balance {
                address: addr.clone(),
                pending: 0,
                paid: 100,
            })
            .expect("seed legacy balance");
        store
            .add_payout(&addr, 100, 2, "legacy-paid-tx")
            .expect("insert legacy payout");

        let report = store
            .backfill_payout_reconciliation_state()
            .expect("backfill payout reconciliation state");
        assert_eq!(report.payouts_linked, 1);
        assert_eq!(report.payouts_unreversible, 0);

        let events = store
            .get_block_credit_events(height)
            .expect("credit events after backfill");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].paid_amount, 100);
        assert!(events[0].reversible);

        let reverted = store
            .revert_completed_payout_tx("legacy-paid-tx", "missing after recovery")
            .expect("revert legacy payout");
        assert_eq!(reverted.reverted_payout_rows, 1);
        assert_eq!(reverted.restored_pending_amount, 100);
        assert_eq!(reverted.dropped_orphaned_amount, 0);
        assert!(!reverted.manual_reconciliation_required);
        let balance = store.get_balance(&addr).expect("balance after revert");
        assert_eq!(balance.pending, 100);
        assert_eq!(balance.paid, 0);
        assert!(store
            .get_recent_payouts(10)
            .expect("recent payouts")
            .is_empty());
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
            .lock()
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
            .lock()
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
            .get_recent_pool_fees(10)
            .expect("recent fees")
            .is_empty());
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
                orphaned: true,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert orphaned block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(miner.clone(), 100)])
            .expect("apply credits"));
        store
            .record_pool_fee(height, 25, &fee_address, SystemTime::now())
            .expect("record fee");
        store
            .backfill_uncredited_pool_fee_balance_credits(Some(&fee_address))
            .expect("backfill fee credits");

        {
            let mut conn = store.conn().lock();
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
            .escalate_address_risk(
                &address,
                "low difficulty share",
                Duration::from_secs(60),
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(24 * 60 * 60),
            )
            .expect("escalate address risk");

        let expired = SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .expect("expired timestamp");
        store
            .conn()
            .lock()
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
            .record_suspected_fraud(
                &address,
                "invalid share proof",
                1,
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("seed suspected fraud");
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
    fn backfill_uncredited_pool_fee_balance_credits_skips_mismatched_destination_postgres() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {POSTGRES_TEST_URL_ENV} to run postgres integration checks"
            );
            return;
        };

        let suffix = unique_suffix();
        let height = 8_400_000u64 + (rand::random::<u16>() as u64);
        let recorded = format!("recorded-{suffix}");
        let expected = format!("expected-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: format!("mismatched-fee-block-{suffix}"),
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
        store
            .record_pool_fee(height, 30, &recorded, SystemTime::now())
            .expect("record legacy fee");

        let report = store
            .backfill_uncredited_pool_fee_balance_credits(Some(&expected))
            .expect("backfill");
        assert_eq!(report.credited_events, 0);
        assert_eq!(report.credited_amount, 0);
        assert_eq!(report.reconciled_finder_fallback_events, 0);
        assert_eq!(report.reconciled_finder_fallback_amount, 0);
        assert_eq!(report.skipped_mismatched_destination, 1);
        assert_eq!(
            store
                .get_balance(&recorded)
                .expect("recorded balance")
                .pending,
            0
        );
        assert_eq!(
            store
                .get_balance(&expected)
                .expect("expected balance")
                .pending,
            0
        );
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
    fn visible_payouts_for_address_include_broadcast_pending_postgres() {
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
            .lock()
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

        let payouts = store
            .get_recent_visible_payouts_for_address(&addr, 10)
            .expect("visible payouts");
        assert_eq!(payouts.len(), 2);
        assert_eq!(payouts[0].tx_hash, pending_tx);
        assert_eq!(payouts[0].timestamp, broadcast.sent_at.expect("sent at"));
        assert!(!payouts[0].confirmed);
        assert!(payouts[1].confirmed);
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
            .add_payout(&addr1, 10, 1, &format!("tx-a-{suffix}"))
            .expect("add payout a");
        store
            .add_payout(&addr2, 20, 2, &format!("tx-b-{suffix}"))
            .expect("add payout b");
        store
            .add_payout(&addr1, 30, 3, &format!("tx-c-{suffix}"))
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
    fn payouts_page_includes_broadcast_pending_postgres() {
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
            .conn()
            .lock()
            .execute(
                "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES ($1, $2, $3, $4, $5)",
                &[&confirmed_addr, &10i64, &1i64, &confirmed_tx, &100i64],
            )
            .expect("insert confirmed payout");
        store
            .create_pending_payout(&pending_addr, 25)
            .expect("create pending");
        store
            .mark_pending_payout_send_started(&pending_addr)
            .expect("mark started");
        store
            .record_pending_payout_broadcast(&pending_addr, 25, 2, &pending_tx)
            .expect("record broadcast");

        let (items, total) = store
            .get_payouts_page(None, None, "time_desc", 10, 0)
            .expect("payouts page");
        assert!(total >= 2);
        let pending = items
            .iter()
            .find(|item| item.tx_hash == pending_tx)
            .expect("pending payout item");
        assert!(!pending.confirmed);
        let confirmed = items
            .iter()
            .find(|item| item.tx_hash == confirmed_tx)
            .expect("confirmed payout item");
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
            .lock()
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
            .lock()
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
            let mut conn = store.conn().lock();
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
            .lock()
            .query_one(
                "SELECT amount FROM manual_payout_offsets WHERE address = $1",
                &[&application.address],
            )
            .expect("remaining offset row");
        assert_eq!(offset_row.get::<_, i64>(0), 13);

        let source_row = store
            .conn()
            .lock()
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
