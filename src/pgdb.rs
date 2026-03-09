use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use postgres::{types::ToSql, Client, Config as PostgresConfig, NoTls, Row, Transaction};
use tracing::warn;

use crate::db::{
    AddressRiskState, Balance, BlockCreditEvent, DbBlock, DbShare, MonitorHeartbeat,
    MonitorHeartbeatUpsert, MonitorIncident, MonitorIncidentUpsert, Payout, PendingPayout,
    PoolFeeEvent, PoolFeeRecord, PublicPayoutBatch, ShareReplayData, ShareReplayUpdate,
};
use crate::engine::{ShareRecord, ShareStore};
use crate::stats::RejectionReasonCount;
use crate::validation::{
    LoadedValidationState, PersistedValidationAddressState, PersistedValidationProvisional,
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
    timestamp BIGINT NOT NULL
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
    timestamp BIGINT NOT NULL,
    credited_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_balance_credits_fee_address
    ON pool_fee_balance_credits(fee_address);

CREATE TABLE IF NOT EXISTS block_credit_events (
    id BIGSERIAL PRIMARY KEY,
    block_height BIGINT NOT NULL,
    address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    UNIQUE (block_height, address)
);
CREATE INDEX IF NOT EXISTS idx_block_credit_events_block_height
    ON block_credit_events(block_height, amount DESC, address ASC);
CREATE INDEX IF NOT EXISTS idx_block_credit_events_address
    ON block_credit_events(address, block_height DESC);

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
    forced_until BIGINT,
    last_seen_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_address_states_last_seen
    ON validation_address_states(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_address_states_forced_until
    ON validation_address_states(forced_until DESC);

CREATE TABLE IF NOT EXISTS validation_provisionals (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
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
        conn.batch_execute("ALTER TABLE shares ADD COLUMN IF NOT EXISTS reject_reason TEXT")
            .context("ensure shares.reject_reason column")?;
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

        tx.execute(
            "INSERT INTO shares (job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, reject_reason, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                &share.job_id,
                &share.miner,
                &share.worker,
                &u64_to_i64(share.difficulty)?,
                &u64_to_i64(share.nonce)?,
                &share.status,
                &share.was_sampled,
                &share.block_hash,
                &share.reject_reason,
                &created,
            ],
        )?;
        tx.commit()?;
        Ok(())
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

    pub fn get_recent_shares(&self, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares ORDER BY created_at DESC LIMIT $1",
            &[&limit],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE miner = $1 ORDER BY created_at DESC LIMIT $2",
            &[&address, &limit],
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
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at >= $1 AND created_at <= $2",
            &[&start_ts, &end_ts],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn get_last_n_shares_before(&self, before: SystemTime, n: i64) -> Result<Vec<DbShare>> {
        let before_ts = to_unix(before);
        let rows = self.conn().lock().query(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at <= $1 ORDER BY created_at DESC LIMIT $2",
            &[&before_ts, &n],
        )?;
        Ok(rows.into_iter().map(row_to_share).collect())
    }

    pub fn latest_share_timestamp_for_block_hash(&self, hash: &str) -> Result<Option<SystemTime>> {
        let row = self.conn().lock().query_one(
            "SELECT MAX(created_at) FROM shares WHERE block_hash = $1",
            &[&hash],
        )?;
        Ok(row.get::<_, Option<i64>>(0).map(from_unix))
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

    pub fn get_all_blocks(&self) -> Result<Vec<DbBlock>> {
        let rows = self.conn().lock().query(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out, effort_pct
             FROM blocks ORDER BY height DESC",
            &[],
        )?;
        Ok(rows.into_iter().map(|row| row_to_block(&row)).collect())
    }

    pub fn avg_effort_pct(&self) -> Result<Option<f64>> {
        let row = self.conn().lock().query_one(
            "SELECT AVG(effort_pct) FROM blocks WHERE orphaned = false AND effort_pct IS NOT NULL",
            &[],
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
            "reward_desc" => "reward DESC, height DESC",
            "reward_asc" => "reward ASC, height DESC",
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
    ) -> Result<()> {
        if destination.is_empty() || amount == 0 {
            return Ok(());
        }

        let row = tx.query_opt(
            "SELECT address, pending, paid FROM balances WHERE address = $1",
            &[&destination],
        )?;
        let mut bal = if let Some(row) = row {
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
        };
        bal.pending = bal
            .pending
            .checked_add(amount)
            .ok_or_else(|| anyhow!("balance overflow"))?;

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
    ) -> Result<bool> {
        if amount == 0 {
            return Ok(false);
        }

        let destination = destination.trim();
        if destination.is_empty() {
            return Err(anyhow!("fee address is required"));
        }

        let inserted = tx.execute(
            "INSERT INTO pool_fee_balance_credits (block_height, fee_address, amount, timestamp, credited_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(block_height) DO NOTHING",
            &[
                &u64_to_i64(block_height)?,
                &destination,
                &u64_to_i64(amount)?,
                &to_unix(timestamp),
                &now_unix(),
            ],
        )?;
        if inserted == 0 {
            return Ok(false);
        }

        Self::credit_pending_balance(tx, destination, amount)?;
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
            "SELECT confirmed, orphaned, paid_out FROM blocks WHERE height = $1",
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

            Self::credit_pending_balance(&mut tx, destination, *amount)?;

            tx.execute(
                "INSERT INTO block_credit_events (block_height, address, amount)
                 VALUES ($1, $2, $3)
                 ON CONFLICT(block_height, address) DO NOTHING",
                &[
                    &u64_to_i64(block_height)?,
                    &destination,
                    &u64_to_i64(*amount)?,
                ],
            )?;
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

    pub fn add_payout(&self, address: &str, amount: u64, fee: u64, tx_hash: &str) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES ($1, $2, $3, $4, $5)",
            &[
                &address,
                &u64_to_i64(amount)?,
                &u64_to_i64(fee)?,
                &tx_hash,
                &now_unix(),
            ],
        )?;
        Ok(())
    }

    pub fn get_recent_payouts(&self, limit: i64) -> Result<Vec<Payout>> {
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
             FROM payouts
             ORDER BY id DESC
             LIMIT $1",
            &[&limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_recent_payouts_for_address(&self, address: &str, limit: i64) -> Result<Vec<Payout>> {
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
             FROM payouts
             WHERE address = $1
             ORDER BY id DESC
             LIMIT $2",
            &[&address, &limit],
        )?;
        rows.into_iter().map(row_to_payout).collect()
    }

    pub fn get_recent_visible_payouts_for_address(
        &self,
        address: &str,
        limit: i64,
    ) -> Result<Vec<Payout>> {
        let rows = self.conn().lock().query(
            "SELECT id, address, amount, fee, tx_hash, timestamp, confirmed
             FROM (
                 SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
                 FROM payouts
                 WHERE address = $1
                 UNION ALL
                 SELECT
                     0 AS id,
                     address,
                     amount,
                     COALESCE(fee, 0) AS fee,
                     tx_hash,
                     COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                     0 AS confirmed
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
            "SELECT id, address, amount, fee, tx_hash, timestamp, confirmed
             FROM (
                 SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
                 FROM payouts
                 UNION ALL
                 SELECT
                     0 AS id,
                     address,
                     amount,
                     COALESCE(fee, 0)::bigint AS fee,
                     tx_hash,
                     COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                     0 AS confirmed
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
            "time_asc" => "bucket ASC",
            "amount_desc" => "total_amount DESC, bucket DESC",
            "amount_asc" => "total_amount ASC, bucket DESC",
            _ => "bucket DESC",
        };

        let mut conn = self.conn().lock();
        let total_row = conn.query_one(
            "SELECT COUNT(*) FROM (
                SELECT (timestamp / 300) AS bucket
                FROM (
                    SELECT timestamp FROM payouts
                    UNION ALL
                    SELECT COALESCE(sent_at, send_started_at, initiated_at) AS timestamp
                    FROM pending_payouts
                    WHERE tx_hash IS NOT NULL
                      AND BTRIM(tx_hash) <> ''
                ) visible
                GROUP BY bucket
            ) grouped",
            &[],
        )?;
        let total: i64 = total_row.get(0);

        let sql = format!(
            "WITH visible AS (
                SELECT amount, fee, tx_hash, timestamp, 1 AS confirmed
                FROM payouts
                UNION ALL
                SELECT
                    amount,
                    COALESCE(fee, 0)::bigint AS fee,
                    tx_hash,
                    COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                    0 AS confirmed
                FROM pending_payouts
                WHERE tx_hash IS NOT NULL
                  AND BTRIM(tx_hash) <> ''
             ),
             grouped AS (
                SELECT
                    (timestamp / 300) AS bucket,
                    SUM(amount)::bigint AS total_amount,
                    SUM(fee)::bigint AS total_fee,
                    COUNT(*)::bigint AS recipient_count,
                    STRING_AGG(tx_hash, ',') AS tx_hashes,
                    MAX(timestamp)::bigint AS batch_ts,
                    MIN(confirmed)::bigint AS confirmed
                FROM visible
                GROUP BY bucket
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
            "SELECT COALESCE(SUM(amount)::BIGINT, 0) FROM pool_fee_events",
            &[],
        )?;
        let total: i64 = row.get(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_recent_pool_fees(&self, limit: i64) -> Result<Vec<PoolFeeEvent>> {
        let rows = self.conn().lock().query(
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events ORDER BY id DESC LIMIT $1",
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
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events
             WHERE block_height = $1",
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
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events ORDER BY id DESC",
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
            "SELECT id, block_height, address, amount
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
            })
            .collect())
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
        let force = state.as_ref().is_some_and(|s| {
            s.force_verify_until.is_some_and(|until| until > now)
                || s.quarantined_until.is_some_and(|until| until > now)
        });
        Ok((force, state))
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
            "INSERT INTO pending_payouts (address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at)
             VALUES ($1, $2, $3, NULL, NULL, NULL, NULL)
             ON CONFLICT(address) DO UPDATE SET amount = EXCLUDED.amount, tx_hash = NULL, fee = NULL, sent_at = NULL
             WHERE pending_payouts.send_started_at IS NULL",
            &[&address, &u64_to_i64(amount)?, &now_unix()],
        )?;
        Ok(())
    }

    pub fn mark_pending_payout_send_started(&self, address: &str) -> Result<Option<PendingPayout>> {
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;
        tx.execute(
            "UPDATE pending_payouts
             SET send_started_at = COALESCE(send_started_at, $2)
             WHERE address = $1",
            &[&address, &now_unix()],
        )?;
        let row = tx.query_opt(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             WHERE address = $1",
            &[&address],
        )?;
        tx.commit()?;
        Ok(row.map(|row| PendingPayout {
            address: row.get::<_, String>(0),
            amount: row.get::<_, i64>(1).max(0) as u64,
            initiated_at: from_unix(row.get::<_, i64>(2)),
            send_started_at: row.get::<_, Option<i64>>(3).map(from_unix),
            tx_hash: row.get(4),
            fee: row.get::<_, Option<i64>>(5).map(|v| v.max(0) as u64),
            sent_at: row.get::<_, Option<i64>>(6).map(from_unix),
        }))
    }

    pub fn record_pending_payout_broadcast(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
    ) -> Result<PendingPayout> {
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;

        let pending = tx.query_opt(
            "SELECT amount, tx_hash, fee FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        let Some(pending_row) = pending else {
            return Err(anyhow!("no pending payout for {address}"));
        };
        let pending_amount = pending_row.get::<_, i64>(0).max(0) as u64;
        if pending_amount != amount {
            return Err(anyhow!(
                "pending payout amount mismatch: expected={}, requested={}",
                pending_amount,
                amount
            ));
        }
        let existing_tx_hash = pending_row.get::<_, Option<String>>(1);
        if let Some(existing_tx_hash) = existing_tx_hash.as_deref() {
            if existing_tx_hash != tx_hash {
                return Err(anyhow!(
                    "pending payout tx mismatch: expected={}, requested={}",
                    existing_tx_hash,
                    tx_hash
                ));
            }
        }
        let existing_fee_raw = pending_row.get::<_, Option<i64>>(2);
        if let Some(existing_fee_raw) = existing_fee_raw {
            let existing_fee = existing_fee_raw.max(0) as u64;
            if existing_fee != fee {
                return Err(anyhow!(
                    "pending payout fee mismatch: expected={}, requested={}",
                    existing_fee,
                    fee
                ));
            }
        }

        tx.execute(
            "UPDATE pending_payouts
             SET send_started_at = COALESCE(send_started_at, $2),
                 tx_hash = $3,
                 fee = $4,
                 sent_at = COALESCE(sent_at, $2)
             WHERE address = $1",
            &[&address, &now_unix(), &tx_hash, &u64_to_i64(fee)?],
        )?;

        let row = tx.query_one(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             WHERE address = $1",
            &[&address],
        )?;
        tx.commit()?;
        Ok(PendingPayout {
            address: row.get::<_, String>(0),
            amount: row.get::<_, i64>(1).max(0) as u64,
            initiated_at: from_unix(row.get::<_, i64>(2)),
            send_started_at: row.get::<_, Option<i64>>(3).map(from_unix),
            tx_hash: row.get(4),
            fee: row.get::<_, Option<i64>>(5).map(|v| v.max(0) as u64),
            sent_at: row.get::<_, Option<i64>>(6).map(from_unix),
        })
    }

    pub fn reset_pending_payout_send_state(&self, address: &str) -> Result<()> {
        self.conn().lock().execute(
            "UPDATE pending_payouts
             SET send_started_at = NULL,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL
             WHERE address = $1",
            &[&address],
        )?;
        Ok(())
    }

    pub fn complete_pending_payout(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
    ) -> Result<()> {
        let mut conn = self.conn().lock();
        let mut tx = conn.transaction()?;

        let pending = tx.query_opt(
            "SELECT amount, initiated_at, send_started_at, sent_at, tx_hash, fee
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

        let row = tx.query_opt(
            "SELECT address, pending, paid FROM balances WHERE address = $1",
            &[&address],
        )?;
        let mut bal = if let Some(row) = row {
            Balance {
                address: row.get::<_, String>(0),
                pending: row.get::<_, i64>(1).max(0) as u64,
                paid: row.get::<_, i64>(2).max(0) as u64,
            }
        } else {
            Balance {
                address: address.to_string(),
                pending: 0,
                paid: 0,
            }
        };
        if bal.pending < amount {
            return Err(anyhow!("insufficient balance"));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid overflow"))?;

        tx.execute(
            "INSERT INTO balances (address, pending, paid) VALUES ($1, $2, $3)
             ON CONFLICT(address) DO UPDATE SET pending = EXCLUDED.pending, paid = EXCLUDED.paid",
            &[
                &bal.address,
                &u64_to_i64(bal.pending)?,
                &u64_to_i64(bal.paid)?,
            ],
        )?;

        tx.execute(
            "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES ($1, $2, $3, $4, $5)",
            &[
                &address,
                &u64_to_i64(amount)?,
                &u64_to_i64(fee)?,
                &tx_hash,
                &to_unix(sent_at.or(send_started_at).unwrap_or(initiated_at)),
            ],
        )?;

        tx.execute(
            "DELETE FROM pending_payouts WHERE address = $1",
            &[&address],
        )?;
        tx.commit()?;
        Ok(())
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
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             ORDER BY initiated_at ASC",
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|row| PendingPayout {
                address: row.get::<_, String>(0),
                amount: row.get::<_, i64>(1).max(0) as u64,
                initiated_at: from_unix(row.get::<_, i64>(2)),
                send_started_at: row.get::<_, Option<i64>>(3).map(from_unix),
                tx_hash: row.get(4),
                fee: row.get::<_, Option<i64>>(5).map(|v| v.max(0) as u64),
                sent_at: row.get::<_, Option<i64>>(6).map(from_unix),
            })
            .collect())
    }

    pub fn get_pending_payout(&self, address: &str) -> Result<Option<PendingPayout>> {
        let row = self.conn().lock().query_opt(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             WHERE address = $1",
            &[&address],
        )?;
        Ok(row.map(|row| PendingPayout {
            address: row.get::<_, String>(0),
            amount: row.get::<_, i64>(1).max(0) as u64,
            initiated_at: from_unix(row.get::<_, i64>(2)),
            send_started_at: row.get::<_, Option<i64>>(3).map(from_unix),
            tx_hash: row.get(4),
            fee: row.get::<_, Option<i64>>(5).map(|v| v.max(0) as u64),
            sent_at: row.get::<_, Option<i64>>(6).map(from_unix),
        }))
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
        now: SystemTime,
    ) -> Result<LoadedValidationState> {
        let rows = self.conn().lock().query(
            "SELECT address, total_shares, sampled_shares, invalid_samples, forced_until, last_seen_at
             FROM validation_address_states
             WHERE last_seen_at >= $1 OR (forced_until IS NOT NULL AND forced_until > $2)",
            &[&to_unix(state_cutoff), &to_unix(now)],
        )?;
        let states = rows
            .iter()
            .map(|row| PersistedValidationAddressState {
                address: row.get::<_, String>(0),
                total_shares: row.get::<_, i64>(1).max(0) as u64,
                sampled_shares: row.get::<_, i64>(2).max(0) as u64,
                invalid_samples: row.get::<_, i64>(3).max(0) as u64,
                forced_until: row.get::<_, Option<i64>>(4).map(from_unix),
                last_seen_at: from_unix(row.get::<_, i64>(5)),
            })
            .collect::<Vec<_>>();

        let provisional_rows = self.conn().lock().query(
            "SELECT address, created_at
             FROM validation_provisionals
             WHERE created_at > $1
             ORDER BY address ASC, created_at ASC",
            &[&to_unix(provisional_cutoff)],
        )?;
        let provisionals = provisional_rows
            .iter()
            .map(|row| PersistedValidationProvisional {
                address: row.get::<_, String>(0),
                created_at: from_unix(row.get::<_, i64>(1)),
            })
            .collect::<Vec<_>>();

        Ok(LoadedValidationState {
            states,
            provisionals,
        })
    }

    pub fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO validation_address_states (
                address, total_shares, sampled_shares, invalid_samples, forced_until, last_seen_at
             ) VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT(address) DO UPDATE SET
                total_shares = EXCLUDED.total_shares,
                sampled_shares = EXCLUDED.sampled_shares,
                invalid_samples = EXCLUDED.invalid_samples,
                forced_until = EXCLUDED.forced_until,
                last_seen_at = EXCLUDED.last_seen_at",
            &[
                &state.address,
                &u64_to_i64(state.total_shares)?,
                &u64_to_i64(state.sampled_shares)?,
                &u64_to_i64(state.invalid_samples)?,
                &state.forced_until.map(to_unix),
                &to_unix(state.last_seen_at),
            ],
        )?;
        Ok(())
    }

    pub fn add_validation_provisional(&self, address: &str, created_at: SystemTime) -> Result<()> {
        self.conn().lock().execute(
            "INSERT INTO validation_provisionals (address, created_at) VALUES ($1, $2)",
            &[&address, &to_unix(created_at)],
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
               AND NOT EXISTS (
                   SELECT 1 FROM validation_provisionals vp
                   WHERE vp.address = validation_address_states.address
               )",
            &[&to_unix(state_cutoff), &to_unix(now)],
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
        )?;
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
        )?;
        let removed = tx.execute("DELETE FROM shares WHERE created_at < $1", &[&cutoff])?;
        tx.execute(
            "DELETE FROM share_job_replays replay
             WHERE NOT EXISTS (
                 SELECT 1 FROM shares share
                 WHERE share.job_id = replay.job_id
             )",
            &[],
        )?;
        tx.commit()?;
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
             GROUP BY day_start
             ON CONFLICT(day_start) DO UPDATE SET
                payout_count = payout_daily_summaries.payout_count + EXCLUDED.payout_count,
                total_amount = payout_daily_summaries.total_amount + EXCLUDED.total_amount,
                total_fee = payout_daily_summaries.total_fee + EXCLUDED.total_fee,
                unique_recipients = payout_daily_summaries.unique_recipients + EXCLUDED.unique_recipients",
            &[&cutoff, &SECONDS_PER_DAY],
        )?;
        let removed = tx.execute("DELETE FROM payouts WHERE timestamp < $1", &[&cutoff])?;
        tx.commit()?;
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
        created_at: from_unix(row.get::<_, i64>(9)),
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
    })
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
}
