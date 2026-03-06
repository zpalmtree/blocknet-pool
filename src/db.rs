use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

use crate::engine::{ShareRecord, ShareStore};
use crate::stats::RejectionReasonCount;
use crate::validation::{
    LoadedValidationState, PersistedValidationAddressState, PersistedValidationProvisional,
};

const SHARE_CLAIM_EXPIRY_SECS: i64 = 2 * 60;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

#[derive(Debug, Clone, Serialize)]
pub struct StatSnapshot {
    pub id: i64,
    pub timestamp: SystemTime,
    pub hashrate: f64,
    pub miners: i32,
    pub workers: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbShare {
    pub id: i64,
    pub job_id: String,
    pub miner: String,
    pub worker: String,
    pub difficulty: u64,
    pub nonce: u64,
    pub status: String,
    pub was_sampled: bool,
    pub block_hash: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbBlock {
    pub height: u64,
    pub hash: String,
    pub difficulty: u64,
    pub finder: String,
    pub finder_worker: String,
    pub reward: u64,
    pub timestamp: SystemTime,
    pub confirmed: bool,
    pub orphaned: bool,
    pub paid_out: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Balance {
    pub address: String,
    pub pending: u64,
    pub paid: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Payout {
    pub id: i64,
    pub address: String,
    pub amount: u64,
    pub fee: u64,
    pub tx_hash: String,
    pub timestamp: SystemTime,
    pub confirmed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicPayoutBatch {
    pub total_amount: u64,
    pub total_fee: u64,
    pub recipient_count: usize,
    pub tx_hashes: Vec<String>,
    pub timestamp: SystemTime,
    pub confirmed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingPayout {
    pub address: String,
    pub amount: u64,
    pub initiated_at: SystemTime,
    #[serde(skip_serializing)]
    pub send_started_at: Option<SystemTime>,
    #[serde(skip_serializing)]
    pub tx_hash: Option<String>,
    #[serde(skip_serializing)]
    pub fee: Option<u64>,
    #[serde(skip_serializing)]
    pub sent_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolFeeEvent {
    pub id: i64,
    pub block_height: u64,
    pub amount: u64,
    pub fee_address: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PoolFeeRecord {
    pub amount: u64,
    pub fee_address: String,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AddressRiskState {
    pub address: String,
    pub strikes: u64,
    pub last_reason: Option<String>,
    pub last_event_at: Option<SystemTime>,
    pub quarantined_until: Option<SystemTime>,
    pub force_verify_until: Option<SystemTime>,
}

#[derive(Debug)]
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: &str) -> Result<Arc<Self>> {
        ensure_sqlite_compatible_path(path)?;
        let conn = Connection::open(path).with_context(|| format!("open sqlite {path}"))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("set WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("set synchronous")?;

        let store = Arc::new(Self {
            conn: Mutex::new(conn),
        });
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<()> {
        let sql = r#"
CREATE TABLE IF NOT EXISTS shares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    miner TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty INTEGER NOT NULL,
    nonce INTEGER NOT NULL,
    status TEXT NOT NULL,
    was_sampled INTEGER NOT NULL,
    block_hash TEXT,
    reject_reason TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shares_created_at ON shares(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_created ON shares(miner, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_miner_status_created ON shares(miner, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shares_job_nonce ON shares(job_id, nonce);

CREATE TABLE IF NOT EXISTS blocks (
    height INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    difficulty INTEGER NOT NULL,
    finder TEXT NOT NULL,
    finder_worker TEXT NOT NULL,
    reward INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    confirmed INTEGER NOT NULL,
    orphaned INTEGER NOT NULL,
    paid_out INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_finder ON blocks(finder);
CREATE INDEX IF NOT EXISTS idx_blocks_finder_height ON blocks(finder, height DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_unconfirmed ON blocks(confirmed, orphaned, height ASC);

CREATE TABLE IF NOT EXISTS balances (
    address TEXT PRIMARY KEY,
    pending INTEGER NOT NULL,
    paid INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    amount INTEGER NOT NULL,
    fee INTEGER NOT NULL DEFAULT 0,
    tx_hash TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payouts_timestamp ON payouts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_address_timestamp ON payouts(address, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payouts_tx_hash ON payouts(tx_hash);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS seen_shares (
    job_id TEXT NOT NULL,
    nonce INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, nonce)
);
CREATE INDEX IF NOT EXISTS idx_seen_shares_expiry ON seen_shares(expires_at);

CREATE TABLE IF NOT EXISTS pending_payouts (
    address TEXT PRIMARY KEY,
    amount INTEGER NOT NULL,
    initiated_at INTEGER NOT NULL,
    send_started_at INTEGER,
    tx_hash TEXT,
    fee INTEGER,
    sent_at INTEGER
);

CREATE TABLE IF NOT EXISTS pool_fee_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    block_height INTEGER NOT NULL UNIQUE,
    amount INTEGER NOT NULL,
    fee_address TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_timestamp ON pool_fee_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pool_fee_events_fee_address ON pool_fee_events(fee_address);

CREATE TABLE IF NOT EXISTS address_risk (
    address TEXT PRIMARY KEY,
    strikes INTEGER NOT NULL,
    last_reason TEXT,
    last_event_at INTEGER,
    quarantined_until INTEGER,
    force_verify_until INTEGER
);

CREATE TABLE IF NOT EXISTS vardiff_hints (
    address TEXT NOT NULL,
    worker TEXT NOT NULL,
    difficulty INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (address, worker)
);
CREATE INDEX IF NOT EXISTS idx_vardiff_hints_updated_at ON vardiff_hints(updated_at DESC);

CREATE TABLE IF NOT EXISTS stat_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    hashrate REAL NOT NULL,
    miners INTEGER NOT NULL,
    workers INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_stat_snapshots_timestamp ON stat_snapshots(timestamp DESC);

CREATE TABLE IF NOT EXISTS validation_address_states (
    address TEXT PRIMARY KEY,
    total_shares INTEGER NOT NULL,
    sampled_shares INTEGER NOT NULL,
    invalid_samples INTEGER NOT NULL,
    forced_until INTEGER,
    last_seen_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_address_states_last_seen
    ON validation_address_states(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_address_states_forced_until
    ON validation_address_states(forced_until DESC);

CREATE TABLE IF NOT EXISTS validation_provisionals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_provisionals_created_at
    ON validation_provisionals(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_provisionals_address_created_at
    ON validation_provisionals(address, created_at DESC);

CREATE TABLE IF NOT EXISTS share_daily_summaries (
    day_start INTEGER PRIMARY KEY,
    accepted_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    accepted_difficulty INTEGER NOT NULL,
    unique_miners INTEGER NOT NULL,
    unique_workers INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_share_daily_summaries_day_start
    ON share_daily_summaries(day_start DESC);

CREATE TABLE IF NOT EXISTS share_rejection_reason_daily_summaries (
    day_start INTEGER NOT NULL,
    reason TEXT NOT NULL,
    rejected_count INTEGER NOT NULL,
    PRIMARY KEY (day_start, reason)
);
CREATE INDEX IF NOT EXISTS idx_share_rejection_reason_daily_summaries_day_start
    ON share_rejection_reason_daily_summaries(day_start DESC);

CREATE TABLE IF NOT EXISTS payout_daily_summaries (
    day_start INTEGER PRIMARY KEY,
    payout_count INTEGER NOT NULL,
    total_amount INTEGER NOT NULL,
    total_fee INTEGER NOT NULL,
    unique_recipients INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payout_daily_summaries_day_start
    ON payout_daily_summaries(day_start DESC);
"#;

        self.conn.lock().execute_batch(sql).context("init schema")?;
        self.ensure_payout_fee_column()?;
        self.ensure_share_reject_reason_column()?;
        self.ensure_pending_payout_columns()?;
        Ok(())
    }

    fn ensure_payout_fee_column(&self) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("PRAGMA table_info(payouts)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for row in rows {
            if row?.eq_ignore_ascii_case("fee") {
                return Ok(());
            }
        }
        conn.execute(
            "ALTER TABLE payouts ADD COLUMN fee INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
        Ok(())
    }

    fn ensure_share_reject_reason_column(&self) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("PRAGMA table_info(shares)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for row in rows {
            if row?.eq_ignore_ascii_case("reject_reason") {
                return Ok(());
            }
        }
        conn.execute("ALTER TABLE shares ADD COLUMN reject_reason TEXT", [])?;
        Ok(())
    }

    fn ensure_pending_payout_columns(&self) -> Result<()> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("PRAGMA table_info(pending_payouts)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        let mut present = HashMap::<String, bool>::new();
        for row in rows {
            present.insert(row?.to_ascii_lowercase(), true);
        }
        if !present.contains_key("send_started_at") {
            conn.execute(
                "ALTER TABLE pending_payouts ADD COLUMN send_started_at INTEGER",
                [],
            )?;
        }
        if !present.contains_key("tx_hash") {
            conn.execute("ALTER TABLE pending_payouts ADD COLUMN tx_hash TEXT", [])?;
        }
        if !present.contains_key("fee") {
            conn.execute("ALTER TABLE pending_payouts ADD COLUMN fee INTEGER", [])?;
        }
        if !present.contains_key("sent_at") {
            conn.execute("ALTER TABLE pending_payouts ADD COLUMN sent_at INTEGER", [])?;
        }
        Ok(())
    }

    pub fn add_share_immediate(&self, share: ShareRecord) -> Result<()> {
        self.add_share(share)
    }

    pub fn add_share(&self, share: ShareRecord) -> Result<()> {
        let created = to_unix(share.created_at);
        self.conn.lock().execute(
            "INSERT INTO shares (job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, reject_reason, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                share.job_id,
                share.miner,
                share.worker,
                u64_to_i64(share.difficulty)?,
                u64_to_i64(share.nonce)?,
                share.status,
                if share.was_sampled { 1i64 } else { 0i64 },
                share.block_hash,
                share.reject_reason,
                created,
            ],
        )?;
        Ok(())
    }

    pub fn get_recent_shares(&self, limit: i64) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE miner = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![address, limit], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_shares_since(&self, since: SystemTime) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at >= ?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![to_unix(since)], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_last_n_shares(&self, n: i64) -> Result<Vec<DbShare>> {
        self.get_recent_shares(n)
    }

    pub fn get_shares_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at >= ?1 AND created_at <= ?2",
        )?;
        let rows = stmt.query_map(params![to_unix(start), to_unix(end)], row_to_share)?;
        collect_rows(rows)
    }

    pub fn get_last_n_shares_before(&self, before: SystemTime, n: i64) -> Result<Vec<DbShare>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, miner, worker, difficulty, nonce, status, was_sampled, block_hash, created_at
             FROM shares WHERE created_at <= ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![to_unix(before), n], row_to_share)?;
        collect_rows(rows)
    }

    /// Returns (total_difficulty, share_count, oldest_ts, newest_ts) for accepted
    /// shares from a specific miner within the given window.
    pub fn hashrate_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<(u64, u64, Option<SystemTime>, Option<SystemTime>)> {
        let conn = self.conn.lock();
        let row = conn.query_row(
            "SELECT COALESCE(SUM(difficulty),0), COUNT(*), MIN(created_at), MAX(created_at)
             FROM shares
             WHERE miner = ?1 AND created_at >= ?2
               AND status IN ('verified','provisional')",
            params![address, to_unix(since)],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )?;
        Ok((
            row.0.max(0) as u64,
            row.1.max(0) as u64,
            row.2.map(from_unix),
            row.3.map(from_unix),
        ))
    }

    /// Returns (total_difficulty, share_count, oldest_ts, newest_ts) for all
    /// accepted shares within the given window (pool-wide).
    pub fn hashrate_stats_pool(
        &self,
        since: SystemTime,
    ) -> Result<(u64, u64, Option<SystemTime>, Option<SystemTime>)> {
        let conn = self.conn.lock();
        let row = conn.query_row(
            "SELECT COALESCE(SUM(difficulty),0), COUNT(*), MIN(created_at), MAX(created_at)
             FROM shares
             WHERE created_at >= ?1
               AND status IN ('verified','provisional')",
            params![to_unix(since)],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )?;
        Ok((
            row.0.max(0) as u64,
            row.1.max(0) as u64,
            row.2.map(from_unix),
            row.3.map(from_unix),
        ))
    }

    pub fn get_total_share_count(&self) -> Result<u64> {
        let conn = self.conn.lock();
        let live_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM shares", [], |row| row.get(0))?;
        let summarized_count: i64 = conn.query_row(
            "SELECT COALESCE(SUM(accepted_count + rejected_count), 0) FROM share_daily_summaries",
            [],
            |row| row.get(0),
        )?;
        Ok((live_count.max(0) as u64).saturating_add(summarized_count.max(0) as u64))
    }

    pub fn share_outcome_counts_since(&self, since: SystemTime) -> Result<(u64, u64)> {
        let conn = self.conn.lock();
        let row = conn.query_row(
            "SELECT
                COALESCE(SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END), 0)
             FROM shares
             WHERE created_at >= ?1",
            params![to_unix(since)],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        Ok((row.0.max(0) as u64, row.1.max(0) as u64))
    }

    pub fn total_rejected_share_count(&self) -> Result<u64> {
        let conn = self.conn.lock();
        let live_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM shares WHERE status NOT IN ('verified','provisional')",
            [],
            |row| row.get(0),
        )?;
        let summarized_count: i64 = conn.query_row(
            "SELECT COALESCE(SUM(rejected_count), 0) FROM share_daily_summaries",
            [],
            |row| row.get(0),
        )?;
        Ok((live_count.max(0) as u64).saturating_add(summarized_count.max(0) as u64))
    }

    pub fn rejection_reason_counts_since(
        &self,
        since: SystemTime,
    ) -> Result<Vec<RejectionReasonCount>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT
                COALESCE(NULLIF(TRIM(reject_reason), ''), 'legacy / unknown') AS reason,
                COUNT(*) AS rejected_count
             FROM shares
             WHERE created_at >= ?1
               AND status NOT IN ('verified','provisional')
             GROUP BY reason
             ORDER BY rejected_count DESC, reason ASC",
        )?;
        let rows = stmt.query_map(params![to_unix(since)], |row| {
            Ok(RejectionReasonCount {
                reason: row.get(0)?,
                count: row.get::<_, i64>(1)?.max(0) as u64,
            })
        })?;
        collect_rows(rows)
    }

    pub fn total_rejection_reason_counts(&self) -> Result<Vec<RejectionReasonCount>> {
        let conn = self.conn.lock();
        let live = {
            let mut stmt = conn.prepare(
                "SELECT
                    COALESCE(NULLIF(TRIM(reject_reason), ''), 'legacy / unknown') AS reason,
                    COUNT(*) AS rejected_count
                 FROM shares
                 WHERE status NOT IN ('verified','provisional')
                 GROUP BY reason",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?.max(0) as u64,
                ))
            })?;
            collect_rows(rows)?
        };
        let summarized = {
            let mut stmt = conn.prepare(
                "SELECT reason, COALESCE(SUM(rejected_count), 0)
                 FROM share_rejection_reason_daily_summaries
                 GROUP BY reason",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?.max(0) as u64,
                ))
            })?;
            collect_rows(rows)?
        };

        Ok(sort_reason_counts(
            live.into_iter()
                .chain(summarized)
                .collect::<Vec<(String, u64)>>(),
        ))
    }

    pub fn add_block(&self, block: &DbBlock) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(height) DO UPDATE SET
                 hash=excluded.hash,
                 difficulty=excluded.difficulty,
                 finder=excluded.finder,
                 finder_worker=excluded.finder_worker,
                 reward=excluded.reward,
                 timestamp=excluded.timestamp,
                 confirmed=excluded.confirmed,
                 orphaned=excluded.orphaned,
                 paid_out=excluded.paid_out",
            params![
                u64_to_i64(block.height)?,
                block.hash,
                u64_to_i64(block.difficulty)?,
                block.finder,
                block.finder_worker,
                u64_to_i64(block.reward)?,
                to_unix(block.timestamp),
                if block.confirmed { 1i64 } else { 0i64 },
                if block.orphaned { 1i64 } else { 0i64 },
                if block.paid_out { 1i64 } else { 0i64 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_block_if_absent(&self, block: &DbBlock) -> Result<bool> {
        let inserted = self.conn.lock().execute(
            "INSERT INTO blocks (height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(height) DO NOTHING",
            params![
                u64_to_i64(block.height)?,
                block.hash,
                u64_to_i64(block.difficulty)?,
                block.finder,
                block.finder_worker,
                u64_to_i64(block.reward)?,
                to_unix(block.timestamp),
                if block.confirmed { 1i64 } else { 0i64 },
                if block.orphaned { 1i64 } else { 0i64 },
                if block.paid_out { 1i64 } else { 0i64 },
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn get_block(&self, height: u64) -> Result<Option<DbBlock>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE height = ?1",
            params![u64_to_i64(height)?],
            row_to_block,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn update_block(&self, block: &DbBlock) -> Result<()> {
        self.add_block(block)
    }

    pub fn get_recent_blocks(&self, limit: i64) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks ORDER BY height DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_block)?;
        collect_rows(rows)
    }

    pub fn get_all_blocks(&self) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks ORDER BY height DESC",
        )?;
        let rows = stmt.query_map([], row_to_block)?;
        collect_rows(rows)
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

        let conn = self.conn.lock();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM blocks
             WHERE (?1 IS NULL OR LOWER(finder) LIKE ?1)
               AND (
                   ?2 IS NULL
                   OR (?2 = 'confirmed' AND confirmed = 1 AND orphaned = 0)
                   OR (?2 = 'orphaned' AND orphaned = 1)
                   OR (?2 = 'pending' AND confirmed = 0 AND orphaned = 0)
                   OR (?2 = 'paid' AND paid_out = 1)
                   OR (?2 = 'unpaid' AND paid_out = 0)
               )",
            params![finder_pattern.as_deref(), status_filter],
            |row| row.get(0),
        )?;

        let sql = format!(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks
             WHERE (?1 IS NULL OR LOWER(finder) LIKE ?1)
               AND (
                   ?2 IS NULL
                   OR (?2 = 'confirmed' AND confirmed = 1 AND orphaned = 0)
                   OR (?2 = 'orphaned' AND orphaned = 1)
                   OR (?2 = 'pending' AND confirmed = 0 AND orphaned = 0)
                   OR (?2 = 'paid' AND paid_out = 1)
                   OR (?2 = 'unpaid' AND paid_out = 0)
               )
             ORDER BY {order_clause}
             LIMIT ?3 OFFSET ?4"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![
                finder_pattern.as_deref(),
                status_filter,
                limit.max(0),
                offset.max(0)
            ],
            row_to_block,
        )?;
        let items = collect_rows(rows)?;

        Ok((items, total.max(0) as u64))
    }

    pub fn get_unconfirmed_blocks(&self) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE confirmed = 0 AND orphaned = 0 ORDER BY height ASC",
        )?;
        let rows = stmt.query_map([], row_to_block)?;
        collect_rows(rows)
    }

    pub fn get_unpaid_blocks(&self) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE confirmed = 1 AND orphaned = 0 AND paid_out = 0 ORDER BY height ASC",
        )?;
        let rows = stmt.query_map([], row_to_block)?;
        collect_rows(rows)
    }

    pub fn get_block_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM blocks", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }

    /// Returns (confirmed_non_orphaned, orphaned, pending_non_orphaned).
    pub fn get_block_status_counts(&self) -> Result<(u64, u64, u64)> {
        let row: (i64, i64, i64) = self.conn.lock().query_row(
            "SELECT
                 COALESCE(SUM(CASE WHEN confirmed = 1 AND orphaned = 0 THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN orphaned = 1 THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN confirmed = 0 AND orphaned = 0 THEN 1 ELSE 0 END), 0)
             FROM blocks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok((
            row.0.max(0) as u64,
            row.1.max(0) as u64,
            row.2.max(0) as u64,
        ))
    }

    pub fn get_balance(&self, address: &str) -> Result<Balance> {
        let conn = self.conn.lock();
        if let Some(bal) = conn
            .query_row(
                "SELECT address, pending, paid FROM balances WHERE address = ?1",
                params![address],
                |row| {
                    Ok(Balance {
                        address: row.get::<_, String>(0)?,
                        pending: row.get::<_, i64>(1)?.max(0) as u64,
                        paid: row.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?
        {
            return Ok(bal);
        }
        Ok(Balance {
            address: address.to_string(),
            pending: 0,
            paid: 0,
        })
    }

    pub fn update_balance(&self, bal: &Balance) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO balances (address, pending, paid) VALUES (?1, ?2, ?3)
             ON CONFLICT(address) DO UPDATE SET pending = excluded.pending, paid = excluded.paid",
            params![bal.address, u64_to_i64(bal.pending)?, u64_to_i64(bal.paid)?],
        )?;
        Ok(())
    }

    pub fn credit_balance(&self, address: &str, amount: u64) -> Result<()> {
        if amount == 0 {
            return Ok(());
        }

        let mut bal = self.get_balance(address)?;
        bal.pending = bal
            .pending
            .checked_add(amount)
            .ok_or_else(|| anyhow!("balance overflow"))?;
        self.update_balance(&bal)
    }

    pub fn debit_balance(&self, address: &str, amount: u64) -> Result<()> {
        if amount == 0 {
            return Ok(());
        }

        let mut bal = self.get_balance(address)?;
        if bal.pending < amount {
            return Err(anyhow!(
                "insufficient balance: have={}, need={}",
                bal.pending,
                amount
            ));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid balance overflow"))?;
        self.update_balance(&bal)
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
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;

        let block_state: Option<(i64, i64, i64)> = tx
            .query_row(
                "SELECT confirmed, orphaned, paid_out FROM blocks WHERE height = ?1",
                params![u64_to_i64(block_height)?],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((confirmed, orphaned, paid_out)) = block_state else {
            return Err(anyhow!("block {block_height} not found"));
        };
        if paid_out != 0 {
            return Ok(false);
        }
        if confirmed == 0 || orphaned != 0 {
            return Err(anyhow!("block {block_height} is not eligible for payout"));
        }

        for (address, amount) in credits {
            let destination = address.trim();
            if destination.is_empty() || *amount == 0 {
                continue;
            }

            let mut bal = tx
                .query_row(
                    "SELECT address, pending, paid FROM balances WHERE address = ?1",
                    params![destination],
                    |row| {
                        Ok(Balance {
                            address: row.get::<_, String>(0)?,
                            pending: row.get::<_, i64>(1)?.max(0) as u64,
                            paid: row.get::<_, i64>(2)?.max(0) as u64,
                        })
                    },
                )
                .optional()?
                .unwrap_or_else(|| Balance {
                    address: destination.to_string(),
                    pending: 0,
                    paid: 0,
                });
            bal.pending = bal
                .pending
                .checked_add(*amount)
                .ok_or_else(|| anyhow!("balance overflow"))?;

            tx.execute(
                "INSERT INTO balances (address, pending, paid) VALUES (?1, ?2, ?3)
                 ON CONFLICT(address) DO UPDATE SET pending = excluded.pending, paid = excluded.paid",
                params![
                    bal.address,
                    u64_to_i64(bal.pending)?,
                    u64_to_i64(bal.paid)?,
                ],
            )?;
        }

        if let Some(fee) = fee_record {
            if fee.amount > 0 {
                let destination = fee.fee_address.trim();
                if destination.is_empty() {
                    return Err(anyhow!("fee address is required"));
                }
                tx.execute(
                    "INSERT OR IGNORE INTO pool_fee_events (block_height, amount, fee_address, timestamp)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        u64_to_i64(block_height)?,
                        u64_to_i64(fee.amount)?,
                        destination,
                        to_unix(fee.timestamp)
                    ],
                )?;
            }
        }

        let updated = tx.execute(
            "UPDATE blocks SET paid_out = 1 WHERE height = ?1 AND paid_out = 0",
            params![u64_to_i64(block_height)?],
        )?;
        if updated == 0 {
            return Ok(false);
        }

        tx.commit()?;
        Ok(true)
    }

    pub fn get_all_balances(&self) -> Result<Vec<Balance>> {
        let conn = self.conn.lock();
        let mut stmt =
            conn.prepare("SELECT address, pending, paid FROM balances ORDER BY pending DESC")?;
        let rows = stmt.query_map([], |row| {
            Ok(Balance {
                address: row.get::<_, String>(0)?,
                pending: row.get::<_, i64>(1)?.max(0) as u64,
                paid: row.get::<_, i64>(2)?.max(0) as u64,
            })
        })?;
        collect_rows(rows)
    }

    pub fn add_payout(&self, address: &str, amount: u64, fee: u64, tx_hash: &str) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![address, u64_to_i64(amount)?, u64_to_i64(fee)?, tx_hash, now_unix()],
        )?;
        Ok(())
    }

    pub fn get_recent_payouts(&self, limit: i64) -> Result<Vec<Payout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
             FROM payouts
             ORDER BY id DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(Payout {
                id: row.get(0)?,
                address: row.get(1)?,
                amount: row.get::<_, i64>(2)?.max(0) as u64,
                fee: row.get::<_, i64>(3)?.max(0) as u64,
                tx_hash: row.get(4)?,
                timestamp: from_unix(row.get::<_, i64>(5)?),
                confirmed: row.get::<_, i64>(6)? != 0,
            })
        })?;
        collect_rows(rows)
    }

    pub fn get_recent_payouts_for_address(&self, address: &str, limit: i64) -> Result<Vec<Payout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
             FROM payouts
             WHERE address = ?1
             ORDER BY id DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![address, limit], |row| {
            Ok(Payout {
                id: row.get(0)?,
                address: row.get(1)?,
                amount: row.get::<_, i64>(2)?.max(0) as u64,
                fee: row.get::<_, i64>(3)?.max(0) as u64,
                tx_hash: row.get(4)?,
                timestamp: from_unix(row.get::<_, i64>(5)?),
                confirmed: row.get::<_, i64>(6)? != 0,
            })
        })?;
        collect_rows(rows)
    }

    pub fn get_recent_visible_payouts_for_address(
        &self,
        address: &str,
        limit: i64,
    ) -> Result<Vec<Payout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, address, amount, fee, tx_hash, timestamp, confirmed
             FROM (
                 SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
                 FROM payouts
                 WHERE address = ?1
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
                 WHERE address = ?1
                   AND tx_hash IS NOT NULL
                   AND TRIM(tx_hash) <> ''
             )
             ORDER BY timestamp DESC, confirmed ASC, id DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![address, limit], |row| {
            Ok(Payout {
                id: row.get(0)?,
                address: row.get(1)?,
                amount: row.get::<_, i64>(2)?.max(0) as u64,
                fee: row.get::<_, i64>(3)?.max(0) as u64,
                tx_hash: row.get(4)?,
                timestamp: from_unix(row.get::<_, i64>(5)?),
                confirmed: row.get::<_, i64>(6)? != 0,
            })
        })?;
        collect_rows(rows)
    }

    pub fn get_all_payouts(&self) -> Result<Vec<Payout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, address, amount, fee, tx_hash, timestamp, 1 AS confirmed
             FROM payouts
             ORDER BY id DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Payout {
                id: row.get(0)?,
                address: row.get(1)?,
                amount: row.get::<_, i64>(2)?.max(0) as u64,
                fee: row.get::<_, i64>(3)?.max(0) as u64,
                tx_hash: row.get(4)?,
                timestamp: from_unix(row.get::<_, i64>(5)?),
                confirmed: row.get::<_, i64>(6)? != 0,
            })
        })?;
        collect_rows(rows)
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

        let conn = self.conn.lock();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM (
                 SELECT address, tx_hash
                 FROM payouts
                 UNION ALL
                 SELECT address, tx_hash
                 FROM pending_payouts
                 WHERE tx_hash IS NOT NULL
                   AND TRIM(tx_hash) <> ''
             ) visible
             WHERE (?1 IS NULL OR LOWER(address) LIKE ?1)
               AND (?2 IS NULL OR LOWER(tx_hash) LIKE ?2)",
            params![address_pattern.as_deref(), tx_hash_pattern.as_deref()],
            |row| row.get(0),
        )?;

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
                     COALESCE(fee, 0) AS fee,
                     tx_hash,
                     COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                     0 AS confirmed
                 FROM pending_payouts
                 WHERE tx_hash IS NOT NULL
                   AND TRIM(tx_hash) <> ''
             ) visible
             WHERE (?1 IS NULL OR LOWER(address) LIKE ?1)
               AND (?2 IS NULL OR LOWER(tx_hash) LIKE ?2)
             ORDER BY {order_clause}
             LIMIT ?3 OFFSET ?4"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![
                address_pattern.as_deref(),
                tx_hash_pattern.as_deref(),
                limit.max(0),
                offset.max(0)
            ],
            |row| {
                Ok(Payout {
                    id: row.get(0)?,
                    address: row.get(1)?,
                    amount: row.get::<_, i64>(2)?.max(0) as u64,
                    fee: row.get::<_, i64>(3)?.max(0) as u64,
                    tx_hash: row.get(4)?,
                    timestamp: from_unix(row.get::<_, i64>(5)?),
                    confirmed: row.get::<_, i64>(6)? != 0,
                })
            },
        )?;
        let items = collect_rows(rows)?;

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

        let conn = self.conn.lock();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM (
                 SELECT (timestamp / 300) AS bucket
                 FROM (
                     SELECT timestamp FROM payouts
                     UNION ALL
                     SELECT COALESCE(sent_at, send_started_at, initiated_at) AS timestamp
                     FROM pending_payouts
                     WHERE tx_hash IS NOT NULL
                       AND TRIM(tx_hash) <> ''
                 ) visible
                 GROUP BY bucket
             )",
            [],
            |row| row.get(0),
        )?;

        let sql = format!(
            "WITH visible AS (
                SELECT amount, fee, tx_hash, timestamp, 1 AS confirmed
                FROM payouts
                UNION ALL
                SELECT
                    amount,
                    COALESCE(fee, 0) AS fee,
                    tx_hash,
                    COALESCE(sent_at, send_started_at, initiated_at) AS timestamp,
                    0 AS confirmed
                FROM pending_payouts
                WHERE tx_hash IS NOT NULL
                  AND TRIM(tx_hash) <> ''
             ),
             grouped AS (
                SELECT
                    (timestamp / 300) AS bucket,
                    SUM(amount) AS total_amount,
                    SUM(fee) AS total_fee,
                    COUNT(*) AS recipient_count,
                    GROUP_CONCAT(tx_hash) AS tx_hashes,
                    MAX(timestamp) AS batch_ts,
                    MIN(confirmed) AS confirmed
                FROM visible
                GROUP BY bucket
             )
             SELECT total_amount, total_fee, recipient_count, tx_hashes, batch_ts, confirmed
             FROM grouped
             ORDER BY {order_clause}
             LIMIT ?1 OFFSET ?2"
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![limit.max(0), offset.max(0)], |row| {
            let tx_hashes: Option<String> = row.get(3)?;
            Ok(PublicPayoutBatch {
                total_amount: row.get::<_, i64>(0)?.max(0) as u64,
                total_fee: row.get::<_, i64>(1)?.max(0) as u64,
                recipient_count: row.get::<_, i64>(2)?.max(0) as usize,
                tx_hashes: tx_hashes
                    .unwrap_or_default()
                    .split(',')
                    .filter(|v| !v.trim().is_empty())
                    .map(|v| v.to_string())
                    .collect(),
                timestamp: from_unix(row.get::<_, i64>(4)?),
                confirmed: row.get::<_, i64>(5)? != 0,
            })
        })?;
        let items = collect_rows(rows)?;

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

        let inserted = self.conn.lock().execute(
            "INSERT OR IGNORE INTO pool_fee_events (block_height, amount, fee_address, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                u64_to_i64(block_height)?,
                u64_to_i64(amount)?,
                destination,
                to_unix(timestamp)
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn get_total_pool_fees(&self) -> Result<u64> {
        let total: Option<i64> =
            self.conn
                .lock()
                .query_row("SELECT SUM(amount) FROM pool_fee_events", [], |row| {
                    row.get(0)
                })?;
        Ok(total.unwrap_or(0).max(0) as u64)
    }

    pub fn get_recent_pool_fees(&self, limit: i64) -> Result<Vec<PoolFeeEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_pool_fee_event)?;
        collect_rows(rows)
    }

    pub fn get_all_pool_fees(&self) -> Result<Vec<PoolFeeEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events ORDER BY id DESC",
        )?;
        let rows = stmt.query_map([], row_to_pool_fee_event)?;
        collect_rows(rows)
    }

    pub fn get_pool_fees_page(
        &self,
        fee_address: Option<&str>,
        sort: &str,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<PoolFeeEvent>, u64)> {
        let fee_address_pattern = fee_address
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|v| format!("%{}%", v.to_ascii_lowercase()));

        let order_clause = match sort {
            "time_asc" => "timestamp ASC, id ASC",
            "amount_desc" => "amount DESC, id DESC",
            "amount_asc" => "amount ASC, id DESC",
            "height_asc" => "block_height ASC, id ASC",
            "height_desc" => "block_height DESC, id DESC",
            _ => "timestamp DESC, id DESC",
        };

        let conn = self.conn.lock();
        let total: i64 = conn.query_row(
            "SELECT COUNT(*)
             FROM pool_fee_events
             WHERE (?1 IS NULL OR LOWER(fee_address) LIKE ?1)",
            params![fee_address_pattern.as_deref()],
            |row| row.get(0),
        )?;

        let sql = format!(
            "SELECT id, block_height, amount, fee_address, timestamp
             FROM pool_fee_events
             WHERE (?1 IS NULL OR LOWER(fee_address) LIKE ?1)
             ORDER BY {order_clause}
             LIMIT ?2 OFFSET ?3"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![fee_address_pattern.as_deref(), limit.max(0), offset.max(0)],
            row_to_pool_fee_event,
        )?;
        let items = collect_rows(rows)?;

        Ok((items, total.max(0) as u64))
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        let value = conn
            .query_row(
                "SELECT value FROM meta WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value)
    }

    pub fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        let expires_at = now_unix() + SHARE_CLAIM_EXPIRY_SECS;
        self.conn.lock().execute(
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = excluded.expires_at",
            params![job_id, u64_to_i64(nonce)?, expires_at],
        )?;
        Ok(())
    }

    pub fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        if self.has_persisted_share(job_id, nonce)? {
            return Ok(false);
        }
        let now = now_unix();
        let expires_at = now + SHARE_CLAIM_EXPIRY_SECS;
        let claimed = self.conn.lock().execute(
            "INSERT INTO seen_shares (job_id, nonce, expires_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(job_id, nonce) DO UPDATE SET expires_at = excluded.expires_at
             WHERE seen_shares.expires_at <= ?4",
            params![job_id, u64_to_i64(nonce)?, expires_at, now],
        )?;
        Ok(claimed > 0)
    }

    pub fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.conn.lock().execute(
            "DELETE FROM seen_shares WHERE job_id = ?1 AND nonce = ?2",
            params![job_id, u64_to_i64(nonce)?],
        )?;
        Ok(())
    }

    pub fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        if self.has_persisted_share(job_id, nonce)? {
            return Ok(true);
        }
        let now = now_unix();
        let expiry: Option<i64> = self
            .conn
            .lock()
            .query_row(
                "SELECT expires_at FROM seen_shares WHERE job_id = ?1 AND nonce = ?2",
                params![job_id, u64_to_i64(nonce)?],
                |row| row.get(0),
            )
            .optional()?;

        Ok(expiry.is_some_and(|ts| ts > now))
    }

    fn has_persisted_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        let exists = self
            .conn
            .lock()
            .query_row(
                "SELECT 1 FROM shares WHERE job_id = ?1 AND nonce = ?2 LIMIT 1",
                params![job_id, u64_to_i64(nonce)?],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        Ok(exists.is_some())
    }

    pub fn clean_expired_seen_shares(&self) -> Result<u64> {
        let now = now_unix();
        let affected = self.conn.lock().execute(
            "DELETE FROM seen_shares WHERE expires_at <= ?1",
            params![now],
        )?;
        Ok(affected as u64)
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT address, strikes, last_reason, last_event_at, quarantined_until, force_verify_until
             FROM address_risk WHERE address = ?1",
            params![address],
            |row| {
                Ok(AddressRiskState {
                    address: row.get(0)?,
                    strikes: row.get::<_, i64>(1)?.max(0) as u64,
                    last_reason: row.get(2)?,
                    last_event_at: row.get::<_, Option<i64>>(3)?.map(from_unix),
                    quarantined_until: row.get::<_, Option<i64>>(4)?.map(from_unix),
                    force_verify_until: row.get::<_, Option<i64>>(5)?.map(from_unix),
                })
            },
        )
        .optional()
        .map_err(Into::into)
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
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
        apply_quarantine: bool,
    ) -> Result<AddressRiskState> {
        if address.trim().is_empty() {
            return Err(anyhow!("address is required"));
        }

        let now = SystemTime::now();
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO address_risk (address, strikes, last_reason, last_event_at, quarantined_until, force_verify_until)
             VALUES (?1, 0, NULL, NULL, NULL, NULL)
             ON CONFLICT(address) DO NOTHING",
            params![address],
        )?;
        tx.execute(
            "UPDATE address_risk
             SET strikes = strikes + 1,
                 last_reason = ?2,
                 last_event_at = ?3
             WHERE address = ?1",
            params![address, reason, to_unix(now)],
        )?;

        let (strikes_raw, existing_quarantine_raw, existing_force_raw): (
            i64,
            Option<i64>,
            Option<i64>,
        ) = tx.query_row(
            "SELECT strikes, quarantined_until, force_verify_until FROM address_risk WHERE address = ?1",
            params![address],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let strikes = strikes_raw.max(0) as u64;
        let existing_quarantine = existing_quarantine_raw.map(from_unix);
        let existing_force = existing_force_raw.map(from_unix);

        let force_verify_until = merge_optional_later(
            existing_force,
            if force_verify_duration.is_zero() {
                None
            } else {
                Some(now + force_verify_duration)
            },
        );
        let quarantined_until = merge_optional_later(
            existing_quarantine,
            if apply_quarantine && !quarantine_base.is_zero() {
                Some(quarantine_until_for_strikes(
                    now,
                    strikes,
                    quarantine_base,
                    quarantine_max,
                ))
            } else {
                None
            },
        );

        tx.execute(
            "UPDATE address_risk
             SET quarantined_until = ?2,
                 force_verify_until = ?3
             WHERE address = ?1",
            params![
                address,
                quarantined_until.map(to_unix),
                force_verify_until.map(to_unix),
            ],
        )?;
        tx.commit()?;

        Ok(AddressRiskState {
            address: address.to_string(),
            strikes,
            last_reason: Some(reason.to_string()),
            last_event_at: Some(now),
            quarantined_until,
            force_verify_until,
        })
    }

    pub fn get_risk_summary(&self) -> Result<(u64, u64)> {
        let now = now_unix();
        let conn = self.conn.lock();

        let quarantined: i64 = conn.query_row(
            "SELECT COUNT(*) FROM address_risk WHERE quarantined_until IS NOT NULL AND quarantined_until > ?1",
            params![now],
            |row| row.get(0),
        )?;

        let forced: i64 = conn.query_row(
            "SELECT COUNT(*) FROM address_risk
             WHERE (force_verify_until IS NOT NULL AND force_verify_until > ?1)
                OR (quarantined_until IS NOT NULL AND quarantined_until > ?1)",
            params![now],
            |row| row.get(0),
        )?;

        Ok((quarantined.max(0) as u64, forced.max(0) as u64))
    }

    pub fn create_pending_payout(&self, address: &str, amount: u64) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO pending_payouts (address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at)
             VALUES (?1, ?2, ?3, NULL, NULL, NULL, NULL)
             ON CONFLICT(address) DO UPDATE SET amount = excluded.amount, tx_hash = NULL, fee = NULL, sent_at = NULL
             WHERE pending_payouts.send_started_at IS NULL",
            params![address, u64_to_i64(amount)?, now_unix()],
        )?;
        Ok(())
    }

    pub fn mark_pending_payout_send_started(&self, address: &str) -> Result<Option<PendingPayout>> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE pending_payouts
             SET send_started_at = COALESCE(send_started_at, ?2)
             WHERE address = ?1",
            params![address, now_unix()],
        )?;
        let pending = tx
            .query_row(
                "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
                 FROM pending_payouts
                 WHERE address = ?1",
                params![address],
                |row| {
                    Ok(PendingPayout {
                        address: row.get(0)?,
                        amount: row.get::<_, i64>(1)?.max(0) as u64,
                        initiated_at: from_unix(row.get::<_, i64>(2)?),
                        send_started_at: row.get::<_, Option<i64>>(3)?.map(from_unix),
                        tx_hash: row.get(4)?,
                        fee: row.get::<_, Option<i64>>(5)?.map(|v| v.max(0) as u64),
                        sent_at: row.get::<_, Option<i64>>(6)?.map(from_unix),
                    })
                },
            )
            .optional()?;
        tx.commit()?;
        Ok(pending)
    }

    pub fn record_pending_payout_broadcast(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
    ) -> Result<PendingPayout> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;

        let pending: Option<(i64, Option<String>, Option<i64>)> = tx
            .query_row(
                "SELECT amount, tx_hash, fee FROM pending_payouts WHERE address = ?1",
                params![address],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((pending_amount_raw, existing_tx_hash, existing_fee_raw)) = pending else {
            return Err(anyhow!("no pending payout for {address}"));
        };
        let pending_amount = pending_amount_raw.max(0) as u64;
        if pending_amount != amount {
            return Err(anyhow!(
                "pending payout amount mismatch: expected={}, requested={}",
                pending_amount,
                amount
            ));
        }
        if let Some(existing_tx_hash) = existing_tx_hash.as_deref() {
            if existing_tx_hash != tx_hash {
                return Err(anyhow!(
                    "pending payout tx mismatch: expected={}, requested={}",
                    existing_tx_hash,
                    tx_hash
                ));
            }
        }
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
             SET send_started_at = COALESCE(send_started_at, ?2),
                 tx_hash = ?3,
                 fee = ?4,
                 sent_at = COALESCE(sent_at, ?2)
             WHERE address = ?1",
            params![address, now_unix(), tx_hash, u64_to_i64(fee)?],
        )?;

        let pending = tx.query_row(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             WHERE address = ?1",
            params![address],
            |row| {
                Ok(PendingPayout {
                    address: row.get(0)?,
                    amount: row.get::<_, i64>(1)?.max(0) as u64,
                    initiated_at: from_unix(row.get::<_, i64>(2)?),
                    send_started_at: row.get::<_, Option<i64>>(3)?.map(from_unix),
                    tx_hash: row.get(4)?,
                    fee: row.get::<_, Option<i64>>(5)?.map(|v| v.max(0) as u64),
                    sent_at: row.get::<_, Option<i64>>(6)?.map(from_unix),
                })
            },
        )?;
        tx.commit()?;
        Ok(pending)
    }

    pub fn reset_pending_payout_send_state(&self, address: &str) -> Result<()> {
        self.conn.lock().execute(
            "UPDATE pending_payouts
             SET send_started_at = NULL,
                 tx_hash = NULL,
                 fee = NULL,
                 sent_at = NULL
             WHERE address = ?1",
            params![address],
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
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;

        let pending: Option<(
            i64,
            i64,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<i64>,
        )> = tx
            .query_row(
                "SELECT amount, initiated_at, send_started_at, sent_at, tx_hash, fee
                 FROM pending_payouts
                 WHERE address = ?1",
                params![address],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            pending_amount_raw,
            initiated_at_raw,
            send_started_at_raw,
            sent_at_raw,
            pending_tx_hash,
            pending_fee_raw,
        )) = pending
        else {
            return Err(anyhow!("no pending payout for {address}"));
        };
        let pending_amount = pending_amount_raw.max(0) as u64;
        let initiated_at = from_unix(initiated_at_raw);
        let send_started_at = send_started_at_raw.map(from_unix);
        let sent_at = sent_at_raw.map(from_unix);
        if pending_amount != amount {
            return Err(anyhow!(
                "pending payout amount mismatch: expected={}, requested={}",
                pending_amount,
                amount
            ));
        }
        if let Some(pending_tx_hash) = pending_tx_hash.as_deref() {
            if pending_tx_hash != tx_hash {
                return Err(anyhow!(
                    "pending payout tx mismatch: expected={}, requested={}",
                    pending_tx_hash,
                    tx_hash
                ));
            }
        }
        if let Some(pending_fee_raw) = pending_fee_raw {
            let pending_fee = pending_fee_raw.max(0) as u64;
            if pending_fee != fee {
                return Err(anyhow!(
                    "pending payout fee mismatch: expected={}, requested={}",
                    pending_fee,
                    fee
                ));
            }
        }

        let mut bal = tx
            .query_row(
                "SELECT address, pending, paid FROM balances WHERE address = ?1",
                params![address],
                |row| {
                    Ok(Balance {
                        address: row.get(0)?,
                        pending: row.get::<_, i64>(1)?.max(0) as u64,
                        paid: row.get::<_, i64>(2)?.max(0) as u64,
                    })
                },
            )
            .optional()?
            .unwrap_or_else(|| Balance {
                address: address.to_string(),
                pending: 0,
                paid: 0,
            });
        if bal.pending < amount {
            return Err(anyhow!("insufficient balance"));
        }
        bal.pending -= amount;
        bal.paid = bal
            .paid
            .checked_add(amount)
            .ok_or_else(|| anyhow!("paid overflow"))?;

        tx.execute(
            "INSERT INTO balances (address, pending, paid) VALUES (?1, ?2, ?3)
             ON CONFLICT(address) DO UPDATE SET pending = excluded.pending, paid = excluded.paid",
            params![bal.address, u64_to_i64(bal.pending)?, u64_to_i64(bal.paid)?],
        )?;

        tx.execute(
            "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                address,
                u64_to_i64(amount)?,
                u64_to_i64(fee)?,
                tx_hash,
                to_unix(sent_at.or(send_started_at).unwrap_or(initiated_at))
            ],
        )?;

        tx.execute(
            "DELETE FROM pending_payouts WHERE address = ?1",
            params![address],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn cancel_pending_payout(&self, address: &str) -> Result<()> {
        self.conn.lock().execute(
            "DELETE FROM pending_payouts WHERE address = ?1",
            params![address],
        )?;
        Ok(())
    }

    pub fn get_pending_payouts(&self) -> Result<Vec<PendingPayout>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             ORDER BY initiated_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(PendingPayout {
                address: row.get(0)?,
                amount: row.get::<_, i64>(1)?.max(0) as u64,
                initiated_at: from_unix(row.get::<_, i64>(2)?),
                send_started_at: row.get::<_, Option<i64>>(3)?.map(from_unix),
                tx_hash: row.get(4)?,
                fee: row.get::<_, Option<i64>>(5)?.map(|v| v.max(0) as u64),
                sent_at: row.get::<_, Option<i64>>(6)?.map(from_unix),
            })
        })?;
        collect_rows(rows)
    }

    pub fn get_pending_payout(&self, address: &str) -> Result<Option<PendingPayout>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT address, amount, initiated_at, send_started_at, tx_hash, fee, sent_at
             FROM pending_payouts
             WHERE address = ?1",
            params![address],
            |row| {
                Ok(PendingPayout {
                    address: row.get(0)?,
                    amount: row.get::<_, i64>(1)?.max(0) as u64,
                    initiated_at: from_unix(row.get::<_, i64>(2)?),
                    send_started_at: row.get::<_, Option<i64>>(3)?.map(from_unix),
                    tx_hash: row.get(4)?,
                    fee: row.get::<_, Option<i64>>(5)?.map(|v| v.max(0) as u64),
                    sent_at: row.get::<_, Option<i64>>(6)?.map(from_unix),
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
    ) -> Result<Option<(u64, SystemTime)>> {
        let conn = self.conn.lock();
        conn.query_row(
            "SELECT difficulty, updated_at FROM vardiff_hints WHERE address = ?1 AND worker = ?2",
            params![address, worker],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?.max(1) as u64,
                    from_unix(row.get::<_, i64>(1)?),
                ))
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO vardiff_hints (address, worker, difficulty, updated_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(address, worker) DO UPDATE SET difficulty = excluded.difficulty, updated_at = excluded.updated_at",
            params![address, worker, u64_to_i64(difficulty.max(1))?, to_unix(updated_at)],
        )?;
        Ok(())
    }

    pub fn add_stat_snapshot(
        &self,
        timestamp: SystemTime,
        hashrate: f64,
        miners: i32,
        workers: i32,
    ) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO stat_snapshots (timestamp, hashrate, miners, workers) VALUES (?1, ?2, ?3, ?4)",
            params![to_unix(timestamp), hashrate, miners, workers],
        )?;
        Ok(())
    }

    pub fn load_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState> {
        let conn = self.conn.lock();
        let mut states_stmt = conn.prepare(
            "SELECT address, total_shares, sampled_shares, invalid_samples, forced_until, last_seen_at
             FROM validation_address_states
             WHERE last_seen_at >= ?1 OR (forced_until IS NOT NULL AND forced_until > ?2)",
        )?;
        let states = states_stmt
            .query_map(params![to_unix(state_cutoff), to_unix(now)], |row| {
                Ok(PersistedValidationAddressState {
                    address: row.get::<_, String>(0)?,
                    total_shares: row.get::<_, i64>(1)?.max(0) as u64,
                    sampled_shares: row.get::<_, i64>(2)?.max(0) as u64,
                    invalid_samples: row.get::<_, i64>(3)?.max(0) as u64,
                    forced_until: row.get::<_, Option<i64>>(4)?.map(from_unix),
                    last_seen_at: row.get::<_, i64>(5).map(from_unix)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut provisional_stmt = conn.prepare(
            "SELECT address, created_at
             FROM validation_provisionals
             WHERE created_at > ?1
             ORDER BY address ASC, created_at ASC",
        )?;
        let provisionals = provisional_stmt
            .query_map(params![to_unix(provisional_cutoff)], |row| {
                Ok(PersistedValidationProvisional {
                    address: row.get::<_, String>(0)?,
                    created_at: row.get::<_, i64>(1).map(from_unix)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(LoadedValidationState {
            states,
            provisionals,
        })
    }

    pub fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO validation_address_states (
                address, total_shares, sampled_shares, invalid_samples, forced_until, last_seen_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(address) DO UPDATE SET
                total_shares = excluded.total_shares,
                sampled_shares = excluded.sampled_shares,
                invalid_samples = excluded.invalid_samples,
                forced_until = excluded.forced_until,
                last_seen_at = excluded.last_seen_at",
            params![
                &state.address,
                u64_to_i64(state.total_shares)?,
                u64_to_i64(state.sampled_shares)?,
                u64_to_i64(state.invalid_samples)?,
                state.forced_until.map(to_unix),
                to_unix(state.last_seen_at),
            ],
        )?;
        Ok(())
    }

    pub fn add_validation_provisional(&self, address: &str, created_at: SystemTime) -> Result<()> {
        self.conn.lock().execute(
            "INSERT INTO validation_provisionals (address, created_at) VALUES (?1, ?2)",
            params![address, to_unix(created_at)],
        )?;
        Ok(())
    }

    pub fn clean_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM validation_provisionals WHERE created_at <= ?1",
            params![to_unix(provisional_cutoff)],
        )?;
        tx.execute(
            "DELETE FROM validation_address_states
             WHERE last_seen_at < ?1
               AND (forced_until IS NULL OR forced_until <= ?2)
               AND NOT EXISTS (
                   SELECT 1 FROM validation_provisionals vp
                   WHERE vp.address = validation_address_states.address
               )",
            params![to_unix(state_cutoff), to_unix(now)],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_stat_snapshots(&self, since: SystemTime) -> Result<Vec<StatSnapshot>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id, timestamp, hashrate, miners, workers FROM stat_snapshots WHERE timestamp >= ?1 ORDER BY timestamp ASC",
        )?;
        let rows = stmt.query_map(params![to_unix(since)], |row| {
            Ok(StatSnapshot {
                id: row.get(0)?,
                timestamp: from_unix(row.get(1)?),
                hashrate: row.get(2)?,
                miners: row.get(3)?,
                workers: row.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn clean_old_snapshots(&self, retain_duration: Duration) -> Result<u64> {
        let cutoff = SystemTime::now()
            .checked_sub(retain_duration)
            .unwrap_or(UNIX_EPOCH);
        let removed = self.conn.lock().execute(
            "DELETE FROM stat_snapshots WHERE timestamp < ?1",
            params![to_unix(cutoff)],
        )?;
        Ok(removed as u64)
    }

    pub fn rollup_and_prune_shares_before(&self, before: SystemTime) -> Result<u64> {
        let cutoff = floor_to_day_start_unix(to_unix(before));
        if cutoff <= 0 {
            return Ok(0);
        }

        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO share_daily_summaries
                (day_start, accepted_count, rejected_count, accepted_difficulty, unique_miners, unique_workers)
             SELECT
                (created_at / ?2) * ?2 AS day_start,
                SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END) AS accepted_count,
                SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END) AS rejected_count,
                SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END) AS accepted_difficulty,
                COUNT(DISTINCT miner) AS unique_miners,
                COUNT(DISTINCT worker) AS unique_workers
             FROM shares
             WHERE created_at < ?1
             GROUP BY day_start
             ON CONFLICT(day_start) DO UPDATE SET
                accepted_count = share_daily_summaries.accepted_count + excluded.accepted_count,
                rejected_count = share_daily_summaries.rejected_count + excluded.rejected_count,
                accepted_difficulty = share_daily_summaries.accepted_difficulty + excluded.accepted_difficulty,
                unique_miners = share_daily_summaries.unique_miners + excluded.unique_miners,
                unique_workers = share_daily_summaries.unique_workers + excluded.unique_workers",
            params![cutoff, SECONDS_PER_DAY],
        )?;
        tx.execute(
            "INSERT INTO share_rejection_reason_daily_summaries
                (day_start, reason, rejected_count)
             SELECT
                (created_at / ?2) * ?2 AS day_start,
                COALESCE(NULLIF(TRIM(reject_reason), ''), 'legacy / unknown') AS reason,
                COUNT(*) AS rejected_count
             FROM shares
             WHERE created_at < ?1
               AND status NOT IN ('verified','provisional')
             GROUP BY day_start, reason
             ON CONFLICT(day_start, reason) DO UPDATE SET
                rejected_count = share_rejection_reason_daily_summaries.rejected_count + excluded.rejected_count",
            params![cutoff, SECONDS_PER_DAY],
        )?;
        let removed = tx.execute("DELETE FROM shares WHERE created_at < ?1", params![cutoff])?;
        tx.commit()?;
        Ok(removed as u64)
    }

    pub fn rollup_and_prune_payouts_before(&self, before: SystemTime) -> Result<u64> {
        let cutoff = floor_to_day_start_unix(to_unix(before));
        if cutoff <= 0 {
            return Ok(0);
        }

        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO payout_daily_summaries
                (day_start, payout_count, total_amount, total_fee, unique_recipients)
             SELECT
                (timestamp / ?2) * ?2 AS day_start,
                COUNT(*) AS payout_count,
                COALESCE(SUM(amount), 0) AS total_amount,
                COALESCE(SUM(fee), 0) AS total_fee,
                COUNT(DISTINCT address) AS unique_recipients
             FROM payouts
             WHERE timestamp < ?1
             GROUP BY day_start
             ON CONFLICT(day_start) DO UPDATE SET
                payout_count = payout_daily_summaries.payout_count + excluded.payout_count,
                total_amount = payout_daily_summaries.total_amount + excluded.total_amount,
                total_fee = payout_daily_summaries.total_fee + excluded.total_fee,
                unique_recipients = payout_daily_summaries.unique_recipients + excluded.unique_recipients",
            params![cutoff, SECONDS_PER_DAY],
        )?;
        let removed = tx.execute("DELETE FROM payouts WHERE timestamp < ?1", params![cutoff])?;
        tx.commit()?;
        Ok(removed as u64)
    }

    /// Returns bucketed hashrate data for a miner: Vec<(bucket_ts, total_diff, count)>
    pub fn hashrate_history_for_miner(
        &self,
        address: &str,
        since: SystemTime,
        bucket_secs: i64,
    ) -> Result<Vec<(i64, u64, u64)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT (created_at / ?3) * ?3 AS bucket, SUM(difficulty), COUNT(*)
             FROM shares
             WHERE miner = ?1 AND created_at >= ?2
               AND status IN ('verified','provisional')
             GROUP BY bucket ORDER BY bucket",
        )?;
        let rows = stmt.query_map(params![address, to_unix(since), bucket_secs], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Returns per-worker stats: Vec<(worker, accepted, rejected, total_diff, last_share_at)>
    pub fn worker_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<Vec<(String, u64, u64, u64, i64)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT worker,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END),
                    MAX(created_at)
             FROM shares
             WHERE miner = ?1 AND created_at >= ?2
             GROUP BY worker ORDER BY worker",
        )?;
        let rows = stmt.query_map(params![address, to_unix(since)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
                row.get::<_, i64>(3)?.max(0) as u64,
                row.get::<_, i64>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Returns per-worker hashrate stats:
    /// Vec<(worker, total_diff, accepted_count, oldest_accepted_ts, newest_accepted_ts)>
    pub fn worker_hashrate_stats_for_miner(
        &self,
        address: &str,
        since: SystemTime,
    ) -> Result<Vec<(String, u64, u64, Option<SystemTime>, Option<SystemTime>)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT worker,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN difficulty ELSE 0 END),
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END),
                    MIN(CASE WHEN status IN ('verified','provisional') THEN created_at END),
                    MAX(CASE WHEN status IN ('verified','provisional') THEN created_at END)
             FROM shares
             WHERE miner = ?1 AND created_at >= ?2
             GROUP BY worker ORDER BY worker",
        )?;
        let rows = stmt.query_map(params![address, to_unix(since)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
                row.get::<_, Option<i64>>(3)?.map(from_unix),
                row.get::<_, Option<i64>>(4)?.map(from_unix),
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn get_blocks_for_miner(&self, address: &str) -> Result<Vec<DbBlock>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT height, hash, difficulty, finder, finder_worker, reward, timestamp, confirmed, orphaned, paid_out
             FROM blocks WHERE finder = ?1 ORDER BY height DESC",
        )?;
        let rows = stmt.query_map(params![address], row_to_block)?;
        collect_rows(rows)
    }

    /// Bulk per-miner lifetime counts from the DB: (accepted, rejected, blocks_found, last_share_unix).
    pub fn miner_lifetime_counts(
        &self,
    ) -> Result<std::collections::HashMap<String, (u64, u64, u64, Option<i64>)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT miner,
                    SUM(CASE WHEN status IN ('verified','provisional') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status NOT IN ('verified','provisional') THEN 1 ELSE 0 END),
                    MAX(created_at)
             FROM shares GROUP BY miner",
        )?;
        let mut map = std::collections::HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
                row.get::<_, i64>(2)?.max(0) as u64,
                row.get::<_, Option<i64>>(3)?,
            ))
        })?;
        for row in rows {
            let (miner, accepted, rejected, last_share) = row?;
            map.insert(miner, (accepted, rejected, 0u64, last_share));
        }
        let mut stmt2 = conn.prepare("SELECT finder, COUNT(*) FROM blocks GROUP BY finder")?;
        let block_rows = stmt2.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })?;
        for row in block_rows {
            let (finder, count) = row?;
            map.entry(finder)
                .and_modify(|e| e.2 = count)
                .or_insert((0, 0, count, None));
        }
        Ok(map)
    }

    pub fn miner_worker_counts_since(
        &self,
        since: SystemTime,
    ) -> Result<std::collections::HashMap<String, usize>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT miner, COUNT(DISTINCT worker)
             FROM shares
             WHERE created_at >= ?1
             GROUP BY miner",
        )?;
        let rows = stmt.query_map(params![to_unix(since)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as usize,
            ))
        })?;
        let mut map = std::collections::HashMap::new();
        for row in rows {
            let (miner, worker_count) = row?;
            map.insert(miner, worker_count);
        }
        Ok(map)
    }
}

fn ensure_sqlite_compatible_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    if !p.exists() {
        return Ok(());
    }

    let meta = std::fs::metadata(p).with_context(|| format!("stat {}", p.display()))?;
    if meta.len() == 0 {
        return Ok(());
    }

    let mut file = File::open(p).with_context(|| format!("open {}", p.display()))?;
    let mut header = [0u8; 16];
    let bytes_read = file
        .read(&mut header)
        .with_context(|| format!("read {}", p.display()))?;

    const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
    if bytes_read >= 16 && &header == SQLITE_HEADER {
        return Ok(());
    }

    Err(anyhow!(
        "database path '{}' is not a SQLite file (likely legacy pool DB format). \
move or rename it and restart, e.g. `mv {} {}.legacy`",
        p.display(),
        p.display(),
        p.display()
    ))
}

impl ShareStore for SqliteStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        SqliteStore::is_share_seen(self, job_id, nonce)
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        SqliteStore::mark_share_seen(self, job_id, nonce)
    }

    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        SqliteStore::try_claim_share(self, job_id, nonce)
    }

    fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        SqliteStore::release_share_claim(self, job_id, nonce)
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        SqliteStore::add_share(self, share)
    }

    fn address_risk_strikes(&self, address: &str) -> Result<u64> {
        Ok(SqliteStore::get_address_risk(self, address)?
            .map(|v| v.strikes)
            .unwrap_or(0))
    }

    fn get_vardiff_hint(&self, address: &str, worker: &str) -> Result<Option<(u64, SystemTime)>> {
        SqliteStore::get_vardiff_hint(self, address, worker)
    }

    fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        SqliteStore::upsert_vardiff_hint(self, address, worker, difficulty, updated_at)
    }
}

fn row_to_share(row: &rusqlite::Row<'_>) -> rusqlite::Result<DbShare> {
    Ok(DbShare {
        id: row.get(0)?,
        job_id: row.get(1)?,
        miner: row.get(2)?,
        worker: row.get(3)?,
        difficulty: row.get::<_, i64>(4)?.max(0) as u64,
        nonce: row.get::<_, i64>(5)?.max(0) as u64,
        status: row.get(6)?,
        was_sampled: row.get::<_, i64>(7)? != 0,
        block_hash: row.get(8)?,
        created_at: from_unix(row.get(9)?),
    })
}

fn row_to_block(row: &rusqlite::Row<'_>) -> rusqlite::Result<DbBlock> {
    Ok(DbBlock {
        height: row.get::<_, i64>(0)?.max(0) as u64,
        hash: row.get(1)?,
        difficulty: row.get::<_, i64>(2)?.max(0) as u64,
        finder: row.get(3)?,
        finder_worker: row.get(4)?,
        reward: row.get::<_, i64>(5)?.max(0) as u64,
        timestamp: from_unix(row.get(6)?),
        confirmed: row.get::<_, i64>(7)? != 0,
        orphaned: row.get::<_, i64>(8)? != 0,
        paid_out: row.get::<_, i64>(9)? != 0,
    })
}

fn row_to_pool_fee_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<PoolFeeEvent> {
    Ok(PoolFeeEvent {
        id: row.get(0)?,
        block_height: row.get::<_, i64>(1)?.max(0) as u64,
        amount: row.get::<_, i64>(2)?.max(0) as u64,
        fee_address: row.get(3)?,
        timestamp: from_unix(row.get::<_, i64>(4)?),
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

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
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
    use crate::engine::ShareRecord;
    use crate::validation::{SHARE_STATUS_REJECTED, SHARE_STATUS_VERIFIED};

    fn test_store() -> Arc<SqliteStore> {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("pool.sqlite");
        SqliteStore::open(path.to_str().expect("path")).expect("open store")
    }

    #[test]
    fn share_seen_roundtrip() {
        let store = test_store();
        assert!(!store.is_share_seen("job", 1).expect("seen"));
        store.mark_share_seen("job", 1).expect("mark");
        assert!(store.is_share_seen("job", 1).expect("seen2"));
    }

    #[test]
    fn share_claim_can_be_released_and_reacquired() {
        let store = test_store();

        assert!(store.try_claim_share("job", 1).expect("first claim"));
        assert!(!store
            .try_claim_share("job", 1)
            .expect("second claim should fail"));

        store
            .release_share_claim("job", 1)
            .expect("release claim should succeed");
        assert!(store.try_claim_share("job", 1).expect("reacquire claim"));
    }

    #[test]
    fn persisted_share_blocks_future_claims() {
        let store = test_store();
        store
            .add_share(ShareRecord {
                job_id: "job".into(),
                miner: "addr1".into(),
                worker: "rig1".into(),
                difficulty: 1,
                nonce: 7,
                status: "verified",
                was_sampled: true,
                block_hash: None,
                reject_reason: None,
                created_at: SystemTime::now(),
            })
            .expect("persist share");

        assert!(store
            .is_share_seen("job", 7)
            .expect("persisted share is seen"));
        assert!(!store
            .try_claim_share("job", 7)
            .expect("persisted share should reject new claims"));
    }

    #[test]
    fn add_and_query_shares() {
        let store = test_store();
        store
            .add_share(ShareRecord {
                job_id: "job1".into(),
                miner: "addr1".into(),
                worker: "rig1".into(),
                difficulty: 1,
                nonce: 7,
                status: SHARE_STATUS_VERIFIED,
                was_sampled: true,
                block_hash: None,
                reject_reason: None,
                created_at: SystemTime::now(),
            })
            .expect("add share");

        let shares = store.get_recent_shares(10).expect("recent");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].nonce, 7);
    }

    #[test]
    fn share_rollup_prunes_old_rows_and_preserves_totals() {
        let store = test_store();
        let old_ts = UNIX_EPOCH + Duration::from_secs((5 * SECONDS_PER_DAY + 120) as u64);
        let new_ts = UNIX_EPOCH + Duration::from_secs((40 * SECONDS_PER_DAY + 120) as u64);

        for (nonce, status, created_at) in [
            (1u64, SHARE_STATUS_VERIFIED, old_ts),
            (2u64, SHARE_STATUS_REJECTED, old_ts),
            (3u64, SHARE_STATUS_VERIFIED, new_ts),
        ] {
            store
                .add_share(ShareRecord {
                    job_id: "job-rollup".into(),
                    miner: "addr-rollup".into(),
                    worker: "rig-rollup".into(),
                    difficulty: 10,
                    nonce,
                    status,
                    was_sampled: true,
                    block_hash: None,
                    reject_reason: (status == SHARE_STATUS_REJECTED)
                        .then(|| "low difficulty share".to_string()),
                    created_at,
                })
                .expect("add share");
        }

        let before = UNIX_EPOCH + Duration::from_secs((30 * SECONDS_PER_DAY) as u64);
        let pruned = store
            .rollup_and_prune_shares_before(before)
            .expect("rollup/prune shares");
        assert_eq!(pruned, 2);
        assert_eq!(store.get_recent_shares(10).expect("recent shares").len(), 1);
        assert_eq!(store.get_total_share_count().expect("total shares"), 3);
        assert_eq!(
            store
                .total_rejected_share_count()
                .expect("total rejected shares"),
            1
        );
    }

    #[test]
    fn rejection_reason_rollup_preserves_reason_breakdown() {
        let store = test_store();
        let old_ts = UNIX_EPOCH + Duration::from_secs((5 * SECONDS_PER_DAY + 120) as u64);
        let new_ts = UNIX_EPOCH + Duration::from_secs((40 * SECONDS_PER_DAY + 120) as u64);

        for (nonce, reason, created_at) in [
            (1u64, "low difficulty share", old_ts),
            (2u64, "stale job", old_ts),
            (3u64, "low difficulty share", new_ts),
        ] {
            store
                .add_share(ShareRecord {
                    job_id: "job-reasons".into(),
                    miner: "addr-rollup".into(),
                    worker: "rig-rollup".into(),
                    difficulty: 10,
                    nonce,
                    status: SHARE_STATUS_REJECTED,
                    was_sampled: false,
                    block_hash: None,
                    reject_reason: Some(reason.to_string()),
                    created_at,
                })
                .expect("add rejected share");
        }

        let before = UNIX_EPOCH + Duration::from_secs((30 * SECONDS_PER_DAY) as u64);
        store
            .rollup_and_prune_shares_before(before)
            .expect("rollup/prune shares");

        let mut totals = store
            .total_rejection_reason_counts()
            .expect("total rejection reasons")
            .into_iter()
            .map(|item| (item.reason, item.count))
            .collect::<HashMap<_, _>>();
        assert_eq!(totals.remove("low difficulty share"), Some(2));
        assert_eq!(totals.remove("stale job"), Some(1));

        let window = store
            .rejection_reason_counts_since(new_ts - Duration::from_secs(1))
            .expect("window rejection reasons");
        assert_eq!(window.len(), 1);
        assert_eq!(window[0].reason, "low difficulty share");
        assert_eq!(window[0].count, 1);
    }

    #[test]
    fn payout_rollup_prunes_old_rows() {
        let store = test_store();
        let old_ts = 5 * SECONDS_PER_DAY + 120;
        let new_ts = 40 * SECONDS_PER_DAY + 120;

        store
            .conn
            .lock()
            .execute(
                "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["addr-old", 10i64, 1i64, "tx-old", old_ts],
            )
            .expect("insert old payout");
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["addr-new", 20i64, 2i64, "tx-new", new_ts],
            )
            .expect("insert new payout");

        let before = UNIX_EPOCH + Duration::from_secs((30 * SECONDS_PER_DAY) as u64);
        let pruned = store
            .rollup_and_prune_payouts_before(before)
            .expect("rollup/prune payouts");
        assert_eq!(pruned, 1);

        let payouts = store.get_recent_payouts(10).expect("recent payouts");
        assert_eq!(payouts.len(), 1);
        assert_eq!(payouts[0].tx_hash, "tx-new");
    }

    #[test]
    fn risk_escalation_and_queries() {
        let store = test_store();
        let state = store
            .escalate_address_risk(
                "addr1",
                "fraud",
                Duration::from_secs(60),
                Duration::from_secs(3600),
                Duration::from_secs(3600),
                true,
            )
            .expect("escalate");
        assert_eq!(state.strikes, 1);

        let (force, loaded) = store
            .should_force_verify_address("addr1")
            .expect("force verify");
        assert!(force);
        assert!(loaded.is_some());

        let (quarantined, _) = store.is_address_quarantined("addr1").expect("quarantine");
        assert!(quarantined);
    }

    #[test]
    fn risk_escalation_is_atomic_under_concurrency() {
        use std::sync::Barrier;

        let store = test_store();
        let workers = 16usize;
        let per_worker = 8usize;
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::with_capacity(workers);

        for _ in 0..workers {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..per_worker {
                    store
                        .escalate_address_risk(
                            "addr-concurrent",
                            "fraud",
                            Duration::from_secs(30),
                            Duration::from_secs(30 * 60),
                            Duration::from_secs(10 * 60),
                            true,
                        )
                        .expect("escalate");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("join escalation worker");
        }

        let state = store
            .get_address_risk("addr-concurrent")
            .expect("load risk")
            .expect("risk state");
        assert_eq!(state.strikes, (workers * per_worker) as u64);
        assert!(state.quarantined_until.is_some());
        assert!(state.force_verify_until.is_some());
    }

    #[test]
    fn pending_payout_roundtrip() {
        let store = test_store();
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        let pending = store.get_pending_payout("addr1").expect("get pending");
        assert!(pending.is_some());
        let pending = pending.expect("pending");
        assert_eq!(pending.amount, 100);
        assert!(pending.send_started_at.is_none());

        store.cancel_pending_payout("addr1").expect("cancel");
        assert!(store
            .get_pending_payout("addr1")
            .expect("get after cancel")
            .is_none());
    }

    #[test]
    fn pending_payout_refreshes_until_send_started() {
        let store = test_store();
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        let first = store
            .get_pending_payout("addr1")
            .expect("get pending")
            .expect("pending");

        store
            .create_pending_payout("addr1", 175)
            .expect("refresh pending");
        let refreshed = store
            .get_pending_payout("addr1")
            .expect("get refreshed")
            .expect("pending");

        assert_eq!(refreshed.amount, 175);
        assert_eq!(refreshed.initiated_at, first.initiated_at);
        assert!(refreshed.send_started_at.is_none());
    }

    #[test]
    fn pending_payout_freezes_after_send_started() {
        let store = test_store();
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        let started = store
            .mark_pending_payout_send_started("addr1")
            .expect("mark started")
            .expect("pending");
        assert!(started.send_started_at.is_some());

        store
            .create_pending_payout("addr1", 250)
            .expect("attempt refresh after start");
        let frozen = store
            .get_pending_payout("addr1")
            .expect("get frozen")
            .expect("pending");

        assert_eq!(frozen.amount, 100);
        assert_eq!(frozen.initiated_at, started.initiated_at);
        assert_eq!(frozen.send_started_at, started.send_started_at);
    }

    #[test]
    fn pending_payout_broadcast_roundtrip_and_reset() {
        let store = test_store();
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started("addr1")
            .expect("mark started");

        let broadcast = store
            .record_pending_payout_broadcast("addr1", 100, 42, "tx-1")
            .expect("record broadcast");
        assert_eq!(broadcast.tx_hash.as_deref(), Some("tx-1"));
        assert_eq!(broadcast.fee, Some(42));
        assert!(broadcast.sent_at.is_some());

        store
            .reset_pending_payout_send_state("addr1")
            .expect("reset send state");
        let reset = store
            .get_pending_payout("addr1")
            .expect("get pending")
            .expect("pending");
        assert!(reset.send_started_at.is_none());
        assert!(reset.tx_hash.is_none());
        assert!(reset.fee.is_none());
        assert!(reset.sent_at.is_none());
    }

    #[test]
    fn visible_payouts_for_address_include_broadcast_pending() {
        let store = test_store();
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["addr1", 10i64, 1i64, "tx-confirmed", 100i64],
            )
            .expect("insert confirmed payout");
        store
            .create_pending_payout("addr1", 25)
            .expect("create pending");
        store
            .mark_pending_payout_send_started("addr1")
            .expect("mark started");
        let broadcast = store
            .record_pending_payout_broadcast("addr1", 25, 2, "tx-pending")
            .expect("record broadcast");

        let payouts = store
            .get_recent_visible_payouts_for_address("addr1", 10)
            .expect("visible payouts");
        assert_eq!(payouts.len(), 2);
        assert_eq!(payouts[0].tx_hash, "tx-pending");
        assert_eq!(payouts[0].fee, 2);
        assert_eq!(payouts[0].timestamp, broadcast.sent_at.expect("sent at"));
        assert!(!payouts[0].confirmed);
        assert_eq!(payouts[1].tx_hash, "tx-confirmed");
        assert!(payouts[1].confirmed);
    }

    #[test]
    fn public_payout_batches_include_broadcast_pending() {
        let store = test_store();
        store
            .create_pending_payout("addr1", 25)
            .expect("create pending");
        store
            .mark_pending_payout_send_started("addr1")
            .expect("mark started");
        let broadcast = store
            .record_pending_payout_broadcast("addr1", 25, 2, "tx-pending")
            .expect("record broadcast");

        let (batches, total) = store
            .get_public_payout_batches_page("time_desc", 10, 0)
            .expect("public payout batches");
        assert_eq!(total, 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tx_hashes, vec!["tx-pending".to_string()]);
        assert_eq!(batches[0].timestamp, broadcast.sent_at.expect("sent at"));
        assert!(!batches[0].confirmed);
    }

    #[test]
    fn payouts_page_includes_broadcast_pending() {
        let store = test_store();
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO payouts (address, amount, fee, tx_hash, timestamp) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["addr-confirmed", 10i64, 1i64, "tx-confirmed", 100i64],
            )
            .expect("insert confirmed payout");
        store
            .create_pending_payout("addr-pending", 25)
            .expect("create pending");
        store
            .mark_pending_payout_send_started("addr-pending")
            .expect("mark started");
        store
            .record_pending_payout_broadcast("addr-pending", 25, 2, "tx-pending")
            .expect("record broadcast");

        let (items, total) = store
            .get_payouts_page(None, None, "time_desc", 10, 0)
            .expect("payouts page");
        assert_eq!(total, 2);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].tx_hash, "tx-pending");
        assert!(!items[0].confirmed);
        assert_eq!(items[1].tx_hash, "tx-confirmed");
        assert!(items[1].confirmed);
    }

    #[test]
    fn pool_fee_events_are_idempotent_per_block() {
        let store = test_store();

        assert!(store
            .record_pool_fee(100, 50, "pool-address", SystemTime::now())
            .expect("insert first fee"));
        assert!(!store
            .record_pool_fee(100, 50, "pool-address", SystemTime::now())
            .expect("duplicate fee should be ignored"));

        assert_eq!(store.get_total_pool_fees().expect("total"), 50);
        let events = store.get_recent_pool_fees(10).expect("recent fees");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].block_height, 100);
        assert_eq!(events[0].amount, 50);
    }

    #[test]
    fn apply_block_credits_marks_paid_atomically_and_once() {
        let store = test_store();
        store
            .add_block(&DbBlock {
                height: 7,
                hash: "abc".to_string(),
                difficulty: 1,
                finder: "finder".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
            })
            .expect("insert block");

        let credits = vec![("addr1".to_string(), 60), ("addr2".to_string(), 40)];
        assert!(store
            .apply_block_credits_and_mark_paid(7, &credits)
            .expect("apply credits"));

        assert_eq!(store.get_balance("addr1").expect("bal1").pending, 60);
        assert_eq!(store.get_balance("addr2").expect("bal2").pending, 40);
        assert!(
            store
                .get_block(7)
                .expect("block")
                .expect("present")
                .paid_out
        );

        assert!(!store
            .apply_block_credits_and_mark_paid(7, &credits)
            .expect("second apply should no-op"));
        assert_eq!(store.get_balance("addr1").expect("bal1-2").pending, 60);
        assert_eq!(store.get_balance("addr2").expect("bal2-2").pending, 40);
    }

    #[test]
    fn apply_block_credits_with_fee_records_fee_atomically() {
        let store = test_store();
        store
            .add_block(&DbBlock {
                height: 9,
                hash: "fee-block".to_string(),
                difficulty: 1,
                finder: "finder".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
            })
            .expect("insert block");

        let credits = vec![("addr1".to_string(), 60), ("addr2".to_string(), 40)];
        let fee = PoolFeeRecord {
            amount: 5,
            fee_address: "pool-address".to_string(),
            timestamp: SystemTime::now(),
        };
        assert!(store
            .apply_block_credits_and_mark_paid_with_fee(9, &credits, Some(&fee))
            .expect("apply with fee"));
        assert_eq!(store.get_total_pool_fees().expect("total fees"), 5);
        assert_eq!(store.get_balance("addr1").expect("bal1").pending, 60);

        assert!(!store
            .apply_block_credits_and_mark_paid_with_fee(9, &credits, Some(&fee))
            .expect("second apply no-op"));
        assert_eq!(store.get_total_pool_fees().expect("total fees 2"), 5);
    }

    #[test]
    fn apply_block_credits_with_invalid_fee_address_rolls_back() {
        let store = test_store();
        store
            .add_block(&DbBlock {
                height: 11,
                hash: "rollback-block".to_string(),
                difficulty: 1,
                finder: "finder".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
            })
            .expect("insert block");

        let credits = vec![("addr1".to_string(), 100)];
        let fee = PoolFeeRecord {
            amount: 10,
            fee_address: "   ".to_string(),
            timestamp: SystemTime::now(),
        };

        let err = store
            .apply_block_credits_and_mark_paid_with_fee(11, &credits, Some(&fee))
            .expect_err("empty fee address should fail");
        assert!(err.to_string().contains("fee address"));
        assert_eq!(store.get_total_pool_fees().expect("fees"), 0);
        assert_eq!(store.get_balance("addr1").expect("bal").pending, 0);
        assert!(
            !store
                .get_block(11)
                .expect("block query")
                .expect("block exists")
                .paid_out
        );
    }

    #[test]
    fn complete_pending_payout_rejects_amount_mismatch() {
        let store = test_store();
        store
            .update_balance(&Balance {
                address: "addr1".to_string(),
                pending: 100,
                paid: 0,
            })
            .expect("seed balance");
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");

        let err = store
            .complete_pending_payout("addr1", 90, 0, "tx-1")
            .expect_err("must reject mismatch");
        assert!(err.to_string().contains("amount mismatch"));
    }

    #[test]
    fn complete_pending_payout_rejects_broadcast_tx_mismatch() {
        let store = test_store();
        store
            .update_balance(&Balance {
                address: "addr1".to_string(),
                pending: 100,
                paid: 0,
            })
            .expect("seed balance");
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started("addr1")
            .expect("mark started");
        store
            .record_pending_payout_broadcast("addr1", 100, 42, "tx-expected")
            .expect("record broadcast");

        let err = store
            .complete_pending_payout("addr1", 100, 42, "tx-other")
            .expect_err("must reject tx mismatch");
        assert!(err.to_string().contains("tx mismatch"));
    }

    #[test]
    fn complete_pending_payout_records_network_fee() {
        let store = test_store();
        store
            .update_balance(&Balance {
                address: "addr1".to_string(),
                pending: 100,
                paid: 0,
            })
            .expect("seed balance");
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        store
            .complete_pending_payout("addr1", 100, 42, "tx-2")
            .expect("complete payout");

        let payouts = store.get_recent_payouts(1).expect("recent payouts");
        assert_eq!(payouts.len(), 1);
        assert_eq!(payouts[0].amount, 100);
        assert_eq!(payouts[0].fee, 42);
        assert!(payouts[0].confirmed);
    }

    #[test]
    fn complete_pending_payout_preserves_broadcast_timestamp() {
        let store = test_store();
        store
            .update_balance(&Balance {
                address: "addr1".to_string(),
                pending: 100,
                paid: 0,
            })
            .expect("seed balance");
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending");
        store
            .mark_pending_payout_send_started("addr1")
            .expect("mark started");
        let broadcast = store
            .record_pending_payout_broadcast("addr1", 100, 42, "tx-2")
            .expect("record broadcast");

        store
            .complete_pending_payout("addr1", 100, 42, "tx-2")
            .expect("complete payout");

        let payouts = store.get_recent_payouts(1).expect("recent payouts");
        assert_eq!(payouts.len(), 1);
        assert_eq!(payouts[0].timestamp, broadcast.sent_at.expect("sent at"));
        assert!(payouts[0].confirmed);
    }

    #[test]
    fn get_recent_payouts_for_address_filters_and_orders() {
        let store = test_store();
        store
            .add_payout("addr1", 10, 1, "tx-a")
            .expect("add payout a");
        store
            .add_payout("addr2", 20, 2, "tx-b")
            .expect("add payout b");
        store
            .add_payout("addr1", 30, 3, "tx-c")
            .expect("add payout c");

        let addr1 = store
            .get_recent_payouts_for_address("addr1", 10)
            .expect("addr1 payouts");
        assert_eq!(addr1.len(), 2);
        assert_eq!(addr1[0].tx_hash, "tx-c");
        assert_eq!(addr1[1].tx_hash, "tx-a");

        let addr2 = store
            .get_recent_payouts_for_address("addr2", 10)
            .expect("addr2 payouts");
        assert_eq!(addr2.len(), 1);
        assert_eq!(addr2[0].tx_hash, "tx-b");

        let limited = store
            .get_recent_payouts_for_address("addr1", 1)
            .expect("limited payouts");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].tx_hash, "tx-c");
    }
}
