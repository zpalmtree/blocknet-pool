use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[cfg(test)]
use anyhow::Context;
use anyhow::{anyhow, Result};
use tracing::warn;

use crate::config::Config;
use crate::db::{
    DbBlock, MonitorHeartbeat, MonitorHeartbeatUpsert, MonitorIncident, MonitorIncidentUpsert,
    ShareReplayData,
};
use crate::engine::{FoundBlockRecord, ShareRecord, ShareStore};
use crate::pgdb::{
    MinerShareWindowStats, PoolFeeCreditBackfillReport, PostgresStore, VardiffHintDiagnostic,
    VardiffHintSummary,
};
use crate::validation::{
    LoadedValidationState, PersistedValidationAddressState, ValidationStateStore,
};

// Matches daemon emission curve for provisional pending-block display values.
const INITIAL_REWARD: u64 = 72_325_093_035;
const TAIL_EMISSION: u64 = 200_000_000;
const MONTHS_TO_TAIL: u64 = 48;
const DECAY_RATE: f64 = 0.75;
const BLOCK_INTERVAL_SECS: u64 = 5 * 60;
const BLOCKS_PER_MONTH: u64 = (30 * 24 * 60 * 60) / BLOCK_INTERVAL_SECS;

pub struct PoolStore {
    inner: Arc<PostgresStore>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionPruneReport {
    pub shares_pruned: u64,
    pub payouts_pruned: u64,
}

impl std::fmt::Debug for PoolStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PoolStore::Postgres")
    }
}

impl Deref for PoolStore {
    type Target = PostgresStore;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl PoolStore {
    #[cfg(test)]
    pub(crate) const TEST_POSTGRES_URL_ENV: &'static str = "BLOCKNET_POOL_TEST_POSTGRES_URL";

    pub fn open_from_config(cfg: &Config) -> Result<Arc<Self>> {
        let database_url = cfg.database_url.trim();
        if database_url.is_empty() {
            return Err(anyhow!(
                "config.database_url must be set; SQLite support has been removed"
            ));
        }
        Self::open_postgres_with_pool(database_url, cfg.database_pool_size)
    }

    pub fn open_postgres_with_pool(url: &str, pool_size: i32) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: PostgresStore::connect(url, pool_size)?,
        }))
    }

    pub fn address_risk_strikes(&self, address: &str) -> Result<u64> {
        Ok(self
            .inner
            .get_address_risk(address)?
            .map(|state| state.strikes)
            .unwrap_or_default())
    }

    pub fn miner_share_window_stats_since(
        &self,
        miner: &str,
        since: SystemTime,
    ) -> Result<MinerShareWindowStats> {
        self.inner.miner_share_window_stats_since(miner, since)
    }

    pub fn vardiff_hint_summary(&self, address: &str, floor: u64) -> Result<VardiffHintSummary> {
        self.inner.vardiff_hint_summary(address, floor)
    }

    pub fn recent_vardiff_hint_diagnostics(
        &self,
        address: &str,
        limit: i64,
    ) -> Result<Vec<VardiffHintDiagnostic>> {
        self.inner.recent_vardiff_hint_diagnostics(address, limit)
    }

    pub fn backfill_uncredited_pool_fee_balance_credits(
        &self,
        expected_fee_address: Option<&str>,
    ) -> Result<PoolFeeCreditBackfillReport> {
        self.inner
            .backfill_uncredited_pool_fee_balance_credits(expected_fee_address)
    }

    pub fn upsert_monitor_heartbeat(&self, heartbeat: &MonitorHeartbeatUpsert) -> Result<()> {
        self.inner.upsert_monitor_heartbeat(heartbeat)
    }

    pub fn get_monitor_heartbeats_since(
        &self,
        since: SystemTime,
        source: Option<&str>,
    ) -> Result<Vec<MonitorHeartbeat>> {
        self.inner.get_monitor_heartbeats_since(since, source)
    }

    pub fn get_latest_monitor_heartbeat(
        &self,
        source: Option<&str>,
    ) -> Result<Option<MonitorHeartbeat>> {
        self.inner.get_latest_monitor_heartbeat(source)
    }

    pub fn upsert_monitor_incident(&self, incident: &MonitorIncidentUpsert) -> Result<()> {
        self.inner.upsert_monitor_incident(incident)
    }

    pub fn resolve_monitor_incident(&self, dedupe_key: &str, ended_at: SystemTime) -> Result<u64> {
        self.inner.resolve_monitor_incident(dedupe_key, ended_at)
    }

    pub fn get_open_monitor_incidents(
        &self,
        visibility: Option<&str>,
    ) -> Result<Vec<MonitorIncident>> {
        self.inner.get_open_monitor_incidents(visibility)
    }

    pub fn get_recent_monitor_incidents(
        &self,
        limit: i64,
        visibility: Option<&str>,
    ) -> Result<Vec<MonitorIncident>> {
        self.inner.get_recent_monitor_incidents(limit, visibility)
    }

    pub fn delete_monitor_heartbeats_before(&self, before: SystemTime) -> Result<u64> {
        self.inner.delete_monitor_heartbeats_before(before)
    }

    pub fn delete_resolved_monitor_incidents_before(&self, before: SystemTime) -> Result<u64> {
        self.inner.delete_resolved_monitor_incidents_before(before)
    }

    pub fn rollup_and_prune_retention(
        &self,
        shares_before: Option<SystemTime>,
        payouts_before: Option<SystemTime>,
    ) -> Result<RetentionPruneReport> {
        let mut report = RetentionPruneReport::default();
        if let Some(before) = shares_before {
            report.shares_pruned = self.inner.rollup_and_prune_shares_before(before)?;
        }
        if let Some(before) = payouts_before {
            report.payouts_pruned = self.inner.rollup_and_prune_payouts_before(before)?;
        }
        Ok(report)
    }

    #[cfg(test)]
    pub(crate) fn test_store() -> Option<Arc<Self>> {
        let url = std::env::var(Self::TEST_POSTGRES_URL_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())?;
        let schema = format!(
            "blocknet_pool_test_{}_{}",
            std::process::id(),
            rand::random::<u64>()
        );
        let mut conn = postgres::Client::connect(&url, postgres::NoTls)
            .with_context(|| format!("connect postgres {url} (create test schema)"))
            .expect("connect postgres test admin client");
        conn.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .with_context(|| format!("create postgres test schema {schema}"))
            .expect("create postgres test schema");
        Some(Arc::new(Self {
            inner: PostgresStore::connect_with_schema(&url, 2, Some(&schema))
                .expect("connect postgres test store"),
        }))
    }
}

impl ShareStore for PoolStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        self.inner.is_share_seen(job_id, nonce)
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.inner.mark_share_seen(job_id, nonce)
    }

    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        self.inner.try_claim_share(job_id, nonce)
    }

    fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        self.inner.release_share_claim(job_id, nonce)
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        self.inner.add_share(share)
    }

    fn add_share_with_replay(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<()> {
        self.inner.add_share_with_replay(share, replay)
    }

    fn add_found_block(&self, block: FoundBlockRecord) -> Result<()> {
        let candidate = DbBlock {
            height: block.height,
            hash: block.hash,
            difficulty: block.difficulty,
            finder: block.finder,
            finder_worker: block.finder_worker,
            reward: if block.reward > 0 {
                block.reward
            } else {
                estimated_block_reward(block.height)
            },
            timestamp: block.timestamp,
            confirmed: false,
            orphaned: false,
            paid_out: false,
            effort_pct: None,
        };

        if self.inner.insert_block_if_absent(&candidate)? {
            return Ok(());
        }
        if let Some(existing) = self.inner.get_block(candidate.height)? {
            if existing.hash != candidate.hash {
                warn!(
                    height = candidate.height,
                    existing_hash = %existing.hash,
                    found_hash = %candidate.hash,
                    "ignoring found-block recovery record with conflicting hash"
                );
            }
        }
        Ok(())
    }

    fn is_address_quarantined(&self, address: &str) -> Result<bool> {
        let (quarantined, _) = self.inner.is_address_quarantined(address)?;
        Ok(quarantined)
    }

    fn should_force_verify_address(&self, address: &str) -> Result<bool> {
        let (force_verify, _) = self.inner.should_force_verify_address(address)?;
        Ok(force_verify)
    }

    fn address_risk_strikes(&self, address: &str) -> Result<u64> {
        PoolStore::address_risk_strikes(self, address)
    }

    fn escalate_address_risk(
        &self,
        address: &str,
        reason: &str,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
        apply_quarantine: bool,
    ) -> Result<()> {
        self.inner.escalate_address_risk(
            address,
            reason,
            quarantine_base,
            quarantine_max,
            force_verify_duration,
            apply_quarantine,
        )?;
        Ok(())
    }

    fn get_vardiff_hint(&self, address: &str, worker: &str) -> Result<Option<(u64, SystemTime)>> {
        self.inner.get_vardiff_hint(address, worker)
    }

    fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        self.inner
            .upsert_vardiff_hint(address, worker, difficulty, updated_at)
    }

    fn get_vardiff_hints_for_address(
        &self,
        address: &str,
        limit: usize,
    ) -> Result<Vec<(u64, SystemTime)>> {
        self.inner.get_vardiff_hints_for_address(address, limit)
    }
}

impl ValidationStateStore for PoolStore {
    fn load_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState> {
        self.inner
            .load_validation_state(state_cutoff, provisional_cutoff, now)
    }

    fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()> {
        self.inner.upsert_validation_state(state)
    }

    fn add_validation_provisional(&self, address: &str, created_at: SystemTime) -> Result<()> {
        self.inner.add_validation_provisional(address, created_at)
    }

    fn clean_validation_state(
        &self,
        state_cutoff: SystemTime,
        provisional_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<()> {
        self.inner
            .clean_validation_state(state_cutoff, provisional_cutoff, now)
    }
}

fn estimated_block_reward(height: u64) -> u64 {
    let months = (height / BLOCKS_PER_MONTH) as f64;
    let decay = DECAY_RATE.powf(months / MONTHS_TO_TAIL as f64);
    let reward =
        (INITIAL_REWARD.saturating_sub(TAIL_EMISSION)) as f64 * decay + TAIL_EMISSION as f64;
    if reward < TAIL_EMISSION as f64 {
        TAIL_EMISSION
    } else {
        reward as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Option<Arc<PoolStore>> {
        PoolStore::test_store()
    }

    #[test]
    fn found_block_insert_adds_record_when_missing() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {} to run postgres integration checks",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };

        store
            .add_found_block(FoundBlockRecord {
                height: 42,
                hash: "h42".to_string(),
                difficulty: 123,
                reward: 456,
                finder: "addr1".to_string(),
                finder_worker: "w1".to_string(),
                timestamp: SystemTime::now(),
            })
            .expect("insert found block");

        let block = store
            .get_block(42)
            .expect("query block")
            .expect("block exists");
        assert_eq!(block.hash, "h42");
        assert_eq!(block.reward, 456);
        assert!(!block.confirmed);
        assert!(!block.paid_out);
    }

    #[test]
    fn found_block_insert_does_not_regress_existing_block_state() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {} to run postgres integration checks",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };

        store
            .add_block(&DbBlock {
                height: 77,
                hash: "existing".to_string(),
                difficulty: 999,
                finder: "addr-existing".to_string(),
                finder_worker: "rig-existing".to_string(),
                reward: 500,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: true,
                effort_pct: None,
            })
            .expect("seed existing block");

        store
            .add_found_block(FoundBlockRecord {
                height: 77,
                hash: "conflicting".to_string(),
                difficulty: 1,
                reward: 1,
                finder: "addr-new".to_string(),
                finder_worker: "rig-new".to_string(),
                timestamp: SystemTime::now(),
            })
            .expect("conflicting found block should not overwrite");

        let block = store
            .get_block(77)
            .expect("query block")
            .expect("block exists");
        assert_eq!(block.hash, "existing");
        assert_eq!(block.reward, 500);
        assert!(block.confirmed);
        assert!(block.paid_out);
    }

    #[test]
    fn vardiff_hint_round_trip() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {} to run postgres integration checks",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };

        let when = SystemTime::now();
        store
            .upsert_vardiff_hint("addr1", "rig1", 77, when)
            .expect("upsert hint");

        let hint = store
            .get_vardiff_hint("addr1", "rig1")
            .expect("get hint")
            .expect("hint exists");
        assert_eq!(hint.0, 77);
    }

    #[test]
    fn estimated_block_reward_has_tail_floor() {
        let very_high_height = BLOCKS_PER_MONTH.saturating_mul(5_000);
        assert_eq!(estimated_block_reward(very_high_height), TAIL_EMISSION);
    }
}
