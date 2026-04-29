use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[cfg(test)]
use anyhow::Context;
use anyhow::{anyhow, Result};
use tracing::warn;

use crate::config::Config;
use crate::db::{
    ActiveVerificationHold, AddressRiskState, DbBlock, DbShare, MonitorHeartbeat,
    MonitorHeartbeatUpsert, MonitorIncident, MonitorIncidentUpsert, PendingAuditShare,
    ShareReplayData, ShareReplayUpdate, ValidationHoldState,
};
use crate::engine::{FoundBlockRecord, ShareRecord, ShareStore};
use crate::payout::{is_share_payout_eligible, reward_window_end};
use crate::pgdb::{
    BalanceSourceSummary, ExistingOrphanBlockReconciliationReport,
    HistoricalMissingMinerCreditsBlock, LiveReconciliationBlockers, MinerShareWindowStats,
    MonitorUptimeSummary, PoolFeeCreditBackfillReport, PostgresStore, VardiffHintDiagnostic,
    VardiffHintSummary,
};
use crate::protocol::parse_hash_hex;
use crate::validation::{
    LoadedValidationAddressActivity, LoadedValidationState, PersistedValidationAddressState,
    ValidationClearEvent, ValidationStateStore,
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

    pub fn open(database_url: &str, pool_size: i32) -> Result<Arc<Self>> {
        let database_url = database_url.trim();
        if database_url.is_empty() {
            return Err(anyhow!(
                "config.database_url must be set; SQLite support has been removed"
            ));
        }
        Self::open_postgres_with_pool(database_url, pool_size)
    }

    pub fn open_postgres_with_pool(url: &str, pool_size: i32) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: PostgresStore::connect(url, pool_size)?,
        }))
    }

    pub fn address_risk_strikes(&self, address: &str) -> Result<u64> {
        self.inner.address_risk_strikes(address)
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        self.inner.get_address_risk(address)
    }

    pub fn miner_has_any_activity(&self, address: &str) -> Result<bool> {
        self.inner.miner_has_any_activity(address)
    }

    pub fn list_active_verification_holds(
        &self,
        provisional_cutoff: SystemTime,
    ) -> Result<Vec<ActiveVerificationHold>> {
        self.inner
            .list_active_verification_holds(provisional_cutoff)
    }

    pub fn validation_forced_until(&self, address: &str) -> Result<Option<SystemTime>> {
        self.inner.validation_forced_until(address)
    }

    pub fn validation_hold_state(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
    ) -> Result<Option<ValidationHoldState>> {
        self.inner
            .validation_hold_state(address, provisional_cutoff)
    }

    pub fn active_force_verify_addresses(
        &self,
        addresses: &[String],
        now: SystemTime,
    ) -> Result<HashSet<String>> {
        self.inner.active_force_verify_addresses(addresses, now)
    }

    pub fn load_pending_payout_audit_shares(
        &self,
        address: &str,
        config: &Config,
        now: SystemTime,
        limit: usize,
    ) -> Result<Vec<PendingAuditShare>> {
        let address = address.trim();
        if address.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let provisional_delay = config.provisional_share_delay_duration();
        let mut window_shares = Vec::<DbShare>::new();
        let mut seen = HashSet::<i64>::new();
        for block in self.get_unconfirmed_blocks()? {
            let window_end = reward_window_end(self, &block)?;
            let shares = if config.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
                if config.pplns_window_duration_duration().is_zero() {
                    self.get_last_n_shares_before(
                        window_end,
                        i64::from(config.pplns_window.max(1)),
                    )?
                } else {
                    let start = window_end
                        .checked_sub(config.pplns_window_duration_duration())
                        .unwrap_or(std::time::UNIX_EPOCH);
                    self.get_shares_between(start, window_end)?
                }
            } else {
                let start = window_end
                    .checked_sub(Duration::from_secs(60 * 60))
                    .unwrap_or(std::time::UNIX_EPOCH);
                self.get_shares_between(start, window_end)?
            };

            for share in shares {
                if share.miner == address && seen.insert(share.id) {
                    window_shares.push(share);
                }
            }
        }

        if window_shares.is_empty() {
            return Ok(Vec::new());
        }

        let mut verified_difficulty = 0u64;
        let mut eligible_provisional_difficulty = 0u64;
        let mut replayable = Vec::<DbShare>::new();
        for share in &window_shares {
            match share.status.as_str() {
                "" | "verified" => {
                    verified_difficulty =
                        verified_difficulty.saturating_add(share.difficulty.max(1));
                }
                "provisional" if is_share_payout_eligible(share, now, provisional_delay) => {
                    eligible_provisional_difficulty =
                        eligible_provisional_difficulty.saturating_add(share.difficulty.max(1));
                    if !share.was_sampled {
                        replayable.push(share.clone());
                    }
                }
                _ => {}
            }
        }

        let target_diff = payout_audit_deficit_difficulty(
            verified_difficulty,
            eligible_provisional_difficulty,
            config.payout_provisional_cap_multiplier,
        );
        if target_diff == 0 {
            return Ok(Vec::new());
        }

        replayable.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        let mut selected = Vec::<DbShare>::new();
        let mut covered = 0u64;
        for share in replayable {
            selected.push(share.clone());
            covered = covered.saturating_add(share.difficulty.max(1));
            if selected.len() >= limit || covered >= target_diff {
                break;
            }
        }
        self.build_pending_audit_shares(selected)
    }

    pub fn load_recent_provisional_audit_shares(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
        limit: usize,
    ) -> Result<Vec<PendingAuditShare>> {
        let address = address.trim();
        if address.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let shares = self.inner.get_provisional_shares_for_miner_since(
            address,
            provisional_cutoff,
            limit as i64,
        )?;
        self.build_pending_audit_shares(shares)
    }

    pub fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<()> {
        self.inner.complete_validation_audit(update)
    }

    fn build_pending_audit_shares(&self, shares: Vec<DbShare>) -> Result<Vec<PendingAuditShare>> {
        if shares.is_empty() {
            return Ok(Vec::new());
        }

        let job_ids = shares
            .iter()
            .map(|share| share.job_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let replays = self.get_share_replays_for_job_ids(&job_ids)?;

        let mut out = Vec::with_capacity(shares.len());
        for share in shares {
            let Some(replay) = replays.get(&share.job_id) else {
                continue;
            };
            let claimed_hash = share
                .claimed_hash
                .as_deref()
                .map(parse_hash_hex)
                .transpose()
                .map_err(|err| anyhow!("parse claimed hash for share {}: {}", share.id, err))?;
            out.push(PendingAuditShare {
                share_id: share.id,
                job_id: share.job_id,
                miner: share.miner,
                worker: share.worker,
                difficulty: share.difficulty.max(1),
                nonce: share.nonce,
                claimed_hash,
                header_base: replay.header_base.clone(),
                network_target: replay.network_target,
                created_at: share.created_at,
            });
        }
        Ok(out)
    }

    pub fn latest_share_timestamps_for_block_hashes(
        &self,
        hashes: &[String],
    ) -> Result<std::collections::HashMap<String, SystemTime>> {
        self.inner.latest_share_timestamps_for_block_hashes(hashes)
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

    pub fn list_confirmed_paid_blocks_missing_miner_credits(
        &self,
    ) -> Result<Vec<HistoricalMissingMinerCreditsBlock>> {
        self.inner
            .list_confirmed_paid_blocks_missing_miner_credits()
    }

    pub fn backfill_missing_block_credit_events(
        &self,
        block_height: u64,
        credits: &[(String, u64)],
    ) -> Result<bool> {
        self.inner
            .backfill_missing_block_credit_events(block_height, credits)
    }

    pub fn list_balance_source_summaries(&self) -> Result<Vec<BalanceSourceSummary>> {
        self.inner.list_balance_source_summaries()
    }

    pub fn live_reconciliation_blockers(&self) -> Result<LiveReconciliationBlockers> {
        self.inner.live_reconciliation_blockers()
    }

    pub fn reconcile_all_existing_orphaned_block_credits(
        &self,
    ) -> Result<ExistingOrphanBlockReconciliationReport> {
        self.inner.reconcile_all_existing_orphaned_block_credits()
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

    pub fn get_monitor_uptime_summary(
        &self,
        since: SystemTime,
        source: Option<&str>,
    ) -> Result<MonitorUptimeSummary> {
        self.inner.get_monitor_uptime_summary(since, source)
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

    fn add_share_with_id(&self, share: ShareRecord) -> Result<i64> {
        self.inner.add_share_with_id(share)
    }

    fn add_share_with_replay(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<()> {
        self.inner.add_share_with_replay(share, replay)
    }

    fn add_share_with_replay_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
    ) -> Result<i64> {
        self.inner.add_share_with_replay_and_id(share, replay)
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
                let reconciliation = if existing.orphaned {
                    self.inner
                        .reconcile_existing_orphaned_block_credits(candidate.height)?
                } else {
                    self.inner
                        .orphan_block_and_reverse_unpaid_credits(candidate.height)?
                };
                if reconciliation.manual_reconciliation_required {
                    warn!(
                        height = candidate.height,
                        existing_hash = %existing.hash,
                        found_hash = %candidate.hash,
                        "conflicting found block requires manual payout reconciliation before archival"
                    );
                }
                if self
                    .inner
                    .archive_conflicting_block_and_replace(&candidate)?
                {
                    warn!(
                        height = candidate.height,
                        existing_hash = %existing.hash,
                        found_hash = %candidate.hash,
                        "replaced conflicting live block record after archiving prior fork state"
                    );
                    return Ok(());
                }
                warn!(
                    height = candidate.height,
                    existing_hash = %existing.hash,
                    found_hash = %candidate.hash,
                    "skipped conflicting found block because the live record already matches"
                );
            }
        }
        Ok(())
    }

    fn is_address_quarantined(&self, address: &str) -> Result<bool> {
        let (quarantined, _) = self.inner.is_address_quarantined(address)?;
        Ok(quarantined)
    }

    fn address_risk_state(&self, address: &str) -> Result<Option<AddressRiskState>> {
        PoolStore::get_address_risk(self, address)
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
        strike_window_duration: Duration,
        quarantine_threshold: u64,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
    ) -> Result<()> {
        self.inner.escalate_address_risk(
            address,
            reason,
            strike_window_duration,
            quarantine_threshold,
            quarantine_base,
            quarantine_max,
            force_verify_duration,
        )?;
        Ok(())
    }

    fn record_suspected_fraud(
        &self,
        address: &str,
        reason: &str,
        quarantine_threshold: u64,
        strike_window_duration: Duration,
        quarantine_base: Duration,
        quarantine_max: Duration,
        force_verify_duration: Duration,
    ) -> Result<()> {
        self.inner.record_suspected_fraud(
            address,
            reason,
            quarantine_threshold,
            strike_window_duration,
            quarantine_base,
            quarantine_max,
            force_verify_duration,
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
        accepted_window_cutoff: SystemTime,
        now: SystemTime,
    ) -> Result<LoadedValidationState> {
        self.inner.load_validation_state(
            state_cutoff,
            provisional_cutoff,
            accepted_window_cutoff,
            now,
        )
    }

    fn upsert_validation_state(&self, state: &PersistedValidationAddressState) -> Result<()> {
        self.inner.upsert_validation_state(state)
    }

    fn add_validation_provisional(
        &self,
        address: &str,
        share_id: Option<i64>,
        created_at: SystemTime,
    ) -> Result<()> {
        self.inner
            .add_validation_provisional(address, share_id, created_at)
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

    fn latest_validation_clear_event_id(&self) -> Result<i64> {
        self.inner.latest_validation_clear_event_id()
    }

    fn load_validation_clear_events_since(&self, cursor: i64) -> Result<Vec<ValidationClearEvent>> {
        self.inner.load_validation_clear_events_since(cursor)
    }

    fn load_validation_address_activity(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
        accepted_window_cutoff: SystemTime,
    ) -> Result<LoadedValidationAddressActivity> {
        self.inner.load_validation_address_activity(
            address,
            provisional_cutoff,
            accepted_window_cutoff,
        )
    }

    fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<()> {
        PoolStore::complete_validation_audit(self, update)
    }

    fn load_recent_provisional_audit_shares(
        &self,
        address: &str,
        provisional_cutoff: SystemTime,
        limit: usize,
    ) -> Result<Vec<PendingAuditShare>> {
        PoolStore::load_recent_provisional_audit_shares(self, address, provisional_cutoff, limit)
    }

    fn load_pending_payout_audit_shares(
        &self,
        address: &str,
        config: &Config,
        now: SystemTime,
        limit: usize,
    ) -> Result<Vec<PendingAuditShare>> {
        PoolStore::load_pending_payout_audit_shares(self, address, config, now, limit)
    }
}

fn payout_audit_deficit_difficulty(
    verified_difficulty: u64,
    provisional_difficulty: u64,
    cap_multiplier: f64,
) -> u64 {
    if provisional_difficulty == 0 {
        return 0;
    }
    if cap_multiplier <= 0.0 {
        return provisional_difficulty;
    }

    let verified = verified_difficulty as f64;
    let provisional = provisional_difficulty as f64;
    let cap = cap_multiplier.max(0.0);
    let covered = verified * cap;
    if provisional <= covered + f64::EPSILON {
        return 0;
    }
    ((provisional - covered) / (cap + 1.0)).ceil().max(0.0) as u64
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
    fn found_block_insert_replaces_conflicting_orphaned_height_and_preserves_payout_recovery() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {} to run postgres integration checks",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };

        let suffix = rand::random::<u32>();
        let height = 77_000 + (suffix as u64 % 10_000);
        let address = format!("found-block-replace-{suffix}");
        let old_hash = format!("existing-{suffix}");
        let payout_tx = format!("found-block-replace-tx-{suffix}");
        store
            .add_block(&DbBlock {
                height,
                hash: old_hash.clone(),
                difficulty: 999,
                finder: "addr-existing".to_string(),
                finder_worker: "rig-existing".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("seed existing block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(address.clone(), 100)])
            .expect("apply credits"));
        store
            .create_pending_payout(&address, 100)
            .expect("create pending payout");
        store
            .mark_pending_payout_send_started(&address)
            .expect("mark send started")
            .expect("pending payout exists");
        store
            .record_pending_payout_broadcast(&address, 100, 2, &payout_tx)
            .expect("record pending payout broadcast");
        store
            .complete_pending_payout(&address, 100, 2, &payout_tx)
            .expect("complete pending payout");
        let orphaned = store
            .orphan_block_and_reverse_unpaid_credits(height)
            .expect("mark block orphaned");
        assert!(!orphaned.manual_reconciliation_required);

        store
            .add_found_block(FoundBlockRecord {
                height,
                hash: format!("conflicting-{suffix}"),
                difficulty: 1,
                reward: 1,
                finder: "addr-new".to_string(),
                finder_worker: "rig-new".to_string(),
                timestamp: SystemTime::now(),
            })
            .expect("conflicting found block should replace archived orphan");

        let block = store
            .get_block(height)
            .expect("query block")
            .expect("block exists");
        assert_eq!(block.hash, format!("conflicting-{suffix}"));
        assert_eq!(block.reward, 1);
        assert!(!block.confirmed);
        assert!(!block.paid_out);
        assert!(!block.orphaned);

        let issues = store
            .list_orphaned_block_credit_issues()
            .expect("load orphaned block credit issues");
        let issue = issues
            .iter()
            .find(|issue| issue.height == height && issue.hash == old_hash)
            .expect("archived orphaned issue preserved");
        assert_eq!(issue.credit_event_count, 1);
        assert_eq!(issue.paid_credit_amount, 100);

        let reverted = store
            .revert_completed_payout_tx(&payout_tx, "missing after height reuse")
            .expect("revert completed payout");
        assert_eq!(reverted.reverted_payout_rows, 1);
        assert_eq!(reverted.restored_pending_amount, 0);
        assert_eq!(reverted.dropped_orphaned_amount, 100);
        assert!(!reverted.manual_reconciliation_required);

        let balance = store
            .get_balance(&address)
            .expect("load balance after revert");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 0);
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

    #[test]
    fn payout_audit_deficit_only_requests_needed_verified_difficulty() {
        assert_eq!(payout_audit_deficit_difficulty(10, 190, 19.0), 0);
        assert_eq!(payout_audit_deficit_difficulty(1, 39, 19.0), 1);
        assert_eq!(payout_audit_deficit_difficulty(5, 200, 19.0), 6);
        assert_eq!(payout_audit_deficit_difficulty(0, 25, 0.0), 25);
    }
}
