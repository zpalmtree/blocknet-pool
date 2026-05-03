use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::SystemTime;

#[cfg(test)]
use anyhow::Context;
use anyhow::{anyhow, Result};
use tracing::warn;

use crate::config::Config;
use crate::engine::{FoundBlockRecord, ShareRecord, ShareStore};
use crate::payout::{is_share_payout_eligible, reward_window_end};
use crate::pgdb::PostgresStore;
use crate::rewards::estimated_block_reward;
use crate::validation::{
    is_verified_share_status, LoadedValidationState, PersistedValidationAddressState,
    ValidationClearEvent, ValidationStateStore,
};
use pool_common::db::{
    AddressRiskEscalation, AddressRiskState, DbBlock, DbShare, PendingAuditShare, ShareReplayData,
    ShareReplayUpdate,
};
use pool_common::protocol::parse_hash_hex;

pub struct PoolStore {
    inner: Arc<PostgresStore>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RetentionPruneReport {
    pub(crate) shares_pruned: u64,
    pub(crate) payouts_pruned: u64,
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
                "config.database_url must be set; Postgres is required"
            ));
        }
        Ok(Arc::new(Self {
            inner: PostgresStore::connect(database_url, pool_size)?,
        }))
    }

    fn load_pending_payout_audit_shares(
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
            let start = window_end
                .checked_sub(config.pplns_window_duration())
                .unwrap_or(std::time::UNIX_EPOCH);
            let shares = self.get_shares_between(start, window_end)?;

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
            if is_verified_share_status(&share.status) {
                verified_difficulty = verified_difficulty.saturating_add(share.difficulty.max(1));
            } else if is_share_payout_eligible(share, now, provisional_delay) {
                eligible_provisional_difficulty =
                    eligible_provisional_difficulty.saturating_add(share.difficulty.max(1));
                if !share.was_sampled {
                    replayable.push(share.clone());
                }
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

    fn load_recent_provisional_audit_shares(
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

    pub(crate) fn rollup_and_prune_retention(
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

    fn address_risk_state(&self, address: &str) -> Result<Option<AddressRiskState>> {
        self.inner.get_address_risk(address)
    }

    fn should_force_verify_address(&self, address: &str) -> Result<bool> {
        let (force_verify, _) = self.inner.should_force_verify_address(address)?;
        Ok(force_verify)
    }

    fn escalate_address_risk(&self, escalation: AddressRiskEscalation<'_>) -> Result<()> {
        self.inner.escalate_address_risk(escalation)?;
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

    fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<()> {
        self.inner.complete_validation_audit(update)
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
    fn payout_audit_deficit_only_requests_needed_verified_difficulty() {
        assert_eq!(payout_audit_deficit_difficulty(10, 190, 19.0), 0);
        assert_eq!(payout_audit_deficit_difficulty(1, 39, 19.0), 1);
        assert_eq!(payout_audit_deficit_difficulty(5, 200, 19.0), 6);
        assert_eq!(payout_audit_deficit_difficulty(0, 25, 0.0), 25);
    }
}
