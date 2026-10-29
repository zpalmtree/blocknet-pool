use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[cfg(test)]
use anyhow::Context;
use anyhow::{anyhow, Result};
use tracing::warn;

use crate::config::Config;
use crate::engine::{FoundBlockRecord, ShareRecord, ShareStore, SubmitMetadata, SubmitRisk};
use crate::payout::{is_share_payout_eligible, reward_window_end};
use crate::pgdb::PostgresStore;
use crate::rewards::estimated_block_reward;
use crate::validation::{
    is_verified_share_status, LoadedValidationState, PersistedValidationAddressState,
    ValidationClearEvent, ValidationStateStore,
};
use pool_common::db::{
    AddressRiskEscalation, AddressRiskState, DbBlock, DbShare, PendingAuditShare, ShareKey,
    ShareReplayData, ShareReplayUpdate,
};
use pool_common::protocol::parse_hash_hex;

pub struct PoolStore {
    inner: Arc<PostgresStore>,
    submit_risk: SubmitRiskCache,
}

/// How long a submit may act on a cached risk read. Writes made by this
/// process (strikes, quarantines, validation holds) update or drop the entry
/// at once; the only other writer is the API's admin "clear risk" action,
/// which loosens risk, so its effect reaches Stratum at most this late.
const SUBMIT_RISK_CACHE_TTL: Duration = Duration::from_secs(3);
const SUBMIT_RISK_CACHE_MAX_ENTRIES: usize = 16_384;

/// Short-lived per-address cache of the risk inputs every submit reads.
#[derive(Debug, Default)]
struct SubmitRiskCache {
    entries: parking_lot::Mutex<HashMap<String, (Instant, SubmitRisk)>>,
    /// Bumped by every invalidation, so a read that raced a write does not
    /// cache what it saw before the write.
    generation: AtomicU64,
}

impl SubmitRiskCache {
    fn get(&self, address: &str, now: Instant) -> Option<SubmitRisk> {
        let entries = self.entries.lock();
        let (cached_at, risk) = entries.get(address)?;
        (now.saturating_duration_since(*cached_at) < SUBMIT_RISK_CACHE_TTL).then(|| risk.clone())
    }

    fn insert(&self, address: &str, risk: SubmitRisk, read_started_at: Instant, generation: u64) {
        let mut entries = self.entries.lock();
        if self.generation.load(Ordering::Acquire) != generation {
            return;
        }
        if entries.len() >= SUBMIT_RISK_CACHE_MAX_ENTRIES {
            entries.retain(|_, (cached_at, _)| {
                read_started_at.saturating_duration_since(*cached_at) < SUBMIT_RISK_CACHE_TTL
            });
            if entries.len() >= SUBMIT_RISK_CACHE_MAX_ENTRIES {
                entries.clear();
            }
        }
        entries.insert(address.to_string(), (read_started_at, risk));
    }

    fn invalidate(&self, address: &str) {
        let mut entries = self.entries.lock();
        self.generation.fetch_add(1, Ordering::AcqRel);
        entries.remove(address);
    }

    /// Keep the deadline and cause together when this process writes a hold.
    fn write_validation_hold(&self, state: &PersistedValidationAddressState) {
        let mut entries = self.entries.lock();
        self.generation.fetch_add(1, Ordering::AcqRel);
        if let Some((_, risk)) = entries.get_mut(&state.address) {
            risk.validation_forced_until = state.forced_until;
            risk.validation_hold_cause = state.hold_cause;
        }
    }
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
            submit_risk: SubmitRiskCache::default(),
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

        let candidates = window_shares
            .iter()
            .map(|share| BackfillShare {
                id: share.id,
                created_at: share.created_at,
                difficulty: share.difficulty,
                verified: is_verified_share_status(&share.status),
                eligible_provisional: !is_verified_share_status(&share.status)
                    && is_share_payout_eligible(share, now, provisional_delay),
                sampled: share.was_sampled,
            })
            .collect::<Vec<_>>();
        let by_id = window_shares
            .into_iter()
            .map(|share| (share.id, share))
            .collect::<HashMap<_, _>>();
        let selected = select_payout_backfill(&candidates, config, limit)
            .into_iter()
            .filter_map(|id| by_id.get(&id).cloned())
            .collect::<Vec<_>>();
        self.build_pending_audit_shares(selected)
    }

    fn load_provisional_audit_shares_by_keys(
        &self,
        keys: &[ShareKey],
    ) -> Result<Vec<PendingAuditShare>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let shares = self.inner.get_provisional_shares_by_keys(keys)?;
        self.build_pending_audit_shares(shares)
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

    /// Shadows `PostgresStore::clear_address_risk_history` so this process's
    /// own clears also drop its cached submit risk.
    pub fn clear_address_risk_history(&self, address: &str) -> Result<()> {
        let result = self.inner.clear_address_risk_history(address);
        self.submit_risk.invalidate(address);
        result
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
            submit_risk: SubmitRiskCache::default(),
        }))
    }
}

impl ShareStore for PoolStore {
    fn try_claim_share(&self, job_id: &str, share_key: &str, nonce: u64) -> Result<bool> {
        self.inner.try_claim_share(job_id, share_key, nonce)
    }

    fn release_share_claim(&self, share_key: &str, nonce: u64) -> Result<()> {
        self.inner.release_share_claim(share_key, nonce)
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

    fn add_share_with_replay_metadata_and_id(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
        metadata: &SubmitMetadata,
    ) -> Result<i64> {
        self.inner.add_share_with_replay_metadata_and_id(
            share,
            replay,
            metadata.miner_version.as_deref(),
            metadata.backend.as_deref(),
        )
    }

    fn add_share_with_replay_metadata(
        &self,
        share: ShareRecord,
        replay: Option<ShareReplayData>,
        metadata: &SubmitMetadata,
    ) -> Result<()> {
        self.inner
            .add_share_with_replay_metadata_and_id(
                share,
                replay,
                metadata.miner_version.as_deref(),
                metadata.backend.as_deref(),
            )
            .map(|_| ())
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

    fn submit_risk(&self, address: &str) -> Result<SubmitRisk> {
        let now = Instant::now();
        if let Some(risk) = self.submit_risk.get(address, now) {
            return Ok(risk);
        }
        let generation = self.submit_risk.generation.load(Ordering::Acquire);
        let risk = self.inner.get_submit_risk(address)?;
        self.submit_risk
            .insert(address, risk.clone(), now, generation);
        Ok(risk)
    }

    fn escalate_address_risk(&self, escalation: AddressRiskEscalation<'_>) -> Result<()> {
        let address = escalation.address;
        let result = self.inner.escalate_address_risk(escalation);
        self.submit_risk.invalidate(address);
        result?;
        Ok(())
    }

    fn revoke_provisional_shares(&self, shares: &[ShareKey], reason: &str) -> Result<u64> {
        self.inner.revoke_provisional_shares(shares, reason)
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
        let result = self.inner.upsert_validation_state(state);
        match &result {
            Ok(()) => self.submit_risk.write_validation_hold(state),
            Err(_) => self.submit_risk.invalidate(&state.address),
        }
        result
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

    fn complete_validation_audit(&self, update: &ShareReplayUpdate) -> Result<bool> {
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

    fn load_provisional_audit_shares_by_keys(
        &self,
        keys: &[ShareKey],
    ) -> Result<Vec<PendingAuditShare>> {
        PoolStore::load_provisional_audit_shares_by_keys(self, keys)
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

/// One share of an address's payout window, as backfill selection sees it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BackfillShare {
    pub id: i64,
    pub created_at: SystemTime,
    pub difficulty: u64,
    pub verified: bool,
    /// Provisional and old enough to count toward payouts.
    pub eligible_provisional: bool,
    /// Already audited or queued for audit (`was_sampled`).
    pub sampled: bool,
}

/// Picks the shares of one address's payout window to audit in the
/// background (at most `limit`, in audit order).
///
/// Ratio: the oldest unaudited eligible provisional shares until their
/// difficulty closes the gap to the provisional cap. Count policy: the same,
/// unless closing the count criterion (every eligible provisional share within
/// `count_sample_radius` of a clean sample, and `count_payout_min_samples`
/// clean samples) takes no more audits; payouts credit whichever is higher.
pub(crate) fn select_payout_backfill(
    shares: &[BackfillShare],
    config: &Config,
    limit: usize,
) -> Vec<i64> {
    let mut verified_difficulty = 0u64;
    let mut eligible_provisional_difficulty = 0u64;
    let mut replayable = Vec::<BackfillShare>::new();
    for share in shares {
        if share.verified {
            verified_difficulty = verified_difficulty.saturating_add(share.difficulty.max(1));
        } else if share.eligible_provisional {
            eligible_provisional_difficulty =
                eligible_provisional_difficulty.saturating_add(share.difficulty.max(1));
            if !share.sampled {
                replayable.push(*share);
            }
        }
    }

    let target_diff = payout_audit_deficit_difficulty(
        verified_difficulty,
        eligible_provisional_difficulty,
        config.payout_provisional_cap_multiplier,
    );
    if target_diff == 0 {
        return Vec::new();
    }

    replayable.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });

    if config.count_verification_policy() {
        let mut covered = 0u64;
        let ratio_needed = replayable
            .iter()
            .position(|share| {
                covered = covered.saturating_add(share.difficulty.max(1));
                covered >= target_diff
            })
            .map_or(usize::MAX, |idx| idx + 1);
        if let Some(mut picks) = count_backfill_plan(shares, &replayable, config) {
            if picks.len() <= ratio_needed {
                picks.truncate(limit);
                return picks;
            }
        }
    }

    let mut selected = Vec::<i64>::new();
    let mut covered = 0u64;
    for share in replayable {
        selected.push(share.id);
        covered = covered.saturating_add(share.difficulty.max(1));
        if selected.len() >= limit || covered >= target_diff {
            break;
        }
    }
    selected
}

/// Count policy: audits that would make the window covered by count, or None
/// when the unaudited shares cannot get it there. `replayable` is sorted by
/// time.
fn count_backfill_plan(
    shares: &[BackfillShare],
    replayable: &[BackfillShare],
    config: &Config,
) -> Option<Vec<i64>> {
    let radius = config.count_sample_radius_duration();
    let mut sampled_at = shares
        .iter()
        .filter(|share| share.verified)
        .map(|share| share.created_at)
        .collect::<Vec<_>>();
    sampled_at.sort_unstable();
    let mut uncovered = shares
        .iter()
        .filter(|share| {
            share.eligible_provisional
                && !crate::validation::sample_within(&sampled_at, share.created_at, radius)
        })
        .map(|share| share.created_at)
        .collect::<Vec<_>>();
    uncovered.sort_unstable();

    // Greedy interval cover: for the earliest uncovered share, audit the
    // latest replayable share within `radius` after it (or else before it),
    // which covers everything up to that share plus `radius`.
    let mut picks = Vec::<i64>::new();
    let mut picked = std::collections::HashSet::<i64>::new();
    let mut covered_until: Option<SystemTime> = None;
    for at in uncovered {
        if covered_until.is_some_and(|until| at <= until) {
            continue;
        }
        let low = at.checked_sub(radius).unwrap_or(std::time::UNIX_EPOCH);
        let high = at.checked_add(radius)?;
        let end = replayable.partition_point(|share| share.created_at <= high);
        let pick = replayable[..end]
            .iter()
            .rev()
            .find(|share| share.created_at >= low && !picked.contains(&share.id))?;
        picked.insert(pick.id);
        picks.push(pick.id);
        covered_until = pick.created_at.checked_add(radius);
    }

    // Then enough clean samples, spread over the remaining shares.
    let have = sampled_at.len() + picks.len();
    let need = config.count_payout_min_samples.max(1) as usize;
    if have < need {
        let rest = replayable
            .iter()
            .filter(|share| !picked.contains(&share.id))
            .collect::<Vec<_>>();
        let extra = need - have;
        if rest.len() < extra {
            return None;
        }
        picks.extend((0..extra).map(|i| rest[i * rest.len() / extra].id));
    }
    Some(picks)
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

        // Reconciled credits move to the archive and leave the live issue list.
        assert!(store
            .list_orphaned_block_credit_issues()
            .expect("load live orphaned block credit issues")
            .is_empty());
        assert_eq!(
            store
                .get_unique_block_identity_counts()
                .expect("live and archived identities"),
            (2, 1)
        );

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
    fn submit_risk_is_read_once_and_follows_this_process_writes() {
        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {} to run postgres integration checks",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };
        let now = SystemTime::now();
        let risk = ShareStore::submit_risk(store.as_ref(), "addr-risk").expect("risk");
        assert!(risk.state.is_none() && !risk.force_verify_at(now));

        // Our own strike is visible to the very next submit.
        ShareStore::escalate_address_risk(
            store.as_ref(),
            AddressRiskEscalation::new(
                "addr-risk",
                "test strike",
                Duration::from_secs(3600),
                1,
                Duration::from_secs(3600),
                Duration::from_secs(7200),
                Duration::from_secs(600),
            ),
        )
        .expect("escalate");
        let risk = ShareStore::submit_risk(store.as_ref(), "addr-risk").expect("risk");
        assert!(risk.active_quarantine(SystemTime::now()).is_some());
        assert!(risk.force_verify_at(SystemTime::now()));

        // A write from another process (the API's admin clear) is only seen
        // once the cached read expires.
        store
            .inner
            .clear_address_risk_history("addr-risk")
            .expect("clear behind the cache");
        let cached = ShareStore::submit_risk(store.as_ref(), "addr-risk").expect("risk");
        assert!(cached.active_quarantine(SystemTime::now()).is_some());
        store
            .clear_address_risk_history("addr-risk")
            .expect("clear");
        let fresh = ShareStore::submit_risk(store.as_ref(), "addr-risk").expect("risk");
        assert!(fresh.state.is_none() && !fresh.force_verify_at(SystemTime::now()));

        // Validation holds written by this process update the cached copy.
        let until = SystemTime::now() + Duration::from_secs(900);
        ValidationStateStore::upsert_validation_state(
            store.as_ref(),
            &PersistedValidationAddressState {
                address: "addr-risk".to_string(),
                total_shares: 1,
                sampled_shares: 0,
                invalid_samples: 0,
                risk_sampled_shares: 0,
                risk_invalid_samples: 0,
                forced_started_at: None,
                forced_until: Some(until),
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: None,
                last_seen_at: SystemTime::now(),
            },
        )
        .expect("upsert validation state");
        let held = ShareStore::submit_risk(store.as_ref(), "addr-risk").expect("risk");
        assert!(held.force_verify_at(SystemTime::now()));
        let (force, _) = store
            .inner
            .should_force_verify_address("addr-risk")
            .expect("uncached check");
        assert!(force, "cached and uncached verdicts agree");
    }

    #[test]
    fn submit_risk_classifies_persisted_and_cached_holds_without_changing_payout_holds() {
        use pool_common::db::ValidationHoldCause;

        let Some(store) = test_store() else {
            eprintln!(
                "skipping postgres test: set {}",
                PoolStore::TEST_POSTGRES_URL_ENV
            );
            return;
        };
        let now = SystemTime::now();
        let until = now + Duration::from_secs(900);
        // Prime the cache, then transition in both directions between audit
        // holds and mandatory verification without waiting for the cache TTL.
        ShareStore::submit_risk(store.as_ref(), "addr-hold").expect("prime cache");
        for (cause, requires_sync) in [
            (None, true),
            (Some(ValidationHoldCause::ProvisionalBacklog), false),
            (Some(ValidationHoldCause::InvalidSamples), true),
            (Some(ValidationHoldCause::PayoutCoverage), false),
            (Some(ValidationHoldCause::InvalidSamples), true),
        ] {
            let mut state = PersistedValidationAddressState {
                address: "addr-hold".to_string(),
                total_shares: 1,
                sampled_shares: 0,
                invalid_samples: 0,
                risk_sampled_shares: 0,
                risk_invalid_samples: 0,
                forced_started_at: None,
                forced_until: Some(until),
                forced_sampled_shares: 0,
                forced_invalid_samples: 0,
                resume_forced_at: None,
                hold_cause: cause,
                last_seen_at: now,
            };
            ValidationStateStore::upsert_validation_state(store.as_ref(), &state)
                .expect("write hold");
            let cached = store
                .submit_risk
                .get("addr-hold", Instant::now())
                .expect("warm cache");
            let persisted = store
                .inner
                .get_submit_risk("addr-hold")
                .expect("database read");
            for risk in [cached, persisted] {
                assert_eq!(risk.force_verify_at(now), requires_sync, "{cause:?}");
                assert!(!risk.force_verify_at(until), "expired hold: {cause:?}");
            }
            assert!(
                store
                    .inner
                    .should_force_verify_address("addr-hold")
                    .unwrap()
                    .0,
                "payout checks must still honor {cause:?}"
            );
            assert!(store
                .inner
                .active_force_verify_addresses(&["addr-hold".to_string()], now)
                .unwrap()
                .contains("addr-hold"));

            state.forced_until = None;
            state.hold_cause = None;
            ValidationStateStore::upsert_validation_state(store.as_ref(), &state)
                .expect("clear hold");
            assert!(!ShareStore::submit_risk(store.as_ref(), "addr-hold")
                .unwrap()
                .force_verify_at(now));
        }
    }

    #[test]
    fn payout_audit_deficit_only_requests_needed_verified_difficulty() {
        assert_eq!(payout_audit_deficit_difficulty(10, 190, 19.0), 0);
        assert_eq!(payout_audit_deficit_difficulty(1, 39, 19.0), 1);
        assert_eq!(payout_audit_deficit_difficulty(5, 200, 19.0), 6);
        assert_eq!(payout_audit_deficit_difficulty(0, 25, 0.0), 25);
    }

    /// One share every `spacing`, oldest first, verified where `verified`
    /// says so; all payout-eligible and unaudited otherwise.
    fn backfill_window(
        count: usize,
        spacing: Duration,
        verified: impl Fn(usize) -> bool,
    ) -> Vec<BackfillShare> {
        let start = SystemTime::now() - spacing * count as u32 - Duration::from_secs(3600);
        (0..count)
            .map(|i| BackfillShare {
                id: i as i64 + 1,
                created_at: start + spacing * i as u32,
                difficulty: 1,
                verified: verified(i),
                eligible_provisional: !verified(i),
                sampled: verified(i),
            })
            .collect()
    }

    #[test]
    fn count_backfill_audits_one_share_per_unsampled_stretch() {
        // 2 h of a farm at 2 shares/s, 1% verified, except no sample in a
        // 30-minute stretch (shares 3,600-7,199). A 60-minute stretch
        // (shares 3,600-10,799) needs three audits.
        let shares = backfill_window(14_400, Duration::from_millis(500), |i| {
            i % 100 == 0 && !(3_600..7_200).contains(&i)
        });
        let ratio_cfg = Config::default();
        let count_cfg = Config {
            verification_policy: "count".to_string(),
            ..Config::default()
        };

        // Ratio: the oldest unaudited shares, up to the limit.
        let ratio = select_payout_backfill(&shares, &ratio_cfg, 8);
        assert_eq!(ratio, vec![2, 3, 4, 5, 6, 7, 8, 9]);

        // Count: samples 1,850 s apart leave 650 s farther than the 10-minute
        // radius from both, which one audit covers.
        let count = select_payout_backfill(&shares, &count_cfg, 8);
        assert_eq!(count.len(), 1, "{count:?}");
        let mut sampled_at = shares
            .iter()
            .filter(|share| share.verified)
            .map(|share| share.created_at)
            .chain(count.iter().map(|id| shares[(*id - 1) as usize].created_at))
            .collect::<Vec<_>>();
        sampled_at.sort_unstable();
        let radius = count_cfg.count_sample_radius_duration();
        assert!(
            shares.iter().all(|share| share.verified
                || crate::validation::sample_within(&sampled_at, share.created_at, radius)),
            "the picks cover every provisional share"
        );

        let long_gap = backfill_window(14_400, Duration::from_millis(500), |i| {
            i % 100 == 0 && !(3_600..10_800).contains(&i)
        });
        assert_eq!(select_payout_backfill(&long_gap, &count_cfg, 8).len(), 3);

        // Too few samples in the window: spread extra audits up to the minimum.
        let sparse = backfill_window(3_000, Duration::from_millis(500), |i| i % 300 == 0);
        let picks = select_payout_backfill(&sparse, &count_cfg, 100);
        assert_eq!(picks.len(), 30 - 10);

        // Nothing to do while the ratio cap is satisfied.
        let covered = backfill_window(1_000, Duration::from_millis(500), |i| i % 10 == 0);
        assert!(select_payout_backfill(&covered, &count_cfg, 8).is_empty());
    }
}
