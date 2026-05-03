use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::node::{
    http_error_body_contains, is_http_status, NodeClient, NodeStatus, TxStatus,
    WalletAddressResponse, WalletOutput, WalletOutputRef, WalletRecipient,
};
use crate::pgdb::UnreconciledCompletedPayoutRow;
use crate::store::PoolStore;
use crate::telemetry::NamedTimedOperationTracker;
#[cfg(test)]
use crate::validation::SHARE_STATUS_PROVISIONAL;
use crate::validation::{
    is_provisional_share_status, is_verified_share_status, SHARE_STATUS_REJECTED,
    SHARE_STATUS_VERIFIED,
};
use crate::wallet_send_journal::{
    aggregate_wallet_send_recipients, decode_wallet_send_body, WalletSendIdempotencyJournal,
};
use pool_common::db::{
    allocate_proportional_fees, Balance, DbBlock, DbShare, Payout, PendingPayout,
    PendingPayoutBatchMember, PoolFeeRecord, ShareReplayUpdate,
};
use pool_common::pow::{check_target, difficulty_to_target, Argon2PowHasher, PowHasher};
use pool_common::protocol::{address_network, validate_miner_address_for_network, AddressNetwork};

const MIN_PAYOUT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_PAYOUT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
const MIN_PAYOUT_FEE_BUFFER: u64 = 1_000;
const PENDING_PAYOUT_RETRY_GRACE: Duration = Duration::from_secs(15 * 60);
const PAYOUT_CONFIRMATIONS_REQUIRED: u64 = 1;
const MIN_WALLET_SEND_SPACING: Duration = Duration::from_millis(2_100);
const PAYOUT_TASK_SLOW_LOG_AFTER: Duration = Duration::from_millis(250);
const DEFAULT_TARGET_RECIPIENTS_PER_TX: usize = 12;
const MAX_RECIPIENTS_PER_TX: usize = 48;
const MAX_TXS_PER_SWEEP: usize = 8;
const PAYOUT_WAIT_PRIORITY_THRESHOLD: Duration = Duration::from_secs(6 * 60 * 60);
const CHAIN_RECOVERY_LOOKBACK_BLOCKS: i64 = 1024;
const RECOVERY_PAYOUT_RECONCILIATION_BATCH_SIZE: i64 = 16;
const MAX_CHANGE_SPLIT: u32 = 4;
const REBALANCE_SPENDABLE_SHARE_NUMERATOR: u64 = 1;
const REBALANCE_SPENDABLE_SHARE_DENOMINATOR: u64 = 4;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MissingCompletedPayoutSummary {
    issue_count: u64,
    row_count: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PayoutRuntimeSnapshot {
    #[serde(default)]
    pub payout_interval_seconds: u64,
    #[serde(default)]
    pub maintenance_interval_seconds: u64,
    pub next_sweep_at: Option<SystemTime>,
    pub last_tick_at: Option<SystemTime>,
    #[serde(default)]
    pub reserve_target_amount: u64,
    #[serde(default)]
    pub safe_spend_budget: u64,
    #[serde(default)]
    pub spendable_output_count: usize,
    #[serde(default)]
    pub small_output_count: usize,
    #[serde(default)]
    pub medium_output_count: usize,
    #[serde(default)]
    pub large_output_count: usize,
    #[serde(default)]
    pub planned_batch_count: usize,
    #[serde(default)]
    pub planned_recipient_count: usize,
    #[serde(default)]
    pub rebalance_required: bool,
    #[serde(default)]
    pub rebalance_active: bool,
    #[serde(default)]
    pub inventory_health: String,
}

#[derive(Debug, Clone, Copy)]
pub struct PayoutTrustPolicy {
    pub min_verified_shares: u64,
    pub provisional_cap_multiplier: f64,
}

impl PayoutTrustPolicy {
    fn from_config(cfg: &Config) -> Self {
        Self::from_values(
            cfg.payout_min_verified_shares,
            cfg.payout_provisional_cap_multiplier,
        )
    }

    pub fn from_values(
        payout_min_verified_shares: i32,
        payout_provisional_cap_multiplier: f64,
    ) -> Self {
        Self {
            min_verified_shares: payout_min_verified_shares.max(0) as u64,
            provisional_cap_multiplier: payout_provisional_cap_multiplier.max(0.0),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AddressShareWeights {
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_difficulty: u64,
}

#[derive(Debug, Clone)]
struct PayoutCandidate {
    balance: Balance,
    pending: PendingPayout,
}

#[derive(Debug, Clone, Default)]
struct RecentPayoutStats {
    median_batch_total: u64,
    p90_batch_total: u64,
    p50_recipient_count: usize,
    p90_recipient_count: usize,
    median_recipient_amount: u64,
    p90_recipient_amount: u64,
}

#[derive(Debug, Clone, Default)]
struct OutputInventory {
    spendable_count: usize,
    small_count: usize,
    medium_count: usize,
    large_count: usize,
    small_target_amount: u64,
    medium_target_amount: u64,
    large_target_amount: u64,
    small_target_count: usize,
    medium_target_count: usize,
    large_target_count: usize,
}

struct LiquidityRuntimeSnapshot<'a> {
    reserve_target_amount: u64,
    safe_spend_budget: u64,
    inventory: &'a OutputInventory,
    planned_batch_count: usize,
    planned_recipient_count: usize,
    rebalance_required: bool,
    rebalance_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveredWalletSend {
    txid: String,
    fee: u64,
    recipients: Vec<(String, u64)>,
}

#[derive(Debug, Clone)]
struct PlannedPayoutBatch {
    batch_id: String,
    idempotency_key: String,
    recipients: Vec<PayoutCandidate>,
    recipient_total: u64,
    inputs: Vec<WalletOutput>,
    locked_amount: u64,
    change_split: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShareReplayRecovery {
    pub attempted: bool,
    pub verified: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileWindow {
    None,
    Start(u64),
    NeedsOlderBlocks(u64),
}

#[derive(Debug, Clone, Copy, Default)]
struct RecoveryPayoutReconciliationState {
    active: bool,
    after_payout_id: i64,
}

pub(crate) struct PayoutProcessor {
    cfg: Config,
    configured_address_network: Option<AddressNetwork>,
    store: Arc<PoolStore>,
    node: Arc<NodeClient>,
    runtime: RwLock<PayoutRuntimeSnapshot>,
    recovery_payout_reconciliation: RwLock<RecoveryPayoutReconciliationState>,
    task_metrics: Option<Arc<NamedTimedOperationTracker>>,
}

impl PayoutProcessor {
    #[cfg(test)]
    pub(crate) fn new(cfg: Config, store: Arc<PoolStore>, node: Arc<NodeClient>) -> Arc<Self> {
        Self::new_with_task_metrics(cfg, store, node, None)
    }

    pub(crate) fn new_with_task_metrics(
        cfg: Config,
        store: Arc<PoolStore>,
        node: Arc<NodeClient>,
        task_metrics: Option<Arc<NamedTimedOperationTracker>>,
    ) -> Arc<Self> {
        let configured_address_network = configured_payout_address_network(&cfg);
        Arc::new(Self {
            cfg,
            configured_address_network,
            store,
            node,
            runtime: RwLock::new(PayoutRuntimeSnapshot::default()),
            recovery_payout_reconciliation: RwLock::new(
                RecoveryPayoutReconciliationState::default(),
            ),
            task_metrics,
        })
    }

    pub(crate) fn runtime_snapshot(&self) -> PayoutRuntimeSnapshot {
        self.runtime.read().clone()
    }

    pub(crate) fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let payout_interval = bounded_payout_interval(this.cfg.payout_interval_duration());
            let maintenance_interval = bounded_payout_maintenance_interval(payout_interval);
            {
                let this = Arc::clone(&this);
                let task_metrics = this.task_metrics.clone();
                let started_at = Instant::now();
                let startup = tokio::task::spawn_blocking(move || {
                    this.recover_pending_payouts();
                    this.tick(true);
                    let now = SystemTime::now();
                    this.update_runtime_snapshot(
                        payout_interval,
                        maintenance_interval,
                        now.checked_add(payout_interval),
                        Some(now),
                    );
                })
                .await;
                record_payout_task_observation(
                    task_metrics.as_ref(),
                    "payout_startup",
                    started_at.elapsed(),
                    startup.is_err(),
                );
            }

            let mut next_send_due = Instant::now() + payout_interval;
            let mut next_send_due_at = SystemTime::now().checked_add(payout_interval);
            let mut ticker = tokio::time::interval(maintenance_interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let should_send = Instant::now() >= next_send_due;
                let worker = Arc::clone(&this);
                let started_at = Instant::now();
                let send_completed =
                    tokio::task::spawn_blocking(move || worker.tick(should_send)).await;
                let (send_completed, failed) = match send_completed {
                    Ok(value) => (value, false),
                    Err(err) => {
                        tracing::warn!(error = %err, "payout tick task join failed");
                        (false, true)
                    }
                };
                let now = SystemTime::now();
                if send_completed {
                    next_send_due = Instant::now() + payout_interval;
                    next_send_due_at = now.checked_add(payout_interval);
                } else if should_send {
                    next_send_due_at = now.checked_add(maintenance_interval);
                }
                this.update_runtime_snapshot(
                    payout_interval,
                    maintenance_interval,
                    next_send_due_at,
                    Some(now),
                );
                record_payout_task_observation(
                    this.task_metrics.as_ref(),
                    if should_send {
                        "payout_tick_send"
                    } else {
                        "payout_tick_maintenance"
                    },
                    started_at.elapsed(),
                    failed,
                );
            }
        });
    }

    fn update_runtime_snapshot(
        &self,
        payout_interval: Duration,
        maintenance_interval: Duration,
        next_sweep_at: Option<SystemTime>,
        last_tick_at: Option<SystemTime>,
    ) {
        let mut runtime = self.runtime.write();
        runtime.payout_interval_seconds = payout_interval.as_secs();
        runtime.maintenance_interval_seconds = maintenance_interval.as_secs();
        runtime.next_sweep_at = next_sweep_at;
        runtime.last_tick_at = last_tick_at;
    }

    fn tick(&self, send_payouts: bool) -> bool {
        self.reconcile_existing_orphaned_block_credits();

        let status = match self.node.get_status() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "cannot reach node for payouts");
                return false;
            }
        };
        if !daemon_ready_for_payouts(&status) {
            tracing::warn!(
                chain_height = status.chain_height,
                "daemon is syncing; skipping payout tick"
            );
            return false;
        }

        let recovery_detected = self.reconcile_chain_recovery(&status);
        if recovery_detected {
            self.activate_recovery_payout_reconciliation();
        }
        self.confirm_blocks();
        self.distribute_rewards();
        self.reconcile_pending_payouts();
        if recovery_detected || self.recovery_payout_reconciliation_active() {
            self.reconcile_completed_payouts_after_recovery();
        }
        if send_payouts {
            self.send_payouts();
        }
        send_payouts
    }

    fn activate_recovery_payout_reconciliation(&self) {
        let mut state = self.recovery_payout_reconciliation.write();
        if state.active {
            return;
        }
        state.active = true;
        state.after_payout_id = 0;
    }

    fn recovery_payout_reconciliation_active(&self) -> bool {
        self.recovery_payout_reconciliation.read().active
    }

    fn reconcile_chain_recovery(&self, status: &NodeStatus) -> bool {
        let latest_block = match self
            .store
            .get_latest_non_orphan_block_up_to(status.chain_height)
        {
            Ok(block) => block,
            Err(err) => {
                tracing::warn!(error = %err, "failed to load latest non-orphan pool block");
                return false;
            }
        };
        let Some(latest_block) = latest_block else {
            return false;
        };

        let reconcile_from = match self.find_reconcile_start_height(status.chain_height) {
            Ok(Some(height)) => height,
            Ok(None) => return false,
            Err(err) => {
                tracing::warn!(error = %err, "failed to locate block reconciliation start");
                return false;
            }
        };
        let blocks = match self.store.get_non_orphan_blocks_from_height(reconcile_from) {
            Ok(blocks) => blocks,
            Err(err) => {
                tracing::warn!(
                    start_height = reconcile_from,
                    error = %err,
                    "failed to load pool blocks for reconciliation"
                );
                return false;
            }
        };
        if blocks.is_empty() {
            return true;
        }

        let mut mismatched_blocks = Vec::<(DbBlock, String)>::new();
        for block in blocks {
            let node_block = match self.node.get_block_by_height_optional(block.height) {
                Ok(block) => block,
                Err(err) => {
                    tracing::warn!(
                        height = block.height,
                        error = %err,
                        "failed to fetch daemon block during reconciliation"
                    );
                    return true;
                }
            };
            let Some(node_block) = node_block else {
                return true;
            };
            if node_block.hash == block.hash {
                continue;
            }
            mismatched_blocks.push((block, node_block.hash));
        }

        if mismatched_blocks.is_empty() {
            return false;
        }

        tracing::warn!(
            start_height = mismatched_blocks
                .first()
                .map(|(block, _)| block.height)
                .unwrap_or(reconcile_from),
            latest_height = latest_block.height,
            latest_pool_hash = %latest_block.hash,
            "detected daemon chain divergence window; reconciling pool blocks"
        );

        for (block, daemon_hash) in mismatched_blocks {
            match self
                .store
                .orphan_block_and_reverse_unpaid_credits(block.height)
            {
                Ok(result) => {
                    if !result.orphaned {
                        continue;
                    }
                    if result.manual_reconciliation_required {
                        tracing::warn!(
                            height = block.height,
                            pool_hash = %block.hash,
                            daemon_hash = %daemon_hash,
                            "marked block orphaned, but credited payouts require manual reconciliation"
                        );
                    } else {
                        tracing::info!(
                            height = block.height,
                            pool_hash = %block.hash,
                            daemon_hash = %daemon_hash,
                            reversed_credit_events = result.reversed_credit_events,
                            reversed_credit_amount = result.reversed_credit_amount,
                            reversed_fee_amount = result.reversed_fee_amount,
                            canceled_pending_payouts = result.canceled_pending_payouts,
                            "orphaned diverged block and reversed unpaid credits"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        height = block.height,
                        error = %err,
                        "failed to orphan diverged block during reconciliation"
                    );
                    return true;
                }
            }
        }

        true
    }

    fn find_reconcile_start_height(&self, current_height: u64) -> anyhow::Result<Option<u64>> {
        let recent_blocks = self
            .store
            .get_recent_blocks_up_to(CHAIN_RECOVERY_LOOKBACK_BLOCKS, current_height)?;
        match self.find_reconcile_start_height_in_blocks(&recent_blocks)? {
            ReconcileWindow::None => return Ok(None),
            ReconcileWindow::Start(height) => return Ok(Some(height)),
            ReconcileWindow::NeedsOlderBlocks(_) => {}
        }

        let blocks = self.store.get_non_orphan_blocks_up_to(current_height)?;
        match self.find_reconcile_start_height_in_blocks(&blocks)? {
            ReconcileWindow::None => Ok(None),
            ReconcileWindow::Start(height) | ReconcileWindow::NeedsOlderBlocks(height) => {
                Ok(Some(height))
            }
        }
    }

    fn find_reconcile_start_height_in_blocks(
        &self,
        blocks: &[DbBlock],
    ) -> anyhow::Result<ReconcileWindow> {
        if blocks.is_empty() {
            return Ok(ReconcileWindow::None);
        }

        let mut first_mismatch = None;
        for block in blocks {
            if block.orphaned {
                continue;
            }
            let Some(node_block) = self.node.get_block_by_height_optional(block.height)? else {
                continue;
            };
            if node_block.hash == block.hash {
                if first_mismatch.is_some() {
                    return Ok(ReconcileWindow::Start(block.height.saturating_add(1)));
                }
                continue;
            }
            first_mismatch = Some(block.height);
        }

        Ok(match first_mismatch {
            Some(height) if blocks.len() >= CHAIN_RECOVERY_LOOKBACK_BLOCKS as usize => {
                ReconcileWindow::NeedsOlderBlocks(height)
            }
            Some(height) => ReconcileWindow::Start(height),
            None => ReconcileWindow::None,
        })
    }

    fn confirm_blocks(&self) {
        let blocks = match self.store.get_unconfirmed_blocks() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read unconfirmed blocks");
                return;
            }
        };

        let current_height = self.node.chain_height();
        let required_confirmations = self.cfg.blocks_before_payout.max(0) as u64;

        for mut block in blocks {
            if current_height < block.height {
                continue;
            }
            let depth = current_height.saturating_sub(block.height);

            let node_block = match self.node.get_block(&block.height.to_string()) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(height = block.height, error = %err, "failed to verify block");
                    continue;
                }
            };

            if node_block.hash != block.hash {
                let grace = 6u64;
                if depth < required_confirmations + grace {
                    continue;
                }

                block.orphaned = true;
                if let Err(err) = self.store.update_block(&block) {
                    tracing::warn!(height = block.height, error = %err, "failed to mark orphan block");
                }
                continue;
            }

            let mut changed = false;
            if block.reward != node_block.reward {
                block.reward = node_block.reward;
                changed = true;
            }
            if depth >= required_confirmations && !block.confirmed {
                block.confirmed = true;
                changed = true;
            }
            if changed {
                if let Err(err) = self.store.update_block(&block) {
                    tracing::warn!(height = block.height, error = %err, "failed to update block confirmation state");
                }
            }
        }
    }

    fn distribute_rewards(&self) {
        let blocks = match self.store.get_unpaid_blocks() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read unpaid blocks");
                return;
            }
        };

        for block in blocks {
            if block.reward == 0 {
                continue;
            }

            let fee = self.cfg.pool_fee(block.reward);
            let fee_record = if fee > 0 {
                match resolve_pool_fee_destination(&self.cfg, &block) {
                    Some(fee_address) => Some(PoolFeeRecord {
                        amount: fee,
                        fee_address,
                        timestamp: block.timestamp,
                    }),
                    None => {
                        tracing::error!(
                            height = block.height,
                            fee,
                            "pool fee is configured without a payout destination; refusing to mark block paid"
                        );
                        continue;
                    }
                }
            } else {
                None
            };
            let distributable = block
                .reward
                .saturating_sub(fee_record.as_ref().map(|record| record.amount).unwrap_or(0));

            let credits = self.build_pplns_credits(&block, distributable);
            let credits = match credits {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(height = block.height, error = %err, "failed reward distribution");
                    continue;
                }
            };
            let mut credits_vec = Vec::with_capacity(credits.len());
            for (address, amount) in credits {
                if amount > 0 {
                    credits_vec.push((address, amount));
                }
            }

            let applied = match self.store.apply_block_credits_and_mark_paid_with_fee(
                block.height,
                &credits_vec,
                fee_record.as_ref(),
            ) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        height = block.height,
                        error = %err,
                        "failed reward distribution transaction"
                    );
                    continue;
                }
            };
            if !applied {
                continue;
            }
        }
    }

    fn build_pplns_credits(
        &self,
        block: &DbBlock,
        reward: u64,
    ) -> anyhow::Result<HashMap<String, u64>> {
        let now = SystemTime::now();
        let provisional_delay = self.cfg.provisional_share_delay_duration();
        let window_end = reward_window_end(self.store.as_ref(), block)?;
        let since = window_end
            .checked_sub(self.cfg.pplns_window_duration())
            .unwrap_or(UNIX_EPOCH);
        let shares = self.store.get_shares_between(since, window_end)?;

        let mut credits = HashMap::<String, u64>::new();
        if shares.is_empty() {
            add_credit(&mut credits, &block.finder, reward)?;
            return Ok(credits);
        }

        let mut shares = shares;
        let mut weight_result = self.weight_shares_for_payout(&shares)?;
        if weight_result.1 == 0 {
            let recovery = recover_share_window_by_replay(
                self.store.as_ref(),
                &mut shares,
                now,
                provisional_delay,
                true,
            )?;
            if recovery.attempted {
                tracing::info!(
                    height = block.height,
                    replay_verified = recovery.verified,
                    replay_rejected = recovery.rejected,
                    "replayed payout window shares before PPLNS allocation"
                );
                weight_result = self.weight_shares_for_payout(&shares)?;
            }
        }
        if weight_result.1 == 0 {
            anyhow::bail!(
                "block {} payout window has shares but no payout-eligible verified weight after replay",
                block.height
            );
        }
        let (weights, total_weight) = weight_result;

        self.allocate_weighted_credits(weights, total_weight, reward, &mut credits)?;
        Ok(credits)
    }

    fn weight_shares_for_payout(
        &self,
        shares: &[DbShare],
    ) -> anyhow::Result<(HashMap<String, u64>, u64)> {
        let now = SystemTime::now();
        let provisional_delay = self.cfg.provisional_share_delay_duration();
        let trust_policy = PayoutTrustPolicy::from_config(&self.cfg);
        let mut risk_cache = HashMap::<String, bool>::new();

        weight_shares(shares, now, provisional_delay, trust_policy, |address| {
            if let Some(risky) = risk_cache.get(address) {
                return Ok(*risky);
            }
            let risky = self
                .store
                .should_force_verify_address(address)
                .map(|(force_verify, _)| force_verify)
                .map_err(|err| {
                    anyhow::anyhow!(
                        "risk check failed during payout weighting for {address}: {err}"
                    )
                })?;
            risk_cache.insert(address.to_string(), risky);
            Ok(risky)
        })
    }

    fn allocate_weighted_credits(
        &self,
        weights: HashMap<String, u64>,
        total_weight: u64,
        amount: u64,
        credits: &mut HashMap<String, u64>,
    ) -> anyhow::Result<()> {
        let mut weighted = weights.into_iter().collect::<Vec<(String, u64)>>();
        weighted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        let mut distributed = 0u64;
        for (address, weight) in weighted.iter() {
            let share = ((amount as u128) * (*weight as u128) / (total_weight as u128)) as u64;
            if share == 0 {
                continue;
            }
            add_credit(credits, address, share)?;
            distributed = distributed.saturating_add(share);
        }

        let remainder = amount.saturating_sub(distributed);
        if remainder > 0 {
            if let Some((address, _)) = weighted.first() {
                add_credit(credits, address, remainder)?;
            }
        }
        Ok(())
    }

    fn recover_pending_payouts(&self) {
        let pending = match self.store.get_pending_payouts() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read pending payouts");
                return;
            }
        };

        let now = SystemTime::now();
        let mut warned_batches = HashSet::<String>::new();
        for entry in pending {
            let queued_age = now.duration_since(entry.initiated_at).unwrap_or_default();
            if has_stale_unknown_broadcast_outcome(&entry, now) {
                let send_age = pending_send_age(&entry, now);
                if let Some(batch_id) = entry
                    .batch_id
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                {
                    if warned_batches.insert(batch_id.to_string()) {
                        tracing::warn!(
                            batch_id = %batch_id,
                            queued_age_secs = queued_age.as_secs(),
                            send_age_secs = send_age.as_secs(),
                            "payout batch has unknown broadcast outcome on startup; leaving queued for reconciliation"
                        );
                    }
                } else {
                    tracing::warn!(
                        address = %entry.address,
                        amount = entry.amount,
                        queued_age_secs = queued_age.as_secs(),
                        send_age_secs = send_age.as_secs(),
                        "pending payout has unknown broadcast outcome on startup; leaving queued for reconciliation"
                    );
                }
                continue;
            }

            if queued_age > Duration::from_secs(60 * 60) {
                if entry.send_started_at.is_some() {
                    tracing::warn!(
                        address = %entry.address,
                        amount = entry.amount,
                        age_secs = queued_age.as_secs(),
                        "stale pending payout retained for idempotent retry"
                    );
                } else {
                    tracing::warn!(
                        address = %entry.address,
                        amount = entry.amount,
                        age_secs = queued_age.as_secs(),
                        "stale queued payout has not reached its first send attempt yet"
                    );
                }
            }
        }
    }

    fn reconcile_completed_payouts_after_recovery(&self) {
        let after_payout_id = {
            let state = self.recovery_payout_reconciliation.read();
            if !state.active {
                return;
            }
            state.after_payout_id
        };

        let tx_hashes = match self.store.get_active_payout_tx_hash_batch(
            after_payout_id,
            RECOVERY_PAYOUT_RECONCILIATION_BATCH_SIZE,
        ) {
            Ok(payouts) => payouts,
            Err(err) => {
                tracing::warn!(error = %err, "failed to load completed payouts for recovery reconciliation");
                return;
            }
        };
        if tx_hashes.is_empty() {
            let mut state = self.recovery_payout_reconciliation.write();
            *state = RecoveryPayoutReconciliationState::default();
            return;
        }

        let mut last_scanned_payout_id = after_payout_id;
        for (payout_id, tx_hash) in &tx_hashes {
            last_scanned_payout_id = last_scanned_payout_id.max(*payout_id);
            let status = match self.node.get_tx_status_optional(tx_hash) {
                Ok(status) => status,
                Err(err) => {
                    tracing::warn!(
                        tx = %tx_hash,
                        error = %err,
                        "failed to reconcile completed payout against daemon"
                    );
                    continue;
                }
            };
            if status.as_ref().is_some_and(|status| {
                status.in_mempool || status.confirmations >= PAYOUT_CONFIRMATIONS_REQUIRED
            }) {
                continue;
            }
            if status.is_some() {
                continue;
            }

            match self
                .store
                .revert_completed_payout_tx(tx_hash, "missing from daemon chain after recovery")
            {
                Ok(result) => {
                    if result.manual_reconciliation_required {
                        tracing::warn!(
                            tx = %tx_hash,
                            "completed payout disappeared from the daemon chain but requires manual reconciliation"
                        );
                    } else if result.reverted_payout_rows > 0 {
                        tracing::warn!(
                            tx = %tx_hash,
                            reverted_payout_rows = result.reverted_payout_rows,
                            restored_pending_amount = result.restored_pending_amount,
                            dropped_orphaned_amount = result.dropped_orphaned_amount,
                            "reverted completed payout rows that disappeared from the daemon chain"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        tx = %tx_hash,
                        error = %err,
                        "failed to revert completed payout missing from daemon chain"
                    );
                }
            }
        }

        let mut state = self.recovery_payout_reconciliation.write();
        if tx_hashes.len() < RECOVERY_PAYOUT_RECONCILIATION_BATCH_SIZE as usize {
            *state = RecoveryPayoutReconciliationState::default();
        } else {
            state.active = true;
            state.after_payout_id = last_scanned_payout_id;
        }
    }

    fn reconcile_existing_orphaned_block_credits(&self) {
        match self.store.reconcile_all_existing_orphaned_block_credits() {
            Ok(report) => {
                if report.blocks_scanned == 0 {
                    return;
                }
                if report.blocks_requiring_manual_reconciliation > 0 {
                    tracing::warn!(
                        blocks_scanned = report.blocks_scanned,
                        blocks_reconciled = report.blocks_reconciled,
                        blocks_requiring_manual_reconciliation =
                            report.blocks_requiring_manual_reconciliation,
                        reversed_credit_events = report.reversed_credit_events,
                        reversed_credit_amount = report.reversed_credit_amount,
                        reversed_fee_amount = report.reversed_fee_amount,
                        canceled_pending_payouts = report.canceled_pending_payouts,
                        "reconciled existing orphaned block credits with unresolved leftovers"
                    );
                } else {
                    tracing::info!(
                        blocks_scanned = report.blocks_scanned,
                        blocks_reconciled = report.blocks_reconciled,
                        reversed_credit_events = report.reversed_credit_events,
                        reversed_credit_amount = report.reversed_credit_amount,
                        reversed_fee_amount = report.reversed_fee_amount,
                        canceled_pending_payouts = report.canceled_pending_payouts,
                        "reconciled existing orphaned block credits"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    error_chain = %format!("{err:#}"),
                    "failed reconciling existing orphaned block credits"
                );
            }
        }
    }

    fn send_payouts(&self) {
        if !self.cfg.payouts_enabled {
            tracing::warn!("payouts are disabled by config; skipping payout tick");
            return;
        }
        let pause_file = self.cfg.payout_pause_file.trim();
        if !pause_file.is_empty() && Path::new(pause_file).exists() {
            tracing::warn!(path = pause_file, "payouts paused by marker file");
            return;
        }
        if !self.ensure_wallet_ready() {
            return;
        }
        let reconciliation_blockers = match self.store.live_reconciliation_blockers() {
            Ok(blockers) => blockers,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to load live reconciliation blockers; skipping payout tick"
                );
                return;
            }
        };
        let missing_completed_payouts = if reconciliation_blockers.unresolved_completed_payout_rows
            > 0
        {
            let rows = match self.store.list_unreconciled_completed_payout_rows() {
                Ok(rows) => rows,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "failed to load unreconciled completed payouts; skipping payout tick"
                    );
                    return;
                }
            };
            match summarize_missing_completed_payout_rows(&rows, |tx_hash| {
                self.node.get_tx_status_optional(tx_hash)
            }) {
                Ok(summary) => summary,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "failed to verify unreconciled completed payouts against daemon; skipping payout tick"
                    );
                    return;
                }
            }
        } else {
            MissingCompletedPayoutSummary::default()
        };
        if reconciliation_blockers.orphaned_block_issue_count > 0
            || missing_completed_payouts.row_count > 0
        {
            tracing::warn!(
                orphaned_block_issue_count = reconciliation_blockers.orphaned_block_issue_count,
                orphaned_unpaid_amount = reconciliation_blockers.orphaned_unpaid_amount,
                unresolved_completed_payout_issue_count = missing_completed_payouts.issue_count,
                unresolved_completed_payout_rows = missing_completed_payouts.row_count,
                "payouts blocked by unresolved live reconciliation issues"
            );
            return;
        }

        let now = SystemTime::now();
        let payout_interval = bounded_payout_interval(self.cfg.payout_interval_duration());
        let min_amount = atomic_amount_from_coins(self.cfg.min_payout_amount);
        let max_recipients_per_tick = if self.cfg.payout_max_recipients_per_tick <= 0 {
            usize::MAX
        } else {
            self.cfg.payout_max_recipients_per_tick as usize
        };
        let max_total_per_tick = {
            let value = atomic_amount_from_coins(self.cfg.payout_max_total_per_tick);
            if value == 0 {
                u64::MAX
            } else {
                value
            }
        };
        let max_per_recipient = {
            let value = atomic_amount_from_coins(self.cfg.payout_max_per_recipient);
            if value == 0 {
                None
            } else {
                Some(value)
            }
        };
        let expected_address_network = self.expected_payout_address_network();
        let balances = match self.store.get_all_balances() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to list balances");
                return;
            }
        };
        let mut candidates = Vec::with_capacity(balances.len());

        for bal in balances {
            let existing_pending = match self.store.get_pending_payout(&bal.address) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(address = %bal.address, error = %err, "failed pending payout query");
                    continue;
                }
            };

            if let Err(reason) =
                validate_miner_address_for_network(&bal.address, expected_address_network)
            {
                if existing_pending.is_some() {
                    if let Err(err) = self.store.cancel_pending_payout(&bal.address) {
                        tracing::warn!(
                            address = %bal.address,
                            error = %err,
                            "failed to drop pending payout for invalid address"
                        );
                    } else {
                        tracing::warn!(
                            address = %bal.address,
                            reason = %reason,
                            "dropped pending payout for invalid address; skipping payout retries"
                        );
                    }
                }
                continue;
            }

            let desired_pending_amount = max_per_recipient
                .map(|cap| bal.pending.min(cap))
                .unwrap_or(bal.pending);

            let pending = match existing_pending {
                Some(v) if v.send_started_at.is_some() => v,
                refreshable_pending => {
                    if bal.pending < min_amount || desired_pending_amount == 0 {
                        if refreshable_pending.is_some() {
                            if let Err(err) = self.store.cancel_pending_payout(&bal.address) {
                                tracing::warn!(
                                    address = %bal.address,
                                    error = %err,
                                    "failed to drop refreshable pending payout below threshold"
                                );
                            }
                        }
                        continue;
                    }
                    if let Err(err) = self
                        .store
                        .create_pending_payout(&bal.address, desired_pending_amount)
                    {
                        tracing::warn!(address = %bal.address, error = %err, "failed to queue pending payout");
                        continue;
                    }
                    match self.store.get_pending_payout(&bal.address) {
                        Ok(Some(v)) => v,
                        Ok(None) => continue,
                        Err(err) => {
                            tracing::warn!(
                                address = %bal.address,
                                error = %err,
                                "failed to reload pending payout after refresh"
                            );
                            continue;
                        }
                    }
                }
            };

            let pending_amount = pending.amount;
            if pending_amount == 0 {
                continue;
            }

            candidates.push(PayoutCandidate {
                balance: bal,
                pending,
            });
        }

        sort_payout_candidates(&mut candidates, now, PAYOUT_WAIT_PRIORITY_THRESHOLD);

        let mut queued = candidates
            .into_iter()
            .filter(|candidate| {
                candidate.pending.tx_hash.is_none() && candidate.pending.send_started_at.is_none()
            })
            .collect::<Vec<_>>();

        let wallet_balance = match self.node.get_wallet_balance() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed wallet balance check");
                return;
            }
        };
        let wallet_outputs = match self.node.get_wallet_outputs() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed wallet outputs check");
                return;
            }
        };
        let recent_stats = self.load_recent_payout_stats();
        let mut spendable_outputs = wallet_outputs
            .outputs
            .into_iter()
            .filter(|output| output.status.eq_ignore_ascii_case("unspent"))
            .collect::<Vec<_>>();
        let mut inventory = build_output_inventory(&spendable_outputs, min_amount, &recent_stats);
        let min_reserve_floor = min_amount
            .saturating_mul(4)
            .max(inventory.medium_target_amount)
            .max(MIN_PAYOUT_FEE_BUFFER.saturating_mul(16));
        let reserve_target = recent_stats
            .median_batch_total
            .max(min_reserve_floor)
            .max(recent_stats.p90_recipient_amount);
        let oldest_age = queued
            .first()
            .map(|candidate| payout_wait_age(&candidate.pending, now))
            .unwrap_or_default();
        let initial_spendable = sum_output_amounts(&spendable_outputs);
        let mut safe_spend_budget = compute_safe_spend_budget(
            initial_spendable,
            reserve_target,
            oldest_age,
            payout_interval,
        );
        let mut planned_batch_count = 0usize;
        let mut planned_recipient_count = 0usize;
        let mut rebalance_active = false;
        let rebalance_required = !inventory_is_healthy(&inventory);
        self.update_liquidity_runtime_snapshot(LiquidityRuntimeSnapshot {
            reserve_target_amount: reserve_target,
            safe_spend_budget,
            inventory: &inventory,
            planned_batch_count,
            planned_recipient_count,
            rebalance_required,
            rebalance_active,
        });

        if queued.is_empty() {
            if rebalance_required {
                rebalance_active = self.maybe_rebalance_outputs(
                    &wallet_balance,
                    &spendable_outputs,
                    &inventory,
                    reserve_target,
                );
            }
            self.update_liquidity_runtime_snapshot(LiquidityRuntimeSnapshot {
                reserve_target_amount: reserve_target,
                safe_spend_budget,
                inventory: &inventory,
                planned_batch_count,
                planned_recipient_count,
                rebalance_required,
                rebalance_active,
            });
            return;
        }

        let target_recipients_per_tx = recent_stats
            .p90_recipient_count
            .max(DEFAULT_TARGET_RECIPIENTS_PER_TX)
            .clamp(8, MAX_RECIPIENTS_PER_TX)
            .min(max_recipients_per_tick.max(1));
        let target_recipients = target_recipients_per_tx.max(1);
        let max_txs_per_sweep = (max_recipients_per_tick
            .max(1)
            .saturating_add(target_recipients - 1)
            / target_recipients)
            .clamp(1, MAX_TXS_PER_SWEEP);
        let mut sent_recipients = 0usize;
        let mut sent_total = 0u64;
        let mut sent_txs = 0usize;
        let mut liquidity_backoff_budget = None::<u64>;
        let mut last_wallet_send_attempt = None::<Instant>;

        while !queued.is_empty()
            && sent_txs < max_txs_per_sweep
            && sent_recipients < max_recipients_per_tick
            && sent_total < max_total_per_tick
        {
            let remaining_recipient_cap = max_recipients_per_tick.saturating_sub(sent_recipients);
            if remaining_recipient_cap == 0 || spendable_outputs.is_empty() {
                break;
            }

            let current_spendable = sum_output_amounts(&spendable_outputs);
            let oldest_age = payout_wait_age(&queued[0].pending, now);
            safe_spend_budget = compute_safe_spend_budget(
                current_spendable,
                reserve_target,
                oldest_age,
                payout_interval,
            );
            if let Some(backoff_budget) = liquidity_backoff_budget {
                safe_spend_budget = safe_spend_budget.min(backoff_budget);
            }
            if safe_spend_budget <= MIN_PAYOUT_FEE_BUFFER {
                break;
            }

            let mut batch_candidates = queued
                .iter()
                .take(target_recipients_per_tx.min(remaining_recipient_cap))
                .cloned()
                .collect::<Vec<_>>();
            let total_cap = max_total_per_tick.saturating_sub(sent_total);
            while total_cap != u64::MAX
                && batch_candidates.iter().fold(0u64, |acc, candidate| {
                    acc.saturating_add(candidate.pending.amount)
                }) > total_cap
            {
                if batch_candidates.pop().is_none() {
                    break;
                }
            }
            if batch_candidates.is_empty() {
                break;
            }

            let plan = loop {
                let queue_light = queued.len() <= (target_recipients_per_tx.max(2) / 2);
                match self.plan_payout_batch(
                    &batch_candidates,
                    &spendable_outputs,
                    &inventory,
                    safe_spend_budget,
                    queue_light,
                ) {
                    Some(plan) => break Some(plan),
                    None if batch_candidates.len() > 1 => {
                        batch_candidates.pop();
                    }
                    None => {
                        let Some(candidate) = batch_candidates.first().cloned() else {
                            break None;
                        };
                        if let Some(resized) = self.try_reduce_pending_candidate_for_liquidity(
                            &candidate,
                            &spendable_outputs,
                            &inventory,
                            safe_spend_budget,
                            min_amount,
                            queue_light,
                        ) {
                            if let Some(front) = queued.first_mut() {
                                *front = resized.clone();
                            }
                            batch_candidates = vec![resized];
                            continue;
                        }
                        queued.remove(0);
                        break None;
                    }
                }
            };
            let Some(plan) = plan else {
                continue;
            };

            planned_batch_count = planned_batch_count.saturating_add(1);
            planned_recipient_count = planned_recipient_count.saturating_add(plan.recipients.len());

            let addresses = plan
                .recipients
                .iter()
                .map(|candidate| candidate.balance.address.clone())
                .collect::<Vec<_>>();
            if let Err(err) = self
                .store
                .mark_pending_payouts_send_started_batch(&addresses, &plan.batch_id)
            {
                tracing::warn!(
                    batch_id = %plan.batch_id,
                    error = %err,
                    "failed to freeze payout batch before send"
                );
                break;
            }

            if let Some(last_attempt) = last_wallet_send_attempt {
                let elapsed = last_attempt.elapsed();
                if elapsed < MIN_WALLET_SEND_SPACING {
                    std::thread::sleep(MIN_WALLET_SEND_SPACING - elapsed);
                }
            }
            last_wallet_send_attempt = Some(Instant::now());

            let recipients = plan
                .recipients
                .iter()
                .map(|candidate| WalletRecipient {
                    address: candidate.balance.address.clone(),
                    amount: candidate.pending.amount,
                })
                .collect::<Vec<_>>();
            let inputs = plan
                .inputs
                .iter()
                .map(WalletOutputRef::from)
                .collect::<Vec<_>>();

            let sent = match self.node.wallet_send_advanced(
                &recipients,
                &inputs,
                plan.change_split,
                &plan.idempotency_key,
                false,
            ) {
                Ok(sent) => sent,
                Err(err) => {
                    if should_drop_pending_payout(&err) {
                        for candidate in &plan.recipients {
                            if let Err(cancel_err) =
                                self.store.cancel_pending_payout(&candidate.balance.address)
                            {
                                tracing::warn!(
                                    address = %candidate.balance.address,
                                    error = %cancel_err,
                                    "wallet send failed permanently and pending payout cancel failed"
                                );
                            }
                        }
                        tracing::warn!(
                            batch_id = %plan.batch_id,
                            recipients = plan.recipients.len(),
                            error = %err,
                            "wallet send failed permanently; dropped payout batch"
                        );
                    } else if is_wallet_stale_input_error(&err) {
                        if let Err(reset_err) = self
                            .store
                            .reset_pending_payout_batch_send_state(&plan.batch_id)
                        {
                            tracing::warn!(
                                batch_id = %plan.batch_id,
                                error = %reset_err,
                                "wallet stale-input retry could not reset payout batch"
                            );
                        }
                        remove_spent_outputs(&mut spendable_outputs, &plan.inputs);
                        inventory =
                            build_output_inventory(&spendable_outputs, min_amount, &recent_stats);
                        tracing::info!(
                            batch_id = %plan.batch_id,
                            recipients = plan.recipients.len(),
                            recipient_total = plan.recipient_total,
                            rejected_inputs = plan.inputs.len(),
                            error = %err,
                            "wallet rejected stale payout inputs; retrying with alternate inputs"
                        );
                        continue;
                    } else if is_wallet_send_retryable_error(&err) {
                        if let Err(reset_err) = self
                            .store
                            .reset_pending_payout_batch_send_state(&plan.batch_id)
                        {
                            tracing::warn!(
                                batch_id = %plan.batch_id,
                                error = %reset_err,
                                "wallet send retry could not reset payout batch"
                            );
                        }
                        tracing::info!(
                            batch_id = %plan.batch_id,
                            recipients = plan.recipients.len(),
                            recipient_total = plan.recipient_total,
                            error = %err,
                            "wallet send temporarily unavailable; deferring payout batch retry until next tick"
                        );
                        break;
                    } else if is_wallet_liquidity_error(&err) {
                        if let Err(reset_err) = self
                            .store
                            .reset_pending_payout_batch_send_state(&plan.batch_id)
                        {
                            tracing::warn!(
                                batch_id = %plan.batch_id,
                                error = %reset_err,
                                "wallet liquidity failure could not reset payout batch"
                            );
                        }
                        let next_backoff_budget = plan.locked_amount.saturating_sub(1);
                        if next_backoff_budget > 0 {
                            liquidity_backoff_budget = Some(
                                liquidity_backoff_budget
                                    .map(|existing| existing.min(next_backoff_budget))
                                    .unwrap_or(next_backoff_budget),
                            );
                        }
                        tracing::info!(
                            batch_id = %plan.batch_id,
                            locked_amount = plan.locked_amount,
                            recipient_total = plan.recipient_total,
                            retry_budget = liquidity_backoff_budget.unwrap_or_default(),
                            error = %err,
                            "wallet liquidity insufficient for planned batch; backing off to a smaller retry"
                        );
                        continue;
                    } else {
                        tracing::warn!(
                            batch_id = %plan.batch_id,
                            error = %err,
                            "advanced wallet send failed; retaining idempotent retry state"
                        );
                    }
                    queued.drain(..plan.recipients.len());
                    continue;
                }
            };

            let fee_members = allocate_batch_fees(&plan.recipients, sent.fee);
            if let Err(err) = self.store.record_pending_payout_batch_broadcast(
                &fee_members,
                &plan.batch_id,
                &sent.txid,
            ) {
                tracing::error!(
                    batch_id = %plan.batch_id,
                    tx = %sent.txid,
                    error = %err,
                    "critical payout batch broadcast persistence failure"
                );
                queued.drain(..plan.recipients.len());
                continue;
            }

            sent_txs = sent_txs.saturating_add(1);
            sent_recipients = sent_recipients.saturating_add(plan.recipients.len());
            sent_total = sent_total.saturating_add(plan.recipient_total);
            liquidity_backoff_budget = None;
            remove_spent_outputs(&mut spendable_outputs, &plan.inputs);
            inventory = build_output_inventory(&spendable_outputs, min_amount, &recent_stats);

            tracing::info!(
                batch_id = %plan.batch_id,
                recipients = plan.recipients.len(),
                amount = plan.recipient_total,
                fee = sent.fee,
                change_split = plan.change_split,
                locked_amount = plan.locked_amount,
                tx = %sent.txid,
                idempotency_key = %plan.idempotency_key,
                "payout batch broadcast"
            );
            queued.drain(..plan.recipients.len());
        }

        if queued.is_empty() && !inventory_is_healthy(&inventory) {
            rebalance_active = self.maybe_rebalance_outputs(
                &wallet_balance,
                &spendable_outputs,
                &inventory,
                reserve_target,
            );
        }

        safe_spend_budget = compute_safe_spend_budget(
            sum_output_amounts(&spendable_outputs),
            reserve_target,
            oldest_age,
            payout_interval,
        );
        self.update_liquidity_runtime_snapshot(LiquidityRuntimeSnapshot {
            reserve_target_amount: reserve_target,
            safe_spend_budget,
            inventory: &inventory,
            planned_batch_count,
            planned_recipient_count,
            rebalance_required: !inventory_is_healthy(&inventory),
            rebalance_active,
        });
    }

    fn reconcile_pending_payouts(&self) {
        let pending = match self.store.get_pending_payouts() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read pending payouts for reconciliation");
                return;
            }
        };

        let pending_by_batch = pending
            .iter()
            .filter_map(|entry| {
                entry
                    .batch_id
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .map(|batch_id| (batch_id.to_string(), entry.clone()))
            })
            .fold(
                HashMap::<String, Vec<PendingPayout>>::new(),
                |mut batches, (batch_id, entry)| {
                    batches.entry(batch_id).or_default().push(entry);
                    batches
                },
            );

        let now = SystemTime::now();
        let mut grouped = HashMap::<String, Vec<PendingPayout>>::new();
        let mut warned_batches = HashSet::<String>::new();
        for entry in pending {
            if let Some(tx_hash) = entry
                .tx_hash
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                let key = entry
                    .batch_id
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| format!("tx:{tx_hash}"));
                grouped.entry(key).or_default().push(entry);
                continue;
            }

            if !has_stale_unknown_broadcast_outcome(&entry, now) {
                continue;
            }

            let send_age = pending_send_age(&entry, now);
            if let Some(batch_id) = entry
                .batch_id
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                if warned_batches.insert(batch_id.to_string()) {
                    if let Some(batch_entries) = pending_by_batch.get(batch_id) {
                        match self
                            .recover_pending_batch_from_daemon_idempotency(batch_id, batch_entries)
                        {
                            Ok(Some(recorded)) => {
                                tracing::info!(
                                    batch_id = %batch_id,
                                    recipients = recorded.len(),
                                    send_age_secs = send_age.as_secs(),
                                    "recovered unknown payout batch from daemon idempotency journal"
                                );
                                grouped.insert(batch_id.to_string(), recorded);
                                continue;
                            }
                            Ok(None) => {}
                            Err(err) => {
                                tracing::warn!(
                                    batch_id = %batch_id,
                                    send_age_secs = send_age.as_secs(),
                                    error = %err,
                                    "failed recovering unknown payout batch from daemon idempotency journal"
                                );
                            }
                        }
                    }
                    tracing::warn!(
                        batch_id = %batch_id,
                        send_age_secs = send_age.as_secs(),
                        "stale pre-broadcast payout batch has unknown broadcast outcome; leaving queued for reconciliation"
                    );
                }
            } else {
                tracing::warn!(
                    address = %entry.address,
                    amount = entry.amount,
                    send_age_secs = send_age.as_secs(),
                    "stale pre-broadcast pending payout has unknown broadcast outcome; leaving queued for reconciliation"
                );
            }
        }

        for entries in grouped.into_values() {
            if let Err(err) = self.reconcile_pending_batch(&entries) {
                let batch_id = entries
                    .first()
                    .and_then(|entry| entry.batch_id.as_deref())
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("legacy");
                tracing::warn!(
                    batch_id = %batch_id,
                    error = %err,
                    "failed to reconcile pending payout batch"
                );
            }
        }
    }

    fn recover_pending_batch_from_daemon_idempotency(
        &self,
        batch_id: &str,
        entries: &[PendingPayout],
    ) -> anyhow::Result<Option<Vec<PendingPayout>>> {
        let daemon_data_dir = self.cfg.daemon_data_dir.trim();
        let idempotency_path = PathBuf::from(if daemon_data_dir.is_empty() {
            "data"
        } else {
            daemon_data_dir
        })
        .join("send-idempotency.json");
        let Some(recovered) = load_recovered_wallet_send_for_batch(&idempotency_path, batch_id)?
        else {
            return Ok(None);
        };
        validate_recovered_wallet_send_matches_pending(&recovered, entries)?;
        let members = entries
            .iter()
            .zip(allocate_proportional_fees(
                entries.iter().map(|entry| entry.amount),
                recovered.fee,
            ))
            .map(|(entry, fee)| PendingPayoutBatchMember {
                address: entry.address.clone(),
                amount: entry.amount,
                fee,
            })
            .collect::<Vec<_>>();
        let recorded = self.store.record_pending_payout_batch_broadcast(
            &members,
            batch_id,
            &recovered.txid,
        )?;
        Ok(Some(recorded))
    }

    fn reconcile_pending_batch(&self, entries: &[PendingPayout]) -> anyhow::Result<()> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let Some(tx_hash) = first
            .tx_hash
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(());
        };
        let batch_id = first
            .batch_id
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        let status = self.node.get_tx_status_optional(tx_hash)?;
        match status {
            Some(status) if status.in_mempool => Ok(()),
            Some(status) if status.confirmations >= PAYOUT_CONFIRMATIONS_REQUIRED => {
                if let Some(batch_id) = batch_id {
                    self.store
                        .complete_pending_payout_batch(batch_id, tx_hash)?;
                } else {
                    for pending in entries {
                        self.store.complete_pending_payout(
                            &pending.address,
                            pending.amount,
                            pending.fee.unwrap_or(0),
                            tx_hash,
                        )?;
                    }
                }
                let total_amount = entries
                    .iter()
                    .fold(0u64, |acc, entry| acc.saturating_add(entry.amount));
                let total_fee = entries.iter().fold(0u64, |acc, entry| {
                    acc.saturating_add(entry.fee.unwrap_or(0))
                });
                tracing::info!(
                    batch_id = %batch_id.unwrap_or("legacy"),
                    recipients = entries.len(),
                    amount = total_amount,
                    fee = total_fee,
                    tx = %tx_hash,
                    confirmations = status.confirmations,
                    "payout batch confirmed"
                );
                Ok(())
            }
            Some(_) => Ok(()),
            None => {
                let age = entries
                    .iter()
                    .filter_map(|pending| pending.sent_at.or(pending.send_started_at))
                    .filter_map(|sent_at| SystemTime::now().duration_since(sent_at).ok())
                    .max()
                    .unwrap_or_default();
                if age < PENDING_PAYOUT_RETRY_GRACE {
                    return Ok(());
                }
                if let Some(batch_id) = batch_id {
                    self.store.reset_pending_payout_batch_send_state(batch_id)?;
                    tracing::warn!(
                        batch_id = %batch_id,
                        recipients = entries.len(),
                        tx = %tx_hash,
                        age_secs = age.as_secs(),
                        "broadcast payout batch disappeared from mempool/chain; reset for retry"
                    );
                } else {
                    for pending in entries {
                        self.store
                            .reset_pending_payout_send_state(&pending.address)?;
                    }
                    tracing::warn!(
                        tx = %tx_hash,
                        recipients = entries.len(),
                        age_secs = age.as_secs(),
                        "broadcast payout batch disappeared from mempool/chain; reset legacy rows for retry"
                    );
                }
                Ok(())
            }
        }
    }

    fn update_liquidity_runtime_snapshot(&self, snapshot: LiquidityRuntimeSnapshot<'_>) {
        let mut runtime = self.runtime.write();
        runtime.reserve_target_amount = snapshot.reserve_target_amount;
        runtime.safe_spend_budget = snapshot.safe_spend_budget;
        runtime.spendable_output_count = snapshot.inventory.spendable_count;
        runtime.small_output_count = snapshot.inventory.small_count;
        runtime.medium_output_count = snapshot.inventory.medium_count;
        runtime.large_output_count = snapshot.inventory.large_count;
        runtime.planned_batch_count = snapshot.planned_batch_count;
        runtime.planned_recipient_count = snapshot.planned_recipient_count;
        runtime.rebalance_required = snapshot.rebalance_required;
        runtime.rebalance_active = snapshot.rebalance_active;
        runtime.inventory_health = inventory_health_label(snapshot.inventory).to_string();
    }

    fn load_recent_payout_stats(&self) -> RecentPayoutStats {
        let payouts = match self.store.get_recent_payouts(256) {
            Ok(payouts) => payouts,
            Err(err) => {
                tracing::warn!(error = %err, "failed to load recent payouts for adaptive planning");
                return RecentPayoutStats::default();
            }
        };
        derive_recent_payout_stats(&payouts)
    }

    fn plan_payout_batch(
        &self,
        candidates: &[PayoutCandidate],
        spendable_outputs: &[WalletOutput],
        inventory: &OutputInventory,
        safe_spend_budget: u64,
        queue_light: bool,
    ) -> Option<PlannedPayoutBatch> {
        plan_payout_batch_with_node(
            self.node.as_ref(),
            candidates,
            spendable_outputs,
            inventory,
            safe_spend_budget,
            queue_light,
        )
    }

    fn try_reduce_pending_candidate_for_liquidity(
        &self,
        candidate: &PayoutCandidate,
        spendable_outputs: &[WalletOutput],
        inventory: &OutputInventory,
        safe_spend_budget: u64,
        min_amount: u64,
        queue_light: bool,
    ) -> Option<PayoutCandidate> {
        if candidate.pending.send_started_at.is_some()
            || candidate.pending.amount <= min_amount.max(1)
            || safe_spend_budget <= MIN_PAYOUT_FEE_BUFFER
        {
            return None;
        }

        let best_amount = find_reduced_payout_amount_for_liquidity(
            self.node.as_ref(),
            candidate,
            spendable_outputs,
            inventory,
            safe_spend_budget,
            min_amount,
            queue_light,
        )?;
        if best_amount >= candidate.pending.amount {
            return None;
        }

        if let Err(err) = self
            .store
            .create_pending_payout(&candidate.balance.address, best_amount)
        {
            tracing::warn!(
                address = %candidate.balance.address,
                original_amount = candidate.pending.amount,
                reduced_amount = best_amount,
                error = %err,
                "failed to resize pending payout for constrained wallet liquidity"
            );
            return None;
        }

        let pending = match self.store.get_pending_payout(&candidate.balance.address) {
            Ok(Some(pending)) => pending,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(
                    address = %candidate.balance.address,
                    original_amount = candidate.pending.amount,
                    reduced_amount = best_amount,
                    error = %err,
                    "failed to reload resized pending payout"
                );
                return None;
            }
        };

        tracing::info!(
            address = %candidate.balance.address,
            original_amount = candidate.pending.amount,
            reduced_amount = best_amount,
            safe_spend_budget,
            "reduced queued payout amount to fit current wallet liquidity"
        );

        Some(PayoutCandidate {
            balance: candidate.balance.clone(),
            pending,
        })
    }

    fn maybe_rebalance_outputs(
        &self,
        wallet_balance: &crate::node::WalletBalance,
        spendable_outputs: &[WalletOutput],
        inventory: &OutputInventory,
        reserve_target: u64,
    ) -> bool {
        if inventory_is_healthy(inventory) || spendable_outputs.is_empty() {
            return false;
        }
        let destination = match self.rebalance_destination() {
            Some(destination) => destination,
            None => return false,
        };

        let safe_spend_budget = compute_safe_spend_budget(
            wallet_balance.spendable,
            reserve_target,
            Duration::ZERO,
            Duration::from_secs(1),
        );
        let rebalance_cap = (wallet_balance.spendable / REBALANCE_SPENDABLE_SHARE_DENOMINATOR)
            .saturating_mul(REBALANCE_SPENDABLE_SHARE_NUMERATOR);
        let max_locked_amount = safe_spend_budget.min(rebalance_cap);
        if max_locked_amount
            <= inventory
                .small_target_amount
                .saturating_add(MIN_PAYOUT_FEE_BUFFER)
        {
            return false;
        }

        let input = spendable_outputs
            .iter()
            .filter(|output| {
                output.amount >= inventory.small_target_amount.saturating_mul(2)
                    && output.amount <= max_locked_amount
            })
            .min_by_key(|output| output.amount)
            .cloned()
            .or_else(|| {
                spendable_outputs
                    .iter()
                    .filter(|output| output.amount <= max_locked_amount)
                    .max_by_key(|output| output.amount)
                    .cloned()
            });
        let Some(input) = input else {
            return false;
        };

        let change_split = choose_change_split(inventory, input.amount, true).max(2);
        let recipient_amount = input
            .amount
            .saturating_mul(3)
            .checked_div(5)
            .unwrap_or_default()
            .max(inventory.small_target_amount);
        if recipient_amount <= MIN_PAYOUT_FEE_BUFFER {
            return false;
        }

        let idempotency_key = sha256_hex(format!(
            "blocknet-pool:rebalance:{}:{}:{}",
            input.txid, input.output_index, input.amount
        ));
        let recipients = vec![WalletRecipient {
            address: destination.clone(),
            amount: recipient_amount,
        }];
        let inputs = vec![WalletOutputRef::from(&input)];
        let preview = match self.node.wallet_send_advanced(
            &recipients,
            &inputs,
            change_split,
            &idempotency_key,
            true,
        ) {
            Ok(preview) => preview,
            Err(err) => {
                if is_wallet_send_retryable_error(&err) {
                    tracing::info!(
                        error = %err,
                        "wallet send temporarily unavailable; skipping wallet rebalance this tick"
                    );
                } else {
                    tracing::warn!(error = %err, "failed to preview wallet rebalance");
                }
                return false;
            }
        };
        if input.amount <= recipient_amount.saturating_add(preview.fee) {
            return false;
        }

        match self.node.wallet_send_advanced(
            &recipients,
            &inputs,
            change_split,
            &idempotency_key,
            false,
        ) {
            Ok(sent) => {
                tracing::info!(
                    tx = %sent.txid,
                    input_amount = input.amount,
                    recipient_amount,
                    fee = sent.fee,
                    change_split,
                    "broadcast idle wallet rebalance to daemon wallet"
                );
                true
            }
            Err(err) => {
                tracing::warn!(error = %err, "wallet rebalance send failed");
                false
            }
        }
    }

    fn rebalance_destination(&self) -> Option<String> {
        let wallet = match self.node.get_wallet_address() {
            Ok(wallet) => wallet,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to resolve daemon wallet address for wallet rebalance"
                );
                return None;
            }
        };

        resolve_wallet_rebalance_destination(&wallet, &self.cfg.pool_fee_wallet_address)
            .map(str::to_string)
    }

    fn ensure_wallet_ready(&self) -> bool {
        if self.node.get_wallet_balance().is_ok() {
            return self.validate_daemon_wallet_address();
        }

        let password = std::env::var("BLOCKNET_WALLET_PASSWORD")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let Some(password) = password else {
            return false;
        };

        let probe = match self.node.get_wallet_balance() {
            Ok(_) => return self.validate_daemon_wallet_address(),
            Err(e) => e,
        };

        if is_http_status(&probe, 403) {
            if self.node.wallet_unlock(&password).is_err() {
                return false;
            }
        } else if is_http_status(&probe, 503) {
            if self.node.wallet_load(&password).is_err() {
                return false;
            }
            if self.node.wallet_unlock(&password).is_err() {
                return false;
            }
        } else {
            return false;
        }

        self.node.get_wallet_balance().is_ok() && self.validate_daemon_wallet_address()
    }

    fn validate_daemon_wallet_address(&self) -> bool {
        match self.node.get_wallet_address() {
            Ok(addr) => !addr.address.trim().is_empty() && !addr.view_only,
            Err(_) => false,
        }
    }

    fn expected_payout_address_network(&self) -> Option<AddressNetwork> {
        if let Some(network) = self.configured_address_network {
            return Some(network);
        }

        let wallet = match self.node.get_wallet_address() {
            Ok(wallet) => wallet,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to resolve daemon wallet address for payout validation"
                );
                return None;
            }
        };

        match address_network(wallet.address.trim()) {
            Ok(network) => network,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "daemon wallet address is not a valid payout network anchor; falling back to generic validation"
                );
                None
            }
        }
    }
}

fn record_payout_task_observation(
    task_metrics: Option<&Arc<NamedTimedOperationTracker>>,
    task: &str,
    duration: Duration,
    failed: bool,
) {
    if let Some(task_metrics) = task_metrics {
        task_metrics.record(
            task,
            duration,
            failed,
            duration >= PAYOUT_TASK_SLOW_LOG_AFTER,
        );
    }
    if failed {
        tracing::warn!(
            component = "runtime_perf",
            operation = task,
            duration_ms = duration.as_millis() as u64,
            "payout runtime task failed"
        );
    } else if duration >= PAYOUT_TASK_SLOW_LOG_AFTER {
        tracing::info!(
            component = "runtime_perf",
            operation = task,
            duration_ms = duration.as_millis() as u64,
            "payout runtime task observed"
        );
    }
}

fn configured_payout_address_network(cfg: &Config) -> Option<AddressNetwork> {
    let configured = cfg.pool_fee_wallet_address.trim();
    if configured.is_empty() {
        return None;
    }

    match address_network(configured) {
        Ok(network) => network,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "configured pool_fee_wallet_address is not a valid payout network anchor; falling back to daemon wallet validation"
            );
            None
        }
    }
}

fn resolve_wallet_rebalance_destination<'a>(
    wallet: &'a WalletAddressResponse,
    pool_fee_wallet_address: &str,
) -> Option<&'a str> {
    let destination = wallet.address.trim();
    if destination.is_empty() {
        tracing::warn!("daemon wallet address is empty; skipping wallet rebalance");
        return None;
    }
    if wallet.view_only {
        tracing::warn!("daemon wallet is view-only; skipping wallet rebalance");
        return None;
    }

    let fee_wallet = pool_fee_wallet_address.trim();
    if !fee_wallet.is_empty() && destination == fee_wallet {
        tracing::warn!(
            "daemon wallet address matches pool fee wallet address; skipping wallet rebalance"
        );
        return None;
    }

    Some(destination)
}

pub(crate) fn resolve_pool_fee_destination(cfg: &Config, _block: &DbBlock) -> Option<String> {
    let configured = cfg.pool_fee_wallet_address.trim();
    if !configured.is_empty() {
        return Some(configured.to_string());
    }
    None
}

pub fn reward_window_end(store: &PoolStore, block: &DbBlock) -> anyhow::Result<SystemTime> {
    Ok(store
        .latest_share_timestamp_for_block_hash(&block.hash)?
        .map(|share_time| share_time.max(block.timestamp))
        .unwrap_or(block.timestamp))
}

pub fn recover_share_window_by_replay(
    store: &PoolStore,
    shares: &mut [DbShare],
    now: SystemTime,
    provisional_delay: Duration,
    persist: bool,
) -> anyhow::Result<ShareReplayRecovery> {
    let replay_share_indexes = shares
        .iter()
        .enumerate()
        .filter_map(|(idx, share)| {
            (share.status != SHARE_STATUS_REJECTED
                && !share.was_sampled
                && is_share_payout_eligible(share, now, provisional_delay))
            .then_some(idx)
        })
        .collect::<Vec<_>>();
    if replay_share_indexes.is_empty() {
        return Ok(ShareReplayRecovery::default());
    }

    let job_ids = replay_share_indexes
        .iter()
        .map(|idx| shares[*idx].job_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let replays = store.get_share_replays_for_job_ids(&job_ids)?;
    let missing = job_ids
        .iter()
        .filter(|job_id| !replays.contains_key(*job_id))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        anyhow::bail!(
            "missing replay data for {} assignment(s): {}",
            missing.len(),
            missing.join(", ")
        );
    }

    let hasher = Argon2PowHasher::default();
    let mut recovery = ShareReplayRecovery {
        attempted: true,
        ..ShareReplayRecovery::default()
    };
    let mut updates = Vec::<ShareReplayUpdate>::with_capacity(replay_share_indexes.len());
    for idx in replay_share_indexes {
        let share = &shares[idx];
        let replay = replays.get(&share.job_id).ok_or_else(|| {
            anyhow::anyhow!("missing replay data for assignment {}", share.job_id)
        })?;
        let hash = hasher
            .hash(&replay.header_base, share.nonce)
            .map_err(|err| {
                anyhow::anyhow!(
                    "replay hash failed for share {} (assignment {}): {err}",
                    share.id,
                    share.job_id
                )
            })?;
        let accepted = check_target(hash, difficulty_to_target(share.difficulty));
        let candidate_valid = check_target(hash, replay.network_target);
        if share.block_hash.is_some() && !candidate_valid {
            anyhow::bail!(
                "candidate share {} failed replay verification for assignment {}",
                share.id,
                share.job_id
            );
        }
        if let Some(expected_hash) = share.block_hash.as_deref() {
            let computed_hash = hex::encode(hash);
            if !expected_hash.eq_ignore_ascii_case(&computed_hash) {
                anyhow::bail!(
                    "candidate share {} replay hash mismatch for assignment {}",
                    share.id,
                    share.job_id
                );
            }
        }

        if accepted {
            updates.push(ShareReplayUpdate {
                share_id: share.id,
                status: SHARE_STATUS_VERIFIED.to_string(),
                was_sampled: true,
                reject_reason: None,
            });
            shares[idx].status = SHARE_STATUS_VERIFIED.to_string();
            shares[idx].was_sampled = true;
            recovery.verified = recovery.verified.saturating_add(1);
        } else {
            updates.push(ShareReplayUpdate {
                share_id: share.id,
                status: SHARE_STATUS_REJECTED.to_string(),
                was_sampled: true,
                reject_reason: Some("low difficulty share".to_string()),
            });
            shares[idx].status = SHARE_STATUS_REJECTED.to_string();
            shares[idx].was_sampled = true;
            recovery.rejected = recovery.rejected.saturating_add(1);
        }
    }

    if persist {
        store.apply_share_replay_updates(&updates)?;
    }

    Ok(recovery)
}

fn sort_payout_candidates(
    candidates: &mut [PayoutCandidate],
    now: SystemTime,
    wait_priority_threshold: Duration,
) {
    candidates.sort_by(|a, b| compare_payout_candidates(a, b, now, wait_priority_threshold));
}

fn compare_payout_candidates(
    a: &PayoutCandidate,
    b: &PayoutCandidate,
    now: SystemTime,
    wait_priority_threshold: Duration,
) -> Ordering {
    let a_age = payout_wait_age(&a.pending, now);
    let b_age = payout_wait_age(&b.pending, now);
    let a_promoted = wait_priority_threshold.is_zero() || a_age >= wait_priority_threshold;
    let b_promoted = wait_priority_threshold.is_zero() || b_age >= wait_priority_threshold;

    match (a_promoted, b_promoted) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => b_age
            .cmp(&a_age)
            .then_with(|| b.pending.amount.cmp(&a.pending.amount))
            .then_with(|| a.balance.address.cmp(&b.balance.address)),
    }
}

fn payout_wait_age(p: &PendingPayout, now: SystemTime) -> Duration {
    now.duration_since(p.initiated_at).unwrap_or_default()
}

fn pending_send_age(p: &PendingPayout, now: SystemTime) -> Duration {
    p.send_started_at
        .and_then(|ts| now.duration_since(ts).ok())
        .unwrap_or_default()
}

fn has_stale_unknown_broadcast_outcome(pending: &PendingPayout, now: SystemTime) -> bool {
    pending
        .tx_hash
        .as_deref()
        .is_none_or(|tx| tx.trim().is_empty())
        && pending.send_started_at.is_some()
        && pending_send_age(pending, now) >= PENDING_PAYOUT_RETRY_GRACE
}

fn sum_output_amounts(outputs: &[WalletOutput]) -> u64 {
    outputs
        .iter()
        .fold(0u64, |acc, output| acc.saturating_add(output.amount))
}

fn remove_spent_outputs(spendable_outputs: &mut Vec<WalletOutput>, spent: &[WalletOutput]) {
    let spent_keys = spent
        .iter()
        .map(|output| (output.txid.as_str(), output.output_index))
        .collect::<HashSet<_>>();
    spendable_outputs
        .retain(|output| !spent_keys.contains(&(output.txid.as_str(), output.output_index)));
}

fn derive_recent_payout_stats(payouts: &[Payout]) -> RecentPayoutStats {
    if payouts.is_empty() {
        return RecentPayoutStats::default();
    }

    let mut grouped = HashMap::<String, (u64, usize)>::new();
    let mut recipient_amounts = payouts
        .iter()
        .map(|payout| payout.amount)
        .filter(|amount| *amount > 0)
        .collect::<Vec<_>>();
    for payout in payouts {
        let Some(batch_id) = &payout.batch_id else {
            continue;
        };
        let entry = grouped.entry(batch_id.clone()).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(payout.amount);
        entry.1 = entry.1.saturating_add(1);
    }

    let mut batch_totals = grouped
        .values()
        .map(|(total, _)| *total)
        .collect::<Vec<_>>();
    let mut recipient_counts = grouped
        .values()
        .map(|(_, recipients)| *recipients)
        .collect::<Vec<_>>();
    batch_totals.sort_unstable();
    recipient_counts.sort_unstable();
    recipient_amounts.sort_unstable();

    RecentPayoutStats {
        median_batch_total: quantile(&batch_totals, 1, 2),
        p90_batch_total: quantile(&batch_totals, 9, 10),
        p50_recipient_count: quantile(&recipient_counts, 1, 2),
        p90_recipient_count: quantile(&recipient_counts, 9, 10),
        median_recipient_amount: quantile(&recipient_amounts, 1, 2),
        p90_recipient_amount: quantile(&recipient_amounts, 9, 10),
    }
}

fn quantile<T: Copy + Default>(values: &[T], numerator: usize, denominator: usize) -> T {
    if values.is_empty() {
        return T::default();
    }
    let idx = ((values.len() - 1) * numerator) / denominator.max(1);
    values[idx]
}

fn compute_safe_spend_budget(
    spendable: u64,
    reserve_target: u64,
    oldest_age: Duration,
    payout_interval: Duration,
) -> u64 {
    let mut reserve_floor = reserve_target;
    if oldest_age >= payout_interval.saturating_mul(4) {
        reserve_floor = 0;
    } else if oldest_age >= payout_interval.saturating_mul(2) {
        reserve_floor /= 2;
    }
    spendable
        .saturating_sub(reserve_floor)
        .saturating_sub(MIN_PAYOUT_FEE_BUFFER.saturating_mul(4))
}

fn payout_candidate_with_amount(candidate: &PayoutCandidate, amount: u64) -> PayoutCandidate {
    let mut pending = candidate.pending.clone();
    pending.amount = amount;
    PayoutCandidate {
        balance: candidate.balance.clone(),
        pending,
    }
}

fn find_reduced_payout_amount_for_liquidity(
    node: &NodeClient,
    candidate: &PayoutCandidate,
    spendable_outputs: &[WalletOutput],
    inventory: &OutputInventory,
    safe_spend_budget: u64,
    min_amount: u64,
    queue_light: bool,
) -> Option<u64> {
    let mut low = min_amount.max(1);
    let mut high = candidate.pending.amount.saturating_sub(1);
    let mut best_amount = None::<u64>;

    while low <= high {
        let mid = low + (high - low) / 2;
        let probe = payout_candidate_with_amount(candidate, mid);
        if plan_payout_batch_with_node(
            node,
            std::slice::from_ref(&probe),
            spendable_outputs,
            inventory,
            safe_spend_budget,
            queue_light,
        )
        .is_some()
        {
            best_amount = Some(mid);
            low = mid.saturating_add(1);
        } else {
            high = mid.saturating_sub(1);
        }
    }

    best_amount
}

fn plan_payout_batch_with_node(
    node: &NodeClient,
    candidates: &[PayoutCandidate],
    spendable_outputs: &[WalletOutput],
    inventory: &OutputInventory,
    safe_spend_budget: u64,
    queue_light: bool,
) -> Option<PlannedPayoutBatch> {
    if candidates.is_empty()
        || spendable_outputs.is_empty()
        || safe_spend_budget <= MIN_PAYOUT_FEE_BUFFER
    {
        return None;
    }

    let recipient_total = candidates.iter().fold(0u64, |acc, candidate| {
        acc.saturating_add(candidate.pending.amount)
    });
    if recipient_total == 0 {
        return None;
    }

    let batch_id = payout_batch_id(candidates);
    let idempotency_key = payout_batch_idempotency_key(&batch_id);
    let recipients = candidates
        .iter()
        .map(|candidate| WalletRecipient {
            address: candidate.balance.address.clone(),
            amount: candidate.pending.amount,
        })
        .collect::<Vec<_>>();

    let mut required_amount = recipient_total.saturating_add(MIN_PAYOUT_FEE_BUFFER);
    let mut inputs = select_inputs_for_target(
        spendable_outputs,
        required_amount,
        inventory.large_target_amount,
        safe_spend_budget,
    )?;

    for _ in 0..4 {
        let locked_amount = sum_output_amounts(&inputs);
        if locked_amount > safe_spend_budget {
            return None;
        }

        let input_refs = inputs.iter().map(WalletOutputRef::from).collect::<Vec<_>>();
        let preview = node
            .wallet_send_advanced(&recipients, &input_refs, 1, &idempotency_key, true)
            .ok()?;
        let required_with_fee = recipient_total
            .saturating_add(preview.fee)
            .saturating_add(MIN_PAYOUT_FEE_BUFFER);
        if locked_amount < required_with_fee {
            if required_with_fee <= required_amount {
                return None;
            }
            required_amount = required_with_fee;
            inputs = select_inputs_for_target(
                spendable_outputs,
                required_amount,
                inventory.large_target_amount,
                safe_spend_budget,
            )?;
            continue;
        }

        let change_amount = locked_amount
            .saturating_sub(recipient_total)
            .saturating_sub(preview.fee);
        let change_split = choose_change_split(inventory, change_amount, queue_light);
        let final_preview = if change_split > 1 {
            node.wallet_send_advanced(
                &recipients,
                &input_refs,
                change_split,
                &idempotency_key,
                true,
            )
            .ok()?
        } else {
            preview
        };
        if locked_amount < recipient_total.saturating_add(final_preview.fee) {
            let retry_required = recipient_total
                .saturating_add(final_preview.fee)
                .saturating_add(MIN_PAYOUT_FEE_BUFFER);
            if retry_required <= required_amount {
                return None;
            }
            required_amount = retry_required;
            inputs = select_inputs_for_target(
                spendable_outputs,
                required_amount,
                inventory.large_target_amount,
                safe_spend_budget,
            )?;
            continue;
        }

        return Some(PlannedPayoutBatch {
            batch_id,
            idempotency_key,
            recipients: candidates.to_vec(),
            recipient_total,
            inputs,
            locked_amount,
            change_split,
        });
    }

    None
}

fn build_output_inventory(
    outputs: &[WalletOutput],
    min_amount: u64,
    recent_stats: &RecentPayoutStats,
) -> OutputInventory {
    let small_target_amount = recent_stats
        .median_recipient_amount
        .max(min_amount)
        .max(min_amount.saturating_mul(4));
    let medium_target_amount = recent_stats
        .p90_recipient_amount
        .max(small_target_amount.saturating_mul(2));
    let large_target_amount = recent_stats
        .p90_batch_total
        .max(medium_target_amount.saturating_mul(2));
    let spendable_total = sum_output_amounts(outputs);
    let mut inventory = OutputInventory {
        spendable_count: outputs.len(),
        small_target_amount,
        medium_target_amount,
        large_target_amount,
        small_target_count: recent_stats.p50_recipient_count.div_ceil(2).clamp(2, 6),
        medium_target_count: recent_stats.p90_recipient_count.div_ceil(6).clamp(2, 4),
        large_target_count: if spendable_total < large_target_amount {
            1
        } else {
            2
        },
        ..OutputInventory::default()
    };

    for output in outputs {
        if output.amount < medium_target_amount {
            inventory.small_count = inventory.small_count.saturating_add(1);
        } else if output.amount < large_target_amount {
            inventory.medium_count = inventory.medium_count.saturating_add(1);
        } else {
            inventory.large_count = inventory.large_count.saturating_add(1);
        }
    }

    inventory
}

fn inventory_is_healthy(inventory: &OutputInventory) -> bool {
    inventory.small_count >= inventory.small_target_count
        && inventory.medium_count >= inventory.medium_target_count
        && inventory.large_count >= inventory.large_target_count
}

fn inventory_health_label(inventory: &OutputInventory) -> &'static str {
    if inventory_is_healthy(inventory) {
        "healthy"
    } else if inventory.large_count < inventory.large_target_count {
        "large_deficit"
    } else if inventory.medium_count < inventory.medium_target_count {
        "medium_deficit"
    } else {
        "small_deficit"
    }
}

fn choose_change_split(inventory: &OutputInventory, change_amount: u64, queue_light: bool) -> u32 {
    if !queue_light || change_amount <= inventory.small_target_amount.saturating_mul(2) {
        return 1;
    }
    let missing_small = inventory
        .small_target_count
        .saturating_sub(inventory.small_count);
    let missing_medium = inventory
        .medium_target_count
        .saturating_sub(inventory.medium_count);
    let desired = missing_small
        .saturating_add(missing_medium)
        .clamp(0, MAX_CHANGE_SPLIT as usize) as u32;
    if desired <= 1 {
        return 1;
    }
    let min_piece = change_amount / u64::from(desired);
    if min_piece < inventory.small_target_amount / 2 {
        1
    } else {
        desired
    }
}

fn select_inputs_for_target(
    outputs: &[WalletOutput],
    target_amount: u64,
    _large_target_amount: u64,
    max_locked_amount: u64,
) -> Option<Vec<WalletOutput>> {
    let mut available = outputs
        .iter()
        .filter(|output| output.amount <= max_locked_amount)
        .cloned()
        .collect::<Vec<_>>();
    if available.is_empty() {
        return None;
    }

    available.sort_by_key(|output| output.amount);

    if let Some(single) = available
        .iter()
        .filter(|output| output.amount >= target_amount)
        .min_by_key(|output| output.amount)
    {
        return Some(vec![single.clone()]);
    }

    let mut picked = Vec::<WalletOutput>::new();
    let mut total = 0u64;
    for output in available.iter().rev() {
        if total >= target_amount {
            break;
        }
        if total.saturating_add(output.amount) > max_locked_amount {
            continue;
        }
        picked.push(output.clone());
        total = total.saturating_add(output.amount);
    }
    if total >= target_amount {
        return Some(picked);
    }

    let mut ascending_pick = Vec::<WalletOutput>::new();
    total = 0;
    for output in &available {
        if total >= target_amount {
            break;
        }
        if total.saturating_add(output.amount) > max_locked_amount {
            continue;
        }
        ascending_pick.push(output.clone());
        total = total.saturating_add(output.amount);
    }
    (total >= target_amount).then_some(ascending_pick)
}

fn payout_batch_id(candidates: &[PayoutCandidate]) -> String {
    let mut input = format!(
        "blocknet-pool:payout-batch:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    for candidate in candidates {
        input.push_str(&candidate.balance.address);
        input.push(':');
        input.push_str(&candidate.pending.amount.to_string());
        input.push('|');
    }
    sha256_hex(input)
}

fn payout_batch_idempotency_key(batch_id: &str) -> String {
    sha256_hex(format!("blocknet-pool:payout-batch:{batch_id}"))
}

fn load_recovered_wallet_send_for_batch(
    path: &Path,
    batch_id: &str,
) -> anyhow::Result<Option<RecoveredWalletSend>> {
    let idempotency_key = payout_batch_idempotency_key(batch_id);
    let journal_key = format!("send-advanced:{idempotency_key}");
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed reading daemon idempotency journal {}: {err}",
                path.display()
            ));
        }
    };
    let journal: WalletSendIdempotencyJournal = serde_json::from_slice(&raw).map_err(|err| {
        anyhow::anyhow!(
            "failed parsing daemon idempotency journal {}: {err}",
            path.display()
        )
    })?;
    let Some(entry) = journal.entries.get(&journal_key) else {
        return Ok(None);
    };
    if entry.status != 200 {
        return Ok(None);
    }
    let Some(body) = decode_wallet_send_body(entry).map_err(|err| {
        anyhow::anyhow!(
            "failed decoding daemon idempotency journal entry {} from {}: {err}",
            journal_key,
            path.display()
        )
    })?
    else {
        return Ok(None);
    };
    if body.dry_run {
        return Ok(None);
    }
    let txid = body.txid.trim();
    if txid.is_empty() {
        return Ok(None);
    }
    let recipients = aggregate_wallet_send_recipients(&body.recipients);
    if recipients.is_empty() {
        return Err(anyhow::anyhow!(
            "daemon idempotency journal entry {} has no recipients",
            journal_key
        ));
    }
    Ok(Some(RecoveredWalletSend {
        txid: txid.to_string(),
        fee: body.fee,
        recipients,
    }))
}

fn validate_recovered_wallet_send_matches_pending(
    recovered: &RecoveredWalletSend,
    pending: &[PendingPayout],
) -> anyhow::Result<()> {
    let mut pending_recipients = pending
        .iter()
        .map(|entry| (entry.address.clone(), entry.amount))
        .collect::<Vec<_>>();
    pending_recipients.sort_by(|a, b| a.0.cmp(&b.0));
    if recovered.recipients != pending_recipients {
        return Err(anyhow::anyhow!(
            "daemon idempotency recipients do not match pending batch: recovered={:?} pending={:?}",
            recovered.recipients,
            pending_recipients
        ));
    }
    Ok(())
}

fn allocate_batch_fees(
    candidates: &[PayoutCandidate],
    total_fee: u64,
) -> Vec<PendingPayoutBatchMember> {
    candidates
        .iter()
        .zip(allocate_proportional_fees(
            candidates.iter().map(|candidate| candidate.pending.amount),
            total_fee,
        ))
        .map(|(candidate, fee)| PendingPayoutBatchMember {
            address: candidate.balance.address.clone(),
            amount: candidate.pending.amount,
            fee,
        })
        .collect()
}

pub fn weight_shares<F>(
    shares: &[DbShare],
    now: SystemTime,
    provisional_delay: Duration,
    trust_policy: PayoutTrustPolicy,
    mut force_verify_active: F,
) -> anyhow::Result<(HashMap<String, u64>, u64)>
where
    F: FnMut(&str) -> anyhow::Result<bool>,
{
    let mut by_address = HashMap::<String, AddressShareWeights>::new();
    for share in shares {
        let entry = by_address.entry(share.miner.clone()).or_default();
        if is_verified_share_status(&share.status) {
            entry.verified_shares = entry.verified_shares.saturating_add(1);
            entry.verified_difficulty = entry.verified_difficulty.saturating_add(share.difficulty);
        } else if is_share_payout_eligible(share, now, provisional_delay) {
            entry.provisional_difficulty = entry
                .provisional_difficulty
                .saturating_add(share.difficulty);
        }
    }

    let mut weights = HashMap::<String, u64>::new();
    let mut total = 0u64;

    for (address, stats) in by_address {
        let provisional_difficulty = if force_verify_active(&address)? {
            0
        } else {
            stats.provisional_difficulty
        };
        if stats.verified_shares < trust_policy.min_verified_shares {
            continue;
        }
        let total_uncapped = stats
            .verified_difficulty
            .saturating_add(provisional_difficulty);
        if total_uncapped == 0 {
            continue;
        }

        let counted_provisional = if trust_policy.provisional_cap_multiplier <= 0.0 {
            provisional_difficulty
        } else {
            let provisional_cap = ((stats.verified_difficulty as f64)
                * trust_policy.provisional_cap_multiplier)
                .clamp(0.0, u64::MAX as f64) as u64;
            provisional_difficulty.min(provisional_cap)
        };
        let weight = stats
            .verified_difficulty
            .saturating_add(counted_provisional);
        if weight == 0 {
            continue;
        }

        weights.insert(address, weight);
        total = total.saturating_add(weight);
    }

    Ok((weights, total))
}

pub fn is_share_payout_eligible(
    share: &DbShare,
    now: SystemTime,
    provisional_delay: Duration,
) -> bool {
    is_verified_share_status(&share.status)
        || (is_provisional_share_status(&share.status)
            && now.duration_since(share.created_at).unwrap_or_default() >= provisional_delay)
}

#[cfg(test)]
fn payout_idempotency_key(p: &PendingPayout) -> String {
    let attempt_started_at = p.send_started_at.or(p.sent_at).unwrap_or(p.initiated_at);
    let ts = attempt_started_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let input = format!("blocknet-pool:payout:{}:{}:{}", p.address, p.amount, ts);
    sha256_hex(input)
}

fn sha256_hex(input: impl AsRef<str>) -> String {
    hex::encode(Sha256::digest(input.as_ref().as_bytes()))
}

fn bounded_payout_interval(configured: Duration) -> Duration {
    configured.max(MIN_PAYOUT_INTERVAL)
}

fn bounded_payout_maintenance_interval(payout_interval: Duration) -> Duration {
    bounded_payout_interval(payout_interval).min(MAX_PAYOUT_MAINTENANCE_INTERVAL)
}

fn daemon_ready_for_payouts(status: &NodeStatus) -> bool {
    !status.syncing
}

fn should_drop_pending_payout(err: &anyhow::Error) -> bool {
    http_error_body_contains(err, 400, "invalid address")
        || http_error_body_contains(err, 400, "self-sends are temporarily disabled")
}

fn is_wallet_send_retryable_error(err: &anyhow::Error) -> bool {
    is_wallet_stale_input_error(err)
        || http_error_body_contains(err, 422, "not a canonical on-chain output")
        || http_error_body_contains(err, 429, "send busy, retry later")
        || http_error_body_contains(err, 429, "send rate limit exceeded")
        || err
            .chain()
            .any(|cause| cause.downcast_ref::<reqwest::Error>().is_some())
}

fn is_wallet_stale_input_error(err: &anyhow::Error) -> bool {
    http_error_body_contains(err, 409, "key image already spent")
        || http_error_body_contains(err, 409, "double-spend")
        || http_error_body_contains(err, 400, "output already spent")
}

fn is_wallet_liquidity_error(err: &anyhow::Error) -> bool {
    http_error_body_contains(err, 400, "insufficient spendable balance")
        || http_error_body_contains(err, 400, "insufficient funds")
        || http_error_body_contains(err, 400, "no spendable outputs")
}

fn add_credit(
    credits: &mut HashMap<String, u64>,
    address: &str,
    amount: u64,
) -> anyhow::Result<()> {
    if amount == 0 {
        return Ok(());
    }
    let destination = address.trim();
    if destination.is_empty() {
        return Ok(());
    }
    let entry = credits.entry(destination.to_string()).or_default();
    *entry = entry
        .checked_add(amount)
        .ok_or_else(|| anyhow::anyhow!("credit overflow"))?;
    Ok(())
}

fn atomic_amount_from_coins(coins: f64) -> u64 {
    if !coins.is_finite() || coins <= 0.0 {
        return 0;
    }
    let max_coins = (u64::MAX as f64) / 100_000_000.0;
    (coins.clamp(0.0, max_coins) * 100_000_000.0).round() as u64
}

fn summarize_missing_completed_payout_rows<F>(
    rows: &[UnreconciledCompletedPayoutRow],
    mut tx_status_lookup: F,
) -> anyhow::Result<MissingCompletedPayoutSummary>
where
    F: FnMut(&str) -> anyhow::Result<Option<TxStatus>>,
{
    let mut tx_row_counts = HashMap::<&str, u64>::new();
    for row in rows {
        let tx_hash = row.tx_hash.trim();
        if tx_hash.is_empty() {
            continue;
        }
        *tx_row_counts.entry(tx_hash).or_default() += 1;
    }

    let mut summary = MissingCompletedPayoutSummary::default();
    for (tx_hash, row_count) in tx_row_counts {
        if tx_status_lookup(tx_hash)?.is_some() {
            continue;
        }
        summary.issue_count = summary.issue_count.saturating_add(1);
        summary.row_count = summary.row_count.saturating_add(row_count);
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::HttpError;
    use crate::store::PoolStore;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use base64::Engine as _;
    use pool_common::db::{DbShare, ShareReplayData};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::thread;

    fn sample_share(miner: &str, difficulty: u64, status: &str, created_at: SystemTime) -> DbShare {
        DbShare {
            id: 1,
            job_id: "j".into(),
            miner: miner.into(),
            worker: "w".into(),
            difficulty,
            nonce: 1,
            status: status.into(),
            was_sampled: true,
            block_hash: None,
            claimed_hash: None,
            created_at,
        }
    }

    fn sample_unreconciled_completed_payout_row(
        payout_id: i64,
        tx_hash: &str,
    ) -> UnreconciledCompletedPayoutRow {
        UnreconciledCompletedPayoutRow {
            payout_id,
            address: format!("addr-{payout_id}"),
            amount: 100,
            fee: 1,
            tx_hash: tx_hash.to_string(),
            timestamp: UNIX_EPOCH + Duration::from_secs(1_000 + payout_id.max(0) as u64),
            linked_amount: 0,
            orphaned_linked_amount: 0,
        }
    }

    fn test_store() -> Option<Arc<PoolStore>> {
        PoolStore::test_store()
    }

    macro_rules! require_test_store {
        () => {
            match test_store() {
                Some(store) => store,
                None => {
                    eprintln!(
                        "skipping postgres test: set {} to run postgres integration checks",
                        PoolStore::TEST_POSTGRES_URL_ENV
                    );
                    return;
                }
            }
        };
    }

    fn candidate(address: &str, amount: u64, age: Duration) -> PayoutCandidate {
        let now = SystemTime::now();
        PayoutCandidate {
            balance: Balance {
                address: address.to_string(),
                pending: amount,
                paid: 0,
            },
            pending: PendingPayout {
                address: address.to_string(),
                amount,
                initiated_at: now - age,
                send_started_at: None,
                tx_hash: None,
                fee: None,
                sent_at: None,
                batch_id: None,
            },
        }
    }

    fn trust_policy(
        min_verified_shares: u64,
        provisional_cap_multiplier: f64,
    ) -> PayoutTrustPolicy {
        PayoutTrustPolicy {
            min_verified_shares,
            provisional_cap_multiplier,
        }
    }

    fn spawn_json_server(body: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("server addr");
        let response_body = body.to_string();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });
        format!("http://{}", addr)
    }

    fn spawn_json_router_server(routes: HashMap<String, String>, max_requests: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind router server");
        let addr = listener.local_addr().expect("router addr");
        thread::spawn(move || {
            for _ in 0..max_requests {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 4096];
                let read = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                let (status, body) = match routes.get(path) {
                    Some(body) => ("HTTP/1.1 200 OK", body.clone()),
                    None => (
                        "HTTP/1.1 404 Not Found",
                        r#"{"error":"not found"}"#.to_string(),
                    ),
                };
                let response = format!(
                    "{status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write router response");
            }
        });
        format!("http://{}", addr)
    }

    #[derive(Debug, Clone, Copy)]
    struct MockAdvancedSendPolicy {
        preview_recipient_cap: Option<u64>,
        live_recipient_cap: Option<u64>,
        fee: u64,
        live_busy: bool,
    }

    fn test_address(seed: u8) -> String {
        let mut payload = [seed.max(1); 64];
        payload[0] = seed.max(1);
        bs58::encode(payload).into_string()
    }

    fn wallet_output(txid: &str, output_index: u32, amount: u64) -> WalletOutput {
        WalletOutput {
            txid: txid.to_string(),
            output_index,
            amount,
            status: "unspent".to_string(),
        }
    }

    fn spawn_wallet_send_server(
        wallet_address: &str,
        outputs: Vec<WalletOutput>,
        policy: MockAdvancedSendPolicy,
        max_requests: usize,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind wallet send server");
        let addr = listener.local_addr().expect("wallet send server addr");
        let wallet_address = wallet_address.to_string();
        thread::spawn(move || {
            for _ in 0..max_requests {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 16384];
                let read = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..read]);
                let mut parts = request
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .split_whitespace();
                let method = parts.next().unwrap_or("GET");
                let path = parts.next().unwrap_or("/");
                let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
                let spendable = outputs
                    .iter()
                    .fold(0u64, |acc, output| acc.saturating_add(output.amount));
                let (status, response_body) = match (method, path) {
                    ("GET", "/api/wallet/address") => (
                        "HTTP/1.1 200 OK",
                        serde_json::json!({
                            "address": wallet_address,
                            "view_only": false,
                        })
                        .to_string(),
                    ),
                    ("GET", "/api/wallet/balance") => (
                        "HTTP/1.1 200 OK",
                        serde_json::json!({
                            "spendable": spendable,
                            "pending": 0u64,
                            "pending_unconfirmed": 0u64,
                            "pending_unconfirmed_eta": 0u64,
                            "total": spendable,
                        })
                        .to_string(),
                    ),
                    ("GET", "/api/wallet/outputs") => (
                        "HTTP/1.1 200 OK",
                        serde_json::json!({
                            "chain_height": 1u64,
                            "synced_height": 1u64,
                            "outputs": outputs,
                        })
                        .to_string(),
                    ),
                    ("POST", "/api/wallet/send/advanced") => {
                        let payload = serde_json::from_str::<serde_json::Value>(body)
                            .expect("decode advanced send payload");
                        let dry_run = payload
                            .get("dry_run")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        let recipient_total = payload
                            .get("recipients")
                            .and_then(serde_json::Value::as_array)
                            .map(|recipients| {
                                recipients.iter().fold(0u64, |acc, recipient| {
                                    acc.saturating_add(
                                        recipient
                                            .get("amount")
                                            .and_then(serde_json::Value::as_u64)
                                            .unwrap_or_default(),
                                    )
                                })
                            })
                            .unwrap_or_default();
                        let input_total = payload
                            .get("inputs")
                            .and_then(serde_json::Value::as_array)
                            .map(|inputs| {
                                inputs.iter().fold(0u64, |acc, input| {
                                    let txid = input
                                        .get("txid")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or_default();
                                    let output_index = input
                                        .get("output_index")
                                        .and_then(serde_json::Value::as_u64)
                                        .unwrap_or_default()
                                        as u32;
                                    let amount = outputs
                                        .iter()
                                        .find(|output| {
                                            output.txid == txid
                                                && output.output_index == output_index
                                        })
                                        .map(|output| output.amount)
                                        .unwrap_or_default();
                                    acc.saturating_add(amount)
                                })
                            })
                            .unwrap_or_default();
                        let recipient_cap = if dry_run {
                            policy.preview_recipient_cap
                        } else {
                            policy.live_recipient_cap
                        };
                        if !dry_run && policy.live_busy {
                            (
                                "HTTP/1.1 429 Too Many Requests",
                                r#"{"error":"send busy, retry later"}"#.to_string(),
                            )
                        } else if recipient_cap.is_some_and(|cap| recipient_total > cap)
                            || recipient_total.saturating_add(policy.fee) > input_total
                        {
                            (
                                "HTTP/1.1 400 Bad Request",
                                r#"{"error":"insufficient funds"}"#.to_string(),
                            )
                        } else {
                            (
                                "HTTP/1.1 200 OK",
                                serde_json::json!({
                                    "txid": if dry_run { "" } else { "mock-live-send" },
                                    "fee": policy.fee,
                                    "change": input_total
                                        .saturating_sub(recipient_total)
                                        .saturating_sub(policy.fee),
                                })
                                .to_string(),
                            )
                        }
                    }
                    _ => (
                        "HTTP/1.1 404 Not Found",
                        r#"{"error":"not found"}"#.to_string(),
                    ),
                };
                let response = format!(
                    "{status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write wallet send response");
            }
        });
        format!("http://{}", addr)
    }

    fn sample_status(chain_height: u64) -> NodeStatus {
        NodeStatus {
            peers: 3,
            chain_height,
            syncing: false,
            current_process_block: None,
            last_process_block: None,
        }
    }

    fn seed_completed_reversible_payout(
        store: &Arc<PoolStore>,
        height: u64,
        address: &str,
        tx_hash: &str,
    ) {
        store
            .add_block(&DbBlock {
                height,
                hash: format!("completed-block-{height}"),
                difficulty: 1,
                finder: address.to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert completed payout block");
        assert!(store
            .apply_block_credits_and_mark_paid(height, &[(address.to_string(), 100)])
            .expect("apply completed payout credits"));
        store
            .create_pending_payout(address, 100)
            .expect("create pending payout");
        store
            .record_pending_payout_broadcast(address, 100, 2, tx_hash)
            .expect("record payout broadcast");
        store
            .complete_pending_payout(address, 100, 2, tx_hash)
            .expect("complete payout");
    }

    #[test]
    fn provisional_shares_mature_after_delay() {
        let now = SystemTime::now();
        let mature = sample_share(
            "a1",
            10,
            SHARE_STATUS_PROVISIONAL,
            now - Duration::from_secs(20 * 60),
        );
        let fresh = sample_share(
            "a1",
            10,
            SHARE_STATUS_PROVISIONAL,
            now - Duration::from_secs(30),
        );

        assert!(is_share_payout_eligible(
            &mature,
            now,
            Duration::from_secs(15 * 60)
        ));
        assert!(!is_share_payout_eligible(
            &fresh,
            now,
            Duration::from_secs(15 * 60)
        ));
    }

    #[test]
    fn payout_weighting_includes_mature_provisional() {
        let now = SystemTime::now();
        let shares = vec![
            sample_share("a1", 10, SHARE_STATUS_VERIFIED, now),
            sample_share("a2", 10, SHARE_STATUS_VERIFIED, now),
            sample_share(
                "a2",
                100,
                SHARE_STATUS_PROVISIONAL,
                now - Duration::from_secs(20 * 60),
            ),
        ];

        let (weights, total) = weight_shares(
            &shares,
            now,
            Duration::from_secs(15 * 60),
            trust_policy(1, 3.0),
            |_| Ok(false),
        )
        .expect("weight shares");
        assert_eq!(total, 50);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert_eq!(weights.get("a2").copied(), Some(40));
    }

    #[test]
    fn payout_weighting_requires_verified_anchor() {
        let now = SystemTime::now();
        let shares = vec![
            sample_share("a1", 10, SHARE_STATUS_VERIFIED, now),
            sample_share(
                "a2",
                100,
                SHARE_STATUS_PROVISIONAL,
                now - Duration::from_secs(20 * 60),
            ),
        ];

        let (weights, total) = weight_shares(
            &shares,
            now,
            Duration::from_secs(15 * 60),
            trust_policy(1, 3.0),
            |_| Ok(false),
        )
        .expect("weight shares");
        assert_eq!(total, 10);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert!(!weights.contains_key("a2"));
    }

    #[test]
    fn payout_weighting_caps_provisional_share_weight() {
        let now = SystemTime::now();
        let shares = vec![
            sample_share("a2", 20, SHARE_STATUS_VERIFIED, now),
            sample_share(
                "a2",
                100,
                SHARE_STATUS_PROVISIONAL,
                now - Duration::from_secs(20 * 60),
            ),
        ];

        let (weights, total) = weight_shares(
            &shares,
            now,
            Duration::from_secs(15 * 60),
            trust_policy(1, 1.0),
            |_| Ok(false),
        )
        .expect("weight shares");
        assert_eq!(total, 40);
        assert_eq!(weights.get("a2").copied(), Some(40));
    }

    #[test]
    fn payout_weighting_zero_multiplier_counts_all_eligible_provisional_weight() {
        let now = SystemTime::now();
        let shares = vec![
            sample_share("a2", 20, SHARE_STATUS_VERIFIED, now),
            sample_share(
                "a2",
                100,
                SHARE_STATUS_PROVISIONAL,
                now - Duration::from_secs(20 * 60),
            ),
        ];

        let (weights, total) = weight_shares(
            &shares,
            now,
            Duration::from_secs(15 * 60),
            trust_policy(1, 0.0),
            |_| Ok(false),
        )
        .expect("weight shares");
        assert_eq!(total, 120);
        assert_eq!(weights.get("a2").copied(), Some(120));
    }

    #[test]
    fn payout_weighting_propagates_risk_lookup_failures() {
        let now = SystemTime::now();
        let shares = vec![sample_share("a1", 10, SHARE_STATUS_VERIFIED, now)];

        let err = weight_shares(
            &shares,
            now,
            Duration::from_secs(0),
            trust_policy(1, 0.0),
            |_| Err(anyhow::anyhow!("boom")),
        )
        .expect_err("risk lookup failure should abort weighting");
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn payout_weighting_counts_verified_shares_but_ignores_provisional_for_force_verify() {
        let now = SystemTime::now();
        let shares = vec![
            sample_share("a1", 10, SHARE_STATUS_VERIFIED, now),
            sample_share("a2", 10, SHARE_STATUS_VERIFIED, now),
            sample_share(
                "a2",
                100,
                SHARE_STATUS_PROVISIONAL,
                now - Duration::from_secs(60 * 60),
            ),
        ];

        let (weights, total) = weight_shares(
            &shares,
            now,
            Duration::from_secs(0),
            trust_policy(1, 19.0),
            |address| Ok(address == "a2"),
        )
        .expect("weight shares");
        assert_eq!(total, 20);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert_eq!(weights.get("a2").copied(), Some(10));
    }

    #[test]
    fn weighted_allocation_distributes_full_amount() {
        let store = require_test_store!();
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
        );

        let mut credits = HashMap::<String, u64>::new();
        processor
            .allocate_weighted_credits(
                HashMap::from([("a1".to_string(), 1), ("a2".to_string(), 1)]),
                2,
                3,
                &mut credits,
            )
            .expect("allocate");

        let total = credits.values().copied().sum::<u64>();
        assert_eq!(total, 3);
    }

    #[test]
    fn build_pplns_credits_replays_zero_weight_window_and_persists_verification() {
        let store = require_test_store!();
        let cfg = Config {
            pplns_window_duration: "24h".to_string(),
            provisional_share_delay: "0s".to_string(),
            payout_min_verified_shares: 1,
            ..Config::default()
        };

        let processor = PayoutProcessor::new(
            cfg,
            Arc::clone(&store),
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
        );

        let share_time = UNIX_EPOCH + Duration::from_secs(5_000_000);
        store
            .add_share_with_replay(
                crate::engine::ShareRecord {
                    job_id: "replay-job".to_string(),
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty: 1,
                    nonce: 7,
                    status: SHARE_STATUS_PROVISIONAL,
                    was_sampled: false,
                    block_hash: None,
                    claimed_hash: None,
                    reject_reason: None,
                    created_at: share_time,
                },
                Some(ShareReplayData {
                    job_id: "replay-job".to_string(),
                    header_base: vec![1, 2, 3, 4],
                    network_target: [0xff; 32],
                    created_at: share_time,
                }),
            )
            .expect("add replayable share");

        let credits = processor
            .build_pplns_credits(
                &DbBlock {
                    height: 500,
                    hash: "blk-replay".to_string(),
                    difficulty: 1,
                    finder: "miner-a".to_string(),
                    finder_worker: "wa".to_string(),
                    reward: 1_000,
                    timestamp: share_time + Duration::from_secs(30),
                    confirmed: true,
                    orphaned: false,
                    paid_out: false,
                    effort_pct: None,
                },
                1_000,
            )
            .expect("build credits");
        assert_eq!(credits.get("miner-a").copied(), Some(1_000));

        let persisted = store
            .get_recent_shares(10)
            .expect("recent shares")
            .into_iter()
            .find(|share| share.job_id == "replay-job")
            .expect("persisted replay share");
        assert_eq!(persisted.status, SHARE_STATUS_VERIFIED);
        assert!(persisted.was_sampled);
    }

    #[test]
    fn build_pplns_credits_errors_when_zero_weight_window_has_no_replay_data() {
        let store = require_test_store!();
        let cfg = Config {
            pplns_window_duration: "24h".to_string(),
            provisional_share_delay: "0s".to_string(),
            payout_min_verified_shares: 1,
            ..Config::default()
        };

        let processor = PayoutProcessor::new(
            cfg,
            Arc::clone(&store),
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
        );

        let share_time = UNIX_EPOCH + Duration::from_secs(5_100_000);
        store
            .add_share(crate::engine::ShareRecord {
                job_id: "missing-replay-job".to_string(),
                miner: "miner-a".to_string(),
                worker: "wa".to_string(),
                difficulty: 1,
                nonce: 8,
                status: SHARE_STATUS_PROVISIONAL,
                was_sampled: false,
                block_hash: None,
                claimed_hash: None,
                reject_reason: None,
                created_at: share_time,
            })
            .expect("add provisional share");

        let err = processor
            .build_pplns_credits(
                &DbBlock {
                    height: 501,
                    hash: "blk-missing-replay".to_string(),
                    difficulty: 1,
                    finder: "miner-a".to_string(),
                    finder_worker: "wa".to_string(),
                    reward: 1_000,
                    timestamp: share_time + Duration::from_secs(30),
                    confirmed: true,
                    orphaned: false,
                    paid_out: false,
                    effort_pct: None,
                },
                1_000,
            )
            .expect_err("missing replay data should block payout");
        assert!(err.to_string().contains("missing replay data"));
    }

    #[test]
    fn replay_rejects_candidate_share_that_fails_network_target() {
        let store = require_test_store!();
        let share_time = UNIX_EPOCH + Duration::from_secs(5_200_000);
        store
            .add_share_with_replay(
                crate::engine::ShareRecord {
                    job_id: "candidate-replay-job".to_string(),
                    miner: "miner-a".to_string(),
                    worker: "wa".to_string(),
                    difficulty: 1,
                    nonce: 9,
                    status: SHARE_STATUS_PROVISIONAL,
                    was_sampled: false,
                    block_hash: Some("candidate-hash".to_string()),
                    claimed_hash: None,
                    reject_reason: None,
                    created_at: share_time,
                },
                Some(ShareReplayData {
                    job_id: "candidate-replay-job".to_string(),
                    header_base: vec![9, 8, 7, 6],
                    network_target: [0u8; 32],
                    created_at: share_time,
                }),
            )
            .expect("add candidate share");

        let mut shares = store.get_recent_shares(10).expect("recent shares");
        let err = recover_share_window_by_replay(
            &store,
            &mut shares,
            share_time + Duration::from_secs(30),
            Duration::ZERO,
            false,
        )
        .expect_err("candidate replay mismatch should abort");
        assert!(err.to_string().contains("candidate share"));
    }

    #[test]
    fn payout_candidates_promote_longest_waiting_after_threshold() {
        let now = SystemTime::now();
        let mut candidates = vec![
            candidate("a-fast", 500, Duration::from_secs(30 * 60)),
            candidate("a-old", 100, Duration::from_secs(2 * 60 * 60)),
            candidate("a-older", 200, Duration::from_secs(90 * 60)),
        ];

        sort_payout_candidates(&mut candidates, now, Duration::from_secs(60 * 60));

        let ordered = candidates
            .into_iter()
            .map(|candidate| candidate.balance.address)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["a-old", "a-older", "a-fast"]);
    }

    #[test]
    fn payout_candidates_fall_back_to_amount_before_threshold() {
        let now = SystemTime::now();
        let mut candidates = vec![
            candidate("a-big", 500, Duration::from_secs(30 * 60)),
            candidate("a-small", 100, Duration::from_secs(50 * 60)),
        ];

        sort_payout_candidates(&mut candidates, now, Duration::from_secs(60 * 60));

        let ordered = candidates
            .into_iter()
            .map(|candidate| candidate.balance.address)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["a-small", "a-big"]);
    }

    #[test]
    fn payout_interval_is_bounded_to_safe_minimum() {
        assert_eq!(
            bounded_payout_interval(Duration::from_secs(0)),
            Duration::from_secs(1)
        );
        assert_eq!(
            bounded_payout_interval(Duration::from_millis(10)),
            Duration::from_secs(1)
        );
        assert_eq!(
            bounded_payout_interval(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn payout_maintenance_interval_stays_fast_even_with_slow_send_cadence() {
        assert_eq!(
            bounded_payout_maintenance_interval(Duration::from_secs(0)),
            Duration::from_secs(1)
        );
        assert_eq!(
            bounded_payout_maintenance_interval(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
        assert_eq!(
            bounded_payout_maintenance_interval(Duration::from_secs(60 * 60)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn on_chain_unreconciled_completed_payout_rows_do_not_block_new_sends() {
        let rows = vec![
            sample_unreconciled_completed_payout_row(1, "tx-on-chain"),
            sample_unreconciled_completed_payout_row(2, "tx-on-chain"),
            sample_unreconciled_completed_payout_row(3, "tx-also-on-chain"),
        ];

        let summary = summarize_missing_completed_payout_rows(&rows, |tx_hash| {
            Ok(Some(TxStatus {
                confirmations: if tx_hash == "tx-on-chain" { 3 } else { 1 },
                in_mempool: false,
            }))
        })
        .expect("summarize missing completed payouts");

        assert_eq!(summary.issue_count, 0);
        assert_eq!(summary.row_count, 0);
    }

    #[test]
    fn missing_completed_payout_rows_still_block_new_sends() {
        let rows = vec![
            sample_unreconciled_completed_payout_row(1, "tx-missing"),
            sample_unreconciled_completed_payout_row(2, "tx-missing"),
            sample_unreconciled_completed_payout_row(3, "tx-present"),
            sample_unreconciled_completed_payout_row(4, "tx-other-missing"),
        ];

        let summary = summarize_missing_completed_payout_rows(&rows, |tx_hash| {
            Ok(match tx_hash {
                "tx-present" => Some(TxStatus {
                    confirmations: 2,
                    in_mempool: false,
                }),
                _ => None,
            })
        })
        .expect("summarize missing completed payouts");

        assert_eq!(summary.issue_count, 2);
        assert_eq!(summary.row_count, 3);
    }

    #[test]
    fn payouts_are_skipped_while_syncing() {
        let status = NodeStatus {
            peers: 1,
            chain_height: 100,
            syncing: true,
            current_process_block: None,
            last_process_block: None,
        };
        assert!(!daemon_ready_for_payouts(&status));

        let status_ready = NodeStatus {
            syncing: false,
            ..status
        };
        assert!(daemon_ready_for_payouts(&status_ready));
    }

    #[test]
    fn permanent_wallet_send_errors_drop_pending_payout() {
        let invalid = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 400,
            body: r#"{"error":"invalid address"}"#.to_string(),
        });
        assert!(should_drop_pending_payout(&invalid));

        let self_send = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 400,
            body: r#"{"error":"self-sends are temporarily disabled"}"#.to_string(),
        });
        assert!(should_drop_pending_payout(&self_send));

        let transient = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 429,
            body: r#"{"error":"send rate limit exceeded"}"#.to_string(),
        });
        assert!(!should_drop_pending_payout(&transient));
    }

    #[test]
    fn wallet_liquidity_errors_are_treated_as_retryable() {
        let insufficient = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 400,
            body: r#"{"error":"insufficient funds after fee adjustment: have 100 need 110"}"#
                .to_string(),
        });
        assert!(is_wallet_liquidity_error(&insufficient));

        let no_outputs = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 400,
            body: r#"{"error":"insufficient funds: no spendable outputs"}"#.to_string(),
        });
        assert!(is_wallet_liquidity_error(&no_outputs));
    }

    #[test]
    fn wallet_send_busy_errors_are_treated_as_retryable() {
        let busy = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 429,
            body: r#"{"error":"send busy, retry later"}"#.to_string(),
        });
        assert!(is_wallet_send_retryable_error(&busy));

        let rate_limited = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send".to_string(),
            status_code: 429,
            body: r#"{"error":"send rate limit exceeded"}"#.to_string(),
        });
        assert!(is_wallet_send_retryable_error(&rate_limited));
    }

    #[test]
    fn wallet_double_spend_rejections_are_treated_as_retryable() {
        let key_image = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send/advanced".to_string(),
            status_code: 409,
            body: r#"{"error":"mempool rejected: validation failed: input 0: key image already spent (double-spend)"}"#.to_string(),
        });
        assert!(is_wallet_send_retryable_error(&key_image));

        let spent_output = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send/advanced".to_string(),
            status_code: 400,
            body: r#"{"error":"input 0: output already spent"}"#.to_string(),
        });
        assert!(is_wallet_send_retryable_error(&spent_output));
    }

    #[test]
    fn wallet_noncanonical_ring_member_rejections_are_treated_as_retryable() {
        let noncanonical = anyhow::anyhow!(HttpError {
            path: "/api/wallet/send/advanced".to_string(),
            status_code: 422,
            body: r#"{"error":"mempool rejected: validation failed: input 0 ring member 4: not a canonical on-chain output"}"#.to_string(),
        });
        assert!(is_wallet_send_retryable_error(&noncanonical));
    }

    #[test]
    fn wallet_send_transport_errors_are_treated_as_retryable() {
        use anyhow::Context as _;

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .expect("build reqwest client");
        let err = client
            .post("http://127.0.0.1:1")
            .send()
            .context("POST /api/wallet/send/advanced")
            .expect_err("send to closed port should fail");

        assert!(is_wallet_send_retryable_error(&err));
    }

    #[test]
    fn stale_prebroadcast_pending_payouts_are_held_for_reconciliation_after_retry_grace() {
        let now = SystemTime::now();
        let stale = PendingPayout {
            address: "addr1".to_string(),
            amount: 100,
            initiated_at: now - Duration::from_secs(2 * 60 * 60),
            send_started_at: Some(now - Duration::from_secs(16 * 60)),
            tx_hash: None,
            fee: None,
            sent_at: None,
            batch_id: None,
        };
        assert!(has_stale_unknown_broadcast_outcome(&stale, now));

        let fresh = PendingPayout {
            send_started_at: Some(now - Duration::from_secs(5 * 60)),
            ..stale.clone()
        };
        assert!(!has_stale_unknown_broadcast_outcome(&fresh, now));

        let broadcast = PendingPayout {
            tx_hash: Some("tx-1".to_string()),
            ..stale
        };
        assert!(!has_stale_unknown_broadcast_outcome(&broadcast, now));
    }

    #[test]
    fn recovered_wallet_send_loads_batch_from_daemon_idempotency_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("send-idempotency.json");
        let batch_id = "batch-1";
        let journal_key = format!("send-advanced:{}", payout_batch_idempotency_key(batch_id));
        let body = serde_json::json!({
            "txid": "tx-recovered",
            "dry_run": false,
            "fee": 17u64,
            "recipients": [
                {"address": "addr-b", "amount": 200u64},
                {"address": "addr-a", "amount": 100u64},
                {"address": "addr-b", "amount": 50u64}
            ]
        });
        let journal = serde_json::json!({
            "entries": {
                journal_key: {
                    "status": 200,
                    "body_base64": BASE64_STANDARD.encode(body.to_string())
                }
            }
        });
        fs::write(&path, journal.to_string()).expect("write idempotency journal");

        let recovered = load_recovered_wallet_send_for_batch(&path, batch_id)
            .expect("load recovered send")
            .expect("recovered send");

        assert_eq!(
            recovered,
            RecoveredWalletSend {
                txid: "tx-recovered".to_string(),
                fee: 17,
                recipients: vec![("addr-a".to_string(), 100), ("addr-b".to_string(), 250)],
            }
        );
    }

    #[test]
    fn recovered_wallet_send_must_match_pending_batch() {
        let recovered = RecoveredWalletSend {
            txid: "tx-recovered".to_string(),
            fee: 17,
            recipients: vec![("addr-a".to_string(), 100), ("addr-b".to_string(), 200)],
        };
        let pending = vec![
            PendingPayout {
                address: "addr-b".to_string(),
                amount: 200,
                initiated_at: SystemTime::now(),
                send_started_at: None,
                tx_hash: None,
                fee: None,
                sent_at: None,
                batch_id: Some("batch-1".to_string()),
            },
            PendingPayout {
                address: "addr-a".to_string(),
                amount: 100,
                initiated_at: SystemTime::now(),
                send_started_at: None,
                tx_hash: None,
                fee: None,
                sent_at: None,
                batch_id: Some("batch-1".to_string()),
            },
        ];
        validate_recovered_wallet_send_matches_pending(&recovered, &pending)
            .expect("matching recovered send");

        let mismatched = RecoveredWalletSend {
            recipients: vec![("addr-a".to_string(), 101), ("addr-b".to_string(), 200)],
            ..recovered
        };
        assert!(validate_recovered_wallet_send_matches_pending(&mismatched, &pending).is_err());
    }

    #[test]
    fn payout_idempotency_key_uses_send_attempt_timestamp() {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let first = PendingPayout {
            address: "addr1".to_string(),
            amount: 100,
            initiated_at: base,
            send_started_at: Some(base + Duration::from_secs(5)),
            tx_hash: None,
            fee: None,
            sent_at: None,
            batch_id: None,
        };
        let second = PendingPayout {
            send_started_at: Some(base + Duration::from_secs(15)),
            ..first.clone()
        };

        assert_ne!(
            payout_idempotency_key(&first),
            payout_idempotency_key(&second)
        );
        assert_eq!(
            payout_idempotency_key(&first),
            payout_idempotency_key(&first.clone())
        );
    }

    #[test]
    fn allocate_batch_fees_preserves_total_fee() {
        let candidates = vec![
            candidate("addr1", 100, Duration::from_secs(60)),
            candidate("addr2", 200, Duration::from_secs(120)),
            candidate("addr3", 300, Duration::from_secs(180)),
        ];

        let allocated = allocate_batch_fees(&candidates, 17);
        let total_allocated = allocated
            .iter()
            .fold(0u64, |acc, member| acc.saturating_add(member.fee));

        assert_eq!(allocated.len(), 3);
        assert_eq!(total_allocated, 17);
        assert_eq!(allocated[0].fee, 2);
        assert_eq!(allocated[1].fee, 5);
        assert_eq!(allocated[2].fee, 10);
    }

    #[test]
    fn reduced_payout_amount_fits_when_full_pending_amount_does_not() {
        let outputs = vec![
            wallet_output("out-1", 0, 100_000_000),
            wallet_output("out-2", 0, 50_000_000),
            wallet_output("out-3", 0, 50_000_000),
        ];
        let base_url = spawn_wallet_send_server(
            &test_address(98),
            outputs.clone(),
            MockAdvancedSendPolicy {
                preview_recipient_cap: None,
                live_recipient_cap: None,
                fee: 1_000,
                live_busy: false,
            },
            64,
        );
        let node = NodeClient::new(&base_url, "").expect("node");
        let min_amount = 1_000_000;
        let inventory = build_output_inventory(&outputs, min_amount, &RecentPayoutStats::default());
        let oversized = candidate("addr1", 300_000_000, Duration::from_secs(60));
        let safe_spend_budget = 196_000_000;

        assert!(plan_payout_batch_with_node(
            &node,
            std::slice::from_ref(&oversized),
            &outputs,
            &inventory,
            safe_spend_budget,
            true,
        )
        .is_none());

        let reduced = find_reduced_payout_amount_for_liquidity(
            &node,
            &oversized,
            &outputs,
            &inventory,
            safe_spend_budget,
            min_amount,
            true,
        )
        .expect("reduced payout amount should fit");
        assert!(reduced < oversized.pending.amount);
        assert!(reduced >= min_amount);
        assert!(plan_payout_batch_with_node(
            &node,
            std::slice::from_ref(&payout_candidate_with_amount(&oversized, reduced)),
            &outputs,
            &inventory,
            safe_spend_budget,
            true,
        )
        .is_some());
    }

    #[test]
    fn smaller_safe_spend_budget_forces_planner_to_reduce_batch_size() {
        let outputs = vec![
            wallet_output("out-a", 0, 100_000_000),
            wallet_output("out-b", 0, 40_000_000),
            wallet_output("out-c", 0, 40_000_000),
            wallet_output("out-d", 0, 20_000_000),
        ];
        let base_url = spawn_wallet_send_server(
            &test_address(97),
            outputs.clone(),
            MockAdvancedSendPolicy {
                preview_recipient_cap: None,
                live_recipient_cap: Some(120_000_000),
                fee: 1_000,
                live_busy: false,
            },
            64,
        );
        let node = NodeClient::new(&base_url, "").expect("node");
        let min_amount = 1_000_000;
        let inventory = build_output_inventory(&outputs, min_amount, &RecentPayoutStats::default());
        let candidates = vec![
            candidate("addr-big", 90_000_000, Duration::from_secs(60 * 60)),
            candidate("addr-small", 80_000_000, Duration::from_secs(30 * 60)),
        ];

        let full_plan = plan_payout_batch_with_node(
            &node,
            &candidates,
            &outputs,
            &inventory,
            196_000_000,
            true,
        )
        .expect("full batch should plan before liquidity backoff");
        assert_eq!(full_plan.recipients.len(), 2);

        let backed_off_budget = full_plan.locked_amount.saturating_sub(1);
        assert!(plan_payout_batch_with_node(
            &node,
            &candidates,
            &outputs,
            &inventory,
            backed_off_budget,
            true,
        )
        .is_none());

        let reduced_plan = plan_payout_batch_with_node(
            &node,
            std::slice::from_ref(&candidates[0]),
            &outputs,
            &inventory,
            backed_off_budget,
            true,
        )
        .expect("smaller retry budget should still fit the first queued recipient");
        assert_eq!(reduced_plan.recipients.len(), 1);
        assert!(reduced_plan.recipient_total < full_plan.recipient_total);
    }

    #[test]
    fn planner_can_spend_single_large_output_when_budget_allows() {
        let outputs = vec![wallet_output("large-topup", 0, 120_000_000_000)];
        let base_url = spawn_wallet_send_server(
            &test_address(96),
            outputs.clone(),
            MockAdvancedSendPolicy {
                preview_recipient_cap: None,
                live_recipient_cap: None,
                fee: 1_000,
                live_busy: false,
            },
            8,
        );
        let node = NodeClient::new(&base_url, "").expect("node");
        let inventory = OutputInventory {
            large_target_amount: 100_000_000_000,
            ..OutputInventory::default()
        };
        let candidates = vec![candidate(
            "addr-large",
            10_000_000_000,
            Duration::from_secs(60),
        )];

        let plan = plan_payout_batch_with_node(
            &node,
            &candidates,
            &outputs,
            &inventory,
            120_000_000_000,
            true,
        )
        .expect("large top-up output should be usable for payouts");

        assert_eq!(plan.inputs.len(), 1);
        assert_eq!(plan.inputs[0].txid, "large-topup");
    }

    #[test]
    fn send_payouts_downsizes_single_pending_payout_to_fit_liquidity() {
        let store = require_test_store!();
        let miner = test_address(11);
        let wallet_address = test_address(99);
        store
            .update_balance(&Balance {
                address: miner.clone(),
                pending: 300_000_000,
                paid: 0,
            })
            .expect("seed miner balance");

        let base_url = spawn_wallet_send_server(
            &wallet_address,
            vec![
                wallet_output("out-1", 0, 100_000_000),
                wallet_output("out-2", 0, 50_000_000),
                wallet_output("out-3", 0, 50_000_000),
            ],
            MockAdvancedSendPolicy {
                preview_recipient_cap: None,
                live_recipient_cap: None,
                fee: 1_000,
                live_busy: false,
            },
            64,
        );
        let cfg = Config {
            min_payout_amount: 0.01,
            ..Config::default()
        };
        let processor = PayoutProcessor::new(
            cfg,
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.send_payouts();

        let pending = store
            .get_pending_payout(&miner)
            .expect("lookup pending payout")
            .expect("pending payout should exist after broadcast");
        assert_eq!(pending.tx_hash.as_deref(), Some("mock-live-send"));
        assert_eq!(pending.fee, Some(1_000));
        assert!(pending.amount < 300_000_000);
        assert!(pending.amount >= 1_000_000);
        assert!(pending.amount <= 149_999_000);

        let balance = store
            .get_balance(&miner)
            .expect("miner balance after partial send");
        assert_eq!(balance.pending, 300_000_000);
        assert_eq!(balance.paid, 0);
    }

    #[test]
    fn send_payouts_retries_liquidity_errors_with_smaller_batch() {
        let store = require_test_store!();
        let miner_a = test_address(21);
        let miner_b = test_address(22);
        let wallet_address = test_address(100);
        store
            .update_balance(&Balance {
                address: miner_a.clone(),
                pending: 90_000_000,
                paid: 0,
            })
            .expect("seed miner a balance");
        store
            .update_balance(&Balance {
                address: miner_b.clone(),
                pending: 80_000_000,
                paid: 0,
            })
            .expect("seed miner b balance");

        let base_url = spawn_wallet_send_server(
            &wallet_address,
            vec![
                wallet_output("out-a", 0, 100_000_000),
                wallet_output("out-b", 0, 40_000_000),
                wallet_output("out-c", 0, 40_000_000),
                wallet_output("out-d", 0, 20_000_000),
            ],
            MockAdvancedSendPolicy {
                preview_recipient_cap: None,
                live_recipient_cap: Some(120_000_000),
                fee: 1_000,
                live_busy: false,
            },
            96,
        );
        let cfg = Config {
            min_payout_amount: 0.01,
            ..Config::default()
        };
        let processor = PayoutProcessor::new(
            cfg,
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.send_payouts();

        let pending = store
            .get_pending_payouts()
            .expect("load pending payouts after send");
        let broadcast = pending
            .iter()
            .filter(|entry| entry.tx_hash.as_deref() == Some("mock-live-send"))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(broadcast.len(), 1);
        assert!(broadcast[0].amount <= 120_000_000);
        assert_eq!(broadcast[0].fee, Some(1_000));
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|entry| entry
            .tx_hash
            .as_deref()
            .is_none_or(|tx| tx.trim().is_empty())));
    }

    #[test]
    fn send_payouts_resets_batch_state_when_wallet_send_is_busy() {
        let store = require_test_store!();
        let miner = test_address(31);
        let wallet_address = test_address(101);
        store
            .update_balance(&Balance {
                address: miner.clone(),
                pending: 90_000_000,
                paid: 0,
            })
            .expect("seed miner balance");

        let base_url = spawn_wallet_send_server(
            &wallet_address,
            vec![
                wallet_output("out-1", 0, 100_000_000),
                wallet_output("out-2", 0, 40_000_000),
            ],
            MockAdvancedSendPolicy {
                preview_recipient_cap: None,
                live_recipient_cap: None,
                fee: 1_000,
                live_busy: true,
            },
            32,
        );
        let cfg = Config {
            min_payout_amount: 0.01,
            ..Config::default()
        };
        let processor = PayoutProcessor::new(
            cfg,
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.send_payouts();

        let pending = store
            .get_pending_payout(&miner)
            .expect("lookup pending payout")
            .expect("pending payout should remain queued");
        assert_eq!(pending.amount, 90_000_000);
        assert!(pending.send_started_at.is_none());
        assert!(pending.tx_hash.is_none());
        assert!(pending.fee.is_none());
        assert!(pending.sent_at.is_none());
        assert!(pending.batch_id.is_none());
    }

    #[test]
    fn pool_fee_destination_requires_explicit_wallet() {
        let block = DbBlock {
            height: 1,
            hash: "block-1".to_string(),
            difficulty: 1,
            finder: "miner-a".to_string(),
            finder_worker: "rig-a".to_string(),
            reward: 100,
            timestamp: SystemTime::UNIX_EPOCH,
            confirmed: true,
            orphaned: false,
            paid_out: false,
            effort_pct: None,
        };

        assert_eq!(
            resolve_pool_fee_destination(&Config::default(), &block),
            None
        );

        let cfg = Config {
            pool_fee_wallet_address: "pool-wallet".to_string(),
            ..Config::default()
        };
        assert_eq!(
            resolve_pool_fee_destination(&cfg, &block),
            Some("pool-wallet".to_string())
        );
    }

    #[test]
    fn rebalance_destination_uses_daemon_wallet_not_fee_wallet() {
        let wallet = WalletAddressResponse {
            address: "daemon-hot-wallet".to_string(),
            view_only: false,
        };

        assert_eq!(
            resolve_wallet_rebalance_destination(&wallet, "pool-fee-wallet"),
            Some("daemon-hot-wallet")
        );
    }

    #[test]
    fn rebalance_destination_rejects_fee_wallet_match() {
        let wallet = WalletAddressResponse {
            address: "same-wallet".to_string(),
            view_only: false,
        };

        assert_eq!(
            resolve_wallet_rebalance_destination(&wallet, "same-wallet"),
            None
        );
    }

    #[test]
    fn rebalance_destination_rejects_view_only_wallet() {
        let wallet = WalletAddressResponse {
            address: "daemon-hot-wallet".to_string(),
            view_only: true,
        };

        assert_eq!(
            resolve_wallet_rebalance_destination(&wallet, "pool-fee-wallet"),
            None
        );
    }

    #[test]
    fn confirmed_broadcast_payouts_reconcile_without_waiting_for_next_send_tick() {
        let store = require_test_store!();
        store
            .update_balance(&Balance {
                address: "addr1".to_string(),
                pending: 100,
                paid: 0,
            })
            .expect("seed balance");
        store
            .create_pending_payout("addr1", 100)
            .expect("create pending payout");
        store
            .mark_pending_payout_send_started("addr1")
            .expect("freeze pending payout")
            .expect("pending payout exists");
        store
            .record_pending_payout_broadcast("addr1", 100, 2, "tx-1")
            .expect("record broadcast");

        let base_url = spawn_json_server(r#"{"confirmations":1,"in_mempool":false}"#);
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.reconcile_pending_payouts();

        assert!(store
            .get_pending_payout("addr1")
            .expect("lookup pending payout")
            .is_none());
        let balance = store.get_balance("addr1").expect("updated balance");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 100);

        let payouts = store
            .get_recent_payouts_for_address("addr1", 5)
            .expect("recent payouts");
        assert_eq!(payouts.len(), 1);
        assert_eq!(payouts[0].tx_hash, "tx-1");
    }

    #[test]
    fn chain_reconciliation_orphans_diverged_blocks_and_reverses_unpaid_credits() {
        let store = require_test_store!();
        let common_height = 9_100_000u64 + (rand::random::<u16>() as u64);
        let diverged_height = common_height + 1;

        store
            .add_block(&DbBlock {
                height: common_height,
                hash: format!("common-{common_height}"),
                difficulty: 1,
                finder: "finder-common".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert common block");
        store
            .add_block(&DbBlock {
                height: diverged_height,
                hash: format!("pool-diverged-{diverged_height}"),
                difficulty: 1,
                finder: "finder-diverged".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert diverged block");
        assert!(store
            .apply_block_credits_and_mark_paid(
                diverged_height,
                &[("miner-diverged".to_string(), 100)],
            )
            .expect("apply diverged credits"));
        store
            .create_pending_payout("miner-diverged", 100)
            .expect("create pending payout");

        let base_url = spawn_json_router_server(
            HashMap::from([
                (
                    format!("/api/block/{common_height}"),
                    format!(
                        r#"{{"height":{common_height},"hash":"common-{common_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
                (
                    format!("/api/block/{diverged_height}"),
                    format!(
                        r#"{{"height":{diverged_height},"hash":"daemon-diverged-{diverged_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
            ]),
            6,
        );
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.reconcile_chain_recovery(&sample_status(diverged_height));

        let common = store
            .get_block(common_height)
            .expect("load common block")
            .expect("common block exists");
        let diverged = store
            .get_block(diverged_height)
            .expect("load diverged block")
            .expect("diverged block exists");
        assert!(!common.orphaned);
        assert!(diverged.orphaned);
        assert!(!diverged.confirmed);
        assert!(!diverged.paid_out);
        assert_eq!(
            store
                .get_balance("miner-diverged")
                .expect("miner balance after reconcile")
                .pending,
            0
        );
        assert!(store
            .get_pending_payout("miner-diverged")
            .expect("pending payout lookup")
            .is_none());
        assert!(store
            .get_block_credit_events(diverged_height)
            .expect("credit events after reconcile")
            .is_empty());
    }

    #[test]
    fn chain_reconciliation_orphans_historical_diverged_window_below_matching_tip() {
        let store = require_test_store!();
        let common_height = 9_150_000u64 + (rand::random::<u16>() as u64);
        let diverged_height = common_height + 1;
        let matching_tip_height = diverged_height + 1;

        store
            .add_block(&DbBlock {
                height: common_height,
                hash: format!("common-{common_height}"),
                difficulty: 1,
                finder: "finder-common".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert common block");
        store
            .add_block(&DbBlock {
                height: diverged_height,
                hash: format!("pool-diverged-{diverged_height}"),
                difficulty: 1,
                finder: "finder-diverged".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert diverged block");
        store
            .add_block(&DbBlock {
                height: matching_tip_height,
                hash: format!("matching-tip-{matching_tip_height}"),
                difficulty: 1,
                finder: "finder-tip".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert matching tip");
        assert!(store
            .apply_block_credits_and_mark_paid(
                diverged_height,
                &[("miner-diverged".to_string(), 100)],
            )
            .expect("apply diverged credits"));
        store
            .create_pending_payout("miner-diverged", 100)
            .expect("create pending payout");

        let base_url = spawn_json_router_server(
            HashMap::from([
                (
                    format!("/api/block/{common_height}"),
                    format!(
                        r#"{{"height":{common_height},"hash":"common-{common_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
                (
                    format!("/api/block/{diverged_height}"),
                    format!(
                        r#"{{"height":{diverged_height},"hash":"daemon-diverged-{diverged_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
                (
                    format!("/api/block/{matching_tip_height}"),
                    format!(
                        r#"{{"height":{matching_tip_height},"hash":"matching-tip-{matching_tip_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
            ]),
            9,
        );
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        assert!(processor.reconcile_chain_recovery(&sample_status(matching_tip_height)));

        let common = store
            .get_block(common_height)
            .expect("load common block")
            .expect("common block exists");
        let diverged = store
            .get_block(diverged_height)
            .expect("load diverged block")
            .expect("diverged block exists");
        let matching_tip = store
            .get_block(matching_tip_height)
            .expect("load matching tip")
            .expect("matching tip exists");
        assert!(!common.orphaned);
        assert!(diverged.orphaned);
        assert!(!matching_tip.orphaned);
        assert_eq!(
            store
                .get_balance("miner-diverged")
                .expect("miner balance after reconcile")
                .pending,
            0
        );
        assert!(store
            .get_pending_payout("miner-diverged")
            .expect("pending payout lookup")
            .is_none());
    }

    #[test]
    fn reconcile_start_height_ignores_already_orphaned_blocks() {
        let store = require_test_store!();
        let common_height = 9_175_000u64 + (rand::random::<u16>() as u64);
        let orphaned_height = common_height + 1;
        let tip_height = orphaned_height + 1;

        let common = DbBlock {
            height: common_height,
            hash: format!("common-{common_height}"),
            difficulty: 1,
            finder: "finder-common".to_string(),
            finder_worker: "rig".to_string(),
            reward: 100,
            timestamp: SystemTime::now(),
            confirmed: true,
            orphaned: false,
            paid_out: false,
            effort_pct: None,
        };
        let orphaned = DbBlock {
            height: orphaned_height,
            hash: format!("pool-orphaned-{orphaned_height}"),
            difficulty: 1,
            finder: "finder-orphaned".to_string(),
            finder_worker: "rig".to_string(),
            reward: 100,
            timestamp: SystemTime::now(),
            confirmed: false,
            orphaned: true,
            paid_out: false,
            effort_pct: None,
        };
        let tip = DbBlock {
            height: tip_height,
            hash: format!("tip-{tip_height}"),
            difficulty: 1,
            finder: "finder-tip".to_string(),
            finder_worker: "rig".to_string(),
            reward: 100,
            timestamp: SystemTime::now(),
            confirmed: true,
            orphaned: false,
            paid_out: false,
            effort_pct: None,
        };
        store.add_block(&common).expect("insert common block");
        store
            .add_block(&orphaned)
            .expect("insert orphaned historical block");
        store.add_block(&tip).expect("insert matching tip block");

        let base_url = spawn_json_router_server(
            HashMap::from([
                (
                    format!("/api/block/{common_height}"),
                    format!(
                        r#"{{"height":{common_height},"hash":"common-{common_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
                (
                    format!("/api/block/{tip_height}"),
                    format!(
                        r#"{{"height":{tip_height},"hash":"tip-{tip_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
            ]),
            4,
        );
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );
        let recent_blocks = vec![tip, orphaned, common];

        assert!(matches!(
            processor
                .find_reconcile_start_height_in_blocks(&recent_blocks)
                .expect("scan blocks"),
            ReconcileWindow::None
        ));
    }

    #[test]
    fn chain_recovery_reverts_completed_payouts_missing_from_daemon_chain() {
        let store = require_test_store!();
        let common_height = 9_200_000u64 + (rand::random::<u16>() as u64);
        let diverged_height = common_height + 1;
        let miner = "miner-paid-diverged";
        let tx_hash = "tx-diverged-missing";

        store
            .add_block(&DbBlock {
                height: common_height,
                hash: format!("common-{common_height}"),
                difficulty: 1,
                finder: "finder-common".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert common block");
        store
            .add_block(&DbBlock {
                height: diverged_height,
                hash: format!("pool-diverged-{diverged_height}"),
                difficulty: 1,
                finder: miner.to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: true,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert diverged block");
        assert!(store
            .apply_block_credits_and_mark_paid(diverged_height, &[(miner.to_string(), 100)])
            .expect("apply diverged credits"));
        store
            .create_pending_payout(miner, 100)
            .expect("create pending payout");
        store
            .record_pending_payout_broadcast(miner, 100, 2, tx_hash)
            .expect("record broadcast");
        store
            .complete_pending_payout(miner, 100, 2, tx_hash)
            .expect("complete payout");

        let base_url = spawn_json_router_server(
            HashMap::from([
                (
                    format!("/api/block/{common_height}"),
                    format!(
                        r#"{{"height":{common_height},"hash":"common-{common_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
                (
                    format!("/api/block/{diverged_height}"),
                    format!(
                        r#"{{"height":{diverged_height},"hash":"daemon-diverged-{diverged_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
            ]),
            8,
        );
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        assert!(processor.reconcile_chain_recovery(&sample_status(diverged_height)));
        processor.reconcile_completed_payouts_after_recovery();

        let balance = store.get_balance(miner).expect("balance after recovery");
        assert_eq!(balance.pending, 0);
        assert_eq!(balance.paid, 0);
        assert!(store
            .get_active_payouts()
            .expect("active payouts")
            .into_iter()
            .all(|payout| payout.tx_hash != tx_hash));
        assert!(store
            .get_recent_payouts_for_address(miner, 5)
            .expect("visible payouts after recovery")
            .is_empty());
    }

    #[test]
    fn recovery_reconciliation_processes_completed_payouts_in_batches() {
        let store = require_test_store!();
        let base_height = 9_300_000u64 + (rand::random::<u16>() as u64);
        let payout_count = RECOVERY_PAYOUT_RECONCILIATION_BATCH_SIZE as usize + 1;

        for index in 0..payout_count {
            seed_completed_reversible_payout(
                &store,
                base_height + index as u64,
                &format!("batched-miner-{index}"),
                &format!("batched-tx-{index}"),
            );
        }

        let base_url = spawn_json_router_server(HashMap::new(), payout_count + 4);
        let processor = PayoutProcessor::new(
            Config::default(),
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.activate_recovery_payout_reconciliation();
        processor.reconcile_completed_payouts_after_recovery();

        let remaining = store
            .get_active_payouts()
            .expect("active payouts after first batch");
        assert_eq!(remaining.len(), 1);
        assert!(processor.recovery_payout_reconciliation_active());

        processor.reconcile_completed_payouts_after_recovery();

        assert!(store
            .get_active_payouts()
            .expect("active payouts after second batch")
            .is_empty());
        assert!(!processor.recovery_payout_reconciliation_active());
    }

    #[test]
    fn tick_confirms_blocks_while_recovery_reconciliation_is_active() {
        let store = require_test_store!();
        let mature_height = 9_400_000u64 + (rand::random::<u16>() as u64);
        let payout_count = RECOVERY_PAYOUT_RECONCILIATION_BATCH_SIZE as usize + 1;

        store
            .add_block(&DbBlock {
                height: mature_height,
                hash: format!("mature-{mature_height}"),
                difficulty: 1,
                finder: "mature-miner".to_string(),
                finder_worker: "rig".to_string(),
                reward: 100,
                timestamp: SystemTime::now(),
                confirmed: false,
                orphaned: false,
                paid_out: false,
                effort_pct: None,
            })
            .expect("insert mature unconfirmed block");

        for index in 0..payout_count {
            seed_completed_reversible_payout(
                &store,
                mature_height + 100 + index as u64,
                &format!("active-miner-{index}"),
                &format!("active-tx-{index}"),
            );
        }

        let chain_height = mature_height + 100;
        let base_url = spawn_json_router_server(
            HashMap::from([
                (
                    "/api/status".to_string(),
                    format!(r#"{{"peers":3,"chain_height":{chain_height},"syncing":false}}"#),
                ),
                (
                    format!("/api/block/{mature_height}"),
                    format!(
                        r#"{{"height":{mature_height},"hash":"mature-{mature_height}","reward":100,"difficulty":1,"timestamp":0,"tx_count":1}}"#
                    ),
                ),
            ]),
            payout_count + 16,
        );
        let processor = PayoutProcessor::new(
            Config {
                blocks_before_payout: 60,
                ..Config::default()
            },
            Arc::clone(&store),
            Arc::new(NodeClient::new(&base_url, "").expect("node")),
        );

        processor.activate_recovery_payout_reconciliation();
        assert!(!processor.tick(false));

        let block = store
            .get_block(mature_height)
            .expect("load mature block")
            .expect("mature block exists");
        assert!(block.confirmed);
        assert!(block.paid_out);

        let balance = store
            .get_balance("mature-miner")
            .expect("mature miner balance");
        assert_eq!(balance.pending, 100);
        assert!(processor.recovery_payout_reconciliation_active());
    }
}
