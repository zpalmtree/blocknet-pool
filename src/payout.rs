use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::db::{Balance, DbBlock, DbShare, PendingPayout, PoolFeeRecord};
use crate::node::{http_error_body_contains, is_http_status, NodeClient, NodeStatus};
use crate::protocol::validate_miner_address;
use crate::store::PoolStore;
use crate::validation::{SHARE_STATUS_PROVISIONAL, SHARE_STATUS_VERIFIED};

const MIN_PAYOUT_INTERVAL: Duration = Duration::from_secs(1);
const MIN_PAYOUT_FEE_BUFFER: u64 = 1_000;
const PENDING_PAYOUT_RETRY_GRACE: Duration = Duration::from_secs(15 * 60);
const PAYOUT_CONFIRMATIONS_REQUIRED: u64 = 1;

#[derive(Debug, Clone, Copy)]
pub struct PayoutTrustPolicy {
    pub min_verified_shares: u64,
    pub min_verified_ratio: f64,
    pub provisional_cap_multiplier: f64,
}

impl PayoutTrustPolicy {
    fn from_config(cfg: &Config) -> Self {
        Self {
            min_verified_shares: cfg.payout_min_verified_shares.max(0) as u64,
            min_verified_ratio: cfg.payout_min_verified_ratio.clamp(0.0, 1.0),
            provisional_cap_multiplier: cfg.payout_provisional_cap_multiplier.max(0.0),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AddressShareWeights {
    verified_shares: u64,
    verified_difficulty: u64,
    provisional_difficulty: u64,
}

#[derive(Debug)]
pub struct PayoutProcessor {
    cfg: Config,
    store: Arc<PoolStore>,
    node: Arc<NodeClient>,
}

impl PayoutProcessor {
    pub fn new(cfg: Config, store: Arc<PoolStore>, node: Arc<NodeClient>) -> Arc<Self> {
        Arc::new(Self { cfg, store, node })
    }

    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            {
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.recover_pending_payouts();
                    this.tick();
                })
                .await;
            }

            let payout_interval = bounded_payout_interval(this.cfg.payout_interval_duration());
            let mut ticker = tokio::time::interval(payout_interval);
            loop {
                ticker.tick().await;
                let this = Arc::clone(&this);
                let _ = tokio::task::spawn_blocking(move || {
                    this.tick();
                })
                .await;
            }
        });
    }

    fn tick(&self) {
        let status = match self.node.get_status() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "cannot reach node for payouts");
                return;
            }
        };
        if !daemon_ready_for_payouts(&status) {
            tracing::warn!(
                chain_height = status.chain_height,
                "daemon is syncing; skipping payout tick"
            );
            return;
        }

        self.confirm_blocks();
        self.distribute_rewards();
        self.send_payouts();
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
            let distributable = block.reward.saturating_sub(fee);
            let fee_address = if fee > 0 {
                self.resolve_pool_fee_address(&block)
            } else {
                None
            };
            let fee_record = if fee > 0 {
                if let Some(fee_address) = fee_address.as_ref() {
                    Some(PoolFeeRecord {
                        amount: fee,
                        fee_address: fee_address.clone(),
                        timestamp: block.timestamp,
                    })
                } else {
                    tracing::warn!(
                        height = block.height,
                        fee,
                        "pool fee applied without an explicit fee destination"
                    );
                    None
                }
            } else {
                None
            };

            let credits = if self.cfg.payout_scheme.trim().eq_ignore_ascii_case("pplns") {
                self.build_pplns_credits(&block, distributable)
            } else {
                self.build_proportional_credits(&block, distributable)
            };
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
        let shares = if self.cfg.pplns_window_duration_duration().is_zero() {
            self.store.get_last_n_shares_before(
                block.timestamp,
                i64::from(self.cfg.pplns_window.max(1)),
            )?
        } else {
            let since = block
                .timestamp
                .checked_sub(self.cfg.pplns_window_duration_duration())
                .unwrap_or(UNIX_EPOCH);
            self.store.get_shares_between(since, block.timestamp)?
        };

        let mut credits = HashMap::<String, u64>::new();
        if shares.is_empty() {
            add_credit(&mut credits, &block.finder, reward)?;
            return Ok(credits);
        }

        let (weights, total_weight) = self.weight_shares_for_payout(&shares)?;
        if total_weight == 0 {
            add_credit(&mut credits, &block.finder, reward)?;
            return Ok(credits);
        }

        let mut distributable = reward;
        if self.cfg.block_finder_bonus && self.cfg.block_finder_bonus_pct > 0.0 {
            let bonus = (reward as f64 * self.cfg.block_finder_bonus_pct / 100.0) as u64;
            add_credit(&mut credits, &block.finder, bonus)?;
            distributable = distributable.saturating_sub(bonus);
        }

        self.allocate_weighted_credits(weights, total_weight, distributable, &mut credits)?;
        Ok(credits)
    }

    fn build_proportional_credits(
        &self,
        block: &DbBlock,
        reward: u64,
    ) -> anyhow::Result<HashMap<String, u64>> {
        let since = block
            .timestamp
            .checked_sub(Duration::from_secs(60 * 60))
            .unwrap_or(UNIX_EPOCH);
        let shares = self.store.get_shares_between(since, block.timestamp)?;
        let mut credits = HashMap::<String, u64>::new();
        if shares.is_empty() {
            add_credit(&mut credits, &block.finder, reward)?;
            return Ok(credits);
        }

        let (weights, total_weight) = self.weight_shares_for_payout(&shares)?;
        if total_weight == 0 {
            add_credit(&mut credits, &block.finder, reward)?;
            return Ok(credits);
        }

        let mut distributable = reward;
        if self.cfg.block_finder_bonus && self.cfg.block_finder_bonus_pct > 0.0 {
            let bonus = (reward as f64 * self.cfg.block_finder_bonus_pct / 100.0) as u64;
            add_credit(&mut credits, &block.finder, bonus)?;
            distributable = distributable.saturating_sub(bonus);
        }

        self.allocate_weighted_credits(weights, total_weight, distributable, &mut credits)?;
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

        Ok(weight_shares(
            shares,
            now,
            provisional_delay,
            trust_policy,
            |address| {
                if let Some(risky) = risk_cache.get(address) {
                    return *risky;
                }
                let risky = match self.store.should_force_verify_address(address) {
                    Ok((force_verify, _)) => force_verify,
                    Err(err) => {
                        tracing::warn!(
                            address = %address,
                            error = %err,
                            "failed risk check during payout weighting; treating address as risky"
                        );
                        true
                    }
                };
                risk_cache.insert(address.to_string(), risky);
                risky
            },
        ))
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

        for entry in pending {
            let age = SystemTime::now()
                .duration_since(entry.initiated_at)
                .unwrap_or_default();
            if age > Duration::from_secs(60 * 60) {
                if entry.send_started_at.is_some() {
                    tracing::warn!(
                        address = %entry.address,
                        amount = entry.amount,
                        age_secs = age.as_secs(),
                        "stale pending payout retained for idempotent retry"
                    );
                } else {
                    tracing::warn!(
                        address = %entry.address,
                        amount = entry.amount,
                        age_secs = age.as_secs(),
                        "stale queued payout has not reached its first send attempt yet"
                    );
                }
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

        let min_amount = atomic_amount_from_coins(self.cfg.min_payout_amount);
        let max_recipients_per_tick = if self.cfg.payout_max_recipients_per_tick <= 0 {
            None
        } else {
            Some(self.cfg.payout_max_recipients_per_tick as usize)
        };
        let max_total_per_tick = {
            let value = atomic_amount_from_coins(self.cfg.payout_max_total_per_tick);
            if value == 0 {
                None
            } else {
                Some(value)
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
        let mut sent_recipients = 0usize;
        let mut sent_total = 0u64;
        let balances = match self.store.get_all_balances() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to list balances");
                return;
            }
        };

        for bal in balances {
            if max_recipients_per_tick.is_some_and(|cap| sent_recipients >= cap) {
                tracing::warn!(
                    cap = max_recipients_per_tick.unwrap_or(0),
                    sent_recipients,
                    sent_total,
                    "stopping payout tick due to max recipient cap"
                );
                break;
            }
            if max_total_per_tick.is_some_and(|cap| sent_total >= cap) {
                tracing::warn!(
                    cap = max_total_per_tick.unwrap_or(0),
                    sent_recipients,
                    sent_total,
                    "stopping payout tick due to max payout total cap"
                );
                break;
            }

            let existing_pending = match self.store.get_pending_payout(&bal.address) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(address = %bal.address, error = %err, "failed pending payout query");
                    continue;
                }
            };

            if let Err(reason) = validate_miner_address(&bal.address) {
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
            match self.reconcile_broadcast_payout(&bal, &pending) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(err) => {
                    tracing::warn!(
                        address = %bal.address,
                        error = %err,
                        "failed to reconcile broadcast payout state"
                    );
                    continue;
                }
            }
            if bal.pending < pending_amount {
                tracing::warn!(
                    address = %bal.address,
                    pending_amount,
                    balance_pending = bal.pending,
                    "pending payout exceeds local pending balance; skipping"
                );
                continue;
            }
            if let Some(cap) = max_total_per_tick {
                if sent_total.saturating_add(pending_amount) > cap {
                    tracing::warn!(
                        cap,
                        sent_total,
                        next_amount = pending_amount,
                        "stopping payout tick due to max payout total cap"
                    );
                    break;
                }
            }

            let wallet_balance = match self.node.get_wallet_balance() {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(error = %err, "failed wallet balance check");
                    return;
                }
            };
            if wallet_balance.spendable < pending_amount.saturating_add(MIN_PAYOUT_FEE_BUFFER) {
                continue;
            }

            let pending = if pending.send_started_at.is_some() {
                pending
            } else {
                match self.store.mark_pending_payout_send_started(&bal.address) {
                    Ok(Some(v)) => v,
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(
                            address = %bal.address,
                            error = %err,
                            "failed to freeze pending payout before send"
                        );
                        continue;
                    }
                }
            };

            let pending_for_send = PendingPayout {
                address: pending.address.clone(),
                amount: pending_amount,
                initiated_at: pending.initiated_at,
                send_started_at: pending.send_started_at,
                tx_hash: None,
                fee: None,
                sent_at: None,
            };
            let idempotency_key = payout_idempotency_key(&pending_for_send);
            let send = self
                .node
                .wallet_send(&bal.address, pending_amount, &idempotency_key);
            let sent = match send {
                Ok(v) => v,
                Err(err) => {
                    if should_drop_pending_payout(&err) {
                        if let Err(cancel_err) = self.store.cancel_pending_payout(&bal.address) {
                            tracing::warn!(
                                address = %bal.address,
                                error = %cancel_err,
                                "wallet send failed permanently and pending payout cancel failed"
                            );
                        } else {
                            tracing::warn!(
                                address = %bal.address,
                                error = %err,
                                "wallet send failed permanently; dropped pending payout"
                            );
                        }
                    } else if is_wallet_liquidity_error(&err) {
                        tracing::info!(
                            address = %bal.address,
                            amount = pending_amount,
                            error = %err,
                            "wallet liquidity insufficient to cover payout plus fee; retaining queued payout"
                        );
                    } else {
                        tracing::warn!(address = %bal.address, error = %err, "wallet send failed");
                    }
                    continue;
                }
            };

            if let Err(err) = self.store.record_pending_payout_broadcast(
                &bal.address,
                pending_amount,
                sent.fee,
                &sent.txid,
            ) {
                tracing::error!(
                    address = %bal.address,
                    tx = %sent.txid,
                    error = %err,
                    "critical payout broadcast persistence failure"
                );
                continue;
            }
            sent_recipients = sent_recipients.saturating_add(1);
            sent_total = sent_total.saturating_add(pending_amount);

            tracing::info!(
                address = %bal.address,
                amount = pending_amount,
                fee = sent.fee,
                tx = %sent.txid,
                idempotency_key = %idempotency_key,
                "payout broadcast"
            );
        }
    }

    fn reconcile_broadcast_payout(
        &self,
        bal: &Balance,
        pending: &PendingPayout,
    ) -> anyhow::Result<bool> {
        let Some(tx_hash) = pending.tx_hash.as_deref().filter(|v| !v.trim().is_empty()) else {
            return Ok(false);
        };

        let status = self.node.get_tx_status_optional(tx_hash)?;
        match status {
            Some(status) if status.in_mempool => Ok(true),
            Some(status) if status.confirmations >= PAYOUT_CONFIRMATIONS_REQUIRED => {
                let fee = pending.fee.unwrap_or(0);
                self.store
                    .complete_pending_payout(&bal.address, pending.amount, fee, tx_hash)?;
                tracing::info!(
                    address = %bal.address,
                    amount = pending.amount,
                    fee,
                    tx = %tx_hash,
                    confirmations = status.confirmations,
                    "payout confirmed"
                );
                Ok(true)
            }
            Some(_) => Ok(true),
            None => {
                let sent_at = pending.sent_at.or(pending.send_started_at);
                let age = sent_at
                    .and_then(|ts| SystemTime::now().duration_since(ts).ok())
                    .unwrap_or_default();
                if age < PENDING_PAYOUT_RETRY_GRACE {
                    return Ok(true);
                }
                self.store.reset_pending_payout_send_state(&bal.address)?;
                tracing::warn!(
                    address = %bal.address,
                    amount = pending.amount,
                    tx = %tx_hash,
                    age_secs = age.as_secs(),
                    "broadcast payout disappeared from mempool/chain; reset for retry"
                );
                Ok(true)
            }
        }
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

    fn resolve_pool_fee_address(&self, block: &DbBlock) -> Option<String> {
        let configured = self.cfg.pool_wallet_address.trim();
        if !configured.is_empty() {
            return Some(configured.to_string());
        }

        let finder = block.finder.trim();
        if !finder.is_empty() {
            return Some(finder.to_string());
        }
        None
    }
}

pub fn weight_shares<F>(
    shares: &[DbShare],
    now: SystemTime,
    provisional_delay: Duration,
    trust_policy: PayoutTrustPolicy,
    mut is_risky: F,
) -> (HashMap<String, u64>, u64)
where
    F: FnMut(&str) -> bool,
{
    let mut by_address = HashMap::<String, AddressShareWeights>::new();
    for share in shares {
        let entry = by_address.entry(share.miner.clone()).or_default();
        match share.status.as_str() {
            "" | SHARE_STATUS_VERIFIED => {
                entry.verified_shares = entry.verified_shares.saturating_add(1);
                entry.verified_difficulty =
                    entry.verified_difficulty.saturating_add(share.difficulty);
            }
            SHARE_STATUS_PROVISIONAL if is_share_payout_eligible(share, now, provisional_delay) => {
                entry.provisional_difficulty = entry
                    .provisional_difficulty
                    .saturating_add(share.difficulty);
            }
            _ => {}
        }
    }

    let mut weights = HashMap::<String, u64>::new();
    let mut total = 0u64;

    for (address, stats) in by_address {
        if is_risky(&address) {
            continue;
        }
        if stats.verified_shares < trust_policy.min_verified_shares {
            continue;
        }
        let total_uncapped = stats
            .verified_difficulty
            .saturating_add(stats.provisional_difficulty);
        if total_uncapped == 0 {
            continue;
        }
        if trust_policy.min_verified_ratio > 0.0 {
            let verified_ratio = stats.verified_difficulty as f64 / total_uncapped as f64;
            if verified_ratio < trust_policy.min_verified_ratio {
                continue;
            }
        }

        let counted_provisional = if trust_policy.provisional_cap_multiplier <= 0.0 {
            stats.provisional_difficulty
        } else {
            let provisional_cap = ((stats.verified_difficulty as f64)
                * trust_policy.provisional_cap_multiplier)
                .clamp(0.0, u64::MAX as f64) as u64;
            stats.provisional_difficulty.min(provisional_cap)
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

    (weights, total)
}

pub fn is_share_payout_eligible(
    share: &DbShare,
    now: SystemTime,
    provisional_delay: Duration,
) -> bool {
    match share.status.as_str() {
        "" | SHARE_STATUS_VERIFIED => true,
        SHARE_STATUS_PROVISIONAL => {
            now.duration_since(share.created_at).unwrap_or_default() >= provisional_delay
        }
        _ => false,
    }
}

fn payout_idempotency_key(p: &PendingPayout) -> String {
    use sha2::{Digest, Sha256};

    let ts = p
        .initiated_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let input = format!("blocknet-pool:payout:{}:{}:{}", p.address, p.amount, ts);
    let digest = Sha256::digest(input.as_bytes());
    hex::encode(digest)
}

fn bounded_payout_interval(configured: Duration) -> Duration {
    configured.max(MIN_PAYOUT_INTERVAL)
}

fn daemon_ready_for_payouts(status: &NodeStatus) -> bool {
    !status.syncing
}

fn should_drop_pending_payout(err: &anyhow::Error) -> bool {
    http_error_body_contains(err, 400, "invalid address")
        || http_error_body_contains(err, 400, "self-sends are temporarily disabled")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbShare;
    use crate::node::HttpError;
    use crate::store::PoolStore;
    use std::sync::Arc;

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
            created_at,
        }
    }

    fn test_store() -> Arc<PoolStore> {
        let path = std::env::temp_dir().join(format!(
            "blocknet-pool-payout-test-{}.sqlite",
            rand::random::<u64>()
        ));
        PoolStore::open_sqlite(path.to_str().expect("path")).expect("open sqlite store")
    }

    fn trust_policy(
        min_verified_shares: u64,
        min_verified_ratio: f64,
        provisional_cap_multiplier: f64,
    ) -> PayoutTrustPolicy {
        PayoutTrustPolicy {
            min_verified_shares,
            min_verified_ratio,
            provisional_cap_multiplier,
        }
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
            trust_policy(1, 0.05, 3.0),
            |_| false,
        );
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
            trust_policy(1, 0.0, 3.0),
            |_| false,
        );
        assert_eq!(total, 10);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert!(!weights.contains_key("a2"));
    }

    #[test]
    fn payout_weighting_enforces_verified_ratio() {
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
            trust_policy(1, 0.25, 3.0),
            |_| false,
        );
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
            trust_policy(1, 0.0, 1.0),
            |_| false,
        );
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
            trust_policy(1, 0.0, 0.0),
            |_| false,
        );
        assert_eq!(total, 120);
        assert_eq!(weights.get("a2").copied(), Some(120));
    }

    #[test]
    fn payout_weighting_excludes_addresses_under_force_verify() {
        let store = test_store();
        store
            .escalate_address_risk(
                "a2",
                "fraud",
                Duration::from_secs(60),
                Duration::from_secs(60 * 60),
                Duration::from_secs(60 * 60),
                false,
            )
            .expect("seed risk");

        let processor = PayoutProcessor::new(
            Config {
                provisional_share_delay: "0s".to_string(),
                ..Config::default()
            },
            Arc::clone(&store),
            Arc::new(NodeClient::new("http://127.0.0.1:1", "").expect("node")),
        );

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

        let (weights, total) = processor
            .weight_shares_for_payout(&shares)
            .expect("weight shares");
        assert_eq!(total, 10);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert!(!weights.contains_key("a2"));
    }

    #[test]
    fn weighted_allocation_distributes_full_amount() {
        let store = test_store();
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
    fn payouts_are_skipped_while_syncing() {
        let status = NodeStatus {
            peer_id: "peer".to_string(),
            peers: 1,
            chain_height: 100,
            best_hash: "abc".to_string(),
            total_work: 1,
            mempool_size: 0,
            mempool_bytes: 0,
            syncing: true,
            identity_age: "1m".to_string(),
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
}
