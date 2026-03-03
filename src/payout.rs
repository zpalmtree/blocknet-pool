use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::db::{DbBlock, DbShare, PendingPayout, PoolFeeRecord};
use crate::node::{http_error_body_contains, is_http_status, NodeClient, NodeStatus};
use crate::protocol::validate_miner_address;
use crate::store::PoolStore;
use crate::validation::{SHARE_STATUS_PROVISIONAL, SHARE_STATUS_VERIFIED};

const MIN_PAYOUT_INTERVAL: Duration = Duration::from_secs(1);

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

        for mut block in blocks {
            if current_height < block.height
                || current_height - block.height < self.cfg.blocks_before_payout.max(0) as u64
            {
                continue;
            }

            let node_block = match self.node.get_block(&block.height.to_string()) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(height = block.height, error = %err, "failed to verify block");
                    continue;
                }
            };

            if node_block.hash != block.hash {
                let grace = 6u64;
                if current_height - block.height
                    < self.cfg.blocks_before_payout.max(0) as u64 + grace
                {
                    continue;
                }

                block.orphaned = true;
                if let Err(err) = self.store.update_block(&block) {
                    tracing::warn!(height = block.height, error = %err, "failed to mark orphan block");
                }
                continue;
            }

            block.confirmed = true;
            block.reward = node_block.reward;
            if let Err(err) = self.store.update_block(&block) {
                tracing::warn!(height = block.height, error = %err, "failed to update confirmed block");
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
        let mut risk_cache = HashMap::<String, bool>::new();

        Ok(weight_shares(shares, now, provisional_delay, |address| {
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
        }))
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
                tracing::warn!(
                    address = %entry.address,
                    amount = entry.amount,
                    age_secs = age.as_secs(),
                    "stale pending payout retained for idempotent retry"
                );
            }
        }
    }

    fn send_payouts(&self) {
        if !self.ensure_wallet_ready() {
            return;
        }

        let min_amount = (self.cfg.min_payout_amount * 100_000_000.0) as u64;
        let balances = match self.store.get_all_balances() {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(error = %err, "failed to list balances");
                return;
            }
        };

        for bal in balances {
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

            let pending = match existing_pending {
                Some(v) => v,
                None => {
                    if bal.pending < min_amount {
                        continue;
                    }
                    if let Err(err) = self.store.create_pending_payout(&bal.address, bal.pending) {
                        tracing::warn!(address = %bal.address, error = %err, "failed create pending payout");
                        continue;
                    }
                    match self.store.get_pending_payout(&bal.address) {
                        Ok(Some(v)) => v,
                        _ => continue,
                    }
                }
            };

            if pending.amount == 0 {
                continue;
            }
            if bal.pending < pending.amount {
                tracing::warn!(
                    address = %bal.address,
                    pending_amount = pending.amount,
                    balance_pending = bal.pending,
                    "pending payout exceeds local pending balance; skipping"
                );
                continue;
            }

            let wallet_balance = match self.node.get_wallet_balance() {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(error = %err, "failed wallet balance check");
                    return;
                }
            };
            if wallet_balance.spendable < pending.amount {
                continue;
            }

            let idempotency_key = payout_idempotency_key(&pending);
            let send = self
                .node
                .wallet_send(&bal.address, pending.amount, &idempotency_key);
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
                    } else {
                        tracing::warn!(address = %bal.address, error = %err, "wallet send failed");
                    }
                    continue;
                }
            };

            if let Err(err) =
                self.store
                    .complete_pending_payout(&bal.address, pending.amount, &sent.txid)
            {
                tracing::error!(address = %bal.address, tx = %sent.txid, error = %err, "critical payout reconciliation failure");
                continue;
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
    mut is_risky: F,
) -> (HashMap<String, u64>, u64)
where
    F: FnMut(&str) -> bool,
{
    let mut weights = HashMap::<String, u64>::new();
    let mut total = 0u64;

    for share in shares {
        if !is_share_payout_eligible(share, now, provisional_delay) {
            continue;
        }
        if is_risky(&share.miner) {
            continue;
        }
        let weight = share.difficulty;
        let entry = weights.entry(share.miner.clone()).or_default();
        *entry = entry.saturating_add(weight);
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
            sample_share(
                "a2",
                100,
                SHARE_STATUS_PROVISIONAL,
                now - Duration::from_secs(20 * 60),
            ),
        ];

        let (weights, total) = weight_shares(&shares, now, Duration::from_secs(15 * 60), |_| false);
        assert_eq!(total, 110);
        assert_eq!(weights.get("a1").copied(), Some(10));
        assert_eq!(weights.get("a2").copied(), Some(100));
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
}
