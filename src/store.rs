use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use tracing::warn;

use crate::config::Config;
use crate::db::{
    AddressRiskState, Balance, DbBlock, DbShare, Payout, PendingPayout, PoolFeeEvent,
    PoolFeeRecord, SqliteStore,
};
use crate::engine::{FoundBlockRecord, ShareRecord, ShareStore};
use crate::pgdb::PostgresStore;

pub enum PoolStore {
    Sqlite(Arc<SqliteStore>),
    Postgres(Arc<PostgresStore>),
}

impl std::fmt::Debug for PoolStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolStore::Sqlite(_) => f.write_str("PoolStore::Sqlite"),
            PoolStore::Postgres(_) => f.write_str("PoolStore::Postgres"),
        }
    }
}

impl PoolStore {
    pub fn open_from_config(cfg: &Config) -> Result<Arc<Self>> {
        if !cfg.database_url.trim().is_empty() {
            return Self::open_postgres(&cfg.database_url);
        }
        Self::open_sqlite(&cfg.database_path)
    }

    pub fn open_sqlite(path: &str) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::Sqlite(SqliteStore::open(path)?)))
    }

    pub fn open_postgres(url: &str) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::Postgres(PostgresStore::connect(url)?)))
    }

    pub fn get_shares_for_miner(&self, address: &str, limit: i64) -> Result<Vec<DbShare>> {
        match self {
            PoolStore::Sqlite(v) => v.get_shares_for_miner(address, limit),
            PoolStore::Postgres(v) => v.get_shares_for_miner(address, limit),
        }
    }

    pub fn get_total_share_count(&self) -> Result<u64> {
        match self {
            PoolStore::Sqlite(v) => v.get_total_share_count(),
            PoolStore::Postgres(v) => v.get_total_share_count(),
        }
    }

    pub fn add_block(&self, block: &DbBlock) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.add_block(block),
            PoolStore::Postgres(v) => v.add_block(block),
        }
    }

    pub fn get_block(&self, height: u64) -> Result<Option<DbBlock>> {
        match self {
            PoolStore::Sqlite(v) => v.get_block(height),
            PoolStore::Postgres(v) => v.get_block(height),
        }
    }

    pub fn update_block(&self, block: &DbBlock) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.update_block(block),
            PoolStore::Postgres(v) => v.update_block(block),
        }
    }

    pub fn get_recent_blocks(&self, limit: i64) -> Result<Vec<DbBlock>> {
        match self {
            PoolStore::Sqlite(v) => v.get_recent_blocks(limit),
            PoolStore::Postgres(v) => v.get_recent_blocks(limit),
        }
    }

    pub fn get_unconfirmed_blocks(&self) -> Result<Vec<DbBlock>> {
        match self {
            PoolStore::Sqlite(v) => v.get_unconfirmed_blocks(),
            PoolStore::Postgres(v) => v.get_unconfirmed_blocks(),
        }
    }

    pub fn get_unpaid_blocks(&self) -> Result<Vec<DbBlock>> {
        match self {
            PoolStore::Sqlite(v) => v.get_unpaid_blocks(),
            PoolStore::Postgres(v) => v.get_unpaid_blocks(),
        }
    }

    pub fn get_block_count(&self) -> Result<u64> {
        match self {
            PoolStore::Sqlite(v) => v.get_block_count(),
            PoolStore::Postgres(v) => v.get_block_count(),
        }
    }

    pub fn get_balance(&self, address: &str) -> Result<Balance> {
        match self {
            PoolStore::Sqlite(v) => v.get_balance(address),
            PoolStore::Postgres(v) => v.get_balance(address),
        }
    }

    pub fn update_balance(&self, bal: &Balance) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.update_balance(bal),
            PoolStore::Postgres(v) => v.update_balance(bal),
        }
    }

    pub fn credit_balance(&self, address: &str, amount: u64) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.credit_balance(address, amount),
            PoolStore::Postgres(v) => v.credit_balance(address, amount),
        }
    }

    pub fn debit_balance(&self, address: &str, amount: u64) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.debit_balance(address, amount),
            PoolStore::Postgres(v) => v.debit_balance(address, amount),
        }
    }

    pub fn get_all_balances(&self) -> Result<Vec<Balance>> {
        match self {
            PoolStore::Sqlite(v) => v.get_all_balances(),
            PoolStore::Postgres(v) => v.get_all_balances(),
        }
    }

    pub fn add_payout(&self, address: &str, amount: u64, fee: u64, tx_hash: &str) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.add_payout(address, amount, fee, tx_hash),
            PoolStore::Postgres(v) => v.add_payout(address, amount, fee, tx_hash),
        }
    }

    pub fn get_recent_payouts(&self, limit: i64) -> Result<Vec<Payout>> {
        match self {
            PoolStore::Sqlite(v) => v.get_recent_payouts(limit),
            PoolStore::Postgres(v) => v.get_recent_payouts(limit),
        }
    }

    pub fn record_pool_fee(
        &self,
        block_height: u64,
        amount: u64,
        fee_address: &str,
        timestamp: SystemTime,
    ) -> Result<bool> {
        match self {
            PoolStore::Sqlite(v) => v.record_pool_fee(block_height, amount, fee_address, timestamp),
            PoolStore::Postgres(v) => {
                v.record_pool_fee(block_height, amount, fee_address, timestamp)
            }
        }
    }

    pub fn get_total_pool_fees(&self) -> Result<u64> {
        match self {
            PoolStore::Sqlite(v) => v.get_total_pool_fees(),
            PoolStore::Postgres(v) => v.get_total_pool_fees(),
        }
    }

    pub fn get_recent_pool_fees(&self, limit: i64) -> Result<Vec<PoolFeeEvent>> {
        match self {
            PoolStore::Sqlite(v) => v.get_recent_pool_fees(limit),
            PoolStore::Postgres(v) => v.get_recent_pool_fees(limit),
        }
    }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.set_meta(key, value),
            PoolStore::Postgres(v) => v.set_meta(key, value),
        }
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            PoolStore::Sqlite(v) => v.get_meta(key),
            PoolStore::Postgres(v) => v.get_meta(key),
        }
    }

    pub fn clean_expired_seen_shares(&self) -> Result<u64> {
        match self {
            PoolStore::Sqlite(v) => v.clean_expired_seen_shares(),
            PoolStore::Postgres(v) => v.clean_expired_seen_shares(),
        }
    }

    pub fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        match self {
            PoolStore::Sqlite(v) => v.try_claim_share(job_id, nonce),
            PoolStore::Postgres(v) => v.try_claim_share(job_id, nonce),
        }
    }

    pub fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.release_share_claim(job_id, nonce),
            PoolStore::Postgres(v) => v.release_share_claim(job_id, nonce),
        }
    }

    pub fn get_address_risk(&self, address: &str) -> Result<Option<AddressRiskState>> {
        match self {
            PoolStore::Sqlite(v) => v.get_address_risk(address),
            PoolStore::Postgres(v) => v.get_address_risk(address),
        }
    }

    pub fn is_address_quarantined(
        &self,
        address: &str,
    ) -> Result<(bool, Option<AddressRiskState>)> {
        match self {
            PoolStore::Sqlite(v) => v.is_address_quarantined(address),
            PoolStore::Postgres(v) => v.is_address_quarantined(address),
        }
    }

    pub fn should_force_verify_address(
        &self,
        address: &str,
    ) -> Result<(bool, Option<AddressRiskState>)> {
        match self {
            PoolStore::Sqlite(v) => v.should_force_verify_address(address),
            PoolStore::Postgres(v) => v.should_force_verify_address(address),
        }
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
        match self {
            PoolStore::Sqlite(v) => v.escalate_address_risk(
                address,
                reason,
                quarantine_base,
                quarantine_max,
                force_verify_duration,
                apply_quarantine,
            ),
            PoolStore::Postgres(v) => v.escalate_address_risk(
                address,
                reason,
                quarantine_base,
                quarantine_max,
                force_verify_duration,
                apply_quarantine,
            ),
        }
    }

    pub fn get_risk_summary(&self) -> Result<(u64, u64)> {
        match self {
            PoolStore::Sqlite(v) => v.get_risk_summary(),
            PoolStore::Postgres(v) => v.get_risk_summary(),
        }
    }

    pub fn create_pending_payout(&self, address: &str, amount: u64) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.create_pending_payout(address, amount),
            PoolStore::Postgres(v) => v.create_pending_payout(address, amount),
        }
    }

    pub fn complete_pending_payout(
        &self,
        address: &str,
        amount: u64,
        fee: u64,
        tx_hash: &str,
    ) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.complete_pending_payout(address, amount, fee, tx_hash),
            PoolStore::Postgres(v) => v.complete_pending_payout(address, amount, fee, tx_hash),
        }
    }

    pub fn cancel_pending_payout(&self, address: &str) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.cancel_pending_payout(address),
            PoolStore::Postgres(v) => v.cancel_pending_payout(address),
        }
    }

    pub fn get_pending_payouts(&self) -> Result<Vec<PendingPayout>> {
        match self {
            PoolStore::Sqlite(v) => v.get_pending_payouts(),
            PoolStore::Postgres(v) => v.get_pending_payouts(),
        }
    }

    pub fn get_pending_payout(&self, address: &str) -> Result<Option<PendingPayout>> {
        match self {
            PoolStore::Sqlite(v) => v.get_pending_payout(address),
            PoolStore::Postgres(v) => v.get_pending_payout(address),
        }
    }

    pub fn get_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
    ) -> Result<Option<(u64, SystemTime)>> {
        match self {
            PoolStore::Sqlite(v) => v.get_vardiff_hint(address, worker),
            PoolStore::Postgres(v) => v.get_vardiff_hint(address, worker),
        }
    }

    pub fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.upsert_vardiff_hint(address, worker, difficulty, updated_at),
            PoolStore::Postgres(v) => {
                v.upsert_vardiff_hint(address, worker, difficulty, updated_at)
            }
        }
    }

    pub fn get_shares_since(&self, since: SystemTime) -> Result<Vec<DbShare>> {
        match self {
            PoolStore::Sqlite(v) => v.get_shares_since(since),
            PoolStore::Postgres(v) => v.get_shares_since(since),
        }
    }

    pub fn get_last_n_shares(&self, n: i64) -> Result<Vec<DbShare>> {
        match self {
            PoolStore::Sqlite(v) => v.get_last_n_shares(n),
            PoolStore::Postgres(v) => v.get_last_n_shares(n),
        }
    }

    pub fn get_shares_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<DbShare>> {
        match self {
            PoolStore::Sqlite(v) => v.get_shares_between(start, end),
            PoolStore::Postgres(v) => v.get_shares_between(start, end),
        }
    }

    pub fn get_last_n_shares_before(&self, before: SystemTime, n: i64) -> Result<Vec<DbShare>> {
        match self {
            PoolStore::Sqlite(v) => v.get_last_n_shares_before(before, n),
            PoolStore::Postgres(v) => v.get_last_n_shares_before(before, n),
        }
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
        match self {
            PoolStore::Sqlite(v) => {
                v.apply_block_credits_and_mark_paid_with_fee(block_height, credits, fee_record)
            }
            PoolStore::Postgres(v) => {
                v.apply_block_credits_and_mark_paid_with_fee(block_height, credits, fee_record)
            }
        }
    }
}

impl ShareStore for PoolStore {
    fn is_share_seen(&self, job_id: &str, nonce: u64) -> Result<bool> {
        match self {
            PoolStore::Sqlite(v) => v.is_share_seen(job_id, nonce),
            PoolStore::Postgres(v) => v.is_share_seen(job_id, nonce),
        }
    }

    fn mark_share_seen(&self, job_id: &str, nonce: u64) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.mark_share_seen(job_id, nonce),
            PoolStore::Postgres(v) => v.mark_share_seen(job_id, nonce),
        }
    }

    fn try_claim_share(&self, job_id: &str, nonce: u64) -> Result<bool> {
        PoolStore::try_claim_share(self, job_id, nonce)
    }

    fn release_share_claim(&self, job_id: &str, nonce: u64) -> Result<()> {
        PoolStore::release_share_claim(self, job_id, nonce)
    }

    fn add_share(&self, share: ShareRecord) -> Result<()> {
        match self {
            PoolStore::Sqlite(v) => v.add_share(share),
            PoolStore::Postgres(v) => v.add_share(share),
        }
    }

    fn add_found_block(&self, block: FoundBlockRecord) -> Result<()> {
        let candidate = DbBlock {
            height: block.height,
            hash: block.hash,
            difficulty: block.difficulty,
            finder: block.finder,
            finder_worker: block.finder_worker,
            reward: 0,
            timestamp: block.timestamp,
            confirmed: false,
            orphaned: false,
            paid_out: false,
        };

        match self {
            PoolStore::Sqlite(v) => {
                if v.insert_block_if_absent(&candidate)? {
                    return Ok(());
                }
                if let Some(existing) = v.get_block(candidate.height)? {
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
            PoolStore::Postgres(v) => {
                if v.insert_block_if_absent(&candidate)? {
                    return Ok(());
                }
                if let Some(existing) = v.get_block(candidate.height)? {
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
        }
    }

    fn is_address_quarantined(&self, address: &str) -> Result<bool> {
        let (quarantined, _) = PoolStore::is_address_quarantined(self, address)?;
        Ok(quarantined)
    }

    fn should_force_verify_address(&self, address: &str) -> Result<bool> {
        let (force_verify, _) = PoolStore::should_force_verify_address(self, address)?;
        Ok(force_verify)
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
        PoolStore::escalate_address_risk(
            self,
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
        PoolStore::get_vardiff_hint(self, address, worker)
    }

    fn upsert_vardiff_hint(
        &self,
        address: &str,
        worker: &str,
        difficulty: u64,
        updated_at: SystemTime,
    ) -> Result<()> {
        PoolStore::upsert_vardiff_hint(self, address, worker, difficulty, updated_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn test_store() -> Arc<PoolStore> {
        let path = std::env::temp_dir().join(format!(
            "blocknet-pool-store-test-{}.sqlite",
            rand::random::<u64>()
        ));
        PoolStore::open_sqlite(path.to_str().expect("path")).expect("open sqlite store")
    }

    #[test]
    fn found_block_insert_adds_record_when_missing() {
        let store = test_store();
        store
            .add_found_block(FoundBlockRecord {
                height: 42,
                hash: "h42".to_string(),
                difficulty: 123,
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
        assert_eq!(block.reward, 0);
        assert!(!block.confirmed);
        assert!(!block.paid_out);
    }

    #[test]
    fn found_block_insert_does_not_regress_existing_block_state() {
        let store = test_store();
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
            })
            .expect("seed existing block");

        store
            .add_found_block(FoundBlockRecord {
                height: 77,
                hash: "conflicting".to_string(),
                difficulty: 1,
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
        let store = test_store();
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
}
