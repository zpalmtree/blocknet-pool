use std::time::SystemTime;

use serde::Serialize;

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
    pub effort_pct: Option<f64>,
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

#[derive(Debug, Clone, Serialize)]
pub struct BlockCreditEvent {
    pub id: i64,
    pub block_height: u64,
    pub address: String,
    pub amount: u64,
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
