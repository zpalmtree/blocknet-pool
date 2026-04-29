use std::time::SystemTime;

use serde::{Deserialize, Serialize};

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
    pub claimed_hash: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ShareReplayData {
    pub job_id: String,
    pub header_base: Vec<u8>,
    pub network_target: [u8; 32],
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ShareReplayUpdate {
    pub share_id: i64,
    pub status: String,
    pub was_sampled: bool,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingAuditShare {
    pub share_id: i64,
    pub job_id: String,
    pub miner: String,
    pub worker: String,
    pub difficulty: u64,
    pub nonce: u64,
    pub claimed_hash: Option<[u8; 32]>,
    pub header_base: Vec<u8>,
    pub network_target: [u8; 32],
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

#[derive(Debug, Clone)]
pub struct DbLuckRound {
    pub block_height: u64,
    pub block_hash: String,
    pub timestamp: SystemTime,
    pub difficulty: u64,
    pub round_work: u64,
    pub duration_seconds: u64,
    pub orphaned: bool,
    pub confirmed: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Balance {
    pub address: String,
    pub pending: u64,
    pub paid: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationHoldCause {
    InvalidSamples,
    ProvisionalBacklog,
    PayoutCoverage,
}

impl ValidationHoldCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidSamples => "invalid_samples",
            Self::ProvisionalBacklog => "provisional_backlog",
            Self::PayoutCoverage => "payout_coverage",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveVerificationHold {
    pub address: String,
    pub strikes: u64,
    pub suspected_fraud_strikes: u64,
    pub last_reason: Option<String>,
    pub reason: Option<String>,
    pub last_event_at: Option<SystemTime>,
    pub quarantined_until: Option<SystemTime>,
    pub force_verify_until: Option<SystemTime>,
    pub validation_forced_until: Option<SystemTime>,
    pub validation_hold_cause: Option<ValidationHoldCause>,
    pub validation_pending_provisional: u64,
    pub validation_recent_verified_difficulty: u64,
    pub validation_recent_provisional_difficulty: u64,
}

#[derive(Debug, Clone)]
pub struct ValidationHoldState {
    pub forced_started_at: Option<SystemTime>,
    pub forced_until: Option<SystemTime>,
    pub hold_cause: Option<ValidationHoldCause>,
    pub pending_provisional: u64,
    pub recent_verified_difficulty: u64,
    pub recent_provisional_difficulty: u64,
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
    #[serde(skip_serializing)]
    pub batch_id: Option<String>,
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
    #[serde(skip_serializing)]
    pub batch_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingPayoutBatchMember {
    pub address: String,
    pub amount: u64,
    pub fee: u64,
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
    pub paid_amount: u64,
    pub reversible: bool,
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
    pub strike_window_until: Option<SystemTime>,
    pub last_reason: Option<String>,
    pub last_event_at: Option<SystemTime>,
    pub quarantined_until: Option<SystemTime>,
    pub force_verify_until: Option<SystemTime>,
    pub suspected_fraud_strikes: u64,
    pub suspected_fraud_window_until: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorHeartbeat {
    pub id: i64,
    pub sampled_at: SystemTime,
    pub source: String,
    pub synthetic: bool,
    pub api_up: Option<bool>,
    pub stratum_up: Option<bool>,
    pub db_up: bool,
    pub daemon_up: Option<bool>,
    pub public_http_up: Option<bool>,
    pub daemon_syncing: Option<bool>,
    pub chain_height: Option<u64>,
    pub template_age_seconds: Option<u64>,
    pub last_refresh_millis: Option<u64>,
    pub stratum_snapshot_age_seconds: Option<u64>,
    pub connected_miners: Option<u64>,
    pub connected_workers: Option<u64>,
    pub estimated_hashrate: Option<f64>,
    pub wallet_up: Option<bool>,
    pub last_accepted_share_at: Option<SystemTime>,
    pub last_accepted_share_age_seconds: Option<u64>,
    pub payout_pending_count: Option<u64>,
    pub payout_pending_amount: Option<u64>,
    pub oldest_pending_payout_at: Option<SystemTime>,
    pub oldest_pending_payout_age_seconds: Option<u64>,
    pub oldest_pending_send_started_at: Option<SystemTime>,
    pub oldest_pending_send_age_seconds: Option<u64>,
    pub validation_candidate_queue_depth: Option<u64>,
    pub validation_regular_queue_depth: Option<u64>,
    pub summary_state: String,
    pub details_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorHeartbeatUpsert {
    pub sampled_at: SystemTime,
    pub source: String,
    pub synthetic: bool,
    pub api_up: Option<bool>,
    pub stratum_up: Option<bool>,
    pub db_up: bool,
    pub daemon_up: Option<bool>,
    pub public_http_up: Option<bool>,
    pub daemon_syncing: Option<bool>,
    pub chain_height: Option<u64>,
    pub template_age_seconds: Option<u64>,
    pub last_refresh_millis: Option<u64>,
    pub stratum_snapshot_age_seconds: Option<u64>,
    pub connected_miners: Option<u64>,
    pub connected_workers: Option<u64>,
    pub estimated_hashrate: Option<f64>,
    pub wallet_up: Option<bool>,
    pub last_accepted_share_at: Option<SystemTime>,
    pub last_accepted_share_age_seconds: Option<u64>,
    pub payout_pending_count: Option<u64>,
    pub payout_pending_amount: Option<u64>,
    pub oldest_pending_payout_at: Option<SystemTime>,
    pub oldest_pending_payout_age_seconds: Option<u64>,
    pub oldest_pending_send_started_at: Option<SystemTime>,
    pub oldest_pending_send_age_seconds: Option<u64>,
    pub validation_candidate_queue_depth: Option<u64>,
    pub validation_regular_queue_depth: Option<u64>,
    pub summary_state: String,
    pub details_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorIncident {
    pub id: i64,
    pub dedupe_key: String,
    pub kind: String,
    pub severity: String,
    pub visibility: String,
    pub source: String,
    pub summary: String,
    pub detail: Option<String>,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
    pub ended_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorIncidentUpsert {
    pub dedupe_key: String,
    pub kind: String,
    pub severity: String,
    pub visibility: String,
    pub source: String,
    pub summary: String,
    pub detail: Option<String>,
    pub started_at: SystemTime,
    pub updated_at: SystemTime,
}
