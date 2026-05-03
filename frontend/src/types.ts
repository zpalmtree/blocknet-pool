export type Route =
  | "dashboard"
  | "start"
  | "luck"
  | "blocks"
  | "payouts"
  | "stats"
  | "admin"
  | "status";
export type AdminTab =
  | "miners"
  | "holds"
  | "balances"
  | "shares"
  | "rewards"
  | "recovery"
  | "logs";
export type Range = "1h" | "24h" | "7d" | "30d";

export type UnixLike =
  | number
  | string
  | { secs_since_epoch?: number; nanos_since_epoch?: number }
  | null
  | undefined;

export interface StatsResponse {
  pool: {
    miners: number;
    hashrate: number;
    blocks_found: number;
    orphaned_blocks: number;
    orphan_rate_pct: number;
    paid_to_miners_total: number;
  };
  chain: {
    current_job_height: number | null;
    network_hashrate: number | null;
  };
}

export interface InfoResponse {
  pool_name: string;
  pool_url: string;
  stratum_port: number;
  pool_fee_pct: number;
  min_payout_amount: number;
  blocks_before_payout: number;
  pplns_window_duration: string;
}

export interface BlockItem {
  height: number;
  hash: string;
  reward: number;
  confirmed: boolean;
  orphaned: boolean;
  timestamp: UnixLike;
  effort_pct: number | null;
  duration_seconds: number | null;
}

export interface PayoutItem {
  total_amount: number;
  total_fee: number;
  recipient_count: number;
  tx_hashes: string[];
  timestamp: UnixLike;
  confirmed: boolean;
}

export interface PagedResponse<T> {
  items: T[];
  total: number;
}

interface MinerWorker {
  worker: string;
  hashrate: number;
  accepted: number;
  rejected: number;
  last_share_at: UnixLike;
}

interface MinerShare {
  job_id: string;
  worker: string;
  difficulty: number;
  status: string;
  created_at: UnixLike;
}

interface MinerPendingBlockEstimate {
  height: number;
  hash: string;
  estimated_credit: number;
  credit_withheld: boolean;
  validation_state: string;
  validation_detail: string;
  confirmations_remaining: number;
  timestamp: UnixLike;
}

export interface MinerPendingEstimate {
  estimated_pending: number;
  blocks: MinerPendingBlockEstimate[];
}

interface MinerPayout {
  amount: number;
  fee: number;
  tx_hash: string;
  timestamp: UnixLike;
  confirmed: boolean;
}

interface MinerVerificationHold {
  mode: 'verified_only' | 'quarantined';
  reason: string | null;
  started_at: UnixLike;
  verified_only_until: UnixLike;
  quarantined_until: UnixLike;
  validation_hold_cause: 'invalid_samples' | 'provisional_backlog' | 'payout_coverage' | null;
  validation_pending_provisional: number | null;
}

interface MinerBalanceDetails {
  pending_confirmed: number;
  pending_queued: number;
  paid: number;
}

export interface MinerBalancePayload {
  address: string;
  balance: MinerBalanceDetails;
  pending_estimate: MinerPendingEstimate;
}

export interface ActiveVerificationHold {
  address: string;
  strikes: number;
  last_reason: string | null;
  reason: string | null;
  last_event_at: UnixLike;
  quarantined_until: UnixLike;
  force_verify_until: UnixLike;
  validation_forced_until: UnixLike;
  validation_hold_cause: 'invalid_samples' | 'provisional_backlog' | 'payout_coverage' | null;
  validation_pending_provisional: number;
  validation_recent_verified_difficulty: number;
  validation_recent_provisional_difficulty: number;
}

export interface MinerResponse {
  hashrate: number;
  mining_since: UnixLike;
  workers: MinerWorker[];
  shares: MinerShare[];
  blocks_found: number;
  payouts: MinerPayout[];
  verification_hold: MinerVerificationHold | null;
  total_accepted: number;
  total_rejected: number;
}

export interface MinerListItem {
  address: string;
  worker_count: number;
  hashrate: number;
  shares_accepted: number;
  shares_rejected: number;
  blocks_found: number;
  last_share_at: UnixLike;
}

export interface AdminBalanceItem {
  address: string;
  clean_payable: number;
  orphan_backed: number;
  pending: number;
  paid: number;
}

interface RewardWindowSummary {
  label: string;
  share_count: number;
  participant_count: number;
}

interface BlockRewardBlock {
  height: number;
  reward: number;
  timestamp: UnixLike;
  orphaned: boolean;
  paid_out: boolean;
}

interface BlockRewardParticipant {
  address: string;
  finder: boolean;
  risky: boolean;
  verified_shares: number;
  verified_difficulty: number;
  provisional_shares_eligible: number;
  provisional_difficulty_eligible: number;
  preview_weight: number;
  preview_share_pct: number;
  preview_credit: number;
  preview_status: string;
  payout_weight: number;
  payout_share_pct: number;
  payout_credit: number;
  payout_status: string;
  actual_credit: number | null;
  delta_vs_payout: number | null;
}

export interface BlockRewardBreakdownResponse {
  block: BlockRewardBlock;
  share_window: RewardWindowSummary;
  fee_amount: number;
  distributable_reward: number;
  preview_total_weight: number;
  payout_total_weight: number;
  actual_credit_total: number;
  actual_fee_amount: number | null;
  participants: BlockRewardParticipant[];
}

interface ValidationSummary {
  in_flight: number;
  candidate_queue_depth: number;
  regular_queue_depth: number;
  audit_queue_depth: number;
  candidate_oldest_age_millis: number | null;
  regular_oldest_age_millis: number | null;
  audit_oldest_age_millis: number | null;
  candidate_wait: PercentileSummary;
  regular_wait: PercentileSummary;
  audit_wait: PercentileSummary;
  validation_duration: PercentileSummary;
  audit_duration: PercentileSummary;
  sampled_shares: number;
  fraud_detections: number;
  candidate_false_claims: number;
  hot_accepts: number;
  sync_full_verifies: number;
  audit_enqueued: number;
  audit_verified: number;
  audit_rejected: number;
  audit_deferred: number;
  overload_mode: OverloadMode;
  effective_sample_rate: number;
}

export interface HealthResponse {
  pool_activity: {
    connected_miners: number;
    estimated_hashrate: number;
  };
  active_verification_holds: ActiveVerificationHold[];
}

export interface AdminBalanceOverviewResponse {
  wallet: {
    spendable: number;
    pending: number;
    total: number;
  };
  payouts: {
    clean_unpaid_count: number;
    queued_count: number;
    queued_amount: number;
  };
  ledger: {
    miner_paid_total: number;
    miner_unpaid_total: number;
    miner_clean_unpaid_total: number;
    miner_orphan_backed_unpaid_total: number;
    miner_balance_source_drift_total: number;
    net_block_reward_total: number;
    pool_fee_total: number;
    pool_fee_clean_unpaid_total: number;
    pool_fee_orphan_backed_unpaid_total: number;
    pool_fee_balance_source_drift_total: number;
    pool_fee_balance_total: number;
  };
}

interface PercentileSummary {
  p95_millis: number | null;
}

type OverloadMode = 'normal' | 'shed' | 'emergency';

interface AdminSubmitSummary {
  candidate_queue_depth: number;
  regular_queue_depth: number;
  candidate_oldest_age_millis: number | null;
  regular_oldest_age_millis: number | null;
  candidate_wait: PercentileSummary;
  regular_wait: PercentileSummary;
}

export interface AdminShareDiagnosticsWindow {
  label: string;
  accepted: number;
  rejected: number;
  by_reason: RejectionReasonCount[];
}

export interface AdminShareDiagnosticsResponse {
  windows: AdminShareDiagnosticsWindow[];
  submit: AdminSubmitSummary;
  validation: ValidationSummary;
}

export type RecoveryInstanceId = "primary" | "standby";
type RecoveryInstanceState =
  | "stopped"
  | "starting"
  | "syncing"
  | "ready"
  | "degraded"
  | "failed";
type RecoveryOperationState =
  | "running"
  | "succeeded"
  | "failed";
export type RecoveryOperationKind =
  | "pause_payouts"
  | "resume_payouts"
  | "start_standby_sync"
  | "rebuild_standby_wallet"
  | "cutover"
  | "purge_inactive_daemon";

interface RecoveryWalletStatus {
  loaded: boolean;
  address: string | null;
  synced_height: number | null;
  chain_height: number | null;
  outputs_total: number | null;
  outputs_unspent: number | null;
  outputs_pending: number | null;
  spendable: number | null;
  pending_unconfirmed: number | null;
  pending_unconfirmed_eta: number | null;
}

export interface RecoveryInstanceStatus {
  instance: RecoveryInstanceId;
  service: string;
  api: string;
  wallet_path: string;
  data_dir: string;
  service_state: string;
  state: RecoveryInstanceState;
  reachable: boolean;
  cookie_present: boolean;
  chain_height: number | null;
  peers: number | null;
  syncing: boolean | null;
  wallet: RecoveryWalletStatus;
  error: string | null;
}

export interface RecoveryOperation {
  id: number;
  kind: RecoveryOperationKind;
  target: RecoveryInstanceId | null;
  state: RecoveryOperationState;
  started_at: UnixLike;
  finished_at: UnixLike;
  message: string | null;
}

export interface RecoveryStatusResponse {
  payouts_paused: boolean;
  secret_configured: boolean;
  proxy_target: RecoveryInstanceId | null;
  active_instance: RecoveryInstanceId | null;
  warning: string | null;
  instances: RecoveryInstanceStatus[];
  operations: RecoveryOperation[];
}

export type ReconciliationPayoutResolutionAction = "restore_pending" | "drop_paid";

export interface AdminMissingCompletedPayoutIssue {
  tx_hash: string;
  payout_row_count: number;
  total_amount: number;
  total_fee: number;
  latest_timestamp: UnixLike;
  addresses: string[];
  live_linked_amount: number;
  orphaned_linked_amount: number;
  unlinked_amount: number;
}

interface AdminOrphanedBlockIssue {
  height: number;
  hash: string;
  credit_event_count: number;
  credited_address_count: number;
  remaining_credit_amount: number;
  paid_credit_amount: number;
  remaining_fee_amount: number;
  paid_fee_amount: number;
  pending_payout_count: number;
  broadcast_pending_payout_count: number;
}

export interface AdminReconciliationIssuesResponse {
  generated_at: UnixLike;
  missing_payouts: AdminMissingCompletedPayoutIssue[];
  orphaned_blocks: AdminOrphanedBlockIssue[];
}

export interface HashratePoint {
  timestamp: UnixLike;
  hashrate: number;
}

interface RoundProgress {
  elapsed_seconds: number;
  effort_pct: number | null;
  expected_block_seconds: number | null;
  timer_effort_pct: number | null;
}

interface PayoutEta {
  next_sweep_at: UnixLike;
  pending_total_amount: number;
  wallet_spendable: number | null;
  wallet_pending: number | null;
}

export interface LuckRound {
  block_height: number;
  block_hash: string;
  timestamp: UnixLike;
  effort_pct: number;
  duration_seconds: number;
  orphaned: boolean;
  confirmed: boolean;
}

interface RejectionReasonCount {
  reason: string;
  count: number;
}

interface RejectionAnalytics {
  accepted: number;
  rejected: number;
  by_reason: RejectionReasonCount[];
  totals_by_reason: RejectionReasonCount[];
}

export interface StatsInsightsResponse {
  round: RoundProgress;
  payout_eta: PayoutEta;
  avg_effort_pct: number | null;
  luck_history: LuckRound[];
  rejections: RejectionAnalytics;
}

interface StatusUptimeWindow {
  label: string;
  sample_count: number;
  external_sample_count: number;
  api_up_pct: number | null;
  stratum_up_pct: number | null;
  pool_up_pct: number | null;
  daemon_up_pct: number | null;
  database_up_pct: number | null;
  public_http_up_pct: number | null;
}

interface StatusIncident {
  id: number;
  kind: string;
  severity: string;
  started_at: UnixLike;
  duration_seconds: number | null;
  message: string;
  ongoing: boolean;
}

interface StatusServiceHealth {
  observed: boolean;
  healthy: boolean;
}

export interface StatusResponse {
  healthy: boolean;
  pool_uptime_seconds: number;
  services: {
    public_http: StatusServiceHealth;
    api: StatusServiceHealth;
    stratum: StatusServiceHealth;
    database: StatusServiceHealth;
    daemon: StatusServiceHealth;
  };
  daemon: {
    reachable: boolean;
    chain_height: number | null;
    syncing: boolean | null;
  };
  template: {
    observed: boolean;
    fresh: boolean;
    age_seconds: number | null;
    last_refresh_millis: number | null;
  };
  uptime: StatusUptimeWindow[];
  incidents: StatusIncident[];
}

export interface PagerState {
  offset: number;
  limit: number;
  total: number;
}
