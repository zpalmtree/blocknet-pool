export type Route =
  | 'dashboard'
  | 'start'
  | 'luck'
  | 'blocks'
  | 'payouts'
  | 'stats'
  | 'admin'
  | 'status';
export type AdminTab = 'miners' | 'payouts' | 'fees' | 'devfee' | 'rewards' | 'health' | 'balances' | 'recovery' | 'logs';
export type Range = '1h' | '24h' | '7d' | '30d';

export type UnixLike =
  | number
  | string
  | { secs_since_epoch?: number; nanos_since_epoch?: number }
  | null
  | undefined;

export interface StatsResponse {
  pool: {
    miners: number;
    workers: number;
    hashrate: number;
    blocks_found: number;
    orphaned_blocks: number;
    orphan_rate_pct: number;
  };
  chain: {
    current_job_height?: number | null;
    network_hashrate?: number | null;
  };
}

export interface InfoResponse {
  pool_name: string;
  pool_url: string;
  stratum_port: number;
  pool_fee_pct?: number;
  pool_fee_flat?: number;
  min_payout_amount?: number;
  blocks_before_payout?: number;
  payout_scheme?: string;
  pplns_window?: number;
  pplns_window_duration?: string;
  provisional_share_delay?: string;
  sample_rate?: number;
  warmup_shares?: number;
  min_sample_every?: number;
  payout_min_verified_shares?: number;
  payout_min_verified_ratio?: number;
  payout_provisional_cap_multiplier?: number;
}

export interface BlockItem {
  height: number;
  hash: string;
  difficulty: number;
  finder: string;
  finder_worker: string;
  reward: number;
  confirmed: boolean;
  orphaned: boolean;
  paid_out: boolean;
  timestamp: UnixLike;
  effort_pct?: number | null;
  duration_seconds?: number | null;
  timer_effort_pct?: number | null;
  effort_band?: EffortBand | null;
}

export interface PayoutItem {
  total_amount: number;
  total_fee?: number;
  recipient_count: number;
  tx_hashes?: string[];
  timestamp: UnixLike;
  confirmed?: boolean;
}

export interface PagedMeta {
  limit: number;
  offset: number;
  returned: number;
  total: number;
}

export interface PagedResponse<T> {
  items: T[];
  page?: PagedMeta;
}

export interface MinerWorker {
  worker: string;
  hashrate: number;
  accepted: number;
  rejected: number;
  last_share_at: UnixLike;
}

export interface MinerShare {
  job_id: string;
  worker: string;
  difficulty: number;
  status: string;
  created_at: UnixLike;
}

export interface MinerPendingBlockEstimate {
  height: number;
  hash: string;
  reward: number;
  estimated_credit: number;
  credit_withheld?: boolean;
  validation_state?: string;
  validation_label?: string;
  validation_tone?: string;
  validation_detail?: string;
  confirmations_remaining: number;
  timestamp: UnixLike;
}

export interface MinerPendingEstimate {
  estimated_pending: number;
  blocks: MinerPendingBlockEstimate[];
}

export interface MinerPayout {
  id: number;
  address: string;
  amount: number;
  fee: number;
  tx_hash: string;
  timestamp: UnixLike;
  confirmed?: boolean;
}

export interface PendingPayout {
  address: string;
  amount: number;
  initiated_at: UnixLike;
}

export interface MinerResponse {
  hashrate: number;
  mining_since?: UnixLike;
  balance?: {
    pending: number;
    pending_confirmed?: number;
    pending_queued?: number;
    pending_unqueued?: number;
    paid: number;
  };
  workers?: MinerWorker[];
  shares?: MinerShare[];
  blocks_found?: BlockItem[];
  payouts?: MinerPayout[];
  pending_payout?: PendingPayout | null;
  pending_estimate?: MinerPendingEstimate;
  pending_note?: string | null;
  payout_note?: string | null;
  total_accepted?: number;
  total_rejected?: number;
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

export interface AdminPayoutItem {
  address: string;
  amount: number;
  fee: number;
  tx_hash: string;
  timestamp: UnixLike;
  confirmed?: boolean;
}

export interface AdminBalanceItem {
  address: string;
  pending: number;
  paid: number;
}

export interface FeeEvent {
  block_height: number;
  amount: number;
  fee_address: string;
  timestamp: UnixLike;
  status?: 'collected' | 'pending' | 'ready' | 'missing';
  confirmations_remaining?: number | null;
}

export interface RewardWindowSummary {
  label: string;
  start?: UnixLike | null;
  end: UnixLike;
  share_count: number;
  participant_count: number;
}

export interface BlockRewardParticipant {
  address: string;
  finder: boolean;
  risky: boolean;
  verified_shares: number;
  verified_difficulty: number;
  provisional_shares_eligible: number;
  provisional_difficulty_eligible: number;
  provisional_shares_ineligible: number;
  provisional_difficulty_ineligible: number;
  preview_weight: number;
  preview_share_pct: number;
  preview_credit: number;
  preview_status: string;
  payout_weight: number;
  payout_share_pct: number;
  payout_credit: number;
  payout_status: string;
  actual_credit?: number | null;
  delta_vs_payout?: number | null;
}

export interface BlockRewardBreakdownResponse {
  block: BlockItem;
  payout_scheme: string;
  share_window: RewardWindowSummary;
  fee_amount: number;
  distributable_reward: number;
  preview_total_weight: number;
  payout_total_weight: number;
  actual_credit_events_available: boolean;
  actual_credit_total: number;
  actual_fee_amount?: number | null;
  participants: BlockRewardParticipant[];
}

export interface FeesResponse {
  total_collected: number;
  total_pending?: number;
  recent?: PagedResponse<FeeEvent>;
}

export interface HealthResponse {
  uptime_seconds: number;
  api_key_configured?: boolean;
  daemon?: {
    reachable?: boolean;
    chain_height?: number | null;
    peers?: number | null;
    syncing?: boolean | null;
    mempool_size?: number | null;
    best_hash?: string | null;
    error?: string | null;
  };
  job?: {
    current_height?: number | null;
    current_difficulty?: number | null;
    template_id?: string | null;
    template_age_seconds?: number | null;
    last_refresh_millis?: number | null;
    tracked_templates?: number;
    active_assignments?: number;
  };
  payouts?: {
    pending_count?: number;
    pending_amount?: number;
    last_payout?: AdminPayoutItem | null;
  };
  wallet?: {
    spendable?: number;
    pending?: number;
    pending_unconfirmed?: number;
    pending_unconfirmed_eta?: number;
    total?: number;
  };
  validation?: {
    in_flight?: number;
    candidate_queue_depth?: number;
    regular_queue_depth?: number;
    tracked_addresses?: number;
    forced_verify_addresses?: number;
    total_shares?: number;
    sampled_shares?: number;
    invalid_samples?: number;
    pending_provisional?: number;
    fraud_detections?: number;
  };
}

export type RecoveryInstanceId = 'primary' | 'standby';
export type RecoveryInstanceState = 'stopped' | 'starting' | 'syncing' | 'ready' | 'degraded' | 'failed';
export type RecoveryOperationState = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
export type RecoveryOperationKind =
  | 'pause_payouts'
  | 'resume_payouts'
  | 'start_standby_sync'
  | 'rebuild_standby_wallet'
  | 'cutover'
  | 'purge_inactive_daemon';

export interface RecoveryWalletStatus {
  loaded: boolean;
  address?: string | null;
  spendable?: number | null;
  pending?: number | null;
  total?: number | null;
}

export interface RecoveryInstanceStatus {
  instance: RecoveryInstanceId;
  service: string;
  api: string;
  wallet_path: string;
  data_dir: string;
  cookie_path: string;
  service_state: string;
  state: RecoveryInstanceState;
  reachable: boolean;
  cookie_present: boolean;
  chain_height?: number | null;
  peers?: number | null;
  syncing?: boolean | null;
  best_hash?: string | null;
  wallet: RecoveryWalletStatus;
  error?: string | null;
}

export interface RecoveryOperation {
  id: number;
  kind: RecoveryOperationKind;
  target?: RecoveryInstanceId | null;
  state: RecoveryOperationState;
  created_at: UnixLike;
  started_at?: UnixLike;
  finished_at?: UnixLike;
  message?: string | null;
}

export interface RecoveryStatusResponse {
  enabled: boolean;
  payouts_paused: boolean;
  payout_pause_file: string;
  secret_configured: boolean;
  proxy_target?: RecoveryInstanceId | null;
  active_cookie_target?: RecoveryInstanceId | null;
  active_instance?: RecoveryInstanceId | null;
  warning?: string | null;
  instances: RecoveryInstanceStatus[];
  operations: RecoveryOperation[];
}

export interface AdminDevFeeWindow {
  label: string;
  window_seconds: number;
  pool_accepted_difficulty: number;
  dev_accepted_difficulty: number;
  dev_rejected_difficulty: number;
  dev_gross_difficulty: number;
  accepted_shares: number;
  rejected_shares: number;
  stale_rejected_shares: number;
  stale_rejected_difficulty: number;
  accepted_pct: number;
  gross_pct: number;
  reject_rate_pct: number;
  stale_reject_rate_pct: number;
}

export interface AdminDevFeeHintSummary {
  total_workers: number;
  below_floor_workers: number;
  at_floor_workers: number;
  above_floor_workers: number;
  min_difficulty?: number | null;
  median_difficulty?: number | null;
  max_difficulty?: number | null;
  latest_updated_at?: UnixLike | null;
}

export interface AdminDevFeeHintRow {
  worker: string;
  difficulty: number;
  updated_at: UnixLike;
  position: 'below-floor' | 'at-floor' | 'above-floor' | string;
}

export interface AdminDevFeeTelemetryResponse {
  address: string;
  reference_target_pct: number;
  hint_floor: number;
  windows: AdminDevFeeWindow[];
  hints: AdminDevFeeHintSummary;
  recent_hints: AdminDevFeeHintRow[];
}

export interface HashratePoint {
  timestamp: UnixLike;
  hashrate: number;
}

export interface EffortBand {
  label: string;
  tone: 'ok' | 'warn' | 'critical' | string;
}

export interface RoundProgress {
  round_start?: UnixLike;
  elapsed_seconds: number;
  round_work: number;
  expected_work?: number | null;
  effort_pct?: number | null;
  expected_block_seconds?: number | null;
  timer_effort_pct?: number | null;
  effort_band: EffortBand;
  timer_band: EffortBand;
  target_block_seconds: number;
}

export interface PayoutEta {
  last_payout_at?: UnixLike;
  estimated_next_payout_at?: UnixLike;
  eta_seconds?: number | null;
  typical_interval_seconds?: number | null;
  configured_interval_seconds?: number | null;
  next_sweep_at?: UnixLike;
  next_sweep_in_seconds?: number | null;
  pending_count: number;
  pending_total_amount: number;
  wallet_spendable?: number | null;
  wallet_pending?: number | null;
  queue_shortfall_amount?: number;
  liquidity_constrained?: boolean;
}

export interface LuckRound {
  block_height: number;
  block_hash: string;
  timestamp: UnixLike;
  difficulty: number;
  round_work: number;
  effort_pct: number;
  duration_seconds: number;
  timer_effort_pct: number;
  effort_band: EffortBand;
  orphaned: boolean;
  confirmed: boolean;
}

export interface RejectionReasonCount {
  reason: string;
  count: number;
}

export interface RejectionAnalytics {
  window_seconds: number;
  accepted: number;
  rejected: number;
  rejection_rate_pct: number;
  by_reason: RejectionReasonCount[];
  totals_by_reason: RejectionReasonCount[];
  total_rejected: number;
}

export interface StatsInsightsResponse {
  round: RoundProgress;
  payout_eta: PayoutEta;
  avg_effort_pct?: number | null;
  luck_history: LuckRound[];
  rejections: {
    window: RejectionAnalytics;
  };
}

export interface StatusUptimeWindow {
  label: string;
  window_seconds: number;
  sample_count: number;
  external_sample_count?: number | null;
  api_up_pct?: number | null;
  stratum_up_pct?: number | null;
  pool_up_pct?: number | null;
  daemon_up_pct?: number | null;
  database_up_pct?: number | null;
  public_http_up_pct?: number | null;
}

export interface StatusIncident {
  id: number;
  kind: string;
  severity: string;
  started_at: UnixLike;
  ended_at?: UnixLike;
  duration_seconds?: number | null;
  message: string;
  ongoing: boolean;
}

export interface StatusResponse {
  checked_at: UnixLike;
  pool_uptime_seconds: number;
  pool: {
    healthy: boolean;
    database_reachable: boolean;
    error?: string | null;
  };
  services: {
    public_http: {
      observed: boolean;
      healthy: boolean;
      last_sample_at?: UnixLike;
      message?: string | null;
    };
    api: {
      observed: boolean;
      healthy: boolean;
      last_sample_at?: UnixLike;
      message?: string | null;
    };
    stratum: {
      observed: boolean;
      healthy: boolean;
      last_sample_at?: UnixLike;
      message?: string | null;
    };
    database: {
      observed: boolean;
      healthy: boolean;
      last_sample_at?: UnixLike;
      message?: string | null;
    };
    daemon: {
      observed: boolean;
      healthy: boolean;
      last_sample_at?: UnixLike;
      message?: string | null;
    };
  };
  daemon: {
    reachable: boolean;
    chain_height?: number | null;
    peers?: number | null;
    syncing?: boolean | null;
    mempool_size?: number | null;
    best_hash?: string | null;
    error?: string | null;
  };
  template: {
    observed: boolean;
    fresh: boolean;
    age_seconds?: number | null;
    last_refresh_millis?: number | null;
  };
  uptime: StatusUptimeWindow[];
  incidents: StatusIncident[];
}

export interface PagerState {
  offset: number;
  limit: number;
  total: number;
}
