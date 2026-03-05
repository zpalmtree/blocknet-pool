export type Route =
  | 'dashboard'
  | 'start'
  | 'blocks'
  | 'payouts'
  | 'stats'
  | 'admin'
  | 'status';
export type AdminTab = 'miners' | 'payouts' | 'fees' | 'health';
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
}

export interface BlockItem {
  height: number;
  hash: string;
  reward: number;
  confirmed: boolean;
  orphaned: boolean;
  timestamp: UnixLike;
}

export interface PayoutItem {
  total_amount: number;
  total_fee?: number;
  recipient_count: number;
  tx_hashes?: string[];
  timestamp: UnixLike;
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
}

export interface MinerResponse {
  hashrate: number;
  balance?: {
    pending: number;
    pending_total?: number;
    pending_confirmed?: number;
    pending_estimated?: number;
    paid: number;
  };
  workers?: MinerWorker[];
  shares?: MinerShare[];
  blocks_found?: BlockItem[];
  payouts?: MinerPayout[];
  pending_estimate?: MinerPendingEstimate;
  pending_note?: string | null;
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
}

export interface FeeEvent {
  block_height: number;
  amount: number;
  fee_address: string;
  timestamp: UnixLike;
}

export interface FeesResponse {
  total_collected: number;
  recent?: PagedResponse<FeeEvent>;
}

export interface HealthResponse {
  uptime_seconds: number;
  daemon?: { reachable?: boolean };
  job?: { template_age_seconds?: number | null };
  payouts?: { pending_count?: number };
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
  pending_count: number;
  pending_total_amount: number;
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
  luck_history: LuckRound[];
  rejections: {
    window: RejectionAnalytics;
  };
}

export interface StatusUptimeWindow {
  label: string;
  window_seconds: number;
  sample_count: number;
  up_pct?: number | null;
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
  daemon: {
    reachable: boolean;
    chain_height?: number | null;
    peers?: number | null;
    syncing?: boolean | null;
    mempool_size?: number | null;
    best_hash?: string | null;
    error?: string | null;
  };
  uptime: StatusUptimeWindow[];
  incidents: StatusIncident[];
}

export interface PagerState {
  offset: number;
  limit: number;
  total: number;
}
