export type Route = 'dashboard' | 'start' | 'blocks' | 'payouts' | 'stats' | 'admin';
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

export interface MinerResponse {
  hashrate: number;
  balance?: { pending: number; paid: number };
  workers?: MinerWorker[];
  shares?: MinerShare[];
  blocks_found?: BlockItem[];
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

export interface PagerState {
  offset: number;
  limit: number;
  total: number;
}
