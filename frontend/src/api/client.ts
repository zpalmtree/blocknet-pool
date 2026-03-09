import type {
  AdminBalanceItem,
  AdminPayoutItem,
  BlockRewardBreakdownResponse,
  BlockItem,
  FeesResponse,
  HashratePoint,
  HealthResponse,
  InfoResponse,
  LuckRound,
  MinerListItem,
  MinerResponse,
  PagedResponse,
  PayoutItem,
  RecoveryInstanceId,
  RecoveryOperation,
  RecoveryStatusResponse,
  StatsInsightsResponse,
  StatusResponse,
  StatsResponse,
} from '../types';

interface QueryParams {
  [key: string]: string | number | undefined | null;
}

interface FetchOptions {
  auth?: boolean;
  method?: string;
  body?: BodyInit | null;
  headers?: Record<string, string>;
}

interface DaemonLogStreamOptions {
  tail?: number;
  signal?: AbortSignal;
  onLine: (line: string) => void;
}

export interface ApiClient {
  fetchJson<T>(path: string, opts?: FetchOptions): Promise<T>;
  getInfo(): Promise<InfoResponse>;
  getStats(): Promise<StatsResponse>;
  getStatsHistory(range: string): Promise<HashratePoint[]>;
  getStatsInsights(rejectionWindow?: string): Promise<StatsInsightsResponse>;
  getLuckHistory(params: QueryParams): Promise<PagedResponse<LuckRound>>;
  getStatus(): Promise<StatusResponse>;
  getBlocks(params: QueryParams): Promise<PagedResponse<BlockItem>>;
  getRecentPayouts(params: QueryParams): Promise<PagedResponse<PayoutItem>>;
  getMiner(address: string): Promise<MinerResponse>;
  getMinerHashrate(address: string, range: string): Promise<HashratePoint[]>;
  getMiners(params: QueryParams): Promise<PagedResponse<MinerListItem>>;
  getAdminPayouts(params: QueryParams): Promise<PagedResponse<AdminPayoutItem>>;
  getFees(params: QueryParams): Promise<FeesResponse>;
  getAdminBlockRewardBreakdown(height: number): Promise<BlockRewardBreakdownResponse>;
  getHealth(): Promise<HealthResponse>;
  getAdminBalances(params: QueryParams): Promise<PagedResponse<AdminBalanceItem>>;
  getRecoveryStatus(): Promise<RecoveryStatusResponse>;
  pauseRecoveryPayouts(): Promise<RecoveryOperation>;
  resumeRecoveryPayouts(): Promise<RecoveryOperation>;
  startInactiveSync(): Promise<RecoveryOperation>;
  rebuildInactiveWallet(): Promise<RecoveryOperation>;
  cutoverDaemon(target: RecoveryInstanceId): Promise<RecoveryOperation>;
  purgeInactiveDaemon(): Promise<RecoveryOperation>;
  streamDaemonLogs(opts: DaemonLogStreamOptions): Promise<void>;
}

function withQuery(path: string, params: QueryParams): string {
  const url = new URL(path, window.location.origin);
  for (const [k, v] of Object.entries(params)) {
    if (v != null && String(v).trim() !== '') {
      url.searchParams.set(k, String(v));
    }
  }
  return `${url.pathname}${url.search}`;
}

export function createApiClient(getApiKey: () => string, showError: (message: string) => void): ApiClient {
  const fetchJson = async <T,>(path: string, opts?: FetchOptions): Promise<T> => {
    const headers: Record<string, string> = { ...(opts?.headers ?? {}) };
    if (opts?.auth) {
      const key = getApiKey();
      if (key) headers['x-api-key'] = key;
    }

    let res: Response;
    try {
      res = await fetch(path, {
        method: opts?.method ?? 'GET',
        headers,
        body: opts?.body ?? null,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : 'network error';
      showError(message);
      throw err;
    }

    let body: unknown = null;
    try {
      body = await res.json();
    } catch {
      body = null;
    }

    if (!res.ok) {
      const msg =
        typeof body === 'object' && body && 'error' in (body as Record<string, unknown>)
          ? String((body as Record<string, unknown>).error)
          : `HTTP ${res.status}`;
      showError(msg);
      throw new Error(msg);
    }

    return body as T;
  };

  return {
    fetchJson,
    getInfo: () => fetchJson<InfoResponse>('/api/info'),
    getStats: () => fetchJson<StatsResponse>('/api/stats'),
    getStatsHistory: (range) => fetchJson<HashratePoint[]>(`/api/stats/history?range=${encodeURIComponent(range)}`),
    getStatsInsights: (rejectionWindow) =>
      fetchJson<StatsInsightsResponse>(
        rejectionWindow
          ? `/api/stats/insights?rejection_window=${encodeURIComponent(rejectionWindow)}`
          : '/api/stats/insights'
      ),
    getLuckHistory: (params) => fetchJson<PagedResponse<LuckRound>>(withQuery('/api/luck', params)),
    getStatus: () => fetchJson<StatusResponse>('/api/status'),
    getBlocks: (params) => fetchJson<PagedResponse<BlockItem>>(withQuery('/api/blocks', params)),
    getRecentPayouts: (params) => fetchJson<PagedResponse<PayoutItem>>(withQuery('/api/payouts/recent', params)),
    getMiner: (address) => fetchJson<MinerResponse>(`/api/miner/${encodeURIComponent(address)}`),
    getMinerHashrate: (address, range) =>
      fetchJson<HashratePoint[]>(`/api/miner/${encodeURIComponent(address)}/hashrate?range=${encodeURIComponent(range)}`),
    getMiners: (params) => fetchJson<PagedResponse<MinerListItem>>(withQuery('/api/miners', params), { auth: true }),
    getAdminPayouts: (params) => fetchJson<PagedResponse<AdminPayoutItem>>(withQuery('/api/payouts', params), {
      auth: true,
    }),
    getFees: (params) => fetchJson<FeesResponse>(withQuery('/api/fees', params), { auth: true }),
    getAdminBlockRewardBreakdown: (height) =>
      fetchJson<BlockRewardBreakdownResponse>(`/api/admin/blocks/${encodeURIComponent(String(height))}/reward-breakdown`, {
        auth: true,
      }),
    getHealth: () => fetchJson<HealthResponse>('/api/health', { auth: true }),
    getAdminBalances: (params: QueryParams) =>
      fetchJson<PagedResponse<AdminBalanceItem>>(withQuery('/api/admin/balances', params), { auth: true }),
    getRecoveryStatus: () => fetchJson<RecoveryStatusResponse>('/api/admin/recovery/status', { auth: true }),
    pauseRecoveryPayouts: () =>
      fetchJson<RecoveryOperation>('/api/admin/recovery/payouts/pause', { auth: true, method: 'POST' }),
    resumeRecoveryPayouts: () =>
      fetchJson<RecoveryOperation>('/api/admin/recovery/payouts/resume', { auth: true, method: 'POST' }),
    startInactiveSync: () =>
      fetchJson<RecoveryOperation>('/api/admin/recovery/inactive/start-sync', { auth: true, method: 'POST' }),
    rebuildInactiveWallet: () =>
      fetchJson<RecoveryOperation>('/api/admin/recovery/inactive/rebuild-wallet', { auth: true, method: 'POST' }),
    cutoverDaemon: (target: RecoveryInstanceId) =>
      fetchJson<RecoveryOperation>('/api/admin/recovery/cutover', {
        auth: true,
        method: 'POST',
        body: JSON.stringify({ target }),
        headers: {
          'content-type': 'application/json',
        },
      }),
    purgeInactiveDaemon: () =>
      fetchJson<RecoveryOperation>('/api/admin/recovery/inactive/purge-resync', {
        auth: true,
        method: 'POST',
      }),
    streamDaemonLogs: async ({ tail, signal, onLine }) => {
      const key = getApiKey().trim();
      if (!key) {
        throw new Error('api key required');
      }

      const url = withQuery('/api/daemon/logs/stream', {
        tail: tail ?? 200,
      });
      let res: Response;
      try {
        res = await fetch(url, {
          headers: {
            'x-api-key': key,
          },
          signal,
        });
      } catch (err) {
        if (err instanceof DOMException && err.name === 'AbortError') {
          throw err;
        }
        const message = err instanceof Error ? err.message : 'network error';
        showError(message);
        throw err;
      }

      if (!res.ok) {
        let message = `HTTP ${res.status}`;
        try {
          const body = (await res.json()) as { error?: unknown };
          if (body && typeof body.error === 'string' && body.error.trim()) {
            message = body.error;
          }
        } catch {
          // keep default status message
        }
        showError(message);
        throw new Error(message);
      }

      if (!res.body) {
        throw new Error('stream body unavailable');
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let pending = '';

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        pending += decoder.decode(value, { stream: true });
        for (;;) {
          const newline = pending.indexOf('\n');
          if (newline < 0) break;
          const line = pending.slice(0, newline).replace(/\r$/, '');
          pending = pending.slice(newline + 1);
          if (line !== '') {
            onLine(line);
          }
        }
      }

      pending += decoder.decode();
      const lastLine = pending.replace(/\r$/, '');
      if (lastLine) {
        onLine(lastLine);
      }
    },
  };
}
