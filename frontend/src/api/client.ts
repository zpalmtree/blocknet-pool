import type {
  AdminBalanceItem,
  AdminBalanceOverviewResponse,
  AdminReconciliationIssuesResponse,
  AdminShareDiagnosticsResponse,
  BlockRewardBreakdownResponse,
  CheckpointsResponse,
  BlockItem,
  HashratePoint,
  HealthResponse,
  InfoResponse,
  LuckRound,
  MinerBalancePayload,
  MinerListItem,
  MinerResponse,
  PagedResponse,
  PayoutItem,
  ReconciliationPayoutResolutionAction,
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
  pollAccepted?: boolean;
  method?: string;
  body?: BodyInit | null;
  headers?: Record<string, string>;
}

interface DaemonLogStreamOptions {
  tail?: number;
  signal?: AbortSignal;
  onLine: (line: string) => void;
}

interface BlocknetHandleResolution {
  address: string;
  handle: string;
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

function retryDelay(response: Response): number {
  const value = response.headers.get('retry-after');
  if (!value) return 5000;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 5000;
}

function cooldownKey(path: string): string {
  if (path === '/api/stats' || /^\/api\/miner\/[^/]+(?:\/balance)?(?:\?|$)/.test(path)) {
    return 'public-telemetry';
  }
  return path;
}

export function createApiClient(getApiKey: () => string, showError: (message: string) => void) {
  const inFlight = new Map<string, Promise<unknown>>();
  const cooldowns = new Map<string, number>();
  const fetchOnce = async <T,>(path: string, opts?: FetchOptions, polls = 0): Promise<T> => {
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

    if (res.status === 202 && opts?.pollAccepted) {
      if (polls >= 120) throw new Error('Reward breakdown is still running; retry shortly');
      await new Promise((resolve) => setTimeout(resolve, Math.max(1000, retryDelay(res))));
      return fetchOnce<T>(path, opts, polls + 1);
    }

    if (!res.ok) {
      if (res.status === 429 || res.status === 503) {
        cooldowns.set(cooldownKey(path), Date.now() + retryDelay(res));
      }
      const msg =
        typeof body === 'object' && body && 'error' in (body as Record<string, unknown>)
          ? String((body as Record<string, unknown>).error)
          : `HTTP ${res.status}`;
      showError(msg);
      throw new Error(msg);
    }

    return body as T;
  };
  const fetchJson = <T,>(path: string, opts?: FetchOptions): Promise<T> => {
    // Coalesce overlapping refreshes, but never deduplicate writes.
    if (opts?.method && opts.method !== 'GET') return fetchOnce<T>(path, opts);
    const current = inFlight.get(path);
    if (current) return current as Promise<T>;
    for (const [key, until] of cooldowns) {
      if (until <= Date.now()) cooldowns.delete(key);
    }
    if (cooldowns.has(cooldownKey(path))) {
      return Promise.reject(new Error('Temporarily busy; waiting before retrying'));
    }
    const request = fetchOnce<T>(path, opts).finally(() => inFlight.delete(path));
    inFlight.set(path, request);
    return request;
  };
  const postJson = <T,>(path: string, body?: unknown): Promise<T> =>
    fetchJson<T>(path, {
      auth: true,
      method: 'POST',
      body: body == null ? null : JSON.stringify(body),
      headers: body == null ? undefined : { 'content-type': 'application/json' },
    });

  return {
    getInfo: () => fetchJson<InfoResponse>('/api/info'),
    getCheckpoints: () => fetchJson<CheckpointsResponse>('/api/checkpoints'),
    getStats: () => fetchJson<StatsResponse>('/api/stats'),
    getStatsHistory: (range: string) => fetchJson<HashratePoint[]>(`/api/stats/history?range=${encodeURIComponent(range)}`),
    getStatsInsights: (rejectionWindow?: string) =>
      fetchJson<StatsInsightsResponse>(
        rejectionWindow
          ? `/api/stats/insights?rejection_window=${encodeURIComponent(rejectionWindow)}`
          : '/api/stats/insights'
      ),
    getLuckHistory: (limit: number, offset: number) =>
      fetchJson<PagedResponse<LuckRound>>(withQuery('/api/luck', { limit, offset })),
    getStatus: () => fetchJson<StatusResponse>('/api/status'),
    getBlocks: (limit: number, offset: number, status?: string) =>
      fetchJson<PagedResponse<BlockItem>>(withQuery('/api/blocks', { limit, offset, status })),
    getRecentPayouts: (limit: number, offset: number) =>
      fetchJson<PagedResponse<PayoutItem>>(withQuery('/api/payouts/recent', { limit, offset })),
    getMiner: (address: string) => fetchJson<MinerResponse>(`/api/miner/${encodeURIComponent(address)}`),
    getMinerBalance: (address: string, includePendingEstimate = true) =>
      fetchJson<MinerBalancePayload>(
        `/api/miner/${encodeURIComponent(address)}/balance?include_pending_estimate=${includePendingEstimate ? 'true' : 'false'}`
      ),
    getMinerHashrate: (address: string, range: string) =>
      fetchJson<HashratePoint[]>(`/api/miner/${encodeURIComponent(address)}/hashrate?range=${encodeURIComponent(range)}`),
    resolveBlocknetHandle: (handle: string) =>
      fetchJson<BlocknetHandleResolution>(`https://blocknet.id/api/v1/resolve/${encodeURIComponent(handle)}`),
    getMiners: (limit: number, offset: number, sort: string, search?: string) =>
      fetchJson<PagedResponse<MinerListItem>>(withQuery('/api/miners', { limit, offset, sort, search }), { auth: true }),
    getAdminBlockRewardBreakdown: (height: number) =>
      fetchJson<BlockRewardBreakdownResponse>(`/api/admin/blocks/${encodeURIComponent(String(height))}/reward-breakdown`, {
        auth: true,
        pollAccepted: true,
      }),
    getHealth: () => fetchJson<HealthResponse>('/api/health', { auth: true }),
    getAdminBalanceOverview: () =>
      fetchJson<AdminBalanceOverviewResponse>('/api/admin/balance-overview', { auth: true }),
    getAdminReconciliationIssues: () =>
      fetchJson<AdminReconciliationIssuesResponse>('/api/admin/reconciliation/issues', { auth: true }),
    getAdminShareDiagnostics: () =>
      fetchJson<AdminShareDiagnosticsResponse>('/api/admin/shares', { auth: true }),
    getAdminBalances: (limit: number, offset: number, sort: string, search?: string) =>
      fetchJson<PagedResponse<AdminBalanceItem>>(withQuery('/api/admin/balances', { limit, offset, sort, search }), {
        auth: true,
      }),
    resolveAdminReconciliationPayout: (txHash: string, action: ReconciliationPayoutResolutionAction) =>
      postJson<void>('/api/admin/reconciliation/payouts/resolve', {
        tx_hash: txHash,
        action,
      }),
    retryAdminOrphanedBlockCleanup: (blockHeight: number) =>
      postJson<void>('/api/admin/reconciliation/orphan-blocks/retry-cleanup', {
        block_height: blockHeight,
      }),
    clearAddressRiskHistory: (address: string) =>
      postJson<void>('/api/admin/addresses/clear-risk-history', { address }),
    getRecoveryStatus: () => fetchJson<RecoveryStatusResponse>('/api/admin/recovery/status', { auth: true }),
    pauseRecoveryPayouts: () => postJson<RecoveryOperation>('/api/admin/recovery/payouts/pause'),
    resumeRecoveryPayouts: () => postJson<RecoveryOperation>('/api/admin/recovery/payouts/resume'),
    startInactiveSync: () => postJson<RecoveryOperation>('/api/admin/recovery/inactive/start-sync'),
    rebuildInactiveWallet: () => postJson<RecoveryOperation>('/api/admin/recovery/inactive/rebuild-wallet'),
    cutoverDaemon: (target: RecoveryInstanceId) => postJson<RecoveryOperation>('/api/admin/recovery/cutover', { target }),
    purgeInactiveDaemon: () => postJson<RecoveryOperation>('/api/admin/recovery/inactive/purge-resync'),
    streamDaemonLogs: async ({ tail, signal, onLine }: DaemonLogStreamOptions) => {
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

export type ApiClient = ReturnType<typeof createApiClient>;
