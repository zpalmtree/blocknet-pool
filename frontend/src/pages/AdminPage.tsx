import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
import { parseAnsiLine, type ParsedAnsiSegment } from '../lib/ansi';
import {
  fmtSeconds,
  formatCoinAmount,
  formatCoins,
  formatFee,
  humanRate,
  shortAddr,
  shortTx,
  timeAgo,
  toUnixMs,
} from '../lib/format';
import type {
  AdminBalanceItem,
  AdminDevFeeTelemetryResponse,
  AdminPayoutItem,
  AdminTab,
  BlockRewardBreakdownResponse,
  BlockItem,
  FeeEvent,
  HealthResponse,
  MinerListItem,
  PagerState,
} from '../types';

const MAX_DAEMON_LOG_LINES = 1000;
const DAEMON_LOG_RECONNECT_DELAY_MS = 1500;

function rewardStatusLabel(status: string): string {
  switch (status) {
    case 'included':
      return 'Included';
    case 'capped_provisional':
      return 'Included (capped)';
    case 'finder_fallback':
      return 'Finder fallback';
    case 'risky':
      return 'Risky';
    case 'awaiting_verified_shares':
      return 'Needs verified shares';
    case 'awaiting_verified_ratio':
      return 'Needs verified ratio';
    case 'recorded_only':
      return 'Recorded only';
    default:
      return 'No eligible shares';
  }
}

function rewardStatusTone(status: string): string {
  switch (status) {
    case 'included':
    case 'finder_fallback':
      return 'var(--good)';
    case 'capped_provisional':
      return 'var(--warn)';
    case 'risky':
      return 'var(--bad)';
    case 'recorded_only':
      return 'var(--muted)';
    default:
      return 'var(--warn)';
  }
}

function feeStatusLabel(status: string | undefined): string {
  switch (status) {
    case 'pending':
      return 'Pending';
    case 'ready':
      return 'Ready';
    case 'missing':
      return 'Missing';
    default:
      return 'Collected';
  }
}

function feeStatusBadgeClass(status: string | undefined): string {
  switch (status) {
    case 'missing':
      return 'badge-orphaned';
    case 'pending':
    case 'ready':
      return 'badge-pending';
    default:
      return 'badge-confirmed';
  }
}

function feeStatusNote(item: FeeEvent): string | null {
  if (item.status === 'pending' && (item.confirmations_remaining ?? 0) > 0) {
    return `${item.confirmations_remaining} conf`;
  }
  if (item.status === 'ready') {
    return 'awaiting fee sweep';
  }
  if (item.status === 'missing') {
    return 'fee row missing';
  }
  return null;
}

function formatSignedCoins(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const prefix = value > 0 ? '+' : value < 0 ? '-' : '';
  return `${prefix}${formatCoinAmount(Math.abs(value))} BNT`;
}

function pct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(2)}%`;
}

function formatRefreshLag(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return '-';
  if (ms < 1000) return '<1s';
  return fmtSeconds(Math.max(1, Math.floor(ms / 1000)));
}

function compactHash(value: string | null | undefined): string {
  if (!value) return '-';
  if (value.length <= 18) return value;
  return `${value.slice(0, 8)}...${value.slice(-8)}`;
}

function devFeeHintBadgeClass(position: string): string {
  switch (position) {
    case 'below-floor':
      return 'badge-orphaned';
    case 'above-floor':
      return 'badge-confirmed';
    default:
      return 'badge-pending';
  }
}

function devFeeHintLabel(position: string): string {
  switch (position) {
    case 'below-floor':
      return 'Below floor';
    case 'above-floor':
      return 'Above floor';
    default:
      return 'At floor';
  }
}

function rewardBlockOptionLabel(block: BlockItem): string {
  const status = block.orphaned ? 'orphaned' : block.confirmed ? 'confirmed' : 'pending';
  return `#${block.height} • ${status} • ${timeAgo(block.timestamp)}`;
}

interface AdminPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
  apiKey: string;
  apiKeyInput: string;
  setApiKeyInput: (value: string) => void;
  onSaveApiKey: () => void;
  onClearApiKey: () => void;
  onJumpToStats: (address: string) => void;
}

interface DaemonLogLine {
  id: number;
  segments: ParsedAnsiSegment[];
}

export function AdminPage({
  active,
  api,
  liveTick,
  apiKey,
  apiKeyInput,
  setApiKeyInput,
  onSaveApiKey,
  onClearApiKey,
  onJumpToStats,
}: AdminPageProps) {
  const [tab, setTab] = useState<AdminTab>('miners');

  const [minersSearch, setMinersSearch] = useState('');
  const [minersSort, setMinersSort] = useState('hashrate_desc');
  const [minersItems, setMinersItems] = useState<MinerListItem[]>([]);
  const [minersPager, setMinersPager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });

  const [payoutAddress, setPayoutAddress] = useState('');
  const [payoutTx, setPayoutTx] = useState('');
  const [payoutItems, setPayoutItems] = useState<AdminPayoutItem[]>([]);
  const [payoutPager, setPayoutPager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });

  const [feesTotal, setFeesTotal] = useState(0);
  const [feesPendingTotal, setFeesPendingTotal] = useState(0);
  const [feeItems, setFeeItems] = useState<FeeEvent[]>([]);
  const [feePager, setFeePager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });
  const [devFeeTelemetry, setDevFeeTelemetry] = useState<AdminDevFeeTelemetryResponse | null>(null);

  const [rewardBlockInput, setRewardBlockInput] = useState('');
  const [rewardBlockOptions, setRewardBlockOptions] = useState<BlockItem[]>([]);
  const [rewardBlockOptionsLoading, setRewardBlockOptionsLoading] = useState(false);
  const [rewardAddressFilter, setRewardAddressFilter] = useState('');
  const [rewardBreakdown, setRewardBreakdown] = useState<BlockRewardBreakdownResponse | null>(null);
  const [rewardBreakdownLoading, setRewardBreakdownLoading] = useState(false);

  const [health, setHealth] = useState<HealthResponse | null>(null);

  const [balancesSearch, setBalancesSearch] = useState('');
  const [balancesSort, setBalancesSort] = useState('pending_desc');
  const [balancesItems, setBalancesItems] = useState<AdminBalanceItem[]>([]);
  const [balancesPager, setBalancesPager] = useState<PagerState>({ offset: 0, limit: 50, total: 0 });

  const [daemonLogs, setDaemonLogs] = useState<DaemonLogLine[]>([]);
  const [daemonLogsTail, setDaemonLogsTail] = useState(200);
  const [daemonLogsStatus, setDaemonLogsStatus] = useState<'idle' | 'connecting' | 'live' | 'error'>('idle');
  const [daemonLogsError, setDaemonLogsError] = useState('');
  const [daemonLogsAutoScroll, setDaemonLogsAutoScroll] = useState(true);
  const [daemonLogsConnectSeq, setDaemonLogsConnectSeq] = useState(0);
  const daemonLogsRef = useRef<HTMLDivElement | null>(null);
  const daemonLogSeq = useRef(0);
  const rewardBreakdownRequestSeq = useRef(0);

  const loadMiners = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getMiners({
        paged: 'true',
        limit: minersPager.limit,
        offset: minersPager.offset,
        sort: minersSort,
        search: minersSearch.trim() || undefined,
      });
      const items = d.items || [];
      setMinersItems(items);
      setMinersPager((prev) => ({ ...prev, total: d.page ? d.page.total : items.length }));
    } catch {
      setMinersItems([]);
    }
  }, [api, apiKey, minersPager.limit, minersPager.offset, minersSearch, minersSort]);

  const loadPayouts = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getAdminPayouts({
        paged: 'true',
        limit: payoutPager.limit,
        offset: payoutPager.offset,
        sort: 'time_desc',
        address: payoutAddress.trim() || undefined,
        tx_hash: payoutTx.trim() || undefined,
      });
      const items = d.items || [];
      setPayoutItems(items);
      setPayoutPager((prev) => ({ ...prev, total: d.page ? d.page.total : items.length }));
    } catch {
      setPayoutItems([]);
    }
  }, [api, apiKey, payoutAddress, payoutPager.limit, payoutPager.offset, payoutTx]);

  const loadFees = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getFees({
        paged: 'true',
        limit: feePager.limit,
        offset: feePager.offset,
        sort: 'time_desc',
      });
      setFeesTotal(d.total_collected || 0);
      setFeesPendingTotal(d.total_pending || 0);
      const items = d.recent?.items || [];
      setFeeItems(items);
      setFeePager((prev) => ({ ...prev, total: d.recent?.page ? d.recent.page.total : items.length }));
    } catch {
      setFeesPendingTotal(0);
      setFeeItems([]);
    }
  }, [api, apiKey, feePager.limit, feePager.offset]);

  const loadDevFeeTelemetry = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.fetchJson<AdminDevFeeTelemetryResponse>('/api/admin/dev-fee', { auth: true });
      setDevFeeTelemetry(d);
    } catch {
      setDevFeeTelemetry(null);
    }
  }, [api, apiKey]);

  const loadRewardBreakdown = useCallback(
    async (heightOverride?: number | string) => {
      if (!apiKey) return;
      const raw = String(heightOverride ?? rewardBlockInput).trim();
      const height = Math.floor(Number(raw));
      if (!raw || !Number.isFinite(height) || height < 0) return;
      const requestSeq = rewardBreakdownRequestSeq.current + 1;
      rewardBreakdownRequestSeq.current = requestSeq;

      setRewardBlockInput(String(height));
      setRewardBreakdownLoading(true);
      try {
        const d = await api.getAdminBlockRewardBreakdown(height);
        if (rewardBreakdownRequestSeq.current !== requestSeq) return;
        setRewardBreakdown(d);
      } catch {
        // handled by api client
      } finally {
        if (rewardBreakdownRequestSeq.current !== requestSeq) return;
        setRewardBreakdownLoading(false);
      }
    },
    [api, apiKey, rewardBlockInput]
  );

  const loadRewardBlocks = useCallback(async () => {
    if (!apiKey) return;
    setRewardBlockOptionsLoading(true);
    try {
      const d = await api.getBlocks({
        paged: 'true',
        limit: 50,
        offset: 0,
        sort: 'height_desc',
      });
      const items = d.items || [];
      setRewardBlockOptions(items);
      setRewardBlockInput((prev) => {
        if (prev.trim() || !items.length) return prev;
        return String(items[0].height);
      });
    } catch {
      setRewardBlockOptions([]);
    } finally {
      setRewardBlockOptionsLoading(false);
    }
  }, [api, apiKey]);

  const loadHealth = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getHealth();
      setHealth(d);
    } catch {
      setHealth(null);
    }
  }, [api, apiKey]);

  const loadBalances = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getAdminBalances({
        paged: 'true',
        limit: balancesPager.limit,
        offset: balancesPager.offset,
        sort: balancesSort,
        search: balancesSearch.trim() || undefined,
      });
      const items = d.items || [];
      setBalancesItems(items);
      setBalancesPager((prev) => ({ ...prev, total: d.page ? d.page.total : items.length }));
    } catch {
      setBalancesItems([]);
    }
  }, [api, apiKey, balancesPager.limit, balancesPager.offset, balancesSearch, balancesSort]);

  useEffect(() => {
    if (!active || !apiKey) return;

    if (tab === 'miners') void loadMiners();
    if (tab === 'payouts') void loadPayouts();
    if (tab === 'fees') void loadFees();
    if (tab === 'devfee') void loadDevFeeTelemetry();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'health') void loadHealth();
    if (tab === 'balances') void loadBalances();
  }, [
    active,
    apiKey,
    loadBalances,
    loadFees,
    loadDevFeeTelemetry,
    loadHealth,
    loadMiners,
    loadPayouts,
    loadRewardBlocks,
    loadRewardBreakdown,
    rewardBlockInput,
    tab,
  ]);

  useEffect(() => {
    if (!active || !apiKey || liveTick <= 0) return;
    if (liveTick % 2 !== 0) return;

    if (tab === 'miners') void loadMiners();
    if (tab === 'payouts') void loadPayouts();
    if (tab === 'fees') void loadFees();
    if (tab === 'devfee') void loadDevFeeTelemetry();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'health') void loadHealth();
    if (tab === 'balances') void loadBalances();
  }, [
    active,
    apiKey,
    liveTick,
    tab,
    loadBalances,
    loadFees,
    loadDevFeeTelemetry,
    loadHealth,
    loadMiners,
    loadPayouts,
    loadRewardBlocks,
    loadRewardBreakdown,
    rewardBlockInput,
  ]);

  useEffect(() => {
    if (!active || !apiKey || tab !== 'logs') return;

    const controller = new AbortController();
    let stopped = false;
    let reconnectTimer: number | null = null;

    const waitForReconnect = () =>
      new Promise<void>((resolve) => {
        const finish = () => {
          controller.signal.removeEventListener('abort', finish);
          if (reconnectTimer != null) {
            window.clearTimeout(reconnectTimer);
            reconnectTimer = null;
          }
          resolve();
        };

        reconnectTimer = window.setTimeout(() => {
          finish();
        }, DAEMON_LOG_RECONNECT_DELAY_MS);
        controller.signal.addEventListener('abort', finish, { once: true });
      });

    const connect = async () => {
      while (!stopped && !controller.signal.aborted) {
        setDaemonLogs([]);
        setDaemonLogsStatus('connecting');
        setDaemonLogsError('');

        try {
          await api.streamDaemonLogs({
            tail: daemonLogsTail,
            signal: controller.signal,
            onLine: (line) => {
              setDaemonLogsStatus('live');
              setDaemonLogs((prev) => {
                const next = prev.concat({
                  id: daemonLogSeq.current++,
                  segments: parseAnsiLine(line),
                });
                if (next.length <= MAX_DAEMON_LOG_LINES) {
                  return next;
                }
                return next.slice(next.length - MAX_DAEMON_LOG_LINES);
              });
            },
          });
          if (stopped || controller.signal.aborted) return;
          setDaemonLogsStatus('connecting');
          await waitForReconnect();
        } catch (err) {
          if (stopped || controller.signal.aborted) return;
          if (err instanceof DOMException && err.name === 'AbortError') return;
          setDaemonLogsStatus('error');
          setDaemonLogsError(err instanceof Error ? err.message : 'failed to stream daemon logs');
          return;
        }
      }
    };

    void connect();
    return () => {
      stopped = true;
      controller.abort();
    };
  }, [active, api, apiKey, daemonLogsTail, daemonLogsConnectSeq, tab]);

  useEffect(() => {
    if (!daemonLogsAutoScroll || tab !== 'logs') return;
    const viewport = daemonLogsRef.current;
    if (!viewport) return;
    viewport.scrollTop = viewport.scrollHeight;
  }, [daemonLogs, daemonLogsAutoScroll, tab]);

  const apiStatus = apiKey ? 'Key set' : 'No key';
  const devFee24h = useMemo(
    () => devFeeTelemetry?.windows?.find((row) => row.label === '24h') ?? devFeeTelemetry?.windows?.[0] ?? null,
    [devFeeTelemetry]
  );
  const daemonLogsStatusText =
    daemonLogsStatus === 'connecting'
      ? 'Connecting'
      : daemonLogsStatus === 'live'
        ? 'Live'
        : daemonLogsStatus === 'error'
          ? 'Error'
          : 'Idle';
  const daemonLogsStatusDot =
    daemonLogsStatus === 'live'
      ? 'dot-green'
      : daemonLogsStatus === 'error'
        ? 'dot-red'
        : 'dot-amber';
  const rewardLoadDisabled = !rewardBlockInput.trim() || !Number.isFinite(Number(rewardBlockInput.trim()));
  const rewardSelectedBlockValue = useMemo(() => {
    const selected = rewardBlockInput.trim();
    if (!selected) return '';
    return rewardBlockOptions.some((block) => String(block.height) === selected) ? selected : '';
  }, [rewardBlockInput, rewardBlockOptions]);
  const filteredRewardParticipants = useMemo(() => {
    const items = rewardBreakdown?.participants || [];
    const filter = rewardAddressFilter.trim().toLowerCase();
    if (!filter) return items;
    return items.filter((row) => row.address.toLowerCase().includes(filter));
  }, [rewardAddressFilter, rewardBreakdown?.participants]);
  const rewardBreakdownTotals = useMemo(() => {
    if (!rewardBreakdown) return null;
    return rewardBreakdown.participants.reduce(
      (totals, row) => {
        totals.previewCredit += row.preview_credit;
        totals.payoutCredit += row.payout_credit;
        totals.verifiedDifficulty += row.verified_difficulty;
        totals.provisionalEligibleDifficulty += row.provisional_difficulty_eligible;
        return totals;
      },
      {
        previewCredit: 0,
        payoutCredit: 0,
        verifiedDifficulty: 0,
        provisionalEligibleDifficulty: 0,
      }
    );
  }, [rewardBreakdown]);
  const rewardBreakdownOrphaned = rewardBreakdown?.block.orphaned ?? false;
  const rewardBreakdownProjected = !!rewardBreakdown && !rewardBreakdownOrphaned && !rewardBreakdown.block.paid_out;
  const rawHealthJson = useMemo(() => (health ? JSON.stringify(health, null, 2) : ''), [health]);
  const copyHealthJson = useCallback(() => {
    if (!rawHealthJson || !navigator.clipboard) return;
    void navigator.clipboard.writeText(rawHealthJson);
  }, [rawHealthJson]);
  const rewardActualBlockTotal =
    rewardBreakdown && rewardBreakdown.actual_credit_events_available && rewardBreakdown.actual_fee_amount != null
      ? rewardBreakdown.actual_credit_total + rewardBreakdown.actual_fee_amount
      : null;
  const rewardFeeDelta =
    rewardBreakdown?.actual_fee_amount != null ? rewardBreakdown.actual_fee_amount - rewardBreakdown.fee_amount : null;
  const rewardProjectedBlockTotal =
    rewardBreakdown && rewardBreakdownTotals
      ? rewardBreakdownTotals.payoutCredit + rewardBreakdown.fee_amount
      : null;
  const rewardActualBlockDelta =
    rewardActualBlockTotal != null && rewardProjectedBlockTotal != null
      ? rewardActualBlockTotal - rewardProjectedBlockTotal
      : null;
  const rewardPreviewColumnLabel = rewardBreakdownOrphaned ? 'Preview Estimate' : 'Preview';
  const rewardPayoutColumnLabel = rewardBreakdownProjected ? 'Projected Payout' : 'Payout';
  const rewardPayoutWeightLabel = rewardBreakdownProjected ? 'Projected Weight' : 'Payout Weight';
  const rewardStatusColumnLabel = rewardBreakdownOrphaned
    ? 'Resolution'
    : rewardBreakdownProjected
      ? 'Projected Status'
      : 'Status';
  const rewardAuditIntro = rewardBreakdownOrphaned
    ? 'Preview shows the estimate miners would have seen before the round was orphaned. The actual payout outcome from an orphaned block is always zero.'
    : rewardBreakdownProjected
      ? 'Preview shows the current estimator used on My Stats. Projected payout shows the final split under the pool payout rules if this block reaches payout processing. Actual shows what the payout processor has recorded so far.'
      : 'Preview shows the unconfirmed estimator used on My Stats. Payout shows the final weighting rules, including verified-share anchors and any provisional cap. Actual shows what the payout processor recorded for this block, when available.';
  const rewardAuditNotice = rewardBreakdown
    ? rewardBreakdownOrphaned
      ? 'This block was orphaned. Preview is shown for reference, but the actual payout outcome from this block was 0.'
      : rewardBreakdownProjected
        ? 'This block has not been paid yet. The Projected columns show the current final payout math; Actual remains empty until the payout processor records credits.'
        : rewardBreakdown.actual_credit_events_available
        ? ''
        : rewardBreakdown.block.paid_out
          ? 'This block was paid before per-block credit audit rows were available, so the Actual column cannot be shown from storage.'
          : ''
    : '';
  const rewardAuditNoticeStyles = rewardBreakdownOrphaned
    ? {
        background: 'var(--status-orphaned-bg)',
        borderColor: 'var(--status-orphaned-border)',
      }
    : {
        background: 'rgba(247, 180, 75, 0.12)',
        borderColor: 'rgba(247, 180, 75, 0.45)',
      };

  return (
    <div className={active ? 'page active' : 'page'} id="page-admin">
      <h2>Admin</h2>

      <div className="card section">
        <h3>API Key</h3>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            type="password"
            placeholder="Enter API key"
            style={{ flex: 1, minWidth: 200 }}
            value={apiKeyInput}
            onChange={(e) => setApiKeyInput(e.target.value)}
          />
          <button className="btn btn-primary" onClick={onSaveApiKey}>
            Save
          </button>
          <button className="btn btn-secondary" onClick={onClearApiKey}>
            Clear
          </button>
          <span style={{ fontSize: 13, color: 'var(--muted)' }}>
            <span className={`status-dot ${apiKey ? 'dot-green' : 'dot-amber'}`} />
            {apiStatus}
          </span>
        </div>
      </div>

      {!apiKey ? (
        <div className="auth-gate card section">
          <p>Enter your API key above to access admin features.</p>
        </div>
      ) : (
        <div id="admin-content">
          <div className="sub-tabs" id="admin-tabs">
            <button className={tab === 'miners' ? 'active' : ''} onClick={() => setTab('miners')}>
              Miners
            </button>
            <button className={tab === 'payouts' ? 'active' : ''} onClick={() => setTab('payouts')}>
              Payouts
            </button>
            <button className={tab === 'fees' ? 'active' : ''} onClick={() => setTab('fees')}>
              Fees
            </button>
            <button className={tab === 'devfee' ? 'active' : ''} onClick={() => setTab('devfee')}>
              Dev Fee
            </button>
            <button className={tab === 'rewards' ? 'active' : ''} onClick={() => setTab('rewards')}>
              Rewards
            </button>
            <button className={tab === 'health' ? 'active' : ''} onClick={() => setTab('health')}>
              Health
            </button>
            <button className={tab === 'balances' ? 'active' : ''} onClick={() => setTab('balances')}>
              Balances
            </button>
            <button className={tab === 'logs' ? 'active' : ''} onClick={() => setTab('logs')}>
              Daemon Logs
            </button>
          </div>

          <div style={{ display: tab === 'miners' ? '' : 'none' }}>
            <div className="filter-bar">
              <input
                type="text"
                placeholder="Search address..."
                value={minersSearch}
                onChange={(e) => setMinersSearch(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    setMinersPager((p) => ({ ...p, offset: 0 }));
                    void loadMiners();
                  }
                }}
              />
              <select value={minersSort} onChange={(e) => setMinersSort(e.target.value)}>
                <option value="hashrate_desc">Hashrate (high)</option>
                <option value="accepted_desc">Accepted (high)</option>
                <option value="rejected_desc">Rejected (high)</option>
                <option value="last_share_desc">Last Share (recent)</option>
                <option value="address_asc">Address (A-Z)</option>
              </select>
              <button
                className="btn btn-primary"
                onClick={() => {
                  setMinersPager((p) => ({ ...p, offset: 0 }));
                }}
              >
                Search
              </button>
            </div>

            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>Workers</th>
                    <th>Hashrate</th>
                    <th>Accepted</th>
                    <th>Rejected</th>
                    <th>Blocks</th>
                    <th>Last Share</th>
                  </tr>
                </thead>
                <tbody>
                  {!minersItems.length ? (
                    <tr>
                      <td colSpan={7} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No miners connected
                      </td>
                    </tr>
                  ) : (
                    minersItems.map((m) => (
                      <tr key={m.address}>
                        <td>
                          <a
                            href="/stats"
                            onClick={(e) => {
                              e.preventDefault();
                              onJumpToStats(m.address);
                            }}
                          >
                            {shortAddr(m.address)}
                          </a>
                        </td>
                        <td>{m.worker_count || 0}</td>
                        <td>{humanRate(m.hashrate)}</td>
                        <td>{m.shares_accepted || 0}</td>
                        <td>{m.shares_rejected || 0}</td>
                        <td>{m.blocks_found || 0}</td>
                        <td title={new Date(toUnixMs(m.last_share_at)).toLocaleString()}>{timeAgo(m.last_share_at)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              <Pager
                offset={minersPager.offset}
                limit={minersPager.limit}
                total={minersPager.total}
                onPrev={() => setMinersPager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
                onNext={() => setMinersPager((p) => ({ ...p, offset: p.offset + p.limit }))}
              />
            </div>
          </div>

          <div style={{ display: tab === 'payouts' ? '' : 'none' }}>
            <div className="filter-bar">
              <input
                type="text"
                placeholder="Filter by address..."
                value={payoutAddress}
                onChange={(e) => setPayoutAddress(e.target.value)}
              />
              <input
                type="text"
                placeholder="Filter by tx hash..."
                value={payoutTx}
                onChange={(e) => setPayoutTx(e.target.value)}
              />
              <button className="btn btn-primary" onClick={() => setPayoutPager((p) => ({ ...p, offset: 0 }))}>
                Search
              </button>
            </div>

            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>Amount</th>
                    <th>Fee</th>
                    <th>TX Hash</th>
                    <th>Status</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!payoutItems.length ? (
                    <tr>
                      <td colSpan={6} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No payouts
                      </td>
                    </tr>
                  ) : (
                    payoutItems.map((p, idx) => (
                      <tr key={`${p.tx_hash}-${idx}`}>
                        <td title={p.address}>
                          <a
                            href="/stats"
                            onClick={(e) => {
                              e.preventDefault();
                              onJumpToStats(p.address);
                            }}
                          >
                            {shortAddr(p.address)}
                          </a>
                        </td>
                        <td>{formatCoins(p.amount)}</td>
                        <td>{formatFee(p.fee)}</td>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/tx/${p.tx_hash}`} target="_blank" rel="noopener" title={p.tx_hash}>
                            {shortTx(p.tx_hash)}
                          </a>
                        </td>
                        <td>
                          <span className={`badge ${p.confirmed === false ? 'badge-pending' : 'badge-confirmed'}`}>
                            {p.confirmed === false ? 'unconfirmed' : 'confirmed'}
                          </span>
                        </td>
                        <td title={new Date(toUnixMs(p.timestamp)).toLocaleString()}>{timeAgo(p.timestamp)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              <Pager
                offset={payoutPager.offset}
                limit={payoutPager.limit}
                total={payoutPager.total}
                onPrev={() => setPayoutPager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
                onNext={() => setPayoutPager((p) => ({ ...p, offset: p.offset + p.limit }))}
              />
            </div>
          </div>

          <div style={{ display: tab === 'fees' ? '' : 'none' }}>
            <p style={{ fontSize: 14, color: 'var(--muted)', marginBottom: 12 }}>
              Collected: <span className="mono">{formatCoins(feesTotal)}</span>
              {' · '}
              Pending estimate: <span className="mono">{formatCoins(feesPendingTotal)}</span>
            </p>

            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Block</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!feeItems.length ? (
                    <tr>
                      <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No fee rows
                      </td>
                    </tr>
                  ) : (
                    feeItems.map((f, idx) => (
                      <tr key={`${f.block_height}-${idx}`}>
                        <td>{f.block_height}</td>
                        <td>{formatCoins(f.amount)}</td>
                        <td>
                          <div className="fee-status-cell">
                            <span className={`badge ${feeStatusBadgeClass(f.status)}`}>{feeStatusLabel(f.status)}</span>
                            {feeStatusNote(f) ? <span className="fee-status-cell__meta">{feeStatusNote(f)}</span> : null}
                          </div>
                        </td>
                        <td title={new Date(toUnixMs(f.timestamp)).toLocaleString()}>{timeAgo(f.timestamp)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              <Pager
                offset={feePager.offset}
                limit={feePager.limit}
                total={feePager.total}
                onPrev={() => setFeePager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
                onNext={() => setFeePager((p) => ({ ...p, offset: p.offset + p.limit }))}
              />
            </div>
          </div>

          <div style={{ display: tab === 'devfee' ? '' : 'none' }}>
            <div className="card section" style={{ marginBottom: 16 }}>
              <p className="section-lead" style={{ margin: 0 }}>
                Credited % tracks the dev fee that actually lands in pool rewards. Gross % includes rejected dev work,
                which makes pool or miner-side loss visible. The reference target assumes connected hash is mining with
                Seine on this pool.
              </p>
            </div>

            <div className="stats-grid">
              <div className="stat-card">
                <div className="label">Reference Target</div>
                <div className="value mono">{pct(devFeeTelemetry?.reference_target_pct)}</div>
              </div>
              <div className="stat-card">
                <div className="label">24h Credited</div>
                <div className="value mono">{pct(devFee24h?.accepted_pct)}</div>
              </div>
              <div className="stat-card">
                <div className="label">24h Gross</div>
                <div className="value mono">{pct(devFee24h?.gross_pct)}</div>
              </div>
              <div className="stat-card">
                <div className="label">Hint Floor</div>
                <div className="value mono">{devFeeTelemetry?.hint_floor ?? '-'}</div>
              </div>
            </div>

            <div className="stats-grid">
              <div className="stat-card">
                <div className="label">Tracked Workers</div>
                <div className="value mono">{devFeeTelemetry?.hints.total_workers ?? '-'}</div>
              </div>
              <div className="stat-card">
                <div className="label">Below Floor</div>
                <div className="value mono">{devFeeTelemetry?.hints.below_floor_workers ?? '-'}</div>
              </div>
              <div className="stat-card">
                <div className="label">At Floor</div>
                <div className="value mono">{devFeeTelemetry?.hints.at_floor_workers ?? '-'}</div>
              </div>
              <div className="stat-card">
                <div className="label">Above Floor</div>
                <div className="value mono">{devFeeTelemetry?.hints.above_floor_workers ?? '-'}</div>
              </div>
            </div>

            <div className="section">
              <div className="section-header">
                <div>
                  <h3>Windowed Attribution</h3>
                  <p className="section-lead">
                    Compare dev accepted share difficulty to total pool accepted share difficulty over the same window.
                  </p>
                </div>
              </div>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Window</th>
                      <th>Credited %</th>
                      <th>Gross %</th>
                      <th>Reject Rate</th>
                      <th>Stale Reject %</th>
                      <th>Accepted Diff</th>
                      <th>Rejected Diff</th>
                      <th>Accepted Shares</th>
                      <th>Rejected Shares</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!devFeeTelemetry?.windows?.length ? (
                      <tr>
                        <td colSpan={9} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                          No dev fee telemetry yet
                        </td>
                      </tr>
                    ) : (
                      devFeeTelemetry.windows.map((row) => (
                        <tr key={row.label}>
                          <td>{row.label}</td>
                          <td className="mono">{pct(row.accepted_pct)}</td>
                          <td className="mono">{pct(row.gross_pct)}</td>
                          <td className="mono">{pct(row.reject_rate_pct)}</td>
                          <td className="mono">
                            {pct(row.stale_reject_rate_pct)}
                            <div style={{ fontSize: 11, color: 'var(--muted)' }}>{row.stale_rejected_shares} stale</div>
                          </td>
                          <td className="mono">{row.dev_accepted_difficulty}</td>
                          <td className="mono">{row.dev_rejected_difficulty}</td>
                          <td className="mono">{row.accepted_shares}</td>
                          <td className="mono">{row.rejected_shares}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="section">
              <div className="section-header">
                <div>
                  <h3>Hint Diagnostics</h3>
                  <p className="section-lead">
                    Recent dev-worker vardiff hints help confirm whether workers are stuck on the floor or ramping to a
                    healthier range.
                  </p>
                </div>
              </div>

              <div className="stats-grid stats-grid-3">
                <div className="stat-card">
                  <div className="label">Median Hint</div>
                  <div className="value mono">{devFeeTelemetry?.hints.median_difficulty ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Min / Max</div>
                  <div className="value mono">
                    {devFeeTelemetry?.hints.min_difficulty ?? '-'} / {devFeeTelemetry?.hints.max_difficulty ?? '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Latest Hint Update</div>
                  <div className="value mono" style={{ fontSize: 16 }}>
                    {devFeeTelemetry?.hints.latest_updated_at ? timeAgo(devFeeTelemetry.hints.latest_updated_at) : '-'}
                  </div>
                </div>
              </div>

              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Worker</th>
                      <th>Difficulty</th>
                      <th>Status</th>
                      <th>Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!devFeeTelemetry?.recent_hints?.length ? (
                      <tr>
                        <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                          No dev worker hints recorded yet
                        </td>
                      </tr>
                    ) : (
                      devFeeTelemetry.recent_hints.map((row) => (
                        <tr key={`${row.worker}-${String(row.updated_at)}`}>
                          <td title={row.worker}>{row.worker}</td>
                          <td className="mono">{row.difficulty}</td>
                          <td>
                            <span className={`badge ${devFeeHintBadgeClass(row.position)}`}>
                              {devFeeHintLabel(row.position)}
                            </span>
                          </td>
                          <td title={new Date(toUnixMs(row.updated_at)).toLocaleString()}>{timeAgo(row.updated_at)}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div style={{ display: tab === 'rewards' ? '' : 'none' }}>
            <div className="filter-bar">
              <select
                value={rewardSelectedBlockValue}
                onChange={(e) => {
                  const value = e.target.value;
                  setRewardBlockInput(value);
                  if (value && !rewardBreakdownLoading) {
                    void loadRewardBreakdown(value);
                  }
                }}
                disabled={rewardBlockOptionsLoading}
              >
                <option value="">
                  {rewardBlockOptionsLoading
                    ? 'Loading mined blocks...'
                    : rewardBlockOptions.length
                      ? 'Select a mined block...'
                      : 'No mined blocks loaded'}
                </option>
                {rewardBlockOptions.map((block) => (
                  <option key={`${block.height}-${block.hash}`} value={block.height}>
                    {rewardBlockOptionLabel(block)}
                  </option>
                ))}
              </select>
              <input
                type="text"
                placeholder="Filter participant address..."
                value={rewardAddressFilter}
                onChange={(e) => setRewardAddressFilter(e.target.value)}
              />
              <button
                className="btn btn-primary"
                disabled={rewardLoadDisabled || rewardBreakdownLoading}
                onClick={() => void loadRewardBreakdown()}
              >
                {rewardBreakdownLoading ? 'Loading…' : 'Load Block'}
              </button>
            </div>

            {!rewardBreakdown ? (
              <div className="card section">
                <h3>Reward Breakdown</h3>
                <p style={{ color: 'var(--muted)', fontSize: 14 }}>
                  Load a block height to inspect the current preview math, the final payout math, and any recorded
                  credited amounts for that block.
                </p>
              </div>
            ) : (
              <>
                <div className="stats-grid stats-grid-dense" style={{ marginBottom: 20 }}>
                  <div className="stat-card">
                    <div className="label">Block</div>
                    <div className="value">{rewardBreakdown.block.height}</div>
                    <div className="stat-meta">{timeAgo(rewardBreakdown.block.timestamp)}</div>
                  </div>
                  <div className="stat-card">
                    <div className="label">{rewardBreakdown.block.orphaned ? 'Nominal Reward' : 'Reward'}</div>
                    <div className="value">{formatCoins(rewardBreakdown.block.reward)}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Round was orphaned, so no distributable credits were finalized.'
                        : `Fee ${formatCoins(rewardBreakdown.fee_amount)} · Net ${formatCoins(
                            rewardBreakdown.distributable_reward
                          )}`}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Window</div>
                    <div className="value">{rewardBreakdown.share_window.label}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.share_window.share_count} shares · {rewardBreakdown.share_window.participant_count} miners
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Preview Weight</div>
                    <div className="value mono">{rewardBreakdown.preview_total_weight}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Share split before the round resolved as orphaned'
                        : 'Matches the My Stats estimate path'}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">{rewardBreakdown.block.orphaned ? 'Resolution' : rewardPayoutWeightLabel}</div>
                    <div className={rewardBreakdown.block.orphaned ? 'value' : 'value mono'}>
                      {rewardBreakdown.block.orphaned ? 'Orphaned' : rewardBreakdown.payout_total_weight}
                    </div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Estimated payout collapsed to zero when the block orphaned'
                        : rewardBreakdownProjected
                          ? 'Current final split if the block reaches payout processing'
                        : 'Final reward split after payout gates'}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Recorded Credits</div>
                    <div className="value">
                      {rewardBreakdown.block.orphaned ? formatCoins(0) : formatCoins(rewardBreakdown.actual_credit_total)}
                    </div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Orphaned blocks resolve to zero credited payout'
                        : rewardBreakdown.actual_credit_events_available
                          ? 'Audit rows available'
                          : 'Not recorded yet'}
                    </div>
                  </div>
                </div>

                <div className="card" style={{ marginBottom: 20 }}>
                  <div style={{ display: 'flex', gap: 12, justifyContent: 'space-between', flexWrap: 'wrap' }}>
                    <div style={{ display: 'grid', gap: 6 }}>
                      <div style={{ fontSize: 14, fontWeight: 700 }}>
                        {rewardBreakdown.payout_scheme.toUpperCase()} reward audit for block {rewardBreakdown.block.height}
                      </div>
                      <div style={{ fontSize: 13, color: 'var(--muted)' }}>
                        {rewardAuditIntro}
                      </div>
                    </div>
                    <div style={{ display: 'grid', gap: 6, justifyItems: 'start' }}>
                      <span
                        className={
                          rewardBreakdown.block.orphaned
                            ? 'badge badge-orphaned'
                            : rewardBreakdown.block.confirmed
                              ? 'badge badge-confirmed'
                              : 'badge badge-pending'
                        }
                      >
                        {rewardBreakdown.block.orphaned
                          ? 'orphaned'
                          : rewardBreakdown.block.confirmed
                            ? 'confirmed'
                            : 'unconfirmed'}
                      </span>
                      <span style={{ fontSize: 12, color: 'var(--muted)' }}>
                        Window end {new Date(toUnixMs(rewardBreakdown.share_window.end)).toLocaleString()}
                      </span>
                    </div>
                  </div>
                </div>

                {rewardAuditNotice ? (
                  <div
                    className="card"
                    style={{
                      marginBottom: 20,
                      ...rewardAuditNoticeStyles,
                    }}
                  >
                    <div style={{ color: 'var(--text)', fontSize: 13 }}>{rewardAuditNotice}</div>
                  </div>
                ) : null}

                <div className="card table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Address</th>
                        <th>{rewardPreviewColumnLabel}</th>
                        {rewardBreakdown.block.orphaned ? <th>Actual</th> : <th>Preview Weight</th>}
                        {rewardBreakdown.block.orphaned ? <th>Delta</th> : null}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardPayoutColumnLabel}</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>Actual</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>Delta</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardPayoutWeightLabel}</th>}
                        <th>Verified Diff</th>
                        <th>Eligible Prov Diff</th>
                        <th>{rewardStatusColumnLabel}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {!filteredRewardParticipants.length ? (
                        <tr>
                          <td colSpan={rewardBreakdown.block.orphaned ? 7 : 10} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                            No participants match the current filter.
                          </td>
                        </tr>
                      ) : (
                        filteredRewardParticipants.map((row) => (
                          <tr key={row.address}>
                            <td title={row.address}>
                              <a
                                href="/stats"
                                onClick={(e) => {
                                  e.preventDefault();
                                  onJumpToStats(row.address);
                                }}
                              >
                                {shortAddr(row.address)}
                              </a>
                              {row.finder ? (
                                <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>finder</div>
                              ) : null}
                            </td>
                            <td>
                              <div>{formatCoins(row.preview_credit)}</div>
                              <div className="mono" style={{ fontSize: 12, color: 'var(--muted)' }}>
                                {row.preview_weight} · {row.preview_share_pct.toFixed(3)}%
                              </div>
                            </td>
                            {rewardBreakdown.block.orphaned ? <td>{formatCoins(0)}</td> : <td className="mono">{row.preview_weight}</td>}
                            {rewardBreakdown.block.orphaned ? (
                              <td
                                style={{
                                  color: row.preview_credit === 0 ? 'var(--muted)' : 'var(--bad)',
                                }}
                              >
                                {formatSignedCoins(-row.preview_credit)}
                              </td>
                            ) : null}
                            {rewardBreakdown.block.orphaned ? null : (
                              <td>
                                <div>{formatCoins(row.payout_credit)}</div>
                                <div className="mono" style={{ fontSize: 12, color: 'var(--muted)' }}>
                                  {row.payout_weight} · {row.payout_share_pct.toFixed(3)}%
                                </div>
                              </td>
                            )}
                            {rewardBreakdown.block.orphaned ? null : (
                              <td>{row.actual_credit != null ? formatCoins(row.actual_credit) : '-'}</td>
                            )}
                            {rewardBreakdown.block.orphaned ? null : (
                              <td
                                style={{
                                  color:
                                    row.delta_vs_payout == null
                                      ? 'var(--muted)'
                                      : row.delta_vs_payout === 0
                                        ? 'var(--good)'
                                        : 'var(--warn)',
                                }}
                              >
                                {formatSignedCoins(row.delta_vs_payout)}
                              </td>
                            )}
                            {rewardBreakdown.block.orphaned ? null : <td className="mono">{row.payout_weight}</td>}
                            <td className="mono">
                              {row.verified_difficulty}
                              <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                {row.verified_shares} shares
                              </div>
                            </td>
                            <td className="mono">
                              {row.provisional_difficulty_eligible}
                              <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                {row.provisional_shares_eligible} eligible
                              </div>
                            </td>
                            <td>
                              {rewardBreakdown.block.orphaned ? (
                                <>
                                  <div style={{ color: 'var(--bad)', fontWeight: 600 }}>
                                    Orphaned
                                  </div>
                                  <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                    Preview: {rewardStatusLabel(row.preview_status)}
                                    {row.risky ? ' · risky' : ''}
                                  </div>
                                </>
                              ) : (
                                <>
                                  <div style={{ color: rewardStatusTone(row.payout_status), fontWeight: 600 }}>
                                    {rewardStatusLabel(row.payout_status)}
                                  </div>
                                  <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                    Preview: {rewardStatusLabel(row.preview_status)}
                                    {row.risky ? ' · risky' : ''}
                                  </div>
                                </>
                              )}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                    {!rewardBreakdown.block.orphaned && rewardBreakdownTotals ? (
                      <tfoot>
                        <tr style={{ background: 'var(--surface-hover)' }}>
                          <td style={{ fontWeight: 700, textAlign: 'left' }}>
                            Pool Fee
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>not share-weighted</div>
                          </td>
                          <td>
                            <div>{formatCoins(rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              withheld from reward
                            </div>
                          </td>
                          <td className="mono">-</td>
                          <td>
                            <div>{formatCoins(rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              {pct(
                                rewardBreakdown.block.reward > 0
                                  ? (rewardBreakdown.fee_amount * 100) / rewardBreakdown.block.reward
                                  : 0
                              )}
                            </div>
                          </td>
                          <td>{rewardBreakdown.actual_fee_amount != null ? formatCoins(rewardBreakdown.actual_fee_amount) : '-'}</td>
                          <td
                            style={{
                              color:
                                rewardFeeDelta == null
                                  ? 'var(--muted)'
                                  : rewardFeeDelta === 0
                                    ? 'var(--good)'
                                    : 'var(--warn)',
                            }}
                          >
                            {formatSignedCoins(rewardFeeDelta)}
                          </td>
                          <td className="mono">-</td>
                          <td className="mono">-</td>
                          <td className="mono">-</td>
                          <td>
                            <div style={{ color: 'var(--muted)', fontWeight: 600 }}>Pool fee</div>
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              {rewardBreakdown.actual_fee_amount != null
                                ? 'tracked separately'
                                : rewardBreakdown.block.paid_out
                                  ? 'fee row missing'
                                  : 'pending'}
                            </div>
                          </td>
                        </tr>
                        <tr style={{ background: 'var(--surface-hover)' }}>
                          <td style={{ fontWeight: 700, textAlign: 'left' }}>
                            Block Total
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              all participants + fee
                            </div>
                          </td>
                          <td>
                            <div>{formatCoins(rewardBreakdownTotals.previewCredit + rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              target {formatCoins(rewardBreakdown.block.reward)}
                            </div>
                          </td>
                          <td className="mono">{rewardBreakdown.preview_total_weight}</td>
                          <td>
                            <div>{formatCoins(rewardBreakdownTotals.payoutCredit + rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              target {formatCoins(rewardBreakdown.block.reward)}
                            </div>
                          </td>
                          <td>{rewardActualBlockTotal != null ? formatCoins(rewardActualBlockTotal) : '-'}</td>
                          <td
                            style={{
                              color:
                                rewardActualBlockDelta == null
                                  ? 'var(--muted)'
                                  : rewardActualBlockDelta === 0
                                    ? 'var(--good)'
                                    : 'var(--warn)',
                            }}
                          >
                            {formatSignedCoins(rewardActualBlockDelta)}
                          </td>
                          <td className="mono">{rewardBreakdown.payout_total_weight}</td>
                          <td className="mono">{rewardBreakdownTotals.verifiedDifficulty}</td>
                          <td className="mono">{rewardBreakdownTotals.provisionalEligibleDifficulty}</td>
                          <td>
                            <div
                              style={{
                                color:
                                  rewardActualBlockDelta == null
                                    ? 'var(--muted)'
                                    : rewardActualBlockDelta === 0
                                      ? 'var(--good)'
                                      : 'var(--warn)',
                                fontWeight: 600,
                              }}
                            >
                              {rewardActualBlockDelta == null ? 'Pending' : rewardActualBlockDelta === 0 ? 'Balanced' : 'Mismatch'}
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              summary across the full block
                            </div>
                          </td>
                        </tr>
                      </tfoot>
                    ) : null}
                  </table>
                  {rewardAddressFilter.trim() && !rewardBreakdown.block.orphaned ? (
                    <div style={{ marginTop: 12, fontSize: 12, color: 'var(--muted)' }}>
                      Summary rows include all participants for the block, not just the filtered addresses.
                    </div>
                  ) : null}
                </div>
              </>
            )}
          </div>

          <div style={{ display: tab === 'health' ? '' : 'none' }}>
            <div className="stats-card-group">
              <div className="stats-card-group-title">Runtime</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Uptime</div>
                  <div className="value mono">{health ? fmtSeconds(health.uptime_seconds || 0) : '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">API Key</div>
                  <div className="value">
                    {health == null ? (
                      '-'
                    ) : health.api_key_configured ? (
                      <>
                        <span className="status-dot dot-green" />Configured
                      </>
                    ) : (
                      <>
                        <span className="status-dot dot-amber" />Missing
                      </>
                    )}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Daemon Reachability</div>
                  <div className="value">
                    {health == null ? (
                      '-'
                    ) : health.daemon?.reachable ? (
                      <>
                        <span className="status-dot dot-green" />OK
                      </>
                    ) : (
                      <>
                        <span className="status-dot dot-red" />Down
                      </>
                    )}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Sync State</div>
                  <div className="value">
                    {health?.daemon?.syncing == null ? '-' : health.daemon.syncing ? 'Syncing' : 'Ready'}
                  </div>
                </div>
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">Daemon</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Chain Height</div>
                  <div className="value mono">{health?.daemon?.chain_height ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Peers</div>
                  <div className="value mono">{health?.daemon?.peers ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Mempool Size</div>
                  <div className="value mono">{health?.daemon?.mempool_size ?? '-'}</div>
                </div>
                {health?.daemon?.error ? (
                  <div className="stat-card">
                    <div className="label">Error</div>
                    <div className="value" style={{ fontSize: 14 }}>{health.daemon.error}</div>
                  </div>
                ) : null}
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">Job Template</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Current Height</div>
                  <div className="value mono">{health?.job?.current_height ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Current Difficulty</div>
                  <div className="value mono">{health?.job?.current_difficulty ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Refresh Lag</div>
                  <div className="value mono">{formatRefreshLag(health?.job?.last_refresh_millis)}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Template Age</div>
                  <div className="value mono">
                    {health?.job?.template_age_seconds != null ? fmtSeconds(health.job.template_age_seconds) : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Tracked Templates</div>
                  <div className="value mono">{health?.job?.tracked_templates ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Active Assignments</div>
                  <div className="value mono">{health?.job?.active_assignments ?? '-'}</div>
                </div>
                <div className="stat-card" title={health?.job?.template_id ?? undefined}>
                  <div className="label">Template ID</div>
                  <div className="value mono">{compactHash(health?.job?.template_id)}</div>
                </div>
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">
                Payouts
                {health?.payouts?.last_payout ? (
                  <span style={{ fontWeight: 400, textTransform: 'none', letterSpacing: 0, marginLeft: 8, fontSize: 11, color: 'var(--muted)' }}>
                    Last: {timeAgo(health.payouts.last_payout.timestamp)}
                    {' \u2022 '}
                    <span className="mono">{shortTx(health.payouts.last_payout.tx_hash)}</span>
                    {' \u2022 '}
                    {formatCoins(health.payouts.last_payout.amount)}
                  </span>
                ) : null}
              </div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Pending Payouts</div>
                  <div className="value mono">{health?.payouts?.pending_count ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Pending Amount</div>
                  <div className="value mono">
                    {health?.payouts?.pending_amount != null
                      ? formatCoins(health.payouts.pending_amount)
                      : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Last Payout</div>
                  <div className="value mono">
                    {health?.payouts?.last_payout?.timestamp ? timeAgo(health.payouts.last_payout.timestamp) : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Wallet Pending</div>
                  <div className="value mono">
                    {health?.wallet?.pending != null
                      ? formatCoins(health.wallet.pending)
                      : '-'}
                  </div>
                </div>
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">Wallet</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Wallet Spendable</div>
                  <div className="value mono">
                    {health?.wallet?.spendable != null
                      ? formatCoins(health.wallet.spendable)
                      : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Unconfirmed Change</div>
                  <div className="value mono">
                    {health?.wallet?.pending_unconfirmed != null
                      ? formatCoins(health.wallet.pending_unconfirmed)
                      : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Change ETA</div>
                  <div className="value mono">
                    {health?.wallet?.pending_unconfirmed_eta != null
                      ? fmtSeconds(health.wallet.pending_unconfirmed_eta)
                      : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Wallet Total</div>
                  <div className="value mono">
                    {health?.wallet?.total != null
                      ? formatCoins(health.wallet.total)
                      : '-'}
                  </div>
                </div>
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">Validation</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">In Flight</div>
                  <div className="value mono">{health?.validation?.in_flight ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Candidate Queue</div>
                  <div className="value mono">{health?.validation?.candidate_queue_depth ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Regular Queue</div>
                  <div className="value mono">{health?.validation?.regular_queue_depth ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Tracked Addresses</div>
                  <div className="value mono">{health?.validation?.tracked_addresses ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Forced Verify</div>
                  <div className="value mono">{health?.validation?.forced_verify_addresses ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Pending Provisional</div>
                  <div className="value mono">{health?.validation?.pending_provisional ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Sampled Shares</div>
                  <div className="value mono">{health?.validation?.sampled_shares ?? '-'}</div>
                  <div className="stat-meta">of {health?.validation?.total_shares ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Invalid Samples</div>
                  <div className="value mono">{health?.validation?.invalid_samples ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Fraud Detections</div>
                  <div className="value mono">{health?.validation?.fraud_detections ?? '-'}</div>
                </div>
              </div>
            </div>

            <div className="card section">
              <div className="section-header">
                <div>
                  <h3>Raw Health Data</h3>
                  <p className="section-lead">
                    Keep the full protected health payload available for copy and low-level debugging without making it
                    the primary UI.
                  </p>
                </div>
                <button className="btn btn-secondary" onClick={copyHealthJson} disabled={!rawHealthJson}>
                  Copy JSON
                </button>
              </div>
              <details className="health-raw-toggle">
                <summary>Show raw JSON</summary>
                <pre className="raw-json">{rawHealthJson || 'Loading...'}</pre>
              </details>
            </div>
          </div>

          <div style={{ display: tab === 'balances' ? '' : 'none' }}>
            <div className="filter-bar">
              <input
                type="text"
                placeholder="Search address..."
                value={balancesSearch}
                onChange={(e) => setBalancesSearch(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    setBalancesPager((p) => ({ ...p, offset: 0 }));
                  }
                }}
              />
              <select value={balancesSort} onChange={(e) => { setBalancesSort(e.target.value); setBalancesPager((p) => ({ ...p, offset: 0 })); }}>
                <option value="pending_desc">Owed (high first)</option>
                <option value="pending_asc">Owed (low first)</option>
                <option value="paid_desc">Paid (high first)</option>
                <option value="paid_asc">Paid (low first)</option>
                <option value="address_asc">Address A-Z</option>
                <option value="address_desc">Address Z-A</option>
              </select>
              <button className="btn btn-primary" onClick={() => setBalancesPager((p) => ({ ...p, offset: 0 }))}>
                Search
              </button>
            </div>

            <div className="card table-scroll">
              <table className="admin-balance-table">
                <colgroup>
                  <col className="admin-balance-table__address-col" />
                  <col className="admin-balance-table__amount-col" />
                  <col className="admin-balance-table__amount-col" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>Owed</th>
                    <th>Total Paid</th>
                  </tr>
                </thead>
                <tbody>
                  {!balancesItems.length ? (
                    <tr>
                      <td colSpan={3} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No balances
                      </td>
                    </tr>
                  ) : (
                    balancesItems.map((b) => (
                      <tr key={b.address}>
                        <td title={b.address}>
                          <a
                            href="/stats"
                            onClick={(e) => {
                              e.preventDefault();
                              onJumpToStats(b.address);
                            }}
                          >
                            {shortAddr(b.address)}
                          </a>
                        </td>
                        <td className="mono">
                          {b.pending > 0 ? (
                            <span style={{ color: 'var(--warn)' }}>{formatCoins(b.pending)}</span>
                          ) : (
                            formatCoins(b.pending)
                          )}
                        </td>
                        <td className="mono">{formatCoins(b.paid)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              <Pager
                offset={balancesPager.offset}
                limit={balancesPager.limit}
                total={balancesPager.total}
                onPrev={() => setBalancesPager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
                onNext={() => setBalancesPager((p) => ({ ...p, offset: p.offset + p.limit }))}
              />
            </div>
          </div>

          <div style={{ display: tab === 'logs' ? '' : 'none' }}>
            <div className="filter-bar">
              <span style={{ fontSize: 13, color: 'var(--muted)' }}>
                <span className={`status-dot ${daemonLogsStatusDot}`} />
                {daemonLogsStatusText}
              </span>

              <select value={String(daemonLogsTail)} onChange={(e) => setDaemonLogsTail(Number(e.target.value) || 200)}>
                <option value="100">Tail 100</option>
                <option value="200">Tail 200</option>
                <option value="500">Tail 500</option>
                <option value="1000">Tail 1000</option>
              </select>

              <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: 'var(--muted)' }}>
                <input
                  type="checkbox"
                  checked={daemonLogsAutoScroll}
                  onChange={(e) => setDaemonLogsAutoScroll(e.target.checked)}
                />
                Auto-scroll
              </label>

              <button
                className="btn btn-secondary"
                onClick={() => setDaemonLogsConnectSeq((seq) => seq + 1)}
              >
                Reconnect
              </button>
              <button className="btn btn-secondary" onClick={() => setDaemonLogs([])}>
                Clear
              </button>
            </div>

            {daemonLogsError ? (
              <p style={{ fontSize: 13, color: 'var(--bad)', marginBottom: 10 }}>{daemonLogsError}</p>
            ) : null}

            <div ref={daemonLogsRef} className="log-stream">
              {daemonLogs.length ? (
                daemonLogs.map((line) => (
                  <div key={line.id} className="log-line">
                    {line.segments.map((segment, index) => (
                      <span key={index} className="log-segment" style={segment.style}>
                        {segment.text}
                      </span>
                    ))}
                  </div>
                ))
              ) : (
                <div className="log-line log-line-placeholder">No daemon log lines yet.</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
