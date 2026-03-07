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

  const [rewardBlockInput, setRewardBlockInput] = useState('');
  const [rewardBlockOptions, setRewardBlockOptions] = useState<BlockItem[]>([]);
  const [rewardBlockOptionsLoading, setRewardBlockOptionsLoading] = useState(false);
  const [rewardAddressFilter, setRewardAddressFilter] = useState('');
  const [rewardBreakdown, setRewardBreakdown] = useState<BlockRewardBreakdownResponse | null>(null);
  const [rewardBreakdownLoading, setRewardBreakdownLoading] = useState(false);

  const [health, setHealth] = useState<HealthResponse | null>(null);
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

  useEffect(() => {
    if (!active || !apiKey) return;

    if (tab === 'miners') void loadMiners();
    if (tab === 'payouts') void loadPayouts();
    if (tab === 'fees') void loadFees();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'health') void loadHealth();
  }, [
    active,
    apiKey,
    loadFees,
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
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'health') void loadHealth();
  }, [
    active,
    apiKey,
    liveTick,
    tab,
    loadFees,
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
            <button className={tab === 'rewards' ? 'active' : ''} onClick={() => setTab('rewards')}>
              Rewards
            </button>
            <button className={tab === 'health' ? 'active' : ''} onClick={() => setTab('health')}>
              Health
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
                    <div className="label">Reward</div>
                    <div className="value">{formatCoins(rewardBreakdown.block.reward)}</div>
                    <div className="stat-meta">
                      Fee {formatCoins(rewardBreakdown.fee_amount)} · Net {formatCoins(rewardBreakdown.distributable_reward)}
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
                    <div className="stat-meta">Matches the My Stats estimate path</div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Payout Weight</div>
                    <div className="value mono">{rewardBreakdown.payout_total_weight}</div>
                    <div className="stat-meta">Final reward split after payout gates</div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Recorded Credits</div>
                    <div className="value">{formatCoins(rewardBreakdown.actual_credit_total)}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.actual_credit_events_available ? 'Audit rows available' : 'Not recorded yet'}
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
                        Preview shows the unconfirmed estimator used on My Stats. Payout shows the final weighting
                        rules, including verified-share anchors and any provisional cap. Actual shows what the payout
                        processor recorded for this block, when available.
                      </div>
                    </div>
                    <div style={{ display: 'grid', gap: 6, justifyItems: 'start' }}>
                      <span
                        className={
                          rewardBreakdown.block.orphaned
                            ? 'badge badge-pending'
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

                {!rewardBreakdown.actual_credit_events_available ? (
                  <div
                    className="card"
                    style={{
                      marginBottom: 20,
                      background: 'rgba(247, 180, 75, 0.12)',
                      borderColor: 'rgba(247, 180, 75, 0.45)',
                    }}
                  >
                    <div style={{ color: 'var(--text)', fontSize: 13 }}>
                      {rewardBreakdown.block.paid_out
                        ? 'This block was paid before per-block credit audit rows were available, so the Actual column cannot be shown from storage.'
                        : 'This block has not been paid yet, so the Actual column remains empty until the payout processor records credits.'}
                    </div>
                  </div>
                ) : null}

                <div className="card table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Address</th>
                        <th>Preview</th>
                        <th>Payout</th>
                        <th>Actual</th>
                        <th>Delta</th>
                        <th>Preview Weight</th>
                        <th>Payout Weight</th>
                        <th>Verified Diff</th>
                        <th>Eligible Prov Diff</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {!filteredRewardParticipants.length ? (
                        <tr>
                          <td colSpan={10} style={{ textAlign: 'center', color: 'var(--muted)' }}>
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
                            <td>
                              <div>{formatCoins(row.payout_credit)}</div>
                              <div className="mono" style={{ fontSize: 12, color: 'var(--muted)' }}>
                                {row.payout_weight} · {row.payout_share_pct.toFixed(3)}%
                              </div>
                            </td>
                            <td>{row.actual_credit != null ? formatCoins(row.actual_credit) : '-'}</td>
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
                            <td className="mono">{row.preview_weight}</td>
                            <td className="mono">{row.payout_weight}</td>
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
                              <div style={{ color: rewardStatusTone(row.payout_status), fontWeight: 600 }}>
                                {rewardStatusLabel(row.payout_status)}
                              </div>
                              <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                Preview: {rewardStatusLabel(row.preview_status)}
                                {row.risky ? ' · risky' : ''}
                              </div>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>

          <div style={{ display: tab === 'health' ? '' : 'none' }}>
            <div
              className="stats-grid"
              style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', marginBottom: 20 }}
            >
              <div className="stat-card">
                <div className="label">Uptime</div>
                <div className="value">{health ? fmtSeconds(health.uptime_seconds || 0) : '-'}</div>
              </div>
              <div className="stat-card">
                <div className="label">Daemon</div>
                <div className="value">
                  {health?.daemon?.reachable ? (
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
                <div className="label">Template Age</div>
                <div className="value">
                  {health?.job?.template_age_seconds != null ? fmtSeconds(health.job.template_age_seconds) : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Pending Payouts</div>
                <div className="value">{health?.payouts?.pending_count ?? '-'}</div>
              </div>
              <div className="stat-card">
                <div className="label">Wallet Spendable</div>
                <div className="value">
                  {health?.wallet?.spendable != null
                    ? formatCoins(health.wallet.spendable)
                    : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Wallet Pending</div>
                <div className="value">
                  {health?.wallet?.pending != null
                    ? formatCoins(health.wallet.pending)
                    : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Unconfirmed Change</div>
                <div className="value">
                  {health?.wallet?.pending_unconfirmed != null
                    ? formatCoins(health.wallet.pending_unconfirmed)
                    : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Change ETA</div>
                <div className="value">
                  {health?.wallet?.pending_unconfirmed_eta != null
                    ? fmtSeconds(health.wallet.pending_unconfirmed_eta)
                    : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Wallet Total</div>
                <div className="value">
                  {health?.wallet?.total != null
                    ? formatCoins(health.wallet.total)
                    : '-'}
                </div>
              </div>
            </div>

            <h3>Raw Health Data</h3>
            <pre className="raw-json">{health ? JSON.stringify(health, null, 2) : 'Loading...'}</pre>
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
