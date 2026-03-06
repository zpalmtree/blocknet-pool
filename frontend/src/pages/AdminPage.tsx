import { useCallback, useEffect, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
import { fmtSeconds, formatCoins, formatFee, humanRate, shortAddr, shortTx, timeAgo, toUnixMs } from '../lib/format';
import type { AdminPayoutItem, AdminTab, FeeEvent, HealthResponse, MinerListItem, PagerState } from '../types';

const MAX_DAEMON_LOG_LINES = 1000;

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
  const [feeItems, setFeeItems] = useState<FeeEvent[]>([]);
  const [feePager, setFeePager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });

  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [daemonLogs, setDaemonLogs] = useState<string[]>([]);
  const [daemonLogsTail, setDaemonLogsTail] = useState(200);
  const [daemonLogsStatus, setDaemonLogsStatus] = useState<'idle' | 'connecting' | 'live' | 'error'>('idle');
  const [daemonLogsError, setDaemonLogsError] = useState('');
  const [daemonLogsAutoScroll, setDaemonLogsAutoScroll] = useState(true);
  const [daemonLogsConnectSeq, setDaemonLogsConnectSeq] = useState(0);
  const daemonLogsRef = useRef<HTMLPreElement | null>(null);

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
      const items = d.recent?.items || [];
      setFeeItems(items);
      setFeePager((prev) => ({ ...prev, total: d.recent?.page ? d.recent.page.total : items.length }));
    } catch {
      setFeeItems([]);
    }
  }, [api, apiKey, feePager.limit, feePager.offset]);

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
    if (tab === 'health') void loadHealth();
  }, [active, apiKey, loadFees, loadHealth, loadMiners, loadPayouts, tab]);

  useEffect(() => {
    if (!active || !apiKey || liveTick <= 0) return;
    if (liveTick % 2 !== 0) return;

    if (tab === 'miners') void loadMiners();
    if (tab === 'payouts') void loadPayouts();
    if (tab === 'fees') void loadFees();
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
  ]);

  useEffect(() => {
    if (!active || !apiKey || tab !== 'logs') return;

    const controller = new AbortController();
    let seenLine = false;
    setDaemonLogsStatus('connecting');
    setDaemonLogsError('');

    const connect = async () => {
      try {
        await api.streamDaemonLogs({
          tail: daemonLogsTail,
          signal: controller.signal,
          onLine: (line) => {
            seenLine = true;
            setDaemonLogsStatus('live');
            setDaemonLogs((prev) => {
              const next = prev.concat(line);
              if (next.length <= MAX_DAEMON_LOG_LINES) {
                return next;
              }
              return next.slice(next.length - MAX_DAEMON_LOG_LINES);
            });
          },
        });
        if (controller.signal.aborted) return;
        if (!seenLine) {
          setDaemonLogsStatus('idle');
        }
      } catch (err) {
        if (controller.signal.aborted) return;
        if (err instanceof DOMException && err.name === 'AbortError') return;
        setDaemonLogsStatus('error');
        setDaemonLogsError(err instanceof Error ? err.message : 'failed to stream daemon logs');
      }
    };

    void connect();
    return () => {
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
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!payoutItems.length ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No payouts
                      </td>
                    </tr>
                  ) : (
                    payoutItems.map((p, idx) => (
                      <tr key={`${p.tx_hash}-${idx}`}>
                        <td title={p.address}>{shortAddr(p.address)}</td>
                        <td>{formatCoins(p.amount)}</td>
                        <td>{formatFee(p.fee)}</td>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/tx/${p.tx_hash}`} target="_blank" rel="noopener" title={p.tx_hash}>
                            {shortTx(p.tx_hash)}
                          </a>
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
            Total Collected: <span className="mono">{formatCoins(feesTotal)}</span>
          </p>

            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Block</th>
                    <th>Amount</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!feeItems.length ? (
                    <tr>
                      <td colSpan={3} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No fee events
                      </td>
                    </tr>
                  ) : (
                    feeItems.map((f, idx) => (
                      <tr key={`${f.block_height}-${idx}`}>
                        <td>{f.block_height}</td>
                        <td>{formatCoins(f.amount)}</td>
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

          <div style={{ display: tab === 'health' ? '' : 'none' }}>
            <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(4,1fr)', marginBottom: 20 }}>
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

            <pre ref={daemonLogsRef} className="log-stream">
              {daemonLogs.length ? daemonLogs.join('\n') : 'No daemon log lines yet.'}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
