import { useCallback, useEffect, useMemo, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { HashrateChart } from '../components/HashrateChart';
import { LAST_MINER_LOOKUP_KEY } from '../lib/storage';
import { formatCoins, humanRate, timeAgo, toUnixMs } from '../lib/format';
import type { HashratePoint, MinerResponse, Range } from '../types';

interface StatsPageProps {
  active: boolean;
  api: ApiClient;
}

export function StatsPage({ active, api }: StatsPageProps) {
  const [minerInput, setMinerInput] = useState(localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '');
  const [minerAddress, setMinerAddress] = useState('');
  const [minerData, setMinerData] = useState<MinerResponse | null>(null);
  const [range, setRange] = useState<Range>('1h');
  const [history, setHistory] = useState<HashratePoint[]>([]);

  const loadMinerLookup = useCallback(
    async (input?: string) => {
      const addr = (input ?? minerInput).trim();
      if (!addr) return;
      try {
        const d = await api.getMiner(addr);
        setMinerAddress(addr);
        setMinerInput(addr);
        localStorage.setItem(LAST_MINER_LOOKUP_KEY, addr);
        setMinerData(d);
      } catch {
        // handled by api client
      }
    },
    [api, minerInput]
  );

  const loadMinerHashrate = useCallback(async () => {
    if (!minerAddress) return;
    try {
      const d = await api.getMinerHashrate(minerAddress, range);
      setHistory(d || []);
    } catch {
      setHistory([]);
    }
  }, [api, minerAddress, range]);

  useEffect(() => {
    if (!active) return;

    const stored = localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '';
    if (stored && stored !== minerInput) {
      setMinerInput(stored);
    }

    if (!minerAddress && stored) {
      void loadMinerLookup(stored);
      return;
    }

    if (!minerAddress && minerInput.trim()) {
      void loadMinerLookup(minerInput);
    }
  }, [active, loadMinerLookup, minerAddress, minerInput]);

  useEffect(() => {
    if (!active || !minerAddress) return;
    void loadMinerHashrate();
  }, [active, loadMinerHashrate, minerAddress, range]);

  const lookupDisabled = useMemo(() => {
    const addr = minerInput.trim();
    const sameAsLoaded = !!minerAddress && addr === minerAddress;
    return !addr || sameAsLoaded;
  }, [minerAddress, minerInput]);

  const minerAvgDiff = useMemo(() => {
    const shares = minerData?.shares || [];
    const totalAccepted = minerData?.total_accepted || 0;
    if (!shares.length || totalAccepted <= 0) return '-';
    const take = Math.min(shares.length, totalAccepted);
    const sum = shares.slice(0, take).reduce((acc, s) => acc + (s.difficulty || 0), 0);
    if (!sum) return '-';
    return (sum / take).toFixed(0);
  }, [minerData]);

  const minerOldestShareDate = useMemo(() => {
    const shares = minerData?.shares || [];
    let oldest = 0;
    for (const s of shares) {
      const t = toUnixMs(s.created_at);
      if (!t) continue;
      if (!oldest || t < oldest) oldest = t;
    }
    return oldest ? new Date(oldest).toLocaleDateString() : '-';
  }, [minerData]);

  return (
    <div className={active ? 'page active' : 'page'} id="page-stats">
      <h2>My Stats</h2>
      <div className="card" style={{ marginBottom: 24 }}>
        <div className="lookup-form" style={{ display: 'flex', gap: 10, marginBottom: 0, flexWrap: 'wrap' }}>
          <input
            type="text"
            placeholder="Enter your wallet address"
            style={{ flex: 1, minWidth: 200 }}
            value={minerInput}
            onChange={(e) => setMinerInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !lookupDisabled) {
                void loadMinerLookup();
              }
            }}
          />
          <button
            className={`btn btn-primary ${minerAddress && minerInput.trim() === minerAddress ? 'is-faded' : ''}`}
            disabled={lookupDisabled}
            onClick={() => void loadMinerLookup()}
          >
            Lookup
          </button>
          <button
            className="btn btn-secondary"
            onClick={() => {
              setMinerInput('');
              setMinerAddress('');
              setMinerData(null);
              setHistory([]);
              localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
            }}
          >
            Clear
          </button>
        </div>
      </div>

      {minerData && (
        <div id="lookup-result">
          <div className="stats-grid" style={{ marginBottom: 24 }}>
            <div className="stat-card">
              <div className="label">Hashrate</div>
              <div className="value">{humanRate(minerData.hashrate || 0)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Pending Balance</div>
              <div className="value">{formatCoins(minerData.balance?.pending || 0)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Paid Balance</div>
              <div className="value">{formatCoins(minerData.balance?.paid || 0)}</div>
            </div>
            <div className="stat-card">
              <div className="label">Blocks Found</div>
              <div className="value">{(minerData.blocks_found || []).length}</div>
            </div>
          </div>

          <div className="stats-grid" style={{ marginBottom: 24 }}>
            <div className="stat-card">
              <div className="label">Shares Accepted</div>
              <div className="value">{minerData.total_accepted || 0}</div>
            </div>
            <div className="stat-card">
              <div className="label">Shares Rejected</div>
              <div className="value">{minerData.total_rejected || 0}</div>
            </div>
            <div className="stat-card">
              <div className="label">Avg Difficulty</div>
              <div className="value">{minerAvgDiff}</div>
            </div>
            <div className="stat-card">
              <div className="label">Mining Since</div>
              <div className="value">{minerOldestShareDate}</div>
            </div>
          </div>

          <div className="section">
            <div className="section-header">
              <h2>Hashrate History</h2>
              <div className="range-tabs">
                {(['1h', '24h', '7d', '30d'] as Range[]).map((r) => (
                  <button key={r} className={range === r ? 'active' : ''} onClick={() => setRange(r)}>
                    {r}
                  </button>
                ))}
              </div>
            </div>
            <HashrateChart data={history} range={range} />
          </div>

          <div className="section">
            <h2>Workers</h2>
            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Worker</th>
                    <th>Hashrate</th>
                    <th>Accepted</th>
                    <th>Rejected</th>
                    <th>Last Share</th>
                  </tr>
                </thead>
                <tbody>
                  {!minerData.workers?.length ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No workers
                      </td>
                    </tr>
                  ) : (
                    minerData.workers.map((w) => (
                      <tr key={w.worker}>
                        <td>{w.worker || 'default'}</td>
                        <td>{humanRate(w.hashrate || 0)}</td>
                        <td>{w.accepted || 0}</td>
                        <td>{w.rejected || 0}</td>
                        <td title={w.last_share_at ? new Date(toUnixMs(w.last_share_at)).toLocaleString() : ''}>
                          {timeAgo(w.last_share_at)}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {!!minerData.blocks_found?.length && (
            <div className="section">
              <h2>Blocks Found</h2>
              <div className="card table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Height</th>
                      <th>Reward</th>
                      <th>Status</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {minerData.blocks_found.map((b) => (
                      <tr key={`${b.height}-${b.hash}`}>
                        <td>
                          <a href={`https://explorer.blocknetcrypto.com/block/${b.hash || ''}`} target="_blank" rel="noopener">
                            {b.height}
                          </a>
                        </td>
                        <td>{formatCoins(b.reward)}</td>
                        <td>
                          <BlockStatusBadge confirmed={b.confirmed} orphaned={b.orphaned} />
                        </td>
                        <td title={new Date(toUnixMs(b.timestamp)).toLocaleString()}>{timeAgo(b.timestamp)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          <div className="section">
            <h2>Recent Shares</h2>
            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Job</th>
                    <th>Worker</th>
                    <th>Difficulty</th>
                    <th>Status</th>
                    <th>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {!minerData.shares?.length ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No shares
                      </td>
                    </tr>
                  ) : (
                    minerData.shares.slice(0, 50).map((s, idx) => (
                      <tr key={`${s.job_id}-${idx}`}>
                        <td>{s.job_id || ''}</td>
                        <td>{s.worker || ''}</td>
                        <td>{s.difficulty}</td>
                        <td>{s.status || ''}</td>
                        <td title={new Date(toUnixMs(s.created_at)).toLocaleString()}>{timeAgo(s.created_at)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
