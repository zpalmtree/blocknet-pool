import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { Pager } from '../components/Pager';
import { fmtSeconds, formatCoins, timeAgo, toUnixMs } from '../lib/format';
import type { BlockItem, PagerState } from '../types';

interface BlocksPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
}

function fmtPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(1)}%`;
}

function toneClass(tone: string | undefined): string {
  if (tone === 'critical') return 'is-critical';
  if (tone === 'warn') return 'is-warn';
  return 'is-ok';
}

export function BlocksPage({ active, api, liveTick }: BlocksPageProps) {
  const [filter, setFilter] = useState('');
  const [items, setItems] = useState<BlockItem[]>([]);
  const [pager, setPager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });

  const loadPage = useCallback(async () => {
    try {
      const d = await api.getBlocks({
        paged: 'true',
        limit: pager.limit,
        offset: pager.offset,
        sort: 'height_desc',
        status: filter || undefined,
      });
      const nextItems = d.items || [];
      setItems(nextItems);
      setPager((prev) => ({ ...prev, total: d.page ? d.page.total : nextItems.length }));
    } catch {
      setItems([]);
    }
  }, [api, filter, pager.limit, pager.offset]);

  useEffect(() => {
    if (!active) return;
    void loadPage();
  }, [active, loadPage]);

  useEffect(() => {
    if (!active || liveTick <= 0 || liveTick % 12 !== 0) return;
    void loadPage();
  }, [active, liveTick, loadPage]);

  return (
    <div className={active ? 'page active' : 'page'} id="page-blocks">
      <div className="page-header">
        <span className="page-kicker">Block Discovery</span>
        <h1>Recently found Blocknet blocks</h1>
        <p className="page-intro">
          Browse confirmed, pending, and orphaned pool blocks with reward, round effort, and elapsed round time for
          each Blocknet block.
        </p>
      </div>
      <div className="filter-bar">
        <select value={filter} onChange={(e) => setFilter(e.target.value)}>
          <option value="">All Status</option>
          <option value="confirmed">Confirmed</option>
          <option value="pending">Pending</option>
          <option value="orphaned">Orphaned</option>
        </select>
        <button className="btn btn-primary" onClick={() => setPager((p) => ({ ...p, offset: 0 }))}>
          Filter
        </button>
      </div>
      <div className="card table-scroll">
        <table>
          <thead>
            <tr>
              <th>Height</th>
              <th>Reward</th>
              <th>Effort</th>
              <th>Round Time</th>
              <th>Status</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (
              <tr>
                <td colSpan={6} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                  No blocks
                </td>
              </tr>
            ) : (
              items.map((b) => (
                <tr key={`${b.height}-${b.hash}`}>
                  <td>
                    <a href={`https://explorer.blocknetcrypto.com/block/${b.hash}`} target="_blank" rel="noopener">
                      {b.height}
                    </a>
                  </td>
                  <td>{formatCoins(b.reward)}</td>
                  <td>
                    {b.effort_pct == null ? (
                      '-'
                    ) : (
                      <span className={`round-chip ${toneClass(b.effort_band?.tone)}`}>{fmtPct(b.effort_pct)}</span>
                    )}
                  </td>
                  <td>{b.duration_seconds == null ? '-' : fmtSeconds(b.duration_seconds)}</td>
                  <td>
                    <BlockStatusBadge confirmed={b.confirmed} orphaned={b.orphaned} />
                  </td>
                  <td title={new Date(toUnixMs(b.timestamp)).toLocaleString()}>{timeAgo(b.timestamp)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        <Pager
          offset={pager.offset}
          limit={pager.limit}
          total={pager.total}
          onPrev={() => setPager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
          onNext={() => setPager((p) => ({ ...p, offset: p.offset + p.limit }))}
        />
      </div>

      <div className="seo-copy-grid">
        <div className="card seo-copy-card">
          <h3>Confirmed Rounds</h3>
          <p>Confirmed blocks cleared the pool confirmation window and are eligible for payout processing.</p>
        </div>
        <div className="card seo-copy-card">
          <h3>Pending Rounds</h3>
          <p>Pending blocks are fresh finds still moving toward payout eligibility while the chain confirmation count rises.</p>
        </div>
        <div className="card seo-copy-card">
          <h3>Orphan Diagnostics</h3>
          <p>
            Orphaned rounds are still part of the pool story because they show how often shares landed on losing
            branches.
          </p>
        </div>
      </div>
    </div>
  );
}
