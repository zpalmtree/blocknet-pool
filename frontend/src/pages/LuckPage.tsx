import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
import { fmtSeconds, timeAgo, toUnixMs } from '../lib/format';
import type { LuckRound, PagerState } from '../types';

interface LuckPageProps {
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

export function LuckPage({ active, api, liveTick }: LuckPageProps) {
  const [items, setItems] = useState<LuckRound[]>([]);
  const [pager, setPager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });

  const loadPage = useCallback(async () => {
    try {
      const d = await api.getLuckHistory({ limit: pager.limit, offset: pager.offset });
      const nextItems = d.items || [];
      setItems(nextItems);
      setPager((prev) => ({ ...prev, total: d.page ? d.page.total : nextItems.length }));
    } catch {
      setItems([]);
    }
  }, [api, pager.limit, pager.offset]);

  useEffect(() => {
    if (!active) return;
    void loadPage();
  }, [active, loadPage]);

  useEffect(() => {
    if (!active || liveTick <= 0 || liveTick % 12 !== 0) return;
    void loadPage();
  }, [active, liveTick, loadPage]);

  return (
    <div className={active ? 'page active' : 'page'} id="page-luck">
      <div className="page-header">
        <span className="page-kicker">Round History</span>
        <h1>Blocknet pool luck history</h1>
        <p className="page-intro">
          Compare round effort and duration over time to understand how actual block discovery compares with expected
          pool luck.
        </p>
      </div>
      <div className="card table-scroll">
        <table>
          <thead>
            <tr>
              <th>Block</th>
              <th>Effort</th>
              <th>Round Time</th>
              <th>Status</th>
              <th>Found</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (
              <tr>
                <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                  No round history yet
                </td>
              </tr>
            ) : (
              items.map((row) => (
                <tr key={`${row.block_height}-${row.block_hash}`}>
                  <td>
                    <a href={`https://explorer.blocknetcrypto.com/block/${row.block_hash}`} target="_blank" rel="noopener">
                      {row.block_height}
                    </a>
                  </td>
                  <td>
                    <span className={`round-chip ${toneClass(row.effort_band?.tone)}`}>{fmtPct(row.effort_pct)}</span>
                  </td>
                  <td>{fmtSeconds(row.duration_seconds)}</td>
                  <td>
                    {row.orphaned ? (
                      <span className="badge badge-orphaned">orphaned</span>
                    ) : row.confirmed ? (
                      <span className="badge badge-confirmed">confirmed</span>
                    ) : (
                      <span className="badge badge-pending">pending</span>
                    )}
                  </td>
                  <td title={new Date(toUnixMs(row.timestamp)).toLocaleString()}>{timeAgo(row.timestamp)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        <Pager
          offset={pager.offset}
          limit={pager.limit}
          total={pager.total}
          onPrev={() => setPager((prev) => ({ ...prev, offset: Math.max(0, prev.offset - prev.limit) }))}
          onNext={() => setPager((prev) => ({ ...prev, offset: prev.offset + prev.limit }))}
        />
      </div>

      <div className="seo-copy-grid">
        <div className="card seo-copy-card">
          <h3>Round Effort Matters</h3>
          <p>Luck compares the work spent in a round with the work that was statistically expected before a block was found.</p>
        </div>
        <div className="card seo-copy-card">
          <h3>Variance Is Normal</h3>
          <p>Rounds above 100% effort happen naturally. Compare recent rounds together instead of overreacting to one slow block.</p>
        </div>
        <div className="card seo-copy-card">
          <h3>Use It With Block History</h3>
          <p>
            Pair this luck view with <a href="/blocks">recent blocks</a> to see how round variance translates into
            confirmed and orphaned blocks.
          </p>
        </div>
      </div>
    </div>
  );
}
