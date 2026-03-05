import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { Pager } from '../components/Pager';
import { formatCoins, timeAgo, toUnixMs } from '../lib/format';
import type { BlockItem, PagerState } from '../types';

interface BlocksPageProps {
  active: boolean;
  api: ApiClient;
}

export function BlocksPage({ active, api }: BlocksPageProps) {
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

  return (
    <div className={active ? 'page active' : 'page'} id="page-blocks">
      <h2>Blocks Found</h2>
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
              <th>Status</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (
              <tr>
                <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
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
    </div>
  );
}
