import { useCallback, useState } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { ExplorerLink } from '../components/ExplorerLink';
import { Pager } from '../components/Pager';
import { fmtSeconds, formatCoins, formatPct, roundToneClass, timeAgo, timestampTitle } from '../lib/format';
import { usePagedData } from '../lib/paging';
import type { BlockItem } from '../types';

interface BlocksPageProps {
  api: ApiClient;
  liveTick: number;
}

export function BlocksPage({ api, liveTick }: BlocksPageProps) {
  const [filter, setFilter] = useState('');
  const fetchBlocks = useCallback(
    (limit: number, offset: number) => api.getBlocks(limit, offset, filter || undefined),
    [api, filter]
  );
  const { items, setPager, pagerProps } = usePagedData<BlockItem>(liveTick, fetchBlocks);

  return (
    <div id="page-blocks">
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
              <EmptyTableRow colSpan={6}>No blocks</EmptyTableRow>
            ) : (
              items.map((b) => (
                <tr key={`${b.height}-${b.hash}`}>
                  <td>
                    <ExplorerLink kind="block" value={b.hash}>{b.height}</ExplorerLink>
                  </td>
                  <td>{formatCoins(b.reward)}</td>
                  <td>
                    {b.effort_pct == null ? (
                      '-'
                    ) : (
                      <span className={`round-chip ${roundToneClass(b.effort_pct)}`}>{formatPct(b.effort_pct)}</span>
                    )}
                  </td>
                  <td>{b.duration_seconds == null ? '-' : fmtSeconds(b.duration_seconds)}</td>
                  <td>
                    <BlockStatusBadge confirmed={b.confirmed} orphaned={b.orphaned} />
                  </td>
                  <td title={timestampTitle(b.timestamp)}>{timeAgo(b.timestamp)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
        <Pager {...pagerProps} />
      </div>

    </div>
  );
}
