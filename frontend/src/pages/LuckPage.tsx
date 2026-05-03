import { useCallback } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { Pager } from '../components/Pager';
import { fmtSeconds, formatPct, roundToneClass, timeAgo, toUnixMs } from '../lib/format';
import { usePagedData } from '../lib/paging';
import type { LuckRound } from '../types';

interface LuckPageProps {
  api: ApiClient;
  liveTick: number;
}

export function LuckPage({ api, liveTick }: LuckPageProps) {
  const fetchLuck = useCallback((limit: number, offset: number) => api.getLuckHistory(limit, offset), [api]);
  const { items, pagerProps } = usePagedData<LuckRound>(liveTick, fetchLuck);

  return (
    <div id="page-luck">
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
              <EmptyTableRow colSpan={5}>No round history yet</EmptyTableRow>
            ) : (
              items.map((row) => (
                <tr key={`${row.block_height}-${row.block_hash}`}>
                  <td>
                    <a href={`https://explorer.blocknetcrypto.com/block/${row.block_hash}`} target="_blank" rel="noopener">
                      {row.block_height}
                    </a>
                  </td>
                  <td>
                    <span className={`round-chip ${roundToneClass(row.effort_pct)}`}>{formatPct(row.effort_pct)}</span>
                  </td>
                  <td>{fmtSeconds(row.duration_seconds)}</td>
                  <td>
                    <BlockStatusBadge confirmed={row.confirmed} orphaned={row.orphaned} />
                  </td>
                  <td title={new Date(toUnixMs(row.timestamp)).toLocaleString()}>{timeAgo(row.timestamp)}</td>
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
