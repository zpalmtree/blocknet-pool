import { t } from '../i18n';
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
        <span className="page-kicker">{t("m368")}</span>
        <h1>{t("m369")}</h1>
        <p className="page-intro">{t("m370")}</p>
      </div>
      <div className="filter-bar">
        <select value={filter} onChange={(e) => setFilter(e.target.value)}>
          <option value="">{t("m371")}</option>
          <option value="confirmed">{t("m372")}</option>
          <option value="pending">{t("m143")}</option>
          <option value="orphaned">{t("m109")}</option>
        </select>
        <button className="btn btn-primary" onClick={() => setPager((p) => ({ ...p, offset: 0 }))}>{t("m373")}</button>
      </div>
      <div className="card table-scroll">
        <table>
          <thead>
            <tr>
              <th>{t("m374")}</th>
              <th>{t("m100")}</th>
              <th>{t("m375")}</th>
              <th>{t("m376")}</th>
              <th>{t("m006")}</th>
              <th>{t("m377")}</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (<EmptyTableRow colSpan={6}>{t("m378")}</EmptyTableRow>) : (items.map((b) => (
                <tr key={`${b.height}-${b.hash}`}>
                  <td>
                    <ExplorerLink kind="block" value={b.hash}>{b.height}</ExplorerLink>
                  </td>
                  <td>{formatCoins(b.reward)}</td>
                  <td>
                    {b.effort_pct == null ? ('-') : (<span className={`round-chip ${roundToneClass(b.effort_pct)}`}>{formatPct(b.effort_pct)}</span>)}
                  </td>
                  <td>{b.duration_seconds == null ? '-' : fmtSeconds(b.duration_seconds)}</td>
                  <td>
                    <BlockStatusBadge confirmed={b.confirmed} orphaned={b.orphaned} />
                  </td>
                  <td title={timestampTitle(b.timestamp)}>{timeAgo(b.timestamp)}</td>
                </tr>
              )))}
          </tbody>
        </table>
        <Pager {...pagerProps} />
      </div>

    </div>
  );
}
