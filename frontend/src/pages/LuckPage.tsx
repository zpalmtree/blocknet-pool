import { t } from '../i18n';
import { useCallback } from 'react';

import type { ApiClient } from '../api/client';
import { BlockStatusBadge } from '../components/BlockStatusBadge';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { ExplorerLink } from '../components/ExplorerLink';
import { Pager } from '../components/Pager';
import { fmtSeconds, formatPct, roundToneClass, timeAgo, timestampTitle } from '../lib/format';
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
        <span className="page-kicker">{t("m441")}</span>
        <h1>{t("m442")}</h1>
        <p className="page-intro">{t("m443")}</p>
      </div>
      <div className="card table-scroll">
        <table>
          <thead>
            <tr>
              <th>{t("m098")}</th>
              <th>{t("m375")}</th>
              <th>{t("m376")}</th>
              <th>{t("m006")}</th>
              <th>{t("m435")}</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (<EmptyTableRow colSpan={5}>{t("m436")}</EmptyTableRow>) : (items.map((row) => (
                <tr key={`${row.block_height}-${row.block_hash}`}>
                  <td>
                    <ExplorerLink kind="block" value={row.block_hash}>{row.block_height}</ExplorerLink>
                  </td>
                  <td>
                    <span className={`round-chip ${roundToneClass(row.effort_pct)}`}>{formatPct(row.effort_pct)}</span>
                  </td>
                  <td>{fmtSeconds(row.duration_seconds)}</td>
                  <td>
                    <BlockStatusBadge confirmed={row.confirmed} orphaned={row.orphaned} />
                  </td>
                  <td title={timestampTitle(row.timestamp)}>{timeAgo(row.timestamp)}</td>
                </tr>
              )))}
          </tbody>
        </table>
        <Pager {...pagerProps} />
      </div>

    </div>
  );
}
