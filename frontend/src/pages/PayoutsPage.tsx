import { t } from '../i18n';
import { useCallback } from 'react';

import type { ApiClient } from '../api/client';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { Pager } from '../components/Pager';
import { PayoutStatusBadge } from '../components/PayoutStatusBadge';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import { formatCoins, formatFee, timeAgo, timestampTitle, toUnixMs } from '../lib/format';
import { usePagedData } from '../lib/paging';
import type { PayoutItem } from '../types';

interface PayoutsPageProps {
  api: ApiClient;
  liveTick: number;
}

export function PayoutsPage({ api, liveTick }: PayoutsPageProps) {
  const fetchPayouts = useCallback((limit: number, offset: number) => api.getRecentPayouts(limit, offset), [api]);
  const { items, pagerProps } = usePagedData<PayoutItem>(liveTick, fetchPayouts);

  return (
    <div id="page-payouts">
      <div className="page-header">
        <span className="page-kicker">{t("m444")}</span>
        <h1>{t("m445")}</h1>
        <p className="page-intro">{t("m446")}</p>
      </div>
      <p style={{ fontSize: 14, color: 'var(--muted)', marginBottom: 16 }}>{t("m447")}</p>
      <div className="card table-scroll">
        <table>
          <thead>
            <tr>
              <th>{t("m438")}</th>
              <th>{t("m439")}</th>
              <th>{t("m448")}</th>
              <th>{t("m449")}</th>
              <th>{t("m006")}</th>
              <th>{t("m377")}</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (<EmptyTableRow colSpan={6}>{t("m440")}</EmptyTableRow>) : (items.map((p, idx) => (
                <tr key={`${toUnixMs(p.timestamp)}-${idx}`}>
                  <td>{formatCoins(p.total_amount)}</td>
                  <td>{p.recipient_count}</td>
                  <td>{formatFee(p.total_fee)}</td>
                  <td>
                    <PayoutTxLinks hashes={p.tx_hashes} />
                  </td>
                  <td>
                    <PayoutStatusBadge status={p.confirmed ? 'confirmed' : 'unconfirmed'} />
                  </td>
                  <td title={timestampTitle(p.timestamp)}>{timeAgo(p.timestamp)}</td>
                </tr>
              )))}
          </tbody>
        </table>
        <Pager {...pagerProps} />
      </div>

    </div>
  );
}
