import { useCallback } from 'react';

import type { ApiClient } from '../api/client';
import { EmptyTableRow } from '../components/EmptyTableRow';
import { Pager } from '../components/Pager';
import { PayoutStatusBadge } from '../components/PayoutStatusBadge';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import { formatCoins, formatFee, timeAgo, toUnixMs } from '../lib/format';
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
        <span className="page-kicker">Payout Transparency</span>
        <h1>Recent Blocknet pool payouts</h1>
        <p className="page-intro">
          Review payout totals, recipient counts, network fees, and explorer transaction links for recent pool payout
          batches.
        </p>
      </div>
      <p style={{ fontSize: 14, color: 'var(--muted)', marginBottom: 16 }}>
        Broadcast payout batches appear here immediately as unconfirmed and flip to confirmed shortly after the first
        block confirmation is observed by the pool. Transaction hashes link to the block explorer for verification.
      </p>
      <div className="card table-scroll">
        <table>
          <thead>
            <tr>
              <th>Amount</th>
              <th>Miners Paid</th>
              <th>Network Fee</th>
              <th>Transaction</th>
              <th>Status</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {!items.length ? (
              <EmptyTableRow colSpan={6}>No payouts yet</EmptyTableRow>
            ) : (
              items.map((p, idx) => (
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
                  <td title={new Date(toUnixMs(p.timestamp)).toLocaleString()}>{timeAgo(p.timestamp)}</td>
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
