import { useCallback } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
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
              <tr>
                <td colSpan={6} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                  No payouts yet
                </td>
              </tr>
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
                    <span className={`badge ${p.confirmed ? 'badge-confirmed' : 'badge-pending'}`}>
                      {p.confirmed ? 'confirmed' : 'unconfirmed'}
                    </span>
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
