import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
import { PayoutTxLinks } from '../components/PayoutTxLinks';
import { formatCoins, formatFee, timeAgo, toUnixMs } from '../lib/format';
import type { PagerState, PayoutItem } from '../types';

interface PayoutsPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
}

export function PayoutsPage({ active, api, liveTick }: PayoutsPageProps) {
  const [items, setItems] = useState<PayoutItem[]>([]);
  const [pager, setPager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });

  const loadPage = useCallback(async () => {
    try {
      const d = await api.getRecentPayouts({ limit: pager.limit, offset: pager.offset });
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
    <div className={active ? 'page active' : 'page'} id="page-payouts">
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
                    <span className={`badge ${p.confirmed === false ? 'badge-pending' : 'badge-confirmed'}`}>
                      {p.confirmed === false ? 'unconfirmed' : 'confirmed'}
                    </span>
                  </td>
                  <td title={new Date(toUnixMs(p.timestamp)).toLocaleString()}>{timeAgo(p.timestamp)}</td>
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
          <h3>On-Chain Status</h3>
          <p>Transaction hashes point to the public Blocknet explorer, and fresh sends stay marked unconfirmed until the first confirming block is observed by the pool.</p>
        </div>
        <div className="card seo-copy-card">
          <h3>Batch Visibility</h3>
          <p>Each payout batch shows how many miners were paid and how much value moved on-chain in that window.</p>
        </div>
        <div className="card seo-copy-card">
          <h3>Payout Cadence</h3>
          <p>
            Use the recent payout page together with <a href="/status">status</a> and <a href="/">dashboard</a> data to
            understand pool rhythm over time.
          </p>
        </div>
      </div>
    </div>
  );
}
