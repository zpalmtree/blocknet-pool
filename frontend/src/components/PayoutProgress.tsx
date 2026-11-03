import { useEffect, useState } from 'react';
import type { ApiClient } from '../api/client';
import { t, tx } from '../i18n';
import { formatCoinAmount, formatCoins } from '../lib/format';
import { heroPayout, minimumPayoutAtoms } from '../lib/payout-progress';

interface PayoutProgressProps {
  api: ApiClient;
  memoAccount?: boolean;
  accountMinimum?: number;
  confirmed?: number;
  queued: number;
  liveTick: number;
}

export function PayoutProgress({ api, confirmed, queued, liveTick, memoAccount = false, accountMinimum }: PayoutProgressProps) {
  // undefined is loading; null is unavailable. Never assume a configured minimum.
  const [minimum, setMinimum] = useState<number | null | undefined>(undefined);
  const refresh = Math.floor(liveTick / 12);
  useEffect(() => {
    if (accountMinimum !== undefined) {
      setMinimum(minimumPayoutAtoms(accountMinimum));
      return;
    }
    let active = true;
    api.getInfo().then((info) => {
      if (active) setMinimum(minimumPayoutAtoms(memoAccount ? info.memo_min_payout_amount : info.min_payout_amount));
    }).catch(() => {
      if (active) setMinimum(null);
    });
    return () => { active = false; };
  }, [api, refresh, memoAccount, accountMinimum]);

  if (minimum === undefined) return <div className="miner-pay"><p>{t('m094')}</p></div>;
  if (minimum === null) return <div className="miner-pay"><p>{t('statsPayoutUnavailable')}</p></div>;
  const payout = heroPayout(minimum, confirmed, queued);
  return <div className="miner-pay">
    {payout.state === 'unavailable' && <p>{t('statsPayoutBalanceUnavailable')}</p>}
    {payout.state === 'below' && <>
      <div className="miner-pay-top"><span>{t('nextPayout')}</span>
        <span>{tx('payoutOf', [<strong key="have" dir="ltr">{formatCoinAmount(payout.confirmed)}</strong>, <bdi key="min" dir="ltr">{formatCoins(payout.minimum)}</bdi>])}</span></div>
      <div className="miner-pay-bar"><div style={{ width: `${payout.fillPct}%` }} /></div>
    </>}
    {payout.state === 'ready' && <>
      <div className="miner-pay-top"><span className="miner-pay-tag"><i aria-hidden="true" />{t('payoutReady')}</span>
        <strong dir="ltr">{formatCoins(payout.amount)}</strong></div>
      <div className="miner-pay-bar"><div style={{ width: '100%' }} /></div>
    </>}
    {payout.state === 'queued' && <>
      <div className="miner-pay-top"><span className="miner-pay-tag"><i aria-hidden="true" />{t('payoutQueuedTag')}</span>
        <strong dir="ltr">{formatCoins(payout.amount)}</strong></div>
      <div className="miner-pay-bar"><div className="is-queued" style={{ width: '100%' }} /></div>
    </>}
  </div>;
}
