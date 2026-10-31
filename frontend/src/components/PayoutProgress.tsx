import { useEffect, useState } from 'react';
import type { ApiClient } from '../api/client';
import { t, tx } from '../i18n';
import { formatExactCoins } from '../lib/format';
import { minimumPayoutAtoms, payoutProgress } from '../lib/payout-progress';

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
      if (active) setMinimum(minimumPayoutAtoms(accountMinimum ?? (memoAccount ? info.memo_min_payout_amount : info.min_payout_amount)));
    }).catch(() => {
      if (active) setMinimum(null);
    });
    return () => { active = false; };
  }, [api, refresh, memoAccount, accountMinimum]);

  const progress = minimum == null ? null : payoutProgress(minimum, confirmed, queued);
  return <div className="miner-payout-progress">
    <strong>{tx('statsPayoutMinimum', [minimum == null ? '—' : <bdi dir="ltr">{formatExactCoins(minimum)}</bdi>])}</strong>
    {minimum === undefined ? <p>{t('m094')}</p>
      : minimum === null ? <p>{t('statsPayoutUnavailable')}</p>
      : progress?.state === 'unavailable' ? <p>{t('statsPayoutBalanceUnavailable')}</p>
      : progress?.state === 'below' ? <p>{tx('statsPayoutRemaining', [<bdi dir="ltr">{formatExactCoins(progress.amount)}</bdi>])}</p>
      : progress?.state === 'queued' ? <p>{tx('statsPayoutQueued', [<bdi dir="ltr">{formatExactCoins(progress.amount)}</bdi>])}</p>
      : progress?.state === 'met' ? <p>{t('statsPayoutMet')}</p> : null}
    {memoAccount && <p>{t('memoPolicy')}</p>}
    <p>{t('statsPayoutBasis')}</p>
  </div>;
}
