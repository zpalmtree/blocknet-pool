import { useState } from 'react';
import type { InfoResponse } from '../types';
import { t, tx } from '../i18n';
import { formatExactCoins } from '../lib/format';
import { minimumPayoutAtoms } from '../lib/payout-progress';
import { knownExchange, shellQuote, validMemo } from '../lib/payout-destination';

export function ExchangeSetup({ info, poolUrl }: { info: InfoResponse | null; poolUrl: string }) {
  const [address, setAddress] = useState('');
  const [memo, setMemo] = useState('');
  const [windows, setWindows] = useState(false);
  const [copied, setCopied] = useState(false);
  const exchange = knownExchange(info, address);
  const valid = /^[1-9A-HJ-NP-Za-km-z]{90,96}$/.test(address.trim()) && !!memo && validMemo(memo);
  const personalMinimum = minimumPayoutAtoms(info?.min_payout_amount);
  const memoMinimum = minimumPayoutAtoms(info?.memo_min_payout_amount);
  const enabled = info?.payout_memos_enabled === true;
  const command = `${windows ? '.\\seine.exe' : './seine'} --pool-url ${shellQuote(poolUrl, windows)} --address ${shellQuote(address.trim(), windows)} --payout-memo ${shellQuote(memo, windows)}`;
  return <details className="command-option">
    <summary>{t('memoTitle')}</summary>
    <div className="memo-fields">
      <label htmlFor="exchange-address">{t('memoAddress')}</label>
      <input type="text" id="exchange-address" value={address} dir="ltr" autoComplete="off" spellCheck={false} onChange={e => { setAddress(e.target.value); setCopied(false); }} />
      <label htmlFor="exchange-memo">{t('memoLabel')}</label>
      <input type="text" id="exchange-memo" value={memo} dir="ltr" autoComplete="off" spellCheck={false} aria-invalid={!validMemo(memo) || (!!exchange && !memo)}
        aria-describedby="exchange-memo-help" onChange={e => { setMemo(e.target.value); setCopied(false); }} />
      <p id="exchange-memo-help" role={exchange && !memo ? 'alert' : undefined}>{exchange ? t('memoRequired', [exchange.name]) : t('memoLimit')}</p>
      {!validMemo(memo) && <p role="alert">{t('memoLimit')}</p>}
      {enabled ? <p>{t('memoMinimum', [info.memo_min_payout_amount ?? '—'])}</p> : <p role="status">{t('memoUnavailable')}</p>}
      {enabled && personalMinimum != null && memoMinimum != null && personalMinimum < memoMinimum &&
        <p>{tx('memoPersonalMinimum', [<bdi dir="ltr">{formatExactCoins(personalMinimum)}</bdi>])}</p>}
      <label><input type="checkbox" checked={windows} onChange={e => { setWindows(e.target.checked); setCopied(false); }} /> Windows · PowerShell</label>
      {valid && enabled && <pre className="config-block" dir="ltr">{command}</pre>}
      <button className="btn btn-secondary" type="button" disabled={!valid || !enabled} onClick={() => {
        void navigator.clipboard?.writeText(command).then(() => setCopied(true)).catch(() => setCopied(false));
      }}>{copied ? t('m386') : t('m461')}</button>
    </div>
  </details>;
}
