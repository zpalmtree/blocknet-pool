import { useState } from 'react';
import { t, tx, localeInfo } from '../i18n';
import type { MinerResponse } from '../types';

const DISMISS_KEY_PREFIX = 'bntpool:farm-proxy-hint:dismissed:';

function farmProxyGuideUrl(): string {
  const prefix = localeInfo.path ? `${localeInfo.path}/` : '';
  return `https://seineminer.com/${prefix}blog/blocknet-mining-farm-proxy/`;
}

function readDismissed(address: string): boolean {
  try { return localStorage.getItem(DISMISS_KEY_PREFIX + address) === '1'; } catch { return false; }
}

/** Suggests Seine's farm proxy to an account mining over many direct connections. */
export function MinerFarmProxyHint({ miner, address }: { miner: MinerResponse | null; address: string }) {
  const [dismissedFor, setDismissedFor] = useState<string | null>(null);
  const directConnections = miner?.farm_proxy_hint?.direct_connections;
  if (!directConnections || !address) return null;
  if (dismissedFor === address || readDismissed(address)) return null;

  const dismiss = () => {
    try { localStorage.setItem(DISMISS_KEY_PREFIX + address, '1'); } catch { /* Hidden for this visit only. */ }
    setDismissedFor(address);
  };

  return (
    <section className="miner-update-banner miner-farm-hint" aria-labelledby="miner-farm-hint-title" role="note">
      <svg className="miner-update-banner__icon" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
        <path d="M12 4v6M5 20v-4h14v4M12 10v6M5 16l7-6 7 6" />
        <circle cx="12" cy="4" r="1.6" />
      </svg>
      <div className="miner-update-banner__copy">
        <span className="miner-update-banner__eyebrow">{t('farmProxyEyebrow')}</span>
        <h3 id="miner-farm-hint-title">{t('farmProxyTitle')}</h3>
        <p>{t('farmProxyCount', [directConnections.toLocaleString(localeInfo.code)])} {t('farmProxyBody')}</p>
      </div>
      <div className="miner-update-banner__actions">
        <a className="btn btn-primary" href={farmProxyGuideUrl()} target="_blank" rel="noopener noreferrer">{tx('farmProxyAction', [<span key="arrow" aria-hidden="true">↗</span>])}</a>
        <button type="button" className="miner-farm-hint__dismiss" onClick={dismiss}>{t('farmProxyDismiss')}</button>
      </div>
    </section>
  );
}
