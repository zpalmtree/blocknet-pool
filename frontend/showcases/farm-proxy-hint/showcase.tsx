// Development-only: the real My Stats banners with synthetic miner data.
import { createRoot } from 'react-dom/client';
import { initializeLanguage, t, localeInfo } from '../../src/i18n';
import { MinerFarmProxyHint } from '../../src/components/MinerFarmProxyHint';
import { MinerUpdateBanner } from '../../src/components/MinerUpdateBanner';
import type { MinerResponse } from '../../src/types';
import '../../src/styles.css';

// ?state=hint (default) | with-update | dismissed | none
const params = new URLSearchParams(location.search);
const state = params.get('state') || 'hint';
document.documentElement.dataset.theme = params.get('theme') === 'light' ? 'light' : 'dark';
const address = 'DEMOxFarmxAddressxOnlyxForxThexShowcasexNotxAxRealxWalletxDEMOxFarmxAddressxOnlyxForxShowcase';
try {
  if (state === 'dismissed') localStorage.setItem(`bntpool:farm-proxy-hint:dismissed:${address}`, '1');
  else localStorage.removeItem(`bntpool:farm-proxy-hint:dismissed:${address}`);
} catch { /* fixture only */ }
const now = Math.floor(Date.now() / 1000);
const miner: MinerResponse = {
  hashrate: 2400, mining_since: now - 86400, blocks_found: 0, total_accepted: 9000, total_rejected: 12,
  latest_miner_version: state === 'with-update' ? '0.3.1' : '',
  workers: Array.from({ length: 6 }, (_, i) => ({
    worker: `seine-${1000 + i}`, version_status: state === 'with-update' && i < 4 ? 'outdated' : 'current', hashrate: 400,
    accepted: 1500, rejected: 2, last_share_at: now - 30, miner_version: state === 'with-update' && i < 4 ? '0.2.45' : '0.3.1', backend: 'cpu#1',
  })),
  shares: [], payouts: [], verification_hold: null,
  farm_proxy_hint: state === 'none' ? undefined : { direct_connections: 1840 },
};

initializeLanguage().then(() => {
  document.documentElement.lang = localeInfo.htmlLang;
  document.documentElement.dir = localeInfo.dir;
  createRoot(document.getElementById('root')!).render(
    <main className="container" style={{ paddingTop: 32 }}>
      <div id="page-stats">
        <div className="page-header"><h1>{t('m616')}</h1></div>
        <MinerUpdateBanner miner={miner} />
        <MinerFarmProxyHint miner={miner} address={address} />
        <div className="card" style={{ padding: 24 }}>{state === 'dismissed' || state === 'none' ? 'No farm proxy hint for this account.' : 'Rest of My Stats.'}</div>
      </div>
    </main>,
  );
});
