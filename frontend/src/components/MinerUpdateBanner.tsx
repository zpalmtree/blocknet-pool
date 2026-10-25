import { t, tx } from '../i18n';
import type { MinerResponse } from '../types';

const SEINE_RELEASES_URL = 'https://github.com/zpalmtree/seine/releases';

export function MinerUpdateBanner({ miner }: { miner: MinerResponse | null }) {
  const version = miner?.latest_miner_version?.trim();
  if (!version || !miner) return null;

  const outdated = miner.workers.filter((worker) => worker.version_status === 'outdated').length;
  const unknown = miner.workers.filter((worker) => worker.version_status === 'unknown').length;
  if (!outdated && !unknown) return null;

  return (
    <section className="miner-update-banner" aria-labelledby="miner-update-title" role="status">
      <svg className="miner-update-banner__icon" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
        <path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5" />
      </svg>
      <div className="miner-update-banner__copy">
        <span className="miner-update-banner__eyebrow">{outdated ? t("m579") : t("m580")}</span>
        <h3 id="miner-update-title">{tx("m581", [<bdi dir="ltr">{version}</bdi>])}</h3>
        <p>{tx("m586", [outdated > 0 && t('m607', [outdated]), unknown > 0 && t('m608', [unknown]), outdated ? t("m584") : t("m585")])}</p>
      </div>
      <div className="miner-update-banner__actions">
        <a className="btn btn-primary" href={SEINE_RELEASES_URL} target="_blank" rel="noopener noreferrer">{tx("m587", [<span aria-hidden="true">↗</span>])}</a>
        <a className="miner-update-banner__workers" href="#miner-workers">{tx("m588", [<span aria-hidden="true">↓</span>])}</a>
      </div>
    </section>
  );
}
