import type { MinerResponse } from '../types';

export const SEINE_RELEASES_URL = 'https://github.com/zpalmtree/seine/releases';

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
        <span className="miner-update-banner__eyebrow">{outdated ? 'Miner update available' : 'Check your miner version'}</span>
        <h3 id="miner-update-title">seine {version} is available</h3>
        <p>
          {outdated > 0 && `${outdated} ${outdated === 1 ? 'worker is' : 'workers are'} running an older version. `}
          {unknown > 0 && `${unknown} ${unknown === 1 ? 'worker has' : 'workers have'} an unknown version. `}
          {outdated ? 'Update to the latest release.' : 'Check your workers against the latest release.'}
        </p>
      </div>
      <div className="miner-update-banner__actions">
        <a className="btn btn-primary" href={SEINE_RELEASES_URL} target="_blank" rel="noopener noreferrer">
          Download seine <span aria-hidden="true">↗</span>
        </a>
        <a className="miner-update-banner__workers" href="#miner-workers">Review workers <span aria-hidden="true">↓</span></a>
      </div>
    </section>
  );
}
