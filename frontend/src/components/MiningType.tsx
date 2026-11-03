import { t } from '../i18n';

// Group of miners for pool (PPLNS) work, one miner for solo.
const POOL_ICON = (
  <svg viewBox="0 0 16 16" aria-hidden="true">
    <circle cx="5" cy="6" r="2" />
    <circle cx="11" cy="6" r="2" />
    <path d="M1.5 13c.4-2.2 1.8-3.3 3.5-3.3S8.1 10.8 8.5 13M7.5 13c.4-2.2 1.8-3.3 3.5-3.3s3.1 1.1 3.5 3.3" />
  </svg>
);
const SOLO_ICON = (
  <svg viewBox="0 0 16 16" aria-hidden="true">
    <circle cx="8" cy="5.5" r="2.3" />
    <path d="M4 13.5c.5-2.6 2-3.9 4-3.9s3.5 1.3 4 3.9" />
  </svg>
);

/** How a block or rig was mined: shared PPLNS pool work or solo. */
export function MiningType({ solo }: { solo?: boolean }) {
  return (
    <span className="mining-type">
      {solo ? SOLO_ICON : POOL_ICON}
      <span>{solo ? t('typeSolo') : t('typePool')}</span>
    </span>
  );
}
