import type { WorkerVersionStatus } from '../types';

import { SEINE_RELEASES_URL } from './MinerUpdateBanner';

interface WorkerVersionBadgeProps {
  status: WorkerVersionStatus;
  latestVersion: string;
}

export function WorkerVersionBadge({ status, latestVersion }: WorkerVersionBadgeProps) {
  if (status === 'current' || !latestVersion.trim()) return null;
  const tooltip =
    `seine ${latestVersion} is available — ${status === 'unknown' ? 'this worker’s version is unknown' : 'this worker is running an older version'}. ` +
    'Click to open the seine releases page.';
  return (
    <a
      className="badge badge-pending worker-version-badge"
      href={SEINE_RELEASES_URL}
      target="_blank"
      rel="noopener noreferrer"
      title={tooltip}
    >
      {status === 'unknown' ? 'check version' : 'update available'}
    </a>
  );
}
