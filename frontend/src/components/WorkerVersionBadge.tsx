import type { WorkerVersionStatus } from '../types';

const SEINE_RELEASES_URL = 'https://github.com/zpalmtree/seine/releases';

interface WorkerVersionBadgeProps {
  status: WorkerVersionStatus;
  latestVersion: string;
}

export function WorkerVersionBadge({ status, latestVersion }: WorkerVersionBadgeProps) {
  if (status === 'current' || !latestVersion.trim()) return null;
  const tooltip =
    `seine ${latestVersion} is available — this worker appears to be running an older or unknown miner version. ` +
    'Updating seine adds block-candidate preservation for GPU miners, degraded-run warnings, and faster GPU startup. ' +
    'Click to open the seine releases page.';
  return (
    <a
      className="badge badge-pending worker-version-badge"
      href={SEINE_RELEASES_URL}
      target="_blank"
      rel="noopener noreferrer"
      title={tooltip}
    >
      update available
    </a>
  );
}
