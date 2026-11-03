// Seine releases after v0.3.1 accept `solo:` logins; older ones reject them.
const LAST_RELEASE_WITHOUT_SOLO: readonly number[] = [0, 3, 1];

/** Whether the latest Seine release accepts `solo:` addresses. */
export function seineSupportsSolo(latestVersion: string | null | undefined): boolean {
  const match = /^v?(\d+)\.(\d+)\.(\d+)/.exec((latestVersion ?? '').trim());
  if (!match) return false;
  const version = match.slice(1, 4).map(Number);
  for (let index = 0; index < 3; index += 1) {
    if (version[index] !== LAST_RELEASE_WITHOUT_SOLO[index]) {
      return version[index] > LAST_RELEASE_WITHOUT_SOLO[index];
    }
  }
  return false;
}
