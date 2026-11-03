import { t, tx } from '../i18n';
import { fmtSeconds, formatPct } from '../lib/format';
import type { StatsInsightsResponse } from '../types';

/** Current round effort on a 0-200% work scale, with the average block interval. */
export function CurrentRound({ round }: { round: StatsInsightsResponse['round'] | undefined }) {
  const roundEffortPct = round?.effort_pct;
  const hasRoundEffort = roundEffortPct != null && Number.isFinite(roundEffortPct) && roundEffortPct >= 0;
  // This is a work scale, not progress toward a guaranteed block. Keep the
  // exact value above it even when a long round extends past the scale.
  const roundScalePct = hasRoundEffort ? Math.min(roundEffortPct / 2, 100) : 0;
  const averageBlockInterval = round?.expected_block_seconds;
  return (
    <section className="section" aria-labelledby="current-round-title">
      <div className="section-header">
        <h2 id="current-round-title">{t('currentRound')}</h2>
      </div>
      <div className="card">
        <div className="round-meta">
          <div>
            <span className="label">{t("m418")}</span>
            <div className="value mono">{hasRoundEffort ? formatPct(roundEffortPct) : '-'}</div>
          </div>
          <div>
            <span className="label">{t("m421")}</span>
            <div className="value mono">{round && Number.isFinite(round.elapsed_seconds) && round.elapsed_seconds >= 0 ? fmtSeconds(round.elapsed_seconds) : '-'}</div>
          </div>
        </div>

        <div className="round-effort-scale" dir="ltr" aria-hidden="true">
          <div className="round-effort-track">
            <div className="round-effort-fill" style={{ width: `${roundScalePct}%` }} />
            <div className="round-effort-average" />
          </div>
          <div className="round-effort-labels">
            <span>0%</span>
            <span className="round-effort-reference"><bdi>100%</bdi><span dir="auto">{t('roundAverageWork')}</span></span>
            <span>200%+</span>
          </div>
        </div>

        <p className="round-note">{t('roundEffortExplanation')}</p>
        <p className="round-interval">{tx('roundAverageInterval', [
          <bdi className="mono" key="interval">{averageBlockInterval != null && Number.isFinite(averageBlockInterval) && averageBlockInterval > 0 ? fmtSeconds(Math.floor(averageBlockInterval)) : '-'}</bdi>,
        ])}</p>
      </div>
    </section>
  );
}
