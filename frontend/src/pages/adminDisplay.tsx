import type { CSSProperties, ReactNode } from 'react';

import { StatCard } from '../components/StatCard';
import {
  formatCoinAmount,
  formatCoins,
  formatPct,
  ratioPct as calculateRatioPct,
  timestampTitle,
  timeUntil,
  toUnixMs,
} from '../lib/format';
import type {
  ActiveVerificationHold,
  AdminMissingCompletedPayoutIssue,
  AdminShareDiagnosticsWindow,
  RecoveryInstanceId,
  RecoveryInstanceStatus,
  RecoveryOperationKind,
  RecoveryStatusResponse,
  UnixLike,
} from '../types';

export const MAX_DAEMON_LOG_LINES = 1000;
export const DAEMON_LOG_RECONNECT_DELAY_MS = 1500;
export const HOT_PATH_LATENCY_WARN_MILLIS = 1000;
export const HOT_PATH_LATENCY_SPIKE_MILLIS = 5000;
export const ACKNOWLEDGED_LAUNCH_ERA_MINER_SHORTFALL = 1_546_507_661_992;
export const WARN_TEXT_STYLE = { color: 'var(--warn)' };
export const GOOD_TEXT_STYLE = { color: 'var(--good)' };
export const MUTED_TEXT_STYLE = { color: 'var(--muted)' };
export const SMALL_MUTED_STYLE: CSSProperties = { fontSize: 11, color: 'var(--muted)' };
export const SMALL_MUTED_TOP_STYLE: CSSProperties = { ...SMALL_MUTED_STYLE, marginTop: 4 };
export const MONO_MUTED_STYLE: CSSProperties = { fontSize: 12, color: 'var(--muted)' };
export const MUTED_SUMMARY_STYLE: CSSProperties = { marginTop: 12, fontSize: 12, color: 'var(--muted)' };
export const SEMIBOLD_STYLE: CSSProperties = { fontWeight: 600 };
export const MUTED_SEMIBOLD_STYLE: CSSProperties = { color: 'var(--muted)', fontWeight: 600 };
export const FOOTER_LABEL_CELL_STYLE: CSSProperties = { fontWeight: 700, textAlign: 'left' };
export const TOP_6_STYLE: CSSProperties = { marginTop: 6 };
export const SHARE_DIAGNOSTIC_REASONS = [
  'invalid share proof',
  'low difficulty share',
  'stale job',
  'address quarantined',
  'server busy',
  'validation timeout',
];
export const REWARD_STATUS_META: Record<string, { label: string; tone: string }> = {
  included: { label: 'Included', tone: 'var(--good)' },
  capped_provisional: { label: 'Included (capped)', tone: 'var(--warn)' },
  awaiting_verified_shares: { label: 'Needs verified shares', tone: 'var(--warn)' },
  recorded_only: { label: 'Recorded only', tone: 'var(--muted)' },
};
export const RECOVERY_OPERATION_STATE_LABELS: Record<string, string> = {
  running: 'Running',
  succeeded: 'Succeeded',
  failed: 'Failed',
};

const OVERLOAD_MODE_LABELS: Record<string, string> = {
  emergency: 'Emergency',
  shed: 'Shedding',
};
const RECOVERY_INSTANCE_LABELS: Record<string, string> = {
  primary: 'Primary',
  standby: 'Standby',
};
const RECOVERY_OPERATION_LABELS: Record<string, string> = {
  pause_payouts: 'Pause payouts',
  resume_payouts: 'Resume payouts',
  start_standby_sync: 'Start inactive sync',
  rebuild_standby_wallet: 'Rebuild inactive wallet',
  cutover: 'Cut over',
  purge_inactive_daemon: 'Purge inactive daemon',
};
const VALIDATION_HOLD_CAUSE_LABELS: Record<string, string> = {
  provisional_backlog: 'Backlog drain',
  payout_coverage: 'Payout boost',
  invalid_samples: 'Validation review',
};
const RECOVERY_STATE_BADGE_FALLBACK = { label: 'Stopped', className: 'badge-pending' };
const RECOVERY_STATE_BADGE_META: Record<string, { label: string; className: string }> = {
  ready: { label: 'Ready', className: 'badge-confirmed' },
  syncing: { label: 'Syncing', className: 'badge-pending' },
  starting: { label: 'Starting', className: 'badge-pending' },
  failed: { label: 'Failed', className: 'badge-orphaned' },
  degraded: { label: 'Degraded', className: 'badge-orphaned' },
};

export const warnTextStyle = (active: boolean) => (active ? WARN_TEXT_STYLE : undefined);
export const warnGoodTextStyle = (warn: boolean) => (warn ? WARN_TEXT_STYLE : GOOD_TEXT_STYLE);

export const labelFor = (value: string | null | undefined, labels: Record<string, string>, fallback: string) =>
  value ? labels[value] ?? fallback : fallback;

export function rewardStatusLabel(status: string): string {
  return REWARD_STATUS_META[status]?.label ?? 'No eligible shares';
}

export function rewardDeltaStyle(value: number | null | undefined, paidOut = false): CSSProperties {
  if (value == null) return { color: 'var(--muted)' };
  if (value === 0) return { color: 'var(--good)' };
  return { color: paidOut ? 'var(--muted)' : 'var(--warn)' };
}

export function formatSignedCoins(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const prefix = value > 0 ? '+' : value < 0 ? '-' : '';
  return `${prefix}${formatCoinAmount(Math.abs(value))} BNT`;
}

export function warnPositiveCoins(value: number) {
  return value > 0 ? <span style={WARN_TEXT_STYLE}>{formatCoins(value)}</span> : formatCoins(value);
}

export function formatMillis(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  if (value >= 10_000) return `${(value / 1000).toFixed(0)}s`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}s`;
  return `${Math.round(value)}ms`;
}

export function overloadModeLabel(mode: string | null | undefined): string {
  return labelFor(mode, OVERLOAD_MODE_LABELS, 'Normal');
}

export function reconciliationRecommendation(issue: AdminMissingCompletedPayoutIssue): string {
  if (issue.orphaned_linked_amount > 0 && issue.live_linked_amount === 0 && issue.unlinked_amount === 0) {
    return 'Known source credits only point at orphaned blocks. Dropping the paid amount is the usual resolution.';
  }
  if (issue.live_linked_amount > 0 && issue.orphaned_linked_amount === 0 && issue.unlinked_amount === 0) {
    return 'Known source credits still point at live blocks. Restoring the amount to pending is the usual resolution.';
  }
  if (issue.unlinked_amount > 0) {
    return 'Part of this payout could not be reconstructed from historical source credits. Choose the operator override that matches the real chain outcome.';
  }
  if (issue.orphaned_linked_amount > 0 && issue.live_linked_amount > 0) {
    return 'This payout mixes live and orphaned known sources. Review before choosing an override.';
  }
  return 'Choose the override that matches the current chain state.';
}

export function formatWholeNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '0';
  return Math.round(value).toLocaleString();
}

export function hasActiveUntil(value: UnixLike | null | undefined): boolean {
  const ms = value ? toUnixMs(value) : 0;
  return !!ms && ms > Date.now();
}

export function holdUntilLabel(value: UnixLike | null | undefined): string {
  return hasActiveUntil(value) ? timeUntil(value as UnixLike) : '-';
}

export function holdUntilTitle(value: UnixLike | null | undefined): string | undefined {
  return hasActiveUntil(value) ? timestampTitle(value as UnixLike) || undefined : undefined;
}

export function VerificationHoldBadge({ hold }: { hold: ActiveVerificationHold }) {
  const quarantined = hasActiveUntil(hold.quarantined_until);
  const forceVerified = hasActiveUntil(hold.force_verify_until);
  const validationForced = hasActiveUntil(hold.validation_forced_until);
  let label = 'Active';
  if (quarantined) label = 'Quarantined';
  else if (forceVerified && validationForced) label = 'Risk + validation';
  else if (forceVerified) label = 'Risk forced';
  else if (validationForced) {
    label = labelFor(hold.validation_hold_cause, VALIDATION_HOLD_CAUSE_LABELS, 'Validation forced');
  }
  const active = quarantined || forceVerified || validationForced;
  const className = !active ? 'badge-pending' : quarantined ? 'badge-orphaned' : 'badge-confirmed';
  return <span className={className}>{label}</span>;
}

export function ValidationHoldUntilCell({ hold }: { hold: ActiveVerificationHold }) {
  const active = hasActiveUntil(hold.validation_forced_until);
  const label = active ? holdUntilLabel(hold.validation_forced_until) : '-';
  let hint: string | null = null;
  switch (hold.validation_hold_cause) {
    case 'provisional_backlog':
      if (
        (hold.validation_recent_provisional_difficulty ?? 0) > 0 ||
        (hold.validation_recent_verified_difficulty ?? 0) > 0
      ) {
        hint = `auto-clears once recent provisional diff ${formatWholeNumber(
          hold.validation_recent_provisional_difficulty
        )} settles near verified diff ${formatWholeNumber(hold.validation_recent_verified_difficulty)}`;
        break;
      }
      hint = 'auto-clears once backlog drains';
      break;
    case 'payout_coverage':
      hint = 'auto-clears once coverage recovers';
      break;
  }
  const temporaryAssist =
    active &&
    !hasActiveUntil(hold.quarantined_until) &&
    !hasActiveUntil(hold.force_verify_until) &&
    (hold.validation_hold_cause === 'provisional_backlog' || hold.validation_hold_cause === 'payout_coverage');
  return (
    <td className="mono" title={holdUntilTitle(hold.validation_forced_until)}>
      <div>{temporaryAssist ? `up to ${label}` : label}</div>
      {active && hint ? <div style={SMALL_MUTED_TOP_STYLE}>{hint}</div> : null}
    </td>
  );
}

export function RecoveryStateBadge({ state }: { state: RecoveryInstanceStatus['state'] | undefined }) {
  const { label, className } = state
    ? RECOVERY_STATE_BADGE_META[state] ?? RECOVERY_STATE_BADGE_FALLBACK
    : RECOVERY_STATE_BADGE_FALLBACK;
  return <span className={`badge ${className}`}>{label}</span>;
}

export function shareWindowReasonCount(
  window: AdminShareDiagnosticsWindow | null | undefined,
  reason: string
): number {
  const target = reason.trim().toLowerCase();
  if (!window?.by_reason?.length || !target) return 0;
  const match = window.by_reason.find((item) => item.reason.trim().toLowerCase() === target);
  return match?.count ?? 0;
}

export function shareWindowTotal(window: AdminShareDiagnosticsWindow | null | undefined): number {
  return (window?.accepted ?? 0) + (window?.rejected ?? 0);
}

export function shareWindowRejectPct(window: AdminShareDiagnosticsWindow | null | undefined): number {
  return calculateRatioPct(window?.rejected, shareWindowTotal(window));
}

export function shareWindowReasonCell(window: AdminShareDiagnosticsWindow, reason: string) {
  const count = shareWindowReasonCount(window, reason);
  const total = shareWindowTotal(window);
  const pct = total > 0 ? (count / total) * 100 : null;
  return (
    <td key={reason} className="mono">
      {formatPct(pct, 2)}
      <div style={SMALL_MUTED_STYLE}>{count} rejects</div>
    </td>
  );
}

export function recoveryInstanceLabel(instance: RecoveryInstanceId | null | undefined): string {
  return labelFor(instance, RECOVERY_INSTANCE_LABELS, 'Unknown');
}

export function recoveryOperationLabel(kind: RecoveryOperationKind | null | undefined): string {
  return labelFor(kind, RECOVERY_OPERATION_LABELS, 'Unknown operation');
}

export function RecoveryWalletSyncCard({ item }: { item: RecoveryInstanceStatus | null }) {
  const syncedHeight = item?.wallet.synced_height;
  const chainHeight = item?.chain_height ?? item?.wallet.chain_height;
  const value =
    syncedHeight == null && chainHeight == null
      ? '-'
      : syncedHeight == null
        ? `- / ${chainHeight}`
        : chainHeight == null
          ? `${syncedHeight}`
          : `${syncedHeight} / ${chainHeight}`;
  const lag =
    syncedHeight == null || chainHeight == null
      ? null
      : syncedHeight >= chainHeight
        ? 'caught up'
        : `${chainHeight - syncedHeight} blocks behind`;
  return (
    <StatCard label="Wallet Sync" value={value} mono>
      {lag ? (
        <div className="label" style={TOP_6_STYLE}>
          {lag}
        </div>
      ) : null}
    </StatCard>
  );
}

export function recoveryPendingDeltaNote(status: RecoveryStatusResponse | null): string | null {
  const activeInstance = status?.active_instance;
  if (activeInstance == null) return null;
  const active = status.instances.find((item) => item.instance === activeInstance) ?? null;
  const inactive = status.instances.find((item) => item.instance !== activeInstance) ?? null;
  if (!active?.wallet.loaded || !inactive?.wallet.loaded) return null;
  if (!active.wallet.address || !inactive.wallet.address) return null;
  if (active.wallet.address !== inactive.wallet.address) return null;
  if ((active.wallet.pending_unconfirmed ?? 0) <= 0) return null;
  return 'Primary and standby share the same wallet seed but keep separate wallet files. Unconfirmed sends only live on the active daemon until they confirm, so temporary spendable deltas are expected.';
}

export function AdminNoticeCard({ children, tone }: { children: ReactNode; tone?: 'warning' | 'error' }) {
  return (
    <div
      className="card section"
      style={{
        marginBottom: 16,
        borderColor:
          tone === 'warning'
            ? 'rgba(247, 180, 75, 0.45)'
            : tone === 'error'
              ? 'rgba(214, 88, 88, 0.45)'
              : undefined,
      }}
    >
      <p className="section-lead" style={{ margin: 0, color: tone === 'error' ? 'var(--bad)' : undefined }}>
        {children}
      </p>
    </div>
  );
}
