import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
import { parseAnsiLine, type ParsedAnsiSegment } from '../lib/ansi';
import {
  fmtSeconds,
  formatCoinAmount,
  formatCoins,
  formatCompactCoins,
  humanRate,
  shortAddr,
  timeAgo,
  timeUntil,
  toUnixMs,
} from '../lib/format';
import type {
  ActiveVerificationHold,
  AdminBalanceItem,
  AdminBalanceOverviewResponse,
  AdminMissingCompletedPayoutIssue,
  AdminOrphanedBlockIssue,
  AdminReconciliationIssuesResponse,
  AdminShareDiagnosticsResponse,
  AdminShareDiagnosticsWindow,
  AdminTab,
  BlockRewardBreakdownResponse,
  BlockItem,
  HealthResponse,
  MinerListItem,
  PagerState,
  ReconciliationPayoutResolutionAction,
  RecoveryInstanceId,
  RecoveryInstanceStatus,
  RecoveryOperationKind,
  RecoveryStatusResponse,
  UnixLike,
} from '../types';

const MAX_DAEMON_LOG_LINES = 1000;
const DAEMON_LOG_RECONNECT_DELAY_MS = 1500;
const HOT_PATH_LATENCY_WARN_MILLIS = 1000;
const HOT_PATH_LATENCY_SPIKE_MILLIS = 5000;
const ACKNOWLEDGED_LAUNCH_ERA_MINER_SHORTFALL = 1_546_507_661_992;

function rewardStatusLabel(status: string): string {
  switch (status) {
    case 'included':
      return 'Included';
    case 'capped_provisional':
      return 'Included (capped)';
    case 'finder_fallback':
      return 'Finder fallback';
    case 'risky':
      return 'Verified only';
    case 'awaiting_verified_shares':
      return 'Needs verified shares';
    case 'awaiting_verified_ratio':
      return 'Needs verified ratio';
    case 'recorded_only':
      return 'Recorded only';
    default:
      return 'No eligible shares';
  }
}

function rewardStatusTone(status: string): string {
  switch (status) {
    case 'included':
    case 'finder_fallback':
      return 'var(--good)';
    case 'capped_provisional':
      return 'var(--warn)';
    case 'risky':
      return 'var(--warn)';
    case 'recorded_only':
      return 'var(--muted)';
    default:
      return 'var(--warn)';
  }
}


function formatSignedCoins(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  const prefix = value > 0 ? '+' : value < 0 ? '-' : '';
  return `${prefix}${formatCoinAmount(Math.abs(value))} BNT`;
}

function pct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(2)}%`;
}

function ratioPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${(value * 100).toFixed(2)}%`;
}

function formatMillis(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '-';
  if (value >= 10_000) return `${(value / 1000).toFixed(0)}s`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}s`;
  return `${Math.round(value)}ms`;
}

function overloadModeLabel(mode: string | null | undefined): string {
  switch (mode) {
    case 'emergency':
      return 'Emergency';
    case 'shed':
      return 'Shedding';
    default:
      return 'Normal';
  }
}

function reconciliationRecommendation(issue: AdminMissingCompletedPayoutIssue): string {
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

function roundChipClass(tone: 'ok' | 'warn' | 'critical'): string {
  switch (tone) {
    case 'critical':
      return 'round-chip is-critical';
    case 'warn':
      return 'round-chip is-warn';
    default:
      return 'round-chip is-ok';
  }
}



function formatAdminTimestamp(value: UnixLike): string {
  const ms = toUnixMs(value);
  return ms ? new Date(ms).toLocaleString() : '-';
}

function formatWholeNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '0';
  return Math.round(value).toLocaleString();
}

function hasActiveUntil(value: UnixLike | null | undefined): boolean {
  const ms = value ? toUnixMs(value) : 0;
  return !!ms && ms > Date.now();
}

function holdUntilLabel(value: UnixLike | null | undefined): string {
  return hasActiveUntil(value) ? timeUntil(value as UnixLike) : '-';
}

function holdUntilTitle(value: UnixLike | null | undefined): string | undefined {
  return hasActiveUntil(value) ? formatAdminTimestamp(value as UnixLike) : undefined;
}

function verificationHoldBadgeClass(active: boolean, tone: 'warn' | 'good' = 'warn'): string {
  if (!active) return 'badge-pending';
  return tone === 'good' ? 'badge-confirmed' : 'badge-orphaned';
}

function verificationHoldActive(hold: ActiveVerificationHold): boolean {
  return (
    hasActiveUntil(hold.quarantined_until) ||
    hasActiveUntil(hold.force_verify_until) ||
    hasActiveUntil(hold.validation_forced_until)
  );
}

function verificationHoldTone(hold: ActiveVerificationHold): 'warn' | 'good' {
  return hasActiveUntil(hold.quarantined_until) ? 'warn' : 'good';
}

function verificationHoldLabel(hold: ActiveVerificationHold): string {
  if (hasActiveUntil(hold.quarantined_until)) return 'Quarantined';
  if (hasActiveUntil(hold.force_verify_until) && hasActiveUntil(hold.validation_forced_until)) {
    return 'Risk + validation';
  }
  if (hasActiveUntil(hold.force_verify_until)) return 'Risk forced';
  if (hasActiveUntil(hold.validation_forced_until)) {
    switch (hold.validation_hold_cause) {
      case 'provisional_backlog':
        return 'Backlog drain';
      case 'payout_coverage':
        return 'Payout boost';
      case 'invalid_samples':
        return 'Validation review';
      default:
        return 'Validation forced';
    }
  }
  return 'Active';
}

function isTemporaryValidationAssist(hold: ActiveVerificationHold): boolean {
  return (
    !hasActiveUntil(hold.quarantined_until) &&
    !hasActiveUntil(hold.force_verify_until) &&
    hasActiveUntil(hold.validation_forced_until) &&
    (hold.validation_hold_cause === 'provisional_backlog' || hold.validation_hold_cause === 'payout_coverage')
  );
}

function isRiskVerificationHold(hold: ActiveVerificationHold): boolean {
  return (
    hasActiveUntil(hold.quarantined_until) ||
    hasActiveUntil(hold.force_verify_until) ||
    hold.validation_hold_cause === 'invalid_samples'
  );
}

function validationHoldUntilLabel(hold: ActiveVerificationHold): string {
  if (!hasActiveUntil(hold.validation_forced_until)) return '-';
  const label = holdUntilLabel(hold.validation_forced_until);
  return isTemporaryValidationAssist(hold) ? `up to ${label}` : label;
}

function validationHoldUntilHint(hold: ActiveVerificationHold): string | null {
  if (!hasActiveUntil(hold.validation_forced_until)) return null;
  switch (hold.validation_hold_cause) {
    case 'provisional_backlog':
      if (
        (hold.validation_recent_provisional_difficulty ?? 0) > 0 ||
        (hold.validation_recent_verified_difficulty ?? 0) > 0
      ) {
        return `auto-clears once recent provisional diff ${formatWholeNumber(
          hold.validation_recent_provisional_difficulty
        )} settles near verified diff ${formatWholeNumber(hold.validation_recent_verified_difficulty)}`;
      }
      return 'auto-clears once backlog drains';
    case 'payout_coverage':
      return 'auto-clears once coverage recovers';
    default:
      return null;
  }
}


function rewardBlockOptionLabel(block: BlockItem): string {
  const status = block.orphaned ? 'orphaned' : block.confirmed ? 'confirmed' : 'pending';
  return `#${block.height} • ${status} • ${timeAgo(block.timestamp)}`;
}

function recoveryStateLabel(state: RecoveryInstanceStatus['state'] | undefined): string {
  switch (state) {
    case 'ready':
      return 'Ready';
    case 'syncing':
      return 'Syncing';
    case 'starting':
      return 'Starting';
    case 'failed':
      return 'Failed';
    case 'degraded':
      return 'Degraded';
    default:
      return 'Stopped';
  }
}

function recoveryStateBadgeClass(state: RecoveryInstanceStatus['state'] | undefined): string {
  switch (state) {
    case 'ready':
      return 'badge-confirmed';
    case 'syncing':
    case 'starting':
      return 'badge-pending';
    case 'failed':
    case 'degraded':
      return 'badge-orphaned';
    default:
      return 'badge-pending';
  }
}


function shareWindowReasonCount(window: AdminShareDiagnosticsWindow | null | undefined, reason: string): number {
  const target = reason.trim().toLowerCase();
  if (!window?.by_reason?.length || !target) return 0;
  const match = window.by_reason.find((item) => item.reason.trim().toLowerCase() === target);
  return match?.count ?? 0;
}

function shareWindowReasonPct(window: AdminShareDiagnosticsWindow | null | undefined, reason: string): number | null {
  if (!window) return null;
  const total = window.total ?? 0;
  if (total <= 0) return null;
  return (shareWindowReasonCount(window, reason) / total) * 100;
}


function recoveryInstanceLabel(instance: RecoveryInstanceId | null | undefined): string {
  switch (instance) {
    case 'primary':
      return 'Primary';
    case 'standby':
      return 'Standby';
    default:
      return 'Unknown';
  }
}

function otherRecoveryInstance(instance: RecoveryInstanceId | null | undefined): RecoveryInstanceId | null {
  switch (instance) {
    case 'primary':
      return 'standby';
    case 'standby':
      return 'primary';
    default:
      return null;
  }
}

function recoveryOperationLabel(kind: RecoveryOperationKind | null | undefined): string {
  switch (kind) {
    case 'pause_payouts':
      return 'Pause payouts';
    case 'resume_payouts':
      return 'Resume payouts';
    case 'start_standby_sync':
      return 'Start inactive sync';
    case 'rebuild_standby_wallet':
      return 'Rebuild inactive wallet';
    case 'cutover':
      return 'Cut over';
    case 'purge_inactive_daemon':
      return 'Purge inactive daemon';
    default:
      return 'Unknown operation';
  }
}

function recoveryOperationStateLabel(state: string | null | undefined): string {
  switch (state) {
    case 'running':
      return 'Running';
    case 'succeeded':
      return 'Succeeded';
    case 'failed':
      return 'Failed';
    case 'cancelled':
      return 'Cancelled';
    default:
      return 'Queued';
  }
}

function formatRecoveryWalletSync(item: RecoveryInstanceStatus | null): string {
  const syncedHeight = item?.wallet.synced_height;
  const chainHeight = item?.chain_height ?? item?.wallet.chain_height;
  if (syncedHeight == null && chainHeight == null) return '-';
  if (syncedHeight == null) return `- / ${chainHeight}`;
  if (chainHeight == null) return `${syncedHeight}`;
  return `${syncedHeight} / ${chainHeight}`;
}

function recoveryWalletLagLabel(item: RecoveryInstanceStatus | null): string | null {
  const syncedHeight = item?.wallet.synced_height;
  const chainHeight = item?.chain_height ?? item?.wallet.chain_height;
  if (syncedHeight == null || chainHeight == null) return null;
  if (syncedHeight >= chainHeight) return 'caught up';
  const lag = chainHeight - syncedHeight;
  return `${lag} blocks behind`;
}

function recoveryPendingDeltaNote(status: RecoveryStatusResponse | null): string | null {
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

interface AdminPageProps {
  active: boolean;
  api: ApiClient;
  liveTick: number;
  apiKey: string;
  apiKeyInput: string;
  setApiKeyInput: (value: string) => void;
  onSaveApiKey: () => void;
  onJumpToStats: (address: string) => void;
}

interface DaemonLogLine {
  id: number;
  segments: ParsedAnsiSegment[];
}

export function AdminPage({
  active,
  api,
  liveTick,
  apiKey,
  apiKeyInput,
  setApiKeyInput,
  onSaveApiKey,
  onJumpToStats,
}: AdminPageProps) {
  const [tab, setTab] = useState<AdminTab>('miners');

  const [minersSearch, setMinersSearch] = useState('');
  const [minersSort, setMinersSort] = useState('hashrate_desc');
  const [minersItems, setMinersItems] = useState<MinerListItem[]>([]);
  const [minersPager, setMinersPager] = useState<PagerState>({ offset: 0, limit: 25, total: 0 });


  const [rewardBlockInput, setRewardBlockInput] = useState('');
  const [rewardBlockOptions, setRewardBlockOptions] = useState<BlockItem[]>([]);
  const [rewardBlockOptionsLoading, setRewardBlockOptionsLoading] = useState(false);
  const [rewardAddressFilter, setRewardAddressFilter] = useState('');
  const [rewardBreakdown, setRewardBreakdown] = useState<BlockRewardBreakdownResponse | null>(null);
  const [rewardBreakdownLoading, setRewardBreakdownLoading] = useState(false);

  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [balanceOverview, setBalanceOverview] = useState<AdminBalanceOverviewResponse | null>(null);
  const [shareDiagnostics, setShareDiagnostics] = useState<AdminShareDiagnosticsResponse | null>(null);

  const [balancesSearch, setBalancesSearch] = useState('');
  const [balancesSort, setBalancesSort] = useState('pending_desc');
  const [balancesItems, setBalancesItems] = useState<AdminBalanceItem[]>([]);
  const [balancesPager, setBalancesPager] = useState<PagerState>({ offset: 0, limit: 50, total: 0 });

  const [recoveryStatus, setRecoveryStatus] = useState<RecoveryStatusResponse | null>(null);
  const [reconciliationIssues, setReconciliationIssues] = useState<AdminReconciliationIssuesResponse | null>(null);
  const [recoveryActionError, setRecoveryActionError] = useState('');
  const [reconciliationActionError, setReconciliationActionError] = useState('');
  const [recoveryBusy, setRecoveryBusy] = useState<RecoveryOperationKind | null>(null);
  const [reconciliationBusyKey, setReconciliationBusyKey] = useState<string | null>(null);
  const [holdActionError, setHoldActionError] = useState('');
  const [holdBusyAddress, setHoldBusyAddress] = useState<string | null>(null);

  const [daemonLogs, setDaemonLogs] = useState<DaemonLogLine[]>([]);
  const [daemonLogsTail, setDaemonLogsTail] = useState(200);
  const [daemonLogsStatus, setDaemonLogsStatus] = useState<'idle' | 'connecting' | 'live' | 'error'>('idle');
  const [daemonLogsError, setDaemonLogsError] = useState('');
  const [daemonLogsAutoScroll, setDaemonLogsAutoScroll] = useState(true);
  const [daemonLogsConnectSeq, setDaemonLogsConnectSeq] = useState(0);
  const daemonLogsRef = useRef<HTMLDivElement | null>(null);
  const daemonLogSeq = useRef(0);
  const rewardBreakdownRequestSeq = useRef(0);

  const loadMiners = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getMiners({
        paged: 'true',
        limit: minersPager.limit,
        offset: minersPager.offset,
        sort: minersSort,
        search: minersSearch.trim() || undefined,
      });
      const items = d.items || [];
      setMinersItems(items);
      setMinersPager((prev) => ({ ...prev, total: d.page ? d.page.total : items.length }));
    } catch {
      setMinersItems([]);
    }
  }, [api, apiKey, minersPager.limit, minersPager.offset, minersSearch, minersSort]);


  const loadRewardBreakdown = useCallback(
    async (heightOverride?: number | string) => {
      if (!apiKey) return;
      const raw = String(heightOverride ?? rewardBlockInput).trim();
      const height = Math.floor(Number(raw));
      if (!raw || !Number.isFinite(height) || height < 0) return;
      const requestSeq = rewardBreakdownRequestSeq.current + 1;
      rewardBreakdownRequestSeq.current = requestSeq;

      setRewardBlockInput(String(height));
      setRewardBreakdownLoading(true);
      try {
        const d = await api.getAdminBlockRewardBreakdown(height);
        if (rewardBreakdownRequestSeq.current !== requestSeq) return;
        setRewardBreakdown(d);
      } catch {
        // handled by api client
      } finally {
        if (rewardBreakdownRequestSeq.current !== requestSeq) return;
        setRewardBreakdownLoading(false);
      }
    },
    [api, apiKey, rewardBlockInput]
  );

  const loadRewardBlocks = useCallback(async () => {
    if (!apiKey) return;
    setRewardBlockOptionsLoading(true);
    try {
      const d = await api.getBlocks({
        paged: 'true',
        limit: 50,
        offset: 0,
        sort: 'height_desc',
      });
      const items = d.items || [];
      setRewardBlockOptions(items);
      setRewardBlockInput((prev) => {
        if (prev.trim() || !items.length) return prev;
        return String(items[0].height);
      });
    } catch {
      setRewardBlockOptions([]);
    } finally {
      setRewardBlockOptionsLoading(false);
    }
  }, [api, apiKey]);

  const loadHealth = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getHealth();
      setHealth(d);
    } catch {
      setHealth(null);
    }
  }, [api, apiKey]);

  const loadBalanceOverview = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getAdminBalanceOverview();
      setBalanceOverview(d);
    } catch {
      setBalanceOverview(null);
    }
  }, [api, apiKey]);

  const loadShareDiagnostics = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getAdminShareDiagnostics();
      setShareDiagnostics(d);
    } catch {
      setShareDiagnostics(null);
    }
  }, [api, apiKey]);

  const loadBalances = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getAdminBalances({
        paged: 'true',
        limit: balancesPager.limit,
        offset: balancesPager.offset,
        sort: balancesSort,
        search: balancesSearch.trim() || undefined,
      });
      const items = d.items || [];
      setBalancesItems(items);
      setBalancesPager((prev) => ({ ...prev, total: d.page ? d.page.total : items.length }));
    } catch {
      setBalancesItems([]);
    }
  }, [api, apiKey, balancesPager.limit, balancesPager.offset, balancesSearch, balancesSort]);

  const loadRecovery = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getRecoveryStatus();
      setRecoveryStatus(d);
    } catch {
      setRecoveryStatus(null);
    }
  }, [api, apiKey]);

  const loadReconciliationIssues = useCallback(async () => {
    if (!apiKey) return;
    try {
      const d = await api.getAdminReconciliationIssues();
      setReconciliationIssues(d);
    } catch {
      setReconciliationIssues(null);
    }
  }, [api, apiKey]);

  const runRecoveryAction = useCallback(
    async (kind: RecoveryOperationKind, fn: () => Promise<unknown>) => {
      setRecoveryActionError('');
      setRecoveryBusy(kind);
      try {
        await fn();
        await loadRecovery();
      } catch (err) {
        setRecoveryActionError(err instanceof Error ? err.message : 'recovery action failed');
      } finally {
        setRecoveryBusy(null);
      }
    },
    [loadRecovery]
  );

  const resolveReconciliationPayoutIssue = useCallback(
    async (txHash: string, action: ReconciliationPayoutResolutionAction) => {
      const trimmed = txHash.trim();
      if (!trimmed) return;
      const message =
        action === 'restore_pending'
          ? `Restore ${trimmed} back to miners' pending balances and remove it from paid history?`
          : `Drop ${trimmed} from paid history without restoring pending balances?`;
      if (!window.confirm(message)) {
        return;
      }

      setReconciliationActionError('');
      setReconciliationBusyKey(`payout:${trimmed}:${action}`);
      try {
        await api.resolveAdminReconciliationPayout(trimmed, action);
        await Promise.all([loadReconciliationIssues(), loadBalanceOverview(), loadHealth()]);
      } catch (err) {
        setReconciliationActionError(
          err instanceof Error ? err.message : 'failed resolving reconciliation payout issue'
        );
      } finally {
        setReconciliationBusyKey(null);
      }
    },
    [api, loadBalanceOverview, loadHealth, loadReconciliationIssues]
  );

  const retryOrphanedBlockCleanup = useCallback(
    async (blockHeight: number) => {
      if (!window.confirm(`Retry orphan-credit cleanup for block ${blockHeight}?`)) {
        return;
      }

      setReconciliationActionError('');
      setReconciliationBusyKey(`block:${blockHeight}`);
      try {
        await api.retryAdminOrphanedBlockCleanup(blockHeight);
        await Promise.all([loadReconciliationIssues(), loadBalanceOverview(), loadHealth()]);
      } catch (err) {
        setReconciliationActionError(
          err instanceof Error ? err.message : 'failed retrying orphaned block cleanup'
        );
      } finally {
        setReconciliationBusyKey(null);
      }
    },
    [api, loadBalanceOverview, loadHealth, loadReconciliationIssues]
  );

  const clearAddressRiskHistory = useCallback(
    async (address: string) => {
      const trimmed = address.trim();
      if (!trimmed) return;
      if (
        !window.confirm(
          `Clear all quarantine, force-verify, fraud, and validation hold history for ${trimmed}?`
        )
      ) {
        return;
      }

      setHoldActionError('');
      setHoldBusyAddress(trimmed);
      try {
        await api.clearAddressRiskHistory(trimmed);
        await loadHealth();
      } catch (err) {
        setHoldActionError(err instanceof Error ? err.message : 'failed clearing address risk history');
      } finally {
        setHoldBusyAddress(null);
      }
    },
    [api, loadHealth]
  );

  useEffect(() => {
    if (!active || !apiKey) return;

    // Always load overview data regardless of tab
    void loadHealth();
    void loadBalanceOverview();
    void loadShareDiagnostics();

    if (tab === 'miners') void loadMiners();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'balances') void loadBalances();
    if (tab === 'recovery') {
      void loadRecovery();
      void loadReconciliationIssues();
    }
  }, [
    active,
    apiKey,
    loadBalances,
    loadBalanceOverview,
    loadHealth,
    loadReconciliationIssues,
    loadShareDiagnostics,
    loadMiners,
    loadRecovery,
    loadRewardBlocks,
    loadRewardBreakdown,
    rewardBlockInput,
    tab,
  ]);

  useEffect(() => {
    if (!active || !apiKey || liveTick <= 0) return;

    // Always refresh overview data
    void loadHealth();
    void loadBalanceOverview();
    void loadShareDiagnostics();

    if (tab === 'miners') void loadMiners();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'balances') void loadBalances();
    if (tab === 'recovery') {
      void loadRecovery();
      void loadReconciliationIssues();
    }
  }, [
    active,
    apiKey,
    liveTick,
    tab,
    loadBalances,
    loadBalanceOverview,
    loadHealth,
    loadReconciliationIssues,
    loadShareDiagnostics,
    loadMiners,
    loadRecovery,
    loadRewardBlocks,
    loadRewardBreakdown,
    rewardBlockInput,
  ]);

  useEffect(() => {
    if (!active || !apiKey || tab !== 'logs') return;

    const controller = new AbortController();
    let stopped = false;
    let reconnectTimer: number | null = null;

    const waitForReconnect = () =>
      new Promise<void>((resolve) => {
        const finish = () => {
          controller.signal.removeEventListener('abort', finish);
          if (reconnectTimer != null) {
            window.clearTimeout(reconnectTimer);
            reconnectTimer = null;
          }
          resolve();
        };

        reconnectTimer = window.setTimeout(() => {
          finish();
        }, DAEMON_LOG_RECONNECT_DELAY_MS);
        controller.signal.addEventListener('abort', finish, { once: true });
      });

    const connect = async () => {
      while (!stopped && !controller.signal.aborted) {
        setDaemonLogs([]);
        setDaemonLogsStatus('connecting');
        setDaemonLogsError('');

        try {
          await api.streamDaemonLogs({
            tail: daemonLogsTail,
            signal: controller.signal,
            onLine: (line) => {
              setDaemonLogsStatus('live');
              setDaemonLogs((prev) => {
                const next = prev.concat({
                  id: daemonLogSeq.current++,
                  segments: parseAnsiLine(line),
                });
                if (next.length <= MAX_DAEMON_LOG_LINES) {
                  return next;
                }
                return next.slice(next.length - MAX_DAEMON_LOG_LINES);
              });
            },
          });
          if (stopped || controller.signal.aborted) return;
          setDaemonLogsStatus('connecting');
          await waitForReconnect();
        } catch (err) {
          if (stopped || controller.signal.aborted) return;
          if (err instanceof DOMException && err.name === 'AbortError') return;
          setDaemonLogsStatus('error');
          setDaemonLogsError(err instanceof Error ? err.message : 'failed to stream daemon logs');
          return;
        }
      }
    };

    void connect();
    return () => {
      stopped = true;
      controller.abort();
    };
  }, [active, api, apiKey, daemonLogsTail, daemonLogsConnectSeq, tab]);

  useEffect(() => {
    if (!daemonLogsAutoScroll || tab !== 'logs') return;
    const viewport = daemonLogsRef.current;
    if (!viewport) return;
    viewport.scrollTop = viewport.scrollHeight;
  }, [daemonLogs, daemonLogsAutoScroll, tab]);


  const daemonLogsStatusText =
    daemonLogsStatus === 'connecting'
      ? 'Connecting'
      : daemonLogsStatus === 'live'
        ? 'Live'
        : daemonLogsStatus === 'error'
          ? 'Error'
          : 'Idle';
  const daemonLogsStatusDot =
    daemonLogsStatus === 'live'
      ? 'dot-green'
      : daemonLogsStatus === 'error'
        ? 'dot-red'
        : 'dot-amber';
  const rewardLoadDisabled = !rewardBlockInput.trim() || !Number.isFinite(Number(rewardBlockInput.trim()));
  const rewardSelectedBlockValue = useMemo(() => {
    const selected = rewardBlockInput.trim();
    if (!selected) return '';
    return rewardBlockOptions.some((block) => String(block.height) === selected) ? selected : '';
  }, [rewardBlockInput, rewardBlockOptions]);
  const filteredRewardParticipants = useMemo(() => {
    const items = rewardBreakdown?.participants || [];
    const filter = rewardAddressFilter.trim().toLowerCase();
    if (!filter) return items;
    return items.filter((row) => row.address.toLowerCase().includes(filter));
  }, [rewardAddressFilter, rewardBreakdown?.participants]);
  const rewardBreakdownTotals = useMemo(() => {
    if (!rewardBreakdown) return null;
    return rewardBreakdown.participants.reduce(
      (totals, row) => {
        totals.previewCredit += row.preview_credit;
        totals.payoutCredit += row.payout_credit;
        totals.verifiedDifficulty += row.verified_difficulty;
        totals.provisionalEligibleDifficulty += row.provisional_difficulty_eligible;
        return totals;
      },
      {
        previewCredit: 0,
        payoutCredit: 0,
        verifiedDifficulty: 0,
        provisionalEligibleDifficulty: 0,
      }
    );
  }, [rewardBreakdown]);
  const rewardBreakdownOrphaned = rewardBreakdown?.block.orphaned ?? false;
  const rewardBreakdownPaidOut = rewardBreakdown?.block.paid_out ?? false;
  const rewardBreakdownProjected = !!rewardBreakdown && !rewardBreakdownOrphaned && !rewardBreakdown.block.paid_out;
  const activeVerificationHolds = health?.active_verification_holds ?? [];
  const riskVerificationHolds = useMemo(
    () => activeVerificationHolds.filter((hold) => isRiskVerificationHold(hold)),
    [activeVerificationHolds]
  );
  const temporaryValidationHolds = useMemo(
    () => activeVerificationHolds.filter((hold) => isTemporaryValidationAssist(hold)),
    [activeVerificationHolds]
  );
  const poolActivity = health?.pool_activity ?? null;
  const unpaidPayoutCount =
    balanceOverview?.payouts.unpaid_count ?? health?.payouts?.unpaid_count ?? health?.payouts?.pending_count ?? null;
  const unpaidPayoutAmount =
    balanceOverview?.payouts.unpaid_amount ?? health?.payouts?.unpaid_amount ?? health?.payouts?.pending_amount ?? null;
  const cleanPayableCount = balanceOverview?.payouts.clean_unpaid_count ?? null;
  const cleanPayableAmount = balanceOverview?.payouts.clean_unpaid_amount ?? null;
  const orphanBackedPendingAmount = balanceOverview?.payouts.orphan_backed_unpaid_amount ?? null;
  const balanceSourceDriftAmount = balanceOverview?.payouts.balance_source_drift_amount ?? null;
  const queuedPayoutCount =
    balanceOverview?.payouts.queued_count ?? health?.payouts?.queued_count ?? health?.payouts?.pending_count ?? null;
  const queuedPayoutAmount =
    balanceOverview?.payouts.queued_amount ?? health?.payouts?.queued_amount ?? health?.payouts?.pending_amount ?? null;
  const poolFeePendingAmount = balanceOverview?.payouts.pool_fee_unpaid_amount ?? null;
  const poolFeeCleanPendingAmount = balanceOverview?.payouts.pool_fee_clean_unpaid_amount ?? null;
  const poolFeeOrphanPendingAmount =
    balanceOverview?.payouts.pool_fee_orphan_backed_unpaid_amount ?? null;
  const poolFeeBalanceSourceDriftAmount =
    balanceOverview?.payouts.pool_fee_balance_source_drift_amount ?? null;
  const minerFundingGapAmount = balanceOverview && cleanPayableAmount != null
    ? Math.max(cleanPayableAmount - balanceOverview.wallet.total, 0)
    : null;
  const minerWalletSurplusAmount = balanceOverview && cleanPayableAmount != null
    ? Math.max(balanceOverview.wallet.total - cleanPayableAmount, 0)
    : null;
  const canonicalMinerRewardTotal = balanceOverview
    ? Math.max(balanceOverview.ledger.net_block_reward_total - balanceOverview.ledger.pool_fee_total, 0)
    : null;
  const canonicalBackedPending = balanceOverview && canonicalMinerRewardTotal != null
    ? Math.max(canonicalMinerRewardTotal - balanceOverview.ledger.miner_paid_total, 0)
    : null;
  const ledgerOverhangAmount = balanceOverview && canonicalMinerRewardTotal != null
    ? Math.max(balanceOverview.ledger.miner_total_credited - canonicalMinerRewardTotal, 0)
    : null;
  const ledgerShortfallAmount = balanceOverview && canonicalMinerRewardTotal != null
    ? Math.max(canonicalMinerRewardTotal - balanceOverview.ledger.miner_total_credited, 0)
    : null;
  const acknowledgedHistoricalShortfallAmount = ledgerShortfallAmount != null
    ? Math.min(ledgerShortfallAmount, ACKNOWLEDGED_LAUNCH_ERA_MINER_SHORTFALL)
    : null;
  const unresolvedLedgerShortfallAmount =
    ledgerShortfallAmount != null && acknowledgedHistoricalShortfallAmount != null
      ? Math.max(ledgerShortfallAmount - acknowledgedHistoricalShortfallAmount, 0)
      : null;
  const hasUnresolvedLedgerMismatch =
    (ledgerOverhangAmount ?? 0) > 0 || (unresolvedLedgerShortfallAmount ?? 0) > 0;
  const balanceDiagnosticsCount = [
    orphanBackedPendingAmount != null && orphanBackedPendingAmount > 0,
    balanceSourceDriftAmount != null && balanceSourceDriftAmount > 0,
    poolFeeOrphanPendingAmount != null && poolFeeOrphanPendingAmount > 0,
    poolFeeBalanceSourceDriftAmount != null && poolFeeBalanceSourceDriftAmount > 0,
    hasUnresolvedLedgerMismatch,
  ].filter(Boolean).length;
  const balanceDiagnosticsSummary = balanceDiagnosticsCount > 0
    ? `${balanceDiagnosticsCount} diagnostic${balanceDiagnosticsCount === 1 ? '' : 's'} in Balances tab`
    : null;
  const shareWindows = shareDiagnostics?.windows ?? [];
  const shareWindow5m = useMemo(
    () => shareWindows.find((item) => item.label === '5m') ?? null,
    [shareWindows]
  );
  const shareWindow1h = useMemo(
    () => shareWindows.find((item) => item.label === '1h') ?? null,
    [shareWindows]
  );
  const shareWindow24h = useMemo(
    () => shareWindows.find((item) => item.label === '24h') ?? null,
    [shareWindows]
  );
  const shareSubmit = shareDiagnostics?.submit ?? null;
  const shareValidation = shareDiagnostics?.validation ?? null;

  const shareSubmitQueueDepth =
    (shareSubmit?.candidate_queue_depth ?? 0) + (shareSubmit?.regular_queue_depth ?? 0);
  const shareValidationQueueDepth =
    (shareValidation?.candidate_queue_depth ?? 0) + (shareValidation?.regular_queue_depth ?? 0);
  const shareAuditQueueDepth = shareValidation?.audit_queue_depth ?? 0;
  const shareSubmitOldestAge = Math.max(
    shareSubmit?.candidate_oldest_age_millis ?? 0,
    shareSubmit?.regular_oldest_age_millis ?? 0
  );
  const shareValidationOldestAge = Math.max(
    shareValidation?.candidate_oldest_age_millis ?? 0,
    shareValidation?.regular_oldest_age_millis ?? 0
  );
  const shareAuditOldestAge = shareValidation?.audit_oldest_age_millis ?? 0;
  const shareSubmitWaitP95 = Math.max(
    shareSubmit?.candidate_wait?.p95_millis ?? 0,
    shareSubmit?.regular_wait?.p95_millis ?? 0
  );
  const shareValidationWaitP95 = Math.max(
    shareValidation?.candidate_wait?.p95_millis ?? 0,
    shareValidation?.regular_wait?.p95_millis ?? 0
  );
  const shareValidationDurationP95 = shareValidation?.validation_duration?.p95_millis ?? 0;
  const shareAuditWaitP95 = shareValidation?.audit_wait?.p95_millis ?? 0;
  const shareAuditDurationP95 = shareValidation?.audit_duration?.p95_millis ?? 0;
  const shareBusy5m = shareWindowReasonPct(shareWindow5m, 'server busy');
  const shareTimeout5m = shareWindowReasonPct(shareWindow5m, 'validation timeout');
  const shareBusyCount5m = shareWindowReasonCount(shareWindow5m, 'server busy');
  const shareTimeoutCount5m = shareWindowReasonCount(shareWindow5m, 'validation timeout');
  const shareInvalidProof5m = shareWindowReasonPct(shareWindow5m, 'invalid share proof') ?? 0;
  const shareHotAccepts = shareValidation?.hot_accepts ?? 0;
  const shareSyncFullVerifies = shareValidation?.sync_full_verifies ?? 0;
  const shareHotPathBackedUp =
    shareBusyCount5m + shareTimeoutCount5m > 0 ||
    shareSubmitQueueDepth > 0 ||
    shareValidationQueueDepth > 0 ||
    shareSubmitOldestAge >= 2000 ||
    shareValidationOldestAge >= 2000;
  const shareHotPathRecentlySlow =
    shareSubmitWaitP95 >= HOT_PATH_LATENCY_SPIKE_MILLIS ||
    shareValidationWaitP95 >= HOT_PATH_LATENCY_SPIKE_MILLIS;
  const shareActiveTopReject =
    shareWindow5m?.by_reason?.[0] ?? shareWindow1h?.by_reason?.[0] ?? shareWindow24h?.by_reason?.[0] ?? null;
  const sharePressureSignal = useMemo(() => {
    if (!shareDiagnostics) {
      return {
        label: 'No data',
        detail: 'Waiting for runtime diagnostics from the pool.',
        tone: 'var(--muted)',
      };
    }
    if (shareValidation?.overload_mode === 'emergency' && shareHotPathBackedUp) {
      return {
        label: 'Emergency shed',
        detail: 'Regular shares are being admitted with minimal verification because the regular share pipeline is severely backed up.',
        tone: 'var(--warn)',
      };
    }
    if (shareValidation?.overload_mode === 'emergency') {
      return {
        label: 'Recovered recently',
        detail: 'The pool recently entered overload protection, but the live submit and validation queues are draining again.',
        tone: 'var(--warn)',
      };
    }
    if (shareValidation?.overload_mode === 'shed' && shareHotPathBackedUp) {
      return {
        label: 'Shedding',
        detail: 'Sample rate is being reduced because the regular share pipeline is backing up.',
        tone: 'var(--warn)',
      };
    }
    if (shareValidation?.overload_mode === 'shed') {
      return {
        label: 'Recovered recently',
        detail: 'The pool recently reduced sampling because of queue pressure, but the live queue is clear now.',
        tone: 'var(--warn)',
      };
    }
    if (shareHotPathBackedUp) {
      return {
        label: 'Queue pressure',
        detail: 'Backlog is visible even though overload shedding has not fully tripped yet.',
        tone: 'var(--warn)',
      };
    }
    if (shareHotPathRecentlySlow) {
      return {
        label: 'Recently slowed',
        detail: 'The live queue is clear, but recent submit or validation waits were elevated.',
        tone: 'var(--warn)',
      };
    }
    return {
      label: 'Normal',
      detail: 'Submit and validation queues are draining without overload symptoms.',
      tone: 'var(--good)',
    };
  }, [
    shareBusyCount5m,
    shareDiagnostics,
    shareHotPathBackedUp,
    shareHotPathRecentlySlow,
    shareValidation?.overload_mode,
    shareTimeoutCount5m,
  ]);
  const shareHotPathFocus = useMemo(() => {
    if (!shareDiagnostics) {
      return {
        tone: 'warn' as const,
        title: 'Waiting for data',
        detail: 'Share runtime diagnostics have not loaded yet.',
      };
    }
    if (
      shareHotPathBackedUp &&
      (shareValidation?.overload_mode === 'emergency' ||
        shareBusyCount5m + shareTimeoutCount5m > 0 ||
        shareSubmitWaitP95 >= HOT_PATH_LATENCY_SPIKE_MILLIS ||
        shareValidationWaitP95 >= HOT_PATH_LATENCY_SPIKE_MILLIS)
    ) {
      return {
        tone: 'critical' as const,
        title: 'Hot path is under stress',
        detail: `Miners are waiting on submit acknowledgements. Submit ${formatMillis(
          shareSubmitWaitP95
        )} · validation ${formatMillis(shareValidationWaitP95)}.`,
      };
    }
    if (shareHotPathRecentlySlow) {
      return {
        tone: 'warn' as const,
        title: 'Hot path recently spiked',
        detail: `Queues have drained, but recent waits were elevated. Submit ${formatMillis(
          shareSubmitWaitP95
        )} · validation ${formatMillis(shareValidationWaitP95)}.`,
      };
    }
    if (
      shareValidation?.overload_mode === 'shed' ||
      shareHotPathBackedUp ||
      shareSubmitWaitP95 >= HOT_PATH_LATENCY_WARN_MILLIS ||
      shareValidationWaitP95 >= HOT_PATH_LATENCY_WARN_MILLIS
    ) {
      return {
        tone: 'warn' as const,
        title: 'Watch submit latency',
        detail: `Hot accepts are still moving, but latency is visible. Submit ${formatMillis(
          shareSubmitWaitP95
        )} · validation ${formatMillis(shareValidationWaitP95)}.`,
      };
    }
    return {
      tone: 'ok' as const,
      title: 'Hot path is healthy',
      detail: 'New shares are being admitted without queue pressure.',
    };
  }, [
    shareBusyCount5m,
    shareDiagnostics,
    shareHotPathBackedUp,
    shareHotPathRecentlySlow,
    shareSubmitWaitP95,
    shareTimeoutCount5m,
    shareValidation?.overload_mode,
    shareValidationWaitP95,
  ]);
  const shareAuditFocus = useMemo(() => {
    if (!shareDiagnostics) {
      return {
        state: 'waiting' as const,
        tone: 'warn' as const,
        title: 'Waiting for data',
        detail: 'Audit timing has not loaded yet.',
        badge: 'Loading',
      };
    }
    if (shareValidation?.audit_deferred || shareAuditQueueDepth >= 25 || shareAuditWaitP95 >= 30000) {
      return {
        state: 'critical' as const,
        tone: 'critical' as const,
        title: 'Audit is falling behind',
        detail: `Coverage work is lagging. Queue ${shareAuditQueueDepth} · wait ${formatMillis(
          shareAuditWaitP95
        )} · deferred ${shareValidation?.audit_deferred ?? 0}.`,
        badge: 'Lagging',
      };
    }
    if (shareAuditQueueDepth >= 10 || (shareAuditQueueDepth > 0 && shareAuditWaitP95 >= 15000)) {
      return {
        state: 'trailing' as const,
        tone: 'warn' as const,
        title: 'Audit is trailing',
        detail: `Background verification is catching up. Queue ${shareAuditQueueDepth} · wait ${formatMillis(
          shareAuditWaitP95
        )}.`,
        badge: 'Trailing',
      };
    }
    if (shareAuditWaitP95 >= 10000) {
      return {
        state: 'recent' as const,
        tone: 'ok' as const,
        title: 'Audit recently lagged',
        detail: `The recent p95 wait reached ${formatMillis(
          shareAuditWaitP95
        )}, but the queue is drained now.`,
        badge: 'Recovered',
      };
    }
    return {
      state: 'ok' as const,
      tone: 'ok' as const,
      title: 'Audit is keeping up',
      detail: 'Background verification is draining normally.',
      badge: 'Keeping up',
    };
  }, [shareAuditQueueDepth, shareAuditWaitP95, shareDiagnostics, shareValidation?.audit_deferred]);
  const shareRejectFocus = useMemo(() => {
    const recentRejectPct = shareWindow5m?.rejection_rate_pct ?? 0;
    const hourlyRejectPct = shareWindow1h?.rejection_rate_pct ?? 0;
    const dailyRejectPct = shareWindow24h?.rejection_rate_pct ?? 0;
    const recentRejectCount = shareWindow5m?.rejected ?? 0;
    const hourlyRejectCount = shareWindow1h?.rejected ?? 0;
    const dailyRejectCount = shareWindow24h?.rejected ?? 0;
    const topReason = shareActiveTopReject ? `${shareActiveTopReject.reason} (${shareActiveTopReject.count})` : 'none';
    if (!shareDiagnostics) {
      return {
        tone: 'warn' as const,
        title: 'Waiting for data',
        detail: 'Reject telemetry has not loaded yet.',
      };
    }
    if (
      recentRejectPct >= 5 ||
      shareInvalidProof5m >= 1 ||
      shareBusyCount5m + shareTimeoutCount5m >= 5
    ) {
      return {
        tone: 'critical' as const,
        title: 'Rejects need attention',
        detail: `5m reject ${pct(recentRejectPct)}. Top reject: ${topReason}.`,
      };
    }
    const meaningfulRecentRejects = recentRejectCount >= 3 || recentRejectPct >= 0.5;
    const meaningfulHourlyRejects = hourlyRejectCount >= 10 || hourlyRejectPct >= 0.5;
    const meaningfulDailyRejects = dailyRejectCount >= 50 || dailyRejectPct >= 1.0;
    if (meaningfulRecentRejects || meaningfulHourlyRejects || meaningfulDailyRejects) {
      return {
        tone: 'warn' as const,
        title: 'Watch rejects',
        detail: `5m ${pct(recentRejectPct)} · 1h ${pct(hourlyRejectPct)} · 24h ${pct(dailyRejectPct)}. Top reject: ${topReason}.`,
      };
    }
    return {
      tone: 'ok' as const,
      title: 'Rejects are clean',
      detail: `No recent reject spike. 24h reject rate is ${pct(dailyRejectPct)}.`,
    };
  }, [
    shareActiveTopReject,
    shareBusyCount5m,
    shareDiagnostics,
    shareWindow1h?.rejected,
    shareWindow24h?.rejected,
    shareWindow5m?.rejected,
    shareInvalidProof5m,
    shareTimeoutCount5m,
    shareWindow1h?.rejection_rate_pct,
    shareWindow24h?.rejection_rate_pct,
    shareWindow5m?.rejection_rate_pct,
  ]);
  const shareOperatorFocus = useMemo(() => {
    if (!shareDiagnostics) {
      return {
        tone: 'warn' as const,
        label: 'Loading',
        title: 'Waiting for share diagnostics',
        detail: 'The pool has not published the latest share runtime snapshot yet.',
        next: 'If this persists, check the API and Stratum runtime snapshots.',
      };
    }
    if (shareHotPathFocus.tone === 'critical') {
      return {
        tone: 'critical' as const,
        label: 'Act now',
        title: 'Protect the hot path',
        detail: 'Miners are feeling submit or validation latency right now. Fix this before worrying about payout coverage.',
        next: 'Watch busy/timeout rejects, submit wait p95, and validation wait p95.',
      };
    }
    if (shareRejectFocus.tone === 'critical') {
      return {
        tone: 'critical' as const,
        label: 'Act now',
        title: 'Investigate the reject spike',
        detail: 'Recent rejects are elevated enough to affect miners directly.',
        next: 'Start with the top reject reason and the 5m window.',
      };
    }
    if (shareHotPathFocus.tone === 'warn') {
      return {
        tone: 'warn' as const,
        label: 'Watch',
        title: 'Submit latency is the main risk',
        detail: 'The hot path is still working, but it is the first thing that could degrade if load rises.',
        next: 'Keep submit and validation waits close to zero.',
      };
    }
    if (shareAuditFocus.state === 'critical' || shareAuditFocus.state === 'trailing') {
      return {
        tone: 'warn' as const,
        label: 'Watch',
        title: 'Background audit is the main follow-up',
        detail: 'Mining is healthy, but payout coverage work is trailing and may need more headroom.',
        next: 'Watch audit queue depth and audit wait p95.',
      };
    }
    if (shareRejectFocus.tone === 'warn') {
      return {
        tone: 'warn' as const,
        label: 'Watch',
        title: 'Mining is healthy, but rejects deserve a look',
        detail: 'There is no hot-path pressure, but rejects are the main thing worth understanding next.',
        next: 'Compare 5m, 1h, and 24h windows to see whether this is active or historical.',
      };
    }
    return {
      tone: 'ok' as const,
      label: 'Healthy',
      title: 'Nothing urgent',
      detail: 'The hot path is healthy, rejects are quiet, and background verification is keeping up.',
      next: 'Only dig into advanced diagnostics if payouts look unfair or a miner reports lag.',
    };
  }, [shareAuditFocus.state, shareDiagnostics, shareHotPathFocus.tone, shareRejectFocus.tone]);
  const recoveryPrimary = useMemo(
    () => recoveryStatus?.instances.find((item) => item.instance === 'primary') ?? null,
    [recoveryStatus]
  );
  const recoveryStandby = useMemo(
    () => recoveryStatus?.instances.find((item) => item.instance === 'standby') ?? null,
    [recoveryStatus]
  );
  const recoveryActiveInstance = recoveryStatus?.active_instance ?? null;
  const recoveryInactiveInstance = otherRecoveryInstance(recoveryActiveInstance);
  const recoveryInactiveStatus =
    recoveryInactiveInstance === 'primary'
      ? recoveryPrimary
      : recoveryInactiveInstance === 'standby'
        ? recoveryStandby
        : null;
  const recoveryPendingNote = useMemo(() => recoveryPendingDeltaNote(recoveryStatus), [recoveryStatus]);
  const recoveryInactiveLabel =
    recoveryInactiveInstance == null ? 'Inactive' : recoveryInstanceLabel(recoveryInactiveInstance);
  const recoveryLatestOperation = recoveryStatus?.operations?.[0] ?? null;
  const recoveryRunningOperation = useMemo(
    () => recoveryStatus?.operations.find((item) => item.state === 'queued' || item.state === 'running') ?? null,
    [recoveryStatus]
  );
  const recoveryBusyReason = useMemo(() => {
    if (recoveryBusy) return `${recoveryOperationLabel(recoveryBusy)} is already running`;
    if (recoveryRunningOperation) {
      return `${recoveryOperationLabel(recoveryRunningOperation.kind)} is already running`;
    }
    return null;
  }, [recoveryBusy, recoveryRunningOperation]);
  const recoveryActionState = useMemo(() => {
    const statusMissing = 'Recovery status is still loading';
    const routingMissing = 'Active daemon routing is not provisioned yet';
    const standbyLabel = recoveryInstanceLabel('standby');
    const primaryLabel = recoveryInstanceLabel('primary');
    const payoutsPaused = recoveryStatus?.payouts_paused ?? false;

    const buttonState = (reason: string | null) => ({
      disabled: reason != null,
      title: reason ?? undefined,
    });

    const pauseReason =
      recoveryBusyReason ??
      (recoveryStatus == null ? statusMissing : recoveryStatus.payouts_paused ? 'Payouts are already paused' : null);
    const resumeReason =
      recoveryBusyReason ??
      (recoveryStatus == null ? statusMissing : !recoveryStatus.payouts_paused ? 'Payouts are already live' : null);

    let startInactiveSyncReason = recoveryBusyReason;
    if (startInactiveSyncReason == null) {
      if (recoveryStatus == null) {
        startInactiveSyncReason = statusMissing;
      } else if (recoveryInactiveInstance == null) {
        startInactiveSyncReason = routingMissing;
      } else if (recoveryInactiveStatus == null) {
        startInactiveSyncReason = `${recoveryInactiveLabel} status is unavailable`;
      } else if (recoveryInactiveStatus.state === 'starting') {
        startInactiveSyncReason = `${recoveryInactiveLabel} is already starting`;
      } else if (recoveryInactiveStatus.syncing) {
        startInactiveSyncReason = `${recoveryInactiveLabel} is already syncing`;
      } else if (recoveryInactiveStatus.reachable) {
        startInactiveSyncReason = `${recoveryInactiveLabel} daemon is already running`;
      }
    }

    let rebuildInactiveReason = recoveryBusyReason;
    if (rebuildInactiveReason == null) {
      if (recoveryStatus == null) {
        rebuildInactiveReason = statusMissing;
      } else if (recoveryInactiveInstance == null) {
        rebuildInactiveReason = routingMissing;
      } else if (!recoveryStatus.secret_configured) {
        rebuildInactiveReason = 'Configure the wallet secret before rebuilding the inactive wallet';
      } else if (recoveryInactiveStatus == null) {
        rebuildInactiveReason = `${recoveryInactiveLabel} status is unavailable`;
      } else if (recoveryInactiveStatus.state === 'starting') {
        rebuildInactiveReason = `${recoveryInactiveLabel} daemon is still starting`;
      } else if (!recoveryInactiveStatus.reachable) {
        rebuildInactiveReason = `${recoveryInactiveLabel} daemon is not reachable`;
      } else if (recoveryInactiveStatus.syncing) {
        rebuildInactiveReason = `${recoveryInactiveLabel} daemon is still syncing`;
      }
    }

    const cutoverReason = (
      target: RecoveryInstanceId,
      targetStatus: RecoveryInstanceStatus | null,
      targetLabel: string
    ): string | null => {
      if (recoveryBusyReason) return recoveryBusyReason;
      if (recoveryStatus == null) return statusMissing;
      if (!payoutsPaused) return 'Pause payouts before cutting over daemons';
      if (recoveryActiveInstance == null) return routingMissing;
      if (recoveryActiveInstance === target) return `${targetLabel} is already active`;
      if (targetStatus == null) return `${targetLabel} status is unavailable`;
      if (!targetStatus.reachable) return `${targetLabel} daemon is not reachable`;
      if (targetStatus.syncing) return `${targetLabel} daemon is still syncing`;
      if (!targetStatus.wallet.loaded) return `${targetLabel} wallet is not loaded`;
      if (!targetStatus.cookie_present) return `${targetLabel} cookie is missing`;
      return null;
    };

    const purgeReason =
      recoveryBusyReason ??
      (recoveryStatus == null
        ? statusMissing
        : recoveryActiveInstance == null
          ? routingMissing
          : null);

    return {
      pause: buttonState(pauseReason),
      resume: buttonState(resumeReason),
      startInactiveSync: buttonState(startInactiveSyncReason),
      rebuildInactiveWallet: buttonState(rebuildInactiveReason),
      cutoverStandby: buttonState(cutoverReason('standby', recoveryStandby, standbyLabel)),
      cutoverPrimary: buttonState(cutoverReason('primary', recoveryPrimary, primaryLabel)),
      purgeInactiveDaemon: buttonState(purgeReason),
    };
  }, [
    recoveryActiveInstance,
    recoveryBusyReason,
    recoveryInactiveInstance,
    recoveryInactiveLabel,
    recoveryInactiveStatus,
    recoveryPrimary,
    recoveryStandby,
    recoveryStatus,
  ]);
  const rewardActualBlockTotal =
    rewardBreakdown && rewardBreakdown.actual_credit_events_available && rewardBreakdown.actual_fee_amount != null
      ? rewardBreakdown.actual_credit_total + rewardBreakdown.actual_fee_amount
      : null;
  const rewardFeeDelta =
    rewardBreakdown?.actual_fee_amount != null ? rewardBreakdown.actual_fee_amount - rewardBreakdown.fee_amount : null;
  const rewardProjectedBlockTotal =
    rewardBreakdown && rewardBreakdownTotals
      ? rewardBreakdownTotals.payoutCredit + rewardBreakdown.fee_amount
      : null;
  const rewardActualBlockDelta =
    rewardActualBlockTotal != null && rewardProjectedBlockTotal != null
      ? rewardActualBlockTotal - rewardProjectedBlockTotal
      : null;
  const rewardPreviewColumnLabel = rewardBreakdownOrphaned ? 'Preview Estimate' : 'Preview';
  const rewardPayoutColumnLabel = rewardBreakdownProjected
    ? 'Projected Payout'
    : rewardBreakdownPaidOut
      ? 'Current Recompute'
      : 'Payout';
  const rewardActualColumnLabel = rewardBreakdownPaidOut ? 'Recorded Payout' : 'Actual';
  const rewardDeltaColumnLabel = rewardBreakdownPaidOut ? 'Delta vs Recompute' : 'Delta';
  const rewardPayoutWeightLabel = rewardBreakdownProjected
    ? 'Projected Weight'
    : rewardBreakdownPaidOut
      ? 'Current Recompute Weight'
      : 'Payout Weight';
  const rewardStatusColumnLabel = rewardBreakdownOrphaned
    ? 'Resolution'
    : rewardBreakdownPaidOut
      ? 'Notes'
      : rewardBreakdownProjected
      ? 'Projected Status'
      : 'Status';

  return (
    <div className={active ? 'page active' : 'page'} id="page-admin">
      <h2>Admin</h2>

      {!apiKey ? (
        <div className="auth-gate card section">
          <p style={{ marginBottom: 12 }}>Enter your API key to access admin features.</p>
          <div className="admin-key-bar">
            <input
              type="password"
              placeholder="API key"
              value={apiKeyInput}
              onChange={(e) => setApiKeyInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') onSaveApiKey();
              }}
            />
            <button className="btn btn-primary" onClick={onSaveApiKey}>
              Save
            </button>
          </div>
        </div>
      ) : (
        <div id="admin-content">
          <div className="stats-grid stats-grid-dense admin-overview-strip">
            <div
              className="stat-card"
              style={riskVerificationHolds.length > 0 ? { borderColor: 'var(--warn)' } : undefined}
              onClick={() => setTab('holds')}
            >
              <div className="label">Risk Holds</div>
              <div
                className="value mono"
                style={riskVerificationHolds.length > 0 ? { color: 'var(--warn)' } : undefined}
              >
                {riskVerificationHolds.length}
              </div>
              <div className="stat-meta">
                {riskVerificationHolds.length > 0 ? 'quarantine or risk review active' : 'no risky miners'}
              </div>
              <div className="stat-meta">
                {temporaryValidationHolds.length > 0
                  ? `${temporaryValidationHolds.length} temporary validation assist${
                      temporaryValidationHolds.length === 1 ? '' : 's'
                    }`
                  : 'no temporary assists'}
              </div>
            </div>
            <div className="stat-card" onClick={() => setTab('shares')}>
              <div className="label">5m Reject Rate</div>
              <div
                className="value mono"
                style={
                  (shareWindow5m?.rejection_rate_pct ?? 0) >= 5 ? { color: 'var(--warn)' } : undefined
                }
              >
                {pct(shareWindow5m?.rejection_rate_pct)}
              </div>
              <div className="stat-meta">
                {shareWindow5m?.rejected ?? 0} rejected of {shareWindow5m?.total ?? 0}
              </div>
            </div>
            <div className="stat-card" onClick={() => setTab('balances')}>
              <div className="label">Clean Payable</div>
              <div className="value mono">
                {cleanPayableAmount != null ? formatCompactCoins(cleanPayableAmount) : '-'}
              </div>
              <div className="stat-meta">
                {cleanPayableCount != null
                    ? `${cleanPayableCount} miner${cleanPayableCount === 1 ? '' : 's'} with payable balances`
                    : unpaidPayoutCount != null
                      ? `${unpaidPayoutCount} miner${unpaidPayoutCount === 1 ? '' : 's'} with payable balances`
                      : '-'}
              </div>
              {minerFundingGapAmount != null && (
                <div
                  className="stat-meta"
                  style={minerFundingGapAmount > 0 ? { color: 'var(--warn)' } : { color: 'var(--good)' }}
                >
                  {minerFundingGapAmount > 0
                    ? `${formatCoins(minerFundingGapAmount)} wallet gap vs clean miner liability`
                    : 'Wallet total covers clean miner liability'}
                </div>
              )}
              {poolFeePendingAmount != null && poolFeePendingAmount > 0 && (
                <div className="stat-meta">
                  {poolFeeCleanPendingAmount != null
                    ? `${formatCoins(poolFeeCleanPendingAmount)} internal pool fee tracked separately`
                    : `${formatCoins(poolFeePendingAmount)} internal pool fee tracked separately`}
                </div>
              )}
              {balanceDiagnosticsSummary && (
                <div className="stat-meta" style={{ color: 'var(--warn)' }}>
                  {balanceDiagnosticsSummary}
                </div>
              )}
              <div className="stat-meta">
                {queuedPayoutAmount != null
                  ? `${queuedPayoutCount ?? 0} queued · ${formatCoins(queuedPayoutAmount)} in queue`
                  : '-'}
              </div>
            </div>
            <div className="stat-card" onClick={() => setTab('balances')}>
              <div className="label">Wallet</div>
              <div className="value mono">
                {balanceOverview ? formatCompactCoins(balanceOverview.wallet.spendable) : '-'}
              </div>
              <div className="stat-meta">
                {balanceOverview
                  ? `${formatCompactCoins(balanceOverview.wallet.total)} total`
                  : '-'}
              </div>
              {minerFundingGapAmount != null && minerFundingGapAmount > 0 && (
                <div className="stat-meta" style={{ color: 'var(--warn)' }}>
                  {`${formatCompactCoins(minerFundingGapAmount)} still needed for clean miners`}
                </div>
              )}
              {minerFundingGapAmount === 0 && minerWalletSurplusAmount != null && (
                <div className="stat-meta" style={{ color: 'var(--good)' }}>
                  {`${formatCompactCoins(minerWalletSurplusAmount)} above clean miner liability`}
                </div>
              )}
              {balanceOverview && hasUnresolvedLedgerMismatch && (
                <div className="stat-meta" style={{ color: 'var(--warn)' }}>
                  Ledger diagnostics in Balances tab
                </div>
              )}
              {balanceOverview &&
                !hasUnresolvedLedgerMismatch &&
                (acknowledgedHistoricalShortfallAmount ?? 0) > 0 && (
                  <div className="stat-meta">Launch-era baseline acknowledged</div>
                )}
            </div>
            <div className="stat-card" onClick={() => setTab('miners')}>
              <div className="label">Connected Miners</div>
              <div className="value mono">{poolActivity?.connected_miners ?? '-'}</div>
              <div className="stat-meta">
                {poolActivity ? humanRate(poolActivity.estimated_hashrate) : '-'}
              </div>
            </div>
          </div>

          <div className="sub-tabs" id="admin-tabs">
            <button className={tab === 'miners' ? 'active' : ''} onClick={() => setTab('miners')}>
              Miners
            </button>
            <button className={tab === 'holds' ? 'active' : ''} onClick={() => setTab('holds')}>
              Holds
            </button>
            <button className={tab === 'balances' ? 'active' : ''} onClick={() => setTab('balances')}>
              Balances
            </button>
            <button className={tab === 'shares' ? 'active' : ''} onClick={() => setTab('shares')}>
              Shares
            </button>
            <button className={tab === 'rewards' ? 'active' : ''} onClick={() => setTab('rewards')}>
              Rewards
            </button>
            <span className="sub-tabs-divider" />
            <button className={tab === 'recovery' ? 'active' : ''} onClick={() => setTab('recovery')}>
              Recovery
            </button>
            <button className={tab === 'logs' ? 'active' : ''} onClick={() => setTab('logs')}>
              Daemon Logs
            </button>
          </div>

          <div style={{ display: tab === 'miners' ? '' : 'none' }}>
            <div className="filter-bar">
              <input
                type="text"
                placeholder="Search address..."
                value={minersSearch}
                onChange={(e) => setMinersSearch(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    setMinersPager((p) => ({ ...p, offset: 0 }));
                    void loadMiners();
                  }
                }}
              />
              <select value={minersSort} onChange={(e) => setMinersSort(e.target.value)}>
                <option value="hashrate_desc">Hashrate (high)</option>
                <option value="accepted_desc">Accepted (high)</option>
                <option value="rejected_desc">Rejected (high)</option>
                <option value="last_share_desc">Last Share (recent)</option>
                <option value="address_asc">Address (A-Z)</option>
              </select>
              <button
                className="btn btn-primary"
                onClick={() => {
                  setMinersPager((p) => ({ ...p, offset: 0 }));
                }}
              >
                Search
              </button>
            </div>

            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>Workers</th>
                    <th>Hashrate</th>
                    <th>Accepted</th>
                    <th>Rejected</th>
                    <th>Blocks</th>
                    <th>Last Share</th>
                  </tr>
                </thead>
                <tbody>
                  {!minersItems.length ? (
                    <tr>
                      <td colSpan={7} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No miners connected
                      </td>
                    </tr>
                  ) : (
                    minersItems.map((m) => (
                      <tr key={m.address}>
                        <td>
                          <a
                            href="/stats"
                            onClick={(e) => {
                              e.preventDefault();
                              onJumpToStats(m.address);
                            }}
                          >
                            {shortAddr(m.address)}
                          </a>
                        </td>
                        <td>{m.worker_count || 0}</td>
                        <td>{humanRate(m.hashrate)}</td>
                        <td>{m.shares_accepted || 0}</td>
                        <td>{m.shares_rejected || 0}</td>
                        <td>{m.blocks_found || 0}</td>
                        <td title={new Date(toUnixMs(m.last_share_at)).toLocaleString()}>{timeAgo(m.last_share_at)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              <Pager
                offset={minersPager.offset}
                limit={minersPager.limit}
                total={minersPager.total}
                onPrev={() => setMinersPager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
                onNext={() => setMinersPager((p) => ({ ...p, offset: p.offset + p.limit }))}
              />
            </div>
          </div>

          <div style={{ display: tab === 'rewards' ? '' : 'none' }}>
            <div className="filter-bar">
              <select
                value={rewardSelectedBlockValue}
                onChange={(e) => {
                  const value = e.target.value;
                  setRewardBlockInput(value);
                  if (value && !rewardBreakdownLoading) {
                    void loadRewardBreakdown(value);
                  }
                }}
                disabled={rewardBlockOptionsLoading}
              >
                <option value="">
                  {rewardBlockOptionsLoading
                    ? 'Loading mined blocks...'
                    : rewardBlockOptions.length
                      ? 'Select a mined block...'
                      : 'No mined blocks loaded'}
                </option>
                {rewardBlockOptions.map((block) => (
                  <option key={`${block.height}-${block.hash}`} value={block.height}>
                    {rewardBlockOptionLabel(block)}
                  </option>
                ))}
              </select>
              <input
                type="text"
                placeholder="Filter participant address..."
                value={rewardAddressFilter}
                onChange={(e) => setRewardAddressFilter(e.target.value)}
              />
              <button
                className="btn btn-primary"
                disabled={rewardLoadDisabled || rewardBreakdownLoading}
                onClick={() => void loadRewardBreakdown()}
              >
                {rewardBreakdownLoading ? 'Loading…' : 'Load Block'}
              </button>
            </div>

            {!rewardBreakdown ? (
              <div className="card section">
                <h3>Reward Breakdown</h3>
                <p style={{ color: 'var(--muted)', fontSize: 14 }}>
                  Load a block height to inspect the preview share math, the current payout recompute, and any recorded
                  credited amounts for that block.
                </p>
              </div>
            ) : (
              <>
                <div className="stats-grid stats-grid-dense" style={{ marginBottom: 20 }}>
                  <div className="stat-card">
                    <div className="label">Block</div>
                    <div className="value">{rewardBreakdown.block.height}</div>
                    <div className="stat-meta">{timeAgo(rewardBreakdown.block.timestamp)}</div>
                  </div>
                  <div className="stat-card">
                    <div className="label">{rewardBreakdown.block.orphaned ? 'Nominal Reward' : 'Reward'}</div>
                    <div className="value">{formatCoins(rewardBreakdown.block.reward)}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Round was orphaned, so no distributable credits were finalized.'
                        : `Fee ${formatCoins(rewardBreakdown.fee_amount)} · Net ${formatCoins(
                            rewardBreakdown.distributable_reward
                          )}`}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Window</div>
                    <div className="value">{rewardBreakdown.share_window.label}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.share_window.share_count} shares · {rewardBreakdown.share_window.participant_count} miners
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Preview Weight</div>
                    <div className="value mono">{rewardBreakdown.preview_total_weight}</div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Share split before the round resolved as orphaned'
                        : 'Matches the My Stats estimate path'}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">{rewardBreakdown.block.orphaned ? 'Resolution' : rewardPayoutWeightLabel}</div>
                    <div className={rewardBreakdown.block.orphaned ? 'value' : 'value mono'}>
                      {rewardBreakdown.block.orphaned ? 'Orphaned' : rewardBreakdown.payout_total_weight}
                    </div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Estimated payout collapsed to zero when the block orphaned'
                        : rewardBreakdownProjected
                          ? 'Current final split if the block reaches payout processing'
                          : rewardBreakdownPaidOut
                            ? 'Live recompute using current policy and risk state; recorded payout is authoritative'
                            : 'Final reward split after payout gates'}
                    </div>
                  </div>
                  <div className="stat-card">
                    <div className="label">Recorded Credits</div>
                    <div className="value">
                      {rewardBreakdown.block.orphaned ? formatCoins(0) : formatCoins(rewardBreakdown.actual_credit_total)}
                    </div>
                    <div className="stat-meta">
                      {rewardBreakdown.block.orphaned
                        ? 'Orphaned blocks resolve to zero credited payout'
                        : rewardBreakdown.actual_credit_events_available
                          ? 'Audit rows available'
                          : 'Not recorded yet'}
                    </div>
                  </div>
                </div>

                {rewardBreakdownPaidOut && !rewardBreakdown.block.orphaned ? (
                  <div
                    className="card section"
                    style={{ marginBottom: 20, borderColor: 'var(--warn)', background: 'var(--surface-hover)' }}
                  >
                    <h3 style={{ marginBottom: 8 }}>Recorded Payout Is Authoritative</h3>
                    <p style={{ color: 'var(--muted)', fontSize: 13, margin: 0 }}>
                      This block is already paid out. <span className="mono">Recorded Payout</span> comes from the
                      historical credit events written at payout time. <span className="mono">Current Recompute</span>{' '}
                      uses today&apos;s verification, risk, and cap state, so it can differ from the recorded payout if
                      an address was forced-verify or otherwise treated differently when the block was finalized.
                    </p>
                  </div>
                ) : null}

                <div className="card table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Address</th>
                        <th>{rewardPreviewColumnLabel}</th>
                        {rewardBreakdown.block.orphaned ? <th>Actual</th> : <th>Preview Weight</th>}
                        {rewardBreakdown.block.orphaned ? <th>Delta</th> : null}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardPayoutColumnLabel}</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardActualColumnLabel}</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardDeltaColumnLabel}</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardPayoutWeightLabel}</th>}
                        <th>Verified Diff</th>
                        <th>Eligible Prov Diff</th>
                        <th>{rewardStatusColumnLabel}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {!filteredRewardParticipants.length ? (
                        <tr>
                          <td colSpan={rewardBreakdown.block.orphaned ? 7 : 10} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                            No participants match the current filter.
                          </td>
                        </tr>
                      ) : (
                        filteredRewardParticipants.map((row) => (
                          <tr key={row.address}>
                            <td title={row.address}>
                              <a
                                href="/stats"
                                onClick={(e) => {
                                  e.preventDefault();
                                  onJumpToStats(row.address);
                                }}
                              >
                                {shortAddr(row.address)}
                              </a>
                              {row.finder ? (
                                <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>finder</div>
                              ) : null}
                            </td>
                            <td>
                              <div>{formatCoins(row.preview_credit)}</div>
                              <div className="mono" style={{ fontSize: 12, color: 'var(--muted)' }}>
                                {row.preview_weight} · {row.preview_share_pct.toFixed(3)}%
                              </div>
                            </td>
                            {rewardBreakdown.block.orphaned ? <td>{formatCoins(0)}</td> : <td className="mono">{row.preview_weight}</td>}
                            {rewardBreakdown.block.orphaned ? (
                              <td
                                style={{
                                  color: row.preview_credit === 0 ? 'var(--muted)' : 'var(--bad)',
                                }}
                              >
                                {formatSignedCoins(-row.preview_credit)}
                              </td>
                            ) : null}
                            {rewardBreakdown.block.orphaned ? null : (
                              <td>
                                <div>{formatCoins(row.payout_credit)}</div>
                                <div className="mono" style={{ fontSize: 12, color: 'var(--muted)' }}>
                                  {row.payout_weight} · {row.payout_share_pct.toFixed(3)}%
                                </div>
                              </td>
                            )}
                            {rewardBreakdown.block.orphaned ? null : (
                              <td>{row.actual_credit != null ? formatCoins(row.actual_credit) : '-'}</td>
                            )}
                            {rewardBreakdown.block.orphaned ? null : (
                              <td
                                style={{
                                  color:
                                    row.delta_vs_payout == null
                                      ? 'var(--muted)'
                                      : row.delta_vs_payout === 0
                                        ? 'var(--good)'
                                        : rewardBreakdownPaidOut
                                          ? 'var(--muted)'
                                          : 'var(--warn)',
                                }}
                              >
                                {formatSignedCoins(row.delta_vs_payout)}
                              </td>
                            )}
                            {rewardBreakdown.block.orphaned ? null : <td className="mono">{row.payout_weight}</td>}
                            <td className="mono">
                              {row.verified_difficulty}
                              <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                {row.verified_shares} shares
                              </div>
                            </td>
                            <td className="mono">
                              {row.provisional_difficulty_eligible}
                              <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                {row.provisional_shares_eligible} eligible
                              </div>
                            </td>
                            <td>
                              {rewardBreakdown.block.orphaned ? (
                                <>
                                  <div style={{ color: 'var(--bad)', fontWeight: 600 }}>
                                    Orphaned
                                  </div>
                                  <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                    Preview: {rewardStatusLabel(row.preview_status)}
                                    {row.risky ? ' · verification hold' : ''}
                                  </div>
                                </>
                              ) : (
                                <>
                                  {rewardBreakdownPaidOut ? (
                                    <>
                                      <div style={{ color: 'var(--muted)', fontWeight: 600 }}>
                                        Recorded payout finalized
                                      </div>
                                      <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                        Current recompute: {rewardStatusLabel(row.payout_status)} · Preview:{' '}
                                        {rewardStatusLabel(row.preview_status)}
                                      </div>
                                    </>
                                  ) : (
                                    <>
                                      <div style={{ color: rewardStatusTone(row.payout_status), fontWeight: 600 }}>
                                        {rewardStatusLabel(row.payout_status)}
                                      </div>
                                      <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                        Preview: {rewardStatusLabel(row.preview_status)}
                                        {row.risky ? ' · verification hold' : ''}
                                      </div>
                                    </>
                                  )}
                                </>
                              )}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                    {!rewardBreakdown.block.orphaned && rewardBreakdownTotals ? (
                      <tfoot>
                        <tr style={{ background: 'var(--surface-hover)' }}>
                          <td style={{ fontWeight: 700, textAlign: 'left' }}>
                            Pool Fee
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>not share-weighted</div>
                          </td>
                          <td>
                            <div>{formatCoins(rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              withheld from reward
                            </div>
                          </td>
                          <td className="mono">-</td>
                          <td>
                            <div>{formatCoins(rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              {pct(
                                rewardBreakdown.block.reward > 0
                                  ? (rewardBreakdown.fee_amount * 100) / rewardBreakdown.block.reward
                                  : 0
                              )}
                            </div>
                          </td>
                          <td>{rewardBreakdown.actual_fee_amount != null ? formatCoins(rewardBreakdown.actual_fee_amount) : '-'}</td>
                          <td
                            style={{
                              color:
                                rewardFeeDelta == null
                                  ? 'var(--muted)'
                                  : rewardFeeDelta === 0
                                    ? 'var(--good)'
                                    : 'var(--warn)',
                            }}
                          >
                            {formatSignedCoins(rewardFeeDelta)}
                          </td>
                          <td className="mono">-</td>
                          <td className="mono">-</td>
                          <td className="mono">-</td>
                          <td>
                            <div style={{ color: 'var(--muted)', fontWeight: 600 }}>Pool fee</div>
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              {rewardBreakdown.actual_fee_amount != null
                                ? 'tracked separately'
                                : rewardBreakdown.block.paid_out
                                  ? 'fee row missing'
                                  : 'pending'}
                            </div>
                          </td>
                        </tr>
                        <tr style={{ background: 'var(--surface-hover)' }}>
                          <td style={{ fontWeight: 700, textAlign: 'left' }}>
                            Block Total
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              all participants + fee
                            </div>
                          </td>
                          <td>
                            <div>{formatCoins(rewardBreakdownTotals.previewCredit + rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              target {formatCoins(rewardBreakdown.block.reward)}
                            </div>
                          </td>
                          <td className="mono">{rewardBreakdown.preview_total_weight}</td>
                          <td>
                            <div>{formatCoins(rewardBreakdownTotals.payoutCredit + rewardBreakdown.fee_amount)}</div>
                            <div className="mono" style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              target {formatCoins(rewardBreakdown.block.reward)}
                            </div>
                          </td>
                          <td>{rewardActualBlockTotal != null ? formatCoins(rewardActualBlockTotal) : '-'}</td>
                          <td
                            style={{
                              color:
                                rewardActualBlockDelta == null
                                  ? 'var(--muted)'
                                  : rewardActualBlockDelta === 0
                                    ? 'var(--good)'
                                    : rewardBreakdownPaidOut
                                      ? 'var(--muted)'
                                      : 'var(--warn)',
                            }}
                          >
                            {formatSignedCoins(rewardActualBlockDelta)}
                          </td>
                          <td className="mono">{rewardBreakdown.payout_total_weight}</td>
                          <td className="mono">{rewardBreakdownTotals.verifiedDifficulty}</td>
                          <td className="mono">{rewardBreakdownTotals.provisionalEligibleDifficulty}</td>
                          <td>
                            <div
                              style={{
                                color:
                                  rewardActualBlockDelta == null
                                    ? 'var(--muted)'
                                    : rewardActualBlockDelta === 0
                                      ? 'var(--good)'
                                      : rewardBreakdownPaidOut
                                        ? 'var(--muted)'
                                        : 'var(--warn)',
                                fontWeight: 600,
                              }}
                            >
                              {rewardActualBlockDelta == null
                                ? 'Pending'
                                : rewardActualBlockDelta === 0
                                  ? 'Balanced'
                                  : rewardBreakdownPaidOut
                                    ? 'Historical differs from recompute'
                                    : 'Mismatch'}
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              {rewardBreakdownPaidOut
                                ? 'recorded payout vs current recompute across the full block'
                                : 'summary across the full block'}
                            </div>
                          </td>
                        </tr>
                      </tfoot>
                    ) : null}
                  </table>
                  {rewardAddressFilter.trim() && !rewardBreakdown.block.orphaned ? (
                    <div style={{ marginTop: 12, fontSize: 12, color: 'var(--muted)' }}>
                      Summary rows include all participants for the block, not just the filtered addresses.
                    </div>
                  ) : null}
                </div>
              </>
            )}
          </div>

          <div style={{ display: tab === 'shares' ? '' : 'none' }}>
            <div className="stats-card-group">
              <div className="section-header" style={{ marginBottom: 12 }}>
                <div>
                  <div className="stats-card-group-title">Shares</div>
                  <div className="share-focus-title">{shareOperatorFocus.title}</div>
                  <div className="section-lead" style={{ maxWidth: '72ch' }}>
                    {shareOperatorFocus.detail}
                  </div>
                </div>
                <span className={roundChipClass(shareOperatorFocus.tone)}>{shareOperatorFocus.label}</span>
              </div>
              <div className="share-focus-next">Next: {shareOperatorFocus.next}</div>
              <div className="share-focus-grid">
                <div className="share-focus-card">
                  <div className="share-focus-card-head">
                    <div className="share-focus-card-title">Hot Path</div>
                    <span className={roundChipClass(shareHotPathFocus.tone)}>
                      {shareHotPathFocus.tone === 'ok'
                        ? 'Healthy'
                        : shareHotPathFocus.tone === 'warn'
                          ? 'Watch'
                          : 'Act now'}
                    </span>
                  </div>
                  <div className="share-focus-card-body">{shareHotPathFocus.title}</div>
                  <div className="stat-meta">
                    <div>{shareHotPathFocus.detail}</div>
                    <div>
                      {shareHotAccepts} hot accepts · {shareSyncFullVerifies} sync verified
                    </div>
                  </div>
                </div>
                <div className="share-focus-card">
                  <div className="share-focus-card-head">
                    <div className="share-focus-card-title">Background Audit</div>
                    <span className={roundChipClass(shareAuditFocus.tone)}>{shareAuditFocus.badge}</span>
                  </div>
                  <div className="share-focus-card-body">{shareAuditFocus.title}</div>
                  <div className="stat-meta">
                    <div>{shareAuditFocus.detail}</div>
                    <div>
                      {shareValidation?.audit_verified ?? 0} verified · {shareValidation?.audit_rejected ?? 0} rejected ·{' '}
                      {shareValidation?.audit_deferred ?? 0} deferred
                    </div>
                  </div>
                </div>
                <div className="share-focus-card">
                  <div className="share-focus-card-head">
                    <div className="share-focus-card-title">Rejects</div>
                    <span className={roundChipClass(shareRejectFocus.tone)}>
                      {shareRejectFocus.tone === 'ok'
                        ? 'Clean'
                        : shareRejectFocus.tone === 'warn'
                          ? 'Watch'
                          : 'Investigate'}
                    </span>
                  </div>
                  <div className="share-focus-card-body">{shareRejectFocus.title}</div>
                  <div className="stat-meta">
                    <div>{shareRejectFocus.detail}</div>
                    <div>
                      5m {pct(shareWindow5m?.rejection_rate_pct)} · 1h {pct(shareWindow1h?.rejection_rate_pct)} · 24h{' '}
                      {pct(shareWindow24h?.rejection_rate_pct)}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">Live Pipeline</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Submit Path</div>
                  <div className="value mono">{formatMillis(shareSubmitWaitP95)}</div>
                  <div className="stat-meta">
                    {shareSubmitQueueDepth} queued · oldest {formatMillis(shareSubmitOldestAge)}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Live Validation</div>
                  <div className="value mono">{formatMillis(shareValidationWaitP95)}</div>
                  <div className="stat-meta">
                    {shareValidationQueueDepth} queued · hash {formatMillis(shareValidationDurationP95)}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Background Audit</div>
                  <div className="value mono">{formatMillis(shareAuditWaitP95)}</div>
                  <div className="stat-meta">
                    {shareAuditQueueDepth} queued · hash {formatMillis(shareAuditDurationP95)}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Verification Coverage</div>
                  <div className="value mono">{ratioPct(shareValidation?.effective_sample_rate)}</div>
                  <div className="stat-meta">
                    {shareValidation?.sampled_shares ?? 0} sampled · overload {overloadModeLabel(shareValidation?.overload_mode)}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">5m Busy / Timeout</div>
                  <div className="value mono">{shareBusyCount5m + shareTimeoutCount5m}</div>
                  <div className="stat-meta">
                    {shareBusyCount5m} busy · {shareTimeoutCount5m} timeout
                  </div>
                </div>
              </div>
            </div>

            <details className="card share-advanced">
              <summary>
                <span>Advanced diagnostics</span>
                <span className="share-advanced-summary">
                  Use this when you need the raw queue timings, counters, and window-by-window reject breakdown.
                </span>
              </summary>
              <div className="share-advanced-body">
                <div className="stats-card-group" style={{ marginBottom: 16 }}>
                  <div className="stats-card-group-title">Detailed Runtime Metrics</div>
                  <div className="stats-card-group-grid stats-grid-dense">
                    <div className="stat-card">
                      <div className="label">Submit Queue</div>
                      <div className="value mono">{shareSubmitQueueDepth}</div>
                      <div className="stat-meta">
                        {shareSubmit?.candidate_queue_depth ?? 0} candidate · {shareSubmit?.regular_queue_depth ?? 0} regular
                      </div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Validation Queue</div>
                      <div className="value mono">{shareValidationQueueDepth}</div>
                      <div className="stat-meta">
                        {shareValidation?.candidate_queue_depth ?? 0} candidate · {shareValidation?.regular_queue_depth ?? 0} regular
                      </div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Audit Queue</div>
                      <div className="value mono">{shareAuditQueueDepth}</div>
                      <div className="stat-meta">oldest {formatMillis(shareAuditOldestAge)}</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Validation Wait P95</div>
                      <div className="value mono">{formatMillis(shareValidationWaitP95)}</div>
                      <div className="stat-meta">oldest {formatMillis(shareValidationOldestAge)}</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Audit Wait / Time P95</div>
                      <div className="value mono">{formatMillis(shareAuditWaitP95)}</div>
                      <div className="stat-meta">hash {formatMillis(shareAuditDurationP95)}</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Validation Time P95</div>
                      <div className="value mono">{formatMillis(shareValidationDurationP95)}</div>
                      <div className="stat-meta">{shareValidation?.in_flight ?? 0} in flight</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Candidate False Claims</div>
                      <div className="value mono">{shareValidation?.candidate_false_claims ?? 0}</div>
                      <div className="stat-meta" style={{ color: sharePressureSignal.tone }}>
                        {sharePressureSignal.label}
                      </div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Hot Accepts / Sync</div>
                      <div className="value mono">{shareHotAccepts}</div>
                      <div className="stat-meta">{shareSyncFullVerifies} sync verified</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Audit Outcomes</div>
                      <div className="value mono">{shareValidation?.audit_verified ?? 0}</div>
                      <div className="stat-meta">
                        {shareValidation?.audit_rejected ?? 0} rejected · {shareValidation?.audit_deferred ?? 0} deferred
                      </div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Audit Enqueued</div>
                      <div className="value mono">{shareValidation?.audit_enqueued ?? 0}</div>
                      <div className="stat-meta">{shareAuditQueueDepth} queued now</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">5m Invalid Proof</div>
                      <div className="value mono">{pct(shareWindowReasonPct(shareWindow5m, 'invalid share proof'))}</div>
                      <div className="stat-meta">{shareWindowReasonCount(shareWindow5m, 'invalid share proof')} rejects</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Overload Mode</div>
                      <div className="value mono">{overloadModeLabel(shareValidation?.overload_mode)}</div>
                      <div className="stat-meta">{sharePressureSignal.detail}</div>
                    </div>
                  </div>
                </div>

                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Window</th>
                        <th>Accepted</th>
                        <th>Rejected</th>
                        <th>Reject %</th>
                        <th>Invalid %</th>
                        <th>Low Diff %</th>
                        <th>Stale %</th>
                        <th>Quarantined %</th>
                        <th>Busy %</th>
                        <th>Timeout %</th>
                        <th>Top Reject</th>
                      </tr>
                    </thead>
                    <tbody>
                      {!shareWindows.length ? (
                        <tr>
                          <td colSpan={11} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                            No share diagnostics available yet
                          </td>
                        </tr>
                      ) : (
                        shareWindows.map((window) => {
                          const topReason = window.by_reason?.[0];
                          const topReasonRejectPct =
                            topReason && window.rejected > 0 ? (topReason.count / window.rejected) * 100 : 0;
                          return (
                            <tr key={window.label}>
                              <td>
                                <div style={{ fontWeight: 600 }}>{window.label}</div>
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>{window.total} submits</div>
                              </td>
                              <td className="mono">{window.accepted}</td>
                              <td className="mono">{window.rejected}</td>
                              <td className="mono">{pct(window.rejection_rate_pct)}</td>
                              <td className="mono">
                                {pct(shareWindowReasonPct(window, 'invalid share proof'))}
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                  {shareWindowReasonCount(window, 'invalid share proof')} rejects
                                </div>
                              </td>
                              <td className="mono">
                                {pct(shareWindowReasonPct(window, 'low difficulty share'))}
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                  {shareWindowReasonCount(window, 'low difficulty share')} rejects
                                </div>
                              </td>
                              <td className="mono">
                                {pct(shareWindowReasonPct(window, 'stale job'))}
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                  {shareWindowReasonCount(window, 'stale job')} rejects
                                </div>
                              </td>
                              <td className="mono">
                                {pct(shareWindowReasonPct(window, 'address quarantined'))}
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                  {shareWindowReasonCount(window, 'address quarantined')} rejects
                                </div>
                              </td>
                              <td className="mono">
                                {pct(shareWindowReasonPct(window, 'server busy'))}
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                  {shareWindowReasonCount(window, 'server busy')} rejects
                                </div>
                              </td>
                              <td className="mono">
                                {pct(shareWindowReasonPct(window, 'validation timeout'))}
                                <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                  {shareWindowReasonCount(window, 'validation timeout')} rejects
                                </div>
                              </td>
                              <td>
                                {!topReason ? (
                                  <span style={{ color: 'var(--muted)' }}>None</span>
                                ) : (
                                  <>
                                    <div style={{ fontWeight: 600 }}>{topReason.reason}</div>
                                    <div style={{ fontSize: 11, color: 'var(--muted)' }}>
                                      {topReason.count} rejects · {pct(topReasonRejectPct)} of rejects
                                    </div>
                                  </>
                                )}
                              </td>
                            </tr>
                          );
                        })
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </details>
          </div>

          <div style={{ display: tab === 'holds' ? '' : 'none' }}>
            <div className="stats-card-group">
              <div className="stats-card-group-title">Verification Holds</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Risk Holds</div>
                  <div className="value mono">{riskVerificationHolds.length}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Temporary Assists</div>
                  <div className="value mono">{temporaryValidationHolds.length}</div>
                  <div className="stat-meta">coverage or backlog boosts</div>
                </div>
                <div className="stat-card">
                  <div className="label">Risk Forced Verify</div>
                  <div className="value mono">
                    {
                      activeVerificationHolds.filter(
                        (hold) => hasActiveUntil(hold.force_verify_until) || hold.validation_hold_cause === 'invalid_samples'
                      ).length
                    }
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Fraud Detections</div>
                  <div className="value mono">{health?.validation?.fraud_detections ?? '-'}</div>
                </div>
              </div>
            </div>

            <div className="card section" style={{ marginTop: 16 }}>
              <div className="section-header">
                <div>
                  <h3>Active Verification Holds</h3>
                  <p className="section-lead">
                    Risk holds and temporary validation assists currently affecting share validation.
                  </p>
                </div>
              </div>
              <div style={{ marginTop: 12, fontSize: 13, color: 'var(--muted)' }}>
                <span className="mono">Backlog drain</span> and <span className="mono">Payout boost</span> are temporary
                assists, not fraud events. They clear early as soon as verification catches up, even if the countdown
                still shows time remaining.
              </div>
              {holdActionError ? (
                <div
                  style={{
                    marginTop: 12,
                    padding: '10px 12px',
                    borderRadius: 12,
                    background: 'var(--error-bg)',
                    color: 'var(--error-text)',
                    fontSize: 13,
                  }}
                >
                  {holdActionError}
                </div>
              ) : null}
              <div className="table-scroll" style={{ marginTop: 12 }}>
                <table>
                  <thead>
                    <tr>
                      <th>Address</th>
                      <th>Mode</th>
                      <th>Quarantine Until</th>
                      <th>Risk Verify Until</th>
                      <th>Validation Until</th>
                      <th>Strikes</th>
                      <th>Reason</th>
                      <th>Last Event</th>
                      <th>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {!activeVerificationHolds.length ? (
                      <tr>
                        <td colSpan={9} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                          No active verification holds
                        </td>
                      </tr>
                    ) : (
                      activeVerificationHolds.map((hold) => (
                        <tr key={hold.address}>
                          <td title={hold.address}>
                            <a
                              href="/stats"
                              onClick={(e) => {
                                e.preventDefault();
                                onJumpToStats(hold.address);
                              }}
                            >
                              {shortAddr(hold.address)}
                            </a>
                          </td>
                          <td>
                            <span
                              className={verificationHoldBadgeClass(
                                verificationHoldActive(hold),
                                verificationHoldTone(hold)
                              )}
                            >
                              {verificationHoldLabel(hold)}
                            </span>
                          </td>
                          <td className="mono" title={holdUntilTitle(hold.quarantined_until)}>
                            {holdUntilLabel(hold.quarantined_until)}
                          </td>
                          <td className="mono" title={holdUntilTitle(hold.force_verify_until)}>
                            {holdUntilLabel(hold.force_verify_until)}
                          </td>
                          <td className="mono" title={holdUntilTitle(hold.validation_forced_until)}>
                            <div>{validationHoldUntilLabel(hold)}</div>
                            {validationHoldUntilHint(hold) ? (
                              <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                {validationHoldUntilHint(hold)}
                              </div>
                            ) : null}
                          </td>
                          <td className="mono">
                            {hold.strikes}
                            {hold.suspected_fraud_strikes > 0 ? ` / fraud ${hold.suspected_fraud_strikes}` : ''}
                          </td>
                          <td title={hold.reason ?? hold.last_reason ?? undefined}>
                            {hold.reason ?? hold.last_reason ?? '-'}
                          </td>
                          <td className="mono" title={hold.last_event_at ? formatAdminTimestamp(hold.last_event_at) : undefined}>
                            {hold.last_event_at ? timeAgo(hold.last_event_at) : '-'}
                          </td>
                          <td>
                            <button
                              className="btn btn-secondary"
                              disabled={holdBusyAddress !== null}
                              onClick={() => void clearAddressRiskHistory(hold.address)}
                              title="Delete all admin risk and validation hold history for this address"
                            >
                              {holdBusyAddress === hold.address ? 'Clearing…' : 'Clear History'}
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div style={{ display: tab === 'balances' ? '' : 'none' }}>
            {balanceOverview ? (
              <>
                {hasUnresolvedLedgerMismatch && (
                  <div
                    className="card"
                    style={{
                      marginBottom: 16,
                      borderColor: 'rgba(247, 180, 75, 0.45)',
                      background: 'rgba(247, 180, 75, 0.08)',
                    }}
                  >
                    <div style={{ color: 'var(--text)', fontSize: 13 }}>
                      Pending balances below are recorded miner ledger state, not clean payable liability.{' '}
                      {ledgerOverhangAmount != null && ledgerOverhangAmount > 0 ? (
                        <>
                          The miner ledger currently exceeds canonical miner rewards by{' '}
                          <span className="mono">{formatCoins(ledgerOverhangAmount)}</span>, so fork-era recovery and
                          payout reconciliation still need operator review.
                        </>
                      ) : unresolvedLedgerShortfallAmount != null && unresolvedLedgerShortfallAmount > 0 ? (
                        <>
                          The miner ledger currently sits{' '}
                          <span className="mono">{formatCoins(unresolvedLedgerShortfallAmount)}</span> below canonical miner
                          rewards, so some canonical rewards have not been fully reflected in miner balances yet.
                        </>
                      ) : (
                        <>Ledger reconciliation is still in progress.</>
                      )}
                    </div>
                  </div>
                )}
                {!hasUnresolvedLedgerMismatch && (acknowledgedHistoricalShortfallAmount ?? 0) > 0 && (
                  <div
                    className="card"
                    style={{
                      marginBottom: 16,
                      borderColor: 'rgba(111, 126, 120, 0.35)',
                      background: 'rgba(111, 126, 120, 0.06)',
                    }}
                  >
                    <div style={{ color: 'var(--text)', fontSize: 13 }}>
                      The remaining{' '}
                      <span className="mono">{formatCoins(acknowledgedHistoricalShortfallAmount)}</span> miner
                      shortfall is the acknowledged launch-era baseline. It is tracked for reference, but not treated
                      as an active payout or fork-recovery incident.
                    </div>
                  </div>
                )}

                <div className="admin-balance-overview__grid" style={{ marginBottom: 20 }}>
                  <div
                    className="admin-balance-overview__panel"
                    style={{
                      borderColor:
                        minerFundingGapAmount != null && minerFundingGapAmount > 0
                          ? 'rgba(247, 180, 75, 0.4)'
                          : 'rgba(22, 163, 74, 0.24)',
                      background:
                        minerFundingGapAmount != null && minerFundingGapAmount > 0
                          ? 'rgba(247, 180, 75, 0.06)'
                          : 'rgba(22, 163, 74, 0.05)',
                    }}
                  >
                    <h3>Operator View</h3>
                    <div style={{ color: 'var(--muted)', fontSize: 12, marginBottom: 14 }}>
                      External liability here means clean payable miner balances only. Internal pool fee balances and
                      ledger diagnostics are shown separately below and do not change this funding-gap number.
                    </div>
                    <div className="admin-balance-overview__rows">
                      <div className="admin-balance-overview__row">
                        <span>External miner liability</span>
                        <span className="mono">{formatCoins(balanceOverview.payouts.clean_unpaid_amount)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Wallet total available</span>
                        <span className="mono">{formatCoins(balanceOverview.wallet.total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>
                          {minerFundingGapAmount != null && minerFundingGapAmount > 0
                            ? 'Estimated miner funding gap'
                            : 'Wallet surplus over miner liability'}
                        </span>
                        <span
                          className="mono"
                          style={
                            minerFundingGapAmount != null && minerFundingGapAmount > 0
                              ? { color: 'var(--warn)' }
                              : { color: 'var(--good)' }
                          }
                        >
                          {formatCoins(
                            minerFundingGapAmount != null && minerFundingGapAmount > 0
                              ? minerFundingGapAmount
                              : minerWalletSurplusAmount ?? 0
                          )}
                        </span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Immediate queue shortfall</span>
                        <span
                          className="mono"
                          style={
                            balanceOverview.liquidity.queue_shortfall_amount > 0
                              ? { color: 'var(--warn)' }
                              : { color: 'var(--good)' }
                          }
                        >
                          {formatCoins(balanceOverview.liquidity.queue_shortfall_amount)}
                        </span>
                      </div>
                      {balanceOverview.payouts.pool_fee_clean_unpaid_amount > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Internal pool fee tracked separately</span>
                          <span className="mono" style={{ color: 'var(--muted)' }}>
                            {formatCoins(balanceOverview.payouts.pool_fee_clean_unpaid_amount)}
                          </span>
                        </div>
                      )}
                      {balanceOverview.payouts.balance_source_drift_amount > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Miner source drift diagnostic</span>
                          <span className="mono" style={{ color: 'var(--warn)' }}>
                            {formatCoins(balanceOverview.payouts.balance_source_drift_amount)}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                  <div className="admin-balance-overview__panel">
                    <h3>Wallet</h3>
                    <div className="admin-balance-overview__rows">
                      <div className="admin-balance-overview__row">
                        <span>Spendable</span>
                        <span className="mono">{formatCoins(balanceOverview.wallet.spendable)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Locked / confirming</span>
                        <span className="mono">{formatCoins(balanceOverview.wallet.pending)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Total</span>
                        <span className="mono">{formatCoins(balanceOverview.wallet.total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Queued payouts</span>
                        <span className="mono">
                          {formatCoins(balanceOverview.payouts.queued_amount)} across{' '}
                          {formatWholeNumber(balanceOverview.payouts.queued_count)}
                        </span>
                      </div>
                      {balanceOverview.liquidity.queue_shortfall_amount > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Immediate queue shortfall</span>
                          <span className="mono" style={{ color: 'var(--warn)' }}>
                            {formatCoins(balanceOverview.liquidity.queue_shortfall_amount)}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                  <div className="admin-balance-overview__panel">
                    <h3>Ledger</h3>
                    <div className="admin-balance-overview__rows">
                      <div className="admin-balance-overview__row">
                        <span>Miner paid total</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.miner_paid_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Recorded miner pending</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.miner_unpaid_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Clean payable miner pending</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.miner_clean_unpaid_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Orphan-backed miner pending</span>
                        <span
                          className="mono"
                          style={
                            balanceOverview.ledger.miner_orphan_backed_unpaid_total > 0
                              ? { color: 'var(--warn)' }
                              : undefined
                          }
                        >
                          {formatCoins(balanceOverview.ledger.miner_orphan_backed_unpaid_total)}
                        </span>
                      </div>
                      {balanceOverview.ledger.miner_balance_source_drift_total > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Miner balance above live sources</span>
                          <span className="mono" style={{ color: 'var(--warn)' }}>
                            {formatCoins(balanceOverview.ledger.miner_balance_source_drift_total)}
                          </span>
                        </div>
                      )}
                      <div className="admin-balance-overview__row">
                        <span>Confirmed block rewards</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.net_block_reward_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Pool fees</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.pool_fee_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Internal pool fee balance</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.pool_fee_balance_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Internal pool fee clean payable</span>
                        <span className="mono">{formatCoins(balanceOverview.ledger.pool_fee_clean_unpaid_total)}</span>
                      </div>
                      <div className="admin-balance-overview__row">
                        <span>Internal pool fee orphan-backed</span>
                        <span
                          className="mono"
                          style={
                            balanceOverview.ledger.pool_fee_orphan_backed_unpaid_total > 0
                              ? { color: 'var(--warn)' }
                              : undefined
                          }
                        >
                          {formatCoins(balanceOverview.ledger.pool_fee_orphan_backed_unpaid_total)}
                        </span>
                      </div>
                      {balanceOverview.ledger.pool_fee_balance_source_drift_total > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Internal pool fee above live sources</span>
                          <span className="mono" style={{ color: 'var(--warn)' }}>
                            {formatCoins(balanceOverview.ledger.pool_fee_balance_source_drift_total)}
                          </span>
                        </div>
                      )}
                      <div className="admin-balance-overview__row">
                        <span>Canonical-backed miner pending</span>
                        <span className="mono">{formatCoins(canonicalBackedPending ?? 0)}</span>
                      </div>
                      {ledgerOverhangAmount != null && ledgerOverhangAmount > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Miner ledger overhang</span>
                          <span className="mono" style={{ color: 'var(--warn)' }}>
                            {formatCoins(ledgerOverhangAmount)}
                          </span>
                        </div>
                      )}
                      {acknowledgedHistoricalShortfallAmount != null && acknowledgedHistoricalShortfallAmount > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Acknowledged launch-era baseline</span>
                          <span className="mono" style={{ color: 'var(--muted)' }}>
                            {formatCoins(acknowledgedHistoricalShortfallAmount)}
                          </span>
                        </div>
                      )}
                      {unresolvedLedgerShortfallAmount != null && unresolvedLedgerShortfallAmount > 0 && (
                        <div className="admin-balance-overview__row">
                          <span>Unallocated canonical miner rewards</span>
                          <span className="mono" style={{ color: 'var(--warn)' }}>
                            {formatCoins(unresolvedLedgerShortfallAmount)}
                          </span>
                        </div>
                      )}
                      <div className="admin-balance-overview__row">
                        <span>Credits balanced</span>
                        <span
                          className="mono"
                          style={
                            balanceOverview.ledger.miner_rewards_balanced || !hasUnresolvedLedgerMismatch
                              ? { color: 'var(--good)' }
                              : { color: 'var(--warn)' }
                          }
                        >
                          {balanceOverview.ledger.miner_rewards_balanced
                            ? 'Yes'
                            : hasUnresolvedLedgerMismatch
                              ? 'No'
                              : 'Baseline accepted'}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              </>
            ) : null}

            <div className="filter-bar">
              <input
                type="text"
                placeholder="Search address..."
                value={balancesSearch}
                onChange={(e) => setBalancesSearch(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    setBalancesPager((p) => ({ ...p, offset: 0 }));
                  }
                }}
              />
              <select value={balancesSort} onChange={(e) => { setBalancesSort(e.target.value); setBalancesPager((p) => ({ ...p, offset: 0 })); }}>
                <option value="pending_desc">Pending (high first)</option>
                <option value="pending_asc">Pending (low first)</option>
                <option value="paid_desc">Paid (high first)</option>
                <option value="paid_asc">Paid (low first)</option>
                <option value="address_asc">Address A-Z</option>
                <option value="address_desc">Address Z-A</option>
              </select>
              <button className="btn btn-primary" onClick={() => setBalancesPager((p) => ({ ...p, offset: 0 }))}>
                Search
              </button>
            </div>

            <div className="card table-scroll">
              <table className="admin-balance-table">
                <colgroup>
                  <col className="admin-balance-table__address-col" />
                  <col className="admin-balance-table__amount-col" />
                  <col className="admin-balance-table__amount-col" />
                  <col className="admin-balance-table__amount-col" />
                  <col className="admin-balance-table__amount-col" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>Clean Payable</th>
                    <th>Orphan-Backed</th>
                    <th>Recorded Pending</th>
                    <th>Total Paid</th>
                  </tr>
                </thead>
                <tbody>
                  {!balancesItems.length ? (
                    <tr>
                      <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No balances
                      </td>
                    </tr>
                  ) : (
                    balancesItems.map((b) => (
                      <tr key={b.address}>
                        <td title={b.address}>
                          <a
                            href="/stats"
                            onClick={(e) => {
                              e.preventDefault();
                              onJumpToStats(b.address);
                            }}
                          >
                            {shortAddr(b.address)}
                          </a>
                        </td>
                        <td className="mono">
                          {b.clean_payable > 0 ? formatCoins(b.clean_payable) : formatCoins(0)}
                        </td>
                        <td className="mono">
                          {b.orphan_backed > 0 ? (
                            <span style={{ color: 'var(--warn)' }}>{formatCoins(b.orphan_backed)}</span>
                          ) : (
                            formatCoins(0)
                          )}
                        </td>
                        <td className="mono">
                          {b.pending > 0 ? (
                            <span style={{ color: 'var(--warn)' }}>{formatCoins(b.pending)}</span>
                          ) : (
                            formatCoins(b.pending)
                          )}
                        </td>
                        <td className="mono">{formatCoins(b.paid)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              <Pager
                offset={balancesPager.offset}
                limit={balancesPager.limit}
                total={balancesPager.total}
                onPrev={() => setBalancesPager((p) => ({ ...p, offset: Math.max(0, p.offset - p.limit) }))}
                onNext={() => setBalancesPager((p) => ({ ...p, offset: p.offset + p.limit }))}
              />
            </div>
          </div>

          <div style={{ display: tab === 'recovery' ? '' : 'none' }}>
            {recoveryStatus?.warning ? (
              <div className="card section" style={{ marginBottom: 16, borderColor: 'rgba(247, 180, 75, 0.45)' }}>
                <p className="section-lead" style={{ margin: 0 }}>
                  {recoveryStatus.warning}
                </p>
              </div>
            ) : null}

            {recoveryActionError ? (
              <div className="card section" style={{ marginBottom: 16, borderColor: 'rgba(214, 88, 88, 0.45)' }}>
                <p className="section-lead" style={{ margin: 0, color: 'var(--bad)' }}>
                  {recoveryActionError}
                </p>
              </div>
            ) : null}

            {reconciliationActionError ? (
              <div className="card section" style={{ marginBottom: 16, borderColor: 'rgba(214, 88, 88, 0.45)' }}>
                <p className="section-lead" style={{ margin: 0, color: 'var(--bad)' }}>
                  {reconciliationActionError}
                </p>
              </div>
            ) : null}

            {recoveryPendingNote ? (
              <div className="card section" style={{ marginBottom: 16 }}>
                <p className="section-lead" style={{ margin: 0 }}>
                  {recoveryPendingNote}
                </p>
              </div>
            ) : null}

            <div className="stats-grid" style={{ marginBottom: 16 }}>
              <div className="stat-card">
                <div className="label">Active Daemon</div>
                <div className="value">
                  {recoveryStatus ? recoveryInstanceLabel(recoveryStatus.active_instance) : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Proxy Target</div>
                <div className="value">
                  {recoveryStatus ? recoveryInstanceLabel(recoveryStatus.proxy_target) : '-'}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Payouts</div>
                <div className="value">
                  {recoveryStatus == null ? (
                    '-'
                  ) : recoveryStatus.payouts_paused ? (
                    <>
                      <span className="status-dot dot-amber" />Paused
                    </>
                  ) : (
                    <>
                      <span className="status-dot dot-green" />Live
                    </>
                  )}
                </div>
              </div>
              <div className="stat-card">
                <div className="label">Wallet Secret</div>
                <div className="value">
                  {recoveryStatus == null ? (
                    '-'
                  ) : recoveryStatus.secret_configured ? (
                    <>
                      <span className="status-dot dot-green" />Configured
                    </>
                  ) : (
                    <>
                      <span className="status-dot dot-red" />Missing
                    </>
                  )}
                </div>
              </div>
            </div>

            <div className="card section" style={{ marginBottom: 16 }}>
              <div className="section-header">
                <div>
                  <h3>Actions</h3>
                  <p className="section-lead">
                    Inactive sync, wallet rebuild, and inactive purge can run while payouts stay live. Cutover still
                    requires payouts to be paused first.
                  </p>
                </div>
                <button className="btn btn-secondary" onClick={() => void loadRecovery()}>
                  Refresh
                </button>
              </div>
              <div className="filter-bar" style={{ gap: 8 }}>
                <button
                  className="btn btn-secondary"
                  disabled={recoveryActionState.pause.disabled}
                  title={recoveryActionState.pause.title}
                  onClick={() => void runRecoveryAction('pause_payouts', () => api.pauseRecoveryPayouts())}
                >
                  {recoveryBusy === 'pause_payouts' ? 'Pausing…' : 'Pause Payouts'}
                </button>
                <button
                  className="btn btn-secondary"
                  disabled={recoveryActionState.resume.disabled}
                  title={recoveryActionState.resume.title}
                  onClick={() => void runRecoveryAction('resume_payouts', () => api.resumeRecoveryPayouts())}
                >
                  {recoveryBusy === 'resume_payouts' ? 'Resuming…' : 'Resume Payouts'}
                </button>
                <button
                  className="btn btn-secondary"
                  disabled={recoveryActionState.startInactiveSync.disabled}
                  title={recoveryActionState.startInactiveSync.title}
                  onClick={() => void runRecoveryAction('start_standby_sync', () => api.startInactiveSync())}
                >
                  {recoveryBusy === 'start_standby_sync'
                    ? 'Starting…'
                    : recoveryInactiveInstance == null
                      ? 'Start Inactive Sync'
                      : `Start Sync On ${recoveryInactiveLabel}`}
                </button>
                <button
                  className="btn btn-secondary"
                  disabled={recoveryActionState.rebuildInactiveWallet.disabled}
                  title={recoveryActionState.rebuildInactiveWallet.title}
                  onClick={() => void runRecoveryAction('rebuild_standby_wallet', () => api.rebuildInactiveWallet())}
                >
                  {recoveryBusy === 'rebuild_standby_wallet'
                    ? 'Rebuilding…'
                    : recoveryInactiveInstance == null
                      ? 'Rebuild Inactive Wallet'
                      : `Rebuild ${recoveryInactiveLabel} Wallet`}
                </button>
                <button
                  className="btn btn-primary"
                  disabled={recoveryActionState.cutoverStandby.disabled}
                  title={recoveryActionState.cutoverStandby.title}
                  onClick={() => void runRecoveryAction('cutover', () => api.cutoverDaemon('standby'))}
                >
                  {recoveryBusy === 'cutover' ? 'Cutting Over…' : 'Cut Over To Standby'}
                </button>
                <button
                  className="btn btn-secondary"
                  disabled={recoveryActionState.cutoverPrimary.disabled}
                  title={recoveryActionState.cutoverPrimary.title}
                  onClick={() => void runRecoveryAction('cutover', () => api.cutoverDaemon('primary'))}
                >
                  {recoveryBusy === 'cutover' ? 'Cutting Over…' : 'Cut Over To Primary'}
                </button>
                <button
                  className="btn btn-secondary"
                  disabled={recoveryActionState.purgeInactiveDaemon.disabled}
                  title={recoveryActionState.purgeInactiveDaemon.title}
                  onClick={() => void runRecoveryAction('purge_inactive_daemon', () => api.purgeInactiveDaemon())}
                >
                  {recoveryBusy === 'purge_inactive_daemon' ? 'Purging…' : 'Purge Inactive Daemon'}
                </button>
              </div>
            </div>

            <div className="card section" style={{ marginBottom: 16 }}>
              <div className="section-header">
                <div>
                  <h3>Accounting Exceptions</h3>
                  <p className="section-lead">
                    These are payout/accounting rows that the pool could not safely resolve on its own after chain
                    recovery. Resolve missing completed payouts here instead of editing balances directly.
                  </p>
                </div>
                <button className="btn btn-secondary" onClick={() => void loadReconciliationIssues()}>
                  Refresh
                </button>
              </div>

              <div className="stats-grid stats-grid-dense" style={{ marginBottom: 16 }}>
                <div className="stat-card">
                  <div className="label">Open Issues</div>
                  <div className="value mono">{reconciliationIssues?.summary.total_open_issues ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Missing Payouts</div>
                  <div className="value mono">{reconciliationIssues?.summary.missing_payout_issue_count ?? '-'}</div>
                  <div className="stat-meta">
                    {reconciliationIssues
                      ? formatCompactCoins(reconciliationIssues.summary.missing_payout_total_amount)
                      : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Orphaned Credit Blocks</div>
                  <div className="value mono">{reconciliationIssues?.summary.orphaned_block_issue_count ?? '-'}</div>
                  <div className="stat-meta">
                    {reconciliationIssues
                      ? formatCompactCoins(reconciliationIssues.summary.orphaned_block_total_credit_amount)
                      : '-'}
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Snapshot</div>
                  <div className="value mono">
                    {reconciliationIssues ? timeAgo(reconciliationIssues.generated_at) : '-'}
                  </div>
                </div>
              </div>

              {!reconciliationIssues ? (
                <p className="section-lead" style={{ marginBottom: 0 }}>
                  Load the current recovery state to inspect outstanding accounting exceptions.
                </p>
              ) : (
                <>
                  <div className="card table-scroll" style={{ marginBottom: 16 }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Missing Completed Payout</th>
                          <th>Known Source Mix</th>
                          <th>Addresses</th>
                          <th>Completed</th>
                          <th>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!reconciliationIssues.missing_payouts.length ? (
                          <tr>
                            <td colSpan={5} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                              No missing completed payouts need operator action
                            </td>
                          </tr>
                        ) : (
                          reconciliationIssues.missing_payouts.map((issue) => {
                            const restoreBusy = reconciliationBusyKey === `payout:${issue.tx_hash}:restore_pending`;
                            const dropBusy = reconciliationBusyKey === `payout:${issue.tx_hash}:drop_paid`;
                            return (
                              <tr key={issue.tx_hash}>
                                <td style={{ minWidth: 260 }}>
                                  <div className="mono" style={{ fontWeight: 600 }}>
                                    {issue.tx_hash}
                                  </div>
                                  <div style={{ marginTop: 6 }}>
                                    {formatCoins(issue.total_amount)} across {issue.payout_row_count} payout row
                                    {issue.payout_row_count === 1 ? '' : 's'}
                                  </div>
                                  <div className="stat-meta" style={{ marginTop: 6 }}>
                                    Fee {formatCoins(issue.total_fee)}
                                  </div>
                                </td>
                                <td style={{ minWidth: 220 }}>
                                  <div>Live linked: {formatCoins(issue.live_linked_amount)}</div>
                                  <div>Orphaned linked: {formatCoins(issue.orphaned_linked_amount)}</div>
                                  <div>Unlinked: {formatCoins(issue.unlinked_amount)}</div>
                                  <div className="stat-meta" style={{ marginTop: 6 }}>
                                    {reconciliationRecommendation(issue)}
                                  </div>
                                </td>
                                <td style={{ minWidth: 220 }}>
                                  <div>
                                    {issue.addresses.slice(0, 3).map((address) => (
                                      <div key={address} className="mono">
                                        {shortAddr(address)}
                                      </div>
                                    ))}
                                  </div>
                                  {issue.addresses.length > 3 ? (
                                    <div className="stat-meta" style={{ marginTop: 6 }}>
                                      +{issue.addresses.length - 3} more
                                    </div>
                                  ) : null}
                                </td>
                                <td title={new Date(toUnixMs(issue.latest_timestamp)).toLocaleString()}>
                                  {timeAgo(issue.latest_timestamp)}
                                </td>
                                <td style={{ minWidth: 220 }}>
                                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                                    <button
                                      className="btn btn-secondary"
                                      disabled={reconciliationBusyKey != null}
                                      onClick={() =>
                                        void resolveReconciliationPayoutIssue(issue.tx_hash, 'restore_pending')
                                      }
                                    >
                                      {restoreBusy ? 'Restoring…' : 'Restore Pending'}
                                    </button>
                                    <button
                                      className="btn btn-secondary"
                                      disabled={reconciliationBusyKey != null}
                                      onClick={() => void resolveReconciliationPayoutIssue(issue.tx_hash, 'drop_paid')}
                                    >
                                      {dropBusy ? 'Dropping…' : 'Drop Paid'}
                                    </button>
                                  </div>
                                </td>
                              </tr>
                            );
                          })
                        )}
                      </tbody>
                    </table>
                  </div>

                  <div className="card table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Orphaned Block Credit Cleanup</th>
                          <th>Remaining Credits</th>
                          <th>Current Blockers</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!reconciliationIssues.orphaned_blocks.length ? (
                          <tr>
                            <td colSpan={4} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                              No orphaned blocks still have lingering credit artifacts
                            </td>
                          </tr>
                        ) : (
                          reconciliationIssues.orphaned_blocks.map((issue) => {
                            const retryBusy = reconciliationBusyKey === `block:${issue.height}`;
                            return (
                              <tr key={`${issue.height}-${issue.hash}`}>
                                <td style={{ minWidth: 240 }}>
                                  <div style={{ fontWeight: 600 }}>#{issue.height}</div>
                                  <div className="mono" style={{ marginTop: 6 }}>
                                    {issue.hash.slice(0, 16)}
                                  </div>
                                  <div className="stat-meta" style={{ marginTop: 6 }}>
                                    {issue.credited_address_count} credited address
                                    {issue.credited_address_count === 1 ? '' : 'es'} · {issue.credit_event_count} credit
                                    row{issue.credit_event_count === 1 ? '' : 's'}
                                  </div>
                                </td>
                                <td style={{ minWidth: 220 }}>
                                  <div>Miner credits: {formatCoins(issue.remaining_credit_amount)}</div>
                                  <div>Paid markers: {formatCoins(issue.paid_credit_amount)}</div>
                                  <div>Fee credits: {formatCoins(issue.remaining_fee_amount)}</div>
                                  {issue.paid_fee_amount > 0 ? (
                                    <div>Fee paid markers: {formatCoins(issue.paid_fee_amount)}</div>
                                  ) : null}
                                </td>
                                <td style={{ minWidth: 180 }}>
                                  <div>{issue.pending_payout_count} pending payout row{issue.pending_payout_count === 1 ? '' : 's'}</div>
                                  <div>{issue.broadcast_pending_payout_count} broadcast pending</div>
                                </td>
                                <td>
                                  <button
                                    className="btn btn-secondary"
                                    disabled={reconciliationBusyKey != null}
                                    onClick={() => void retryOrphanedBlockCleanup(issue.height)}
                                  >
                                    {retryBusy ? 'Retrying…' : 'Retry Cleanup'}
                                  </button>
                                </td>
                              </tr>
                            );
                          })
                        )}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>

            <div className="stats-grid" style={{ marginBottom: 16 }}>
              {[
                { key: 'primary', item: recoveryPrimary },
                { key: 'standby', item: recoveryStandby },
              ].map(({ key, item }) => (
                <div key={key} className="card section" style={{ minWidth: 0 }}>
                  <div className="section-header" style={{ marginBottom: 10 }}>
                    <div>
                      <h3>{item?.instance ? recoveryInstanceLabel(item.instance) : key === 'primary' ? 'Primary' : 'Standby'}</h3>
                      <p className="section-lead">{item?.service ?? 'No status yet'}</p>
                    </div>
                    <span className={`badge ${recoveryStateBadgeClass(item?.state)}`}>
                      {recoveryStateLabel(item?.state)}
                    </span>
                  </div>

                  <div className="stats-grid stats-grid-dense">
                    <div className="stat-card">
                      <div className="label">Service</div>
                      <div className="value mono">{item?.service_state ?? '-'}</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Chain Height</div>
                      <div className="value mono">{item?.chain_height ?? '-'}</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Peers</div>
                      <div className="value mono">{item?.peers ?? '-'}</div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Wallet</div>
                      <div className="value">
                        {item?.wallet.loaded
                          ? item.wallet.address
                            ? shortAddr(item.wallet.address)
                            : 'Loaded'
                          : 'Not loaded'}
                      </div>
                    </div>
                    <div className="stat-card">
                      <div className="label">Wallet Sync</div>
                      <div className="value mono">{formatRecoveryWalletSync(item ?? null)}</div>
                      {recoveryWalletLagLabel(item ?? null) ? (
                        <div className="label" style={{ marginTop: 6 }}>
                          {recoveryWalletLagLabel(item ?? null)}
                        </div>
                      ) : null}
                    </div>
                    <div className="stat-card">
                      <div className="label">Spendable</div>
                      <div className="value mono">
                        {item?.wallet.spendable != null ? formatCoins(item.wallet.spendable) : '-'}
                      </div>
                      {item?.wallet.pending_unconfirmed != null && item.wallet.pending_unconfirmed > 0 ? (
                        <div className="label" style={{ marginTop: 6 }}>
                          {formatCoins(item.wallet.pending_unconfirmed)} unconfirmed
                          {item.wallet.pending_unconfirmed_eta != null && item.wallet.pending_unconfirmed_eta > 0
                            ? ` · ~${fmtSeconds(item.wallet.pending_unconfirmed_eta)}`
                            : ''}
                        </div>
                      ) : null}
                    </div>
                    <div className="stat-card">
                      <div className="label">Wallet Outputs</div>
                      <div className="value mono">{item?.wallet.outputs_total ?? '-'}</div>
                      {item?.wallet.outputs_unspent != null ? (
                        <div className="label" style={{ marginTop: 6 }}>
                          {item.wallet.outputs_unspent} unspent
                          {item.wallet.outputs_pending != null ? ` · ${item.wallet.outputs_pending} pending` : ''}
                        </div>
                      ) : null}
                    </div>
                    <div className="stat-card">
                      <div className="label">Cookie</div>
                      <div className="value">{item?.cookie_present ? 'Present' : 'Missing'}</div>
                    </div>
                  </div>

                  <div style={{ marginTop: 12, fontSize: 12, color: 'var(--muted)' }}>
                    <div>API: <span className="mono">{item?.api ?? '-'}</span></div>
                    <div>Wallet: <span className="mono">{item?.wallet_path ?? '-'}</span></div>
                    <div>Data: <span className="mono">{item?.data_dir ?? '-'}</span></div>
                    {item?.error ? <div style={{ color: 'var(--bad)', marginTop: 8 }}>{item.error}</div> : null}
                  </div>
                </div>
              ))}
            </div>

            <div className="card table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Operation</th>
                    <th>Target</th>
                    <th>Status</th>
                    <th>Started</th>
                    <th>Finished</th>
                    <th>Message</th>
                  </tr>
                </thead>
                <tbody>
                  {!recoveryStatus?.operations?.length ? (
                    <tr>
                      <td colSpan={6} style={{ textAlign: 'center', color: 'var(--muted)' }}>
                        No recovery operations yet
                      </td>
                    </tr>
                  ) : (
                    recoveryStatus.operations.map((operation) => (
                      <tr key={operation.id}>
                        <td>{recoveryOperationLabel(operation.kind)}</td>
                        <td>{operation.target ? recoveryInstanceLabel(operation.target) : '-'}</td>
                        <td>
                          <span
                            className={`badge ${
                              operation.state === 'succeeded'
                                ? 'badge-confirmed'
                                : operation.state === 'failed'
                                  ? 'badge-orphaned'
                                  : 'badge-pending'
                            }`}
                          >
                            {recoveryOperationStateLabel(operation.state)}
                          </span>
                        </td>
                        <td title={operation.started_at ? new Date(toUnixMs(operation.started_at)).toLocaleString() : undefined}>
                          {operation.started_at ? timeAgo(operation.started_at) : '-'}
                        </td>
                        <td title={operation.finished_at ? new Date(toUnixMs(operation.finished_at)).toLocaleString() : undefined}>
                          {operation.finished_at ? timeAgo(operation.finished_at) : '-'}
                        </td>
                        <td style={{ maxWidth: 360 }}>{operation.message || '-'}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {recoveryLatestOperation ? (
              <p style={{ marginTop: 10, fontSize: 12, color: 'var(--muted)' }}>
                Latest: {recoveryOperationLabel(recoveryLatestOperation.kind)}
                {' · '}
                {recoveryOperationStateLabel(recoveryLatestOperation.state)}
                {recoveryLatestOperation.message ? ` · ${recoveryLatestOperation.message}` : ''}
              </p>
            ) : null}
          </div>

          <div style={{ display: tab === 'logs' ? '' : 'none' }}>
            <div className="filter-bar">
              <span style={{ fontSize: 13, color: 'var(--muted)' }}>
                <span className={`status-dot ${daemonLogsStatusDot}`} />
                {daemonLogsStatusText}
              </span>

              <select value={String(daemonLogsTail)} onChange={(e) => setDaemonLogsTail(Number(e.target.value) || 200)}>
                <option value="100">Tail 100</option>
                <option value="200">Tail 200</option>
                <option value="500">Tail 500</option>
                <option value="1000">Tail 1000</option>
              </select>

              <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: 'var(--muted)' }}>
                <input
                  type="checkbox"
                  checked={daemonLogsAutoScroll}
                  onChange={(e) => setDaemonLogsAutoScroll(e.target.checked)}
                />
                Auto-scroll
              </label>

              <button
                className="btn btn-secondary"
                onClick={() => setDaemonLogsConnectSeq((seq) => seq + 1)}
              >
                Reconnect
              </button>
              <button className="btn btn-secondary" onClick={() => setDaemonLogs([])}>
                Clear
              </button>
            </div>

            {daemonLogsError ? (
              <p style={{ fontSize: 13, color: 'var(--bad)', marginBottom: 10 }}>{daemonLogsError}</p>
            ) : null}

            <div ref={daemonLogsRef} className="log-stream">
              {daemonLogs.length ? (
                daemonLogs.map((line) => (
                  <div key={line.id} className="log-line">
                    {line.segments.map((segment, index) => (
                      <span key={index} className="log-segment" style={segment.style}>
                        {segment.text}
                      </span>
                    ))}
                  </div>
                ))
              ) : (
                <div className="log-line log-line-placeholder">No daemon log lines yet.</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
