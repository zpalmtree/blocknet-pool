import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { Pager } from '../components/Pager';
import { parseAnsiLine, type ParsedAnsiSegment } from '../lib/ansi';
import {
  fmtSeconds,
  formatCoinAmount,
  formatCoins,
  humanRate,
  shortAddr,
  timeAgo,
  timeUntil,
  toUnixMs,
} from '../lib/format';
import type {
  ActiveVerificationHold,
  AdminBalanceItem,
  AdminShareDiagnosticsResponse,
  AdminShareDiagnosticsWindow,
  AdminTab,
  BlockRewardBreakdownResponse,
  BlockItem,
  HealthResponse,
  MinerListItem,
  PagerState,
  RecoveryInstanceId,
  RecoveryInstanceStatus,
  RecoveryOperationKind,
  RecoveryStatusResponse,
  UnixLike,
} from '../types';

const MAX_DAEMON_LOG_LINES = 1000;
const DAEMON_LOG_RECONNECT_DELAY_MS = 1500;

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



function formatAdminTimestamp(value: UnixLike): string {
  const ms = toUnixMs(value);
  return ms ? new Date(ms).toLocaleString() : '-';
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
  if (hasActiveUntil(hold.validation_forced_until)) return 'Validation forced';
  return 'Active';
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
  const [shareDiagnostics, setShareDiagnostics] = useState<AdminShareDiagnosticsResponse | null>(null);

  const [balancesSearch, setBalancesSearch] = useState('');
  const [balancesSort, setBalancesSort] = useState('pending_desc');
  const [balancesItems, setBalancesItems] = useState<AdminBalanceItem[]>([]);
  const [balancesPager, setBalancesPager] = useState<PagerState>({ offset: 0, limit: 50, total: 0 });

  const [recoveryStatus, setRecoveryStatus] = useState<RecoveryStatusResponse | null>(null);
  const [recoveryActionError, setRecoveryActionError] = useState('');
  const [recoveryBusy, setRecoveryBusy] = useState<RecoveryOperationKind | null>(null);
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
    void loadShareDiagnostics();

    if (tab === 'miners') void loadMiners();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'balances') void loadBalances();
    if (tab === 'recovery') void loadRecovery();
  }, [
    active,
    apiKey,
    loadBalances,
    loadHealth,
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
    if (liveTick % 2 !== 0) return;

    // Always refresh overview data
    void loadHealth();
    void loadShareDiagnostics();

    if (tab === 'miners') void loadMiners();
    if (tab === 'rewards') {
      void loadRewardBlocks();
      if (rewardBlockInput.trim()) {
        void loadRewardBreakdown(rewardBlockInput);
      }
    }
    if (tab === 'balances') void loadBalances();
    if (tab === 'recovery') void loadRecovery();
  }, [
    active,
    apiKey,
    liveTick,
    tab,
    loadBalances,
    loadHealth,
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
  const rewardBreakdownProjected = !!rewardBreakdown && !rewardBreakdownOrphaned && !rewardBreakdown.block.paid_out;
  const activeVerificationHolds = health?.active_verification_holds ?? [];
  const poolActivity = health?.pool_activity ?? null;
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
  const shareSubmitOldestAge = Math.max(
    shareSubmit?.candidate_oldest_age_millis ?? 0,
    shareSubmit?.regular_oldest_age_millis ?? 0
  );
  const shareValidationOldestAge = Math.max(
    shareValidation?.candidate_oldest_age_millis ?? 0,
    shareValidation?.regular_oldest_age_millis ?? 0
  );
  const shareSubmitWaitP95 = Math.max(
    shareSubmit?.candidate_wait?.p95_millis ?? 0,
    shareSubmit?.regular_wait?.p95_millis ?? 0
  );
  const shareValidationWaitP95 = Math.max(
    shareValidation?.candidate_wait?.p95_millis ?? 0,
    shareValidation?.regular_wait?.p95_millis ?? 0
  );
  const shareValidationDurationP95 = shareValidation?.validation_duration?.p95_millis ?? 0;
  const shareBusy5m = shareWindowReasonPct(shareWindow5m, 'server busy');
  const shareTimeout5m = shareWindowReasonPct(shareWindow5m, 'validation timeout');
  const shareBusyCount5m = shareWindowReasonCount(shareWindow5m, 'server busy');
  const shareTimeoutCount5m = shareWindowReasonCount(shareWindow5m, 'validation timeout');
  const sharePressureSignal = useMemo(() => {
    if (!shareDiagnostics) {
      return {
        label: 'No data',
        detail: 'Waiting for runtime diagnostics from the pool.',
        tone: 'var(--muted)',
      };
    }
    if (shareValidation?.overload_mode === 'emergency') {
      return {
        label: 'Emergency shed',
        detail: 'Regular shares are being admitted with minimal verification to protect candidate processing.',
        tone: 'var(--warn)',
      };
    }
    if (shareValidation?.overload_mode === 'shed') {
      return {
        label: 'Shedding',
        detail: 'Sample rate is being reduced because the regular validation lane is backing up.',
        tone: 'var(--warn)',
      };
    }
    if (shareBusyCount5m > 0 || shareTimeoutCount5m > 0 || shareSubmitOldestAge >= 2000 || shareValidationOldestAge >= 2000) {
      return {
        label: 'Queue pressure',
        detail: 'Backlog is visible even though overload shedding has not fully tripped yet.',
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
    shareSubmitOldestAge,
    shareTimeoutCount5m,
    shareValidation?.overload_mode,
    shareValidationOldestAge,
  ]);
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
  const rewardPayoutColumnLabel = rewardBreakdownProjected ? 'Projected Payout' : 'Payout';
  const rewardPayoutWeightLabel = rewardBreakdownProjected ? 'Projected Weight' : 'Payout Weight';
  const rewardStatusColumnLabel = rewardBreakdownOrphaned
    ? 'Resolution'
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
              style={activeVerificationHolds.length > 0 ? { borderColor: 'var(--warn)' } : undefined}
              onClick={() => setTab('holds')}
            >
              <div className="label">Verification Holds</div>
              <div
                className="value mono"
                style={activeVerificationHolds.length > 0 ? { color: 'var(--warn)' } : undefined}
              >
                {activeVerificationHolds.length}
              </div>
              <div className="stat-meta">
                {activeVerificationHolds.length > 0 ? 'miners quarantined or forced-verify' : 'all clear'}
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
              <div className="label">Pending Payouts</div>
              <div className="value mono">{health?.payouts?.pending_count ?? '-'}</div>
              <div className="stat-meta">
                {health?.payouts?.pending_amount != null
                  ? `${formatCoins(health.payouts.pending_amount)} owed`
                  : '-'}
              </div>
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
                  Load a block height to inspect the current preview math, the final payout math, and any recorded
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

                <div className="card table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Address</th>
                        <th>{rewardPreviewColumnLabel}</th>
                        {rewardBreakdown.block.orphaned ? <th>Actual</th> : <th>Preview Weight</th>}
                        {rewardBreakdown.block.orphaned ? <th>Delta</th> : null}
                        {rewardBreakdown.block.orphaned ? null : <th>{rewardPayoutColumnLabel}</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>Actual</th>}
                        {rewardBreakdown.block.orphaned ? null : <th>Delta</th>}
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
                                  <div style={{ color: rewardStatusTone(row.payout_status), fontWeight: 600 }}>
                                    {rewardStatusLabel(row.payout_status)}
                                  </div>
                                  <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                                    Preview: {rewardStatusLabel(row.preview_status)}
                                    {row.risky ? ' · verification hold' : ''}
                                  </div>
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
                                      : 'var(--warn)',
                                fontWeight: 600,
                              }}
                            >
                              {rewardActualBlockDelta == null ? 'Pending' : rewardActualBlockDelta === 0 ? 'Balanced' : 'Mismatch'}
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
                              summary across the full block
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
              <div className="stats-card-group-title">Overview</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">5m Reject Rate</div>
                  <div className="value mono">{pct(shareWindow5m?.rejection_rate_pct)}</div>
                  <div className="stat-meta">{shareWindow5m?.rejected ?? 0} rejected</div>
                </div>
                <div className="stat-card">
                  <div className="label">5m Invalid Proof</div>
                  <div className="value mono">{pct(shareWindowReasonPct(shareWindow5m, 'invalid share proof'))}</div>
                  <div className="stat-meta">{shareWindowReasonCount(shareWindow5m, 'invalid share proof')} rejects</div>
                </div>
                <div className="stat-card">
                  <div className="label">1h Reject Rate</div>
                  <div className="value mono">{pct(shareWindow1h?.rejection_rate_pct)}</div>
                  <div className="stat-meta">{shareWindow1h?.rejected ?? 0} rejected</div>
                </div>
                <div className="stat-card">
                  <div className="label">24h Reject Rate</div>
                  <div className="value mono">{pct(shareWindow24h?.rejection_rate_pct)}</div>
                  <div className="stat-meta">{shareWindow24h?.rejected ?? 0} rejected</div>
                </div>
                <div className="stat-card">
                  <div className="label">Overload Mode</div>
                  <div className="value mono">{overloadModeLabel(shareValidation?.overload_mode)}</div>
                  <div className="stat-meta">{sharePressureSignal.detail}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Effective Sample Rate</div>
                  <div className="value mono">{ratioPct(shareValidation?.effective_sample_rate)}</div>
                  <div className="stat-meta">{shareValidation?.sampled_shares ?? 0} sampled shares</div>
                </div>
              </div>
            </div>

            <div className="stats-card-group">
              <div className="stats-card-group-title">Runtime Pressure</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Submit Queue</div>
                  <div className="value mono">{shareSubmitQueueDepth}</div>
                  <div className="stat-meta">
                    {shareSubmit?.candidate_queue_depth ?? 0} candidate · {shareSubmit?.regular_queue_depth ?? 0} regular
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Submit Wait P95</div>
                  <div className="value mono">{formatMillis(shareSubmitWaitP95)}</div>
                  <div className="stat-meta">oldest {formatMillis(shareSubmitOldestAge)}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Validation Queue</div>
                  <div className="value mono">{shareValidationQueueDepth}</div>
                  <div className="stat-meta">
                    {shareValidation?.candidate_queue_depth ?? 0} candidate · {shareValidation?.regular_queue_depth ?? 0} regular
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Validation Wait P95</div>
                  <div className="value mono">{formatMillis(shareValidationWaitP95)}</div>
                  <div className="stat-meta">oldest {formatMillis(shareValidationOldestAge)}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Validation Time P95</div>
                  <div className="value mono">{formatMillis(shareValidationDurationP95)}</div>
                  <div className="stat-meta">{shareValidation?.in_flight ?? 0} in flight</div>
                </div>
                <div className="stat-card">
                  <div className="label">Pending Provisional</div>
                  <div className="value mono">{shareValidation?.pending_provisional ?? '-'}</div>
                  <div className="stat-meta">{shareValidation?.forced_verify_addresses ?? 0} forced addresses</div>
                </div>
                <div className="stat-card">
                  <div className="label">5m Busy / Timeout</div>
                  <div className="value mono">
                    {shareBusyCount5m + shareTimeoutCount5m}
                  </div>
                  <div className="stat-meta">
                    {shareBusyCount5m} busy · {shareTimeoutCount5m} timeout
                  </div>
                </div>
                <div className="stat-card">
                  <div className="label">Candidate False Claims</div>
                  <div className="value mono">{shareValidation?.candidate_false_claims ?? 0}</div>
                  <div className="stat-meta" style={{ color: sharePressureSignal.tone }}>
                    {sharePressureSignal.label}
                  </div>
                </div>
              </div>
            </div>

            <div className="card table-scroll">
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

          <div style={{ display: tab === 'holds' ? '' : 'none' }}>
            <div className="stats-card-group">
              <div className="stats-card-group-title">Verification Holds</div>
              <div className="stats-card-group-grid stats-grid-dense">
                <div className="stat-card">
                  <div className="label">Active Holds</div>
                  <div className="value mono">{activeVerificationHolds.length}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Forced Verify</div>
                  <div className="value mono">{health?.validation?.forced_verify_addresses ?? '-'}</div>
                </div>
                <div className="stat-card">
                  <div className="label">Pending Provisional</div>
                  <div className="value mono">{health?.validation?.pending_provisional ?? '-'}</div>
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
                    Addresses currently quarantined or forced into verified-only share validation.
                  </p>
                </div>
              </div>
              <div style={{ marginTop: 12, fontSize: 13, color: 'var(--muted)' }}>
                <span className="mono">Validation forced</span> comes from the share validation engine after repeated
                invalid samples, suspected fraud, or too many provisional shares waiting for full verification.
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
                            {holdUntilLabel(hold.validation_forced_until)}
                          </td>
                          <td className="mono">
                            {hold.strikes}
                            {hold.suspected_fraud_strikes > 0 ? ` / fraud ${hold.suspected_fraud_strikes}` : ''}
                          </td>
                          <td title={hold.last_reason ?? undefined}>{hold.last_reason ?? '-'}</td>
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
                <option value="pending_desc">Owed (high first)</option>
                <option value="pending_asc">Owed (low first)</option>
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
                </colgroup>
                <thead>
                  <tr>
                    <th>Address</th>
                    <th>Owed</th>
                    <th>Total Paid</th>
                  </tr>
                </thead>
                <tbody>
                  {!balancesItems.length ? (
                    <tr>
                      <td colSpan={3} style={{ textAlign: 'center', color: 'var(--muted)' }}>
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
