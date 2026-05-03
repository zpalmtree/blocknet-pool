import type { Range, UnixLike } from '../types';

const COIN_FORMATTER = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const COMPACT_COIN_FORMATTER = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 2,
});

const feeFormatter = (maximumFractionDigits: number) =>
  new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 4,
    maximumFractionDigits,
  });
const SMALL_FEE_FORMATTER = feeFormatter(8);
const FEE_FORMATTER = feeFormatter(4);

export function toUnixMs(val: UnixLike): number {
  if (!val) return 0;
  if (typeof val === 'number') return val < 1e12 ? val * 1000 : val;
  if (typeof val === 'object' && val.secs_since_epoch != null) {
    return val.secs_since_epoch * 1000;
  }
  if (typeof val === 'string') return new Date(val).getTime();
  return 0;
}

export function timeAgo(val: UnixLike): string {
  const ms = toUnixMs(val);
  if (!ms) return '-';

  let diff = Date.now() - ms;
  if (diff < 0) diff = 0;

  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;

  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;

  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;

  const d = Math.floor(h / 24);
  if (d < 30) return `${d}d ago`;

  return `${Math.floor(d / 30)}mo ago`;
}

export function timeUntil(val: UnixLike): string {
  const ms = toUnixMs(val);
  if (!ms) return '-';

  const diff = ms - Date.now();
  if (Math.abs(diff) < 1000) return 'now';
  if (diff < 0) return 'expired';

  const s = Math.ceil(diff / 1000);
  if (s < 60) return `in ${s}s`;

  const m = Math.floor(s / 60);
  if (m < 60) return `in ${m}m`;

  const h = Math.floor(m / 60);
  if (h < 24) return `in ${h}h`;

  const d = Math.floor(h / 24);
  if (d < 30) return `in ${d}d`;

  return `in ${Math.floor(d / 30)}mo`;
}

export function timestampTitle(val: UnixLike): string {
  const ms = toUnixMs(val);
  return ms ? new Date(ms).toLocaleString() : '';
}

export function humanRate(hps: number | null | undefined): string {
  if (!hps || !Number.isFinite(hps)) return '0 H/s';
  const units = ['H/s', 'KH/s', 'MH/s', 'GH/s', 'TH/s', 'PH/s'];
  let i = 0;
  let v = hps;
  while (v >= 1000 && i < units.length - 1) {
    v /= 1000;
    i += 1;
  }
  return `${v.toFixed(2)} ${units[i]}`;
}

export function formatCoins(sats: number | null | undefined): string {
  if (sats == null) return '0 BNT';
  return `${COIN_FORMATTER.format(sats / 1e8)} BNT`;
}

export function formatCompactCoins(sats: number | null | undefined): string {
  if (sats == null) return '0 BNT';
  const amount = sats / 1e8;
  if (!Number.isFinite(amount)) return '0 BNT';
  if (Math.abs(amount) < 1000) return `${COIN_FORMATTER.format(amount)} BNT`;
  return `${COMPACT_COIN_FORMATTER.format(amount)} BNT`;
}

export function formatCoinAmount(sats: number | null | undefined): string {
  if (sats == null) return '0.00';
  return COIN_FORMATTER.format(sats / 1e8);
}

export function formatFee(sats: number | null | undefined): string {
  if (sats == null || sats === 0) return '0 BNT';
  const v = sats / 1e8;
  return `${(v < 0.01 ? SMALL_FEE_FORMATTER : FEE_FORMATTER).format(v)} BNT`;
}

export function formatPct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '-';
  return `${value.toFixed(digits)}%`;
}

export function ratioPct(numerator: number | null | undefined, denominator: number | null | undefined): number {
  const n = numerator ?? 0;
  const d = denominator ?? 0;
  if (!Number.isFinite(n) || !Number.isFinite(d) || d <= 0) return 0;
  return (n / d) * 100;
}

export function effortLabel(effortPct: number | null | undefined): string {
  if (effortPct == null || !Number.isFinite(effortPct)) return 'loading';
  if (effortPct >= 200) return 'very overdue';
  if (effortPct >= 100) return 'overdue';
  return 'on pace';
}

export function roundToneClass(tone: string | number | null | undefined): string {
  if (typeof tone === 'number') {
    if (tone >= 200) return 'is-critical';
    if (tone >= 100) return 'is-warn';
    return 'is-ok';
  }
  if (tone === 'critical') return 'is-critical';
  if (tone === 'warn') return 'is-warn';
  return 'is-ok';
}

export function fmtSeconds(s: number): string {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

export function shortAddr(addr: string): string {
  if (!addr || addr.length <= 16) return addr || '';
  return `${addr.slice(0, 8)}…${addr.slice(-6)}`;
}

export function shortTx(tx: string): string {
  if (!tx || tx.length <= 20) return tx || '';
  return `${tx.slice(0, 6)}…${tx.slice(-6)}`;
}

export function stratumUrl(port: number | null | undefined, poolUrl?: string | null): string {
  const fallback =
    (typeof window !== 'undefined' && window.location.hostname) || 'bntpool.com';
  let host = fallback;

  if (poolUrl) {
    try {
      host = new URL(poolUrl).hostname || fallback;
    } catch {
      host = fallback;
    }
  }

  return `stratum+tcp://${host}:${port ?? 3333}`;
}

export function rangeToDurationMs(range: Range): number {
  if (range === '1h') return 3600 * 1000;
  if (range === '7d') return 7 * 86400 * 1000;
  if (range === '30d') return 30 * 86400 * 1000;
  return 24 * 3600 * 1000;
}

export function smoothChartPoints(points: { t: number; v: number }[], range: Range) {
  if (points.length < 5) return points.slice();
  let window = range === '24h' ? 7 : 5;
  if (window >= points.length) window = points.length - (points.length % 2 === 0 ? 1 : 0);
  const half = Math.floor(window / 2);

  return points.map((point, idx) => {
    const start = Math.max(0, idx - half);
    const end = Math.min(points.length - 1, idx + half);

    let weighted = 0;
    let totalWeight = 0;
    for (let i = start; i <= end; i += 1) {
      const dist = Math.abs(idx - i);
      const weight = half + 1 - dist;
      weighted += points[i].v * weight;
      totalWeight += weight;
    }

    return { t: point.t, v: totalWeight > 0 ? weighted / totalWeight : point.v };
  });
}

export function formatChartTick(ts: number, range: Range, compact: boolean): string {
  const d = new Date(ts);
  if (range === '1h' || range === '24h') {
    if (compact) {
      const hh = String(d.getHours()).padStart(2, '0');
      const mm = String(d.getMinutes()).padStart(2, '0');
      return `${hh}:${mm}`;
    }
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  if (compact) return `${d.getMonth() + 1}/${d.getDate()}`;
  return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
}
