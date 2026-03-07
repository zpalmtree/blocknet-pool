import type { Range, UnixLike } from '../types';

export const STRATUM_HOST = 'bntpool.com';

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
  return `${(sats / 1e8).toFixed(2)} BNT`;
}

export function formatCoinAmount(sats: number | null | undefined): string {
  if (sats == null) return '0.00';
  return (sats / 1e8).toFixed(2);
}

export function formatFee(sats: number | null | undefined): string {
  if (sats == null || sats === 0) return '0 BNT';
  const v = sats / 1e8;
  if (v < 0.01) return `${v.toPrecision(4)} BNT`;
  return `${v.toFixed(4)} BNT`;
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

function poolHostFromUrl(poolUrl: string | null | undefined): string {
  const fallback =
    (typeof window !== 'undefined' && window.location.hostname) || STRATUM_HOST;
  if (!poolUrl) return fallback;

  try {
    return new URL(poolUrl).hostname || fallback;
  } catch {
    return fallback;
  }
}

export function stratumUrl(port: number | null | undefined, poolUrl?: string | null): string {
  return `stratum+tcp://${poolHostFromUrl(poolUrl)}:${port ?? 3333}`;
}

export function rangeToDurationMs(range: Range): number {
  if (range === '1h') return 3600 * 1000;
  if (range === '7d') return 7 * 86400 * 1000;
  if (range === '30d') return 30 * 86400 * 1000;
  return 24 * 3600 * 1000;
}

function smoothingWindowForRange(range: Range, count: number): number {
  if (count < 5) return 1;
  let window = 5;
  if (range === '24h') window = 7;
  if (range === '1h') window = 5;
  if (window >= count) window = count - (count % 2 === 0 ? 1 : 0);
  if (window < 3) return 1;
  if (window % 2 === 0) window -= 1;
  return window;
}

export function smoothChartPoints(points: { t: number; v: number }[], range: Range) {
  const window = smoothingWindowForRange(range, points.length);
  if (window < 3) return points.slice();
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
