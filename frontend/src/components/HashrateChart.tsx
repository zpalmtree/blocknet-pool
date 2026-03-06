import { useEffect, useRef } from 'react';

import { formatChartTick, humanRate, rangeToDurationMs, smoothChartPoints, toUnixMs } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type { HashratePoint, Range } from '../types';

function traceSeriesPath(ctx: CanvasRenderingContext2D, coords: Array<{ x: number; y: number }>) {
  ctx.beginPath();
  ctx.moveTo(coords[0].x, coords[0].y);
  if (coords.length === 2) {
    ctx.lineTo(coords[1].x, coords[1].y);
    return;
  }
  for (let i = 1; i < coords.length - 1; i += 1) {
    const midX = (coords[i].x + coords[i + 1].x) / 2;
    const midY = (coords[i].y + coords[i + 1].y) / 2;
    ctx.quadraticCurveTo(coords[i].x, coords[i].y, midX, midY);
  }
  const prev = coords[coords.length - 2];
  const last = coords[coords.length - 1];
  ctx.quadraticCurveTo(prev.x, prev.y, last.x, last.y);
}

function drawChartOn(canvas: HTMLCanvasElement, data: HashratePoint[], range: Range) {
  const ctx = canvas.getContext('2d');
  if (!ctx) return;
  const styles = getComputedStyle(document.documentElement);
  const themeColor = (name: string, fallback: string) => styles.getPropertyValue(name).trim() || fallback;
  const axisColor = themeColor('--chart-axis', '#6b7c6b');
  const gridColor = themeColor('--chart-grid', '#e2e8d8');
  const lineColor = themeColor('--chart-line', '#16a34a');
  const fillStart = themeColor('--chart-fill-start', 'rgba(22,163,74,0.15)');
  const fillEnd = themeColor('--chart-fill-end', 'rgba(22,163,74,0.01)');

  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);

  const W = rect.width;
  const H = rect.height;
  ctx.clearRect(0, 0, W, H);

  const points = (data || [])
    .map((p) => ({
      t: typeof p.timestamp === 'number' && p.timestamp < 1e12 ? p.timestamp * 1000 : toUnixMs(p.timestamp),
      v: p.hashrate || 0,
    }))
    .filter((p) => Number.isFinite(p.t) && Number.isFinite(p.v));

  if (points.length < 2) {
    ctx.fillStyle = axisColor;
    ctx.font = '14px Manrope,sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('No data yet', W / 2, H / 2);
    return;
  }

  points.sort((a, b) => a.t - b.t);

  const smoothed = smoothChartPoints(points, range);
  const values = smoothed.map((p) => p.v);
  const timestamps = smoothed.map((p) => p.t);

  const maxV = Math.max(...values) * 1.1 || 1;
  const compact = W < 640;
  const yAxisFont = `${compact ? 10 : 11}px JetBrains Mono,monospace`;
  ctx.font = yAxisFont;
  const yLabelMaxWidth = Math.max(
    ...Array.from({ length: 5 }, (_, i) => ctx.measureText(humanRate((maxV * i) / 4)).width),
  );
  const pad = {
    t: 10,
    r: compact ? 12 : 20,
    b: compact ? 32 : 30,
    l: Math.max(compact ? 54 : 64, Math.ceil(yLabelMaxWidth + (compact ? 12 : 14))),
  };
  const cW = W - pad.l - pad.r;
  const cH = H - pad.t - pad.b;
  if (cW <= 0 || cH <= 0) return;

  const endT = Math.max(Date.now(), Math.max(...timestamps));
  const startT = endT - rangeToDurationMs(range);
  const rangeT = Math.max(endT - startT, 1);

  const xFor = (ts: number) => {
    let n = (ts - startT) / rangeT;
    if (n < 0) n = 0;
    if (n > 1) n = 1;
    return pad.l + n * cW;
  };

  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.t + cH - (cH * i) / 4;
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(pad.l + cW, y);
    ctx.stroke();

    ctx.fillStyle = axisColor;
    ctx.font = yAxisFont;
    ctx.textAlign = 'right';
    ctx.fillText(humanRate((maxV * i) / 4), pad.l - 6, y + 4);
  }

  ctx.fillStyle = axisColor;
  ctx.font = `${compact ? 10 : 11}px JetBrains Mono,monospace`;
  const maxTicks = Math.max(3, Math.min(compact ? 4 : 6, Math.floor(cW / (compact ? 120 : 140)) + 1));
  let lastRight = -Infinity;
  for (let j = 0; j < maxTicks; j += 1) {
    const tickTs = startT + (rangeT * j) / Math.max(maxTicks - 1, 1);
    const x = xFor(tickTs);
    const label = formatChartTick(tickTs, range, compact);
    const w = ctx.measureText(label).width;
    let left = x - w / 2;
    let right = x + w / 2;

    if (j === 0) {
      ctx.textAlign = 'left';
      left = x;
      right = x + w;
    } else if (j === maxTicks - 1) {
      ctx.textAlign = 'right';
      left = x - w;
      right = x;
    } else {
      ctx.textAlign = 'center';
    }

    if (left <= lastRight + 8 && j !== 0 && j !== maxTicks - 1) continue;
    ctx.fillText(label, x, H - 6);
    lastRight = right;
  }

  ctx.save();
  ctx.beginPath();
  ctx.rect(pad.l - 1, pad.t - 1, cW + 2, cH + 2);
  ctx.clip();

  const coords = values.map((v, idx) => ({ x: xFor(timestamps[idx]), y: pad.t + cH - (v / maxV) * cH }));
  traceSeriesPath(ctx, coords);
  ctx.strokeStyle = lineColor;
  ctx.lineWidth = 2;
  ctx.stroke();

  traceSeriesPath(ctx, coords);
  const firstX = coords[0].x;
  const lastX = coords[coords.length - 1].x;
  ctx.lineTo(lastX, pad.t + cH);
  ctx.lineTo(firstX, pad.t + cH);
  ctx.closePath();

  const grad = ctx.createLinearGradient(0, pad.t, 0, pad.t + cH);
  grad.addColorStop(0, fillStart);
  grad.addColorStop(1, fillEnd);
  ctx.fillStyle = grad;
  ctx.fill();
  ctx.restore();
}

export function HashrateChart({ data, range, theme }: { data: HashratePoint[]; range: Range; theme: ThemeMode }) {
  const ref = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const run = () => drawChartOn(canvas, data, range);
    run();
    const onResize = () => run();
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, [data, range, theme]);

  return (
    <div className="chart-wrap">
      <canvas ref={ref} />
    </div>
  );
}
