export interface ChartPoint { t: number; v: number }

// Keep each horizontal bucket's extrema in time order, plus the endpoints.
// This bounds canvas work to its displayed resolution without hiding peaks.
export function reduceChartPoints(points: ChartPoint[], columns: number): ChartPoint[] {
  columns = Math.max(1, Math.floor(columns));
  if (points.length <= columns * 2 + 2) return points;
  const first = points[0];
  const last = points[points.length - 1];
  const span = Math.max(1, last.t - first.t);
  const result = [first];
  let bucket = -1;
  let min = 0;
  let max = 0;
  const flush = () => {
    if (bucket < 0) return;
    result.push(points[Math.min(min, max)]);
    if (min !== max) result.push(points[Math.max(min, max)]);
  };
  for (let i = 1; i < points.length - 1; i++) {
    const next = Math.min(columns - 1, Math.floor((points[i].t - first.t) / span * columns));
    if (next !== bucket) {
      flush();
      bucket = next;
      min = max = i;
    } else {
      if (points[i].v < points[min].v) min = i;
      if (points[i].v > points[max].v) max = i;
    }
  }
  flush();
  result.push(last);
  return result;
}
