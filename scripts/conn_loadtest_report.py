#!/usr/bin/env python3
"""Tabulates conn_loadtest.sh steps: python3 scripts/conn_loadtest_report.py results/*"""

import json
import sys
from pathlib import Path


def window_samples(step: Path, start: float, end: float):
    cpu = {"gateway": [], "stratum": []}
    rss = {"gateway": [], "stratum": []}
    wait = {}
    for line in (step / "samples.txt").read_text().splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        ts = float(parts[0])
        if not start <= ts <= end:
            continue
        if parts[1] in cpu:
            fields = dict(p.split("=") for p in parts[2:])
            cpu[parts[1]].append(float(fields["cpu"]))
            rss[parts[1]].append(int(fields["rss_kb"]) / 1024)
        elif "latency_millis_" in parts[1]:
            lane = "miner" if "miner_line_wait" in parts[1] else "engine"
            kind = "sum" if "_sum" in parts[1] else "count"
            wait.setdefault((lane, kind), []).append(float(parts[2]))
    mean_wait = {}
    for lane in ("miner", "engine"):
        s, c = wait.get((lane, "sum"), []), wait.get((lane, "count"), [])
        if len(s) > 1 and len(c) > 1 and c[-1] > c[0]:
            mean_wait[lane] = (s[-1] - s[0]) / (c[-1] - c[0])
    avg = lambda v: sum(v) / len(v) if v else float("nan")
    return cpu, rss, mean_wait, avg


def hist_pct(h: dict, q: float) -> float:
    counts, bounds = h.get("counts", []), h.get("bounds_millis", [])
    total = sum(counts)
    if not total:
        return float("nan")
    seen = 0
    for i, c in enumerate(counts):
        seen += c
        if seen >= q * total:
            return float(bounds[i]) if i < len(bounds) else float("inf")
    return float("inf")


def engine_fanout(step: Path):
    path = step / "runtime_snapshot.json"
    if not path.exists() or not path.read_text().strip():
        return float("nan"), float("nan")
    fan = json.loads(path.read_text()).get("latency", {}).get("job_fanout", {})
    h = fan.get("template_to_written", {})
    return hist_pct(h, 0.5), hist_pct(h, 0.99)


def main() -> int:
    rows = []
    for arg in sys.argv[1:]:
        step = Path(arg)
        summary = json.loads((step / "summary.json").read_text())
        shard = json.loads(next(step.glob("shard-*.json")).read_text())
        start, end = shard["window"]
        cpu, rss, wait, avg = window_samples(step, start, end)
        fan = summary["job_fanout_ms"]
        eng_p50, eng_p99 = engine_fanout(step)
        rows.append((
            summary["connections"], summary["logged_in"], summary["blocks_measured"],
            fan["p50_of_median_delay"], fan["p50_of_p99_delay"], fan["p50_of_last_delay"], fan["max_last_delay"],
            summary["job_coverage_min"],
            avg(cpu["gateway"]), max(cpu["gateway"], default=float("nan")),
            avg(cpu["stratum"]), max(cpu["stratum"], default=float("nan")),
            max(rss["stratum"], default=float("nan")),
            wait.get("miner", float("nan")), wait.get("engine", float("nan")),
            summary["accepted"], sum(summary["rejected"].values()), summary["ack_ms"]["p99"],
            eng_p50, eng_p99,
        ))
    head = ("conns", "logged", "blocks", "fan_p50", "fan_p99", "fan_last", "fan_max", "cover",
            "gw_cpu", "gw_max", "eng_cpu", "eng_max", "eng_mb", "gw_wait_line", "gw_wait_frame",
            "accepted", "rejected", "ack_p99", "eng_written_p50<=", "eng_written_p99<=")
    print(" | ".join(head))
    for r in sorted(rows):
        print(" | ".join(f"{v:.1f}" if isinstance(v, float) else str(v) for v in r))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
