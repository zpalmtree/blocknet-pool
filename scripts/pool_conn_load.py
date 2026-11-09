#!/usr/bin/env python3
"""
Connection-scale Stratum load generator for blocknet-pool.

Opens many mostly idle miner connections (asyncio, one process per shard),
submits shares at a low Poisson rate per connection, and records when every
connection receives the job for each new block height. The spread between
the first and last connection to receive a height's job is the pool's job
fanout time at that connection count, which is what limits how many miners
one gateway and engine can serve.

Times are wall-clock so shards on the same host can be merged:
  python3 scripts/pool_conn_load.py --shard 0 --shards 8 ... --out s0.json
  python3 scripts/pool_conn_load.py --merge s*.json
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import random
import resource
import sys
import time
from collections import Counter, defaultdict


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--merge", nargs="+", help="Merge shard outputs and print the summary.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3333)
    p.add_argument("--connections", type=int, default=1000, help="Total connections across all shards.")
    p.add_argument("--shard", type=int, default=0)
    p.add_argument("--shards", type=int, default=1)
    p.add_argument("--source-ips", default="", help="Range such as 127.0.1.1-127.0.1.64; connections round-robin.")
    p.add_argument("--addresses-file", required=False, help="One miner address per line.")
    p.add_argument("--workers-per-address", type=int, default=50)
    p.add_argument("--shares-per-sec", type=float, default=0.003, help="Mean submit rate per connection.")
    p.add_argument("--ramp-secs", type=float, default=60.0, help="Spread connection starts over this long.")
    p.add_argument("--duration-secs", type=float, default=300.0, help="Measured time after the ramp.")
    p.add_argument("--login-timeout-secs", type=float, default=60.0)
    p.add_argument("--claimed-hash", default="00" * 31 + "02",
                   help="Hash claimed for every share; matches the loadtest-pow build's hasher.")
    p.add_argument("--out", default="", help="Write this shard's raw results here (JSON).")
    return p.parse_args()


def ip_range(spec: str) -> list[str]:
    if not spec:
        return [""]
    first, _, last = spec.partition("-")
    a, b = int(ipaddress.ip_address(first)), int(ipaddress.ip_address(last or first))
    return [str(ipaddress.ip_address(i)) for i in range(a, b + 1)]


class Stats:
    def __init__(self) -> None:
        self.connected = 0
        self.logged_in = 0
        self.login_failed = Counter()
        self.disconnects = Counter()
        self.submitted = 0
        self.accepted = 0
        self.rejected = Counter()
        self.ack_ms: list[float] = []
        # height -> receive wall times, one per connection (first job at that height)
        self.job_rx: dict[int, list[float]] = defaultdict(list)
        self.window_start = 0.0
        self.window_end = 0.0


class Miner:
    def __init__(self, idx: int, args, address: str, source_ip: str, stats: Stats, stop: float) -> None:
        self.idx, self.args, self.address, self.source_ip = idx, args, address, source_ip
        self.stats, self.stop = stats, stop
        self.job: dict | None = None
        self.nonce = 0
        self.req = 1
        self.pending: dict[int, float] = {}
        self.seen_heights: set[int] = set()

    async def run(self) -> None:
        a = self.args
        try:
            local = (self.source_ip, 0) if self.source_ip else None
            reader, writer = await asyncio.open_connection(a.host, a.port, local_addr=local, limit=1 << 16)
        except OSError as exc:
            self.stats.login_failed[f"connect: {exc.errno}"] += 1
            return
        self.stats.connected += 1
        worker = f"lt-{a.shard}-{self.idx}"
        writer.write((json.dumps({"id": 1, "method": "login", "params": {
            "address": self.address, "worker": worker, "protocol_version": 2,
            "capabilities": ["submit_claimed_hash"]}}) + "\n").encode())
        submitter = None
        try:
            logged_in = False
            deadline = time.monotonic() + a.login_timeout_secs
            while True:
                timeout = None if logged_in else max(0.1, deadline - time.monotonic())
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                except asyncio.TimeoutError:
                    self.stats.login_failed["login timeout"] += 1
                    return
                if not line:
                    self.stats.disconnects["closed by pool" if logged_in else "closed before login"] += 1
                    return
                now = time.time()
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                method = msg.get("method")
                if method == "job":
                    self.on_job(msg.get("params") or {}, now)
                    continue
                if method == "notification":
                    kind = (msg.get("params") or {}).get("kind")
                    if kind == "connection_limit":
                        self.stats.login_failed["connection_limit"] += 1
                        return
                    continue
                mid = msg.get("id")
                if mid == 1 and not logged_in:
                    if msg.get("status") == "ok":
                        logged_in = True
                        self.stats.logged_in += 1
                        submitter = asyncio.create_task(self.submit_loop(writer))
                    else:
                        self.stats.login_failed[str(msg.get("error"))[:80]] += 1
                        return
                    continue
                sent = self.pending.pop(mid, None)
                if sent is None:
                    continue
                if self.stats.window_start <= sent <= self.stats.window_end:
                    self.stats.ack_ms.append((now - sent) * 1000)
                    if msg.get("status") == "ok" or (msg.get("result") or {}).get("status") == "OK":
                        self.stats.accepted += 1
                    else:
                        err = str(msg.get("error") or "rejected")
                        self.stats.rejected[err.split(":")[0][:60]] += 1
        except (ConnectionError, OSError):
            self.stats.disconnects["reset"] += 1
        finally:
            if submitter:
                submitter.cancel()
            writer.close()

    def on_job(self, params: dict, now: float) -> None:
        self.job = params
        self.nonce = int(params.get("nonce_start") or 0)
        height = params.get("height")
        if isinstance(height, int) and height not in self.seen_heights:
            self.seen_heights.add(height)
            self.stats.job_rx[height].append(now)

    async def submit_loop(self, writer) -> None:
        rate = self.args.shares_per_sec
        if rate <= 0:
            return
        while time.time() < self.stop:
            await asyncio.sleep(random.expovariate(rate))
            job = self.job
            if not job or not job.get("job_id") or not job.get("target"):
                continue
            self.req += 1
            nonce_end = int(job.get("nonce_end") or (1 << 64) - 1)
            nonce = self.nonce if self.nonce <= nonce_end else int(job.get("nonce_start") or 0)
            self.nonce = nonce + 1
            now = time.time()
            self.pending[self.req] = now
            if self.stats.window_start <= now <= self.stats.window_end:
                self.stats.submitted += 1
            writer.write((json.dumps({"id": self.req, "method": "submit", "params": {
                "job_id": job["job_id"], "nonce": nonce, "claimed_hash": self.args.claimed_hash}}) + "\n").encode())


async def run_shard(args) -> dict:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    addresses = [l.strip() for l in open(args.addresses_file) if l.strip()] if args.addresses_file else []
    if not addresses:
        sys.exit("--addresses-file with at least one address is required")
    ips = ip_range(args.source_ips)
    mine = [i for i in range(args.connections) if i % args.shards == args.shard]
    stats = Stats()
    start = time.time()
    stats.window_start = start + args.ramp_secs
    stats.window_end = stats.window_start + args.duration_secs
    stop = stats.window_end + 5
    tasks = []
    for n, i in enumerate(mine):
        address = addresses[(i // max(1, args.workers_per_address)) % len(addresses)]
        miner = Miner(i, args, address, ips[i % len(ips)], stats, stop)
        delay = args.ramp_secs * n / max(1, len(mine))
        tasks.append(asyncio.create_task(delayed(miner.run(), start + delay)))
    progress = asyncio.create_task(report(stats, stop, args))
    await asyncio.sleep(max(0.0, stop - time.time()))
    for t in tasks:
        t.cancel()
    progress.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return {
        "shard": args.shard, "connections": len(mine), "window": [stats.window_start, stats.window_end],
        "connected": stats.connected, "logged_in": stats.logged_in,
        "login_failed": dict(stats.login_failed), "disconnects": dict(stats.disconnects),
        "submitted": stats.submitted, "accepted": stats.accepted, "rejected": dict(stats.rejected),
        "ack_ms": stats.ack_ms,
        "job_rx": {str(h): v for h, v in stats.job_rx.items()},
    }


async def delayed(coro, at: float):
    await asyncio.sleep(max(0.0, at - time.time()))
    await coro


async def report(stats: Stats, stop: float, args) -> None:
    while time.time() < stop:
        await asyncio.sleep(15)
        print(f"shard {args.shard}: connected {stats.connected} logged_in {stats.logged_in} "
              f"failed {sum(stats.login_failed.values())} disconnects {sum(stats.disconnects.values())} "
              f"submitted {stats.submitted} accepted {stats.accepted}", file=sys.stderr, flush=True)


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]


def merge(paths: list[str]) -> dict:
    shards = [json.load(open(p)) for p in paths]
    window = shards[0]["window"]
    job_rx: dict[str, list[float]] = defaultdict(list)
    for s in shards:
        for h, v in s["job_rx"].items():
            job_rx[h].extend(v)
    logged_in = sum(s["logged_in"] for s in shards)
    spreads, p50s, p99s, coverage = [], [], [], []
    for h, times in job_rx.items():
        times.sort()
        first = times[0]
        if not (window[0] <= first <= window[1]):
            continue
        delays = [t - first for t in times]
        spreads.append(delays[-1])
        p50s.append(pct(delays, 0.5))
        p99s.append(pct(delays, 0.99))
        coverage.append(len(times) / max(1, logged_in))
    rejected: Counter = Counter()
    for s in shards:
        rejected.update(s["rejected"])
    ack = [x for s in shards for x in s["ack_ms"]]
    return {
        "connections": sum(s["connections"] for s in shards),
        "logged_in": logged_in,
        "login_failed": dict(sum((Counter(s["login_failed"]) for s in shards), Counter())),
        "disconnects": dict(sum((Counter(s["disconnects"]) for s in shards), Counter())),
        "blocks_measured": len(spreads),
        "job_fanout_ms": {
            "p50_of_median_delay": round(pct(p50s, 0.5) * 1000, 1),
            "p50_of_p99_delay": round(pct(p99s, 0.5) * 1000, 1),
            "p50_of_last_delay": round(pct(spreads, 0.5) * 1000, 1),
            "max_last_delay": round(max(spreads, default=float("nan")) * 1000, 1),
        },
        "job_coverage_min": round(min(coverage, default=0.0), 4),
        "submitted": sum(s["submitted"] for s in shards),
        "accepted": sum(s["accepted"] for s in shards),
        "rejected": dict(rejected),
        "ack_ms": {"p50": round(pct(ack, 0.5), 1), "p99": round(pct(ack, 0.99), 1)},
    }


def main() -> int:
    args = parse_args()
    if args.merge:
        print(json.dumps(merge(args.merge), indent=2))
        return 0
    result = asyncio.run(run_shard(args))
    text = json.dumps(result)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
