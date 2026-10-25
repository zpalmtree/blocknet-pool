#!/usr/bin/env python3
"""
Multi-worker Stratum load generator for blocknet-pool.

This is intended for local performance validation and A/B benchmarking.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import socket
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Stratum share load against blocknet-pool."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Pool host.")
    parser.add_argument(
        "--stratum-port", type=int, required=True, help="Pool stratum TCP port."
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=0,
        help="Optional pool API port for sampling /api/stats (0 disables sampling).",
    )
    parser.add_argument(
        "--workers", type=int, default=64, help="Number of worker connections."
    )
    parser.add_argument(
        "--duration-secs", type=float, default=60.0, help="Run duration in seconds."
    )
    parser.add_argument(
        "--shares-per-worker-per-sec",
        type=float,
        default=5.0,
        help="Share submit rate per worker connection.",
    )
    parser.add_argument(
        "--response-timeout-secs",
        type=float,
        default=15.0,
        help="Socket read timeout for stratum responses.",
    )
    parser.add_argument(
        "--login-timeout-secs",
        type=float,
        default=10.0,
        help="Timeout waiting for login + first job.",
    )
    parser.add_argument(
        "--stats-sample-interval-secs",
        type=float,
        default=1.0,
        help="Sampling interval for /api/stats when --api-port is set.",
    )
    parser.add_argument(
        "--worker-prefix",
        default="bench",
        help="Prefix for worker names.",
    )
    parser.add_argument(
        "--address-prefix",
        default="bench_addr",
        help="Prefix for miner addresses used in login.",
    )
    parser.add_argument(
        "--address-mode",
        choices=("unique", "shared"),
        default="unique",
        help="Use unique address per worker or one shared address.",
    )
    parser.add_argument(
        "--shared-address",
        default="bench_shared",
        help="Address to use when --address-mode shared.",
    )
    parser.add_argument(
        "--jitter-ms",
        type=float,
        default=25.0,
        help="Start jitter window per worker in milliseconds.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional JSON output path. Printed to stdout regardless.",
    )
    parser.add_argument(
        "--hashrate",
        type=float,
        default=0.0,
        help=(
            "Simulated hashes/s per connection. When > 0 the generator models a "
            "real miner instead of a fixed submit rate: each connection finds "
            "shares as a Poisson process with rate hashrate/difficulty (so vardiff "
            "changes its share rate), submits without waiting for responses, and "
            "never resubmits a rejected share. Shares are still fake "
            "(claimed_hash = target)."
        ),
    )
    parser.add_argument(
        "--addresses-file",
        default="",
        help="File with one valid miner address per line (hashrate mode).",
    )
    parser.add_argument(
        "--address-offset",
        type=int,
        default=0,
        help="First line of --addresses-file to use (hashrate mode).",
    )
    parser.add_argument(
        "--source-ips",
        default="",
        help=(
            "Comma-separated local IPs (or a.b.c.N-M ranges) to bind connections "
            "to round-robin, e.g. 127.0.1.1-127.0.1.30 (hashrate mode)."
        ),
    )
    parser.add_argument(
        "--ramp-secs",
        type=float,
        default=0.0,
        help="Spread connection starts over this many seconds (hashrate mode).",
    )
    parser.add_argument(
        "--label",
        default="",
        help="Population label recorded in the output (hashrate mode).",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    idx = (len(sorted_values) - 1) * p
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return sorted_values[lo]
    frac = idx - lo
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac


def fetch_api_stats(host: str, api_port: int, timeout: float = 2.0) -> Optional[dict]:
    if api_port <= 0:
        return None
    url = f"http://{host}:{api_port}/api/stats"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            payload = resp.read()
    except (urllib.error.URLError, TimeoutError, ValueError):
        return None
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError:
        return None


class ApiSampler(threading.Thread):
    def __init__(self, host: str, api_port: int, interval_secs: float) -> None:
        super().__init__(daemon=True)
        self.host = host
        self.api_port = api_port
        self.interval_secs = max(0.1, interval_secs)
        self.stop_event = threading.Event()
        self.sample_count = 0

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            snapshot = fetch_api_stats(self.host, self.api_port)
            if snapshot is not None:
                self.sample_count += 1
            self.stop_event.wait(self.interval_secs)


@dataclass
class WorkerMetrics:
    worker_id: int
    submitted: int = 0
    responses: int = 0
    accepted: int = 0
    rejected: int = 0
    transport_errors: int = 0
    protocol_errors: int = 0
    login_failed: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    error_reasons: Dict[str, int] = field(default_factory=dict)

    def add_error_reason(self, reason: str) -> None:
        key = reason.strip() or "unknown"
        self.error_reasons[key] = self.error_reasons.get(key, 0) + 1


class Worker(threading.Thread):
    def __init__(
        self,
        worker_id: int,
        host: str,
        port: int,
        address: str,
        worker_name: str,
        shares_per_sec: float,
        login_timeout_secs: float,
        response_timeout_secs: float,
        stop_deadline: float,
        jitter_ms: float,
    ) -> None:
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.address = address
        self.worker_name = worker_name
        self.shares_per_sec = max(0.0, shares_per_sec)
        self.login_timeout_secs = login_timeout_secs
        self.response_timeout_secs = response_timeout_secs
        self.stop_deadline = stop_deadline
        self.jitter_ms = max(0.0, jitter_ms)
        self.metrics = WorkerMetrics(worker_id=worker_id)
        self._nonce = 0
        self._nonce_start = 0
        self._nonce_end = (1 << 64) - 1
        self._job_id: Optional[str] = None
        self._claimed_hash: Optional[str] = None

    def run(self) -> None:
        try:
            self._run_inner()
        except Exception as exc:  # pylint: disable=broad-except
            self.metrics.transport_errors += 1
            self.metrics.add_error_reason(f"worker exception: {exc}")

    def _run_inner(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=5.0) as sock:
            sock.settimeout(self.response_timeout_secs)
            reader = sock.makefile("r", encoding="utf-8", newline="\n")
            writer = sock.makefile("w", encoding="utf-8", newline="\n")
            try:
                self._login(reader, writer)
                self._submit_loop(reader, writer)
            finally:
                try:
                    writer.close()
                except Exception:  # pylint: disable=broad-except
                    pass
                try:
                    reader.close()
                except Exception:  # pylint: disable=broad-except
                    pass

    def _send_json(self, writer, payload: dict) -> None:
        writer.write(json.dumps(payload, separators=(",", ":")) + "\n")
        writer.flush()

    def _read_json(self, reader) -> dict:
        try:
            raw = reader.readline()
        except socket.timeout as exc:
            raise TimeoutError("stratum read timeout") from exc
        if raw == "":
            raise ConnectionError("stratum connection closed")
        line = raw.strip()
        if not line:
            return {}
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid json from pool: {line}") from exc
        if not isinstance(obj, dict):
            raise ValueError("stratum payload is not an object")
        return obj

    def _update_job(self, msg: dict) -> bool:
        method = msg.get("method")
        if method != "job":
            return False
        params = msg.get("params")
        if not isinstance(params, dict):
            return True
        job_id = params.get("job_id")
        if isinstance(job_id, str) and job_id:
            self._job_id = job_id
        target = params.get("target")
        if isinstance(target, str) and len(target) == 64:
            self._claimed_hash = target
        nonce_start = params.get("nonce_start")
        nonce_end = params.get("nonce_end")
        if isinstance(nonce_start, int) and nonce_start >= 0:
            self._nonce_start = nonce_start
        if isinstance(nonce_end, int) and nonce_end >= self._nonce_start:
            self._nonce_end = nonce_end
        self._nonce = self._nonce_start
        return True

    def _login(self, reader, writer) -> None:
        login_id = 1
        self._send_json(
            writer,
            {
                "id": login_id,
                "method": "login",
                "params": {
                    "address": self.address,
                    "worker": self.worker_name,
                    "protocol_version": 2,
                    "capabilities": ["submit_claimed_hash"],
                },
            },
        )

        login_ok = False
        deadline = time.monotonic() + self.login_timeout_secs
        while time.monotonic() < deadline:
            msg = self._read_json(reader)
            if self._update_job(msg):
                if login_ok and self._job_id is not None:
                    return
                continue
            msg_id = msg.get("id")
            if msg_id != login_id:
                continue
            status = msg.get("status")
            if status == "ok":
                login_ok = True
            else:
                self.metrics.login_failed += 1
                self.metrics.add_error_reason(str(msg.get("error") or "login failed"))
                return
            if login_ok and self._job_id is not None:
                return

        if not login_ok:
            self.metrics.login_failed += 1
            self.metrics.add_error_reason("login timeout")
        elif self._job_id is None:
            self.metrics.login_failed += 1
            self.metrics.add_error_reason("job notify timeout")

    def _next_nonce(self) -> int:
        nonce = self._nonce
        if nonce >= self._nonce_end:
            self._nonce = self._nonce_start
        else:
            self._nonce += 1
        return nonce

    def _submit_loop(self, reader, writer) -> None:
        if self._job_id is None:
            return

        req_id = 2
        if self.jitter_ms > 0:
            jitter = random.random() * (self.jitter_ms / 1000.0)
            time.sleep(jitter)

        period = 0.0 if self.shares_per_sec <= 0 else 1.0 / self.shares_per_sec
        next_submit = time.monotonic()

        while time.monotonic() < self.stop_deadline:
            now = time.monotonic()
            if period > 0 and now < next_submit:
                time.sleep(min(next_submit - now, 0.01))
                continue

            if self._job_id is None:
                self.metrics.protocol_errors += 1
                self.metrics.add_error_reason("missing active job")
                break
            if self._claimed_hash is None:
                self.metrics.protocol_errors += 1
                self.metrics.add_error_reason("missing active job target")
                break

            params = {
                "job_id": self._job_id,
                "nonce": self._next_nonce(),
                "claimed_hash": self._claimed_hash,
            }

            request = {
                "id": req_id,
                "method": "submit",
                "params": params,
            }
            submit_t0 = time.perf_counter()
            self._send_json(writer, request)
            self.metrics.submitted += 1

            try:
                while True:
                    msg = self._read_json(reader)
                    if self._update_job(msg):
                        continue

                    msg_id = msg.get("id")
                    if msg_id != req_id:
                        continue

                    self.metrics.responses += 1
                    self.metrics.latencies_ms.append((time.perf_counter() - submit_t0) * 1000.0)
                    if msg.get("status") == "ok":
                        self.metrics.accepted += 1
                    else:
                        self.metrics.rejected += 1
                        self.metrics.add_error_reason(str(msg.get("error") or "unknown"))
                    break
            except TimeoutError:
                self.metrics.transport_errors += 1
                self.metrics.add_error_reason("submit timeout")
            except (ConnectionError, ValueError) as exc:
                self.metrics.transport_errors += 1
                self.metrics.add_error_reason(str(exc))
                break

            req_id += 1
            if period > 0:
                next_submit += period


def merge_error_reasons(metrics: List[WorkerMetrics]) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    for item in metrics:
        for key, value in item.error_reasons.items():
            merged[key] = merged.get(key, 0) + value
    return dict(sorted(merged.items(), key=lambda kv: (-kv[1], kv[0])))


def summarize_api_samples(
    start_snapshot: Optional[dict], end_snapshot: Optional[dict], sample_count: int
) -> dict:
    if start_snapshot is None and end_snapshot is None and sample_count == 0:
        return {"available": False}

    return {
        "available": True,
        "samples": sample_count,
        "snapshot_start": start_snapshot or {},
        "snapshot_end": end_snapshot or {},
    }


def expand_source_ips(spec: str) -> List[str]:
    ips: List[str] = []
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        if "-" in part:
            first, last = part.split("-", 1)
            prefix, start = first.rsplit(".", 1)
            if "." in last:
                last = last.rsplit(".", 1)[1]
            for octet in range(int(start), int(last) + 1):
                ips.append(f"{prefix}.{octet}")
        else:
            ips.append(part)
    return ips


def classify_reject(error: str) -> str:
    text = error.strip()
    if text.startswith("server busy"):
        return "busy"
    if text.startswith("rate limited"):
        return "rate_limited"
    return "other"


class HashrateConn:
    """One simulated miner connection (hashrate mode)."""

    def __init__(self, idx: int, address: str, worker: str, local_ip: Optional[str]) -> None:
        self.idx = idx
        self.address = address
        self.worker = worker
        self.local_ip = local_ip
        self.job_id: Optional[str] = None
        self.target: Optional[str] = None
        self.difficulty = 0
        self.nonce = 0
        self.nonce_end = (1 << 64) - 1
        self.login_ok = False
        self.job_event: Optional["asyncio.Event"] = None
        self.pending: Dict[int, float] = {}
        self.next_id = 2
        self.alive = False

    def update_job(self, params: dict) -> None:
        job_id = params.get("job_id")
        if isinstance(job_id, str) and job_id:
            self.job_id = job_id
        target = params.get("target")
        if isinstance(target, str) and len(target) == 64:
            self.target = target
        difficulty = params.get("difficulty")
        if isinstance(difficulty, int) and difficulty > 0:
            self.difficulty = difficulty
        start = params.get("nonce_start")
        end = params.get("nonce_end")
        if isinstance(start, int) and start >= 0:
            self.nonce = start
        if isinstance(end, int) and end >= self.nonce:
            self.nonce_end = end


def run_hashrate_mode(args: argparse.Namespace) -> dict:
    import asyncio
    import resource
    import statistics

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = min(hard, max(soft, args.workers * 2 + 256))
    if wanted > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))

    addresses: List[str] = []
    if args.addresses_file:
        with open(args.addresses_file, "r", encoding="utf-8") as f:
            addresses = [line.strip() for line in f if line.strip()]
    ips = expand_source_ips(args.source_ips)

    def address_for(i: int) -> str:
        if args.address_mode == "shared":
            if addresses:
                return addresses[args.address_offset]
            return args.shared_address
        if addresses:
            return addresses[(args.address_offset + i) % len(addresses)]
        return f"{args.address_prefix}_{i:04d}"

    conns = [
        HashrateConn(
            i,
            address_for(i),
            f"{args.worker_prefix}_{i:04d}" if args.address_mode == "unique" else args.worker_prefix,
            ips[i % len(ips)] if ips else None,
        )
        for i in range(args.workers)
    ]
    counters: Dict[str, int] = {
        "submitted": 0,
        "responses": 0,
        "accepted": 0,
        "busy": 0,
        "rate_limited": 0,
        "other": 0,
        "transport_errors": 0,
        "login_failed": 0,
    }
    reasons: Dict[str, int] = {}
    latencies: List[float] = []
    timeline: List[dict] = []
    bucket: Dict[str, int] = {}

    def bump(key: str) -> None:
        counters[key] = counters.get(key, 0) + 1
        bucket[key] = bucket.get(key, 0) + 1

    hashrate = args.hashrate
    start = time.monotonic()
    deadline = start + args.duration_secs

    async def read_loop(conn: HashrateConn, reader) -> None:
        while True:
            raw = await reader.readline()
            if not raw:
                return
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            if msg.get("method") == "job":
                params = msg.get("params")
                if isinstance(params, dict):
                    conn.update_job(params)
                    if conn.job_event is not None and conn.job_id and conn.target:
                        conn.job_event.set()
                continue
            msg_id = msg.get("id")
            if msg_id == 1:
                if msg.get("status") == "ok":
                    conn.login_ok = True
                else:
                    bump("login_failed")
                    reason = str(msg.get("error") or "login failed")
                    reasons[f"login: {reason}"] = reasons.get(f"login: {reason}", 0) + 1
                    return
                continue
            sent_at = conn.pending.pop(msg_id, None) if isinstance(msg_id, int) else None
            if sent_at is None:
                continue
            bump("responses")
            latencies.append((time.monotonic() - sent_at) * 1000.0)
            if msg.get("status") == "ok":
                bump("accepted")
                result = msg.get("result")
                if isinstance(result, dict):
                    difficulty = result.get("difficulty")
                    if isinstance(difficulty, int) and difficulty > 0:
                        conn.difficulty = difficulty
            else:
                error = str(msg.get("error") or "unknown")
                bump(classify_reject(error))
                key = error.split(":", 1)[0].split("(", 1)[0].strip()
                reasons[key] = reasons.get(key, 0) + 1

    async def miner(conn: HashrateConn) -> None:
        if args.ramp_secs > 0 and args.workers > 1:
            await asyncio.sleep(args.ramp_secs * conn.idx / (args.workers - 1))
        conn.job_event = asyncio.Event()
        try:
            reader, writer = await asyncio.open_connection(
                args.host,
                args.stratum_port,
                local_addr=(conn.local_ip, 0) if conn.local_ip else None,
                limit=1 << 20,
            )
        except OSError as exc:
            bump("transport_errors")
            reasons[f"connect: {exc}"] = reasons.get(f"connect: {exc}", 0) + 1
            return
        reader_task = asyncio.create_task(read_loop(conn, reader))
        try:
            writer.write(
                (
                    json.dumps(
                        {
                            "id": 1,
                            "method": "login",
                            "params": {
                                "address": conn.address,
                                "worker": conn.worker,
                                "protocol_version": 2,
                                "capabilities": ["submit_claimed_hash"],
                            },
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode()
            )
            await writer.drain()
            try:
                await asyncio.wait_for(conn.job_event.wait(), timeout=args.login_timeout_secs)
            except asyncio.TimeoutError:
                bump("login_failed")
                reasons["login timeout"] = reasons.get("login timeout", 0) + 1
                return
            conn.alive = True
            while time.monotonic() < deadline and not reader_task.done():
                rate = hashrate / max(1, conn.difficulty)
                wait = random.expovariate(rate) if rate > 0 else 1.0
                # Share discovery is memoryless: re-draw at least once a second
                # so a difficulty change takes effect promptly.
                if wait > 1.0:
                    await asyncio.sleep(1.0)
                    continue
                await asyncio.sleep(wait)
                if time.monotonic() >= deadline or conn.job_id is None or conn.target is None:
                    continue
                req_id = conn.next_id
                conn.next_id += 1
                nonce = conn.nonce
                conn.nonce = conn.nonce + 1 if conn.nonce < conn.nonce_end else conn.nonce
                conn.pending[req_id] = time.monotonic()
                writer.write(
                    (
                        json.dumps(
                            {
                                "id": req_id,
                                "method": "submit",
                                "params": {
                                    "job_id": conn.job_id,
                                    "nonce": nonce,
                                    "claimed_hash": conn.target,
                                },
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode()
                )
                bump("submitted")
                await writer.drain()
            # Give in-flight submits a moment to resolve before closing.
            grace = time.monotonic() + 3.0
            while conn.pending and time.monotonic() < grace and not reader_task.done():
                await asyncio.sleep(0.1)
        except (ConnectionError, OSError) as exc:
            bump("transport_errors")
            reasons[f"transport: {exc}"] = reasons.get(f"transport: {exc}", 0) + 1
        finally:
            conn.alive = False
            reader_task.cancel()
            writer.close()

    async def sampler() -> None:
        second = 0
        while time.monotonic() < deadline + 3.0:
            await asyncio.sleep(max(0.0, start + second + 1 - time.monotonic()))
            second += 1
            diffs = sorted(c.difficulty for c in conns if c.alive and c.difficulty > 0)
            row = {"t": second, **{k: bucket.get(k, 0) for k in ("submitted", "accepted", "busy", "rate_limited", "other")}}
            bucket.clear()
            row["connected"] = len(diffs)
            if diffs:
                row["diff_min"] = diffs[0]
                row["diff_p10"] = diffs[len(diffs) // 10]
                row["diff_median"] = diffs[len(diffs) // 2]
                row["diff_max"] = diffs[-1]
            timeline.append(row)

    async def main_async() -> None:
        await asyncio.gather(sampler(), *(miner(c) for c in conns))

    asyncio.run(main_async())
    elapsed = max(0.001, time.monotonic() - start)
    lat = sorted(latencies)
    final_diffs = sorted(c.difficulty for c in conns if c.difficulty > 0)
    return {
        "mode": "hashrate",
        "label": args.label,
        "workers": args.workers,
        "hashrate_per_conn": hashrate,
        "address_mode": args.address_mode,
        "source_ips": len(ips),
        "duration_secs": elapsed,
        "totals": counters,
        "reasons": dict(sorted(reasons.items(), key=lambda kv: (-kv[1], kv[0]))),
        "latency_ms": {
            "avg": statistics.fmean(lat) if lat else 0.0,
            "p50": percentile(lat, 0.50),
            "p95": percentile(lat, 0.95),
            "p99": percentile(lat, 0.99),
            "max": lat[-1] if lat else 0.0,
        },
        "final_difficulty": {
            "min": final_diffs[0] if final_diffs else 0,
            "median": final_diffs[len(final_diffs) // 2] if final_diffs else 0,
            "max": final_diffs[-1] if final_diffs else 0,
        },
        "timeline": timeline,
    }


def main() -> int:
    args = parse_args()
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.duration_secs <= 0:
        raise SystemExit("--duration-secs must be > 0")

    if args.hashrate > 0:
        result = run_hashrate_mode(args)
        rendered = json.dumps(result, indent=2, sort_keys=True)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(rendered)
                f.write("\n")
        else:
            print(rendered)
        return 0

    start_iso = now_utc_iso()
    start_monotonic = time.monotonic()
    stop_deadline = start_monotonic + args.duration_secs

    start_snapshot = fetch_api_stats(args.host, args.api_port) if args.api_port > 0 else None
    sampler: Optional[ApiSampler] = None
    if args.api_port > 0:
        sampler = ApiSampler(args.host, args.api_port, args.stats_sample_interval_secs)
        sampler.start()

    workers: List[Worker] = []
    for i in range(args.workers):
        if args.address_mode == "shared":
            address = args.shared_address
        else:
            address = f"{args.address_prefix}_{i:04d}"
        worker_name = f"{args.worker_prefix}_{i:04d}"
        worker = Worker(
            worker_id=i,
            host=args.host,
            port=args.stratum_port,
            address=address,
            worker_name=worker_name,
            shares_per_sec=args.shares_per_worker_per_sec,
            login_timeout_secs=args.login_timeout_secs,
            response_timeout_secs=args.response_timeout_secs,
            stop_deadline=stop_deadline,
            jitter_ms=args.jitter_ms,
        )
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()

    if sampler is not None:
        sampler.stop()
        sampler.join(timeout=2.0)

    end_snapshot = fetch_api_stats(args.host, args.api_port) if args.api_port > 0 else None

    elapsed = max(0.001, time.monotonic() - start_monotonic)
    all_metrics = [worker.metrics for worker in workers]

    submitted = sum(m.submitted for m in all_metrics)
    responses = sum(m.responses for m in all_metrics)
    accepted = sum(m.accepted for m in all_metrics)
    rejected = sum(m.rejected for m in all_metrics)
    transport_errors = sum(m.transport_errors for m in all_metrics)
    protocol_errors = sum(m.protocol_errors for m in all_metrics)
    login_failed = sum(m.login_failed for m in all_metrics)
    all_latencies = sorted(lat for m in all_metrics for lat in m.latencies_ms)

    latency_summary = {
        "avg": (sum(all_latencies) / len(all_latencies)) if all_latencies else 0.0,
        "p50": percentile(all_latencies, 0.50),
        "p95": percentile(all_latencies, 0.95),
        "p99": percentile(all_latencies, 0.99),
        "max": all_latencies[-1] if all_latencies else 0.0,
    }

    error_breakdown = merge_error_reasons(all_metrics)
    api_summary = summarize_api_samples(
        start_snapshot, end_snapshot, sampler.sample_count if sampler is not None else 0
    )

    result = {
        "started_at": start_iso,
        "finished_at": now_utc_iso(),
        "duration_secs": elapsed,
        "host": args.host,
        "stratum_port": args.stratum_port,
        "api_port": args.api_port,
        "workers": args.workers,
        "shares_per_worker_per_sec": args.shares_per_worker_per_sec,
        "load": {
            "submitted": submitted,
            "responses": responses,
            "accepted": accepted,
            "rejected": rejected,
            "transport_errors": transport_errors,
            "protocol_errors": protocol_errors,
            "login_failed": login_failed,
            "response_tps": responses / elapsed,
            "accept_tps": accepted / elapsed,
            "submit_tps": submitted / elapsed,
            "latency_ms": latency_summary,
        },
        "error_breakdown": error_breakdown,
        "api_stats": api_summary,
    }

    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(rendered)
            f.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
