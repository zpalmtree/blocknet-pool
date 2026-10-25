#!/usr/bin/env python3
"""
Minimal Blocknet daemon stand-in for local pool benchmarks.

Serves the endpoints the pool needs (status, block template, submitblock,
block lookup) using the same template shape as scripts/ci_e2e_smoke.sh. The
chain height advances every --block-secs so the pool keeps broadcasting new
jobs, which is what drives job-tick vardiff retargets in production. The
network target is unreachable, so load-generator shares are never block
candidates.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--block-secs",
        type=float,
        default=15.0,
        help="Seconds between simulated blocks (0 keeps one static template).",
    )
    return parser.parse_args()


class Chain:
    def __init__(self, block_secs: float) -> None:
        self.block_secs = block_secs
        self.started = time.monotonic()
        self.lock = threading.Lock()

    def height(self) -> int:
        if self.block_secs <= 0:
            return 1
        return 1 + int((time.monotonic() - self.started) // self.block_secs)

    def template(self) -> dict:
        height = self.height()
        prev = [height % 256] * 32
        header_base = height.to_bytes(8, "big").hex() + "00" * 84
        return {
            "block": {
                "header": {"Height": height, "Difficulty": 1, "PrevHash": prev},
                "reward": 1000000000,
            },
            "target": ("00" * 31) + "01",
            "header_base": header_base,
            "reward_address_used": "",
            "template_id": f"bench-template-{height}",
        }


def make_handler(chain: Chain):
    class Handler(BaseHTTPRequestHandler):
        server_version = "mock-daemon/1.0"

        def _send_json(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):  # noqa: D401 - silence access log
            return

        def do_GET(self):  # noqa: N802 - http.server API
            path = urlparse(self.path).path
            if path == "/api/status":
                height = chain.height()
                return self._send_json(
                    200,
                    {
                        "peers": 1,
                        "chain_height": height - 1,
                        "best_hash": f"{height - 1:064x}",
                        "syncing": False,
                    },
                )
            if path == "/api/mining/blocktemplate":
                return self._send_json(200, chain.template())
            if path.startswith("/api/block/"):
                return self._send_json(
                    200,
                    {
                        "height": 1,
                        "hash": "cd" * 32,
                        "reward": 1000000000,
                        "difficulty": 1,
                        "timestamp": 1,
                        "tx_count": 1,
                    },
                )
            return self._send_json(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802 - http.server API
            path = urlparse(self.path).path
            content_len = int(self.headers.get("Content-Length", "0"))
            if content_len:
                _ = self.rfile.read(content_len)
            if path == "/api/mining/submitblock":
                return self._send_json(200, {"accepted": False, "hash": "", "height": 0})
            return self._send_json(404, {"error": "not found"})

    return Handler


def main() -> int:
    args = parse_args()
    chain = Chain(args.block_secs)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(chain))
    server.daemon_threads = True
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
