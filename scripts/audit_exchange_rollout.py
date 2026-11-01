#!/usr/bin/env python3
"""Read-only shared-address rollout audit; outputs aggregates, never customer memos."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import shlex
import subprocess
from zoneinfo import ZoneInfo

DEFAULT_ADDRESS = "ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij"


def audit(database, config_path):
    config = json.loads(Path(config_path).read_text())
    exchanges = config.get("shared_exchange_addresses", [{"address": DEFAULT_ADDRESS, "name": "SafeTrade"}])
    psql = ["sudo", "-n", "-u", "postgres", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-d", database]
    report = {
        "sampled_at": datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "settings": {key: config.get(key, default) for key, default in [
            ("payout_memos_enabled", False), ("enforce_shared_exchange_memos", False),
            ("min_payout_amount", 0.1), ("memo_min_payout_amount", 0.0)]},
        "exchanges": [],
        "worker_count_note": "Distinct worker names submitting shares, not connections or unique people. Includes rejected shares.",
    }
    for exchange in exchanges:
        # psql variable substitution quotes the address as an SQL literal.
        sql = """BEGIN READ ONLY;
SET LOCAL statement_timeout = '10s';
SELECT json_build_object(
 'pending_atomic', COALESCE((SELECT pending FROM balances WHERE address=:'address'),0),
 'paid_atomic', COALESCE((SELECT paid FROM balances WHERE address=:'address'),0),
 'pending_payouts', (SELECT count(*) FROM pending_payouts WHERE address=:'address'),
 'destination_table_exists', to_regclass('payout_destinations') IS NOT NULL,
 'workers_24h', count(DISTINCT worker),
 'workers_15m', count(DISTINCT worker) FILTER (WHERE created_at >= extract(epoch FROM now())::bigint-900),
 'shares_24h', count(*))
FROM shares WHERE miner=:'address' AND created_at >= extract(epoch FROM now())::bigint-86400;
COMMIT;
"""
        result = subprocess.run(psql + ["-v", "address=" + exchange["address"]], input=sql,
                                text=True, capture_output=True, check=True, timeout=15)
        report["exchanges"].append({"exchange": exchange["name"], **json.loads(result.stdout)})
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="bntpool")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--database", default="blocknet_pool")
    parser.add_argument("--config", default="/etc/blocknet/pool/config.json")
    args = parser.parse_args()
    if args.local:
        audit(args.database, args.config)
    else:
        remote = ["sudo", "-n", "python3", "-", "--local", "--database", args.database, "--config", args.config]
        subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", args.host, shlex.join(remote)],
                       input=Path(__file__).read_text(), text=True, check=True, timeout=60)


if __name__ == "__main__":
    main()
