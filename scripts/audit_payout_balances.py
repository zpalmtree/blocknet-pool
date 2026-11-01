#!/usr/bin/env python3
"""Read-only payout balance audit. No balances, destinations, or services are changed."""
import argparse
import ctypes
import ctypes.util
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unicodedata

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def address_problem(address, valid_point):
    address = address.strip()
    try:
        number = 0
        for char in address.strip():
            number = number * 58 + ALPHABET.index(char)
        raw = b"\0" * (len(address) - len(address.lstrip("1"))) + number.to_bytes((number.bit_length() + 7) // 8, "big")
    except ValueError:
        return "invalid base58"
    if len(raw) != 68:
        return "unsupported address length"
    checksum = hashlib.sha3_256(b"blocknet_stealth_address_checksumblocknet_mainnet" + raw[:64]).digest()[:4]
    if raw[64:] != checksum:
        return "invalid mainnet checksum"
    if not valid_point(raw[:32]):
        return "invalid spend public key"
    if not valid_point(raw[32:64]):
        return "invalid view public key"
    return None


DEFAULT_SHARED_ADDRESSES = {"ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij"}


def payout_destination_problem(row, valid_point, shared_addresses):
    account = row["address"]
    if account.startswith("memo:"):
        address, memo = row.get("destination"), row.get("memo")
        if not address or not memo:
            return "missing payout destination"
        memo_bytes = memo.encode("utf-8")
        if len(memo_bytes) > 124 or any(unicodedata.category(char) == "Cc" for char in memo):
            return "invalid payout memo"
        encoded = address.encode("utf-8")
        expected = "memo:" + hashlib.sha3_256(b"blocknet_pool_payout_memo_v1\0" + len(encoded).to_bytes(8, "big") + encoded + memo_bytes).hexdigest()
        if account != expected:
            return "payout destination mismatch"
        return address_problem(address, valid_point)
    if account in shared_addresses:
        return "shared exchange address requires a memo"
    return address_problem(account, valid_point)


def audit(database, config_path, include_addresses):
    library = ctypes.util.find_library("sodium")
    if not library:
        raise RuntimeError("libsodium is required to validate Ristretto public keys")
    sodium = ctypes.CDLL(library)
    valid = sodium.crypto_core_ristretto255_is_valid_point
    valid.argtypes = [ctypes.c_char_p]
    valid.restype = ctypes.c_int
    config = json.loads(Path(config_path).read_text())
    minimum = round(float(config["min_payout_amount"]) * 100_000_000)
    psql = ["sudo", "-n", "-u", "postgres", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-d", database]
    has_destinations = subprocess.run(psql, input="SELECT to_regclass('payout_destinations') IS NOT NULL;", text=True, capture_output=True, check=True, timeout=20).stdout.strip() == "t"
    destination_fields = ", 'destination',d.address,'memo',d.memo" if has_destinations else ""
    destination_join = "LEFT JOIN payout_destinations d ON d.account_id=b.address" if has_destinations else ""
    shared_addresses = {entry["address"] for entry in config["shared_exchange_addresses"]} if "shared_exchange_addresses" in config else DEFAULT_SHARED_ADDRESSES
    if not config.get("enforce_shared_exchange_memos", False):
        shared_addresses = set()
    memo_minimum = max(minimum, round(float(config.get("memo_min_payout_amount", 0)) * 100_000_000))
    sql = f"""BEGIN READ ONLY;
SET LOCAL statement_timeout = '15s';
SELECT json_build_object('address',b.address,'pending',b.pending{destination_fields})
FROM balances b {destination_join} WHERE b.pending > 0 ORDER BY b.pending DESC;
COMMIT;"""
    result = subprocess.run(psql, input=sql, text=True, capture_output=True, check=True, timeout=20)
    report = {"minimum_atomic": minimum, "memo_minimum_atomic": memo_minimum, "eligible_count": 0, "eligible_atomic": 0,
              "below_minimum_count": 0, "below_minimum_atomic": 0,
              "blocked_atomic": 0, "blocked": []}
    for line in result.stdout.splitlines():
        row = json.loads(line)
        problem = payout_destination_problem(row, valid, shared_addresses)
        if problem:
            blocked = {"fingerprint": hashlib.sha256(row["address"].encode()).hexdigest()[:16], "amount_atomic": row["pending"], "reason": problem}
            if include_addresses:
                blocked["address"] = row["address"]
            report["blocked"].append(blocked)
            report["blocked_atomic"] += row["pending"]
        else:
            account_minimum = memo_minimum if row["address"].startswith("memo:") else minimum
            bucket = "eligible" if row["pending"] >= account_minimum else "below_minimum"
            report[bucket + "_count"] += 1
            report[bucket + "_atomic"] += row["pending"]
    report["blocked_count"] = len(report["blocked"])
    report["resolution"] = "Keep blocked balances credited. Establish ownership and a validated replacement destination through operator review before any separately authorized ledger change."
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="bntpool")
    parser.add_argument("--local", action="store_true", help="Run on the database host")
    parser.add_argument("--database", default="blocknet_pool")
    parser.add_argument("--config", default="/etc/blocknet/pool/config.json")
    parser.add_argument("--include-addresses", action="store_true")
    args = parser.parse_args()
    if args.local:
        audit(args.database, args.config, args.include_addresses)
    else:
        import shlex
        remote = ["sudo", "-n", "python3", "-", "--local", "--database", args.database, "--config", args.config]
        if args.include_addresses:
            remote.append("--include-addresses")
        subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "--", args.host, shlex.join(remote)], input=Path(__file__).read_text(), text=True, check=True, timeout=45)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(f"Payout audit failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
