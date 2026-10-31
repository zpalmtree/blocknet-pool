#!/usr/bin/env python3
"""Stage gateway certificates; never change listeners, config, or services."""

import argparse
import fcntl
import grp
import json
import os
from pathlib import Path
import subprocess
import tempfile


SOURCE = Path("/etc/letsencrypt/live/bntpool.com")
DESTINATION = Path("/etc/blocknet/pool/tls")
HELPER = Path("/usr/local/libexec/blocknet-pool-gateway-tls.py")
HOOK = Path("/etc/letsencrypt/renewal-hooks/deploy/blocknet-pool-gateway")


def openssl(*args):
    return subprocess.check_output(["openssl", *map(str, args)], stderr=subprocess.PIPE)


def refresh(source=SOURCE, destination=DESTINATION, group="blocknet"):
    gid = grp.getgrnam(group).gr_gid
    destination.mkdir(mode=0o750, parents=True, exist_ok=True)
    os.chown(destination, os.geteuid(), gid)
    os.chmod(destination, 0o750)
    # Certbot and manual preparation can run concurrently.
    with (destination / ".renew.lock").open("a") as lock:
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(prefix=".renew-", dir=destination) as temp:
            staged = Path(temp)
            for name in ("fullchain.pem", "privkey.pem"):
                path = staged / name
                path.write_bytes((source / name).read_bytes())
                os.chown(path, os.geteuid(), gid)
                os.chmod(path, 0o640)
            cert, key = staged / "fullchain.pem", staged / "privkey.pem"
            # x509 -checkhost prints a mismatch but can still exit successfully.
            if b"does match certificate" not in openssl(
                "x509", "-in", cert, "-noout", "-checkhost", "bntpool.com"
            ):
                raise ValueError("certificate does not cover bntpool.com")
            openssl("x509", "-in", cert, "-noout", "-checkend", "0")
            if openssl("x509", "-in", cert, "-pubkey", "-noout") != openssl(
                "pkey", "-in", key, "-pubout"
            ):
                raise ValueError("certificate and private key do not match")
            for name in ("privkey.pem", "fullchain.pem"):
                target = destination / name
                # Avoid replacing unchanged files and triggering a TLS reload.
                if target.exists() and target.read_bytes() == (staged / name).read_bytes():
                    os.chown(target, os.geteuid(), gid)
                    os.chmod(target, 0o640)
                else:
                    os.replace(staged / name, target)
            # Each file is replaced atomically. A gateway check between the two
            # replacements keeps its old certificate and retries on the next poll.


def install():
    if os.geteuid() != 0:
        raise PermissionError("run preparation as root")
    config = json.loads(Path("/etc/blocknet/pool/config.json").read_text())
    if config.get("gateway_tls_port", 0) != 0:
        raise ValueError("preparation expects gateway TLS to remain disabled")
    refresh()
    HELPER.parent.mkdir(parents=True, exist_ok=True)
    HOOK.parent.mkdir(parents=True, exist_ok=True)
    contents = Path(__file__).read_bytes()
    hook = f'#!/bin/sh\nset -eu\nexec /usr/bin/python3 {HELPER}\n'.encode()
    for path, data in ((HELPER, contents), (HOOK, hook)):
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as staged:
            try:
                staged.write(data)
                staged.flush()
                os.fchown(staged.fileno(), 0, 0)
                os.fchmod(staged.fileno(), 0o755)
                os.replace(staged.name, path)
            finally:
                Path(staged.name).unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install", action="store_true", help="install the renewal hook too")
    args = parser.parse_args()
    if args.install:
        install()
    else:
        refresh()
    print("Gateway certificate staged; no configuration, listener, firewall or service changes.")
