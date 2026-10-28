#!/usr/bin/env python3
"""Host-side rolling daemon deployment; --check performs readiness checks only.

Called by deploy_blocknet_daemon_bntpool.sh as root. Uses recoveryd for payout
pausing and transactional proxy/cookie cutovers. Never restarts pool services.
"""

import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request


class NotReady(RuntimeError):
    pass


def timestamp(value):
    return value["secs_since_epoch"] + value.get("nanos_since_epoch", 0) / 1e9


class Deployment:
    def __init__(self, config, timeout=900):
        self.config = config
        self.recovery = config["recovery"]
        self.timeout = timeout
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def run(self, *args, **kwargs):
        return subprocess.run(args, check=True, timeout=180, **kwargs)

    def rpc(self, command, **fields):
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(30)
            connection.connect(self.recovery["socket_path"])
            connection.sendall(json.dumps({"command": command, **fields}).encode() + b"\n")
            with connection.makefile("rb") as response:
                envelope = json.loads(response.readline(2 * 1024 * 1024))
        if not envelope.get("ok"):
            raise RuntimeError(f"recovery {command}: {envelope.get('error')}")
        return envelope["result"]

    def status(self):
        return self.rpc("status")["status"]

    def wait(self, description, probe):
        deadline = time.monotonic() + self.timeout
        last_log = 0
        while True:
            try:
                return probe()
            except (NotReady, OSError, ValueError) as error:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"timed out: {description}: {error}") from error
                if time.monotonic() - last_log >= 30:
                    print(f"Waiting for {description}: {error}", flush=True)
                    last_log = time.monotonic()
                time.sleep(5)

    def operation(self, command, **fields):
        operation = self.rpc(command, **fields)["operation"]
        operation_id = operation["id"]

        def completed():
            for item in self.status()["operations"]:
                if item["id"] != operation_id:
                    continue
                if item["state"] == "succeeded":
                    return
                if item["state"] == "failed":
                    raise RuntimeError(f"recovery {command} failed: {item.get('message')}")
                raise NotReady(f"operation {operation_id} is running")
            raise RuntimeError(f"recovery operation {operation_id} disappeared")

        self.wait(command, completed)

    def fetch(self, api, cookie, path, payload=None):
        headers = {"Authorization": "Bearer " + Path(cookie).read_text().strip()}
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode()
        request = urllib.request.Request(api.rstrip("/") + path, data=data, headers=headers)
        with self.http.open(request, timeout=20) as response:
            return json.load(response)

    def node(self, instance, path, payload=None):
        cfg = self.recovery[instance]
        return self.fetch(cfg["api"], cfg["cookie_path"], path, payload)

    def active(self):
        # Read the actual routing files, not recoveryd's five-second status cache.
        proxy = Path(self.recovery["proxy_include_path"]).read_text().strip()
        cookie = Path(self.recovery["active_cookie_path"]).resolve(strict=True)
        for name in ("primary", "standby"):
            cfg = self.recovery[name]
            if (proxy == f"proxy_pass {cfg['api']};"
                    and cookie == Path(cfg["cookie_path"]).resolve(strict=True)):
                return name
        raise RuntimeError("proxy and cookie do not identify one active daemon")

    def ready(self, instance, expected_address=None, reference=None):
        cfg = self.recovery[instance]
        try:
            self.run("systemctl", "is-active", "--quiet", cfg["service"])
        except subprocess.CalledProcessError as error:
            raise NotReady(f"{instance} service is not active") from error
        status = self.node(instance, "/api/status")
        if status.get("syncing") is not False or status.get("peers", 0) < 1:
            raise NotReady(f"{instance} is syncing or has no peers")
        address = self.node(instance, "/api/wallet/address")["address"]
        if expected_address and address != expected_address:
            raise RuntimeError(f"{instance} wallet address differs from the active wallet")
        balance = self.node(instance, "/api/wallet/balance")
        if (balance.get("scanning") is not False
                or balance.get("synced_height", -1) < balance["chain_height"]):
            raise NotReady(f"{instance} wallet has not caught up")
        if reference:
            other = self.node(reference, "/api/status")
            height = status["chain_height"]
            if height < other["chain_height"]:
                raise NotReady(f"{instance} is behind {reference}")
            # Compare a common canonical block, allowing a new tip during probes.
            height = other["chain_height"]
            a = self.node(instance, f"/api/block/{height}")
            b = self.node(reference, f"/api/block/{height}")
            if a["hash"].lower() != b["hash"].lower():
                raise NotReady("daemon canonical chains disagree")
        template = self.node(instance, "/api/mining/blocktemplate")
        if not all(template.get(key) for key in ("template_id", "header_base", "target")):
            raise NotReady(f"{instance} returned an incomplete mining template")
        renewed = self.node(instance, "/api/mining/renewtemplate",
                            {"template_id": template["template_id"]})
        if (renewed.get("template_id") != template["template_id"]
                or renewed.get("template_expires_at_unix_ms", 0) <= time.time() * 1000):
            raise NotReady(f"{instance} cannot renew its mining template")
        return address

    def runtime(self):
        # One indexed, read-only row. Keep the database credential out of argv/logs.
        url = urllib.parse.urlsplit(self.config["database_url"])
        if url.scheme not in ("postgres", "postgresql") or not url.hostname or not url.username:
            raise RuntimeError("rolling deploy requires a PostgreSQL database URL")
        env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
        env.update(PGHOST=url.hostname, PGPORT=str(url.port or 5432),
                   PGUSER=urllib.parse.unquote(url.username),
                   PGPASSWORD=urllib.parse.unquote(url.password or ""),
                   PGDATABASE=urllib.parse.unquote(url.path.lstrip("/")),
                   PGCONNECT_TIMEOUT="5", PGOPTIONS="-c statement_timeout=5000")
        for key, value in urllib.parse.parse_qsl(url.query):
            if key not in ("sslmode", "sslcert", "sslkey", "sslrootcert"):
                raise RuntimeError(f"unsupported database URL option: {key}")
            env["PG" + key.upper()] = value
        result = self.run(
            "psql", "-X", "-w", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c",
            "SELECT convert_from(value, 'UTF8') FROM meta "
            "WHERE key = 'live_runtime_snapshot_v1'", env=env,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        snapshot = json.loads(result.stdout)
        if not 0 <= time.time() - timestamp(snapshot["sampled_at"]) < 30:
            raise NotReady("Stratum runtime snapshot is stale")
        return snapshot

    def preflight(self):
        status = self.status()
        if not status.get("enabled") or not status.get("secret_configured"):
            raise RuntimeError("recovery and wallet auto-loading must be configured")
        if any(op["state"] == "running" for op in status["operations"]):
            raise RuntimeError("a recovery operation is already running")
        active = self.active()
        if (self.config["daemon_cookie_path"] != self.recovery["active_cookie_path"]
                or self.config["daemon_api"] in
                [self.recovery[name]["api"] for name in ("primary", "standby")]):
            raise RuntimeError("pool must use the managed proxy and active cookie")
        address = self.ready(active)
        self.probe_proxy(active)
        self.runtime()
        return active, address

    def probe_proxy(self, target):
        template = self.fetch(self.config["daemon_api"], self.config["daemon_cookie_path"],
                              "/api/mining/blocktemplate")
        # A lease created through the proxy must exist on the intended daemon.
        renewed = self.node(target, "/api/mining/renewtemplate",
                            {"template_id": template["template_id"]})
        if renewed.get("template_id") != template["template_id"]:
            raise NotReady("proxy template does not belong to the target daemon")

    def restart_inactive(self, target, serving):
        if self.active() != serving or target == serving:
            raise RuntimeError("routing changed; refusing to restart the serving daemon")
        if any(op["state"] == "running" for op in self.status()["operations"]):
            raise RuntimeError("a recovery operation started during deployment")
        self.ready(serving)
        print(f"Restarting inactive {target}; {serving} continues serving", flush=True)
        self.run("systemctl", "restart", self.recovery[target]["service"])

    def cutover(self, target, address, serving):
        if self.active() != serving:
            raise RuntimeError("routing changed before cutover")
        def switch():
            self.ready(target, address, serving)
            try:
                self.operation("cutover", target=target)
            except RuntimeError as error:
                # recoveryd refuses a target behind or off the serving chain
                # before touching routing; a block landing between the checks
                # causes that, so retry rather than abort mid-deploy.
                if "refusing cutover" in str(error):
                    raise NotReady(str(error)) from error
                raise

        self.wait(f"{target} readiness and cutover", switch)
        switched_at = time.time()

        def pool_ready():
            if self.active() != target:
                raise RuntimeError("cutover did not establish expected routing")
            self.probe_proxy(target)
            snapshot = self.runtime()
            lag = snapshot["jobs"].get("last_refresh_success_millis")
            if lag is None or timestamp(snapshot["sampled_at"]) - lag / 1000 <= switched_at:
                raise NotReady("waiting for Stratum to refresh its template after cutover")

        self.wait(f"pool mining through {target}", pool_ready)

    def deploy(self, release, unit):
        original, address = self.preflight()
        inactive = "standby" if original == "primary" else "primary"
        root = release.parent.parent
        current = root / "current"
        previous = current.resolve(strict=True)
        paused_here = False
        switched = False
        try:
            # Publish once; running processes retain the old executable until restarted.
            temporary = root / ".current.rolling"
            temporary.unlink(missing_ok=True)
            temporary.symlink_to(release)
            temporary.replace(current)
            self.run("install", "-m", "0644", str(unit), "/etc/systemd/system/blocknetd@.service")
            self.run("systemctl", "daemon-reload")
            self.restart_inactive(inactive, original)
            self.wait(f"{inactive} wallet, chain and template",
                      lambda: self.ready(inactive, address, original))
            pause_file = Path(self.config["payout_pause_file"])
            paused_here = not pause_file.exists()
            self.operation("payouts_pause")
            paused_at = time.time()

            def payouts_drained():
                tick = self.runtime()["payouts"].get("last_tick_at")
                if tick is None or timestamp(tick) <= paused_at:
                    raise NotReady("waiting for the in-flight payout tick to finish")

            self.wait("payout drain", payouts_drained)
            # From this point an ambiguous failure must leave payouts paused.
            switched = True
            self.cutover(inactive, address, original)
            self.restart_inactive(original, inactive)
            self.wait(f"{original} wallet, chain and template",
                      lambda: self.ready(original, address, inactive))
            self.cutover(original, address, inactive)
            if paused_here:
                self.operation("payouts_resume")
        except BaseException:
            if not switched:
                temporary = root / ".current.rollback"
                temporary.unlink(missing_ok=True)
                temporary.symlink_to(previous)
                temporary.replace(current)
                if paused_here:
                    self.operation("payouts_resume")
            else:
                print("Deploy stopped. Payouts remain paused; inspect both daemons and "
                      "restore the original active wallet before resuming payouts.", file=sys.stderr)
            raise
        print(f"Rolling deploy complete; {original} active; payout state restored", flush=True)
        for name in ("primary", "standby"):
            self.run("systemctl", "show", self.recovery[name]["service"],
                     "-p", "ExecMainStartTimestamp", "-p", "ActiveState")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="/etc/blocknet/pool/config.json")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--release", type=Path)
    parser.add_argument("--unit", type=Path)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if not args.check and (not args.release or not args.unit):
        parser.error("deployment requires --release and --unit")
    deployment = Deployment(json.loads(Path(args.config).read_text()), args.timeout)
    with open("/run/blocknet-daemon-deploy.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.check:
            active, _ = deployment.preflight()
            print(f"Rolling deploy preflight passed; active={active}")
        else:
            release = args.release.resolve(strict=True)
            if not (release / "blocknet").is_file() or not args.unit.is_file():
                parser.error("release binary and unit must exist")
            deployment.deploy(release, args.unit)


if __name__ == "__main__":
    main()
