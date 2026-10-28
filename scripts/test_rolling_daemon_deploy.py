import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location(
    "rolling_deploy", Path(__file__).with_name("rolling_daemon_deploy.py"))
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


def stamp(value):
    return {"secs_since_epoch": int(value), "nanos_since_epoch": int(value % 1 * 1e9)}


class SimulatedDeployment(deploy.Deployment):
    """Model two services; fail immediately if the serving one is restarted."""

    def __init__(self, directory, active="primary", paused=False, fail=None):
        config = {"payout_pause_file": str(directory / "payouts.pause"), "recovery": {
            name: {"service": f"blocknetd@{name}.service"}
            for name in ("primary", "standby")}}
        super().__init__(config)
        self.serving = active
        self.paused = paused
        self.fail = fail
        self.events = []
        self.restarted = set()
        self.ready_after_restart = set()
        self.pause_path = Path(config["payout_pause_file"])
        if paused:
            self.pause_path.touch()

    def preflight(self):
        return self.serving, "pool-wallet"

    def active(self):
        return self.serving

    def status(self):
        return {"operations": []}

    def run(self, *args, **kwargs):
        if args[:2] == ("systemctl", "restart"):
            name = args[2].split("@")[1].split(".")[0]
            if name == self.serving:
                raise AssertionError("restarted serving daemon")
            self.events.append(("restart", name))
            if self.fail == f"restart-{name}":
                raise RuntimeError("restart failed")
            self.restarted.add(name)

    def ready(self, instance, expected_address=None, reference=None):
        if instance in self.restarted:
            if self.fail == f"ready-{instance}":
                raise RuntimeError("readiness timed out")
            self.ready_after_restart.add(instance)
        return "pool-wallet"

    def wait(self, description, probe):
        return probe()

    def operation(self, command, **fields):
        self.events.append((command, fields.get("target")))
        if command == "payouts_pause":
            self.paused = True
            self.pause_path.touch()
        elif command == "payouts_resume":
            self.paused = False
            self.pause_path.unlink()
        else:
            raise AssertionError(command)

    def runtime(self):
        self.events.append(("drain", None))
        if self.fail == "drain":
            raise RuntimeError("payout tick did not drain")
        return {"payouts": {"last_tick_at": stamp(time.time() + 1)}}

    def cutover(self, target, address, serving):
        if not self.paused or target not in self.ready_after_restart:
            raise AssertionError("cutover before pause/readiness")
        self.events.append(("cutover", target))
        if self.fail == f"cutover-{target}":
            raise RuntimeError("cutover failed")
        self.serving = target


class RollingSequenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.previous = self.root / "releases" / "old"
        self.release = self.root / "releases" / "new"
        self.previous.mkdir(parents=True)
        self.release.mkdir()
        self.current = self.root / "current"
        self.current.symlink_to(self.previous)

    def execute(self, **kwargs):
        model = SimulatedDeployment(self.root, **kwargs)
        model.deploy(self.release, self.root / "unit")
        return model

    def test_upgrade_drains_then_returns_to_original_wallet(self):
        model = self.execute()
        self.assertEqual(model.events, [
            ("restart", "standby"), ("payouts_pause", None), ("drain", None),
            ("cutover", "standby"), ("restart", "primary"),
            ("cutover", "primary"), ("payouts_resume", None)])
        self.assertEqual(model.serving, "primary")
        self.assertFalse(model.paused)
        self.assertEqual(self.current.resolve(), self.release)

    def test_standby_can_be_original_active_and_preexisting_pause_is_preserved(self):
        model = self.execute(active="standby", paused=True)
        self.assertEqual(model.events[0], ("restart", "primary"))
        self.assertEqual(model.serving, "standby")
        self.assertTrue(model.paused)
        self.assertNotIn(("payouts_resume", None), model.events)

    def test_inactive_failure_never_restarts_or_switches_active(self):
        for failure in ("restart-standby", "ready-standby", "drain"):
            with self.subTest(failure=failure):
                model = SimulatedDeployment(self.root, fail=failure)
                with self.assertRaises(RuntimeError):
                    model.deploy(self.release, self.root / "unit")
                self.assertEqual(model.serving, "primary")
                self.assertNotIn(("restart", "primary"), model.events)
                self.assertFalse(model.paused)
                self.assertEqual(self.current.resolve(), self.previous)

    def test_failure_after_cutover_keeps_payouts_paused(self):
        for failure in ("cutover-standby", "restart-primary", "ready-primary", "cutover-primary"):
            with self.subTest(failure=failure):
                model = SimulatedDeployment(self.root, fail=failure)
                with self.assertRaises(RuntimeError):
                    model.deploy(self.release, self.root / "unit")
                self.assertTrue(model.paused)
                self.assertNotIn(("payouts_resume", None), model.events)
                if failure == "cutover-standby":
                    self.assertNotIn(("restart", "primary"), model.events)
                    self.assertEqual(model.serving, "primary")
                else:
                    self.assertEqual(model.serving, "standby")


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.subject = deploy.Deployment({"recovery": {"primary": {"service": "primary"}}})
        self.subject.run = Mock()
        self.responses = {
            "/api/status": {"syncing": False, "peers": 2, "chain_height": 10},
            "/api/wallet/address": {"address": "pool-wallet"},
            "/api/wallet/balance": {"scanning": False, "synced_height": 10, "chain_height": 10},
            "/api/mining/blocktemplate": {"template_id": "lease", "target": "ff", "header_base": "aa"},
            "/api/mining/renewtemplate": {"template_id": "lease", "template_expires_at_unix_ms": time.time() * 1000 + 60000},
        }
        self.subject.node = Mock(side_effect=lambda name, path, *args: self.responses[path])

    def test_status_alone_is_not_readiness(self):
        cases = [
            ("/api/status", "syncing", True), ("/api/status", "peers", 0),
            ("/api/wallet/balance", "scanning", True),
            ("/api/wallet/balance", "synced_height", 9),
            ("/api/mining/blocktemplate", "template_id", ""),
            ("/api/mining/renewtemplate", "template_expires_at_unix_ms", 0),
        ]
        for path, field, value in cases:
            with self.subTest(path=path, field=field):
                original = copy.deepcopy(self.responses)
                self.responses[path][field] = value
                with self.assertRaises(deploy.NotReady):
                    self.subject.ready("primary")
                self.responses = original

    def test_wallet_identity_is_required(self):
        with self.assertRaisesRegex(RuntimeError, "wallet address differs"):
            self.subject.ready("primary", "different-wallet")

    def test_ready_requires_actual_lease_renewal(self):
        self.assertEqual(self.subject.ready("primary"), "pool-wallet")
        self.subject.node.assert_any_call("primary", "/api/mining/renewtemplate", {"template_id": "lease"})

    def test_starting_service_is_retryable(self):
        self.subject.run.side_effect = subprocess.CalledProcessError(3, "systemctl")
        with self.assertRaises(deploy.NotReady):
            self.subject.ready("primary")

    def test_forked_standby_is_rejected(self):
        self.subject.node.side_effect = lambda name, path, *args: (
            {"hash": name} if path.startswith("/api/block/") else self.responses[path])
        with self.assertRaisesRegex(deploy.NotReady, "canonical chains disagree"):
            self.subject.ready("primary", reference="standby")

    def test_drain_reads_fresh_runtime_and_keeps_credentials_out_of_argv(self):
        self.subject.config["database_url"] = "postgres://pool:p%40ss@127.0.0.1:5432/pool?sslmode=disable"
        self.subject.run.return_value = Mock(stdout=json.dumps({"sampled_at": stamp(time.time())}))
        self.subject.runtime()
        args, kwargs = self.subject.run.call_args
        self.assertNotIn("p@ss", str(args))
        self.assertEqual(kwargs["env"]["PGPASSWORD"], "p@ss")
        self.assertEqual(kwargs["env"]["PGHOST"], "127.0.0.1")
        self.assertEqual(kwargs["env"]["PGDATABASE"], "pool")
        self.subject.run.return_value = Mock(stdout=json.dumps({"sampled_at": stamp(time.time() - 120)}))
        with self.assertRaises(deploy.NotReady):
            self.subject.runtime()

    def test_proxy_cookie_disagreement_prevents_restart(self):
        self.subject.active = Mock(side_effect=RuntimeError("routing mismatch"))
        with self.assertRaises(RuntimeError):
            self.subject.restart_inactive("standby", "primary")
        self.subject.run.assert_not_called()

    def test_cutover_waits_for_stratum_refresh_before_returning(self):
        subject = self.subject
        subject.config.update(daemon_api="http://proxy", daemon_cookie_path="cookie")
        subject.active = Mock(side_effect=["primary", "standby"])
        subject.ready = Mock()
        subject.operation = Mock()
        subject.fetch = Mock(return_value={"template_id": "new"})
        subject.node = Mock(return_value={"template_id": "new"})
        subject.runtime = Mock(return_value={
            "sampled_at": stamp(time.time()), "jobs": {"last_refresh_success_millis": 60000}})
        subject.wait = lambda description, probe: probe()
        with self.assertRaisesRegex(deploy.NotReady, "Stratum"):
            subject.cutover("standby", "pool-wallet", "primary")
        subject.node.assert_called_with("standby", "/api/mining/renewtemplate", {"template_id": "new"})

    def test_cutover_accepts_fresh_stratum_refresh(self):
        subject = self.subject
        subject.config.update(daemon_api="http://proxy", daemon_cookie_path="cookie")
        subject.active = Mock(side_effect=["primary", "standby"])
        subject.ready = Mock()
        subject.operation = Mock()
        subject.fetch = Mock(return_value={"template_id": "new"})
        subject.node = Mock(return_value={"template_id": "new"})
        subject.runtime = Mock(return_value={
            "sampled_at": stamp(time.time() + 1), "jobs": {"last_refresh_success_millis": 0}})
        subject.wait = lambda description, probe: probe()
        subject.cutover("standby", "pool-wallet", "primary")
        subject.operation.assert_called_once_with("cutover", target="standby")

    def test_cutover_retries_when_recovery_refuses_a_lagging_target(self):
        subject = self.subject
        subject.config.update(daemon_api="http://proxy", daemon_cookie_path="cookie")
        subject.active = Mock(side_effect=["primary", "standby"])
        subject.ready = Mock()
        subject.operation = Mock(side_effect=[
            RuntimeError("recovery cutover failed: refusing cutover to standby: it is at height 9"),
            None])
        subject.fetch = Mock(return_value={"template_id": "new"})
        subject.node = Mock(return_value={"template_id": "new"})
        subject.runtime = Mock(return_value={
            "sampled_at": stamp(time.time() + 1), "jobs": {"last_refresh_success_millis": 0}})

        def retrying_wait(description, probe):
            while True:
                try:
                    return probe()
                except deploy.NotReady:
                    continue

        subject.wait = retrying_wait
        subject.cutover("standby", "pool-wallet", "primary")
        self.assertEqual(subject.operation.call_count, 2)

    def test_cutover_does_not_retry_other_recovery_failures(self):
        subject = self.subject
        subject.active = Mock(return_value="primary")
        subject.ready = Mock()
        subject.operation = Mock(side_effect=RuntimeError("recovery cutover failed: nginx reload failed"))
        subject.wait = lambda description, probe: probe()
        with self.assertRaisesRegex(RuntimeError, "nginx reload failed"):
            subject.cutover("standby", "pool-wallet", "primary")

    def test_failed_recovery_operation_aborts(self):
        self.subject.rpc = Mock(return_value={"operation": {"id": 8}})
        self.subject.status = Mock(return_value={"operations": [
            {"id": 8, "state": "failed", "message": "nginx reload failed"}]})
        with self.assertRaisesRegex(RuntimeError, "nginx reload failed"):
            self.subject.operation("cutover", target="standby")

    def test_wait_has_bounded_timeout(self):
        with patch.object(deploy.time, "monotonic", side_effect=[0, 1000]):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                self.subject.wait("wallet", Mock(side_effect=deploy.NotReady("loading")))


if __name__ == "__main__":
    unittest.main()
