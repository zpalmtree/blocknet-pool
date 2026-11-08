import importlib.util
import os
from pathlib import Path
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location(
    "relay", Path(__file__).with_name("alertmanager_discord_relay.py")
)
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)


def payload(status: str, severity: str) -> dict:
    return {
        "status": status,
        "commonLabels": {"alertname": "BlocknetPoolGatewayRelayStalled", "severity": severity},
        "commonAnnotations": {"summary": "late"},
        "alerts": [{"labels": {"alertname": "BlocknetPoolGatewayRelayStalled"}}],
    }


class MentionTests(unittest.TestCase):
    def test_firing_critical_alert_pings_the_configured_user(self):
        message = relay.format_alert(payload("firing", "critical"), "1234")
        self.assertTrue(message["content"].startswith("<@1234> "))
        self.assertEqual(message["allowed_mentions"], {"parse": [], "users": ["1234"]})

    def test_warnings_and_resolutions_do_not_ping(self):
        for status, severity in [("firing", "warning"), ("resolved", "critical"), ("firing", "info")]:
            message = relay.format_alert(payload(status, severity), "1234")
            self.assertNotIn("content", message)
            self.assertEqual(message["allowed_mentions"], {"parse": []})

    def test_no_ping_without_a_user(self):
        message = relay.format_alert(payload("firing", "critical"), None)
        self.assertNotIn("content", message)

    def test_mention_user_falls_back_to_the_chain_divergence_user(self):
        with mock.patch.dict(os.environ, {"DISCORD_CHAIN_DIVERGENCE_MENTION_USER_ID": "42"}, clear=True):
            self.assertEqual(relay.critical_mention_user_id(), "42")
        with mock.patch.dict(
            os.environ,
            {"DISCORD_CRITICAL_MENTION_USER_ID": "7", "DISCORD_CHAIN_DIVERGENCE_MENTION_USER_ID": "42"},
            clear=True,
        ):
            self.assertEqual(relay.critical_mention_user_id(), "7")
        with mock.patch.dict(os.environ, {"DISCORD_CRITICAL_MENTION_USER_ID": "@everyone"}, clear=True):
            self.assertIsNone(relay.critical_mention_user_id())


if __name__ == "__main__":
    unittest.main()
