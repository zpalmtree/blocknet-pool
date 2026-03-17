#!/usr/bin/env python3
import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request


MAX_CONTENT_LEN = 1800
MAX_EMBED_TITLE_LEN = 256
MAX_EMBED_DESCRIPTION_LEN = 4096
MAX_EMBED_FIELD_VALUE_LEN = 1024
MAX_ALERT_LINES = 8


def clamp(text: str, limit: int = MAX_CONTENT_LEN) -> str:
    text = str(text).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def color_for(status: str, severity: str) -> int:
    status = status.lower()
    severity = severity.lower()
    if status == "resolved":
        return 0x2ECC71
    if severity == "critical":
        return 0xE74C3C
    if severity == "warning":
        return 0xF39C12
    return 0x3498DB


def format_alert_lines(alerts: list[dict]) -> str:
    lines = []
    for alert in alerts[:MAX_ALERT_LINES]:
        labels = alert.get("labels") or {}
        annotations = alert.get("annotations") or {}
        name = labels.get("alertname", "alert")
        detail = annotations.get("summary") or annotations.get("description") or labels.get("severity", "unknown")
        line = f"- {name}: {detail}"
        if alert.get("startsAt"):
            line += f" (since {alert['startsAt']})"
        lines.append(line)
    extra = len(alerts) - MAX_ALERT_LINES
    if extra > 0:
        lines.append(f"- ...and {extra} more alert(s)")
    return clamp("\n".join(lines), MAX_EMBED_FIELD_VALUE_LEN)


def format_alert(payload: dict) -> dict:
    status = str(payload.get("status", "unknown")).upper()
    common_labels = payload.get("commonLabels") or {}
    common_annotations = payload.get("commonAnnotations") or {}
    alertname = str(common_labels.get("alertname", "Alertmanager notification"))
    severity = str(common_labels.get("severity", "unknown"))
    visibility = str(common_labels.get("visibility", "private"))
    summary = common_annotations.get("summary")
    description = common_annotations.get("description")

    alerts = payload.get("alerts") or []
    external = payload.get("externalURL")
    description_parts = []
    if summary:
        description_parts.append(clamp(summary, 400))
    if description:
        description_parts.append(clamp(description, 1200))

    embed = {
        "title": clamp(f"[{status}] {alertname}", MAX_EMBED_TITLE_LEN),
        "color": color_for(status, severity),
        "fields": [
            {"name": "Severity", "value": clamp(severity, MAX_EMBED_FIELD_VALUE_LEN), "inline": True},
            {"name": "Visibility", "value": clamp(visibility, MAX_EMBED_FIELD_VALUE_LEN), "inline": True},
            {"name": "Alerts", "value": str(max(len(alerts), 1)), "inline": True},
        ],
    }
    if description_parts:
        embed["description"] = clamp("\n\n".join(description_parts), MAX_EMBED_DESCRIPTION_LEN)
    if external:
        embed["url"] = str(external)
    if alerts:
        embed["fields"].append(
            {
                "name": "Alert Details",
                "value": format_alert_lines(alerts),
                "inline": False,
            }
        )
    first_alert = alerts[0] if alerts else {}
    if first_alert.get("startsAt"):
        embed["timestamp"] = str(first_alert["startsAt"])
    if external:
        embed["footer"] = {"text": clamp(external, 200)}

    return {
        "allowed_mentions": {"parse": []},
        "embeds": [embed],
    }


def post_discord(webhook: str, message: dict) -> None:
    body = json.dumps(message).encode("utf-8")
    req = request.Request(
        webhook,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "blocknet-pool-alertmanager-discord-relay/1",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=10) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"discord webhook returned HTTP {resp.status}")


class Handler(BaseHTTPRequestHandler):
    server_version = "blocknet-pool-alertmanager-discord-relay/1"

    def do_GET(self):
        if self.path != "/healthz":
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_POST(self):
        if self.path != "/webhook":
            self.send_error(404)
            return

        webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
        if not webhook:
            self.log_error("DISCORD_WEBHOOK_URL is unset; rejecting alert payload")
            self.send_error(503, "DISCORD_WEBHOOK_URL is unset")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            message = format_alert(payload)
            post_discord(webhook, message)
        except Exception as exc:  # noqa: BLE001
            self.log_error("relay failure: %s", exc)
            self.send_error(502, str(exc))
            return

        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, fmt, *args):
        sys.stderr.write(f"{self.address_string()} - {fmt % args}\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Relay Alertmanager webhooks to Discord")
    parser.add_argument("--listen", default="127.0.0.1:24810", help="listen host:port")
    return parser.parse_args()


def main():
    args = parse_args()
    if not os.environ.get("DISCORD_WEBHOOK_URL", "").strip():
        print("warning: DISCORD_WEBHOOK_URL is unset; relay will return HTTP 503 until configured", file=sys.stderr)
    host, port = args.listen.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port)), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
