#!/usr/bin/env python3
import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request


MAX_CONTENT_LEN = 1800


def clamp(text: str) -> str:
    text = text.strip()
    if len(text) <= MAX_CONTENT_LEN:
        return text
    return text[: MAX_CONTENT_LEN - 3] + "..."


def format_alert(payload: dict) -> str:
    status = str(payload.get("status", "unknown")).upper()
    common_labels = payload.get("commonLabels") or {}
    common_annotations = payload.get("commonAnnotations") or {}
    header = [
        f"[{status}] {common_labels.get('alertname', 'Alertmanager notification')}",
        f"severity={common_labels.get('severity', 'unknown')}",
        f"visibility={common_labels.get('visibility', 'private')}",
    ]
    summary = common_annotations.get("summary")
    description = common_annotations.get("description")
    if summary:
        header.append(str(summary))
    if description:
        header.append(str(description))

    alerts = payload.get("alerts") or []
    lines = [" | ".join(header)]
    for alert in alerts[:8]:
        labels = alert.get("labels") or {}
        annotations = alert.get("annotations") or {}
        line = f"- {labels.get('alertname', 'alert')}: {annotations.get('summary') or annotations.get('description') or labels.get('severity', 'unknown')}"
        if alert.get("startsAt"):
            line += f" (since {alert['startsAt']})"
        lines.append(line)

    external = payload.get("externalURL")
    if external:
        lines.append(str(external))
    return clamp("\n".join(lines))


def post_discord(webhook: str, content: str) -> None:
    body = json.dumps({"content": content}).encode("utf-8")
    req = request.Request(
        webhook,
        data=body,
        headers={"Content-Type": "application/json"},
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
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8"))
            if webhook:
                content = format_alert(payload)
                post_discord(webhook, content)
            else:
                self.log_message("DISCORD_WEBHOOK_URL is unset; dropping alert payload")
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
    host, port = args.listen.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port)), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
