export default {
  async scheduled(_event, env, _ctx) {
    await runCheck(env);
  },

  // HTTP callers only see the last stored result. Probing, signed ingest
  // posts, and Discord pages happen exclusively on the cron trigger, so the
  // worker URL (if any is enabled) cannot be used to generate traffic.
  async fetch(request, env, _ctx) {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed\n", {
        status: 405,
        headers: { allow: "GET, HEAD" },
      });
    }
    const state = await loadState(env, "public_http");
    const body = JSON.stringify({
      status: state?.status ?? "unknown",
      lastCheckedAt: state?.lastCheckedAt ?? null,
    });
    return new Response(request.method === "HEAD" ? null : body, {
      headers: {
        "content-type": "application/json",
        "cache-control": "public, max-age=30",
      },
    });
  },
};

async function runCheck(env) {
  const key = "public_http";
  const now = Math.floor(Date.now() / 1000);
  const prior = await loadState(env, key);
  const probe = await probePublicMonitor(env.PUBLIC_MONITOR_URL);

  // `checked_at` is part of the signed body; the pool rejects events outside
  // +/-5 minutes of its clock or not newer than the last accepted one.
  if (!probe.ok) {
    const startedAt = prior?.status === "down" ? prior.startedAt : now;
    const next = { status: "down", startedAt, lastCheckedAt: now, detail: probe.detail };
    await env.MONITOR_STATE.put(key, JSON.stringify(next));
    await deliver([
      () =>
        notifyIngress(env, {
          service: "public_http",
          status: "down",
          started_at: startedAt,
          checked_at: now,
          summary: "Cloudflare could not reach the public pool endpoint",
          detail: probe.detail,
        }),
      // The ingest endpoint is on the same host and often fails during an
      // outage; that must not suppress the Discord page.
      prior?.status !== "down" &&
        (() =>
          notifyDiscord(
            env,
            `[DOWN] public_http\nCloudflare could not reach ${env.PUBLIC_MONITOR_URL}\n${probe.detail}`,
          )),
    ]);
    return next;
  }

  const next = { status: "up", startedAt: null, lastCheckedAt: now, detail: probe.detail };
  await env.MONITOR_STATE.put(key, JSON.stringify(next));
  await deliver([
    () =>
      notifyIngress(env, {
        service: "public_http",
        status: "up",
        started_at: prior?.status === "down" ? prior.startedAt : null,
        ended_at: prior?.status === "down" ? now : null,
        checked_at: now,
        summary:
          prior?.status === "down"
            ? "Cloudflare public HTTP probe recovered"
            : "Cloudflare public HTTP probe healthy",
        detail: probe.detail,
      }),
    prior?.status === "down" &&
      (() =>
        notifyDiscord(
          env,
          `[RECOVERED] public_http\n${env.PUBLIC_MONITOR_URL}\nDowntime started at ${prior.startedAt} and recovered at ${now}.`,
        )),
  ]);

  return next;
}

// Run every notification even if an earlier one fails, then surface the
// first failure so the cron invocation is still reported as an error.
async function deliver(tasks) {
  const results = await Promise.allSettled(
    tasks.filter(Boolean).map((task) => task()),
  );
  const failed = results.find((result) => result.status === "rejected");
  if (failed) {
    throw failed.reason;
  }
}

async function loadState(env, key) {
  const raw = await env.MONITOR_STATE.get(key);
  return raw ? JSON.parse(raw) : null;
}

async function probePublicMonitor(url) {
  try {
    const response = await fetch(url, {
      cf: { cacheTtl: 0, cacheEverything: false },
      headers: { "cache-control": "no-cache" },
    });
    if (!response.ok) {
      return { ok: false, detail: `HTTP ${response.status}` };
    }
    const payload = await response.json();
    if (!payload.ok) {
      return { ok: false, detail: "monitor payload was not ok=true" };
    }
    return {
      ok: true,
      detail: `summary_state=${payload.summary_state ?? "unknown"}`,
    };
  } catch (error) {
    return { ok: false, detail: String(error) };
  }
}

async function notifyIngress(env, payload) {
  const body = JSON.stringify(payload);
  const signature = await signBody(env.INGEST_SECRET, body);
  const response = await fetch(env.INGEST_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-monitor-signature": signature,
    },
    body,
  });
  if (!response.ok) {
    throw new Error(`monitor ingest returned HTTP ${response.status}`);
  }
}

async function notifyDiscord(env, content) {
  if (!env.DISCORD_WEBHOOK_URL) {
    return;
  }
  const response = await fetch(env.DISCORD_WEBHOOK_URL, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ content: content.slice(0, 1800) }),
  });
  if (!response.ok) {
    throw new Error(`discord webhook returned HTTP ${response.status}`);
  }
}

async function signBody(secret, body) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(body),
  );
  const bytes = Array.from(new Uint8Array(signature));
  const hex = bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `sha256=${hex}`;
}
