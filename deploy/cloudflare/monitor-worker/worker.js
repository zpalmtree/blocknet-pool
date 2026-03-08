export default {
  async scheduled(_event, env, _ctx) {
    await runCheck(env);
  },

  async fetch(_request, env, _ctx) {
    const result = await runCheck(env);
    return new Response(JSON.stringify(result, null, 2), {
      headers: { "content-type": "application/json" },
    });
  },
};

async function runCheck(env) {
  const key = "public_http";
  const now = Math.floor(Date.now() / 1000);
  const prior = await loadState(env, key);
  const probe = await probePublicMonitor(env.PUBLIC_MONITOR_URL);

  if (!probe.ok) {
    const startedAt = prior?.status === "down" ? prior.startedAt : now;
    const next = { status: "down", startedAt, lastCheckedAt: now, detail: probe.detail };
    await env.MONITOR_STATE.put(key, JSON.stringify(next));
    await notifyIngress(env, {
      service: "public_http",
      status: "down",
      started_at: startedAt,
      checked_at: now,
      summary: "Cloudflare could not reach the public pool endpoint",
      detail: probe.detail,
    });
    if (prior?.status !== "down") {
      await notifyDiscord(
        env,
        `[DOWN] public_http\nCloudflare could not reach ${env.PUBLIC_MONITOR_URL}\n${probe.detail}`,
      );
    }
    return next;
  }

  const next = { status: "up", startedAt: null, lastCheckedAt: now, detail: probe.detail };
  await env.MONITOR_STATE.put(key, JSON.stringify(next));
  await notifyIngress(env, {
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
  });
  if (prior?.status === "down") {
    await notifyDiscord(
      env,
      `[RECOVERED] public_http\n${env.PUBLIC_MONITOR_URL}\nDowntime started at ${prior.startedAt} and recovered at ${now}.`,
    );
  }

  return next;
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
