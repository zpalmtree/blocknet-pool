# Stratum gateway

Miners stay connected while Stratum is redeployed. Some miners, and many
farms behind proxies, fail over to another pool when their connection
drops, so every Stratum restart used to cost hashrate.

## Shape

```
miners ──TCP :3333──► blocknet-pool-gateway ──unix socket──► blocknet-pool-stratum@a  (primary)
                                             └──────────────► blocknet-pool-stratum@b  (draining, during a deploy)
```

- **Gateway** (`apps/blocknet-pool-gateway`, rarely changed): owns port
  3333 and every miner TCP connection. It relays newline-delimited JSON-RPC
  between miners and engines. It parses only what it routes on: `login`
  requests, `submit` job IDs, the engine's `job` notifications, job IDs in
  `refill` answers and the responses to its own replayed logins. Everything
  else passes through unchanged.
- **Engine** (`blocknet-pool-stratum`, unchanged protocol logic): with
  `gateway_socket` set, it connects to the gateway instead of binding 3333.
  Each miner becomes a virtual connection, an in-memory duplex stream
  driven by the same connection handler as a TCP miner, keyed by the
  miner's real `ip:port`. Per-IP limits, source risk and logs therefore see
  real peers.

## Frames (gateway ⇄ engine)

`[kind: u8][conn: u64 BE][len: u32 BE][payload]`, one frame per message.

| kind | direction | payload |
|---|---|---|
| `Hello` | engine → gateway | JSON `{instance, pid, started_at_ms}` |
| `Role` | gateway → engine | `primary` or `draining` |
| `Open` | gateway → engine | miner peer `ip:port` |
| `Line` | both | one JSON-RPC line, no trailing newline |
| `Close` | both | empty |
| `Retire` | gateway → engine | empty: no connections left, exit cleanly |

## Engine handover (deploy)

1. The new engine instance starts and connects. The newest engine becomes
   `primary`; the previous one becomes `draining` and stops writing the
   shared live runtime snapshot.
2. The gateway migrates miners in batches (rate-limited, so the new engine
   sees no login storm). For each miner it opens a virtual connection on the
   new engine and replays the miner's cached `login` with a reserved request
   ID, then swallows the response. The engine then pushes a fresh `job`,
   which reaches the miner as an ordinary job update. A failed replay leaves
   the miner on the old engine, and it is retried.
3. The gateway remembers which engine issued each job ID per miner (from
   `job` notifications and `refill` answers), so a share for an old job,
   including a **block candidate**, still goes to the engine that can
   validate it and submit the block. `refill` requests go to the current
   engine, which answers `stale job` for another engine's jobs.
4. Once every miner has moved and the stale-share grace has passed, the
   gateway closes the old engine's virtual connections and sends `Retire`.
   The old engine exits with status 0 and its unit stops.

Payout sweeps are already serialized across processes by a Postgres
advisory lock. Each engine instance keeps its own found-block outbox file
and holds an exclusive `flock` on it while running; an engine recovers
another instance's outbox only if it can take that lock (the owner is gone).

## Engine loss

If an engine disappears without a successor, the gateway keeps miners
connected and answers their requests with `server busy, retry` (seine
retries). When an engine connects, miners are migrated as above. Miners
are disconnected only if no engine appears within `engine_absent_timeout`.

## Gateway upgrades

These are rare, because the gateway only relays lines. Today a gateway
restart (`scripts/deploy_bntpool.sh --stratum-only --restart-gateway`)
disconnects every miner once, like a Stratum restart used to. A staggered
drain (a new gateway beside the old one with `SO_REUSEPORT`, the old one
closing miners gradually) is a planned follow-up.

## TLS miners (`stratum+ssl`)

The gateway can also accept miners over TLS (`stratum+ssl://host:port` in
seine): a TLS handshake, then the same newline-delimited JSON-RPC as port
3333. Some networks forge TCP resets into plaintext Stratum; encryption
hides the protocol from that inspection. TLS miners share the relay,
`gateway_max_miners` and metrics with plain miners.

| key | default | |
|---|---|---|
| `gateway_tls_port` | `0` (off) | TLS port on `stratum_host`, e.g. `3443` |
| `gateway_tls_cert` | | PEM certificate chain, leaf first (Let's Encrypt `fullchain.pem`) |
| `gateway_tls_key` | | PEM private key (`privkey.pem`) |

- The gateway runs as `blocknet`, so both files must be readable by that
  user. Certbot keeps `/etc/letsencrypt/{live,archive}` root-only; copy the
  files from a deploy hook (below) rather than loosening those directories.
- The gateway checks the files every 5 minutes and swaps in a renewed
  certificate without a restart. A file it cannot load is logged as a
  warning and the current certificate stays in use.
- Open the port in the firewall (`ufw allow 3443/tcp`). Seine verifies the
  certificate against the miner's pool host name, so miners use
  `stratum+ssl://bntpool.com:3443` (a name the certificate covers).
- Turning TLS on or changing its port needs a gateway restart.

### Prepare certificates while TLS remains disabled

Run from the pool checkout:

```sh
bash scripts/prepare_bntpool_tls.sh
```

This requires the existing Certbot certificate for `bntpool.com` and gateway
TLS to be disabled (`gateway_tls_port` absent or zero). It stages the certificate
and key under `/etc/blocknet/pool/tls`, owned by `root:blocknet` with mode `0640`,
and installs `/etc/letsencrypt/renewal-hooks/deploy/blocknet-pool-gateway`.
The hook runs the root-owned helper at
`/usr/local/libexec/blocknet-pool-gateway-tls.py`. Neither preparation nor renewal
changes pool configuration, firewall rules, listeners or services. Repeating the
command is safe; identical certificates do not trigger reloads.

The helper validates the hostname, expiry and key pairing before replacing
either file. Failed validation preserves the staged pair. Each file replacement
is atomic; a gateway poll between replacements retains its previous certificate
and retries on the next poll. Keys remain on the pool host.

### Activate later, when a TLS-capable miner release is available

1. Verify the published Seine build supports `stratum+ssl://`; v0.2.45 does not.
   Leave the advertised endpoint on plaintext until the TLS endpoint passes checks.
2. Run preparation again, then merge these fields into the existing pool config
   without changing its other settings:

   ```json
   {
     "gateway_tls_port": 3443,
     "gateway_tls_cert": "/etc/blocknet/pool/tls/fullchain.pem",
     "gateway_tls_key": "/etc/blocknet/pool/tls/privkey.pem"
   }
   ```

3. Open TCP port 3443 and deploy a TLS-capable gateway during an explicitly
   approved restart window. A gateway restart disconnects miners once. Engine
   handovers alone do not load gateway listener settings. The deploy script's
   `--restart-gateway` path currently restarts only when the gateway binary hash
   changes; an already-uploaded binary or configuration-only activation needs
   an explicitly scheduled gateway restart as well.
4. Check `openssl s_client -connect bntpool.com:3443 -servername bntpool.com
   -verify_hostname bntpool.com -verify_return_error </dev/null` from outside the
   host, then verify a released Seine client logs in, receives jobs, submits shares
   and reconnects using `stratum+ssl://bntpool.com:3443`. Check gateway TLS metrics
   and confirm existing plaintext miners still work before advertising TLS.
5. Roll back by keeping miners on port 3333. Disabling the TLS listener by setting
   its port to zero requires another approved gateway restart; deleting certificate
   files does not disable a running TLS listener.

Use the hostname for certificate authentication. Seine's IP-address TLS mode
encrypts traffic but does not verify certificate trust or identity. TLS does not
replace the existing plaintext default automatically.

### Local verification

```sh
python3 scripts/test_prepare_gateway_tls.py
cargo test --locked -p pool-gateway
cargo build --locked -p blocknet-pool-gateway-app
```

In a Seine checkout containing the TLS tests, run:

```sh
cargo +1.93.0 test --locked --no-default-features --bin seine pool::tls_tests
SEINE_TEST_GATEWAY_BIN=/absolute/path/to/blocknet-pool/target/debug/blocknet-pool-gateway \
  cargo +1.93.0 test --locked --no-default-features --bin seine \
  tls_gateway_login_job_submit_and_reconnect -- --ignored
```

The cross-repo test starts an isolated gateway on loopback with fixture certificates
and a fixture engine. It exercises the actual Seine client through TLS login, jobs,
share messages and reconnects; it does not start hashing or test PoW acceptance.
Hostname trust rejection and refusal to downgrade TLS to plaintext are covered by
the miner's transport tests. The gateway tests cover renewal, failed renewal,
invalid handshakes and plain/TLS relay compatibility.

Miner sockets, plain and TLS, use TCP keepalive (after 60 s idle, every
10 s, 3 probes) and a 60 s `TCP_USER_TIMEOUT`, so a miner that vanished
without closing frees its slot within about two minutes.

## Operations

- **Deploy Stratum**: `scripts/deploy_bntpool.sh --stratum-only`. In gateway
  mode it starts the idle instance (`blocknet-pool-stratum@a` or `@b`) on
  the new binary, waits for the gateway to retire the old one (it exits 0),
  and moves the enable to the new instance. If the handover stalls for 10
  minutes, both engines keep running and the deploy fails with the gateway's
  metrics.
- **Change validation or vardiff settings**: edit config.json, then
  `sudo systemctl reload blocknet-pool-stratum@<active>` (SIGHUP; see
  `Config::with_hot_settings_from`). No engine restart is needed.
- **Migrate a host** (once): `scripts/deploy_bntpool.sh --stratum-only
  --no-restart`, then `scripts/migrate_bntpool_to_gateway.sh`. Miners
  reconnect once. `scripts/migrate_bntpool_to_gateway.sh --rollback` returns
  to the single Stratum service.
- **Monitoring**: `127.0.0.1:24786/metrics` (`blocknet_pool_gateway_*`):
  miners, miners without an engine, miners on draining engines, engines,
  migrations, replay failures, busy replies, retires, forced reconnects (miners
  still on a draining engine after 3 minutes are reconnected so a handover
  always completes), TLS miners and TLS handshake failures. Alerts: `BlocknetPoolGatewayNoEngine`,
  `BlocknetPoolGatewayMinersWithoutEngine`, `BlocknetPoolGatewayReplayFailures`.
