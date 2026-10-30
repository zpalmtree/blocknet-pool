# Wallet gateway

The optional `hosted_wallet` API configuration puts a wallet at `/wallet` on
the pool site and forwards `/api/wallet/v2/*` to a separate wallet service on
loopback. The wallet service and its page are closed source; builds that serve
the page install it into the git-ignored `frontend/src/wallet-private/` first.
Without it, `/wallet` reports that the wallet is unavailable.

The gateway is disabled by default and only `enabled: true` turns it on. While
it is off, the pool registers no wallet routes, reads no credentials, makes no
backend connections and hides the navigation item.

```json
"hosted_wallet": {
  "enabled": false,
  "advertise": false,
  "backend_url": "http://127.0.0.1:8336",
  "gateway_token_file": "/path/outside/repository/gateway.token",
  "service_key": "<64 hex: the wallet service's Ed25519 envelope public key>"
}
```

- **`advertise`** lists the wallet on the site. While it is false (the
  default), an enabled gateway still forwards wallet requests, but
  `/api/info` reports the wallet as disabled, omits the service key and the
  navigation item stays hidden.
- **`backend_url`** is the wallet service's loopback listener.
- **`gateway_token_file`** holds 32 random bytes as hex, mode `0600`, shared
  with the wallet service. The gateway injects it on every forwarded request
  and never exposes it to browsers.
- **`service_key`** is pinned and served through `/api/info`
  (`hosted_wallet_service_key`).

The gateway forwards only the cookie, Origin, Content-Type and Idempotency-Key
request headers, and adds `X-Client-IP`: the client's address as the API's
client-IP policy resolves it (an IPv6 client's /64), for the wallet service's
per-client limits. Clients cannot set it themselves.
On responses it passes back Content-Type, Content-Disposition and Set-Cookie.
Responses are `no-store` and `nosniff`. Request bodies are limited to 16 KiB.
At most 4 requests are forwarded at once; others wait up to 5 seconds for a
slot, then get 429.

`wallet_hints` (also off by default) serves payout hints to the wallet service
at `GET /api/internal/wallet-hints` behind a bearer token from a `0600` file.

Changes take effect when the pool API restarts; Stratum is unaffected.
Production keeps the wallet disabled.
