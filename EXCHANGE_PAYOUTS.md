# Exchange payout memos

Ordinary address and Blocknet ID mining keeps its existing accounting identity and
payout minimum. Exchange mining is opt-in: each exact address + UTF-8 memo pair has
its own balance, shares, workers, payout queue and statistics. Worker names are
not account identifiers. Memos preserve case, spaces and leading zeros, allow up
to 124 UTF-8 bytes, and reject control characters. Never substitute a worker name
or Stratum password for a deposit memo.

## Operator configuration

New memo logins are disabled by default (`payout_memos_enabled: false`). Before
enabling them, set `memo_min_payout_amount` in BNT to a value strictly above
`min_payout_amount`, appropriate for the exchange. The 10 BNT shown in the local
showcase is synthetic, not a production setting or an exchange requirement.
Ensure any `payout_max_per_recipient` and `payout_max_total_per_tick` caps can
accommodate this minimum. Amount reduction for wallet liquidity never goes below
the account's minimum. The API, setup UI, stats and Seine login report the floor.

`shared_exchange_addresses` is an array of `{ "address": "...", "name": "..." }`.
The default registry includes the SafeTrade shared address supplied by the owner.
An explicit array replaces the defaults; keep retired shared addresses in it when
adding rotated addresses so old configurations still require a memo. Unknown
addresses can use memos without a registry entry. User submissions never alter
the registry. Config changes require the affected services to restart.

Known shared addresses without a memo are rejected at login and excluded from
payout sending. Existing address-only balances are retained for operator
reconciliation; they cannot be assigned to a customer automatically. Changing an
address or memo starts a separate account and does not move an old balance.

## Rollout and recovery

Deploy the updated gateway, pool engine/payout process and API/UI before enabling
memo admissions or publishing miner instructions. The gateway restart disconnects
miners; coordinate it as a separate deployment. Updated gateways reject handover
to an engine that does not acknowledge the same memo account. Updated Seine also
requires the `payout_memo_v1` capability, matching account ID and positive minimum
before accepting jobs, including after reconnect/failover. Legacy miners retain
their existing login contract. Seine farm proxies do not yet support memo mining.

The additive `payout_destinations` PostgreSQL table maps opaque account IDs to
immutable destinations. Include it in backups with the accounting tables. Never
roll the payout process back to a version unaware of these keys after memo
accounts have received credit. Disabling admissions does not delete accounts or
stop already-earned memo payouts; keep their configured floor when disabling.

Wallet preview and live sends use the real address plus hex-encoded memo bytes.
History/journal recovery matches the exact address, memo and amount. Completed
send-status recovery retains the existing batch idempotency key, which includes
the immutable account identity; unknown outcomes remain held by existing recovery
rules. No daemon or consensus changes are required. Solo coinbase mining does not
support payout memos.

## Miner setup and stats

Use an updated Seine build and a pool with memo admissions enabled:

```sh
./seine --pool-url stratum+tcp://POOL:3333 --address 'EXCHANGE_ADDRESS' --payout-memo 'YOUR_DEPOSIT_MEMO'
```

Use the exchange's current full deposit address and your own memo. In the pool UI,
expand **Mining to an exchange?** to create a command. On Stats, known shared
addresses reveal the memo input automatically; **Add deposit memo** supports
unrecognized or newly rotated addresses. Address-only users have no additional
required field. The browser remembers the address and memo together locally.

`POST /api/payout-destination` resolves `{ "address": "...", "memo": "..." }` to
an opaque `account` and `min_payout_amount`, without creating a ledger entry.
Existing `/api/miner/:address` routes accept that account key. Memos are not put
in request URLs or responses. Statistics remain public: the hash is an account
selector, not authentication or a guarantee against guessing a memo.

For fixture previews run `npm --prefix frontend run showcase:memos` and open
http://localhost:5198/showcases/exchange-memos/.
