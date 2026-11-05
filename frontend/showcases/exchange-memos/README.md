# Exchange memo showcase

Run `npm --prefix frontend run showcase:memos` from the pool repository.
Open http://localhost:5198/showcases/exchange-memos/.

Uses the real setup and stats components with synthetic API fixtures. All memos
are synthetic. The SafeTrade shared address is the public address supplied by
the owner; no account-specific deposit memo is included. The displayed 10 BNT
floor is a fixture, not an exchange deposit requirement or production setting.

States cover normal address-only use, detected shared address, successful memo
lookup, empty, loading, request error, invalid UTF-8 length, unrecognized address,
setup, feature disabled, a rotated address registry, pasted `memo:` account keys
(`pasted-key`, `pasted-key-unknown`) and the admin miners table (`admin`). `?lang=ar` exercises RTL.

The source-based [Seine walkthrough](seine.html) shows first-run setup, login
confirmation, and missing/unsupported memo messages. The stats footer now shows
only the minimum and one status line; setup retains the exchange explanation.

Memo accounts show the configured personal-wallet minimum when it is lower.
This comparison replaces the explanation about reducing small deposits.

`rollout-off` and `rollout-off-setup` exercise the legacy API contract: no
required-memo registry, admissions disabled, and a 1 BNT regular minimum.
These states confirm that a code-only deployment does not require existing
shared-address users to enter a memo. The other fixtures retain their synthetic
0.1 BNT regular minimum. The approved release policy is 10 BNT for memo accounts.
