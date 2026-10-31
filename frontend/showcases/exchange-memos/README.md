# Exchange memo showcase

Run `npm --prefix frontend run showcase:memos` from the pool repository.
Open http://localhost:5198/showcases/exchange-memos/.

Uses the real setup and stats components with synthetic API fixtures. All memos
are synthetic. The SafeTrade shared address is the public address supplied by
the owner; no account-specific deposit memo is included. The displayed 10 BNT
floor is a fixture, not an exchange deposit requirement or production setting.

States cover normal address-only use, detected shared address, successful memo
lookup, empty, loading, request error, invalid UTF-8 length, unrecognized address,
setup, feature disabled, and a rotated address registry. `?lang=ar` exercises RTL.
