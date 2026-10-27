# Rewards calculator showcase

Source revision: `0e87e2032233416d271a32d11b472253bfa0711d`.

The production calculator and stylesheet are rendered with synthetic network data.
All 18 desktop (1280px) and mobile (390px) screenshots were reviewed. States cover
success, empty, loading/disabled, API error, invalid input, zero hashrate, stale data,
dark theme, and Arabic RTL. No horizontal overflow was observed.

The miner fee field, release fee note, pool fee readout and fee formula are removed
in all 20 languages. Gross/net results retain the existing fee deductions using
the reviewed release's miner fee and API-provided pool fee.

Validation: 21 frontend tests, TypeScript, translation checks, blog checks and
production build passed. Browser input changes recalculated results correctly.

Stable gallery: http://localhost:5191/

```sh
python3 -m http.server 5191 --directory /home/zp/Code/blocknet/showcases/rewards-calculator
```

Interactive fixture, from this repository's root (requires frontend dependencies):

```sh
node frontend/showcases/rewards-calculator/server.mjs
```

Use `?state=success` (or any state above) and `&lang=ar` for translated fixtures.
The fixture server and gallery use the same port; run one at a time.
