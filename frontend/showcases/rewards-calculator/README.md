# Rewards calculator showcase

Source base: `d1dd758`; calculator markup blob `46fecabb95b24d7aa3a8317e383e828c11bc2737`, runtime blob `8f5009a7034afcbbba02e942e0876f2c1503dc81`.

Updated October 31, 2026 (EDT): one BNT estimate per day and week replaces the before-fees / after-fees comparison in all 20 languages. Existing fee-adjusted calculations are preserved.

The production calculator and stylesheet are rendered with synthetic network data.
All 18 desktop (1280px) and mobile (390px) screenshots were reviewed. States cover
success, empty, loading/disabled, API error, invalid input, zero hashrate, stale data,
dark theme, and Arabic RTL. No horizontal overflow was observed.

Network hashrate uses the same scaled units and two decimal places as the pool
dashboard. Reward calculations and input units are unchanged. Browser checks
covered H/s through PH/s, including the 1000 H/s boundary.

Validation: 33 frontend tests, TypeScript, translation checks, blog checks and
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
