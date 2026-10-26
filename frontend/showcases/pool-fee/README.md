# Pool dashboard header showcase

Source revision: `ebb13223274e45a801bd014d6b79ac588071fae2`. Captures use the real application and synthetic data.
All 26 desktop/mobile captures were reviewed. This stable showcase also covers
the pool fee display and the simplified main hashrate card from earlier updates.

## Preview

Run `npm --prefix frontend run showcase:languages -- --port 5187` from the repo
root. Visit `http://localhost:5187/?demo=success&lang=en`.
Fixture states: `empty`, `loading`, `error`, `missing-fee`, `paid-fee`,
`long-endpoint`. Use `lang=ar` for RTL; the normal theme switch supports dark mode.
Clipboard success/failure captures use a page-local mock of `navigator.clipboard`.

Stable gallery: http://localhost:8877/

```bash
python3 -m http.server 8877 --directory /home/zp/Code/blocknet/showcases/pool-fee
```

## Behavior and checks

The header combines the title, API-backed fee badge, Get Started, rewards
calculator, and Stratum address. It reuses existing button/copy-field styles.
Copy reports success only after the clipboard promise resolves and shows manual
copying guidance on failure. During loading/errors the endpoint is unknown and
Copy is disabled. The header uses neutral colors for every fee value, actions, focus and copy feedback.

20 frontend tests, TypeScript and the production build passed. Browser checks
covered desktop/mobile states, 320px in ten additional languages, localized
link destinations, clipboard success/failure, keyboard focus, and overflow.
The shared browser's existing wallet extension logs an unrelated injection error.

## Operational context

Production pool fee is 0%. The earlier fee cutover restarted Stratum on October
25, 2026 at 11:58:35 PM EDT. Header changes require only an API/UI deployment;
Stratum, monitor, recovery and the daemon should stay running. Seine's flat 2%
miner fee is committed but still needs a separate release.

## Deployment verification

Neutral header deployment verification pending.
