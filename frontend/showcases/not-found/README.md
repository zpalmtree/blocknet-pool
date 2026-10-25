# Page-not-found showcase

Stable gallery: http://localhost:8874/not-found.html

Source behavior: `5e78e70`. Unknown SPA URLs return a translated recovery page
with HTTP 404. Known routes remain HTTP 200, including trailing slashes and
language queries. Unknown API URLs keep JSON 404s and missing blog pages keep
404s. GET and HEAD are covered; HEAD has no response body.

The main app captures use `npm --prefix frontend run showcase:languages -- --port 5185`
and `/missing-page?lang=en`, `ar`, or `th`; the dark scene uses `demo=error`.
The existing fixture config contains public demo data, not real wallet data.
Recovery follows the Get Started link to `/start`. Server-only captures use
production response HTML with its module scripts removed and the matching CSS
served locally; no synthetic replacement markup was drawn.

Twelve desktop/mobile captures were reviewed. There are no separate loading,
empty-data or disabled states because the 404 page does not fetch pool data.
The unchanged language/theme controls retain their established behaviors.

Validation: 113 API tests, 11 frontend tests, strict Clippy, translation check,
production build, 49 live HTTP checks, desktop/mobile inspection, recovery and
back navigation. The first deploy stopped at the stale-UI build guard before
restarts; the normal script rebuilt and deployed successfully on retry.
Only the API restarted, at October 25, 2026, 5:48:26 PM EDT. Other service PIDs
are recorded in verification.json.

To serve this committed copy independently:

```sh
python3 -m http.server 8877 --directory frontend/showcases/not-found
```
