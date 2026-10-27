# Pool languages showcase

Run `npm --prefix frontend run showcase:languages` from the repository root.

- Real app: <http://localhost:5184/stats?lang=es>
- Built-bundle check (run `npm --prefix frontend run build` first):
  <http://localhost:5184/showcases/pool-languages/bundle.html?lang=es>
- Capture gallery: <http://localhost:5184/showcases/pool-languages/>
- All public app routes work; add `lang=ar`, `lang=ja`, etc.
- Synthetic states: `demo=empty`, `demo=loading`, `demo=error`.
- The fixture intercepts `/api/` and Blocknet ID lookups. No live wallet or pool
  is used. The screenshots contain synthetic addresses and example versions.
- Without `lang`, the app opens in the saved or browser language. Choose English
  from the menu to stay in English; a later menu choice replaces it.

`captures.json` records each route/state. Images were captured with the configured
Windows Playwright MCP at desktop and mobile widths and visually checked.
`source-revision.json` identifies the base commit and exact source content.

For a standalone gallery, serve this directory with a static HTTP server. The
interactive fixture requires the Vite command above.
