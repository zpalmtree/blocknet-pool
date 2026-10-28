# First-session tracking guide

Refresh of `track-your-miner-on-bntpool` in all 20 languages, October 28, 2026.
Real production components from `3cddc78`, with invented data injected only by
this showcase's Vite config. No real wallet or miner records are used. Captures
show the English UI; every article provides translated explanations and captions.

Run from `frontend`: `npm exec vite -- --config showcases/pool-tracking/vite.config.ts`.
Open `http://localhost:5197/stats?demo=first-shares` or `?demo=rewards`.
The shared fixture also supports empty, loading, and error states. This change
updates the article, not the stats UI; it adds no loading/empty/disabled controls.

Article captures: shared Windows Chrome Playwright MCP, October 28, 2026,
4:04–4:05 PM EDT, at 1180px desktop and 390px mobile. The mobile first-shares
capture focuses on the share counters; the desktop shows the lookup and balances.
Rewards captures show the actual balance group at both widths. WebP conversion
preserves the captured interface and aspect ratio.

Stable review gallery: http://localhost:8874/pool-tracking/
Start: `python3 -m http.server 8874 --directory /home/zp/Code/blocknet/showcases/blog-i18n`.
Gallery includes English, Arabic and Thai desktop/mobile articles, the source
captures, and missing-image fallback. Inspect the source revision shown there.

Validation: 21 frontend tests; full content/i18n checks, TypeScript, production
build and 440 article cover/metadata checks; real-browser checks of all 20
articles at mobile width, including responsive sources, canonical and alternate
links, same-language stats links, and the preserved `zero-hashrate` anchor.
Warnings about `Workers` refer to the actual pool table label, not CPU threads.
The local static preview lacks the API-served favicon; production supplies it.
