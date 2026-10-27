import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { calculatorHtml } from '../../scripts/rewards-calculator.mjs';
import { LOCALES } from '../../src/i18n/locales.mjs';
const states = ['success', 'empty', 'loading', 'error', 'invalid', 'zero', 'stale', 'dark', 'rtl'];
const port = Number(process.env.PORT || 5191);
const assets = {
  '/blog.css': ['../../blog/blog.css', 'text/css'],
  '/rewards-calculator.js': ['../../blog/rewards-calculator.js', 'text/javascript'],
  '/rewards-math.js': ['../../blog/rewards-math.js', 'text/javascript'],
};
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (assets[url.pathname]) {
    const [file, type] = assets[url.pathname];
    res.setHeader('Content-Type', type);
    return res.end(await readFile(new URL(file, import.meta.url)));
  }
  const state = states.includes(url.searchParams.get('state')) ? url.searchParams.get('state') : 'success';
  const locale = LOCALES.find(l => l.code === url.searchParams.get('lang'))?.code || (state === 'rtl' ? 'ar' : 'en');
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(`<!doctype html><html lang="${locale}" dir="${locale === 'ar' ? 'rtl' : 'ltr'}" data-theme="${state === 'dark' ? 'dark' : 'light'}"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Rewards calculator showcase</title><link rel="stylesheet" href="/blog.css"><body><main class="wrap prose"><p>Rewards calculator · synthetic network data</p><nav>${states.map(s => `<a href="?state=${s}">${s}</a>`).join(' · ')}</nav>${calculatorHtml(locale)}</main><script>
const state = ${JSON.stringify(state)};
window.fetch = async () => {
  if (state === 'loading') return new Promise(() => {});
  if (state === 'error') throw new Error('Fixture unavailable');
  return { ok: true, json: async () => ({chain: {current_job_height: 69276, network_hashrate: 100000}, pool_fee_pct: 0}) };
};
const input = document.querySelector('[data-calc="hashrate"]');
input.value = state === 'empty' ? '' : state === 'invalid' ? '100001' : state === 'zero' ? '0' : '100';
if (state === 'stale') {
  const observer = new MutationObserver(() => {
    if (document.querySelector('#mining-calculator').getAttribute('aria-busy') === 'false') {
      observer.disconnect(); const now = Date.now(); Date.now = () => now + 300001;
      document.dispatchEvent(new Event('visibilitychange'));
    }
  });
  observer.observe(document.querySelector('#mining-calculator'), {attributes: true});
}
</script><script type="module" src="/rewards-calculator.js"></script></body></html>`);
}).listen(port, '0.0.0.0', () => console.log(`Showcase: http://localhost:${port}/`));
