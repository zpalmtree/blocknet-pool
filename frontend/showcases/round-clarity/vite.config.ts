import { defineConfig } from 'vite';
import { readFileSync } from 'node:fs';

export default defineConfig({
  base: '/',
  server: { host: '0.0.0.0', port: 5194, strictPort: true },
  plugins: [{
    name: 'round-clarity-fixtures',
    transformIndexHtml() {
      return [{ tag: 'script', attrs: { type: 'module', src: '/showcases/round-clarity/fixtures.ts' }, injectTo: 'head-prepend' }];
    },
    configureServer(server) {
      server.middlewares.use('/ui-assets', (req, res, next) => {
        const name = req.url?.slice(1);
        if (name !== 'pool-entered.png' && name !== 'mining-tui.png') return next();
        res.setHeader('Content-Type', 'image/png');
        res.end(readFileSync(new URL(`../../../apps/blocknet-pool-api/src/ui/assets/${name}`, import.meta.url)));
      });
      server.middlewares.use('/favicon.svg', (_req, res) => { res.setHeader('Content-Type', 'image/svg+xml'); res.end('<svg xmlns="http://www.w3.org/2000/svg"/>'); });
    },
  }],
});
