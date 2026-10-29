import { defineConfig } from 'vite';
import { readFileSync, existsSync } from 'node:fs';
import { resolve, sep } from 'node:path';

// Exercise the compiled production app with synthetic APIs, not the live pool.
const dist = resolve(import.meta.dirname, '../../dist');
export default defineConfig({
  base: '/',
  server: { host: '0.0.0.0', port: 5195, strictPort: true },
  plugins: [{
    name: 'performance-production-preview',
    configureServer(server) {
      let chunkDemo = '';
      server.middlewares.use((req, res, next) => {
        const path = new URL(req.url || '/', 'http://localhost').pathname;
        if (path.startsWith('/ui-assets/')) {
          const file = resolve(dist, path.slice('/ui-assets/'.length));
          if (!file.startsWith(dist + sep) || !existsSync(file)) { res.statusCode = 404; res.end(); return; }
          const type = file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : 'application/json';
          res.setHeader('Content-Type', type);
          res.setHeader('Cache-Control', 'no-store');
          if (path.includes('/chunks/StatsPage-') && chunkDemo === 'chunk-error') {
            res.statusCode = 404;
            res.end();
            return;
          }
          if (path.includes('/chunks/StatsPage-') && chunkDemo === 'chunk-loading') {
            setTimeout(() => res.end(readFileSync(file)), 3000);
            return;
          }
          res.end(readFileSync(file));
          return;
        }
        if (req.headers.accept?.includes('text/html') && !path.startsWith('/showcases/')) {
          chunkDemo = new URL(req.url || '/', 'http://localhost').searchParams.get('demo') || '';
          const html = readFileSync(resolve(dist, 'index.html'), 'utf8').replace('<head>', '<head><title>Pool performance preview</title><script type="module" src="/showcases/performance/fixtures.ts"></script>');
          res.setHeader('Content-Type', 'text/html');
          res.end(html);
          return;
        }
        next();
      });
    },
  }],
});
