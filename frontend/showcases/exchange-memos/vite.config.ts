import { defineConfig } from 'vite';
export default defineConfig({
  base: '/',
  publicDir: '../apps/blocknet-pool-api/src/ui/assets',
  server: { host: '0.0.0.0', port: 5198, strictPort: true },
  plugins: [{ name: 'showcase-public-assets', configureServer(server) {
    server.middlewares.use((req, _res, next) => {
      if (req.url?.startsWith('/ui-assets/')) req.url = req.url.slice('/ui-assets'.length);
      next();
    });
  } }],
  build: { outDir: 'dist-memo-showcase', rollupOptions: { input: 'showcases/exchange-memos/index.html' } },
});
