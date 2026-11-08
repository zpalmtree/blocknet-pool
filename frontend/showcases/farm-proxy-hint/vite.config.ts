import { defineConfig } from 'vite';
export default defineConfig({
  base: '/',
  publicDir: '../apps/blocknet-pool-api/src/ui/assets',
  server: { host: '0.0.0.0', port: 5205, strictPort: true },
  plugins: [{ name: 'showcase-public-assets', configureServer(server) {
    server.middlewares.use((req, _res, next) => {
      if (req.url?.startsWith('/ui-assets/')) req.url = req.url.slice('/ui-assets'.length);
      next();
    });
  } }],
  build: { outDir: 'dist-farm-proxy-showcase', rollupOptions: { input: 'showcases/farm-proxy-hint/index.html' } },
});
