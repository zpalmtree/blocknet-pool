import { defineConfig } from 'vite';
export default defineConfig({
  base: '/',
  server: { host: '0.0.0.0', port: 5199, strictPort: true },
  build: { rollupOptions: { input: 'showcases/solo-mining/index.html' } },
});
