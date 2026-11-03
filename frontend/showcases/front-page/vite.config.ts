import { defineConfig } from 'vite';
export default defineConfig({
  base: '/',
  server: { host: '0.0.0.0', port: 5201, strictPort: true },
  build: { rollupOptions: { input: 'showcases/front-page/index.html' } },
});
