// Match the pool app's theme choice (stored by src/lib/theme.ts). External file: the CSP
// forbids inline scripts.
try {
  if (window.localStorage.getItem('pool_theme') === 'dark') document.documentElement.dataset.theme = 'dark';
} catch (e) {
  // Storage blocked: keep the light default, like the app.
}
