import '../ui-simplify/fixtures';
const params = new URLSearchParams(location.search);
const fixtureFetch = window.fetch.bind(window);
const demo = params.get('demo') || 'success';
window.fetch = async (input, init) => {
  const path = String(input);
  if ((path.startsWith('/api/stats/history') || path.includes('/hashrate?')) && !['empty', 'loading', 'error'].includes(demo)) {
    const range = new URL(path, location.origin).searchParams.get('range') || '7d';
    const count = { '1h': 12, '24h': 288, '7d': 2016, '30d': 8640 }[range] || 12;
    const delay = demo === 'race' && range === '30d' ? 1600 : 80;
    await new Promise((resolve) => setTimeout(resolve, delay));
    const now = Math.floor(Date.now() / 1000);
    return Response.json(Array.from({ length: count }, (_, i) => ({ timestamp: now - (count - 1 - i) * 300, hashrate: 125000 + Math.sin(i / 20) * 15000 + (i % 223 === 0 ? 40000 : 0) })));
  }
  return fixtureFetch(input, init);
};
