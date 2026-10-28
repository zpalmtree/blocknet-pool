// Development-only: exercise the real dashboard with synthetic API responses.
import '../ui-simplify/fixtures';
const params = new URLSearchParams(location.search);
const state = params.get('round') || 'success';
const fixtureFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const response = await fixtureFetch(input, init);
  if (!String(input).startsWith('/api/stats/insights') || !response.ok) return response;
  const data = await response.json();
  data.round = {
    elapsed_seconds: state === 'zero' ? 0 : 2644,
    effort_pct: state === 'unknown' ? null : state === 'zero' ? 0 : state === 'high' ? 312.4 : state === 'low' ? 42.8 : 114.3,
    expected_block_seconds: ['unknown', 'no-rate'].includes(state) ? null : 3360,
    timer_effort_pct: 78.7,
  };
  return Response.json(data);
};
