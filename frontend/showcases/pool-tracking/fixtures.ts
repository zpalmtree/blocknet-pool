// Real StatsPage, isolated local showcase origin, invented examples only.
import '../ui-simplify/fixtures';

const state = new URLSearchParams(location.search).get('demo') || 'first-shares';
const baseFetch = window.fetch.bind(window);
const timestamp = Math.floor(Date.now() / 1000) - 30;
const address = 'demo-worker-address-not-a-real-wallet';
const hash = 'a'.repeat(64);
const fresh = state === 'first-shares';
window.fetch = async (input, init) => {
  const path = String(input);
  if (['loading', 'empty', 'error'].includes(state)) return baseFetch(input, init);
  if (path === '/api/info') return Response.json({ pool_name: 'bntpool.com', pool_url: 'https://bntpool.com', stratum_port: 3333, pool_fee_pct: 0, min_payout_amount: 0.1, blocks_before_payout: 60, pplns_window_duration: '6h' });
  if (path.startsWith('/api/miner/')) {
    if (path.includes('/balance')) return Response.json({ address, balance: { pending_confirmed: fresh ? 0 : 12000000, pending_queued: 0, paid: fresh ? 0 : 36000000 }, pending_estimate: { estimated_pending: fresh ? 0 : 8000000, blocks: fresh ? [] : [{ height: 149999, hash, estimated_credit: 8000000, credit_withheld: false, validation_state: 'verified', validation_detail: '', confirmations_remaining: 24, timestamp }] } });
    if (path.includes('/hashrate')) return Response.json(Array.from({ length: 12 }, (_, i) => ({ timestamp: timestamp - (11-i)*300, hashrate: 1150+i*10 })));
    return Response.json({ hashrate: 1250, mining_since: timestamp - (fresh ? 600 : 86400), blocks_found: 0, total_accepted: fresh ? 18 : 12840, total_rejected: 0, latest_miner_version: '0.2.45', workers: [{ worker: 'seine-4821', version_status: 'current', miner_version: '0.2.45', hashrate: 1250, accepted: fresh ? 18 : 12840, rejected: 0, last_share_at: timestamp, backend: 'cpu' }], shares: Array.from({ length: 3 }, (_, i) => ({ job_id: 'example-job', worker: 'seine-4821', difficulty: 37500, status: 'accepted', created_at: timestamp-i*30 })), payouts: fresh ? [] : [{ amount: 36000000, fee: 10000, tx_hash: hash, timestamp: timestamp-3600, confirmed: true }], verification_hold: null });
  }
  return baseFetch(input, init);
};
