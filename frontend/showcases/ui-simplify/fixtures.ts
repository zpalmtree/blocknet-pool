// Only injected by this showcase's Vite config. No live API or wallet data.
// demo=farm adds a large rig list, a long payout history and a due payout sweep.
const demo = new URLSearchParams(location.search).get('demo') || 'success';
const address = 'demo-worker-address-not-a-real-wallet';
if (demo === 'empty') localStorage.removeItem('pool_last_miner_lookup_address');
else localStorage.setItem('pool_last_miner_lookup_address', address);
const timestamp = Math.floor(Date.now() / 1000) - 120;
const hash = 'a'.repeat(64);
const nativeFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const path = String(input);
  if (path.startsWith('https://blocknet.id/')) return Response.json({ address, handle: 'demo' });
  if (!path.startsWith('/api/')) return nativeFetch(input, init);
  if (demo === 'loading') return new Promise<Response>(() => {});
  if (demo === 'error') return Response.json({}, { status: 503 });
  if (path === '/api/info') return Response.json({ pool_name: 'bntpool.com', pool_url: demo === 'long-endpoint' ? 'https://long-regional-pool-endpoint.example.invalid' : 'https://bntpool.com', stratum_port: 3333, pool_fee_pct: demo === 'paid-fee' ? 2.5 : demo === 'missing-fee' ? undefined : 0, min_payout_amount: 1, blocks_before_payout: 60, pplns_window_duration: '24h' });
  if (path === '/api/stats') return Response.json({ pool: { miners: 42, hashrate: 125000, raw_hashrate: 125000, blocks_found: 138, orphaned_blocks: 2, orphan_rate_pct: 1.4, paid_to_miners_total: 72000000000 }, chain: { current_job_height: 150000, network_hashrate: 8000000 } });
  if (path.startsWith('/api/stats/insights')) return Response.json({ round: { elapsed_seconds: 140, expected_block_seconds: 200, effort_pct: 70, timer_effort_pct: 70 }, payout_eta: { next_sweep_at: demo === 'farm' ? timestamp - 60 : timestamp + 300, pending_total_amount: 1200000000, wallet_spendable: 2400000000 }, avg_effort_pct: 95, luck_history: ['empty', 'zero'].includes(demo) ? [] : [{ block_height: 149999, block_hash: hash, timestamp, effort_pct: 95, duration_seconds: 190, confirmed: true, orphaned: false }], rejections: { accepted: 12840, rejected: 2, by_reason: [], totals_by_reason: [] } });
  if (path === '/api/checkpoints') return Response.json({ available: demo !== 'empty', url: '/checkpoints.dat', entries: 150, latest_height: 149000, latest_hash: hash, bytes: 4800, sha256: hash, updated_at: timestamp });
  if (path === '/api/status') return Response.json({ healthy: true, pool_uptime_seconds: 86400, services: Object.fromEntries(['public_http','api','stratum','database','daemon'].map((key) => [key,{ observed: true, healthy: true }])), daemon: { reachable: true, chain_height: 150000, syncing: false }, template: { observed: true, fresh: true, age_seconds: 2, last_refresh_millis: 120 }, uptime: [{ label: '24h', sample_count: 2880, external_sample_count: 1440, api_up_pct: 100, stratum_up_pct: 100, pool_up_pct: 100, daemon_up_pct: 100, database_up_pct: 100, public_http_up_pct: 100 }], incidents: [] });
  if (path.includes('/hashrate') || path.startsWith('/api/stats/history')) return Response.json(['empty', 'zero'].includes(demo) ? [] : Array.from({length: 12}, (_, i) => ({timestamp: timestamp - (11-i)*300, hashrate: 1000+i*25})));
  if (path.startsWith('/api/miner/')) {
    if (path.includes('/balance')) return Response.json({address, balance: {pending_confirmed: demo==='zero'?0:240000000, pending_queued: 0, paid: demo==='zero'?0:720000000}, pending_estimate: {estimated_pending: demo==='zero'?0:120000000, blocks: []}});
    return Response.json({hashrate:demo==='zero'?0:1250,mining_since:timestamp-86400,blocks_found:demo==='zero'?0:3,total_accepted:demo==='zero'?0:12840,total_rejected:demo==='zero'?0:2,latest_miner_version:'0.2.23',workers:['empty','zero'].includes(demo)?[]:(demo==='farm'?Array.from({length:47},(_,i)=>i%3===0?'outdated':'current'):['outdated','unknown','current']).map((version_status,i)=>({worker:`demo-rig-${i+1}`,version_status,miner_version:version_status==='unknown'?null:version_status==='current'?'0.2.23':'0.2.22',hashrate:1250,accepted:12840,rejected:2,last_share_at:timestamp,backend:'cuda'})),shares:[],payouts:demo==='farm'?Array.from({length:26},(_,i)=>({amount:150000000+i*1000000,fee:10000,tx_hash:(i+10).toString(16).repeat(32).slice(0,64),timestamp:timestamp-i*3600,confirmed:i>0})):[],verification_hold:null});
  }
  if (path.startsWith('/api/blocks')) return Response.json({items:demo==='empty'?[]:[{height:149999,hash,reward:600000000000,confirmed:true,orphaned:false,timestamp,effort_pct:95,duration_seconds:190}],total:demo==='empty'?0:1});
  if (path.startsWith('/api/luck')) return Response.json({items:demo==='empty'?[]:[{block_height:149999,block_hash:hash,timestamp,effort_pct:95,duration_seconds:190,confirmed:true,orphaned:false}],total:demo==='empty'?0:1});
  if (path.startsWith('/api/payouts/recent')) return Response.json({items:demo==='empty'?[]:[{total_amount:600000000,total_fee:100000,recipient_count:8,tx_hashes:[hash],timestamp,confirmed:true}],total:demo==='empty'?0:1});
  return Response.json({error:'Not available in the public showcase'}, {status:404});
};
