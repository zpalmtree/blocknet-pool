import { blockReward, decimalInput, estimateRewards, liveInputs, LIVE_DATA_MAX_AGE_MS } from './rewards-math.js';

const root = document.querySelector('#mining-calculator');
if (root) {
  const messages = JSON.parse(root.querySelector('[data-calculator-messages]').textContent);
  const find = name => root.querySelector(`[data-calc="${name}"]`);
  const fieldset = root.querySelector('fieldset');
  const rate = find('hashrate'), units = find('units');
  const minerFee = Number(root.dataset.minerFee);
  const status = find('status'), hint = find('hint'), refresh = find('refresh');
  const result = find('result');
  const format = new Intl.NumberFormat(document.documentElement.lang, { maximumFractionDigits: 6 });
  const integer = new Intl.NumberFormat(document.documentElement.lang, { maximumFractionDigits: 0 });
  const rateFormat = new Intl.NumberFormat(document.documentElement.lang, {
    numberingSystem: 'latn', minimumFractionDigits: 2, maximumFractionDigits: 2,
  });
  function humanRate(hps) {
    const units = ['H/s', 'KH/s', 'MH/s', 'GH/s', 'TH/s', 'PH/s'];
    let i = 0;
    while (hps >= 1000 && i < units.length - 1) { hps /= 1000; i++; }
    return `${rateFormat.format(hps)} ${units[i]}`;
  }
  let snapshot = null, expires = 0, expirationTimer;
  const clearResults = () => { result.hidden = true; };

  function render() {
    clearResults();
    rate.removeAttribute('aria-invalid');
    if (!snapshot || Date.now() >= expires) return;
    if (!rate.value.trim()) { hint.textContent = messages.calcEmpty; return; }
    const hashrate = decimalInput(rate.value) * Number(units.value);
    try {
      const values = estimateRewards(snapshot, hashrate, minerFee);
      for (const [key, value] of Object.entries(values)) find(key).textContent = value > 0 && value < 0.000001 ? '< ' + format.format(0.000001) : format.format(value);
      result.hidden = false; hint.textContent = '';
    } catch {
      rate.setAttribute('aria-invalid', String(!Number.isFinite(hashrate) || hashrate < 0 || hashrate > snapshot.network));
      hint.textContent = messages.calcInvalid;
    }
  }

  async function load() {
    if (refresh.disabled) return;
    clearTimeout(expirationTimer);
    snapshot = null; clearResults(); fieldset.disabled = true;
    refresh.disabled = true; root.setAttribute('aria-busy', 'true');
    status.textContent = messages.calcLoading; hint.textContent = '';
    for (const key of ['network', 'height', 'reward']) find(key).textContent = '—';
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const [stats, info] = await Promise.all(['/api/stats', '/api/info'].map(async url => {
        const response = await fetch(url, { signal: controller.signal, credentials: 'omit', cache: 'no-store' });
        if (!response.ok) throw new Error('Live data request failed');
        return response.json();
      }));
      snapshot = liveInputs(stats, info);
      expires = Date.now() + LIVE_DATA_MAX_AGE_MS;
      find('network').textContent = humanRate(snapshot.network);
      find('height').textContent = integer.format(snapshot.height);
      find('reward').textContent = format.format(blockReward(snapshot.height)) + ' BNT';
      status.textContent = messages.calcReady.replace('{time}', new Intl.DateTimeFormat(document.documentElement.lang, { hour: 'numeric', minute: '2-digit', second: '2-digit' }).format(new Date()));
      fieldset.disabled = false; render();
      expirationTimer = setTimeout(expire, LIVE_DATA_MAX_AGE_MS);
    } catch {
      controller.abort(); status.textContent = messages.calcError;
    } finally {
      clearTimeout(timeout); refresh.disabled = false; root.setAttribute('aria-busy', 'false');
    }
  }
  function expire() {
    if (snapshot && Date.now() >= expires) {
      snapshot = null; clearResults(); fieldset.disabled = true;
      status.textContent = messages.calcStale; hint.textContent = '';
    }
  }
  for (const input of [rate, units]) input.addEventListener('input', () => { expire(); render(); });
  document.addEventListener('visibilitychange', expire);
  refresh.addEventListener('click', load);
  refresh.disabled = false;
  void load();
}
