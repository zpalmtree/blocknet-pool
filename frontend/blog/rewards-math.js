// Blocknet consensus: blocknet-core/miner.go GetBlockReward; block.go BlockIntervalSec.
// Keep these constants in step with the active chain when its emission rules change.
export const BLOCKS_PER_DAY = 288;
const BLOCKS_PER_MONTH = 8640;
export const LIVE_DATA_MAX_AGE_MS = 5 * 60 * 1000;

export function blockReward(height) {
  if (!Number.isSafeInteger(height) || height < 1) throw new RangeError('Invalid block height');
  const month = Math.floor(height / BLOCKS_PER_MONTH);
  const atomic = month >= 48 ? 200000000 : Math.floor((72325093035 - 200000000) * Math.exp(-0.75 * (month / 12)) + 200000000);
  return atomic / 1e8;
}

// Integrate each reward period, including a month/tail boundary during the estimate.
export function scheduledRewards(height, days) {
  blockReward(height);
  if (!Number.isInteger(days) || days < 1 || days > 7 || !Number.isSafeInteger(height + days * BLOCKS_PER_DAY)) throw new RangeError('Invalid period');
  let remaining = days * BLOCKS_PER_DAY, total = 0;
  while (remaining > 0) {
    const count = Math.min(remaining, BLOCKS_PER_MONTH - height % BLOCKS_PER_MONTH);
    total += count * blockReward(height);
    height += count;
    remaining -= count;
  }
  return total;
}

export function liveInputs(stats, info) {
  const height = stats?.chain?.current_job_height;
  const network = stats?.chain?.network_hashrate;
  const poolFee = info?.pool_fee_pct;
  blockReward(height);
  if (typeof network !== 'number' || !Number.isFinite(network) || network <= 0 ||
      typeof poolFee !== 'number' || !Number.isFinite(poolFee) || poolFee < 0 || poolFee > 100) throw new RangeError('Invalid live inputs');
  return { height, network, poolFee };
}

// Accept decimal commas and Arabic digits without interpreting grouping separators.
// Ambiguous mixed separators, exponents and trailing text are rejected.
export function decimalInput(value) {
  const normalized = String(value).trim().replace(/[٠-٩]/g, c => String(c.charCodeAt(0) - 1632))
    .replace(/[۰-۹]/g, c => String(c.charCodeAt(0) - 1776)).replace(/[٫,]/g, '.');
  if (!/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(normalized)) return NaN;
  return Number(normalized);
}

export function estimateRewards({ height, network, poolFee }, hashrate, minerFee) {
  liveInputs({ chain: { current_job_height: height, network_hashrate: network } }, { pool_fee_pct: poolFee });
  if (!Number.isFinite(hashrate) || hashrate < 0 || hashrate > network ||
      !Number.isFinite(minerFee) || minerFee < 0 || minerFee > 100) throw new RangeError('Invalid mining inputs');
  const share = hashrate / network;
  const keep = (1 - minerFee / 100) * (1 - poolFee / 100);
  const day = share * scheduledRewards(height, 1), week = share * scheduledRewards(height, 7);
  return { day, week, netDay: day * keep, netWeek: week * keep };
}
