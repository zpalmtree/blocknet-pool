export function minimumPayoutAtoms(coins: number | null | undefined): number | null {
  if (coins == null || !Number.isFinite(coins) || coins < 0) return null;
  // Match the payout engine's conversion from config coins to atomic units.
  const atoms = Math.round(coins * 1e8);
  return Number.isSafeInteger(atoms) ? atoms : null;
}

export function payoutProgress(minimum: number, confirmed: number | undefined, queued: number) {
  if (confirmed == null || !Number.isSafeInteger(confirmed) || confirmed < 0 ||
      !Number.isSafeInteger(queued) || queued < 0) {
    return { state: 'unavailable' as const, amount: 0 };
  }
  if (queued > 0) return { state: 'queued' as const, amount: queued };
  const remaining = Math.max(0, minimum - confirmed);
  if (remaining > 0) return { state: 'below' as const, amount: remaining };
  return { state: confirmed > 0 ? 'met' as const : 'empty' as const, amount: 0 };
}

export type HeroPayout =
  | { state: 'unavailable' }
  | { state: 'queued'; amount: number }
  | { state: 'ready'; amount: number; minimum: number }
  | { state: 'below'; confirmed: number; minimum: number; remaining: number; fillPct: number };

/**
 * The front page's next-payout block. Over the minimum the bar is full; below
 * it the bar fills toward the minimum.
 */
export function heroPayout(minimum: number | null, confirmed: number | undefined, queued: number): HeroPayout {
  if (minimum == null) return { state: 'unavailable' };
  const progress = payoutProgress(minimum, confirmed, queued);
  if (progress.state === 'unavailable') return { state: 'unavailable' };
  if (progress.state === 'queued') return { state: 'queued', amount: progress.amount };
  const balance = confirmed ?? 0;
  if (progress.state === 'met') {
    return { state: 'ready', amount: balance, minimum };
  }
  return {
    state: 'below', confirmed: balance, minimum, remaining: progress.amount,
    fillPct: minimum > 0 ? Math.min(100, Math.max(0, (balance / minimum) * 100)) : 0,
  };
}
