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
