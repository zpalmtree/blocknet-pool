// API amounts are integer atomic units (100,000,000 per BNT).
export function createCoinFormatters(locale: string) {
  const options = { numberingSystem: 'latn', minimumFractionDigits: 2 };
  const regular = new Intl.NumberFormat(locale, { ...options, maximumFractionDigits: 2 });
  const small = new Intl.NumberFormat(locale, { ...options, maximumFractionDigits: 8 });
  const compact = new Intl.NumberFormat(locale, {
    numberingSystem: 'latn', notation: 'compact', maximumFractionDigits: 2,
  });
  const formatCoinAmount = (atoms: number | null | undefined): string => {
    const amount = (atoms ?? 0) / 1e8;
    if (!Number.isFinite(amount)) return regular.format(0);
    return (Math.abs(amount) < 1 ? small : regular).format(amount);
  };
  const formatCoins = (atoms: number | null | undefined): string =>
    atoms == null ? '0 BNT' : `${formatCoinAmount(atoms)} BNT`;
  const formatCompactCoins = (atoms: number | null | undefined): string => {
    if (atoms == null || !Number.isFinite(atoms)) return '0 BNT';
    const amount = atoms / 1e8;
    return Math.abs(amount) < 1000 ? formatCoins(atoms) : `${compact.format(amount)} BNT`;
  };
  return { formatCoins, formatCompactCoins, formatCoinAmount };
}
