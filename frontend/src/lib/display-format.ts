// One set per page locale, shared by table refreshes and chart redraws.
export function createDisplayFormatters(locale: string) {
  const timestamp = new Intl.DateTimeFormat(locale, {
    calendar: 'gregory', year: 'numeric', month: 'numeric', day: 'numeric',
    hour: 'numeric', minute: 'numeric', second: 'numeric',
  });
  const time = new Intl.DateTimeFormat(locale, { hour: '2-digit', minute: '2-digit' });
  const compactDate = new Intl.DateTimeFormat(locale, { calendar: 'gregory', month: 'numeric', day: 'numeric' });
  const date = new Intl.DateTimeFormat(locale, { calendar: 'gregory', month: 'short', day: 'numeric' });
  const percentages = new Map<number, Intl.NumberFormat>();
  const formatDate = (formatter: Intl.DateTimeFormat, ms: number) =>
    Number.isNaN(new Date(ms).getTime()) ? 'Invalid Date' : formatter.format(ms);

  return {
    timestamp: (ms: number) => formatDate(timestamp, ms),
    time: (ms: number) => formatDate(time, ms),
    chartDate: (ms: number, compact: boolean) => formatDate(compact ? compactDate : date, ms),
    percent(value: number, digits = 1) {
      let formatter = percentages.get(digits);
      if (!formatter) {
        formatter = new Intl.NumberFormat(locale, {
          numberingSystem: 'latn', maximumFractionDigits: digits, minimumFractionDigits: digits,
        });
        percentages.set(digits, formatter);
      }
      return `${formatter.format(value)}%`;
    },
  };
}
