import { t } from '../i18n';

const RANGE_LABELS: Record<string, string> = { '1h': 'oneHour', '24h': 'hours24', '7d': 'days7', '30d': 'days30' };

type RangeTabsProps<T extends string> = { id?: string; options: readonly T[]; value: T; onChange: (value: T) => void };

export function RangeTabs<T extends string,>({ id, options, value, onChange }: RangeTabsProps<T>) {
  return (
    <div className="range-tabs" id={id}>
      {options.map((option) => (
        <button key={option} className={value === option ? 'active' : ''} onClick={() => onChange(option)}
          aria-label={RANGE_LABELS[option] ? t(RANGE_LABELS[option]) : option} aria-pressed={value === option}>
          {option}
        </button>
      ))}
    </div>
  );
}
