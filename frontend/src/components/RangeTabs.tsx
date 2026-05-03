type RangeTabsProps<T extends string> = { id?: string; options: readonly T[]; value: T; onChange: (value: T) => void };

export function RangeTabs<T extends string,>({ id, options, value, onChange }: RangeTabsProps<T>) {
  return (
    <div className="range-tabs" id={id}>
      {options.map((option) => (
        <button key={option} className={value === option ? 'active' : ''} onClick={() => onChange(option)}>
          {option}
        </button>
      ))}
    </div>
  );
}
