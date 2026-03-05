interface PagerProps {
  offset: number;
  limit: number;
  total: number;
  onPrev: () => void;
  onNext: () => void;
}

export function Pager({ offset, limit, total, onPrev, onNext }: PagerProps) {
  const start = total > 0 ? offset + 1 : 0;
  const end = total > 0 ? Math.min(offset + limit, total) : 0;

  return (
    <div className="pager">
      <span>{total > 0 ? `${start}-${end} of ${total}` : 'No results'}</span>
      <div>
        <button disabled={offset === 0} onClick={onPrev}>
          Prev
        </button>{' '}
        <button disabled={offset + limit >= total} onClick={onNext}>
          Next
        </button>
      </div>
    </div>
  );
}
