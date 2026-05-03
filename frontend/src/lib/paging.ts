import { useCallback, useEffect, useMemo, useState } from 'react';

import type { PagedResponse, PagerState } from '../types';

export function usePagedData<T>(
  liveTick: number,
  fetchPage: (limit: number, offset: number) => Promise<PagedResponse<T>>,
  initialLimit = 25,
  refreshModulo = 12,
  enabled = true
) {
  const [items, setItems] = useState<T[]>([]);
  const [pager, setPager] = useState<PagerState>({ offset: 0, limit: initialLimit, total: 0 });

  const loadPage = useCallback(async () => {
    if (!enabled) return;
    try {
      const page = await fetchPage(pager.limit, pager.offset);
      setItems(page.items);
      setPager((prev) => ({ ...prev, total: page.total }));
    } catch {
      setItems([]);
    }
  }, [enabled, fetchPage, pager.limit, pager.offset]);

  useEffect(() => {
    void loadPage();
  }, [loadPage]);

  useEffect(() => {
    if (!enabled || liveTick <= 0 || liveTick % refreshModulo !== 0) return;
    void loadPage();
  }, [enabled, liveTick, loadPage, refreshModulo]);

  const pagerProps = useMemo(
    () => ({
      offset: pager.offset,
      limit: pager.limit,
      total: pager.total,
      onPrev: () => setPager((prev) => ({ ...prev, offset: Math.max(0, prev.offset - prev.limit) })),
      onNext: () => setPager((prev) => ({ ...prev, offset: prev.offset + prev.limit })),
    }),
    [pager.limit, pager.offset, pager.total]
  );

  return { items, pager, setPager, pagerProps };
}
