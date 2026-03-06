import { useState } from 'react';

import { shortTx } from '../lib/format';

const PREVIEW_COUNT = 3;

export function PayoutTxLinks({ hashes }: { hashes?: string[] }) {
  const txHashes = hashes || [];
  const [expanded, setExpanded] = useState(false);

  if (!txHashes.length) return <>-</>;

  if (txHashes.length === 1) {
    const h = txHashes[0];
    return (
      <a href={`https://explorer.blocknetcrypto.com/tx/${h}`} target="_blank" rel="noopener" title={h}>
        {shortTx(h)}
      </a>
    );
  }

  const visibleHashes = expanded ? txHashes : txHashes.slice(0, PREVIEW_COUNT);
  const hiddenCount = Math.max(0, txHashes.length - PREVIEW_COUNT);

  return (
    <span className="payout-tx-links">
      <span className="payout-tx-links__list">
        {visibleHashes.map((h, idx) => (
          <span key={h}>
            {idx > 0 ? ', ' : ''}
            <a href={`https://explorer.blocknetcrypto.com/tx/${h}`} target="_blank" rel="noopener" title={h}>
              {shortTx(h)}
            </a>
          </span>
        ))}
        {!expanded && hiddenCount > 0 ? <span className="payout-tx-links__more">{` +${hiddenCount}`}</span> : null}
      </span>
      {hiddenCount > 0 ? (
        <button className="payout-tx-links__toggle" type="button" onClick={() => setExpanded((open) => !open)}>
          {expanded ? 'Show less' : `Show all ${txHashes.length}`}
        </button>
      ) : null}
    </span>
  );
}
