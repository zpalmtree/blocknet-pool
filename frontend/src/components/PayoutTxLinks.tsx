import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';

import { shortTx } from '../lib/format';

const PREVIEW_COUNT = 2;

export function PayoutTxLinks({ hashes }: { hashes?: string[] }) {
  const txHashes = hashes || [];
  const [showDialog, setShowDialog] = useState(false);

  if (!txHashes.length) return <>-</>;

  if (txHashes.length === 1) {
    const h = txHashes[0];
    return (
      <a href={`https://explorer.blocknetcrypto.com/tx/${h}`} target="_blank" rel="noopener" title={h}>
        {shortTx(h)}
      </a>
    );
  }

  useEffect(() => {
    if (!showDialog) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setShowDialog(false);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [showDialog]);

  const visibleHashes = txHashes.slice(0, PREVIEW_COUNT);
  const hiddenCount = Math.max(0, txHashes.length - PREVIEW_COUNT);

  return (
    <>
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
          {hiddenCount > 0 ? <span className="payout-tx-links__more">{` +${hiddenCount}`}</span> : null}
        </span>
        {hiddenCount > 0 ? (
          <button className="payout-tx-links__toggle" type="button" onClick={() => setShowDialog(true)}>
            {`Show all ${txHashes.length}`}
          </button>
        ) : null}
      </span>
      {showDialog
        ? createPortal(
            <div className="payout-tx-dialog-backdrop" role="presentation" onClick={() => setShowDialog(false)}>
              <div
                className="payout-tx-dialog"
                role="dialog"
                aria-modal="true"
                aria-label={`Payout transaction hashes (${txHashes.length})`}
                onClick={(event) => event.stopPropagation()}
              >
                <div className="payout-tx-dialog__header">
                  <div>
                    <div className="payout-tx-dialog__title">Payout Transactions</div>
                    <div className="payout-tx-dialog__meta">{`${txHashes.length} transaction hashes`}</div>
                  </div>
                  <button className="payout-tx-dialog__close" type="button" onClick={() => setShowDialog(false)}>
                    Close
                  </button>
                </div>
                <div className="payout-tx-dialog__list">
                  {txHashes.map((h) => (
                    <a
                      key={h}
                      className="payout-tx-dialog__item"
                      href={`https://explorer.blocknetcrypto.com/tx/${h}`}
                      target="_blank"
                      rel="noopener"
                      title={h}
                    >
                      {h}
                    </a>
                  ))}
                </div>
              </div>
            </div>,
            document.body
          )
        : null}
    </>
  );
}
