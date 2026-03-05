import { shortTx } from '../lib/format';

export function PayoutTxLinks({ hashes }: { hashes?: string[] }) {
  const txHashes = hashes || [];
  if (!txHashes.length) return <>-</>;

  if (txHashes.length === 1) {
    const h = txHashes[0];
    return (
      <a href={`https://explorer.blocknetcrypto.com/tx/${h}`} target="_blank" rel="noopener" title={h}>
        {shortTx(h)}
      </a>
    );
  }

  return (
    <>
      {txHashes.slice(0, 3).map((h, idx) => (
        <span key={h}>
          {idx > 0 ? ', ' : ''}
          <a href={`https://explorer.blocknetcrypto.com/tx/${h}`} target="_blank" rel="noopener" title={h}>
            {shortTx(h)}
          </a>
        </span>
      ))}
      {txHashes.length > 3 ? ` +${txHashes.length - 3}` : ''}
    </>
  );
}
