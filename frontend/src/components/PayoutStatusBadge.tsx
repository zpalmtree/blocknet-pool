type PayoutStatus = 'confirmed' | 'unconfirmed' | 'queued';

export function PayoutStatusBadge({ status }: { status: PayoutStatus }) {
  return (
    <span className={`badge ${status === 'confirmed' ? 'badge-confirmed' : 'badge-pending'}`}>
      {status}
    </span>
  );
}
