import { t } from '../i18n';
type PayoutStatus = 'confirmed' | 'unconfirmed' | 'queued';

export function PayoutStatusBadge({ status }: { status: PayoutStatus }) {
  return (
    <span className={`badge ${status === 'confirmed' ? 'badge-confirmed' : 'badge-pending'}`}>
      {t({ confirmed: 'm091', unconfirmed: 'payoutUnconfirmed', queued: 'payoutQueued' }[status])}
    </span>
  );
}
