import { t } from '../i18n';
export function BlockStatusBadge({ confirmed, orphaned }: { confirmed: boolean; orphaned: boolean }) {
  if (orphaned) return <span className="badge badge-orphaned">{t("m090")}</span>;
  if (confirmed) return <span className="badge badge-confirmed">{t("m091")}</span>;
  return <span className="badge badge-pending">{t("m092")}</span>;
}
