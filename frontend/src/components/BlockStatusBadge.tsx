export function BlockStatusBadge({ confirmed, orphaned }: { confirmed: boolean; orphaned: boolean }) {
  if (orphaned) return <span className="badge badge-orphaned">orphaned</span>;
  if (confirmed) return <span className="badge badge-confirmed">confirmed</span>;
  return <span className="badge badge-pending">pending</span>;
}
