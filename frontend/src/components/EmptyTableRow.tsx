import type { ReactNode } from 'react';

export function EmptyTableRow({ colSpan, children }: { colSpan: number; children: ReactNode }) {
  return (
    <tr>
      <td className="table-empty" colSpan={colSpan}>
        {children}
      </td>
    </tr>
  );
}
