import type { ReactNode } from 'react';

const EXPLORER_BASE_URL = 'https://explorer.blocknetcrypto.com';
type ExplorerLinkProps = { kind: 'block' | 'tx'; value: string; className?: string; children: ReactNode };

export function ExplorerLink({ kind, value, className, children }: ExplorerLinkProps) {
  return (
    <a className={className} href={`${EXPLORER_BASE_URL}/${kind}/${value}`} target="_blank" rel="noopener" title={value}>
      {children}
    </a>
  );
}
