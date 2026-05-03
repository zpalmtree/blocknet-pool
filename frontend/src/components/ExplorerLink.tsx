import type { ReactNode } from 'react';

type ExplorerLinkProps = { kind: 'block' | 'tx'; value: string; className?: string; children: ReactNode };

export function ExplorerLink({ kind, value, className, children }: ExplorerLinkProps) {
  return (
    <a className={className} href={`https://explorer.blocknetcrypto.com/${kind}/${value}`} target="_blank" rel="noopener" title={value}>
      {children}
    </a>
  );
}
