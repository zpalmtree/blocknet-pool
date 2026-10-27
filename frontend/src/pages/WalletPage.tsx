import type { ComponentType } from 'react';

// The wallet page is closed source and lives in the private bnt-wallet repo.
// Builds that serve it copy it into src/wallet-private/ (git-ignored) first;
// other builds show that the wallet is unavailable.
const privatePage = Object.values(
  import.meta.glob<{ WalletPage: ComponentType }>('../wallet-private/WalletPage.tsx', { eager: true }),
)[0]?.WalletPage;

export function WalletPage() {
  if (privatePage) {
    const Page = privatePage;
    return <Page />;
  }
  return (
    <section className="card">
      <h2>Wallet unavailable</h2>
      <p>This build of the pool does not include the wallet.</p>
    </section>
  );
}
