import { useCallback, useEffect, useState } from 'react';

import { stratumUrl } from '../lib/format';
import type { ThemeMode } from '../lib/theme';
import type { InfoResponse } from '../types';

interface StartPageProps {
  active: boolean;
  poolInfo: InfoResponse | null;
  theme: ThemeMode;
}

function feeDisplayFor(poolInfo: InfoResponse | null): string {
  const pct = poolInfo?.pool_fee_pct;
  const flat = poolInfo?.pool_fee_flat;
  let out = '';
  if (pct != null && pct > 0) out += `${pct}%`;
  if (flat != null && flat > 0) {
    if (out) out += ' + ';
    out += `${flat} BNT flat`;
  }
  if (!out) out = '0% (no fee)';
  return out;
}

export function StartPage({ active, poolInfo, theme }: StartPageProps) {
  const [copiedKey, setCopiedKey] = useState('');
  const poolUrl = stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url);
  const commandExample = `./seine --pool-url ${poolUrl} --address YOUR_BLOCKNET_ADDRESS`;

  useEffect(() => {
    if (!copiedKey) return;
    const timer = window.setTimeout(() => setCopiedKey(''), 1200);
    return () => window.clearTimeout(timer);
  }, [copiedKey]);

  const copyToClipboard = useCallback((value: string, key: string) => {
    if (!navigator.clipboard?.writeText) return;
    void navigator.clipboard.writeText(value);
    setCopiedKey(key);
  }, []);

  const setupPoolUrlKey = theme === 'dark' ? 'pool-url-quickstart' : 'pool-url-setup';

  const setupSection =
    theme === 'dark' ? (
      <div className="card section">
        <div className="section-header">
          <div>
            <h3>Quick Start</h3>
            <p className="section-lead">
              Download Seine, point it at the pool, and track payouts with your wallet address.
            </p>
          </div>
        </div>

        <div className="quickstart-grid">
          <div className="quickstart-card">
            <span className="quickstart-step">1</span>
            <strong>Download Seine</strong>
            <p>
              Grab the latest release from{' '}
              <a href="https://github.com/zpalmtree/seine/releases" target="_blank" rel="noopener">
                github.com/zpalmtree/seine
              </a>
              .
            </p>
          </div>

          <div className="quickstart-card">
            <span className="quickstart-step">2</span>
            <strong>Run it once</strong>
            <p>
              Start with <code className="mono inline-code">./seine</code> and enter your wallet address plus the pool
              URL.
            </p>
            <div className="quickstart-inline">
              <span className="quickstart-inline-label">Pool URL</span>
              <button
                type="button"
                className="inline-copy-code mono"
                onClick={() => copyToClipboard(poolUrl, setupPoolUrlKey)}
                title="Click to copy"
              >
                {poolUrl}
              </button>
              {copiedKey === setupPoolUrlKey && <span className="inline-copy-note">Copied</span>}
            </div>
          </div>

          <div className="quickstart-card">
            <span className="quickstart-step">3</span>
            <strong>Start mining</strong>
            <p>
              Seine saves your config automatically. Watch the TUI locally or open <a href="/stats">My Stats</a> to
              follow hashrate and balance.
            </p>
          </div>
        </div>

        <p className="quickstart-label">
          Quick start command. Replace only <code className="mono inline-code">YOUR_BLOCKNET_ADDRESS</code>.
        </p>
        <div className="command-copy">
          <pre className="config-block">{commandExample}</pre>
          <button className="copy-btn" type="button" onClick={() => copyToClipboard(commandExample, 'cmd')}>
            {copiedKey === 'cmd' ? 'Copied' : 'Copy Command'}
          </button>
        </div>

        <div className="quickstart-notes">
          <p>
            Saved config: <code className="mono inline-code">seine-data/seine.config.json</code>
          </p>
          <p>
            Need to reset? Edit or delete that file, or override with <code className="mono inline-code">--address</code>,{' '}
            <code className="mono inline-code">--pool-url</code>, and <code className="mono inline-code">--pool-worker</code>.
          </p>
        </div>
      </div>
    ) : (
      <div className="card section">
        <h3>Setup Instructions</h3>
        <ol className="steps">
          <li>
            <strong>Download Seine</strong>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 4 }}>
              Seine is the official Blocknet miner with CPU and GPU support. Download the latest release for your
              platform from{' '}
              <a href="https://github.com/zpalmtree/seine/releases" target="_blank" rel="noopener">
                github.com/zpalmtree/seine
              </a>
              .
            </p>
          </li>
          <li>
            <strong>Run Seine</strong>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 4 }}>
              Open a terminal and run{' '}
              <code className="mono" style={{ background: 'var(--bg)', padding: '1px 6px', borderRadius: 4, fontSize: 13 }}>
                ./seine
              </code>
              . On first launch, Seine will prompt you for your settings:
            </p>
            <ul
              style={{
                color: 'var(--muted)',
                fontSize: 14,
                marginTop: 8,
                listStyle: 'disc',
                paddingLeft: 20,
                lineHeight: 2,
              }}
            >
              <li>
                <strong style={{ color: 'var(--text)' }}>Blocknet address</strong> - your wallet address for receiving
                payouts
              </li>
              <li>
                <strong style={{ color: 'var(--text)' }}>Pool URL</strong> - enter{' '}
                <button
                  type="button"
                  className="inline-copy-code mono"
                  onClick={() => copyToClipboard(poolUrl, setupPoolUrlKey)}
                  title="Click to copy"
                >
                  {poolUrl}
                </button>
                {copiedKey === setupPoolUrlKey && <span className="inline-copy-note">Copied</span>}
              </li>
            </ul>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 10 }}>
              Quick start command (replace only <code className="mono">YOUR_BLOCKNET_ADDRESS</code>):
            </p>
            <div className="command-copy">
              <pre className="config-block">{commandExample}</pre>
              <button className="copy-btn" type="button" onClick={() => copyToClipboard(commandExample, 'cmd')}>
                {copiedKey === 'cmd' ? 'Copied' : 'Copy Command'}
              </button>
            </div>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 8 }}>
              Your settings are saved to{' '}
              <code className="mono" style={{ background: 'var(--bg)', padding: '1px 6px', borderRadius: 4, fontSize: 13 }}>
                seine-data/seine.config.json
              </code>{' '}
              so you won't be prompted again on subsequent runs.
            </p>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 8 }}>
              Already ran Seine and need to reconfigure? Edit or delete{' '}
              <code className="mono" style={{ background: 'var(--bg)', padding: '1px 6px', borderRadius: 4, fontSize: 13 }}>
                seine-data/seine.config.json
              </code>{' '}
              to update/reset saved values, or override at launch with{' '}
              <code className="mono" style={{ background: 'var(--bg)', padding: '1px 6px', borderRadius: 4, fontSize: 13 }}>
                --address
              </code>
              ,{' '}
              <code className="mono" style={{ background: 'var(--bg)', padding: '1px 6px', borderRadius: 4, fontSize: 13 }}>
                --pool-url
              </code>
              , and{' '}
              <code className="mono" style={{ background: 'var(--bg)', padding: '1px 6px', borderRadius: 4, fontSize: 13 }}>
                --pool-worker
              </code>
              .
            </p>
          </li>
          <li>
            <strong>Start mining</strong>
            <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 4 }}>
              Seine will connect to the pool and start submitting shares. You can monitor your progress in the Seine TUI
              or check <a href="/stats">My Stats</a> using your wallet address to see your hashrate and balance.
            </p>
          </li>
        </ol>
      </div>
    );

  return (
    <div className={active ? 'page active' : 'page'} id="page-start">
      <div className="page-header">
        <span className="page-kicker">Blocknet Mining Guide</span>
        <h1>How to start mining Blocknet</h1>
        <p className="page-intro">
          Download Seine, connect to the pool stratum endpoint, and monitor your Blocknet hashrate and payouts from the
          public dashboard.
        </p>
      </div>

      <div className="card section">
        <h3>Pool Information</h3>
        <table className="info-table" style={{ maxWidth: 500 }}>
          <tbody>
            <tr>
              <td>Stratum</td>
              <td>
                <button
                  type="button"
                  className="inline-copy-code mono"
                  onClick={() => copyToClipboard(poolUrl, 'pool-url-info')}
                  title="Click to copy"
                >
                  {poolUrl}
                </button>
                {copiedKey === 'pool-url-info' && <span className="inline-copy-note">Copied</span>}
              </td>
            </tr>
            <tr>
              <td>Fee</td>
              <td>
                <span className="info-value mono">{feeDisplayFor(poolInfo)}</span>
              </td>
            </tr>
            <tr>
              <td>Payout Scheme</td>
              <td>
                <span className="info-value">{(poolInfo?.payout_scheme || 'pplns').toUpperCase()}</span>
              </td>
            </tr>
            <tr>
              <td>Min Payout</td>
              <td>
                <span className="info-value mono">{poolInfo?.min_payout_amount != null ? `${poolInfo.min_payout_amount} BNT` : '-'}</span>
              </td>
            </tr>
            <tr>
              <td>Block Confirmations</td>
              <td>
                <span className="info-value mono">
                  {poolInfo?.blocks_before_payout != null ? `${poolInfo.blocks_before_payout} blocks` : '-'}
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      {setupSection}

      <div className="card section">
        <h3>Seine in Action</h3>
        <div className="screenshot-grid">
          <div>
            <img src="/ui-assets/pool-entered.png" alt="Seine pool setup screen" />
            <div className="caption">First-run setup</div>
          </div>
          <div>
            <img src="/ui-assets/mining-tui.png" alt="Seine mining TUI" />
            <div className="caption">Mining TUI</div>
          </div>
        </div>
      </div>

      <div className="seo-copy-grid">
        <div className="card seo-copy-card">
          <h3>Transparent pool data</h3>
          <p>
            Review blocks, payout batches, and pool status from public pages before you point any hashpower at the
            pool.
          </p>
        </div>
        <div className="card seo-copy-card">
          <h3>Simple Blocknet setup</h3>
          <p>
            The setup flow is built around Seine, so you only need your Blocknet wallet address and the pool URL to get
            started.
          </p>
        </div>
        <div className="card seo-copy-card">
          <h3>Operator visibility</h3>
          <p>
            Live dashboard metrics, historical luck, and uptime tracking make it easier to compare pool performance over
            time.
          </p>
        </div>
      </div>

      <div className="card section">
        <div className="section-header">
          <div>
            <h2>Mining Pool FAQ</h2>
            <p className="section-lead">
              Important setup and payout questions miners usually ask before connecting to the pool.
            </p>
          </div>
        </div>
        <div className="seo-copy-grid" style={{ marginTop: 0, marginBottom: 0 }}>
          <div className="card seo-copy-card">
            <h3>What miner should I use?</h3>
            <p>Use Seine, then point it at the pool stratum endpoint and connect your Blocknet payout address.</p>
          </div>
          <div className="card seo-copy-card">
            <h3>What pool URL should I enter?</h3>
            <p>Use {poolUrl} as the pool URL in Seine or any compatible Blocknet mining configuration.</p>
          </div>
          <div className="card seo-copy-card">
            <h3>How do payouts work?</h3>
            <p>
              {(poolInfo?.payout_scheme || 'pplns').toUpperCase()} payouts are used here, with{' '}
              {poolInfo?.min_payout_amount != null ? `${poolInfo.min_payout_amount} BNT` : 'the configured minimum'} after{' '}
              {poolInfo?.blocks_before_payout ?? 0} confirmations. Confirmed balances are picked up on the regular payout
              sweep, while tentative previews from unconfirmed blocks are shown separately and can still move until those
              blocks fully confirm.
            </p>
          </div>
        </div>
      </div>

      <div className="seo-copy-grid">
        <div className="card seo-copy-card">
          <h3>Verify Recent Payouts</h3>
          <p>
            Check <a href="/payouts">recent payout batches</a> and explorer transaction links before you direct any
            hashpower at the pool.
          </p>
        </div>
        <div className="card seo-copy-card">
          <h3>Review Live Status</h3>
          <p>
            Use the <a href="/status">status page</a> and <a href="/">dashboard</a> to confirm uptime, daemon health,
            and current pool activity.
          </p>
        </div>
        <div className="card seo-copy-card">
          <h3>Compare Round History</h3>
          <p>
            Open <a href="/luck">luck history</a> and <a href="/blocks">recent blocks</a> to see how pool variance
            translates into actual block discovery over time.
          </p>
        </div>
      </div>
    </div>
  );
}
