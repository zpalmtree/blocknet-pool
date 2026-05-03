import { useCallback, useEffect, useState } from 'react';

import { stratumUrl } from '../lib/format';
import type { InfoResponse } from '../types';

interface StartPageProps {
  poolInfo: InfoResponse | null;
}

export function StartPage({ poolInfo }: StartPageProps) {
  const [copiedKey, setCopiedKey] = useState('');
  const poolFeePct = poolInfo?.pool_fee_pct;
  const poolFeeLabel = poolFeePct != null && poolFeePct > 0 ? `${poolFeePct}%` : '0% (no fee)';
  const poolUrl = stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url);
  const commandExample = `./seine --pool-url ${poolUrl} --address YOUR_BLOCKNET_ADDRESS`;
  const pplnsWindowLabel = poolInfo?.pplns_window_duration;

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

  const setupPoolUrlKey = 'pool-url-quickstart';

  const setupSection = (
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
  );

  return (
    <div id="page-start">
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
                <span className="info-value mono">{poolFeeLabel}</span>
              </td>
            </tr>
            <tr>
              <td>Payout Scheme</td>
              <td>
                <span className="info-value">PPLNS</span>
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
        <h3>How Rewards Are Counted</h3>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>
          This pool uses <strong style={{ color: 'var(--text)' }}>PPLNS</strong>
          {pplnsWindowLabel ? (
            <>
              {' '}
              with a <strong style={{ color: 'var(--text)' }}>{pplnsWindowLabel}</strong> payout window.
            </>
          ) : null}
        </p>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>
          Rewards are based on shares submitted before a block is found. If your hashrate increases right after a block,
          that work helps with later blocks, not the one that was just found.
        </p>
        <p style={{ color: 'var(--muted)', fontSize: 14 }}>
          <a href="/stats">My Stats</a> shows tentative rewards from unconfirmed blocks separately. Those estimates can
          still move until the block confirms or is orphaned.
        </p>
      </div>

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

    </div>
  );
}
