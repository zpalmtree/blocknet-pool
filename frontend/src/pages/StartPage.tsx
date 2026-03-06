import { useCallback, useEffect, useState } from 'react';

import { stratumUrl } from '../lib/format';
import type { InfoResponse } from '../types';

interface StartPageProps {
  active: boolean;
  poolInfo: InfoResponse | null;
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

export function StartPage({ active, poolInfo }: StartPageProps) {
  const [copiedKey, setCopiedKey] = useState('');
  const poolUrl = stratumUrl(poolInfo?.stratum_port);
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

  return (
    <div className={active ? 'page active' : 'page'} id="page-start">
      <h2>Get Started Mining</h2>

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
                  onClick={() => copyToClipboard(poolUrl, 'pool-url')}
                  title="Click to copy"
                >
                  {poolUrl}
                </button>
                {copiedKey === 'pool-url' && <span className="inline-copy-note">Copied</span>}
              </td>
            </tr>
            <tr>
              <td>Fee</td>
              <td>{feeDisplayFor(poolInfo)}</td>
            </tr>
            <tr>
              <td>Payout Scheme</td>
              <td>{(poolInfo?.payout_scheme || 'pplns').toUpperCase()}</td>
            </tr>
            <tr>
              <td>Min Payout</td>
              <td>{poolInfo?.min_payout_amount != null ? `${poolInfo.min_payout_amount} BNT` : '-'}</td>
            </tr>
            <tr>
              <td>Block Confirmations</td>
              <td>{poolInfo?.blocks_before_payout != null ? `${poolInfo.blocks_before_payout} blocks` : '-'}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="card section">
        <div className="section-header">
          <div>
            <h3>Quick Start</h3>
            <p className="section-lead">
              Download Seine, point it at the pool, and track payouts with your wallet address.
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
                onClick={() => copyToClipboard(poolUrl, 'pool-url')}
                title="Click to copy"
              >
                {poolUrl}
              </button>
              {copiedKey === 'pool-url' && <span className="inline-copy-note">Copied</span>}
            </div>
          </div>

          <div className="quickstart-card">
            <span className="quickstart-step">3</span>
            <strong>Start mining</strong>
            <p>
              Seine saves your config automatically. Watch the TUI locally or open <a href="#/stats">My Stats</a> to
              follow hashrate and balance.
            </p>
          </div>
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
