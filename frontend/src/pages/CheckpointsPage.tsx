import { useCallback, useEffect, useState } from 'react';

import type { ApiClient } from '../api/client';
import { timeAgo, timestampTitle } from '../lib/format';
import type { CheckpointsResponse } from '../types';

interface CheckpointsPageProps {
  api: ApiClient;
  liveTick: number;
}

export function CheckpointsPage({ api, liveTick }: CheckpointsPageProps) {
  const [checkpoints, setCheckpoints] = useState<CheckpointsResponse | null>(null);
  const [copiedKey, setCopiedKey] = useState('');

  const loadCheckpoints = useCallback(async () => {
    try {
      const d = await api.getCheckpoints();
      setCheckpoints(d);
    } catch {
      setCheckpoints(null);
    }
  }, [api]);

  useEffect(() => {
    void loadCheckpoints();
  }, [loadCheckpoints]);

  useEffect(() => {
    if (liveTick <= 0 || liveTick % 12 !== 0) return;
    void loadCheckpoints();
  }, [liveTick, loadCheckpoints]);

  useEffect(() => {
    if (!copiedKey) return;
    const timer = window.setTimeout(() => setCopiedKey(''), 1200);
    return () => window.clearTimeout(timer);
  }, [copiedKey]);

  const available = Boolean(checkpoints?.available);
  const checkpointUrl = checkpoints?.url || `${window.location.origin}/checkpoints.dat`;
  const updatedLabel = checkpoints?.updated_at ? timeAgo(checkpoints.updated_at) : '-';
  const heightLabel = checkpoints?.latest_height != null ? checkpoints.latest_height.toLocaleString() : '-';
  const sha256 = checkpoints?.sha256 || '';
  const curlCommand = `curl -L ${checkpointUrl} -o blocknet-data-mainnet/checkpoints.dat`;
  const verifyCommand = 'sha256sum blocknet-data-mainnet/checkpoints.dat';

  const copyToClipboard = useCallback((value: string, key: string) => {
    if (!navigator.clipboard?.writeText) return;
    void navigator.clipboard.writeText(value);
    setCopiedKey(key);
  }, []);

  return (
    <div id="page-checkpoints">
      <div className="page-header">
        <span className="page-kicker">Chain Bootstrap</span>
        <h1>Blocknet checkpoints</h1>
        <p className="page-intro">
          Pool daemon checkpoints for fast initial sync and trusted chain pinning.
        </p>
      </div>

      <div className="card checkpoint-hero">
        <div className="checkpoint-facts">
          <div className="checkpoint-fact">
            <span className="checkpoint-fact-label">Latest height</span>
            <span className="checkpoint-fact-value mono">{heightLabel}</span>
          </div>
          <div className="checkpoint-fact">
            <span className="checkpoint-fact-label">Updated</span>
            <span className="checkpoint-fact-value mono" title={timestampTitle(checkpoints?.updated_at)}>
              {updatedLabel}
            </span>
          </div>
        </div>
        {!available && <p className="checkpoint-unavailable">Checkpoint file is currently unavailable.</p>}
        <div className="checkpoint-actions">
          <a className="btn btn-primary" href="/checkpoints.dat" download>
            Download checkpoints.dat
          </a>
          <div className="copy-field">
            <a className="copy-field-value mono" href="/checkpoints.dat" title={checkpointUrl}>
              {checkpointUrl}
            </a>
            <button
              className={`copy-field-btn${copiedKey === 'url' ? ' is-copied' : ''}`}
              type="button"
              onClick={() => copyToClipboard(checkpointUrl, 'url')}
            >
              {copiedKey === 'url' ? 'Copied' : 'Copy URL'}
            </button>
          </div>
        </div>
      </div>

      <div className="card section checkpoint-usage">
        <div className="section-header">
          <div>
            <h3>Using checkpoints</h3>
            <p className="section-lead">
              Blocknet loads <span className="mono">checkpoints.dat</span> from the daemon data directory. The hashes
              pin known block heights while a node catches up, so initial sync can skip expensive verification up to the
              latest matching checkpoint.
            </p>
          </div>
        </div>
        <div className="checkpoint-stack">
          <div className="checkpoint-use-step">
            <h4>Download manually</h4>
            <p>Save the file as <span className="mono">checkpoints.dat</span> inside the data directory used by <span className="mono">--data</span>.</p>
            <div className="checkpoint-command-row">
              <pre className="config-block">{curlCommand}</pre>
              <button className="copy-btn" type="button" onClick={() => copyToClipboard(curlCommand, 'curl')}>
                {copiedKey === 'curl' ? 'Copied' : 'Copy'}
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="card section checkpoint-integrity">
        <div className="section-header">
          <div>
            <h3>Verify integrity</h3>
            <p className="section-lead">
              Confirm your download matches the checksum published by the pool before trusting it.
            </p>
          </div>
        </div>
        <div className="checkpoint-stack">
          <div className="checkpoint-use-step">
            <h4>Published SHA-256</h4>
            <p>The expected checksum for the current file.</p>
            <div className="copy-field copy-field--block">
              <span className="copy-field-value mono">{sha256 || '-'}</span>
              {sha256 && (
                <button
                  className={`copy-field-btn${copiedKey === 'sha' ? ' is-copied' : ''}`}
                  type="button"
                  onClick={() => copyToClipboard(sha256, 'sha')}
                >
                  {copiedKey === 'sha' ? 'Copied' : 'Copy'}
                </button>
              )}
            </div>
          </div>
          <div className="checkpoint-use-step">
            <h4>Check your copy</h4>
            <p>Run this in the data directory, then compare the output against the hash above.</p>
            <div className="checkpoint-command-row">
              <pre className="config-block">{verifyCommand}</pre>
              <button className="copy-btn" type="button" onClick={() => copyToClipboard(verifyCommand, 'verify')}>
                {copiedKey === 'verify' ? 'Copied' : 'Copy'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
