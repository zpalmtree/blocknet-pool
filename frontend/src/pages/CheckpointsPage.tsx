import { locale, t, tx } from '../i18n';
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
  const heightLabel = checkpoints?.latest_height != null ? checkpoints.latest_height.toLocaleString(locale) : '-';
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
        <span className="page-kicker">{t("m379")}</span>
        <h1>{t("m380")}</h1>
        <p className="page-intro">{t("m381")}</p>
      </div>

      <div className="card checkpoint-hero">
        <div className="checkpoint-facts">
          <div className="checkpoint-fact">
            <span className="checkpoint-fact-label">{t("m382")}</span>
            <span className="checkpoint-fact-value mono">{heightLabel}</span>
          </div>
          <div className="checkpoint-fact">
            <span className="checkpoint-fact-label">{t("m383")}</span>
            <span className="checkpoint-fact-value mono" title={timestampTitle(checkpoints?.updated_at)}>
              {updatedLabel}
            </span>
          </div>
        </div>
        {!available && <p className="checkpoint-unavailable">{t("m384")}</p>}
        <div className="checkpoint-actions">
          <a className="btn btn-primary" href="/checkpoints.dat" download>{t("m385")}</a>
          <div className="copy-field">
            <a className="copy-field-value mono" href="/checkpoints.dat" title={checkpointUrl}>
              {checkpointUrl}
            </a>
            <button
              className={`copy-field-btn${copiedKey === 'url' ? ' is-copied' : ''}`}
              type="button"
              onClick={() => copyToClipboard(checkpointUrl, 'url')}
            >
              {copiedKey === 'url' ? t("m386") : t("m387")}
            </button>
          </div>
        </div>
      </div>

      <div className="card section checkpoint-usage">
        <div className="section-header">
          <div>
            <h3>{t("m388")}</h3>
            <p className="section-lead">{tx("m389", [<span className="mono">checkpoints.dat</span>])}</p>
          </div>
        </div>
        <div className="checkpoint-stack">
          <div className="checkpoint-use-step">
            <h4>{t("m390")}</h4>
            <p>{tx("m391", [<span className="mono">checkpoints.dat</span>, <span className="mono">--data</span>])}</p>
            <div className="checkpoint-command-row">
              <pre className="config-block">{curlCommand}</pre>
              <button className="copy-btn" type="button" onClick={() => copyToClipboard(curlCommand, 'curl')}>
                {copiedKey === 'curl' ? t("m386") : t("m392")}
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="card section checkpoint-integrity">
        <div className="section-header">
          <div>
            <h3>{t("m393")}</h3>
            <p className="section-lead">{t("m394")}</p>
          </div>
        </div>
        <div className="checkpoint-stack">
          <div className="checkpoint-use-step">
            <h4>{t("m395")}</h4>
            <p>{t("m396")}</p>
            <div className="copy-field copy-field--block">
              <span className="copy-field-value mono">{sha256 || '-'}</span>
              {sha256 && (<button
                  className={`copy-field-btn${copiedKey === 'sha' ? ' is-copied' : ''}`}
                  type="button"
                  onClick={() => copyToClipboard(sha256, 'sha')}
                >
                  {copiedKey === 'sha' ? t("m386") : t("m392")}
                </button>)}
            </div>
          </div>
          <div className="checkpoint-use-step">
            <h4>{t("m397")}</h4>
            <p>{t("m398")}</p>
            <div className="checkpoint-command-row">
              <pre className="config-block">{verifyCommand}</pre>
              <button className="copy-btn" type="button" onClick={() => copyToClipboard(verifyCommand, 'verify')}>
                {copiedKey === 'verify' ? t("m386") : t("m392")}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
