import { t, tx, localizePath, locale, localeInfo } from '../i18n';
import { useCallback, useEffect, useState } from 'react';

import { stratumUrl } from '../lib/format';
import type { InfoResponse } from '../types';

interface StartPageProps {
  poolInfo: InfoResponse | null;
}

export function StartPage({ poolInfo }: StartPageProps) {
  const [copiedKey, setCopiedKey] = useState('');
  const poolFeePct = poolInfo?.pool_fee_pct;
  const poolFeeLabel = poolFeePct == null || !Number.isFinite(poolFeePct) ? '-'
    : poolFeePct === 0 ? t('m598')
    : `${poolFeePct.toLocaleString(locale, { maximumFractionDigits: 20 })}%`;
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
          <h3>{t("m450")}</h3>
          <p className="section-lead">{t("m451")}</p>
        </div>
      </div>

      <div className="quickstart-grid">
        <div className="quickstart-card">
          <span className="quickstart-step">1</span>
          <strong>{t("m452")}</strong>
          <p>{tx("m453", [<a href="https://seineminer.com/#download" target="_blank" rel="noopener">
              seineminer.com
            </a>])}</p>
        </div>

        <div className="quickstart-card">
          <span className="quickstart-step">2</span>
          <strong>{t("m454")}</strong>
          <p>{tx("m455", [<code className="mono inline-code">./seine</code>, <code className="mono inline-code">@name</code>, <code className="mono inline-code">$name</code>])}</p>
          <div className="quickstart-inline">
            <span className="quickstart-inline-label">{t("m456")}</span>
            <button
              type="button"
              className="inline-copy-code mono"
              onClick={() => copyToClipboard(poolUrl, setupPoolUrlKey)}
              title={t("m457")}
            >
              {poolUrl}
            </button>
            {copiedKey === setupPoolUrlKey && <span className="inline-copy-note">{t("m386")}</span>}
          </div>
        </div>

        <div className="quickstart-card">
          <span className="quickstart-step">3</span>
          <strong>{t("m458")}</strong>
          <p>{tx("m459", [<a href={localizePath("/stats")}>{t("m005")}</a>])}</p>
        </div>
      </div>

      <p className="quickstart-label">{tx("m460", [<code className="mono inline-code">YOUR_BLOCKNET_ADDRESS</code>, <code className="mono inline-code">'@yourname'</code>, <code className="mono inline-code">'$yourname'</code>])}</p>
      <div className="command-copy">
        <pre className="config-block">{commandExample}</pre>
        <button className="copy-btn" type="button" onClick={() => copyToClipboard(commandExample, 'cmd')}>
          {copiedKey === 'cmd' ? t("m386") : t("m461")}
        </button>
      </div>

      <div className="quickstart-notes">
        <p><a href={`https://seineminer.com/${localeInfo.path ? `${localeInfo.path}/` : ""}blog/mine-blocknet-on-windows/`}>{t("windowsGuide")}</a></p>
        <p><a href={`${localizePath("/blog/")}track-your-miner-on-bntpool/#zero-hashrate`}>{t("minerHelp")}</a></p>
        <p>{tx("m463", [<a href="https://blocknet.id" target="_blank" rel="noopener">{t("m462")}</a>, <a href={localizePath("/stats")}>{t("m005")}</a>])}</p>
        <p>{tx("m464", [<code className="mono inline-code">seine-data/seine.config.json</code>])}</p>
        <p>{tx("m465", [<code className="mono inline-code">--address</code>, <code className="mono inline-code">--pool-url</code>, <code className="mono inline-code">--pool-worker</code>])}</p>
      </div>
    </div>
  );

  return (
    <div id="page-start">
      <div className="page-header">
        <span className="page-kicker">{t("m466")}</span>
        <h1>{t("m467")}</h1>
        <p className="page-intro">{t("m468")}</p>
      </div>

      <div className="card section">
        <h3>{t("m469")}</h3>
        <table className="info-table" style={{ maxWidth: 500 }}>
          <tbody>
            <tr>
              <td>{t("m402")}</td>
              <td>
                <button
                  type="button"
                  className="inline-copy-code mono"
                  onClick={() => copyToClipboard(poolUrl, 'pool-url-info')}
                  title={t("m457")}
                >
                  {poolUrl}
                </button>
                {copiedKey === 'pool-url-info' && <span className="inline-copy-note">{t("m386")}</span>}
              </td>
            </tr>
            <tr>
              <td>{t("poolFee")}</td>
              <td>
                <span className="info-value mono">{poolFeeLabel}</span>
              </td>
            </tr>
            <tr>
              <td>{t("m471")}</td>
              <td>
                <span className="info-value">PPLNS</span>
              </td>
            </tr>
            <tr>
              <td>{t("m472")}</td>
              <td>
                <span className="info-value mono">{poolInfo?.min_payout_amount != null ? t("m473", [poolInfo.min_payout_amount.toLocaleString(locale, { maximumFractionDigits: 20 })]) : '-'}</span>
              </td>
            </tr>
            <tr>
              <td>{t("m474")}</td>
              <td>
                <span className="info-value mono">
                  {poolInfo?.blocks_before_payout != null ? t("m475", [poolInfo.blocks_before_payout.toLocaleString(locale)]) : '-'}
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      {setupSection}

      <div className="card section">
        <h3>{t("m476")}</h3>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>{tx("m478", [<strong style={{ color: 'var(--text)' }}>PPLNS</strong>, pplnsWindowLabel ? (<>{tx("m477", [<strong style={{ color: 'var(--text)' }}>{pplnsWindowLabel}</strong>])}</>) : null])}</p>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>{t("m479")}</p>
        <p style={{ color: 'var(--muted)', fontSize: 14 }}>{tx("m480", [<a href={localizePath("/stats")}>{t("m005")}</a>])}</p>
      </div>

      <div className="card section">
        <h3>{t("m481")}</h3>
        <div className="screenshot-grid">
          <div>
            <img src="/ui-assets/pool-entered.png" alt={t("m482")} />
            <div className="caption">{t("m483")}</div>
          </div>
          <div>
            <img src="/ui-assets/mining-tui.png" alt={t("m484")} />
            <div className="caption">{t("m485")}</div>
          </div>
        </div>
      </div>

    </div>
  );
}
