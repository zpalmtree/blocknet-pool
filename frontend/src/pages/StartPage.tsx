import { t, tx, localizePath, locale, localeInfo } from '../i18n';
import { useCallback, useEffect, useState } from 'react';

import { stratumUrl } from '../lib/format';
import type { InfoResponse } from '../types';
import { ExchangeSetup } from '../components/ExchangeSetup';
import { MiningType } from '../components/MiningType';
import { SeineScreen, type TerminalCapture } from '../components/SeineScreen';
import seineScreensData from '../data/seine-screens.json';
import { seineSupportsSolo } from '../lib/solo';

// seine v0.3.1's own renderer with example data; regenerate with frontend/scripts/seine-screens-export.rs.
const seineScreens = seineScreensData as unknown as Record<string, TerminalCapture>;

const minerSettings = [
  ['minerSettingsNvidia', '--backend nvidia'],
  ['minerSettingsAmd', '--backend amd'],
  ['minerSettingsCpu', '--backend cpu'],
  ['minerSettingsThreads', '--threads 4'],
] as const;

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
  const windowsCommand = `.\\seine.exe --pool-url ${poolUrl} --address YOUR_BLOCKNET_ADDRESS`;
  const soloCommand = `./seine --pool-url ${poolUrl} --address solo:YOUR_BLOCKNET_ADDRESS`;
  const soloWindowsCommand = `.\\seine.exe --pool-url ${poolUrl} --address solo:YOUR_BLOCKNET_ADDRESS`;
  const pplnsWindowLabel = poolInfo?.pplns_window_duration;
  // Show the solo guide once a Seine release that accepts solo: is out.
  const soloGuide = seineSupportsSolo(poolInfo?.latest_miner_version);

  useEffect(() => {
    if (!copiedKey) return;
    const timer = window.setTimeout(() => setCopiedKey(''), 1200);
    return () => window.clearTimeout(timer);
  }, [copiedKey]);

  const copyToClipboard = useCallback((value: string, key: string) => {
    if (!navigator.clipboard?.writeText) return;
    void navigator.clipboard.writeText(value).then(() => setCopiedKey(key)).catch(() => {});
  }, []);

  const setupPoolUrlKey = 'pool-url-quickstart';

  const seineGuideBase = `https://seineminer.com/${localeInfo.path ? `${localeInfo.path}/` : ""}blog/`;

  return (
    <div id="page-start">
      <div className="page-header">
        <span className="page-kicker">{t("m466")}</span>
        <h1>{t("m467")}</h1>
        <p className="page-intro">{t("m468")}</p>
      </div>

      <div className="card section">
        <div className="section-header">
          <div>
            <h3>{t("m450")}</h3>
            <p className="section-lead">{t("m451")}</p>
          </div>
        </div>

        <ol className="quickstart-grid">
          <li className="quickstart-card">
            <span className="quickstart-step">1</span>
            <div className="quickstart-text">
              <strong>{t("walletStep")}</strong>
              <p>{tx("walletStepBody", [<a href="#wallet">{t("walletStepLink")}</a>])}</p>
            </div>
          </li>

          <li className="quickstart-card">
            <span className="quickstart-step">2</span>
            <div className="quickstart-text">
              <strong>{t("m452")}</strong>
              <p>{tx("m453", [<a href={`https://seineminer.com/${localeInfo.path ? `${localeInfo.path}/` : ''}#download`} target="_blank" rel="noopener">
                  seineminer.com
                </a>])}</p>
            </div>
            <div className="quickstart-action">
              <span className="quickstart-inline-label">{t('setupGuide')}</span>
              <span className="quickstart-guides">
                {['windows', 'mac', 'linux'].map((platform, index) => <a key={platform} href={`${seineGuideBase}mine-blocknet-on-${platform}/`}>{['Windows', 'macOS', 'Linux'][index]}</a>)}
              </span>
            </div>
          </li>

          <li className="quickstart-card">
            <span className="quickstart-step">3</span>
            <div className="quickstart-text">
              <strong>{t("m454")}</strong>
              <p>{tx("launchBody", [<code className="mono inline-code">seine.exe</code>, <code className="mono inline-code">./seine</code>, <code className="mono inline-code">@name</code>, <code className="mono inline-code">$name</code>])}</p>
            </div>
            <div className="quickstart-action">
              <span className="quickstart-inline-label">{t("m456")}</span>
              <div className="quickstart-copy">
                <code className="mono">{poolUrl}</code>
                <button type="button" onClick={() => copyToClipboard(poolUrl, setupPoolUrlKey)} aria-label={t("m387")}>
                  <span role="status">{copiedKey === setupPoolUrlKey ? t("m386") : t("m392")}</span>
                </button>
              </div>
            </div>
          </li>

          <li className="quickstart-card">
            <span className="quickstart-step">4</span>
            <div className="quickstart-text">
              <strong>{t("trackWorker")}</strong>
              <p>{t('trackBody')}</p>
            </div>
            <div className="quickstart-action">
              <a className="btn btn-secondary" href={localizePath('/stats')}>{t('m005')}</a>
            </div>
          </li>
        </ol>

        <details className="command-option">
        <summary>{t('commandOption')}</summary>
        <p className="quickstart-label">{tx("m460", [<code className="mono inline-code">YOUR_BLOCKNET_ADDRESS</code>, <code className="mono inline-code">'@yourname'</code>, <code className="mono inline-code">'$yourname'</code>])}</p>
        <p className="quickstart-label">Windows · PowerShell</p>
        <div className="command-copy">
          <pre className="config-block" dir="ltr">{windowsCommand}</pre>
          <button className="copy-btn" type="button" onClick={() => copyToClipboard(windowsCommand, 'cmd-windows')}>
            {copiedKey === 'cmd-windows' ? t('m386') : t('m461')}
          </button>
        </div>
        <p className="quickstart-label">macOS / Linux</p>
        <div className="command-copy">
          <pre className="config-block" dir="ltr">{commandExample}</pre>
          <button className="copy-btn" type="button" onClick={() => copyToClipboard(commandExample, 'cmd')}>
            {copiedKey === 'cmd' ? t("m386") : t("m461")}
          </button>
        </div>
        </details>
        <details className="command-option" id="miner-settings">
          <summary>{t('minerSettingsTitle')}</summary>
          <p className="quickstart-label">{tx('minerSettingsLead', [<code className="mono inline-code">.\seine.exe</code>, <code className="mono inline-code">./seine</code>])}</p>
          <table className="info-table miner-settings-table">
            <tbody>
              {minerSettings.map(([key, flags]) => (
                <tr key={key}><td>{t(key)}</td><td><code className="mono inline-code" dir="ltr">./seine {flags}</code></td></tr>
              ))}
            </tbody>
          </table>
          <p className="quickstart-label"><a href={`${seineGuideBase}configure-seine-miner/`}>{t('minerSettingsGuide')}</a></p>
        </details>
        {soloGuide && <details className="command-option" id="solo-mining">
          <summary>{t('soloTitle')}</summary>
          <p className="quickstart-label">{t('soloLead', [poolFeeLabel])}</p>
          <p className="quickstart-label">{tx('soloHow', [<code className="mono inline-code">solo:</code>])}</p>
          <p className="quickstart-label">Windows · PowerShell</p>
          <div className="command-copy">
            <pre className="config-block" dir="ltr">{soloWindowsCommand}</pre>
            <button className="copy-btn" type="button" onClick={() => copyToClipboard(soloWindowsCommand, 'solo-windows')}>
              {copiedKey === 'solo-windows' ? t('m386') : t('m461')}
            </button>
          </div>
          <p className="quickstart-label">macOS / Linux</p>
          <div className="command-copy">
            <pre className="config-block" dir="ltr">{soloCommand}</pre>
            <button className="copy-btn" type="button" onClick={() => copyToClipboard(soloCommand, 'solo')}>
              {copiedKey === 'solo' ? t('m386') : t('m461')}
            </button>
          </div>
          <p className="quickstart-label">{t('soloFit')}</p>
          <p className="quickstart-label">{tx('soloTrack', [<a href={localizePath('/stats')}>{t('m005')}</a>, <code className="mono inline-code">solo:</code>])}</p>
        </details>}
        <ExchangeSetup info={poolInfo} poolUrl={poolUrl} />
      </div>

      <div className="card section" id="wallet">
        <h3>{t("walletTitle")}</h3>
        <p className="section-lead">{t("walletLead")}</p>
        <div className="wallet-grid">
          <div className="wallet-option">
            <strong>{t("walletDesktop")}</strong>
            <p>{tx("walletDesktopBody", [<strong>Create New Wallet</strong>, <strong>Receive</strong>])}</p>
            <a className="btn btn-primary" href="https://github.com/blocknetprivacy/blocknet-gui/releases/latest" target="_blank" rel="noopener">
              {t("walletDesktopDownload")}
            </a>
          </div>
          <div className="wallet-option">
            <strong>{t("walletCli")}</strong>
            <p>{tx("walletCliBody", [<code className="mono inline-code">blocknet setup</code>, <code className="mono inline-code">blocknet attach mainnet</code>, <code className="mono inline-code">load</code>, <strong>Create a new wallet</strong>, <code className="mono inline-code">address</code>])}</p>
            <a className="btn btn-secondary" href="https://github.com/blocknetprivacy/blocknet/releases/latest" target="_blank" rel="noopener">
              {t("walletCliDownload")}
            </a>
          </div>
        </div>
        <div className="quickstart-notes">
          <p>{t("walletSeed")}</p>
          <p>{tx("m463", [<a href="https://blocknet.id" target="_blank" rel="noopener">{t("m462")}</a>, <a href={localizePath("/stats")}>{t("m005")}</a>])}</p>
          <p><a href={`${seineGuideBase}blocknet-wallet-for-mining/`}>{t("walletGuide")}</a></p>
        </div>
      </div>

      <div className="card section">
        <h3>{t("helpTitle")}</h3>
        <ul className="help-links" aria-label={t('browseTopics')}>
          <li><a href={`${localizePath('/blog/')}#getting-started`}>{t('topicGettingStarted')}</a></li>
          <li><a href={`${seineGuideBase}mine-blocknet-on-windows/`}>{t("windowsGuide")}</a></li>
          <li><a href={`${localizePath("/blog/")}track-your-miner-on-bntpool/#zero-hashrate`}>{t("minerHelp")}</a></li>
          <li><a href={`${localizePath('/blog/')}#troubleshooting`}>{t('topicTroubleshooting')}</a></li>
        </ul>
        <div className="quickstart-notes">
          <p>{tx("m464", [<code className="mono inline-code">seine-data/seine.config.json</code>])}</p>
          <p>{tx("m465", [<code className="mono inline-code">--address</code>, <code className="mono inline-code">--pool-url</code>, <code className="mono inline-code">--pool-worker</code>])}</p>
        </div>
      </div>

      <div className="card section" id="pool-information">
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


      <div className="card section">
        <h3>{t("m476")}</h3>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>{tx("m478", [<strong style={{ color: 'var(--text)' }}>PPLNS</strong>, pplnsWindowLabel ? (<>{tx("m477", [<strong style={{ color: 'var(--text)' }}>{pplnsWindowLabel}</strong>])}</>) : null])}</p>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>{t("m479")}</p>
        <p style={{ color: 'var(--muted)', fontSize: 14, marginBottom: 10 }}>{tx("m480", [<a href={localizePath("/stats")}>{t("m005")}</a>])}</p>
        <p style={{ color: 'var(--muted)', fontSize: 14 }}>{tx("soloRewardsNote", [<MiningType solo />])}</p>
      </div>

      <div className="card section">
        <h3>{t("m481")}</h3>
        <div className="seine-screens">
          <div className="seine-screens-prompts">
            <SeineScreen desktop={seineScreens['address-desktop']} mobile={seineScreens['address-mobile']} caption={t('seineScreenAddress')} />
            <SeineScreen desktop={seineScreens['pool-desktop']} mobile={seineScreens['pool-mobile']} caption={t('seineScreenPool')} />
          </div>
          <SeineScreen desktop={seineScreens['mining-desktop']} mobile={seineScreens['mining-mobile']} caption={t('seineScreenMining')} />
        </div>
      </div>

    </div>
  );
}
