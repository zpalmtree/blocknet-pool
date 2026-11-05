import { t, tx, localizePath, locale, localeInfo } from '../i18n';
import { useCallback, useEffect, useState, type ReactNode } from 'react';

import { stratumUrl } from '../lib/format';
import type { InfoResponse } from '../types';
import { ExchangeSetup } from '../components/ExchangeSetup';
import { SeineScreen, type TerminalCapture } from '../components/SeineScreen';
import seineScreensData from '../data/seine-screens.json';
import { seineSupportsSolo } from '../lib/solo';
import { shellQuote } from '../lib/payout-destination';

// seine v0.3.1's own renderer with example data; regenerate with frontend/scripts/seine-screens-export.rs.
const seineScreens = seineScreensData as unknown as Record<string, TerminalCapture>;

const minerSettings = [
  ['minerSettingsNvidia', '--backend nvidia'],
  ['minerSettingsAmd', '--backend amd'],
  ['minerSettingsCpu', '--backend cpu'],
  ['minerSettingsThreads', '--threads 4'],
] as const;

type Platform = 'windows' | 'mac' | 'linux';
const PLATFORMS: readonly [Platform, string][] = [['windows', 'Windows'], ['mac', 'macOS'], ['linux', 'Linux']];
const RELEASE_DOWNLOAD = 'https://github.com/zpalmtree/seine/releases/latest/download/';
const DOWNLOADS: Record<Platform, string> = {
  windows: `${RELEASE_DOWNLOAD}seine-windows-x86_64.zip`,
  mac: `${RELEASE_DOWNLOAD}seine-macos-aarch64.tar.gz`,
  linux: `${RELEASE_DOWNLOAD}seine-linux-x86_64.tar.gz`,
};
const RUN_KEYS: Record<Platform, string> = { windows: 'runWindows', mac: 'runMac', linux: 'runLinux' };

function detectPlatform(): Platform {
  const agent = typeof navigator === 'undefined' ? '' : navigator.userAgent;
  if (/Mac/i.test(agent) && !/iPhone|iPad/i.test(agent)) return 'mac';
  if (/Linux|X11|CrOS/i.test(agent) && !/Android/i.test(agent)) return 'linux';
  return 'windows';
}

/** Addresses and plain @names pass to the shell as they are; anything else is quoted. */
function shellArg(value: string, powershell: boolean) {
  return /^[A-Za-z0-9@._:-]+$/.test(value) ? value : shellQuote(value, powershell);
}

function Choice<T extends string>({ label, value, options, onChange }: {
  label: string; value: T; options: readonly (readonly [T, string])[]; onChange: (value: T) => void;
}) {
  return (
    <div className="start-choice" role="group" aria-label={label}>
      {options.map(([key, text]) => (
        <button key={key} type="button" aria-pressed={value === key} className={value === key ? 'active' : ''} onClick={() => onChange(key)}>{text}</button>
      ))}
    </div>
  );
}

function Step({ number, id, title, children }: { number: number; id?: string; title: string; children: ReactNode }) {
  return (
    <li id={id}>
      <span className="start-step-number" aria-hidden="true">{number}</span>
      <div className="start-step-body">
        <h2>{title}</h2>
        {children}
      </div>
    </li>
  );
}

interface StartPageProps {
  poolInfo: InfoResponse | null;
}

export function StartPage({ poolInfo }: StartPageProps) {
  const [copiedKey, setCopiedKey] = useState('');
  const [platform, setPlatform] = useState<Platform>(detectPlatform);
  const [address, setAddress] = useState('');
  const [solo, setSolo] = useState(false);
  const [commandOpen, setCommandOpen] = useState(() => typeof location !== 'undefined' && location.hash === '#solo-mining');
  const poolFeePct = poolInfo?.pool_fee_pct;
  const feeKnown = poolFeePct != null && Number.isFinite(poolFeePct);
  const poolFeeLabel = !feeKnown ? '-'
    : poolFeePct === 0 ? t('m598')
    : `${poolFeePct.toLocaleString(locale, { maximumFractionDigits: 20 })}%`;
  const poolFeeShort = feeKnown ? `${poolFeePct.toLocaleString(locale, { maximumFractionDigits: 20 })}%` : '-';
  const poolUrl = stratumUrl(poolInfo?.stratum_port, poolInfo?.pool_url);
  // Solo needs a Seine release that accepts solo: logins.
  const soloAvailable = seineSupportsSolo(poolInfo?.latest_miner_version);
  const mineSolo = solo && soloAvailable;
  const powershell = platform === 'windows';
  const destination = `${mineSolo ? 'solo:' : ''}${address.trim() || 'YOUR_BLOCKNET_ADDRESS'}`;
  const command = `${powershell ? '.\\seine.exe' : './seine'} --pool-url ${poolUrl} --address ${shellArg(destination, powershell)}`;
  const platformName = PLATFORMS.find(([key]) => key === platform)![1];

  useEffect(() => {
    if (!copiedKey) return;
    const timer = window.setTimeout(() => setCopiedKey(''), 1200);
    return () => window.clearTimeout(timer);
  }, [copiedKey]);

  useEffect(() => {
    const openFromHash = () => { if (location.hash === '#solo-mining') setCommandOpen(true); };
    window.addEventListener('hashchange', openFromHash);
    return () => window.removeEventListener('hashchange', openFromHash);
  }, []);

  const copyToClipboard = useCallback((value: string, key: string) => {
    if (!navigator.clipboard?.writeText) return;
    void navigator.clipboard.writeText(value).then(() => setCopiedKey(key)).catch(() => {});
  }, []);

  const seineSite = `https://seineminer.com/${localeInfo.path ? `${localeInfo.path}/` : ''}`;
  const seineGuideBase = `${seineSite}blog/`;
  const platformChoice = <Choice<Platform> label={t('systemLabel')} value={platform} options={PLATFORMS} onChange={setPlatform} />;

  return (
    <div id="page-start">
      <div className="page-header">
        <h1>{t("m467")}</h1>
        <p className="page-intro">{feeKnown ? t('startIntro', [poolFeeShort]) : t('startIntroPlain')}</p>
      </div>

      <div className="card section">
        <ol className="start-steps">
          <Step number={1} id="wallet" title={t('walletStep')}>
            <p>{t('walletStepShort')}</p>
            <div className="start-actions">
              <a className="btn btn-secondary" href="https://github.com/blocknetprivacy/blocknet-gui/releases/latest" target="_blank" rel="noopener">{t('walletDesktopDownload')}</a>
              <a href="https://github.com/blocknetprivacy/blocknet/releases/latest" target="_blank" rel="noopener">{t('walletCli')}</a>
              <a href={`${seineGuideBase}blocknet-wallet-for-mining/`}>{t('walletGuide')}</a>
            </div>
            <p className="start-note">{t('walletSeed')}</p>
          </Step>

          <Step number={2} title={t('m452')}>
            {platformChoice}
            <div className="start-actions">
              <a className="btn btn-primary" href={DOWNLOADS[platform]}>{t('downloadFor', [platformName])}</a>
              <a href={`${seineSite}#download`} target="_blank" rel="noopener">{t('otherDownloads')}</a>
              <a href={`${seineGuideBase}mine-blocknet-on-${platform}/`}>{t('setupGuide')}</a>
            </div>
          </Step>

          <Step number={3} title={t('runStepTitle')}>
            <p>{tx(RUN_KEYS[platform], [<code className="mono inline-code">{platform === 'windows' ? 'seine.exe' : './seine'}</code>])}{' '}
              {tx('runPaste', [<code className="mono inline-code">@name</code>])}</p>
            <SeineScreen desktop={seineScreens['address-desktop']} mobile={seineScreens['address-mobile']} />
          </Step>

          <Step number={4} title={t('poolStepTitle')}>
            <p>{t('poolStepBody')}</p>
            <SeineScreen desktop={seineScreens['pool-desktop']} mobile={seineScreens['pool-mobile']} />
          </Step>

          <Step number={5} title={t('miningStepTitle')}>
            <p>{tx('miningStepBody', [<a href={localizePath('/stats')}>{t('m005')}</a>])}</p>
            <SeineScreen desktop={seineScreens['mining-desktop']} mobile={seineScreens['mining-mobile']} />
            <div className="start-actions">
              <a className="btn btn-secondary" href={localizePath('/stats')}>{t('m005')}</a>
            </div>
          </Step>
        </ol>
      </div>

      <div className="card section">
        <h2 className="start-card-title">{t('moreOptions')}</h2>
        <div className="start-options">
          <details id="solo-mining" open={commandOpen} onToggle={(event) => setCommandOpen(event.currentTarget.open)}>
            <summary>{t(soloAvailable ? 'commandOrSolo' : 'commandOption')}</summary>
            <div className="start-builder">
              <p>{t('builderLead')}</p>
              <label className="start-field">
                <span>{t('builderAddress')}</span>
                <input type="text" id="start-address" value={address} dir="ltr" autoComplete="off" spellCheck={false}
                  placeholder="YOUR_BLOCKNET_ADDRESS" onChange={(event) => setAddress(event.target.value)} />
              </label>
              <div className="start-builder-choices">
                {soloAvailable && <Choice label={t('modeLabel')} value={mineSolo ? 'solo' : 'pool'}
                  options={[['pool', t('modePool')], ['solo', t('modeSolo')]] as const} onChange={(mode) => setSolo(mode === 'solo')} />}
                {platformChoice}
              </div>
              <div className="start-command">
                <pre dir="ltr">{command}</pre>
                <button type="button" onClick={() => copyToClipboard(command, 'command')}>
                  <span role="status">{copiedKey === 'command' ? t('m386') : t('m461')}</span>
                </button>
              </div>
              {mineSolo ? <>
                <p className="start-note">{t('soloLead', [poolFeeLabel])} {t('soloFit')}</p>
                <p className="start-note">{tx('soloTrack', [<a href={localizePath('/stats')}>{t('m005')}</a>, <code className="mono inline-code">solo:</code>])}</p>
              </> : <p className="start-note">{t('poolModeNote')}</p>}
            </div>
          </details>
          <details id="miner-settings">
            <summary>{t('minerSettingsTitle')}</summary>
            <p className="start-note">{tx('minerSettingsLead', [<code className="mono inline-code">.\seine.exe</code>, <code className="mono inline-code">./seine</code>])}</p>
            <table className="info-table miner-settings-table">
              <tbody>
                {minerSettings.map(([key, flags]) => (
                  <tr key={key}><td>{t(key)}</td><td><code className="mono inline-code" dir="ltr">./seine {flags}</code></td></tr>
                ))}
              </tbody>
            </table>
            <p className="start-note"><a href={`${seineGuideBase}configure-seine-miner/`}>{t('minerSettingsGuide')}</a></p>
          </details>
          <ExchangeSetup info={poolInfo} poolUrl={poolUrl} />
          <details id="help">
            <summary>{t('helpRow')}</summary>
            <ul className="help-links" aria-label={t('browseTopics')}>
              <li><a href={`${localizePath('/blog/')}#getting-started`}>{t('topicGettingStarted')}</a></li>
              <li><a href={`${seineGuideBase}mine-blocknet-on-windows/`}>{t("windowsGuide")}</a></li>
              <li><a href={`${localizePath("/blog/")}track-your-miner-on-bntpool/#zero-hashrate`}>{t("minerHelp")}</a></li>
              <li><a href={`${localizePath('/blog/')}#troubleshooting`}>{t('topicTroubleshooting')}</a></li>
            </ul>
            <p className="start-note">{tx("m464", [<code className="mono inline-code">seine-data/seine.config.json</code>])}</p>
            <p className="start-note">{tx("m465", [<code className="mono inline-code">--address</code>, <code className="mono inline-code">--pool-url</code>, <code className="mono inline-code">--pool-worker</code>])}</p>
          </details>
        </div>
      </div>

      <div className="card section" id="pool-information">
        <div className="start-stratum">
          <span>{t('m402')}</span>
          <div className="quickstart-copy">
            <code className="mono" dir="ltr">{poolUrl}</code>
            <button type="button" onClick={() => copyToClipboard(poolUrl, 'pool-url')} aria-label={t('m387')}>
              <span role="status">{copiedKey === 'pool-url' ? t('m386') : t('m392')}</span>
            </button>
          </div>
        </div>
        <p className="start-note start-info-link">{tx('startInfoLink', [<a href={localizePath('/info')}>{t('infoNav')}</a>])}</p>
      </div>
    </div>
  );
}
