import { locale, localizePath, t, tx } from '../i18n';
import { useCallback, useEffect, useId, useRef, useState } from 'react';

import type { ApiClient } from '../api/client';
import { SettingsError } from '../api/client';
import { timeAgo } from '../lib/format';
import type { MinerAlertChoices, MinerSettingsState, MinerSettingsValues } from '../types';

const LOCK = (
  <svg viewBox="0 0 16 16" aria-hidden="true" className="settings-lock">
    <rect x="3" y="7" width="10" height="7" rx="1.5" />
    <path d="M5.5 7V5a2.5 2.5 0 0 1 5 0v2" />
  </svg>
);
const UNLOCK = (
  <svg viewBox="0 0 16 16" aria-hidden="true" className="settings-lock">
    <rect x="3" y="7" width="10" height="7" rx="1.5" />
    <path d="M5.5 7V5a2.5 2.5 0 0 1 4.9-.7" />
  </svg>
);

export function SettingsLockIcon({ open }: { open: boolean }) {
  return open ? UNLOCK : LOCK;
}

function lang(): string | undefined {
  return typeof document !== 'undefined' ? document.documentElement.lang || undefined : undefined;
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat(lang(), { maximumFractionDigits: 8 }).format(value);
}

function formatShare(share: number): string {
  return new Intl.NumberFormat(lang(), { style: 'percent', maximumFractionDigits: 0 }).format(share);
}

/** "in 45 minutes" or "in 3 hours", rounded up, in the page language. */
function formatWait(seconds: number): string {
  const minutes = Math.max(1, Math.ceil(seconds / 60));
  const [value, unit]: [number, Intl.RelativeTimeFormatUnit] =
    minutes < 90 ? [minutes, 'minute'] : [Math.ceil(minutes / 60), 'hour'];
  return new Intl.RelativeTimeFormat(lang(), { numeric: 'always' }).format(value, unit);
}

/** "10 min" or "1 hour" for the rig-stopped choice. */
function formatWaitShort(minutes: number): string {
  const [value, unit]: [number, Intl.NumberFormatOptions['unit']] = minutes < 60 ? [minutes, 'minute'] : [minutes / 60, 'hour'];
  return new Intl.NumberFormat(lang(), { style: 'unit', unit, unitDisplay: 'long' }).format(value);
}

/** A form message for a refused request. */
function errorText(error: unknown): string {
  if (!(error instanceof SettingsError)) return t('settingsError');
  switch (error.code) {
    case 'wrong_password':
      return t('settingsWrongPassword', [error.triesLeft ?? 0]);
    case 'rate_limited':
      return t('settingsTooManyTries', [formatWait(error.retryAfter ?? 60)]);
    case 'cooling_down':
      return t('settingsCoolingDown', [formatWait(error.retryAfter ?? 3600)]);
    case 'not_enough_work':
      return t('settingsTakeBackFail', [error.triesLeft ?? 0]);
    case 'invalid_ip':
      return t('settingsInvalidIp');
    case 'invalid_password':
      return t('settingsPasswordShort');
    case 'already_claimed':
      return t('settingsAlreadyClaimed');
    case 'locked':
      return t('settingsLockedAgain');
    case 'invalid_email':
      return t('alertsInvalidEmail');
    case 'email_failed':
      return t('alertsSendFailed');
    case 'discord_failed':
      return t('alertsDiscordFailed');
    case 'invalid_webhook':
      return t('alertsDiscordInvalid');
    case 'not_mining':
      return t('alertsNotMining');
    default:
      return t('settingsError');
  }
}

type Help = { text: string; href: string; link: string };

function Row({ label, hint, help, children }: { label: string; hint?: React.ReactNode; help?: Help; children: React.ReactNode }) {
  const [open, setOpen] = useState(false);
  const id = useId();
  return (
    <div className="settings-row">
      <div className="settings-row-label">{label}{help && (
        <button type="button" className="stats-help" aria-expanded={open} aria-controls={id}
          aria-label={t('settingsHelpLabel')} title={t('settingsHelpLabel')} onClick={() => setOpen((value) => !value)}>?</button>
      )}</div>
      <div className="settings-row-control">
        {children}
        {hint && <p className="settings-hint">{hint}</p>}
        {help && open && <p id={id} className="stats-help-note settings-help-note">{help.text} <a href={help.href}>{help.link}</a></p>}
      </div>
    </div>
  );
}

function PasswordForm({ busy, onSubmit }: { busy: boolean; onSubmit: (password: string) => void }) {
  const [password, setPassword] = useState('');
  const [repeat, setRepeat] = useState('');
  const [problem, setProblem] = useState('');
  return (
    <form className="settings-form" onSubmit={(event) => {
      event.preventDefault();
      if ([...password].length < 8) return setProblem(t('settingsPasswordShort'));
      if (password !== repeat) return setProblem(t('settingsPasswordMismatch'));
      setProblem('');
      onSubmit(password);
    }}>
      <div className="settings-inline">
        <input type="password" autoComplete="new-password" placeholder={t('settingsNewPassword')} aria-label={t('settingsNewPassword')}
          value={password} onChange={(event) => setPassword(event.target.value)} />
        <input type="password" autoComplete="new-password" placeholder={t('settingsRepeatPassword')} aria-label={t('settingsRepeatPassword')}
          value={repeat} onChange={(event) => setRepeat(event.target.value)} />
        <button type="submit" className="btn btn-primary" disabled={busy}>{t('settingsSetPassword')}</button>
      </div>
      {problem && <p className="settings-error" role="alert">{problem}</p>}
    </form>
  );
}

interface MinerSettingsProps {
  api: ApiClient;
  account: string;
  /** A change the rest of My Stats shows (the payout minimum). */
  onSaved: () => void;
  /** Whether this browser has the account's settings unlocked. */
  onUnlockedChange?: (unlocked: boolean) => void;
}

const DEFAULT_ALERTS: Required<MinerAlertChoices> = {
  offline_minutes: 30, notify_payouts: true, notify_solo_blocks: false,
  hashrate_drop_minutes: 0, notify_updates: false, weekly_summary: false,
};

function alertChoices(alerts: MinerAlertChoices): Required<MinerAlertChoices> {
  return {
    offline_minutes: alerts.offline_minutes, notify_payouts: alerts.notify_payouts, notify_solo_blocks: alerts.notify_solo_blocks,
    hashrate_drop_minutes: alerts.hashrate_drop_minutes ?? 0, notify_updates: alerts.notify_updates ?? false,
    weekly_summary: alerts.weekly_summary ?? false,
  };
}

type Draft = {
  minPayout: string; solo: boolean; hide: boolean; privateStats: boolean;
  /** The typed starting difficulty, kept while "Set" is chosen. */
  difficulty: string; difficultySet: boolean; alerts: Required<MinerAlertChoices>;
};

/** The toggles as the server has them; typed fields keep what is being typed. */
function followServer(draft: Draft, settings: MinerSettingsValues, alerts: MinerAlertChoices | null | undefined): Draft {
  return {
    ...draft, solo: settings.default_solo, hide: settings.hide_workers, privateStats: settings.private_stats ?? false,
    difficultySet: settings.start_difficulty != null, alerts: alerts ? alertChoices(alerts) : draft.alerts,
  };
}

/** Discord channel webhooks; anything else is refused before it reaches the pool. */
function isDiscordWebhook(url: string): boolean {
  return /^https:\/\/(?:canary\.|ptb\.)?discord(?:app)?\.com\/api\/webhooks\/\d+\/[\w-]+$/.test(url.trim());
}

export function MinerSettings({ api, account, onSaved, onUnlockedChange }: MinerSettingsProps) {
  const [state, setState] = useState<MinerSettingsState | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [takingBack, setTakingBack] = useState(false);
  const [rigIp, setRigIp] = useState('');
  const [changingPassword, setChangingPassword] = useState(false);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [alertEmail, setAlertEmail] = useState('');
  const [discordUrl, setDiscordUrl] = useState('');
  // The Connect Telegram link from this browser's last request.
  const [telegramLink, setTelegramLink] = useState('');
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved'>('idle');
  const [saveNote, setSaveNote] = useState('');
  // Values of the newest change, saved or still on its way to the server.
  const intended = useRef<MinerSettingsValues | null>(null);
  const queued = useRef<MinerSettingsValues | null>(null);
  const saving = useRef(false);
  const minPayoutTimer = useRef<number | undefined>(undefined);
  const difficultyTimer = useRef<number | undefined>(undefined);

  const adopt = useCallback((next: MinerSettingsState) => {
    setState(next);
    onUnlockedChange?.(next.unlocked);
    intended.current = next.settings ?? null;
    setDraft(next.settings ? followServer({
      minPayout: String(next.settings.min_payout),
      difficulty: next.settings.start_difficulty != null ? formatNumber(next.settings.start_difficulty) : '',
      solo: false, hide: false, privateStats: false, difficultySet: false, alerts: DEFAULT_ALERTS,
    }, next.settings, next.alerts) : null);
    setAlertEmail(next.alerts?.email ?? '');
    setDiscordUrl('');
  }, [onUnlockedChange]);

  useEffect(() => () => {
    window.clearTimeout(minPayoutTimer.current);
    window.clearTimeout(difficultyTimer.current);
  }, []);

  // While Telegram waits for Start in the bot, check every few seconds so the
  // row turns connected without a reload.
  const pendingTelegram = !!state?.alerts?.telegram && 'pending' in state.alerts.telegram;
  useEffect(() => {
    if (!pendingTelegram) return;
    const started = Date.now();
    const timer = window.setInterval(() => {
      if (Date.now() - started > 10 * 60_000) {
        window.clearInterval(timer);
        return;
      }
      api.minerSettings.get(account).then((next) => {
        setState(next);
        const settings = next.settings;
        if (settings) setDraft((draft) => draft ? followServer(draft, settings, next.alerts) : draft);
      }).catch(() => {});
    }, 4000);
    return () => window.clearInterval(timer);
  }, [pendingTelegram, api, account]);

  useEffect(() => {
    let active = true;
    setState(null);
    setLoadFailed(false);
    setError('');
    setNotice('');
    setTakingBack(false);
    setChangingPassword(false);
    setSaveStatus('idle');
    api.minerSettings.get(account)
      .then((next) => { if (active) adopt(next); })
      .catch(() => { if (active) setLoadFailed(true); });
    return () => { active = false; };
  }, [api, account, adopt]);

  const run = async (request: () => Promise<MinerSettingsState>, after?: (next: MinerSettingsState) => void) => {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const next = await request();
      adopt(next);
      after?.(next);
    } catch (err) {
      setError(errorText(err));
      if (err instanceof SettingsError && (err.code === 'locked' || err.code === 'already_claimed')) {
        api.minerSettings.get(account).then(adopt).catch(() => {});
      }
    } finally {
      setBusy(false);
    }
  };

  /** Saves each change as it is made, one request at a time, newest values last. */
  const persist = async (values: MinerSettingsValues) => {
    intended.current = values;
    if (saving.current) {
      queued.current = values;
      return;
    }
    saving.current = true;
    // Leave any notice above the form in place so the controls do not move under the pointer.
    setSaveStatus('saving');
    setError('');
    const before = state?.settings;
    let next: MinerSettingsValues | null = values;
    try {
      let result: MinerSettingsState | null = null;
      while (next) {
        result = await api.minerSettings.save(account, next);
        next = queued.current;
        queued.current = null;
      }
      if (!result) return;
      setState(result);
      onUnlockedChange?.(result.unlocked);
      const settings = result.settings;
      intended.current = settings ?? null;
      // Keep whatever is being typed in the amount fields; the toggles follow the server.
      setDraft((draft) => settings && draft ? followServer(draft, settings, result.alerts) : draft);
      const notes: string[] = [];
      if (settings && before && settings.min_payout !== before.min_payout) notes.push(t('settingsSavedMinNote'));
      if (settings && before && settings.default_solo !== before.default_solo) notes.push(t('settingsSavedModeNote'));
      setSaveNote(notes.join(' '));
      setSaveStatus('saved');
      onSaved();
    } catch (err) {
      queued.current = null;
      setSaveStatus('idle');
      setError(errorText(err));
      // Show what the server still has, not the change that failed.
      const saved = state?.settings;
      intended.current = saved ?? null;
      if (saved) setDraft((draft) => draft ? followServer(draft, saved, state?.alerts) : draft);
      if (err instanceof SettingsError && (err.code === 'locked' || err.code === 'already_claimed')) {
        api.minerSettings.get(account).then(adopt).catch(() => {});
      }
    } finally {
      saving.current = false;
    }
  };

  if (loadFailed) {
    return <div className="card settings-card"><p className="settings-error" role="alert">{t('settingsError')}</p></div>;
  }
  if (!state) {
    return <div className="card settings-card" aria-busy="true"><p className="settings-lead">{t('m094')}</p></div>;
  }

  const open = state.unlocked;
  const changed = state.changed_at != null ? timeAgo(state.changed_at) : '';
  const startTakeBack = () => {
    setRigIp(state.your_ip ?? '');
    setError('');
    setNotice('');
    setTakingBack(true);
  };

  let body: React.ReactNode;
  if (takingBack && !open) {
    body = (<>
      <h3 className="settings-title">{t('settingsTakeBackTitle')}</h3>
      <p className="settings-lead">{t('settingsTakeBackLead')}</p>
      <form className="settings-form" onSubmit={(event) => {
        event.preventDefault();
        void run(() => api.minerSettings.takeBack(account, rigIp.trim()), (next) => {
          setTakingBack(false);
          if (next.share != null) setNotice(t('settingsTakeBackOk', [rigIp.trim(), formatShare(next.share)]));
        });
      }}>
        <div className="settings-inline">
          <input type="text" dir="ltr" inputMode="decimal" autoComplete="off" spellCheck={false}
            placeholder={t('settingsRigIp')} aria-label={t('settingsRigIp')} aria-invalid={!!error}
            value={rigIp} onChange={(event) => setRigIp(event.target.value)} />
          <button type="submit" className="btn btn-primary" disabled={busy || !rigIp.trim()}>{t('settingsCheck')}</button>
          <button type="button" className="link-button" onClick={() => { setTakingBack(false); setError(''); }}>{t('settingsCancel')}</button>
        </div>
        {error ? <p className="settings-error" role="alert">{error}</p> : <p className="settings-foot">{t('settingsIpHint')}</p>}
      </form>
    </>);
  } else if (!state.claimed) {
    body = (<>
      <p className="settings-lead">{t('settingsClaimLead')}</p>
      <PasswordForm busy={busy} onSubmit={(password) => void run(() => api.minerSettings.claim(account, password))} />
      {error && <p className="settings-error" role="alert">{error}</p>}
    </>);
  } else if (!open) {
    body = (<>
      {state.has_password ? (<>
        <p className="settings-lead">{t('settingsUnlockLead')}</p>
        <form className="settings-form" onSubmit={(event) => {
          event.preventDefault();
          const field = event.currentTarget.elements.namedItem('password') as HTMLInputElement;
          void run(() => api.minerSettings.unlock(account, field.value), () => { field.value = ''; });
        }}>
          <div className="settings-inline">
            <input name="password" type="password" autoComplete="current-password" placeholder={t('settingsPassword')}
              aria-label={t('settingsPassword')} aria-invalid={!!error} />
            <button type="submit" className="btn btn-primary" disabled={busy}>{t('settingsUnlock')}</button>
          </div>
        </form>
      </>) : <p className="settings-lead">{t('settingsNoPasswordLead')}</p>}
      {error && <p className="settings-error" role="alert">{error}</p>}
      <p className="settings-foot">{state.has_password && <>{t('settingsForgot')} </>}
        <button type="button" className="link-button" onClick={startTakeBack}>{t('settingsTakeBackLink')}</button></p>
    </>);
  } else if (!state.has_password) {
    body = (<>
      <p className="settings-saved" role="status">{notice || t('settingsSetNewLead')}</p>
      <PasswordForm busy={busy} onSubmit={(password) => void run(() => api.minerSettings.setPassword(account, password),
        () => setNotice(t('settingsPasswordChanged')))} />
      {error && <p className="settings-error" role="alert">{error}</p>}
    </>);
  } else if (draft && state.settings) {
    const min = state.pool_min_payout;
    const max = state.max_min_payout;
    const parseMinPayout = (text: string) => Number(text.trim().replace(',', '.'));
    const change = (values: Partial<MinerSettingsValues>) => {
      const base = intended.current ?? state.settings!;
      void persist({ ...base, ...values });
    };
    const changeAlerts = (choices: Partial<MinerAlertChoices>) => {
      const alerts = { ...draft.alerts, ...choices };
      setDraft({ ...draft, alerts });
      change({ alerts });
    };
    /** Saves a typed minimum once it is valid; `final` also reports a bad value. */
    const commitMinPayout = (text: string, final: boolean) => {
      window.clearTimeout(minPayoutTimer.current);
      const minPayout = parseMinPayout(text);
      if (!text.trim() || !Number.isFinite(minPayout) || minPayout < min || minPayout > max) {
        if (final) {
          setError(t('settingsMinPayoutInvalid', [formatNumber(min), formatNumber(max)]));
        }
        return;
      }
      if (minPayout !== (intended.current ?? state.settings!).min_payout) change({ min_payout: minPayout });
    };
    const range = state.difficulty;
    /** Saves a typed starting difficulty once it is a whole number in range. */
    const commitDifficulty = (text: string, final: boolean) => {
      window.clearTimeout(difficultyTimer.current);
      if (!range) return;
      const value = Number(text.trim().replace(/[\s,._]/g, ''));
      if (!text.trim() || !Number.isInteger(value) || value < range.min || value > range.max) {
        if (final) setError(t('settingsDifficultyInvalid', [formatNumber(range.min), formatNumber(range.max)]));
        return;
      }
      if (value !== (intended.current ?? state.settings!).start_difficulty) change({ start_difficulty: value });
    };
    const alerts = state.alerts;
    const telegram = alerts?.telegram;
    const canAlert = !!alerts && (alerts.confirmed || !!alerts.discord || (!!telegram && 'name' in telegram));
    const removeEmail = () => void run(() => api.minerSettings.removeAlertEmail(account), () => setNotice(t('alertsRemoved')));
    body = (<>
      {notice && <p className="settings-saved" role="status">{notice}</p>}
      <form className="settings-form" onSubmit={(event) => {
        event.preventDefault();
        commitMinPayout(draft.minPayout, true);
        if (draft.difficultySet) commitDifficulty(draft.difficulty, true);
      }}>
        <h3 className="settings-section">{t('settingsSectionPayouts')}</h3>
        <Row label={t('settingsMinPayout')} hint={t('settingsMinPayoutRange', [formatNumber(min), formatNumber(max)])}
          help={{ text: t('settingsMinPayoutHelp'), href: `${localizePath('/info')}#minimum-payout`, link: t('settingsHowPayoutsWork') }}>
          <div className="settings-amount">
            <input type="text" inputMode="decimal" dir="ltr" enterKeyHint="done" aria-label={t('settingsMinPayout')} value={draft.minPayout}
              onChange={(event) => {
                const text = event.target.value;
                setDraft({ ...draft, minPayout: text });
                setError('');
                window.clearTimeout(minPayoutTimer.current);
                minPayoutTimer.current = window.setTimeout(() => commitMinPayout(text, false), 1200);
              }}
              onBlur={(event) => commitMinPayout(event.target.value, true)} />
            <span>BNT</span>
          </div>
        </Row>
        <Row label={t('settingsMode')} hint={tx('settingsModeHint', [<code key="solo">solo:</code>])}
          help={{ text: t('infoSoloLead'), href: `${localizePath('/info')}#solo`, link: t('settingsPoolVsSolo') }}>
          <div className="start-choice" role="group" aria-label={t('settingsMode')}>
            <button type="button" className={draft.solo ? '' : 'active'} aria-pressed={!draft.solo}
              onClick={() => { if (draft.solo) { setDraft({ ...draft, solo: false }); change({ default_solo: false }); } }}>{t('typePool')}</button>
            <button type="button" className={draft.solo ? 'active' : ''} aria-pressed={draft.solo}
              onClick={() => { if (!draft.solo) { setDraft({ ...draft, solo: true }); change({ default_solo: true }); } }}>{t('typeSolo')}</button>
          </div>
        </Row>
        {range && (
          <Row label={t('settingsDifficulty')}
            hint={<>{draft.difficultySet
              ? t('settingsDifficultySetHint', [formatNumber(range.min), formatNumber(range.max)])
              : t('settingsDifficultyAutoHint')}{range.current != null && <> {t('settingsDifficultyNow', [formatNumber(range.current)])}</>}</>}
            help={{ text: t('settingsDifficultyHelp'), href: `${localizePath('/blog/')}track-your-miner-on-bntpool/`, link: t('settingsDifficultyMore') }}>
            <div className="settings-inline">
              <div className="start-choice" role="group" aria-label={t('settingsDifficulty')}>
                <button type="button" className={draft.difficultySet ? '' : 'active'} aria-pressed={!draft.difficultySet}
                  onClick={() => {
                    if (!draft.difficultySet) return;
                    setDraft({ ...draft, difficultySet: false });
                    setError('');
                    change({ start_difficulty: null });
                  }}>{t('settingsDifficultyAuto')}</button>
                <button type="button" className={draft.difficultySet ? 'active' : ''} aria-pressed={draft.difficultySet}
                  onClick={() => {
                    if (draft.difficultySet) return;
                    const text = draft.difficulty || formatNumber(range.current ?? range.min);
                    setDraft({ ...draft, difficultySet: true, difficulty: text });
                    commitDifficulty(text, false);
                  }}>{t('settingsDifficultySet')}</button>
              </div>
              {draft.difficultySet && (
                <div className="settings-amount">
                  <input type="text" inputMode="numeric" dir="ltr" enterKeyHint="done" aria-label={t('settingsDifficulty')} value={draft.difficulty}
                    onChange={(event) => {
                      const text = event.target.value;
                      setDraft({ ...draft, difficulty: text });
                      setError('');
                      window.clearTimeout(difficultyTimer.current);
                      difficultyTimer.current = window.setTimeout(() => commitDifficulty(text, false), 1200);
                    }}
                    onBlur={(event) => commitDifficulty(event.target.value, true)} />
                </div>
              )}
            </div>
          </Row>
        )}
        <h3 className="settings-section">{t('settingsSectionPrivacy')}</h3>
        <Row label={t('settingsPublicStats')}>
          <div className="settings-checks">
            <label><input type="checkbox" checked={draft.hide || draft.privateStats} disabled={draft.privateStats} onChange={(event) => {
              const hide = event.target.checked;
              setDraft({ ...draft, hide });
              change({ hide_workers: hide });
            }} /> {t('settingsHideWorkerNames')}</label>
            <span className="settings-check-note">{t('settingsHideWorkersHint')}</span>
            <label><input type="checkbox" checked={draft.privateStats} onChange={(event) => {
              const privateStats = event.target.checked;
              setDraft({ ...draft, privateStats });
              change({ private_stats: privateStats });
            }} /> {t('settingsPrivateStats')}</label>
            <span className="settings-check-note">{t('settingsPrivateStatsNote')}</span>
          </div>
        </Row>
        {state.alerts_available && (<>
          <h3 className="settings-section">{t('settingsSectionAlerts')}</h3>
          <Row label={t('alertsEmailLabel')} hint={!alerts?.email
            ? t('alertsEmailHint')
            : alerts.confirmed
              ? <button type="button" className="link-button" disabled={busy} onClick={removeEmail}>{t('alertsRemove')}</button>
              : <>{t('alertsPendingHint', [alerts.email])}{' '}
                  <button type="button" className="link-button" disabled={busy}
                    onClick={() => void run(() => api.minerSettings.setAlertEmail(account, alerts.email, locale),
                      () => setNotice(t('alertsLinkSent', [alerts.email])))}>{t('alertsResend')}</button>{' · '}
                  <button type="button" className="link-button" disabled={busy} onClick={removeEmail}>{t('alertsRemove')}</button></>}>
            <div className="settings-email">
              <input type="email" dir="ltr" autoComplete="email" aria-label={t('alertsEmail')} placeholder="you@example.com"
                value={alertEmail} onChange={(event) => setAlertEmail(event.target.value)} />
              {(!alerts?.email || alertEmail.trim() !== alerts.email) && (
                <button type="button" className="btn btn-secondary" disabled={busy || !alertEmail.trim()}
                  onClick={() => void run(() => api.minerSettings.setAlertEmail(account, alertEmail.trim(), locale),
                    () => setNotice(t('alertsLinkSent', [alertEmail.trim()])))}>{t('alertsSendLink')}</button>
              )}
              {alerts?.email && alertEmail.trim() === alerts.email && (alerts.confirmed
                ? <span className="badge settings-badge-ok">{t('alertsConfirmed')}</span>
                : <span className="badge settings-badge-warn">{t('alertsNotConfirmed')}</span>)}
            </div>
          </Row>
          <Row label={t('alertsDiscord')} hint={alerts?.discord
            ? <button type="button" className="link-button" disabled={busy}
                onClick={() => void run(() => api.minerSettings.removeDiscord(account), () => setNotice(t('alertsDiscordRemoved')))}>{t('alertsDisconnect')}</button>
            : t('alertsDiscordHint')}>
            {alerts?.discord ? (
              <div className="settings-email">
                <span className="settings-channel" dir="ltr">{alerts.discord}</span>
                <span className="badge settings-badge-ok">{t('alertsConnected')}</span>
              </div>
            ) : (
              <div className="settings-email">
                <input type="url" dir="ltr" autoComplete="off" spellCheck={false} aria-label={t('alertsDiscord')}
                  placeholder="https://discord.com/api/webhooks/…" value={discordUrl} onChange={(event) => setDiscordUrl(event.target.value)} />
                <button type="button" className="btn btn-secondary" disabled={busy || !discordUrl.trim()}
                  onClick={() => {
                    if (!isDiscordWebhook(discordUrl)) return setError(t('alertsDiscordInvalid'));
                    void run(() => api.minerSettings.setDiscord(account, discordUrl.trim(), locale), () => setNotice(t('alertsDiscordSaved')));
                  }}>{t('alertsDiscordTest')}</button>
              </div>
            )}
          </Row>
          {state.telegram_available && (<Row label={t('alertsTelegram')} hint={telegram && 'name' in telegram
            ? <button type="button" className="link-button" disabled={busy}
                onClick={() => void run(() => api.minerSettings.removeTelegram(account), () => setNotice(t('alertsTelegramRemoved')))}>{t('alertsDisconnect')}</button>
            : telegram && 'pending' in telegram
              ? <>{t('alertsTelegramWaiting')}{(telegram.pending || telegramLink) && <>{' '}
                  <a href={telegram.pending || telegramLink} target="_blank" rel="noreferrer">{t('alertsTelegramOpenAgain')}</a></>}</>
              : t('alertsTelegramHint')}>
            {telegram && 'name' in telegram ? (
              <div className="settings-email">
                <span className="settings-channel" dir="ltr">{telegram.name}</span>
                <span className="badge settings-badge-ok">{t('alertsConnected')}</span>
              </div>
            ) : (
              <button type="button" className="btn btn-secondary" disabled={busy}
                onClick={() => {
                  // Open the tab inside the click so pop-up blockers allow it,
                  // then point it at the bot once the link is ready.
                  const tab = window.open('', '_blank');
                  let opened = false;
                  void run(() => api.minerSettings.connectTelegram(account, locale), (next) => {
                    const link = next.alerts?.telegram;
                    if (link && 'pending' in link && link.pending) {
                      setTelegramLink(link.pending);
                      if (tab) {
                        tab.opener = null;
                        tab.location.href = link.pending;
                        opened = true;
                      }
                    }
                  }).then(() => { if (tab && !opened) tab.close(); });
                }}>{t('alertsTelegramConnect')}</button>
            )}
          </Row>)}
          <Row label={t('alertsWhenAny')} hint={canAlert ? t('alertsLimitHintAll') : t('alertsAfterChannelHint')}>
            <div className={`settings-checks ${canAlert ? '' : 'is-disabled'}`}>
              <label>
                <input type="checkbox" disabled={!canAlert} checked={draft.alerts.offline_minutes > 0}
                  onChange={(event) => changeAlerts({ offline_minutes: event.target.checked ? 30 : 0 })} />
                {' '}{t('alertsRigStops')}
                <select aria-label={t('alertsRigStops')} disabled={!canAlert || draft.alerts.offline_minutes === 0}
                  value={draft.alerts.offline_minutes || 30}
                  onChange={(event) => changeAlerts({ offline_minutes: Number(event.target.value) })}>
                  {[10, 30, 60].map((minutes) => <option key={minutes} value={minutes}>{formatWaitShort(minutes)}</option>)}
                </select>
              </label>
              <span className="settings-check-note">{t('alertsRigStopsNoteAll')}</span>
              <label>
                <input type="checkbox" disabled={!canAlert} checked={draft.alerts.hashrate_drop_minutes > 0}
                  onChange={(event) => changeAlerts({ hashrate_drop_minutes: event.target.checked ? 30 : 0 })} />
                {' '}{t('alertsHashrateDrops')}
                <select aria-label={t('alertsHashrateDrops')} disabled={!canAlert || draft.alerts.hashrate_drop_minutes === 0}
                  value={draft.alerts.hashrate_drop_minutes || 30}
                  onChange={(event) => changeAlerts({ hashrate_drop_minutes: Number(event.target.value) })}>
                  {[30, 60, 120].map((minutes) => <option key={minutes} value={minutes}>{formatWaitShort(minutes)}</option>)}
                </select>
              </label>
              <span className="settings-check-note">{t('alertsHashrateDropsNote')}</span>
              <label><input type="checkbox" disabled={!canAlert} checked={draft.alerts.notify_payouts}
                onChange={(event) => changeAlerts({ notify_payouts: event.target.checked })} /> {t('alertsPayout')}</label>
              <label><input type="checkbox" disabled={!canAlert} checked={draft.alerts.notify_solo_blocks}
                onChange={(event) => changeAlerts({ notify_solo_blocks: event.target.checked })} /> {t('alertsSoloBlock')}</label>
              <label><input type="checkbox" disabled={!canAlert} checked={draft.alerts.notify_updates}
                onChange={(event) => changeAlerts({ notify_updates: event.target.checked })} /> {t('alertsUpdates')}</label>
              <span className="settings-check-note">{t('alertsUpdatesNote')}</span>
              <label><input type="checkbox" disabled={!canAlert} checked={draft.alerts.weekly_summary}
                onChange={(event) => changeAlerts({ weekly_summary: event.target.checked })} /> {t('alertsWeekly')}</label>
              <span className="settings-check-note">{t('alertsWeeklyNote')}</span>
            </div>
          </Row>
        </>)}
        {error && <p className="settings-error settings-row-error" role="alert">{error}</p>}
        <div className="settings-actions">
          <p className={`settings-autosave ${saveStatus === 'saved' ? 'is-saved' : ''}`} role="status" aria-live="polite">
            {saveStatus === 'saving' ? t('settingsSaving')
              : saveStatus === 'saved' ? [t('settingsSaved'), saveNote].filter(Boolean).join(' ')
              : t('settingsAutoSave')}
          </p>
          <button type="button" className="link-button" aria-expanded={changingPassword}
            onClick={() => setChangingPassword((value) => !value)}>{t('settingsChangePassword')}</button>
        </div>
      </form>
      {changingPassword && (<div className="settings-change-password">
        <PasswordForm busy={busy} onSubmit={(password) => void run(() => api.minerSettings.setPassword(account, password), () => {
          setChangingPassword(false);
          setNotice(t('settingsPasswordChanged'));
        })} />
      </div>)}
    </>);
  }

  return (<>
    {state.claimed && changed && (
      <div className="settings-meta">
        <span className={`settings-state ${open ? 'is-open' : ''}`}>
          <SettingsLockIcon open={open} />{t(open ? 'settingsUnlocked' : 'settingsLocked', [changed])}
        </span>
      </div>
    )}
    <div className="card settings-card">{body}</div>
  </>);
}
