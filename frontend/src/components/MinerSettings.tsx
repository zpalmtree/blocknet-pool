import { localizePath, t, tx } from '../i18n';
import { useCallback, useEffect, useId, useState } from 'react';

import type { ApiClient } from '../api/client';
import { SettingsError } from '../api/client';
import { timeAgo } from '../lib/format';
import type { MinerSettingsState, MinerSettingsValues } from '../types';

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

export function MinerSettings({ api, account, onSaved, onUnlockedChange }: MinerSettingsProps) {
  const [state, setState] = useState<MinerSettingsState | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [takingBack, setTakingBack] = useState(false);
  const [rigIp, setRigIp] = useState('');
  const [changingPassword, setChangingPassword] = useState(false);
  const [draft, setDraft] = useState<{ minPayout: string; solo: boolean; hide: boolean } | null>(null);

  const adopt = useCallback((next: MinerSettingsState) => {
    setState(next);
    onUnlockedChange?.(next.unlocked);
    setDraft(next.settings ? {
      minPayout: String(next.settings.min_payout),
      solo: next.settings.default_solo,
      hide: next.settings.hide_workers,
    } : null);
  }, [onUnlockedChange]);

  useEffect(() => {
    let active = true;
    setState(null);
    setLoadFailed(false);
    setError('');
    setNotice('');
    setTakingBack(false);
    setChangingPassword(false);
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
    const saved = state.settings;
    const min = state.pool_min_payout;
    const max = state.max_min_payout;
    const save = () => {
      const minPayout = Number(draft.minPayout.trim().replace(',', '.'));
      if (!Number.isFinite(minPayout) || minPayout < min || minPayout > max) {
        setNotice('');
        setError(t('settingsMinPayoutInvalid', [formatNumber(min), formatNumber(max)]));
        return;
      }
      const next: MinerSettingsValues = { min_payout: minPayout, default_solo: draft.solo, hide_workers: draft.hide };
      void run(() => api.minerSettings.save(account, next), (result) => {
        const notes = [t('settingsSaved')];
        if (result.settings && result.settings.min_payout !== saved.min_payout) notes.push(t('settingsSavedMinNote'));
        if (result.settings && result.settings.default_solo !== saved.default_solo) notes.push(t('settingsSavedModeNote'));
        setNotice(notes.join(' '));
        onSaved();
      });
    };
    body = (<>
      {notice && <p className="settings-saved" role="status">{notice}</p>}
      <form className="settings-form" onSubmit={(event) => { event.preventDefault(); save(); }}>
        <Row label={t('settingsMinPayout')} hint={t('settingsMinPayoutRange', [formatNumber(min), formatNumber(max)])}
          help={{ text: t('settingsMinPayoutHelp'), href: `${localizePath('/info')}#minimum-payout`, link: t('settingsHowPayoutsWork') }}>
          <div className="settings-amount">
            <input type="text" inputMode="decimal" dir="ltr" aria-label={t('settingsMinPayout')} value={draft.minPayout}
              onChange={(event) => setDraft({ ...draft, minPayout: event.target.value })} />
            <span>BNT</span>
          </div>
        </Row>
        <Row label={t('settingsMode')} hint={tx('settingsModeHint', [<code key="solo">solo:</code>])}
          help={{ text: t('infoSoloLead'), href: `${localizePath('/info')}#solo`, link: t('settingsPoolVsSolo') }}>
          <div className="start-choice" role="group" aria-label={t('settingsMode')}>
            <button type="button" className={draft.solo ? '' : 'active'} aria-pressed={!draft.solo}
              onClick={() => setDraft({ ...draft, solo: false })}>{t('typePool')}</button>
            <button type="button" className={draft.solo ? 'active' : ''} aria-pressed={draft.solo}
              onClick={() => setDraft({ ...draft, solo: true })}>{t('typeSolo')}</button>
          </div>
        </Row>
        <Row label={t('settingsWorkerNames')} hint={t('settingsHideWorkersHint')}>
          <div className="settings-checks">
            <label><input type="checkbox" checked={draft.hide} onChange={(event) => setDraft({ ...draft, hide: event.target.checked })} /> {t('settingsHideWorkers')}</label>
          </div>
        </Row>
        {error && <p className="settings-error settings-row-error" role="alert">{error}</p>}
        <div className="settings-actions">
          <button type="submit" className="btn btn-primary" disabled={busy}>{t('settingsSave')}</button>
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
