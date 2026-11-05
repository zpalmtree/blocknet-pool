import { localizePath, t } from '../i18n';
import { useState } from 'react';

import type { ApiClient } from '../api/client';

type Action = 'confirm' | 'unsubscribe';
type Status = 'ready' | 'working' | 'done' | 'invalid' | 'failed';

/** Where links in alert emails land: `/alerts?confirm=…` or `?unsubscribe=…`. */
function linkFromLocation(): { action: Action; token: string } | null {
  const params = new URLSearchParams(window.location.search);
  for (const action of ['confirm', 'unsubscribe'] as const) {
    const token = params.get(action)?.trim().toLowerCase();
    if (token && /^[0-9a-f]{64}$/.test(token)) return { action, token };
  }
  return null;
}

export function AlertsPage({ api }: { api: ApiClient }) {
  const [link] = useState(linkFromLocation);
  const [status, setStatus] = useState<Status>(link ? 'ready' : 'invalid');
  const [account, setAccount] = useState('');

  // Mail scanners open links and some run scripts, so nothing happens until
  // the person presses the button.
  const act = () => {
    if (!link || status === 'working') return;
    setStatus('working');
    api.alertLink(link.action, link.token)
      .then((result) => {
        setAccount(result.account ?? '');
        setStatus('done');
      })
      .catch((err) => setStatus(err?.code === 'invalid_link' ? 'invalid' : 'failed'));
  };

  const statsHref = `${localizePath('/stats')}#settings`;
  let title = '';
  let body = '';
  let action: React.ReactNode = null;
  if ((status === 'ready' || status === 'working') && link) {
    const confirm = link.action === 'confirm';
    title = t(confirm ? 'alertsConfirmPromptTitle' : 'alertsUnsubscribePromptTitle');
    body = t(confirm ? 'alertsConfirmPromptBody' : 'alertsUnsubscribePromptBody');
    action = (
      <button type="button" className="btn btn-primary" disabled={status === 'working'} onClick={act}>
        {t(confirm ? 'alertsConfirmButton' : 'alertsUnsubscribeButton')}
      </button>
    );
  } else if (status === 'done' && link?.action === 'confirm') {
    title = t('alertsConfirmedTitle');
    body = t('alertsConfirmedBody', [account]);
    action = <a className="btn btn-primary" href={statsHref}>{t('alertsOpenSettings')}</a>;
  } else if (status === 'done') {
    title = t('alertsUnsubscribedTitle');
    body = account ? t('alertsUnsubscribedBody', [account]) : t('alertsUnsubscribedBodyAny');
    action = <a className="btn btn-primary" href={statsHref}>{t('alertsTurnBackOn')}</a>;
  } else if (status === 'invalid') {
    title = t('alertsInvalidTitle');
    body = t('alertsInvalidBody');
    action = <a className="btn btn-primary" href={statsHref}>{t('alertsOpenSettings')}</a>;
  } else {
    title = t('alertsFailedTitle');
    body = t('settingsError');
  }

  return (
    <div id="page-alerts" className="alerts-page">
      <div className="page-header">
        <h1>{t('alertsTitle')}</h1>
      </div>
      <div className="card" aria-busy={status === 'working'} role="status">
        <h2>{title}</h2>
        {body && <p>{body}</p>}
        {action}
      </div>
    </div>
  );
}
