import { createRoot } from 'react-dom/client';
import { App } from './App';
import { initializeLanguage } from './i18n';
import { applyTheme, getStoredTheme } from './lib/theme';
import './styles.css';

applyTheme(getStoredTheme());

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('missing root element');
}

// Keep the server's useful page content visible until translations are ready.
initializeLanguage().then(() => {
  createRoot(rootEl).render(<App />);
}).catch(() => {
  const notice = document.createElement('p');
  notice.className = 'page-intro';
  notice.setAttribute('role', 'alert');
  notice.lang = 'en';
  notice.dir = 'ltr';
  notice.append('This language could not load. ');
  const retry = document.createElement('a');
  retry.href = window.location.href;
  retry.textContent = 'Try again';
  const english = document.createElement('a');
  const url = new URL(window.location.href);
  url.searchParams.delete('lang');
  english.href = url.href;
  english.textContent = 'Continue in English';
  notice.append(retry, ' or ', english, '.');
  const content = rootEl.querySelector('main');
  if (content) content.prepend(notice);
  else {
    const fallback = document.createElement('main');
    fallback.className = 'container';
    fallback.append(notice);
    rootEl.append(fallback);
  }
});
