import { createRoot } from 'react-dom/client';
import { App } from './App';
import { applyTheme, getStoredTheme } from './lib/theme';
import './styles.css';

applyTheme(getStoredTheme());

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('missing root element');
}

createRoot(rootEl).render(<App />);
