export type ThemeMode = 'light' | 'dark';

export const THEME_STORAGE_KEY = 'pool_theme';

export function normalizeTheme(value: string | null | undefined): ThemeMode {
  return value === 'dark' ? 'dark' : 'light';
}

export function getStoredTheme(): ThemeMode {
  try {
    return normalizeTheme(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch {
    return 'light';
  }
}

export function setStoredTheme(theme: ThemeMode) {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    // Ignore storage failures and keep the current in-memory theme.
  }
}

export function applyTheme(theme: ThemeMode) {
  if (typeof document === 'undefined') return;
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
}
