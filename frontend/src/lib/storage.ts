export const LAST_MINER_LOOKUP_KEY = 'pool_last_miner_lookup_address';
export const API_KEY_STORAGE_KEY = 'pool_api_key';

export interface SavedDestination { address: string; memo: string }
const KEY = 'pool_payout_destination_v1';
export function readDestination(): SavedDestination {
  try {
    const saved = JSON.parse(localStorage.getItem(KEY) || 'null');
    if (saved?.version === 1 && typeof saved.address === 'string' && typeof saved.memo === 'string') {
      return { address: saved.address, memo: saved.memo };
    }
    return { address: localStorage.getItem(LAST_MINER_LOOKUP_KEY) || '', memo: '' };
  } catch { return { address: '', memo: '' }; }
}
export function rememberDestination(destination: SavedDestination | null) {
  try {
    if (destination) {
      localStorage.setItem(KEY, JSON.stringify({ version: 1, ...destination }));
      // Old UI versions must not automatically look up a shared address.
      if (!destination.memo) localStorage.setItem(LAST_MINER_LOOKUP_KEY, destination.address);
      else localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
    } else {
      localStorage.removeItem(KEY);
      localStorage.removeItem(LAST_MINER_LOOKUP_KEY);
    }
  } catch { /* Lookup still works when storage is unavailable. */ }
}
