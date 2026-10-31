import type { InfoResponse } from '../types';
export function knownExchange(info: InfoResponse | null, address: string) {
  return info?.shared_exchange_addresses?.find(entry => entry.address === address.trim());
}
export function validMemo(memo: string) {
  return new TextEncoder().encode(memo).length <= 124 && !/[\u0000-\u001f\u007f-\u009f]/u.test(memo);
}
export function shellQuote(value: string, powershell: boolean) {
  return "'" + value.split("'").join(powershell ? "''" : "'\"'\"'") + "'";
}
