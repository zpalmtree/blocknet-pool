import { t, locale } from '../i18n';
import type { ReactNode } from 'react';

import { MiningType } from '../components/MiningType';
import type { InfoResponse } from '../types';

// Blocknet targets one block every five minutes.
const BLOCK_TARGET_SECONDS = 300;

function number(value: number) {
  return value.toLocaleString(locale, { maximumFractionDigits: 20 });
}

/** Whole hours as hours, anything shorter or uneven as minutes. */
function duration(seconds: number) {
  const hours = seconds % 3600 === 0;
  return new Intl.NumberFormat(locale, { style: 'unit', unit: hours ? 'hour' : 'minute', unitDisplay: 'long' })
    .format(hours ? seconds / 3600 : Math.max(1, Math.round(seconds / 60)));
}

/** Confirmation time rounded to the nearest hour once it is at least an hour. */
function roughDuration(seconds: number) {
  return duration(seconds >= 3600 ? Math.round(seconds / 3600) * 3600 : seconds);
}

function cadence(seconds: number) {
  return seconds === 3600 ? t('infoHourly') : t('infoEvery', [duration(seconds)]);
}

const positive = (value: number | null | undefined): value is number => value != null && Number.isFinite(value) && value > 0;

interface Step { label: string; value: string; note: string }

function steps(info: InfoResponse | null): Step[] {
  const fee = info?.pool_fee_pct;
  const blocks = info?.blocks_before_payout;
  return [
    { label: t('infoStepMine'), note: t('infoStepMineNote'),
      value: positive(info?.pplns_window_seconds) ? duration(info.pplns_window_seconds) : '-' },
    { label: t('infoStepBlock'), note: t('infoStepBlockNote'),
      value: fee != null && Number.isFinite(fee) ? `${number(fee)}%` : '-' },
    { label: t('infoStepConfirm'),
      value: positive(blocks) ? t('m475', [number(blocks)]) : '-',
      note: positive(blocks) ? t('infoStepConfirmNote', [roughDuration(blocks * BLOCK_TARGET_SECONDS)]) : t('infoStepConfirmFallback') },
    { label: t('infoStepBalance'), note: t('infoStepBalanceNote'),
      value: info?.min_payout_amount != null ? t('m473', [number(info.min_payout_amount)]) : '-' },
    { label: t('infoStepPaid'), note: t('infoStepPaidNote'),
      value: positive(info?.payout_interval_seconds) ? cadence(info.payout_interval_seconds) : '-' },
  ];
}

const ADDRESS = <code className="mono inline-code" dir="ltr">ADDRESS</code>;
const SOLO_ADDRESS = <code className="mono inline-code" dir="ltr">solo:ADDRESS</code>;

interface InfoPageProps {
  poolInfo: InfoResponse | null;
}

export function InfoPage({ poolInfo }: InfoPageProps) {
  const modes: [string, ReactNode, ReactNode][] = [
    [t('infoPaidFrom'), t('infoPoolPaidFrom'), t('infoSoloPaidFrom')],
    [t('m004'), t('infoPoolPayouts'), t('infoSoloPayouts')],
    [t('infoLogin'), ADDRESS, SOLO_ADDRESS],
  ];
  // Exchange deposits carry a memo and have their own, higher minimum.
  const minimum = poolInfo?.min_payout_amount;
  const memoMinimum = poolInfo?.payout_memos_enabled && minimum != null && positive(poolInfo.memo_min_payout_amount)
    && poolInfo.memo_min_payout_amount > minimum ? poolInfo.memo_min_payout_amount : null;

  return (
    <div id="page-info">
      <div className="page-header">
        <span className="page-kicker">{t('infoNav')}</span>
        <h1>{t('infoHeading')}</h1>
      </div>

      <div className="card section" id="payouts">
        <h2 className="start-card-title">{t('infoStepsTitle')}</h2>
        <ol className="info-steps">
          {steps(poolInfo).map((step, index) => (
            <li key={step.label}>
              <span className="start-step-number" aria-hidden="true">{index + 1}</span>
              <span className="info-step-label">{step.label}</span>
              <span className="info-step-value">{step.value}</span>
              <span className="info-step-note">{step.note}</span>
            </li>
          ))}
        </ol>
      </div>

      <div className="card section" id="solo">
        <h2 className="start-card-title">{t('infoSoloTitle')}</h2>
        <p className="info-lead">{t('infoSoloLead')}</p>
        <table className="info-compare">
          <thead><tr><td /><th scope="col"><MiningType /></th><th scope="col"><MiningType solo /></th></tr></thead>
          <tbody>
            {modes.map(([label, pool, solo]) => <tr key={label}><th scope="row">{label}</th><td>{pool}</td><td>{solo}</td></tr>)}
          </tbody>
        </table>
      </div>

      {memoMinimum != null && minimum != null && (
        <div className="card section" id="minimum-payout">
          <h2 className="start-card-title">{t('infoMinTitle')}</h2>
          <p className="info-lead">{t('infoMinLead')}</p>
          <table className="info-compare info-minimums">
            <tbody>
              <tr><th scope="row">{t('infoMinWallet')}</th><td>{t('m473', [number(minimum)])}</td></tr>
              <tr><th scope="row">{t('infoMinExchange')}</th><td>{t('m473', [number(memoMinimum)])}</td></tr>
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
