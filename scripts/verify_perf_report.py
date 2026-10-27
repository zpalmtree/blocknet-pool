#!/usr/bin/env python3
"""Share-path latency and verifier capacity report from the monitor's Prometheus series.

Read-only. Prints p50/p95/p99 per lane for submit acks, queue waits and Argon2
hash time, plus overload-mode time, audit coverage and PoW arena backing.

Last hour on the pool host (runs this script there over SSH, so Prometheus stays
localhost-only):
    verify_perf_report.py --host bntpool
Before/after a deploy, same window length:
    verify_perf_report.py --host bntpool --window 2h --end 2026-10-28T14:00Z --compare-end 2026-10-27T14:00Z
Machine-readable:
    verify_perf_report.py --host bntpool --json
"""
import argparse
import json
import math
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

PREFIX = 'blocknet_pool_monitor_'
HISTOGRAMS = ['submit_ack', 'submit_queue_wait', 'validation_queue_wait', 'validation_hash']
QUANTILES = [0.5, 0.95, 0.99]
RATES = [
    ('submits/s', 'stratum_submits_received_total'),
    ('hot accepts/s', 'validation_hot_accepts_total'),
    ('sync verifies/s', 'validation_sync_full_verifies_total'),
    ('audits enqueued/s', 'validation_audit_enqueued_total'),
    ('audits verified/s', 'validation_audit_verified_total'),
    ('audits rejected/s', 'validation_audit_rejected_total'),
    ('audits deferred/s', 'validation_audit_deferred_total'),
    ('probation enqueued/s', 'validation_probation_enqueued_total'),
    ('fraud detections/s', 'validation_fraud_detections_total'),
]
INCREASES = [
    ('overload transitions', 'validation_overload_transitions_total'),
    ('candidate blocks accepted', 'candidate_block_accepted_total'),
    ('candidate stale blocks', 'candidate_stale_block_total'),
    ('candidate enqueue rejected', 'candidate_enqueue_rejected_total'),
    ('arena cache hits', 'pow_arena_cache_hits_total'),
    ('arena discarded', 'pow_arena_discarded_total'),
]


def parse_time(value):
    if value in (None, 'now'):
        return datetime.now(timezone.utc).timestamp()
    try:
        return float(value)
    except ValueError:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


class Prometheus:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')

    def query(self, expr, at):
        url = f'{self.base_url}/api/v1/query?' + urllib.parse.urlencode({'query': expr, 'time': at})
        with urllib.request.urlopen(url, timeout=30) as response:
            body = json.load(response)
        if body.get('status') != 'success':
            raise RuntimeError(f'query failed: {expr}: {body}')
        return body['data']['result']

    def by_label(self, expr, at, label):
        out = {}
        for row in self.query(expr, at):
            value = float(row['value'][1])
            out[row['metric'].get(label, '')] = None if math.isnan(value) else value
        return out

    def scalar(self, expr, at):
        rows = self.query(expr, at)
        if not rows:
            return None
        value = float(rows[0]['value'][1])
        return None if math.isnan(value) else value


def collect(prom, window, at):
    report = {'end': at, 'window': window, 'histograms': {}, 'overload': {}, 'rates': {}, 'increases': {}}
    for family in HISTOGRAMS:
        name = f'{PREFIX}{family}_millis'
        lanes = {}
        per_second = prom.by_label(f'sum by (lane) (rate({name}_count[{window}]))', at, 'lane')
        mean = prom.by_label(
            f'sum by (lane) (rate({name}_sum[{window}])) / sum by (lane) (rate({name}_count[{window}]))',
            at, 'lane')
        for lane, rate in per_second.items():
            lanes[lane] = {'per_second': rate, 'mean_ms': mean.get(lane)}
        for quantile in QUANTILES:
            values = prom.by_label(
                f'histogram_quantile({quantile}, sum by (lane, le) (rate({name}_bucket[{window}])))',
                at, 'lane')
            for lane, value in values.items():
                lanes.setdefault(lane, {})[f'p{round(quantile * 100)}_ms'] = value
        report['histograms'][family] = lanes
    seconds = prom.by_label(
        f'sum by (mode) (increase({PREFIX}validation_overload_seconds_total[{window}]))', at, 'mode')
    total = sum(value or 0 for value in seconds.values())
    report['overload'] = {mode: (value or 0) / total for mode, value in seconds.items()} if total else {}
    for label, metric in RATES:
        report['rates'][label] = prom.scalar(f'sum(rate({PREFIX}{metric}[{window}]))', at)
    for label, metric in INCREASES:
        report['increases'][label] = prom.scalar(f'sum(increase({PREFIX}{metric}[{window}]))', at)
    report['arena_allocations'] = prom.by_label(
        f'sum by (backing) (increase({PREFIX}pow_arena_allocations_total[{window}]))', at, 'backing')
    report['unprocessed'] = prom.by_label(
        f'sum by (reason) (increase({PREFIX}submit_unprocessed_total[{window}]))', at, 'reason')
    report['regular_queue_depth'] = {
        'avg': prom.scalar(f'avg_over_time({PREFIX}validation_regular_queue_depth[{window}])', at),
        'max': prom.scalar(f'max_over_time({PREFIX}validation_regular_queue_depth[{window}])', at),
    }
    report['connected_workers'] = prom.scalar(f'avg_over_time({PREFIX}connected_workers[{window}])', at)
    return report


def fmt(value, digits=0):
    if value is None:
        return '-'
    return f'{value:,.{digits}f}'


def delta(before, after):
    if before in (None, 0) or after is None:
        return ''
    return f' ({(after - before) / before * 100:+.1f}%)'


def print_report(report, baseline=None):
    end = datetime.fromtimestamp(report['end'], timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    print(f"Window {report['window']} ending {end}; avg workers {fmt(report['connected_workers'])}")
    if baseline:
        base_end = datetime.fromtimestamp(baseline['end'], timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        print(f'Deltas vs the same window ending {base_end}')
    for family, lanes in report['histograms'].items():
        print(f'\n{family} (ms)')
        print(f"  {'lane':<20}{'per s':>9}{'mean':>9}{'p50':>9}{'p95':>9}{'p99':>9}")
        for lane, row in sorted(lanes.items()):
            base = ((baseline or {}).get('histograms', {}).get(family, {}).get(lane)) or {}
            print(f"  {lane:<20}{fmt(row.get('per_second'), 2):>9}{fmt(row.get('mean_ms')):>9}"
                  f"{fmt(row.get('p50_ms')):>9}{fmt(row.get('p95_ms')):>9}{fmt(row.get('p99_ms')):>9}"
                  f"{delta(base.get('p99_ms'), row.get('p99_ms'))}")
    if report['overload']:
        print('\noverload time: ' + ', '.join(
            f'{mode} {share * 100:.1f}%' for mode, share in sorted(report['overload'].items())))
    print('\nrates')
    for label, value in report['rates'].items():
        base = (baseline or {}).get('rates', {}).get(label)
        print(f'  {label:<28}{fmt(value, 3):>12}{delta(base, value)}')
    print('\ncounts over window')
    for label, value in report['increases'].items():
        print(f'  {label:<28}{fmt(value):>12}')
    for label, values in [('arena allocations', report['arena_allocations']),
                          ('unprocessed submits', report['unprocessed'])]:
        if values:
            print(f'  {label:<28}' + ', '.join(f'{key} {fmt(value)}' for key, value in sorted(values.items())))
    depth = report['regular_queue_depth']
    print(f"  {'regular queue depth':<28}avg {fmt(depth['avg'])}, max {fmt(depth['max'])}")


def run_remote(args):
    forwarded = [arg for arg in sys.argv[1:]]
    host_index = forwarded.index('--host')
    del forwarded[host_index:host_index + 2]
    with open(__file__, 'rb') as source:
        result = subprocess.run(['ssh', '-o', 'BatchMode=yes', args.host, 'python3', '-', *forwarded],
                                stdin=source)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--host', help='SSH host to run on (e.g. bntpool)')
    parser.add_argument('--prometheus', default='http://127.0.0.1:9090')
    parser.add_argument('--window', default='1h', help='Prometheus range, e.g. 30m, 2h')
    parser.add_argument('--end', default='now', help='Window end: now, unix seconds or ISO-8601')
    parser.add_argument('--compare-end', help='End of a baseline window of the same length')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    if args.host:
        return run_remote(args)
    prom = Prometheus(args.prometheus)
    report = collect(prom, args.window, parse_time(args.end))
    baseline = collect(prom, args.window, parse_time(args.compare_end)) if args.compare_end else None
    if args.json:
        json.dump({'report': report, 'baseline': baseline}, sys.stdout, indent=2)
        print()
    else:
        print_report(report, baseline)
    return 0


if __name__ == '__main__':
    sys.exit(main())
