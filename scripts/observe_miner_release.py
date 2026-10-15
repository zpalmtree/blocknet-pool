#!/usr/bin/env python3
"""Record read-only release adoption and API health; never send notifications."""

import argparse
import collections
import datetime
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.request


def utc(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc).isoformat()


def command(args):
    return subprocess.check_output(args, text=True, timeout=10, stderr=subprocess.PIPE)


def journal_counts(text, start_us, end_us):
    counts = collections.Counter()
    for line in text.splitlines():
        entry = json.loads(line)
        at = int(entry['__REALTIME_TIMESTAMP'])
        match = re.search(r'route="([^"]+)" status=(429|503)\b', entry.get('MESSAGE', ''))
        if start_us <= at < end_us and match:
            counts[f'{match[1]}:{match[2]}'] += 1
    return dict(counts)


def collect(config_path, database, start_us, end_us):
    sample = {'at': utc(end_us / 1e6), 'interval_start': utc(start_us / 1e6), 'errors': {}}

    def attempt(name, work):
        try:
            sample[name] = work()
        except Exception as error:
            # Never write config credentials or command output into the report.
            sample['errors'][name] = type(error).__name__

    def api():
        with open(config_path) as handle:
            key = json.load(handle)['api_key']
        request = urllib.request.Request('http://127.0.0.1:24783/api/admin/perf', headers={'x-api-key': key})
        with urllib.request.urlopen(request, timeout=5) as response:
            perf = json.load(response)
        return {name: {k: stats.get(k) for k in ['count', 'error_count', 'max_millis', 'duration', 'success_duration']}
                for name, stats in perf['routes'].items()}

    def versions():
        # now() is stable, so PostgreSQL can use the created_at index. A volatile
        # clock_timestamp() predicate would force a scan of the whole share table.
        sql = """SET statement_timeout='2s'; SET default_transaction_read_only=on;
        SELECT COALESCE(json_agg(row_to_json(v)), '[]'::json) FROM (
          SELECT miner_version,COUNT(*) AS active_workers FROM (
            SELECT DISTINCT ON (miner,worker) miner_version FROM shares
            WHERE created_at >= EXTRACT(EPOCH FROM now())::bigint-300
            ORDER BY miner,worker,created_at DESC,id DESC
          ) latest GROUP BY miner_version ORDER BY COUNT(*) DESC
        ) v;"""
        return json.loads(command(['runuser', '-u', 'postgres', '--', 'psql', '-X', '-q', '-t', '-A',
                                   '-d', database, '-v', 'ON_ERROR_STOP=1', '-c', sql]))

    attempt('versions', versions)
    attempt('api_routes', api)
    attempt('http_rejections', lambda: journal_counts(command([
        'journalctl', '-u', 'blocknet-pool-api', '--no-pager', '-o', 'json',
        '--since', f'@{start_us // 1000000}', '--until', f'@{math.ceil(end_us / 1e6)}',
        '--grep', 'status=(429|503)',
    ]), start_us, end_us))
    attempt('api_started_at', lambda: command([
        'systemctl', 'show', 'blocknet-pool-api', '--property=ExecMainStartTimestamp', '--value',
    ]).strip())
    return sample


def update_summary(summary, sample, now, version, max_wait, observe_seconds):
    summary.setdefault('started_at_epoch', now)
    summary.setdefault('first_adoption_epoch', None)
    summary.setdefault('samples', 0)
    summary.setdefault('http_rejections', {})
    summary.setdefault('max_observed_success_p99_ms', {})
    summary.setdefault('samples_with_errors', 0)
    summary['version'] = version
    summary['samples'] += 1
    summary['last_sample_at'] = sample['at']
    summary['samples_with_errors'] += bool(sample['errors'])
    versions = sample.get('versions')
    if versions is not None:
        summary['latest_versions'] = versions
        target = re.compile(r'(?<![0-9])v?' + re.escape(version) + r'(?![0-9.])')
        adopted = sum(row['active_workers'] for row in versions if target.search(row['miner_version'] or ''))
        summary['latest_target_workers'] = adopted
        summary['latest_total_workers'] = sum(row['active_workers'] for row in versions)
        if adopted and summary['first_adoption_epoch'] is None:
            summary['first_adoption_epoch'] = now
    for key, count in sample.get('http_rejections', {}).items():
        summary['http_rejections'][key] = summary['http_rejections'].get(key, 0) + count
    for route, stats in sample.get('api_routes', {}).items():
        p99 = (stats.get('success_duration') or {}).get('p99_millis')
        if p99 is not None:
            previous = summary['max_observed_success_p99_ms'].get(route, 0)
            summary['max_observed_success_p99_ms'][route] = max(previous, p99)
    first = summary['first_adoption_epoch']
    if first is not None:
        summary['status'] = 'complete' if now >= first + observe_seconds else 'observing_adoption'
        summary['observation_ends_at'] = utc(first + observe_seconds)
    else:
        summary['status'] = 'no_adoption_before_deadline' if now >= summary['started_at_epoch'] + max_wait else 'awaiting_adoption'
        summary['adoption_deadline'] = utc(summary['started_at_epoch'] + max_wait)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--config', default='/etc/blocknet/pool/config.json')
    parser.add_argument('--database', default='blocknet_pool')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'\d+\.\d+\.\d+', args.version):
        parser.error('version must be X.Y.Z')
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=True)
    state_path = args.output / 'summary.json'
    summary = json.loads(state_path.read_text()) if state_path.exists() else {}
    if summary.get('version', args.version) != args.version:
        parser.error('output directory belongs to another version')
    while summary.get('status') not in ['complete', 'no_adoption_before_deadline']:
        end_us = time.time_ns() // 1000
        start_us = summary.get('last_end_us', end_us - 300_000_000)
        sample = collect(args.config, args.database, start_us, end_us)
        summary = update_summary(summary, sample, end_us / 1e6, args.version, 7 * 86400, 86400)
        summary['last_end_us'] = end_us
        with (args.output / 'samples.jsonl').open('a') as handle:
            handle.write(json.dumps(sample) + '\n')
        temporary = state_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(summary, indent=2) + '\n')
        temporary.replace(state_path)
        print(json.dumps({'at': sample['at'], 'status': summary['status'], 'errors': sample['errors']}), flush=True)
        if args.once or summary['status'] in ['complete', 'no_adoption_before_deadline']:
            break
        time.sleep(300)


if __name__ == '__main__':
    main()
