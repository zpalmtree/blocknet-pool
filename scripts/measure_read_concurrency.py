#!/usr/bin/env python3
"""Bounded, read-only API latency probe. Stops on failures or >1s responses."""

import argparse
import concurrent.futures
import datetime
import json
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:24783')
    parser.add_argument('--config', help='Local config supplying api_key; never printed')
    parser.add_argument('--path', action='append')
    parser.add_argument('--concurrency', default='1,2,4,8')
    parser.add_argument('--rounds', type=int, default=4)
    parser.add_argument('--pause', type=float, default=3)
    args = parser.parse_args()
    levels = [int(value) for value in args.concurrency.split(',')]
    if not levels or any(n < 1 or n > 8 for n in levels) or not 1 <= args.rounds <= 8:
        parser.error('concurrency must be 1..8 and rounds 1..8')
    if args.pause < 1:
        parser.error('pause must be at least one second')
    paths = args.path or ['/api/luck?limit=200', '/api/blocks?limit=200', '/api/stats', '/api/status']
    if len(paths) > 16 or any(not p.startswith('/api/') or '://' in p for p in paths):
        parser.error('supply at most 16 /api/ paths')
    headers = {}
    if args.config:
        with open(args.config) as config_file:
            headers['x-api-key'] = json.load(config_file)['api_key']

    def read(path):
        started = time.perf_counter()
        status, error = None, None
        try:
            request = urllib.request.Request(args.base_url.rstrip('/') + path, headers=headers)
            with urllib.request.urlopen(request, timeout=3) as response:
                response.read()
                status = response.status
        except urllib.error.HTTPError as exc:
            status = exc.code
        except Exception as exc:
            error = type(exc).__name__
        return {'path': path, 'status': status, 'error': error,
                'milliseconds': round((time.perf_counter() - started) * 1000, 2)}

    report = {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'base_url': args.base_url, 'stages': [], 'stopped_early': False}
    for concurrency in levels:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            results = list(executor.map(read, paths * args.rounds))
        times = sorted(row['milliseconds'] for row in results)
        report['stages'].append({'concurrency': concurrency, 'count': len(results),
                                 'max_ms': max(times), 'results': results})
        if any(row['status'] != 200 or row['milliseconds'] > 1000 for row in results):
            report['stopped_early'] = True
            break
        time.sleep(args.pause)
    print(json.dumps(report, indent=2))
    return int(report['stopped_early'])


if __name__ == '__main__':
    raise SystemExit(main())
