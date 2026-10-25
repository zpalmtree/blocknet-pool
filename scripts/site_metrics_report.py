#!/usr/bin/env python3
"""Read private website aggregates. API key comes from BNTPOOL_API_KEY, never argv.

For a saved report: site_metrics_report.py --input report.json
For localhost through an SSH tunnel: BNTPOOL_API_KEY=... site_metrics_report.py --url http://127.0.0.1:24783
"""
import argparse
import json
import os
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def report(data):
    rows = defaultdict(lambda: defaultdict(int))
    for key, count in data['counts'].items():
        day, site, article, locale, action = key.split('|')
        rows[(site, article, locale)][action] += count
    print(f"Ready: {data['ready']}; retention: {data['retention_days']} daily buckets")
    if data.get('last_persisted_at'):
        when = datetime.fromtimestamp(data['last_persisted_at'], timezone.utc).astimezone(ZoneInfo('America/New_York'))
        print('Last persisted:', when.strftime('%Y-%m-%d %I:%M:%S %p %Z'))
    print('Counts are page loads and click intent, not unique people or completed conversions.')
    print('site\tarticle\tlanguage\tviews\tdownload clicks\tsetup clicks')
    for (site, article, locale), actions in sorted(rows.items()):
        print(site, article, locale, actions['view'], actions['download'], actions['pool_setup'], sep='\t')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', help='Saved JSON file, or - for stdin')
    parser.add_argument('--url', default='https://bntpool.com', help='API origin')
    args = parser.parse_args()
    if args.input:
        with (sys.stdin if args.input == '-' else open(args.input)) as stream:
            data = json.load(stream)
    else:
        key = os.environ.get('BNTPOOL_API_KEY')
        if not key:
            parser.error('Set BNTPOOL_API_KEY or use --input; never pass API keys on the command line.')
        url = args.url.rstrip('/')
        if not (url.startswith('https://') or url in ('http://127.0.0.1:24783', 'http://localhost:24783')):
            parser.error('Use HTTPS or a localhost SSH tunnel.')
        request = urllib.request.Request(url + '/api/admin/site-metrics', headers={'x-api-key': key})
        with urllib.request.urlopen(request, timeout=15) as response:
            data = json.load(response)
    report(data)


if __name__ == '__main__':
    main()
