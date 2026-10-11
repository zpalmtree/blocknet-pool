#!/usr/bin/env python3
"""Audit/rebuild finalized checkpoints using agreement between both pool daemons.

Run on bntpool, or stream over SSH:
  ssh bntpool 'sudo python3 - --apply' < scripts/repair_checkpoints.py
Without --apply this only audits. No services are restarted.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import urllib.request


NODES = [(18331, Path('/var/lib/blocknet/data')),
         (18332, Path('/var/lib/blocknet-standby/data'))]
FINALITY_DEPTH = 100  # blocknet-core MaxReorgDepth


def fetch(port, directory, path):
    token = (directory / 'api.cookie').read_text().strip()
    request = urllib.request.Request(
        f'http://127.0.0.1:{port}{path}',
        headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    originals = [(directory / 'checkpoints.dat').read_bytes()
                 for _, directory in NODES]
    statuses = [fetch(*node, '/api/status') for node in NODES]
    if any(status['syncing'] for status in statuses):
        raise RuntimeError('refusing to publish from a syncing daemon')
    limit = min(status['chain_height'] for status in statuses) - FINALITY_DEPTH
    if limit < 100:
        raise RuntimeError('no finalized checkpoints available')
    canonical = {}
    for height in range(100, limit + 1, 100):
        blocks = [fetch(*node, f'/api/block/{height}') for node in NODES]
        hashes = [block['hash'].upper() for block in blocks]
        if hashes[0] != hashes[1] or any(block['height'] != height for block in blocks):
            raise RuntimeError(f'daemons disagree at height {height}')
        if len(bytes.fromhex(hashes[0])) != 32:
            raise RuntimeError(f'invalid hash at height {height}')
        canonical[height] = hashes[0]
    body = ''.join(f'{height}:{value}\n' for height, value in canonical.items()).encode()
    for (_, directory), original in zip(NODES, originals):
        old = dict(line.split(':', 1) for line in original.decode().splitlines()
                   if line.strip() and not line.startswith('#'))
        changed = [int(h) for h, value in old.items()
                   if int(h) in canonical and canonical[int(h)] != value.upper()]
        removed = [int(h) for h in old if int(h) not in canonical]
        print(json.dumps({'directory': str(directory), 'corrected': changed,
                          'removed_unfinalized': removed, 'entries': len(canonical),
                          'sha256': hashlib.sha256(body).hexdigest()}), flush=True)
    # Detect append activity since the audit before replacing any file.
    for (_, directory), original in zip(NODES, originals):
        if (directory / 'checkpoints.dat').read_bytes() != original:
            raise RuntimeError('checkpoint file changed during audit; retry')
    if not args.apply:
        return
    for (_, directory), original in zip(NODES, originals):
        path = directory / 'checkpoints.dat'
        if original == body:
            continue
        stat = path.stat()
        backup = path.with_name('checkpoints.dat.backup-' + hashlib.sha256(original).hexdigest())
        if not backup.exists():
            with backup.open('xb') as output:
                output.write(original)
                output.flush()
                os.fsync(output.fileno())
        fd, temporary = tempfile.mkstemp(prefix='.checkpoints-', dir=directory)
        try:
            with os.fdopen(fd, 'wb') as output:
                os.fchmod(output.fileno(), stat.st_mode & 0o777)
                os.fchown(output.fileno(), stat.st_uid, stat.st_gid)
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            if path.read_bytes() != original:
                raise RuntimeError('checkpoint file changed before replacement; retry')
            os.replace(temporary, path)
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            print(f'Replaced {path}; backup: {backup}', flush=True)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


if __name__ == '__main__':
    main()
