"""Report runner-owned workload resources that outlive the orchestrator suites.

k8s-runner's E2E leaves orchestrator-started sandboxes to this E2E's idle
lifecycle, so wait for that collection and report what remains with identity
labels and age. Report-only: Ziti identities are compared through the
before/after collector snapshots, and only metadata is ever printed.
"""
import json
import subprocess
import time

NAMESPACE = 'agyn-workloads'
SELECTOR = 'app.kubernetes.io/managed-by=k8s-runner'
KINDS = 'pods,secrets,configmaps,pvc'
IDENTITY = ('agyn.io/workload-id', 'agent-id', 'agent-instance-id', 'thread-id', 'sandbox-id', 'volume_key')
ATTEMPTS, INTERVAL = 36, 5


def remaining():
    out = subprocess.run(['kubectl', 'get', KINDS, '-n', NAMESPACE, '-l', SELECTOR, '-o', 'json'],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)['items']


def describe(item):
    meta = item['metadata']
    labels = meta.get('labels') or {}
    ids = ' '.join(f'{key}={labels[key]}' for key in IDENTITY if labels.get(key))
    phase = (item.get('status') or {}).get('phase')
    state = f' phase={phase}' if phase else ''
    deleting = ' deleting' if meta.get('deletionTimestamp') else ''
    return f"{item['kind'].lower()}/{meta['name']} created={meta['creationTimestamp']}{state}{deleting} {ids}".rstrip()


def main():
    started = time.monotonic()
    for attempt in range(ATTEMPTS):
        items = remaining()
        if not [item for item in items if item['kind'] != 'PersistentVolumeClaim']:
            break
        if attempt + 1 < ATTEMPTS:
            time.sleep(INTERVAL)
    waited = int(time.monotonic() - started)
    if not items:
        print(f'No runner-owned workload resources remain after {waited}s.')
        return
    counts = {}
    for item in items:
        counts[item['kind']] = counts.get(item['kind'], 0) + 1
    summary = ', '.join(f'{count} {kind}' for kind, count in sorted(counts.items()))
    print(f'::warning::runner-owned workload resources remain after {waited}s: {summary}')
    for item in sorted(items, key=lambda item: (item['kind'], item['metadata']['creationTimestamp'])):
        print(describe(item))


if __name__ == '__main__':
    main()
