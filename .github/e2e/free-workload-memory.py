"""Free node memory for the failed-workload retention step.

The default-profile suites run first and can leave agent workload Pods behind
(512Mi requests each) on this single 12Gi disposable VM. The redeployed
orchestrator (256Mi) and the retention test's own workload (512Mi) then cannot
be scheduled. This deletes the leftover runner-managed workload Pods -- Pods
only, never volumes or secrets -- and waits until enough memory is unrequested.
Disposable CI VM only; only metadata is printed.
"""
import json
import subprocess
import sys
import time

NAMESPACE = 'agyn-workloads'
SELECTOR = 'app.kubernetes.io/managed-by=k8s-runner'
REQUIRED_MI = int(sys.argv[1]) if len(sys.argv) > 1 else 1024
ATTEMPTS, INTERVAL = 60, 5

UNITS = {'Ki': 1 / 1024, 'Mi': 1, 'Gi': 1024, 'Ti': 1024 * 1024,
         'k': 1e3 / 2**20, 'M': 1e6 / 2**20, 'G': 1e9 / 2**20, 'T': 1e12 / 2**20}


def mebibytes(quantity):
    for unit in sorted(UNITS, key=len, reverse=True):
        if quantity.endswith(unit):
            return float(quantity[:-len(unit)]) * UNITS[unit]
    return float(quantity) / 2**20


def kubectl_json(*args):
    return json.loads(subprocess.run(['kubectl', *args, '-o', 'json'], check=True, capture_output=True, text=True).stdout)


def memory_request(pod):
    def request(container):
        return mebibytes(((container.get('resources') or {}).get('requests') or {}).get('memory', '0'))
    spec = pod['spec']
    inits = spec.get('initContainers') or []
    sidecars = sum(request(c) for c in inits if c.get('restartPolicy') == 'Always')
    running = sum(request(c) for c in spec.get('containers') or []) + sidecars
    plain = [request(c) for c in inits if c.get('restartPolicy') != 'Always']
    overhead = mebibytes((spec.get('overhead') or {}).get('memory', '0'))
    return max([running, *plain]) + overhead


def unrequested():
    node = kubectl_json('get', 'nodes')['items'][0]
    allocatable = mebibytes(node['status']['allocatable']['memory'])
    used = sum(memory_request(pod) for pod in kubectl_json('get', 'pods', '-A')['items']
               if pod['spec'].get('nodeName') == node['metadata']['name']
               and (pod.get('status') or {}).get('phase') not in ('Succeeded', 'Failed'))
    return allocatable - used, allocatable


def main():
    leftovers = kubectl_json('get', 'pods', '-n', NAMESPACE, '-l', SELECTOR)['items']
    for pod in leftovers:
        meta = pod['metadata']
        print(f"deleting leftover workload pod {meta['name']} created={meta['creationTimestamp']}")
    if leftovers:
        subprocess.run(['kubectl', 'delete', 'pods', '-n', NAMESPACE, '-l', SELECTOR, '--wait=true', '--timeout=180s'], check=False)
    for attempt in range(ATTEMPTS):
        free, allocatable = unrequested()
        if free >= REQUIRED_MI:
            print(f'{free:.0f}Mi of {allocatable:.0f}Mi node memory unrequested (need {REQUIRED_MI}Mi)')
            return
        if attempt + 1 < ATTEMPTS:
            time.sleep(INTERVAL)
    print(f'::error::only {free:.0f}Mi of {allocatable:.0f}Mi node memory unrequested after {ATTEMPTS * INTERVAL}s; need {REQUIRED_MI}Mi')
    subprocess.run(['kubectl', 'describe', 'nodes'], check=False)
    sys.exit(1)


if __name__ == '__main__':
    main()
