"""Read the disposable VM's native networking; never patch policy or DNS.

bundle-vm 0.3.0 (ba8f4bed...) supplies CoreDNS rewrites and platform chart
0.73.0 supplies the workload policy. Connectivity probes run under that policy.
"""
import ipaddress
import json
import os
from pathlib import Path
import subprocess


def get(*args):
    return json.loads(subprocess.check_output(['kubectl', 'get', *args, '-o', 'json'], text=True))


services = get('services', '--all-namespaces')['items']


def service(namespace, name, port):
    matches = [s for s in services if s['metadata']['namespace'] == namespace and s['metadata']['name'] == name]
    assert len(matches) == 1, f'missing or ambiguous VM service {namespace}/{name}'
    result = matches[0]
    address = result['spec']['clusterIP']
    assert ipaddress.ip_address(address).version == 4
    assert any(p['port'] == port for p in result['spec']['ports']), f'{namespace}/{name} does not expose {port}'
    slices = get('endpointslices', '-n', namespace, '-l', f'kubernetes.io/service-name={name}')['items']
    addresses = sorted({a for s in slices for e in s.get('endpoints', [])
                        if e.get('conditions', {}).get('ready') is not False for a in e['addresses']})
    assert addresses, f'{namespace}/{name} has no ready endpoints'
    print(f'{namespace}/{name}: service={address} endpoints={addresses}')
    return address, addresses


dns_ip, dns_endpoints = service('kube-system', 'kube-dns', 53)
service('ziti', 'ziti-controller-client', 2496)
service('ziti', 'ziti-router-edge', 2496)
service('istio-gateway', 'istio-ingressgateway', 443)
service('agyn-platform', 'agents', 50051)
service('agyn-platform', 'runners', 50051)
rewrites = '\n'.join(get('configmap', 'coredns-custom', '-n', 'kube-system')['data'].values())
for host, target in [('ziti.agyn.dev', 'ziti-controller-client.ziti.svc.cluster.local'),
                     ('ziti-router.agyn.dev', 'ziti-router-edge.ziti.svc.cluster.local')]:
    assert any(host in line.split() and target in line.split() for line in rewrites.splitlines()), f'missing native DNS rewrite for {host}'
policy = get('networkpolicy', 'agent-workload-egress', '-n', 'agyn-workloads')
assert policy['spec']['podSelector']['matchLabels']['agyn.dev/managed-by'] == 'agents-orchestrator'
assert 'Egress' in policy['spec']['policyTypes']
with Path(os.environ['GITHUB_ENV']).open('a') as env:
    env.write(f'WORKLOAD_TEST_DNS_SERVICE_IP={dns_ip}\nWORKLOAD_TEST_DNS_ENDPOINT_IP={dns_endpoints[0]}\n')
