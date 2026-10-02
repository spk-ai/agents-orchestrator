"""Explicit-proxy E2E leg: restricted admission and task network negatives.

Disposable VM only. Commands:
  prepare  label agyn-workloads restricted (enforce/warn/audit), prove a
           NET_ADMIN Pod is refused by admission, and replace the chart's
           public-egress policy with the overlay-only task policy (DNS and the
           Ziti controller/router only; no ingress).
  watch    wait for a live orchestrator-assembled task Pod and run the shape,
           privilege and network checks against it; results go to a JSON
           file. Runs in the background while the focused tests drive real
           agent turns (TestRestrictedTaskPodAnswersAndHolds keeps one Pod
           alive for it).

Whether the VM's CNI enforces NetworkPolicy is measured, not assumed: a
canary Pod under the task policy tries a public address. Where policy is not
enforced, the checks that only policy can satisfy (raw TCP, public DNS,
cross-Pod ingress) are reported as skipped with that evidence; everything the
Pod shape and the proxy themselves guarantee is still required.
  verify   wait for the watcher's result, print it and fail on any failure.

No secret, token or environment value of a workload is printed: the Pod
shape check reads only the variable names and fixed platform values below.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

NAMESPACE = 'agyn-workloads'
PROBE_NAMESPACE = 'default'
PROBE_POD = 'restricted-network-probe'
RESULT = Path(os.environ.get('RUNNER_TEMP', '/tmp')) / 'restricted-network-result.json'
PROXY = '127.0.0.1:18080'
RESTRICTED_LABELS = {
    'pod-security.kubernetes.io/enforce': 'restricted',
    'pod-security.kubernetes.io/enforce-version': 'latest',
    'pod-security.kubernetes.io/warn': 'restricted',
    'pod-security.kubernetes.io/audit': 'restricted',
}
CANARY_POD = 'restricted-network-canary'
POLICY_DEPENDENT = 'SKIPPED: this VM does not enforce NetworkPolicy (see the prepare step canary)'
PROXY_ENV = ('HTTP_PROXY', 'http_proxy', 'HTTPS_PROXY', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy')


def kubectl(*args, stdin=None, check=True, timeout=120):
    result = subprocess.run(['kubectl', *args], input=stdin, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f'kubectl {" ".join(args[:3])} failed: {result.stderr.strip()}')
    return result


def get(*args):
    return json.loads(kubectl('get', *args, '-o', 'json').stdout)


def pod_manifest(name, namespace, capabilities_add=None, restricted=True):
    container = {'name': 'probe', 'image': 'busybox:1.37.0', 'command': ['sleep', '5400']}
    spec = {'restartPolicy': 'Never', 'containers': [container]}
    if restricted:
        spec['securityContext'] = {'runAsNonRoot': True, 'runAsUser': 65534, 'runAsGroup': 65534,
                                   'seccompProfile': {'type': 'RuntimeDefault'}}
        container['securityContext'] = {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}}
    if capabilities_add:
        container.setdefault('securityContext', {})['capabilities'] = {'add': capabilities_add}
    return json.dumps({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': namespace}, 'spec': spec})


def prepare():
    kubectl('label', 'namespace', NAMESPACE, '--overwrite', *[f'{k}={v}' for k, v in RESTRICTED_LABELS.items()])
    labels = get('namespace', NAMESPACE)['metadata'].get('labels', {})
    assert all(labels.get(k) == v for k, v in RESTRICTED_LABELS.items()), f'restricted labels missing: {labels}'
    version = json.loads(kubectl('version', '-o', 'json').stdout)
    print(f'agyn-workloads enforces restricted; server {version.get("serverVersion", {}).get("gitVersion")}')
    denied = kubectl('create', '--dry-run=server', '-f', '-', stdin=pod_manifest('net-admin-probe', NAMESPACE, ['NET_ADMIN'], restricted=False), check=False)
    assert denied.returncode != 0 and 'PodSecurity' in denied.stderr and 'restricted' in denied.stderr, \
        f'a NET_ADMIN root Pod was not refused by restricted admission: {denied.stderr.strip()}'
    print('admission refuses a NET_ADMIN Pod:', denied.stderr.strip().splitlines()[-1][:300])
    allowed = kubectl('create', '--dry-run=server', '-f', '-', stdin=pod_manifest('restricted-probe', NAMESPACE), check=False)
    assert allowed.returncode == 0, f'a restricted-compliant Pod was refused: {allowed.stderr.strip()}'

    services = {}
    for name in ('ziti-controller-client', 'ziti-router-edge'):
        service = get('service', name, '-n', 'ziti')
        port = next(p for p in service['spec']['ports'] if p['port'] == 2496)
        target = port.get('targetPort', port['port'])
        selector = service['spec'].get('selector') or {}
        assert selector, f'ziti/{name} has no selector to scope the policy to'
        services[name] = (selector, target)
        print(f'ziti/{name}: selector={selector} targetPort={target}')
    egress = [{
        'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}},
                'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}}}],
        'ports': [{'protocol': 'UDP', 'port': 53}, {'protocol': 'TCP', 'port': 53}],
    }]
    for selector, target in services.values():
        egress.append({'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'ziti'}},
                               'podSelector': {'matchLabels': selector}}],
                       'ports': [{'protocol': 'TCP', 'port': target}]})
    policy = {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
              'metadata': {'name': 'task-overlay-egress', 'namespace': NAMESPACE},
              'spec': {'podSelector': {}, 'policyTypes': ['Ingress', 'Egress'], 'egress': egress}}
    # NetworkPolicies are additive: the chart's public-egress policy must go
    # for the overlay-only one to be the effective rule.
    kubectl('delete', 'networkpolicy', 'agent-workload-egress', '-n', NAMESPACE, '--ignore-not-found')
    kubectl('apply', '-f', '-', stdin=json.dumps(policy))
    remaining = [p['metadata']['name'] for p in get('networkpolicy', '-n', NAMESPACE)['items']]
    print(f'agyn-workloads NetworkPolicies: {remaining}')
    assert 'task-overlay-egress' in remaining and 'agent-workload-egress' not in remaining

    # The probe Pod outside the task namespace is created now, so the watcher
    # does not spend a live task Pod's time waiting for it.
    for name, namespace in ((PROBE_POD, PROBE_NAMESPACE), (CANARY_POD, NAMESPACE)):
        kubectl('delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true')
        kubectl('apply', '-f', '-', stdin=pod_manifest(name, namespace))
        kubectl('wait', f'pod/{name}', '-n', namespace, '--for=condition=Ready', '--timeout=180s')
    enforced = False
    for _ in range(6):
        escaped = kubectl('exec', '-n', NAMESPACE, CANARY_POD, '--', 'sh', '-c', 'timeout 10 nc -w 5 1.1.1.1 443 </dev/null', check=False)
        if escaped.returncode != 0:
            enforced = True
            break
        time.sleep(10)
    lima = subprocess.run(['limactl', 'shell', 'agyn', '--', 'sudo', 'sh', '-c', 'iptables-save 2>/dev/null | grep -c KUBE-NWPLCY || true'],
                          env={**os.environ, 'LIMA_HOME': str(Path.home() / '.agyn/local/lima')}, capture_output=True, text=True, timeout=60)
    print(f'NetworkPolicy enforced for the canary: {enforced}; kube-router policy chains on the node: {lima.stdout.strip() or lima.stderr.strip()[:200]}')
    if not enforced:
        print('::warning::The disposable VM does not enforce NetworkPolicy: a Pod under task-overlay-egress reached 1.1.1.1:443. '
              'Raw-TCP, public-DNS and cross-Pod ingress negatives are reported as skipped here and need the live probe.')
    kubectl('delete', 'pod', CANARY_POD, '-n', NAMESPACE, '--wait=false')
    with Path(os.environ['GITHUB_ENV']).open('a') as env:
        env.write(f'NETWORK_POLICY_ENFORCED={"true" if enforced else "false"}\n')


def sh(pod, container, script, timeout=60):
    return kubectl('exec', '-n', NAMESPACE, pod, '-c', container, '--', 'sh', '-c', script, check=False, timeout=timeout)


class Checks:
    def __init__(self, enforced):
        self.results = []
        self.enforced = enforced

    def record(self, name, ok, detail=''):
        self.results.append({'check': name, 'ok': bool(ok), 'detail': str(detail)[:400]})
        print(f'{"PASS" if ok else "FAIL"} {name}: {str(detail)[:200]}', flush=True)

    def policy(self, name, ok, detail=''):
        """A check only an enforced NetworkPolicy can satisfy."""
        if self.enforced:
            self.record(name, ok, detail)
            return
        self.results.append({'check': name, 'ok': True, 'skipped': True, 'detail': f'{POLICY_DEPENDENT}; observed: {str(detail)[:200]}'})
        print(f'SKIP {name}: {POLICY_DEPENDENT}', flush=True)


def env_of(container):
    return {e['name']: e.get('value') for e in container.get('env', []) if 'name' in e}


def shape_checks(checks, pod):
    spec = pod['spec']
    sc = spec.get('securityContext') or {}
    checks.record('pod runs as non-root 10001 with RuntimeDefault seccomp',
                  sc.get('runAsNonRoot') is True and sc.get('runAsUser') == 10001 and (sc.get('seccompProfile') or {}).get('type') == 'RuntimeDefault', sc)
    checks.record('pod keeps cluster DNS', spec.get('dnsPolicy') != 'None' and not spec.get('dnsConfig'), spec.get('dnsPolicy'))
    checks.record('pod carries the restricted profile annotation',
                  pod['metadata'].get('annotations', {}).get('agyn.dev/pod-security-profile') == 'restricted-v1', '')
    containers = spec.get('initContainers', []) + spec.get('containers', [])
    for c in containers:
        csc = c.get('securityContext') or {}
        caps = csc.get('capabilities') or {}
        checks.record(f'{c["name"]}: no privilege, drop ALL, no added capability',
                      csc.get('allowPrivilegeEscalation') is False and caps.get('drop') == ['ALL'] and not caps.get('add') and not csc.get('privileged'), csc)
        mounts = {m['name'] for m in c.get('volumeMounts', [])}
        expected_identity = c['name'] in ('ziti-enroll', 'ziti-sidecar')
        checks.record(f'{c["name"]}: identity mounted only where needed', ('ziti-identity' in mounts) == expected_identity, sorted(mounts))
        if c['name'] in ('ziti-enroll', 'ziti-sidecar', 'ziti-wait'):
            checks.record(f'{c["name"]}: no proxy variables', not set(env_of(c)) & set(PROXY_ENV), sorted(env_of(c)))
    main = spec['containers'][0]
    env = env_of(main)
    for name, want in {'HTTPS_PROXY': 'http://' + PROXY, 'HTTP_PROXY': 'http://' + PROXY, 'GATEWAY_ADDRESS': '127.0.0.1:18443',
                       'AGYN_NETWORK_MODE': 'explicit-proxy'}.items():
        checks.record(f'main {name}', env.get(name) == want, env.get(name))
    checks.record('main LLM_BASE_URL is the loopback forward', (env.get('LLM_BASE_URL') or '').startswith('http://127.0.0.1:18081/'), env.get('LLM_BASE_URL'))
    checks.record('main NO_PROXY is loopback and has no .agyn', (env.get('NO_PROXY') or '').startswith('127.0.0.1,localhost,::1') and '.agyn' not in env.get('NO_PROXY', ''), env.get('NO_PROXY'))


def connect(pod, authority):
    request = f'CONNECT {authority} HTTP/1.1\\r\\nHost: {authority}\\r\\n\\r\\n'
    return sh(pod, 'ziti-sidecar', f"printf '{request}' | timeout 15 nc -w 10 127.0.0.1 18080 | head -c 600").stdout


def gateway_reachable(pod):
    """Control: the Pod's identity can still reach the gateway through the
    proxy. Run first and last, so checks against a Pod whose agent is being
    deleted are discarded rather than reported."""
    answer = connect(pod, 'gateway.agyn:443')
    return ' 200 ' in (answer.splitlines() or [''])[0], answer.splitlines()[:3]


def network_checks(checks, pod, pod_ip, cluster_ips):
    out = sh(pod, 'ziti-sidecar', 'id -u; grep CapEff /proc/self/status').stdout.split()
    checks.record('overlay sidecar runs as 10001 with no effective capability', out[:1] == ['10001'] and '0000000000000000' in out, out)
    mtu = sh(pod, 'ziti-sidecar', 'ip link set lo mtu 1400')
    checks.record('cannot change the Pod network (no NET_ADMIN)', mtu.returncode != 0, (mtu.stderr or mtu.stdout).strip())
    positive = sh(pod, 'ziti-sidecar', 'timeout 10 nc -w 5 ziti-router.agyn.dev 2496 </dev/null')
    checks.record('control: the router edge is reachable directly', positive.returncode == 0, positive.stderr.strip())
    dns = sh(pod, 'ziti-sidecar', 'timeout 10 nslookup ziti.agyn.dev')
    checks.record('control: cluster DNS resolves the controller', dns.returncode == 0, dns.stdout.strip()[-120:])
    for name, target in [('public internet', '1.1.1.1 443'), ('kubernetes API', f'{cluster_ips["kubernetes"]} 443'),
                         ('platform service', f'{cluster_ips["agents"]} 50051')]:
        raw = sh(pod, 'ziti-sidecar', f'timeout 10 nc -w 5 {target} </dev/null')
        checks.policy(f'raw TCP to {name} is blocked', raw.returncode != 0, (raw.stderr or raw.stdout).strip()[:80])
    external_dns = sh(pod, 'ziti-sidecar', 'timeout 10 nslookup example.com 1.1.1.1')
    checks.policy('DNS to a public resolver is blocked', external_dns.returncode != 0, external_dns.stdout.strip()[-120:])
    for authority in ('unrouted.example.org:443', '1.1.1.1:443', 'agyn-tripwire.invalid:443'):
        answer = connect(pod, authority)
        first = (answer.splitlines() or [''])[0]
        checks.record(f'proxy refuses CONNECT {authority} as destination_not_found',
                      ' 502 ' in first and 'destination_not_found' in answer, answer.splitlines()[:6])
    plain = sh(pod, 'ziti-sidecar', "printf 'GET http://unrouted.example.org/ HTTP/1.1\\r\\nHost: unrouted.example.org\\r\\nConnection: close\\r\\n\\r\\n' | timeout 15 nc -w 10 127.0.0.1 18080 | head -c 600").stdout
    checks.record('proxy refuses plain HTTP to an unknown host', ' 502 ' in (plain.splitlines() or [''])[0] and 'destination_not_found' in plain, plain.splitlines()[:3])
    loop = sh(pod, 'ziti-sidecar', f'timeout 10 nc -w 5 {pod_ip} 18080 </dev/null')
    checks.record('proxy listener is not bound to the Pod IP', loop.returncode != 0, (loop.stderr or loop.stdout).strip())


def cross_pod_checks(checks, pod, pod_ip):
    # A deliberate wildcard listener in the task Pod: reachable on its own Pod
    # IP from inside, unreachable from another Pod, so the ingress deny (not a
    # wrong address) is what stops it.
    listener = subprocess.Popen(['kubectl', 'exec', '-n', NAMESPACE, pod, '-c', 'ziti-sidecar', '--', 'sh', '-c',
                                 'for i in 1 2 3 4 5 6; do timeout 20 nc -l -p 18999 </dev/null >/dev/null 2>&1; done'],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)
        inside = sh(pod, 'ziti-sidecar', f'timeout 10 nc -w 5 {pod_ip} 18999 </dev/null')
        checks.record('control: a wildcard listener answers on the Pod IP from inside', inside.returncode == 0, inside.stderr.strip())
        time.sleep(1)
        # The proxy port is unreachable from outside because it is bound to
        # loopback, which holds with or without an enforced policy; the
        # wildcard listener is unreachable only through the ingress deny.
        probe = kubectl('exec', '-n', PROBE_NAMESPACE, PROBE_POD, '--', 'sh', '-c', f'timeout 10 nc -w 5 {pod_ip} 18080 </dev/null', check=False)
        checks.record('another Pod cannot reach the proxy port in the task Pod', probe.returncode != 0, (probe.stderr or probe.stdout).strip())
        probe = kubectl('exec', '-n', PROBE_NAMESPACE, PROBE_POD, '--', 'sh', '-c', f'timeout 10 nc -w 5 {pod_ip} 18999 </dev/null', check=False)
        checks.policy('another Pod cannot reach a wildcard listener in the task Pod', probe.returncode != 0, (probe.stderr or probe.stdout).strip())
        control = kubectl('exec', '-n', PROBE_NAMESPACE, PROBE_POD, '--', 'sh', '-c', 'timeout 10 nslookup kubernetes.default.svc.cluster.local', check=False)
        checks.record('control: the probe Pod has a working network', control.returncode == 0, control.stdout.strip()[-120:])
    finally:
        listener.kill()


def task_pods():
    pods = get('pods', '-n', NAMESPACE, '-l', 'agyn.dev/managed-by=agents-orchestrator')['items']
    ready = []
    for pod in pods:
        statuses = {s['name']: s for s in pod.get('status', {}).get('containerStatuses', []) + pod.get('status', {}).get('initContainerStatuses', [])}
        main = pod['spec']['containers'][0]['name']
        if pod.get('status', {}).get('phase') == 'Running' and 'running' in statuses.get(main, {}).get('state', {}) \
                and 'running' in statuses.get('ziti-sidecar', {}).get('state', {}) and not pod['metadata'].get('deletionTimestamp'):
            ready.append(pod)
    return ready


def watch():
    RESULT.unlink(missing_ok=True)
    enforced = os.environ.get('NETWORK_POLICY_ENFORCED') == 'true'
    cluster_ips = {'kubernetes': get('service', 'kubernetes', '-n', 'default')['spec']['clusterIP'],
                   'agents': get('service', 'agents', '-n', 'agyn-platform')['spec']['clusterIP']}
    deadline = time.time() + 60 * 60
    attempts = []
    tried = set()
    while time.time() < deadline:
        for pod in task_pods():
            name = pod['metadata']['name']
            if name in tried:
                continue
            tried.add(name)
            checks = Checks(enforced)
            try:
                before, detail = gateway_reachable(name)
                if not before:
                    attempts.append({'pod': name, 'error': f'gateway control failed before checks: {detail}'})
                    continue
                shape_checks(checks, pod)
                network_checks(checks, name, pod['status']['podIP'], cluster_ips)
                cross_pod_checks(checks, name, pod['status']['podIP'])
                after, detail = gateway_reachable(name)
                if not after:
                    attempts.append({'pod': name, 'error': f'gateway control failed after checks (agent torn down?): {detail}'})
                    continue
                checks.record('control: proxy tunnels to the gateway overlay service before and after', True, detail)
            except Exception as error:  # the Pod may have gone mid-check
                attempts.append({'pod': name, 'error': str(error)[:300]})
                continue
            still = kubectl('get', 'pod', name, '-n', NAMESPACE, '-o', 'jsonpath={.metadata.uid}', check=False).stdout
            if still != pod['metadata']['uid']:
                attempts.append({'pod': name, 'error': 'Pod replaced during checks'})
                continue
            RESULT.write_text(json.dumps({'pod': name, 'results': checks.results, 'earlier_attempts': attempts}, indent=2))
            return
        time.sleep(5)
    RESULT.write_text(json.dumps({'pod': None, 'results': [], 'earlier_attempts': attempts}, indent=2))


def verify():
    deadline = time.time() + 20 * 60
    while not RESULT.exists() and time.time() < deadline:
        time.sleep(10)
    if not RESULT.exists():
        sys.exit('restricted network watcher produced no result')
    result = json.loads(RESULT.read_text())
    print(json.dumps(result, indent=2))
    failed = [r for r in result['results'] if not r['ok']]
    if not result['results']:
        sys.exit('no live explicit-proxy task Pod was checked')
    for skipped in [r for r in result['results'] if r.get('skipped')]:
        print(f'::warning::{skipped["check"]}: {skipped["detail"]}')
    if failed:
        sys.exit(f'{len(failed)} restricted network checks failed on {result["pod"]}')
    print(f'all {len(result["results"])} restricted network checks passed on {result["pod"]}')


if __name__ == '__main__':
    {'prepare': prepare, 'watch': watch, 'verify': verify}[sys.argv[1]]()
