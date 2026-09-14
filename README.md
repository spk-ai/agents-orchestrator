# Agents Orchestrator Service

The Agents Orchestrator runs a background reconciler that ensures agent workloads
exist for threads with unacknowledged agent messages.

Architecture: https://github.com/agynio/architecture/blob/main/architecture/agents-orchestrator.md

## Workload DNS With Ziti

Ziti-enabled agents and sandboxes use only the local intercepting resolver
(`127.0.0.1`). Listing the cluster DNS server as a second Pod nameserver is not
a safe fallback: [musl queries nameservers in parallel](https://wiki.musl-libc.org/functional-differences-from-glibc.html#Name-Resolver/DNS),
so a faster ordinary answer can route native agent requests around interception.
The readiness wait restores the same single-resolver configuration.

`WORKLOAD_DNS_UPSTREAM` remains the tunneler's explicit `--dnsUpstream` for names
it does not intercept and its control-plane startup resolution. Enrollment
continues to use its separate configured upstream. If the local resolver is
unavailable, workloads must fail resolution instead of using an ordinary DNS
answer. Ziti-disabled workloads retain their existing DNS behavior.

This is a resolver-routing correction, not an adversarial egress boundary:
direct-IP traffic, custom resolvers, privileges and network policy require
separate enforcement. Existing Pods are not rewritten by this change.

## Local Development

Full setup: https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md

### Prepare environment

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

### Run from sources

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```
