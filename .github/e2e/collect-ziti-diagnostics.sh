#!/usr/bin/env bash
# Read-only, sanitized OpenZiti evidence from the disposable E2E VM.
# Usage: collect-ziti-diagnostics.sh <output-dir>
#
# Raw CLI JSON only flows through jq allow-lists and every log through redact().
# The admin login runs inside the controller container from its own
# environment, so credentials and the session never reach this runner or the job
# log. Collection failures are recorded in collection.log, never fatal.
set -uo pipefail

out="${1:?usage: collect-ziti-diagnostics.sh <output-dir>}"
ziti_ns="${ZITI_NAMESPACE:-ziti}"
platform_ns="${PLATFORM_NAMESPACE:-agyn-platform}"
log_tail="${ZITI_DIAGNOSTICS_LOG_TAIL:-50000}"
cli_home=/tmp/agyn-e2e-ziti-diagnostics
mkdir -p "${out}/logs"
note() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"${out}/collection.log"; }
k() { timeout 90 kubectl "$@"; }

redact() {
  # shellcheck disable=SC1003 # the PEM range uses POSIX c\ text on the next -e.
  sed -E \
    -e 's/eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*/[redacted-jwt]/g' \
    -e 's/-----BEGIN [A-Z ]+-----.*-----END [A-Z ]+-----/[redacted-pem]/g' \
    -e 's/(token=)[^&[:space:]"]+/\1[redacted]/g' \
    -e 's/pem:[^"[:space:]]+/pem:[redacted]/g' \
    -e 's/([A-Za-z_]*(password|passwd|secret|token|jwt|privatekey)"?[[:space:]]*[:=][[:space:]]*"?)[^",[:space:]}]+/\1[redacted]/Ig' \
    -e '/-----BEGIN [A-Z ]+-----/,/-----END [A-Z ]+-----/c\' -e '[redacted-pem]'
}

pods_json() {
  jq '[.items[] | {namespace: .metadata.namespace, name: .metadata.name, node: .spec.nodeName,
    phase: .status.phase, startTime: .status.startTime,
    containers: [.status.containerStatuses[]? | {name, image, imageID, ready, restartCount,
      startedAt: .state.running.startedAt,
      lastTerminated: (.lastState.terminated // null | if . then {reason, exitCode, startedAt, finishedAt} else null end)}],
    initContainers: [.status.initContainerStatuses[]? | {name, image, restartCount,
      terminated: (.state.terminated // null | if . then {reason, exitCode, startedAt, finishedAt} else null end)}]}]'
}

# --- inventory, versions and logs -------------------------------------------
k get pods -n "${ziti_ns}" -o json | pods_json >"${out}/ziti-pods.json" || note "ziti pod inventory unavailable"
k get pods -n "${platform_ns}" -l 'app.kubernetes.io/name in (gateway,ziti-management)' -o json | pods_json \
  >"${out}/platform-ziti-pods.json" || note "platform pod inventory unavailable"

collect_logs() {
  local ns="$1" pod="$2" container file
  for container in $(k get pod "${pod}" -n "${ns}" -o jsonpath='{.spec.initContainers[*].name} {.spec.containers[*].name}'); do
    file="${out}/logs/${ns}.${pod}.${container}"
    k logs "${pod}" -n "${ns}" -c "${container}" --timestamps --tail="${log_tail}" 2>&1 | redact >"${file}.log"
    k logs "${pod}" -n "${ns}" -c "${container}" --timestamps --tail="${log_tail}" --previous 2>/dev/null | redact >"${file}.previous.log"
    [ -s "${file}.previous.log" ] || rm -f "${file}.previous.log"
  done
}

ziti_pods="$(k get pods -n "${ziti_ns}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)"
for pod in ${ziti_pods}; do
  collect_logs "${ziti_ns}" "${pod}"
  container="$(k get pod "${pod}" -n "${ziti_ns}" -o jsonpath='{.spec.containers[0].name}' 2>/dev/null)"
  printf '%s/%s: %s\n' "${pod}" "${container}" \
    "$(k exec -n "${ziti_ns}" "${pod}" -c "${container}" -- ziti version 2>&1 | head -n 3 | tr '\n' ' ')" >>"${out}/versions.txt"
done
jq -r '.[] | .name as $pod | .containers[] | "\($pod) image \(.image) (\(.imageID))"' "${out}/ziti-pods.json" \
  >>"${out}/versions.txt" 2>/dev/null
for pod in $(k get pods -n "${platform_ns}" -l 'app.kubernetes.io/name in (gateway,ziti-management)' \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do
  collect_logs "${platform_ns}" "${pod}"
done

# ziti-management logs only GC and deletion; its lease table holds issue and
# renewal times. Rows carry ids, types and timestamps only.
grep -hE 'garbage collected service identity|service identity GC sweep failed|failed to delete service identity|already deleted|failed to cleanup|ZitiManagementService listening' \
  "${out}"/logs/"${platform_ns}".ziti-management-*.log >"${out}/ziti-management-identity-timeline.txt" 2>/dev/null
# The platform superuser is the container's POSTGRES_USER, over the local socket.
# shellcheck disable=SC2016 # expanded inside the postgres container only.
psql_in() { local pod="$1"; shift; k exec -n "${platform_ns}" "${pod}" -- sh -c 'psql -U "${POSTGRES_USER:-postgres}" "$@"' psql "$@"; }
for pod in $(k get pods -n "${platform_ns}" --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -E 'postgres'); do
  for db in $(psql_in "${pod}" -AtX -c 'select datname from pg_database where not datistemplate' 2>/dev/null); do
    if [ "$(psql_in "${pod}" -d "${db}" -AtX -c "select to_regclass('public.service_identities') is not null" 2>/dev/null)" = "t" ]; then
      psql_in "${pod}" -d "${db}" -AX -F $'\t' -c \
        'select ziti_identity_id, service_type, created_at, lease_expires_at, now() as collected_at from service_identities order by created_at' \
        >"${out}/ziti-management-service-identity-leases.tsv" 2>&1 || note "lease table query failed in ${pod}/${db}"
    fi
  done
done
[ -s "${out}/ziti-management-service-identity-leases.tsv" ] || note "ziti-management lease table unavailable"

# --- controller view through an in-pod admin login ---------------------------
ctrl_pod="$(printf '%s\n' "${ziti_pods}" | grep -m1 '^ziti-controller')"
ctrl_container="$(k get pod "${ctrl_pod:-none}" -n "${ziti_ns}" -o jsonpath='{.spec.containers[0].name}' 2>/dev/null)"
zcli() { k exec -n "${ziti_ns}" "${ctrl_pod}" -c "${ctrl_container}" -- env HOME="${cli_home}" ziti "$@"; }
logged_in=false
if [ -n "${ctrl_pod}" ] && [ -n "${ctrl_container}" ]; then
  # shellcheck disable=SC2016 # expanded inside the controller container only.
  if k exec -n "${ziti_ns}" "${ctrl_pod}" -c "${ctrl_container}" -- bash -c \
    'rm -rf "$0" && mkdir -p "$0" && [ -n "${ZITI_ADMIN_USER:-}" ] && [ -n "${ZITI_ADMIN_PASSWORD:-}" ] &&
     HOME="$0" ziti edge login "${ZITI_MGMT_API:-localhost:1280}" -u "${ZITI_ADMIN_USER}" -p "${ZITI_ADMIN_PASSWORD}" -y >/dev/null 2>&1' \
    "${cli_home}"; then
    logged_in=true
  else
    note "in-pod admin login from controller environment failed; ziti CLI evidence skipped"
  fi
else
  note "ziti controller pod not found"
fi

if [ "${logged_in}" = true ]; then
  zcli edge list edge-routers 'limit 100' -j | jq '[.data[]? | {id, name, isOnline, syncStatus, disabled, isVerified,
      version: .versionInfo.version, revision: .versionInfo.revision, buildDate: .versionInfo.buildDate}]' \
    >"${out}/edge-routers.json" || note "edge router list failed"
  zcli edge list identities 'name contains "svc-gateway" limit 100' -j | jq '[.data[]? | {id, name, createdAt, roleAttributes}]' \
    >"${out}/svc-gateway-identities.json" || note "svc-gateway identity list failed"
  # Comparing snapshots shows which workload identities outlive the suites.
  zcli edge list identities 'limit 500' -j | jq '[.data[]? | {id, name, createdAt, roleAttributes}]' \
    >"${out}/identities.json" || note "identity list failed"
  zcli edge list services 'name="gateway"' -j | jq '[.data[]? | {id, name, roleAttributes, terminatorStrategy, createdAt}]' \
    >"${out}/gateway-service.json" || note "gateway service lookup failed"
  service_id="$(jq -r '.[0].id // empty' "${out}/gateway-service.json" 2>/dev/null)"
  if [ -n "${service_id}" ]; then
    zcli edge list terminators "service=\"${service_id}\" limit 100" -j | jq '[.data[]? | {id, binding, identity, hostId,
        precedence, cost, createdAt, updatedAt, router: (.router.name // .routerId // null)}]' \
      >"${out}/gateway-terminators.json" || note "gateway terminator list failed"
  fi
  for value in data-model-index router-data-model-index router-controllers; do
    zcli fabric inspect "${value}" -j | jq '(.data // .) as $r | {success: $r.success, errors: $r.errors,
        values: [$r.values[]? | {appId, name, value}]}' | redact >"${out}/inspect-${value}.json" || note "inspect ${value} failed"
  done
  zcli fabric validate router-data-model --include-successes 2>&1 | redact >"${out}/validate-router-data-model.txt"
  k exec -n "${ziti_ns}" "${ctrl_pod}" -c "${ctrl_container}" -- rm -rf "${cli_home}" || note "admin CLI home cleanup failed"
fi

# --- merged timeline -----------------------------------------------------------
{
  jq -r '.[] | .name as $pod | .containers[] | select(.startedAt) | "\(.startedAt) pod \($pod)/\(.name) running (restarts \(.restartCount))"' \
    "${out}/ziti-pods.json" "${out}/platform-ziti-pods.json" 2>/dev/null
  jq -r '.[] | "\(.createdAt) controller identity \(.name) \(.id) created"' "${out}/svc-gateway-identities.json" 2>/dev/null
  jq -r '.[] | "\(.createdAt) terminator \(.id) on \(.router) hosted by \(.hostId // .identity)"' "${out}/gateway-terminators.json" 2>/dev/null
  for file in "${out}"/logs/"${platform_ns}".gateway-*.log; do
    [ -f "${file}" ] || continue
    grep -m1 'identity not found by id' "${file}" | sed 's/^\([^ ]*\) /\1 gateway FIRST bind rejection: /'
    grep 'identity not found by id' "${file}" | tail -n 1 | sed 's/^\([^ ]*\) /\1 gateway LAST bind rejection: /'
    grep -E 'gateway listening on ziti|ziti enrollment attempt|re-enrolling ziti|ziti service listener|ziti lease|establish ziti service listener|lost ziti service listener' \
      "${file}" | sed 's/^\([^ ]*\) /\1 gateway: /'
  done
  sed 's/^\([^ ]*\) /\1 ziti-management: /' "${out}/ziti-management-identity-timeline.txt" 2>/dev/null
  for file in "${out}"/logs/"${ziti_ns}".ziti-router-*.log; do
    [ -f "${file}" ] && grep -iE 'data model|subscri|full state|index|gap|identity not found|resync' "${file}" | tail -n 400 | sed 's/^\([^ ]*\) /\1 router: /'
  done
  for file in "${out}"/logs/"${ziti_ns}".ziti-controller-*.log; do
    [ -f "${file}" ] && grep -iE 'data model|subscri|index|raft|starting|started' "${file}" | tail -n 200 | sed 's/^\([^ ]*\) /\1 controller: /'
  done
} | cut -c1-600 | LC_ALL=C sort >"${out}/timeline.txt"
note "collection finished"
