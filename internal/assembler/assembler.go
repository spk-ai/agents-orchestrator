package assembler

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const (
	listPageSize      int32 = 100
	rpcTimeout              = 10 * time.Second
	agynBinVolumeName       = "agyn-bin"
	// The volume is the whole /agyn tree: binaries under bin/, and the agent
	// runtime's config.json beside it rather than among them.
	agynBinMountPath                          = "/agyn"
	agynBinDir                                = "/agyn/bin"
	agynBinBinaryPath                         = agynBinDir + "/agynd"
	mcpBasePort                               = 8100
	mcpResolverOptions                        = "attempts:1 timeout:1 no-aaaa"
	mcpNodeOptions                            = "--dns-result-order=ipv4first"
	ZitiEnrollContainerName                   = "ziti-enroll"
	ZitiSidecarContainerName                  = "ziti-sidecar"
	zitiIdentityVolumeName                    = "ziti-identity"
	zitiIdentityMountPath                     = "/netfoundry"
	ZitiIdentityBasename                      = "agent"
	ZitiEnrollmentTokenEnvVar                 = "ZITI_ENROLL_TOKEN"
	ZitiIdentityBasenameEnvVar                = "ZITI_IDENTITY_BASENAME"
	ZitiIdentityDirEnvVar                     = "ZITI_IDENTITY_DIR"
	ZitiEnrollmentControllerResolveHostEnvVar = "ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST"
	ZitiEnrollmentControllerPortEnvVar        = "ZITI_ENROLLMENT_CONTROLLER_PORT"
	// The distro trust store itself. The bundle written here is the public
	// roots plus the egress CA, so every OpenSSL consumer picks it up without
	// being told where to look.
	egressCACertPath           = "/etc/ssl/certs/ca-certificates.crt"
	zitiDNSNameserver          = "127.0.0.1"
	zitiEnrollEntrypoint       = "/usr/bin/bash"
	zitiSidecarEntrypoint      = "/usr/bin/bash"
	zitiSidecarServicePollRate = "1"
	zitiEnrollScript           = `workload_dns_upstream="$1"
workload_dns_nameserver="$2"
enrollment_controller_resolve_host="$3"
enrollment_controller_port_override="$4"
runtime_controller_resolve_host="$5"
runtime_controller_port_override="$6"
identity_dir="${ZITI_IDENTITY_DIR}"
identity_basename="${ZITI_IDENTITY_BASENAME}"
identity_file="${identity_dir}/${identity_basename}.json"
jwt_file="${identity_dir}/${identity_basename}.jwt"
ziti_controller_cert="${identity_dir}/controller-ca.pem"
ziti_tls_ca_cert="${identity_dir}/controller-tls-ca.pem"
runtime_hosts_file="${identity_dir}/${identity_basename}.runtime.hosts"
resolv_file="${ZITI_RESOLV_CONF:-/etc/resolv.conf}"
hosts_file="${ZITI_HOSTS_FILE:-/etc/hosts}"
ziti_runtime_controller_host=""
ziti_runtime_controller_port=""

printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"
mkdir -p "${identity_dir}"

if [[ ! -s "${identity_file}" ]]; then
  if [[ -z "${ZITI_ENROLL_TOKEN}" ]]; then
    echo "ZITI_ENROLL_TOKEN is required" >&2
    exit 1
  fi
  if [[ -n "${ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST:-}" ]]; then
    enrollment_controller_resolve_host="${ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST}"
  fi
  if [[ -n "${ZITI_ENROLLMENT_CONTROLLER_PORT:-}" ]]; then
    enrollment_controller_port_override="${ZITI_ENROLLMENT_CONTROLLER_PORT}"
  fi
  printf '%s\n' "${ZITI_ENROLL_TOKEN}" > "${jwt_file}"

  jwt_payload="${ZITI_ENROLL_TOKEN#*.}"
  jwt_payload="${jwt_payload%%.*}"
  jwt_payload="$(printf '%s' "${jwt_payload}" | tr '_-' '/+')"
  case $(( ${#jwt_payload} % 4 )) in
    2) jwt_payload="${jwt_payload}==" ;;
    3) jwt_payload="${jwt_payload}=" ;;
  esac
  jwt_payload_json="$(printf '%s' "${jwt_payload}" | base64 -d 2>/dev/null || true)"
  ziti_controller_host="$(printf '%s' "${jwt_payload_json}" | sed -nE 's/.*"iss"[[:space:]]*:[[:space:]]*"https?:\/\/([^"\/:]+).*/\1/p' | head -n 1)"
  if [[ -n "${ziti_controller_host}" ]]; then
    awk -v host="${ziti_controller_host}" '($1 ~ /^(127\.|::1$)/) { for (i = 2; i <= NF; i++) if ($i == host) next } { print }' "${hosts_file}" > "${hosts_file}.tmp"
    cat "${hosts_file}.tmp" > "${hosts_file}"
    rm -f "${hosts_file}.tmp"
  fi
  printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"

  jwt_payload_file="${identity_dir}/${identity_basename}.payload.json"
  printf '%s\n' "${jwt_payload_json}" > "${jwt_payload_file}"
  ziti_controller_url="$(jq -r '.iss // empty' "${jwt_payload_file}")"
  ziti_enrollment_method="$(jq -r '.em // empty' "${jwt_payload_file}")"
  ziti_enrollment_token_id="$(jq -r '.jti // empty' "${jwt_payload_file}")"
  ziti_identity_subject="$(jq -r '.sub // empty' "${jwt_payload_file}")"
  if [[ -z "${ziti_controller_url}" || -z "${ziti_enrollment_method}" || -z "${ziti_enrollment_token_id}" || -z "${ziti_identity_subject}" ]]; then
    echo "ZITI_ENROLL_TOKEN is missing required iss, em, jti, or sub claims" >&2
    exit 1
  fi

  ziti_controller_hostport="$(printf '%s\n' "${ziti_controller_url}" | sed -nE 's#^https?://([^/]+).*#\1#p')"
  ziti_controller_host="${ziti_controller_hostport%%:*}"
  ziti_controller_port="${ziti_controller_hostport##*:}"
  if [[ "${ziti_controller_port}" == "${ziti_controller_hostport}" ]]; then
    ziti_controller_port="443"
  fi
  ziti_enrollment_controller_port="${ziti_controller_port}"
  if [[ -n "${enrollment_controller_port_override}" ]]; then
    ziti_enrollment_controller_port="${enrollment_controller_port_override}"
  fi
  ziti_enrollment_resolve_host="${ziti_controller_host}"
  if [[ -n "${enrollment_controller_resolve_host}" ]]; then
    ziti_enrollment_resolve_host="${enrollment_controller_resolve_host}"
  fi
  ziti_enrollment_controller_ip="$(getent ahostsv4 "${ziti_enrollment_resolve_host}" 2>/dev/null | awk '$2 == "STREAM" { print $1; exit }' || true)"
  if [[ -z "${ziti_enrollment_controller_ip}" ]]; then
    ziti_enrollment_controller_ip="$(awk -v host="${ziti_enrollment_resolve_host}" '{ for (i = 2; i <= NF; i++) if ($i == host) { print $1; exit } }' "${hosts_file}")"
  fi
  if [[ -z "${ziti_enrollment_controller_ip}" ]]; then
    echo "expected resolved controller address for ${ziti_enrollment_resolve_host}" >&2
    exit 1
  fi
  ziti_runtime_controller_host="${ziti_controller_host}"
  ziti_runtime_controller_port="${ziti_controller_port}"
  if [[ -n "${runtime_controller_port_override}" ]]; then
    ziti_runtime_controller_port="${runtime_controller_port_override}"
  fi

  openssl s_client -showcerts -servername "${ziti_controller_host}" -connect "${ziti_enrollment_controller_ip}:${ziti_enrollment_controller_port}" </dev/null 2>/dev/null | awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/ { print }' > "${ziti_controller_cert}"
  if [[ ! -s "${ziti_controller_cert}" ]]; then
    # What was dialled, not what the JWT advertises: an override splits them.
    echo "expected controller certificate from ${ziti_enrollment_resolve_host}:${ziti_enrollment_controller_port} (${ziti_enrollment_controller_ip}), advertised as ${ziti_controller_hostport}" >&2
    exit 1
  fi
  cat "${ziti_controller_cert}" > "${ziti_tls_ca_cert}"
  if [[ -s "${SSL_CERT_FILE:-}" ]]; then
    cat "${SSL_CERT_FILE}" >> "${ziti_tls_ca_cert}"
  fi
  hosts_backup="${identity_dir}/${identity_basename}.hosts"
  cat "${hosts_file}" > "${hosts_backup}"
  restore_hosts() {
    if [[ -s "${hosts_backup}" ]]; then
      cat "${hosts_backup}" > "${hosts_file}"
      rm -f "${hosts_backup}"
    fi
  }
  trap restore_hosts EXIT
  printf '%s\t%s\n' "${ziti_enrollment_controller_ip}" "${ziti_controller_host}" >> "${hosts_file}"
  ziti edge enroll --jwt "${jwt_file}" --ca "${ziti_tls_ca_cert}" --out "${identity_file}"
  restore_hosts
  trap - EXIT
fi

if [[ ! -s "${identity_file}" ]]; then
  echo "expected identity file ${identity_file}" >&2
  exit 1
fi

if [[ -n "${runtime_controller_port_override}" ]]; then
  ziti_runtime_controller_port="${runtime_controller_port_override}"
fi
if [[ -z "${ziti_runtime_controller_host}" || -z "${ziti_runtime_controller_port}" ]]; then
  ziti_runtime_controller_url="$(jq -r '.ztAPI // empty' "${identity_file}")"
  ziti_runtime_controller_hostport="$(printf '%s\n' "${ziti_runtime_controller_url}" | sed -nE 's#^https?://([^/]+).*#\1#p')"
  if [[ -z "${ziti_runtime_controller_hostport}" ]]; then
    echo "expected runtime controller endpoint in ${identity_file}" >&2
    exit 1
  fi
  if [[ -z "${ziti_runtime_controller_host}" ]]; then
    ziti_runtime_controller_host="${ziti_runtime_controller_hostport%%:*}"
  fi
  if [[ -z "${ziti_runtime_controller_port}" ]]; then
    ziti_runtime_controller_port="${ziti_runtime_controller_hostport##*:}"
    if [[ "${ziti_runtime_controller_port}" == "${ziti_runtime_controller_hostport}" ]]; then
      ziti_runtime_controller_port="443"
    fi
  fi
fi
jq --arg ztAPI "https://${ziti_runtime_controller_host}:${ziti_runtime_controller_port}/edge/client/v1" '.ztAPI = $ztAPI | del(.ztAPIs)' "${identity_file}" > "${identity_file}.tmp"
cat "${identity_file}.tmp" > "${identity_file}"
rm -f "${identity_file}.tmp"
if jq -e 'has("ztAPIs")' "${identity_file}" >/dev/null; then
  echo "expected single-controller identity without ztAPIs" >&2
  exit 1
fi
ziti_runtime_resolve_host="${ziti_runtime_controller_host}"
if [[ -n "${runtime_controller_resolve_host}" ]]; then
  ziti_runtime_resolve_host="${runtime_controller_resolve_host}"
fi
printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"
ziti_runtime_controller_ip="$(getent ahostsv4 "${ziti_runtime_resolve_host}" 2>/dev/null | awk '$2 == "STREAM" { print $1; exit }' || true)"
if [[ -z "${ziti_runtime_controller_ip}" ]]; then
  echo "expected resolved runtime controller address for ${ziti_runtime_resolve_host}" >&2
  exit 1
fi
printf '%s\t%s\n' "${ziti_runtime_controller_ip}" "${ziti_runtime_controller_host}" > "${runtime_hosts_file}"
printf 'ziti_identity_ztAPI=%s\n' "$(jq -r '.ztAPI // empty' "${identity_file}")"
printf 'ziti_runtime_host_alias=%s\n' "$(cat "${runtime_hosts_file}")"

printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_nameserver}" > "${resolv_file}"`
	zitiSidecarScript = `workload_dns_upstream="$1"
identity_file="${ZITI_IDENTITY_DIR}/${ZITI_IDENTITY_BASENAME}.json"
runtime_hosts_file="${ZITI_IDENTITY_DIR}/${ZITI_IDENTITY_BASENAME}.runtime.hosts"
resolv_file="${ZITI_RESOLV_CONF:-/etc/resolv.conf}"
hosts_file="${ZITI_HOSTS_FILE:-/etc/hosts}"
if [[ ! -s "${identity_file}" ]]; then
  echo "expected identity file ${identity_file}" >&2
  exit 1
fi
if jq -e 'has("ztAPIs")' "${identity_file}" >/dev/null; then
  echo "expected single-controller identity without ztAPIs" >&2
  exit 1
fi
printf 'ziti_sidecar_identity_ztAPI=%s\n' "$(jq -r '.ztAPI // empty' "${identity_file}")"
cat > "${resolv_file}" <<EOF
nameserver ${workload_dns_upstream}
search svc.cluster.local cluster.local
options ndots:5
EOF
if [[ "$(awk 'BEGIN { first = "" } /^nameserver[[:space:]]+/ { first = $2; exit } END { print first }' "${resolv_file}")" != "${workload_dns_upstream}" ]]; then
  echo "expected workload DNS first in ${resolv_file}" >&2
  exit 1
fi
if [[ ! -s "${runtime_hosts_file}" ]]; then
  echo "expected runtime controller host alias file ${runtime_hosts_file}" >&2
  exit 1
fi
runtime_controller_host="$(awk 'NF >= 2 { print $2; exit }' "${runtime_hosts_file}")"
if [[ -z "${runtime_controller_host}" ]]; then
  echo "expected runtime controller host in ${runtime_hosts_file}" >&2
  exit 1
fi
awk -v host="${runtime_controller_host}" '{ keep = 1; for (i = 2; i <= NF; i++) if ($i == host) keep = 0; if (keep) print }' "${hosts_file}" > "${hosts_file}.tmp"
cat "${runtime_hosts_file}" "${hosts_file}.tmp" > "${hosts_file}"
rm -f "${hosts_file}.tmp"
printf 'ziti_sidecar_runtime_host_alias=%s\n' "$(cat "${runtime_hosts_file}")"
ziti_diverter="/tmp/ziti-output-diverter"
cat > "${ziti_diverter}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-V" ]]; then
  echo "ziti-output-diverter 1"
  exit 0
fi
if [[ $# -lt 2 ]]; then
  echo "expected diverter operation and arguments" >&2
  exit 1
fi
operation="$1"
shift
if [[ "${operation}" != "-I" && "${operation}" != "-D" ]]; then
  echo "unsupported diverter operation ${operation}" >&2
  exit 1
fi
cidr=""
mask=""
protocol="tcp"
low_port=""
high_port=""
target_port=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c) shift; cidr="$1" ;;
    -m) shift; mask="$1" ;;
    -p) shift; protocol="$1" ;;
    -o) shift ;;
    -n) shift ;;
    -N) shift ;;
    -l) shift; low_port="$1" ;;
    -h) shift; high_port="$1" ;;
    -t) shift; target_port="$1" ;;
    -s) shift ;;
    *) echo "unsupported diverter argument $1" >&2; exit 1 ;;
  esac
  shift
done
if [[ -z "${cidr}" || -z "${mask}" || -z "${low_port}" || -z "${high_port}" ]]; then
  echo "missing diverter address or port arguments" >&2
  exit 1
fi
rule_key="${protocol}-${cidr//[^[:alnum:]._-]/_}-${mask}-${low_port}-${high_port}"
rule_file="/tmp/ziti-output-diverter-${rule_key}.port"
if [[ "${operation}" == "-I" ]]; then
  if [[ -z "${target_port}" ]]; then
    echo "missing diverter target port" >&2
    exit 1
  fi
  iptables -t nat -C OUTPUT -p "${protocol}" -d "${cidr}/${mask}" --dport "${low_port}:${high_port}" -j REDIRECT --to-ports "${target_port}" 2>/dev/null || \
    iptables -t nat -I OUTPUT -p "${protocol}" -d "${cidr}/${mask}" --dport "${low_port}:${high_port}" -j REDIRECT --to-ports "${target_port}"
  printf '%s\n' "${target_port}" > "${rule_file}"
  iptables -t nat -S OUTPUT | grep -- "${cidr}/${mask}" || true
else
  if [[ -s "${rule_file}" ]]; then
    while read -r saved_target_port; do
      if [[ -n "${saved_target_port}" ]]; then
        while iptables -t nat -D OUTPUT -p "${protocol}" -d "${cidr}/${mask}" --dport "${low_port}:${high_port}" -j REDIRECT --to-ports "${saved_target_port}" 2>/dev/null; do :; done
      fi
    done < "${rule_file}"
    rm -f "${rule_file}"
  fi
fi
EOF
chmod +x "${ziti_diverter}"
export GODEBUG="netdns=go+1"
# Without an upstream the tunneler answers REFUSED for names it does not intercept, which c-ares clients treat as fatal.
exec "/usr/local/bin/ziti" "tunnel" "tproxy" --identity "${identity_file}" --svcPollRate "${ZITI_SIDECAR_SERVICE_POLL_RATE}" --resolver "udp://127.0.0.1:53" --dnsUpstream "udp://${workload_dns_upstream}:53" --diverter "${ziti_diverter}"`
	zitiRequiredCapabilityNetAdmin = "NET_ADMIN"
	zitiRestartPolicyKey           = "restart_policy"
	zitiRestartPolicyAlways        = "Always"
	zitiDNSSearchService           = "svc.cluster.local"
	zitiDNSSearchCluster           = "cluster.local"
	zitiWaitContainerName          = "ziti-wait"
	// The overlay budget, not a per-target one: the targets come up together.
	zitiWaitTimeoutSeconds = 180
	// Four polls a second, so a miss overshoots by a quarter second rather than
	// a full one. Attempt counts are scaled by this, leaving timeouts in seconds.
	zitiWaitPollsPerSecond = 4
	zitiWaitPollInterval   = "0.25"
)

var reservedEnvNames = map[string]struct{}{
	"AGENT_ID":          {},
	"ENVIRONMENT_ID":    {},
	"AGENT_INSTANCE_ID": {},
	"AGENT_NAME":        {},
	"AGENT_ROLE":        {},
	"AGENT_MODEL":       {},
	"AGENT_CONFIG":      {},
	"WORKLOAD_ID":       {},
	"GATEWAY_ADDRESS":   {},
	"AGYN_GATEWAY_URL":  {},
	"AGYN_IDENTITY_ID":  {},
	"LLM_BASE_URL":      {},
	"LLM_MODE":          {},
	"LLM_MODEL_NAME":    {},
	// Vendor placeholder credentials. Reserved so a user ENV cannot shadow the
	// value the LLM Proxy expects to replace.
	"CLAUDE_CODE_OAUTH_TOKEN":      {},
	"TRACING_ADDRESS":              {},
	"OTEL_EXPORTER_OTLP_ENDPOINT":  {},
	"AGYND_AGENTS_DIRECT_ADDRESS":  {},
	"AGYND_RUNNERS_DIRECT_ADDRESS": {},
	"SSL_CERT_FILE":                {},
	"REQUESTS_CA_BUNDLE":           {},
	"NODE_EXTRA_CA_CERTS":          {},
	"AGENT_MCP_SERVERS":            {},
	"MCP_PORT":                     {},
	ZitiEnrollmentTokenEnvVar:      {},
	ZitiIdentityBasenameEnvVar:     {},
	ZitiIdentityDirEnvVar:          {},
}

type Assembler struct {
	agents       agentsClient
	runners      runnersClient
	secrets      secretsClient
	cfg          *config.Config
	egressCACert []byte
	// Optional. Without them the spec keeps whatever image reference it
	// already carried, which is the pre-catalog behaviour.
	images        ImagesClient
	organizations OrganizationsClient
	imageProxy    ImageProxyClient
	// Optional. Required only by an environment in native LLM mode, which fails
	// assembly without it rather than starting a workload that cannot call a
	// model.
	llm llmClient
}

// WithLLM enables native-mode resolution: which vendors a workload has a
// subscription for, and what each one's container placeholder is called.
func (a *Assembler) WithLLM(client llmClient) *Assembler {
	a.llm = client
	return a
}

// WithCatalog enables rewriting catalog references to the image proxy and
// minting the workload's pull credential.
func (a *Assembler) WithCatalog(images ImagesClient, organizations OrganizationsClient, proxy ImageProxyClient) *Assembler {
	a.images = images
	a.organizations = organizations
	a.imageProxy = proxy
	return a
}

type AssembleResult struct {
	Request        *runnerv1.StartWorkloadRequest
	OrganizationID string
	RunnerLabels   map[string]string
	// RunnerID names the runner the agent's environment places workloads on.
	// Empty for an agent without an environment, which is still placed by
	// labels and capabilities.
	RunnerID string
	// EnvironmentID is recorded on the workload's OpenZiti identity, so a
	// data-plane service can resolve what the workload runs from the
	// connection. Empty for an agent still on the deprecated inline image.
	EnvironmentID string
	// LLMRoleAttributes opt the workload's identity into vendor interception.
	// Empty in platform mode, where vendor traffic is not intercepted.
	LLMRoleAttributes []string
	// GrantedImageIDs are the catalog images this workload may pull. The
	// pull credential is minted against them once the workload id exists,
	// which is after assembly.
	GrantedImageIDs []string
	// Flavor names the catalog entry the workload is allocated from, and is
	// what compute is billed by. Empty for an agent without an environment.
	Flavor string
	// PersistentShells is the environment's answer, carried onto the workload
	// so the Terminal Proxy can read it without resolving an environment.
	// True when the workload has no environment: it is the platform default,
	// and nothing without an environment has a person attaching to it anyway.
	PersistentShells       bool
	PersistentVolumes      []PersistentVolumeInfo
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
}

type PersistentVolumeInfo struct {
	ID              uuid.UUID
	AgentInstanceID uuid.UUID
	Volume          *agentsv1.Volume
	Spec            *runnerv1.VolumeSpec
}

func (i PersistentVolumeInfo) Key() string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s:%s", i.AgentInstanceID.String(), i.ID.String()))).String()
}

func New(agents agentsClient, secrets secretsClient, cfg *config.Config) *Assembler {
	return NewWithEgressCA(agents, secrets, cfg, nil)
}

func NewWithRunners(agents agentsClient, runners runnersClient, secrets secretsClient, cfg *config.Config) *Assembler {
	return NewWithRunnersAndEgressCA(agents, runners, secrets, cfg, nil)
}

func NewWithEgressCA(agents agentsClient, secrets secretsClient, cfg *config.Config, egressCACert []byte) *Assembler {
	return NewWithRunnersAndEgressCA(agents, nil, secrets, cfg, egressCACert)
}

func NewWithRunnersAndEgressCA(agents agentsClient, runners runnersClient, secrets secretsClient, cfg *config.Config, egressCACert []byte) *Assembler {
	return &Assembler{agents: agents, runners: runners, secrets: secrets, cfg: cfg, egressCACert: append([]byte(nil), egressCACert...)}
}

func (a *Assembler) Assemble(ctx context.Context, agentID, agentInstanceID, threadID uuid.UUID) (*AssembleResult, error) {
	agent, err := a.fetchAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}
	runnerLabels := agentRunnerLabels(agent)

	resolver := newEnvResolver(a.secrets)
	volumeResolver := newVolumeResolver(a.agents, agentInstanceID)
	rewriter := newImageRewriter(a.images, a.organizations, a.cfg.ImageProxyHost)

	environment, flavor, err := a.resolveAgentEnvironment(ctx, agent)
	if err != nil {
		return nil, err
	}

	agentEnvs, err := a.listEnvs(ctx, &agentsv1.ListEnvsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent envs: %w", err)
	}
	agentEnvVars, err := resolver.ResolveEnvVars(ctx, agentEnvs)
	if err != nil {
		return nil, fmt.Errorf("resolve agent envs: %w", err)
	}

	// Volumes belong to the environment. An agent declares none of its own, so
	// an agent without an environment gets no mounts beyond the platform's.
	var agentMounts []*runnerv1.VolumeMount
	if environment != nil {
		agentMounts, err = volumeResolver.loadEnvironmentVolumes(ctx, environment.GetMeta().GetId())
		if err != nil {
			return nil, err
		}
	}

	mainImage := agent.GetImage()
	// The agent runtime init container, when the environment names one. Empty
	// leaves the workload on the agent's own init image.
	agentRuntimeImage := ""
	var environmentEnvVars []*runnerv1.EnvVar
	if environment != nil {
		environmentID := environment.GetMeta().GetId()
		mainImage = environment.GetImage()
		// See sandbox assembly: a catalog reference resolves only through the
		// Image Proxy, so an unconfigured proxy is a broken deployment rather
		// than a mode in which the reference is ignored.
		if environment.GetWorkspaceImageId() != "" {
			if !rewriter.enabled() {
				return nil, fmt.Errorf("environment %s names a workspace image but the image proxy is not configured", environmentID)
			}
			mainImage, err = rewriter.Rewrite(ctx, environment.GetWorkspaceImageId(), environment.GetWorkspaceImageTag())
			if err != nil {
				return nil, fmt.Errorf("environment %s workspace image: %w", environmentID, err)
			}
		}
		if environment.GetAgentRuntimeImageId() != "" {
			if !rewriter.enabled() {
				return nil, fmt.Errorf("environment %s names an agent runtime image but the image proxy is not configured", environmentID)
			}
			agentRuntimeImage, err = rewriter.Rewrite(ctx, environment.GetAgentRuntimeImageId(), environment.GetAgentRuntimeImageTag())
			if err != nil {
				return nil, fmt.Errorf("environment %s agent runtime image: %w", environmentID, err)
			}
		}
		environmentEnvs, err := a.listEnvs(ctx, &agentsv1.ListEnvsRequest{EnvironmentId: environmentID})
		if err != nil {
			return nil, fmt.Errorf("list environment envs: %w", err)
		}
		environmentEnvVars, err = resolver.ResolveEnvVars(ctx, environmentEnvs)
		if err != nil {
			return nil, fmt.Errorf("resolve environment envs: %w", err)
		}
	}

	mainEnv := mergeEnvVars(a.baseAgentEnvVars(agent, agentID, agentInstanceID), agentEnvVars, fmt.Sprintf("agent %s", agentID.String()))
	if len(environmentEnvVars) > 0 {
		// mergeEnvVars keeps the first occurrence of a name, so layering the
		// environment's envs onto the agent's leaves the agent's own value in
		// place on a collision: it is the more specific of the two.
		mainEnv = mergeEnvVars(mainEnv, environmentEnvVars, fmt.Sprintf("environment %s", environment.GetMeta().GetId()))
	}
	mainEnv = appendEgressCAEnvVars(mainEnv)

	// Native mode decides what the workload's identity is stamped with and what
	// credential the container holds, so it is resolved before the spec is
	// built and fails assembly rather than the first model call.
	llmMode, err := a.resolveLLMMode(ctx, environment, agentID.String(), agent.GetModelName())
	if err != nil {
		return nil, err
	}
	// Layered last so a user-set ENV cannot shadow the placeholder or the mode.
	mainEnv = mergeEnvVars(append(llmMode.EnvVars, mainEnv...), nil, "llm mode")

	mainMounts := append([]*runnerv1.VolumeMount{}, agentMounts...)
	mainMounts = append(mainMounts, &runnerv1.VolumeMount{Volume: agynBinVolumeName, MountPath: agynBinMountPath})
	main := &runnerv1.ContainerSpec{
		Image:            mainImage,
		Name:             fmt.Sprintf("agent-%s-%s", agentID.String()[:8], agentInstanceID.String()[:8]),
		Cmd:              []string{agynBinBinaryPath},
		Env:              mainEnv,
		Mounts:           mainMounts,
		InlineFileMounts: egressCAInlineFileMounts(a.egressCACert),
	}
	// Two chart-pinned platform init containers, then the environment's agent
	// runtime. Naming no runtime is refused rather than substituted: the
	// substitute was the agent's own init image, and a workload assembled
	// without a runtime starts and then has no CLI to spawn.
	initContainers, err := a.platformInitContainers()
	if err != nil {
		return nil, err
	}
	runtimeInit := a.agentRuntimeInitContainer(agentRuntimeImage)
	if runtimeInit == nil {
		return nil, fmt.Errorf("agent %s: environment names no agent runtime image", agentID)
	}
	initContainers = append(initContainers, runtimeInit)
	if a.cfg.ZitiEnabled {
		if _, err := gatewayHost(a.cfg.AgentGatewayAddress); err != nil {
			return nil, err
		}
		llmProxyTarget, err := zitiServiceWaitTarget(a.cfg.AgentLLMBaseURL)
		if err != nil {
			return nil, err
		}
		zitiEnroll := &runnerv1.ContainerSpec{
			Image:      a.cfg.ZitiSidecarImage,
			Name:       ZitiEnrollContainerName,
			Cmd:        buildZitiEnrollCommand(a.cfg.ZitiEnrollmentDNSUpstream, a.cfg.ZitiEnrollmentControllerResolveHost, a.cfg.ZitiEnrollmentControllerPort, a.cfg.ZitiRuntimeControllerResolveHost, a.cfg.ZitiRuntimeControllerPort),
			Entrypoint: zitiEnrollEntrypoint,
			Env:        zitiEnrollEnvVars(a.cfg.ZitiEnrollmentControllerResolveHost, a.cfg.ZitiEnrollmentControllerPort),
			Mounts:     []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
		}
		zitiSidecar := &runnerv1.ContainerSpec{
			Image:                a.cfg.ZitiSidecarImage,
			Name:                 ZitiSidecarContainerName,
			Cmd:                  buildZitiSidecarCommand(a.cfg.WorkloadDNSUpstream),
			Entrypoint:           zitiSidecarEntrypoint,
			Env:                  zitiSidecarEnvVars(a.cfg.WorkloadDNSUpstream),
			Mounts:               []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
			RequiredCapabilities: []string{zitiRequiredCapabilityNetAdmin},
			// k8s-runner maps restart_policy=Always on init containers to
			// Kubernetes restartable init containers. This lets the tunnel stay
			// up while Kubernetes continues to later init containers and main.
			AdditionalProperties: map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways},
		}
		zitiWait := &runnerv1.ContainerSpec{
			Image:      a.cfg.ZitiSidecarImage,
			Name:       zitiWaitContainerName,
			Entrypoint: zitiSidecarEntrypoint,
			Cmd:        buildZitiWaitCommand(a.cfg.AgentGatewayAddress, llmProxyTarget),
		}
		applyEgressCA(zitiEnroll, a.egressCACert)
		applyEgressCA(zitiSidecar, a.egressCACert)
		applyEgressCA(zitiWait, a.egressCACert)
		// The binaries land while the overlay is coming up, not after it.
		//
		// Kubernetes runs blocking init containers one at a time, so ordering is
		// the whole lever here. The agyn-bin containers only unload binaries into
		// a shared volume -- they need no mesh and no network -- and putting them
		// behind the wait meant their seconds were spent after the tunnel was
		// already up rather than during. The wait goes last, by which time it
		// usually has nothing left to wait for.
		initContainers = append(
			append([]*runnerv1.ContainerSpec{zitiEnroll, zitiSidecar}, initContainers...),
			zitiWait,
		)
	}

	mcps, err := a.listMcps(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list mcps: %w", err)
	}
	log.Printf("assembler: agent %s: found %d MCP servers", agentID, len(mcps))
	mcpAssignments, err := assignMcpPorts(mcps)
	if err != nil {
		return nil, fmt.Errorf("assign mcp ports: %w", err)
	}
	mainResources := agent.GetResources()
	boundedResources := requiresComputeResources(agent.GetCapabilities())
	if boundedResources {
		resources := flavor.GetResources()
		if resources == nil {
			return nil, fmt.Errorf("compute-resources requires a flavor with explicit resource bounds")
		}
		mainResources = &agentsv1.ComputeResources{RequestsCpu: resources.GetRequestsCpu(), RequestsMemory: resources.GetRequestsMemory(),
			LimitsCpu: resources.GetLimitsCpu(), LimitsMemory: resources.GetLimitsMemory()}
		main.Resources, err = runnerComputeResources(mainResources)
		if err != nil {
			return nil, fmt.Errorf("flavor %s resources: %w", flavor.GetName(), err)
		}
	}
	allocatedCPU, allocatedRAM, err := sumAllocatedResources(agent, mcpAssignments, mainResources)
	if err != nil {
		return nil, err
	}

	sidecarCapacity := len(mcpAssignments)
	sidecars := make([]*runnerv1.ContainerSpec, 0, sidecarCapacity)
	mcpServers := make([]string, 0, len(mcpAssignments))
	for _, assignment := range mcpAssignments {
		sidecar, err := a.buildMcpSidecar(ctx, resolver, volumeResolver, rewriter, assignment.mcp, assignment.port)
		if err != nil {
			return nil, err
		}
		if boundedResources {
			sidecar.Resources, err = runnerComputeResources(assignment.mcp.GetResources())
			if err != nil {
				return nil, fmt.Errorf("mcp %s resources: %w", assignment.id, err)
			}
		}
		sidecars = append(sidecars, sidecar)
		mcpServers = append(mcpServers, fmt.Sprintf("%s:%d", assignment.name, assignment.port))
	}
	if len(mcpServers) > 0 {
		main.Env = appendPlatformEnvVar(main.Env, &runnerv1.EnvVar{Name: "AGENT_MCP_SERVERS", Value: strings.Join(mcpServers, ",")})
	}

	agynBinVolume := &runnerv1.VolumeSpec{
		Name: agynBinVolumeName,
		Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
	}
	volumes := append(volumeResolver.Specs(), agynBinVolume)
	if a.cfg.ZitiEnabled {
		volumes = append(volumes, &runnerv1.VolumeSpec{
			Name: zitiIdentityVolumeName,
			Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
		})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].Name < volumes[j].Name })

	request := &runnerv1.StartWorkloadRequest{
		Main:           main,
		Sidecars:       sidecars,
		Volumes:        volumes,
		InitContainers: initContainers,
		Capabilities:   append([]string(nil), agent.GetCapabilities()...),
		InlineFiles:    a.inlineFiles(),
		AdditionalProperties: map[string]string{
			LabelKeyPrefix + LabelManagedBy:  ManagedByValue,
			LabelKeyPrefix + LabelAgentID:    agentID.String(),
			LabelKeyPrefix + LabelInstanceID: agentInstanceID.String(),
			LabelKeyPrefix + LabelThreadID:   threadID.String(),
		},
	}
	if a.cfg.ZitiEnabled {
		// Parallel resolvers (including musl) can bypass interception if an
		// ordinary nameserver is listed here. The tunnel forwards other names.
		request.DnsConfig = &runnerv1.DnsConfig{
			Nameservers: []string{zitiDNSNameserver},
			Searches:    []string{zitiDNSSearchService, zitiDNSSearchCluster},
		}
	}
	persistentVolumes, err := volumeResolver.PersistentVolumes()
	if err != nil {
		return nil, err
	}
	return &AssembleResult{
		Request:                request,
		OrganizationID:         agent.GetOrganizationId(),
		GrantedImageIDs:        rewriter.GrantedImageIDs(),
		RunnerLabels:           runnerLabels,
		RunnerID:               flavor.GetRunnerId(),
		EnvironmentID:          agent.GetEnvironmentId(),
		LLMRoleAttributes:      llmMode.RoleAttributes,
		Flavor:                 flavor.GetName(),
		PersistentShells:       environment == nil || environment.GetPersistentShells(),
		PersistentVolumes:      persistentVolumes,
		AllocatedCPUMillicores: allocatedCPU,
		AllocatedRAMBytes:      allocatedRAM,
	}, nil
}

// resolveAgentEnvironment resolves the environment an agent runs in the same
// way a sandbox resolves its own. environment_id is optional: every agent
// created before environments existed has none and keeps its inline image and
// label-based placement, so a missing id resolves to nil rather than an error.
func (a *Assembler) resolveAgentEnvironment(ctx context.Context, agent *agentsv1.Agent) (*agentsv1.Environment, *runnersv1.Flavor, error) {
	if strings.TrimSpace(agent.GetEnvironmentId()) == "" {
		return nil, nil, nil
	}
	environmentID, err := uuidutil.ParseUUID(agent.GetEnvironmentId(), "agent.environment_id")
	if err != nil {
		return nil, nil, err
	}
	environment, err := a.fetchEnvironment(ctx, environmentID)
	if err != nil {
		return nil, nil, err
	}
	flavor, err := a.resolveFlavor(ctx, environment.GetRunnerId(), environment.GetFlavor())
	if err != nil {
		return nil, nil, err
	}
	return environment, flavor, nil
}

func zitiEnvVars() []*runnerv1.EnvVar {
	return []*runnerv1.EnvVar{
		{Name: ZitiIdentityBasenameEnvVar, Value: ZitiIdentityBasename},
		{Name: ZitiIdentityDirEnvVar, Value: zitiIdentityMountPath},
	}
}

func zitiEnrollEnvVars(enrollmentControllerResolveHost string, enrollmentControllerPort string) []*runnerv1.EnvVar {
	envVars := zitiEnvVars()
	envVars = append(envVars,
		&runnerv1.EnvVar{Name: ZitiEnrollmentControllerResolveHostEnvVar, Value: enrollmentControllerResolveHost},
		&runnerv1.EnvVar{Name: ZitiEnrollmentControllerPortEnvVar, Value: enrollmentControllerPort},
	)
	return envVars
}

func zitiSidecarEnvVars(workloadDNSUpstream string) []*runnerv1.EnvVar {
	envVars := zitiEnvVars()
	envVars = append(envVars,
		&runnerv1.EnvVar{Name: "WORKLOAD_DNS_UPSTREAM", Value: workloadDNSUpstream},
		&runnerv1.EnvVar{Name: "ZITI_SIDECAR_SERVICE_POLL_RATE", Value: zitiSidecarServicePollRate},
	)
	return envVars
}

func buildZitiEnrollCommand(workloadDNSUpstream string, enrollmentControllerResolveHost string, enrollmentControllerPort string, runtimeControllerResolveHost string, runtimeControllerPort string) []string {
	return []string{
		"-ec",
		zitiEnrollScript,
		ZitiEnrollContainerName,
		workloadDNSUpstream,
		zitiDNSNameserver,
		enrollmentControllerResolveHost,
		enrollmentControllerPort,
		runtimeControllerResolveHost,
		runtimeControllerPort,
	}
}

func buildZitiSidecarCommand(workloadDNSUpstream string) []string {
	return []string{
		"-ec",
		zitiSidecarScript,
		ZitiSidecarContainerName,
		workloadDNSUpstream,
	}
}

func agentRunnerLabels(agent *agentsv1.Agent) map[string]string {
	if agent == nil {
		return nil
	}
	// TODO: Add runner_labels to Agent proto and return agent.GetRunnerLabels().
	return nil
}

func (a *Assembler) fetchAgent(ctx context.Context, agentID uuid.UUID) (*agentsv1.Agent, error) {
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := a.agents.GetAgent(rctx, &agentsv1.GetAgentRequest{Id: agentID.String()})
	cancel()
	if err != nil {
		return nil, err
	}
	agent := resp.GetAgent()
	if agent == nil {
		return nil, fmt.Errorf("agent response missing")
	}
	meta := agent.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("agent meta missing")
	}
	metaID, err := uuidutil.ParseUUID(meta.GetId(), "agent.meta.id")
	if err != nil {
		return nil, err
	}
	if metaID != agentID {
		return nil, fmt.Errorf("agent id mismatch: %s", metaID.String())
	}
	if agent.GetOrganizationId() == "" {
		return nil, fmt.Errorf("agent organization id missing")
	}
	return agent, nil
}

func (a *Assembler) listMcps(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Mcp, error) {
	resp := []*agentsv1.Mcp{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListMcps(rctx, &agentsv1.ListMcpsRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetMcps()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest) ([]*agentsv1.Env, error) {
	resp := []*agentsv1.Env{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListEnvs(rctx, &agentsv1.ListEnvsRequest{
			AgentId:       req.GetAgentId(),
			McpId:         req.GetMcpId(),
			EnvironmentId: req.GetEnvironmentId(),
			PageSize:      listPageSize,
			PageToken:     token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetEnvs()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func assignMcpPorts(mcps []*agentsv1.Mcp) ([]mcpAssignment, error) {
	assignments := make([]mcpAssignment, 0, len(mcps))
	for _, mcp := range mcps {
		if mcp == nil {
			return nil, fmt.Errorf("mcp is nil")
		}
		meta := mcp.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("mcp meta missing")
		}
		id := meta.GetId()
		if id == "" {
			return nil, fmt.Errorf("mcp meta id missing")
		}
		name := mcp.GetName()
		if name == "" {
			return nil, fmt.Errorf("mcp name missing")
		}
		assignments = append(assignments, mcpAssignment{mcp: mcp, id: id, name: name})
	}
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].id < assignments[j].id
	})
	for i := range assignments {
		assignments[i].port = mcpBasePort + i
	}
	return assignments, nil
}

func (a *Assembler) buildMcpSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, rewriter *imageRewriter, mcp *agentsv1.Mcp, port int) (*runnerv1.ContainerSpec, error) {
	if mcp == nil {
		return nil, fmt.Errorf("mcp is nil")
	}
	meta := mcp.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("mcp meta missing")
	}
	mcpID, err := uuidutil.ParseUUID(meta.GetId(), "mcp.meta.id")
	if err != nil {
		return nil, err
	}
	envVars, mounts, err := a.resolveSidecarResources(
		ctx,
		resolver,
		volumeResolver,
		&agentsv1.ListEnvsRequest{McpId: mcpID.String()},
		mcp,
	)
	if err != nil {
		return nil, err
	}
	gatewayURL := buildGatewayURL(a.cfg.AgentGatewayAddress)
	envVars = mergeEnvVars([]*runnerv1.EnvVar{
		{Name: "MCP_PORT", Value: strconv.Itoa(port)},
		{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress},
		{Name: "AGYN_GATEWAY_URL", Value: gatewayURL},
	}, envVars, fmt.Sprintf("mcp %s", mcpID.String()))
	envVars = applyMcpResolverEnvVars(envVars)
	envVars = appendEgressCAEnvVars(envVars)

	image := mcp.GetImage()
	if rewriter.enabled() && mcp.GetImageId() != "" {
		image, err = rewriter.Rewrite(ctx, mcp.GetImageId(), mcp.GetImageTag())
		if err != nil {
			return nil, fmt.Errorf("mcp %s image: %w", mcpID, err)
		}
	}
	return &runnerv1.ContainerSpec{
		Image:            image,
		Name:             fmt.Sprintf("mcp-%s", mcpID.String()[:8]),
		Cmd:              []string{"/bin/sh", "-c", mcp.GetCommand()},
		Env:              envVars,
		Mounts:           mounts,
		InlineFileMounts: egressCAInlineFileMounts(a.egressCACert),
	}, nil
}

func applyMcpResolverEnvVars(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	envs = appendDefaultEnvVar(envs, "RES_OPTIONS", mcpResolverOptions)
	return appendComposedEnvVar(envs, "NODE_OPTIONS", mcpNodeOptions)
}

func appendDefaultEnvVar(envs []*runnerv1.EnvVar, name, value string) []*runnerv1.EnvVar {
	for _, env := range envs {
		if env.GetName() == name {
			return envs
		}
	}
	return append(envs, &runnerv1.EnvVar{Name: name, Value: value})
}

func appendComposedEnvVar(envs []*runnerv1.EnvVar, name, value string) []*runnerv1.EnvVar {
	for _, env := range envs {
		if env.GetName() != name {
			continue
		}
		fields := strings.Fields(env.GetValue())
		for _, field := range fields {
			if field == value {
				return envs
			}
		}
		if env.GetValue() == "" {
			env.Value = value
			return envs
		}
		env.Value = env.GetValue() + " " + value
		return envs
	}
	return append(envs, &runnerv1.EnvVar{Name: name, Value: value})
}

func buildGatewayURL(address string) string {
	if strings.Contains(address, "://") {
		return address
	}
	return "http://" + address
}

func gatewayHost(address string) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("parse gateway host from %q: %w", address, err)
	}
	if host == "" {
		return "", fmt.Errorf("gateway host missing from %q", address)
	}
	return host, nil
}

func buildZitiWaitCommand(address string, llmProxy zitiServiceTarget) []string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		panic(fmt.Sprintf("parse gateway address %q: %v", address, err))
	}
	return buildZitiTCPWaitCommand(zitiWaitTimeoutSeconds, []zitiServiceTarget{{host: host, port: port}, llmProxy})
}

type zitiServiceTarget struct {
	host string
	port string
}

func zitiServiceWaitTarget(rawURL string) (zitiServiceTarget, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return zitiServiceTarget{}, fmt.Errorf("parse ziti service wait target from %q: %w", rawURL, err)
	}
	if parsed.Scheme == "" {
		return zitiServiceTarget{}, fmt.Errorf("ziti service wait target %q missing scheme", rawURL)
	}
	if parsed.Host == "" {
		return zitiServiceTarget{}, fmt.Errorf("ziti service wait target %q missing host", rawURL)
	}
	host := parsed.Hostname()
	if host == "" {
		return zitiServiceTarget{}, fmt.Errorf("ziti service wait target %q missing host", rawURL)
	}
	port := parsed.Port()
	if port == "" {
		switch parsed.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return zitiServiceTarget{}, fmt.Errorf("ziti service wait target %q has unsupported scheme %q", rawURL, parsed.Scheme)
		}
	}
	return zitiServiceTarget{host: host, port: port}, nil
}

// buildZitiTCPWaitCommand waits for every target to answer through the overlay.
//
// One container for all of them rather than one each: they become reachable
// together, when the sidecar finishes bringing the tunnel up, so waiting for
// them in sequence bought nothing and cost a container start per target.
//
// The poll is deliberately sub-second. The check runs before the sleep, so a
// target that is already up costs nothing -- but each miss used to overshoot by
// a full second past the moment the mesh became ready. The attempt count is
// scaled by the same factor, so the timeout budget in seconds is unchanged.
func buildZitiTCPWaitCommand(timeoutSeconds int, targets []zitiServiceTarget) []string {
	resolverConfig := fmt.Sprintf(
		"nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5 timeout:1 attempts:1\n",
		zitiDNSNameserver,
	)
	specs := make([]string, 0, len(targets))
	for _, target := range targets {
		specs = append(specs, target.host+":"+target.port)
	}
	script := fmt.Sprintf(
		`targets=%s; i=0; reason="not checked"; while [ $i -lt %d ]; do printf %s > /etc/resolv.conf; ready=1; for t in ${targets}; do h="${t%%%%:*}"; p="${t##*:}"; if ! getent ahostsv4 "${h}" >/tmp/ziti-wait-dns.out 2>/tmp/ziti-wait-dns.err; then ready=0; reason="dns lookup failed for ${h} through pod resolver: $(cat /tmp/ziti-wait-dns.err)"; break; fi; if ! timeout 5 bash -c "cat </dev/null >/dev/tcp/${h}/${p}" >/tmp/ziti-wait-tcp.out 2>/tmp/ziti-wait-tcp.err; then ready=0; reason="tcp connect failed for ${h}:${p}: $(cat /tmp/ziti-wait-tcp.err)"; break; fi; done; if [ "${ready}" -eq 1 ]; then exit 0; fi; if [ $((i %% %d)) -eq 0 ]; then echo "waiting for ${targets} attempt=${i} reason=${reason}; resolv.conf=$(tr "\n" ";" </etc/resolv.conf); dns=$(cat /tmp/ziti-wait-dns.out 2>/dev/null)" >&2; fi; i=$((i+1)); sleep %s; done; echo "timeout waiting for ${targets} (${reason}); resolv.conf=$(tr "\n" ";" </etc/resolv.conf); dns=$(cat /tmp/ziti-wait-dns.out 2>/dev/null)" >&2; exit 1`,
		strconv.Quote(strings.Join(specs, " ")),
		timeoutSeconds*zitiWaitPollsPerSecond,
		strconv.Quote(resolverConfig),
		15*zitiWaitPollsPerSecond,
		zitiWaitPollInterval,
	)
	return []string{"-c", script}
}

func mergeEnvVars(platformEnv, userEnv []*runnerv1.EnvVar, owner string) []*runnerv1.EnvVar {
	merged := make([]*runnerv1.EnvVar, 0, len(platformEnv)+len(userEnv))
	seen := make(map[string]struct{}, len(platformEnv)+len(userEnv))
	for _, env := range platformEnv {
		name := env.Name
		if _, ok := seen[name]; ok {
			continue
		}
		merged = append(merged, env)
		seen[name] = struct{}{}
	}
	for _, env := range userEnv {
		name := env.Name
		if _, ok := reservedEnvNames[name]; ok {
			log.Printf("assembler: warn: dropping reserved env %s for %s", name, owner)
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		merged = append(merged, env)
		seen[name] = struct{}{}
	}
	return merged
}

func appendPlatformEnvVar(envs []*runnerv1.EnvVar, env *runnerv1.EnvVar) []*runnerv1.EnvVar {
	result := make([]*runnerv1.EnvVar, 0, len(envs)+1)
	for _, existing := range envs {
		if existing.Name == env.Name {
			continue
		}
		result = append(result, existing)
	}
	result = append(result, env)
	return result
}

// No THREAD_ID: an instance serves every thread that reaches its inbox, so
// there is no one thread to name at startup. Pinning it here would scope the
// daemon to whichever thread happened to be first unacked when it launched.
func (a *Assembler) baseAgentEnvVars(agent *agentsv1.Agent, agentID, agentInstanceID uuid.UUID) []*runnerv1.EnvVar {
	gatewayURL := buildGatewayURL(a.cfg.AgentGatewayAddress)
	vars := []*runnerv1.EnvVar{
		{Name: "AGENT_INSTANCE_ID", Value: agentInstanceID.String()},
		{Name: "AGENT_ID", Value: agentID.String()},
		{Name: "ENVIRONMENT_ID", Value: agent.GetEnvironmentId()},
		{Name: "AGENT_NAME", Value: agent.GetName()},
		{Name: "AGENT_ROLE", Value: agent.GetRole()},
		{Name: "AGENT_MODEL", Value: agent.GetModel()},
		{Name: "AGENT_CONFIG", Value: agent.GetConfiguration()},
		{Name: "AGYN_ORGANIZATION_ID", Value: agent.GetOrganizationId()},
		{Name: "AGYN_IDENTITY_ID", Value: agentInstanceID.String()},
		{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress},
		{Name: "AGYN_GATEWAY_URL", Value: gatewayURL},
		{Name: "LLM_BASE_URL", Value: a.cfg.AgentLLMBaseURL},
	}
	if a.cfg.AgyndAgentsDirectAddress != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "AGYND_AGENTS_DIRECT_ADDRESS", Value: a.cfg.AgyndAgentsDirectAddress})
	}
	if a.cfg.AgyndRunnersDirectAddress != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "AGYND_RUNNERS_DIRECT_ADDRESS", Value: a.cfg.AgyndRunnersDirectAddress})
	}
	if a.cfg.AgentTracingAddress != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "TRACING_ADDRESS", Value: a.cfg.AgentTracingAddress})
		// The same address, for anything in the workload that exports OTLP on
		// its own rather than through agynd. It named a collector sidecar that
		// no longer runs -- spans were sent to a closed port and the export
		// blocked the turn that produced them.
		vars = append(vars, &runnerv1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://" + a.cfg.AgentTracingAddress})
	}
	return vars
}

type volumeResolver struct {
	agents  agentsClient
	ownerID uuid.UUID
	specs   map[string]*runnerv1.VolumeSpec
	cache   map[string]*agentsv1.Volume
	// environmentByName is the environment's own volumes, which an MCP names in
	// shared_volumes. Names are unique within an environment and deliberately
	// reusable across them, so resolution is late and by name.
	environmentByName map[string]*agentsv1.Volume
}

func newVolumeResolver(agents agentsClient, ownerID uuid.UUID) *volumeResolver {
	return &volumeResolver{
		agents:            agents,
		ownerID:           ownerID,
		specs:             map[string]*runnerv1.VolumeSpec{},
		cache:             map[string]*agentsv1.Volume{},
		environmentByName: map[string]*agentsv1.Volume{},
	}
}

// loadEnvironmentVolumes reads the volumes an environment declares. They mount
// into the main container of every workload running it, agent or sandbox.
func (v *volumeResolver) loadEnvironmentVolumes(ctx context.Context, environmentID string) ([]*runnerv1.VolumeMount, error) {
	volumes, err := v.listVolumes(ctx, &agentsv1.ListVolumesRequest{EnvironmentId: environmentID})
	if err != nil {
		return nil, fmt.Errorf("list environment volumes: %w", err)
	}
	mounts := make([]*runnerv1.VolumeMount, 0, len(volumes))
	for _, volume := range volumes {
		mount, err := v.mountFor(volume)
		if err != nil {
			return nil, err
		}
		v.environmentByName[volume.GetName()] = volume
		mounts = append(mounts, mount)
	}
	return mounts, nil
}

// mountsForMcp is the sidecar's own volumes plus the environment volumes it
// shares, at the same paths the main container sees them. A shared name that
// does not resolve, or a path colliding with one of its own, fails scheduling:
// a sidecar silently missing the files it was configured to read is worse.
func (v *volumeResolver) mountsForMcp(ctx context.Context, mcp *agentsv1.Mcp) ([]*runnerv1.VolumeMount, error) {
	own, err := v.listVolumes(ctx, &agentsv1.ListVolumesRequest{McpId: mcp.GetMeta().GetId()})
	if err != nil {
		return nil, fmt.Errorf("list mcp volumes: %w", err)
	}
	mounts := make([]*runnerv1.VolumeMount, 0, len(own)+len(mcp.GetSharedVolumes()))
	paths := map[string]string{}
	for _, volume := range own {
		mount, err := v.mountFor(volume)
		if err != nil {
			return nil, err
		}
		paths[mount.MountPath] = volume.GetName()
		mounts = append(mounts, mount)
	}
	for _, name := range mcp.GetSharedVolumes() {
		shared, ok := v.environmentByName[name]
		if !ok {
			return nil, fmt.Errorf("mcp %s shares volume %q, which the environment does not declare", mcp.GetName(), name)
		}
		mount, err := v.mountFor(shared)
		if err != nil {
			return nil, err
		}
		if other, clash := paths[mount.MountPath]; clash {
			return nil, fmt.Errorf("mcp %s mounts %q at %s and shares %q at the same path", mcp.GetName(), other, mount.MountPath, name)
		}
		paths[mount.MountPath] = name
		mounts = append(mounts, mount)
	}
	return mounts, nil
}

func (v *volumeResolver) listVolumes(ctx context.Context, req *agentsv1.ListVolumesRequest) ([]*agentsv1.Volume, error) {
	volumes := []*agentsv1.Volume{}
	pageToken := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := v.agents.ListVolumes(rctx, &agentsv1.ListVolumesRequest{
			EnvironmentId: req.GetEnvironmentId(),
			McpId:         req.GetMcpId(),
			PageSize:      listPageSize,
			PageToken:     pageToken,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, page.GetVolumes()...)
		pageToken = page.GetNextPageToken()
		if pageToken == "" {
			return volumes, nil
		}
	}
}

func (v *volumeResolver) mountFor(volume *agentsv1.Volume) (*runnerv1.VolumeMount, error) {
	if volume == nil {
		return nil, fmt.Errorf("volume is nil")
	}
	mountPath := volume.GetMountPath()
	if mountPath == "" {
		return nil, fmt.Errorf("volume %s mount_path is empty", volume.GetMeta().GetId())
	}
	volumeID, err := uuidutil.ParseUUID(volume.GetMeta().GetId(), "volume.id")
	if err != nil {
		return nil, err
	}
	v.cache[volumeID.String()] = volume
	spec := v.ensureSpec(volumeID, volume)
	return &runnerv1.VolumeMount{Volume: spec.Name, MountPath: mountPath}, nil
}

func (v *volumeResolver) Specs() []*runnerv1.VolumeSpec {
	if len(v.specs) == 0 {
		return nil
	}
	specs := make([]*runnerv1.VolumeSpec, 0, len(v.specs))
	for _, spec := range v.specs {
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

func (v *volumeResolver) PersistentVolumes() ([]PersistentVolumeInfo, error) {
	if len(v.specs) == 0 {
		return nil, nil
	}
	volumes := make([]PersistentVolumeInfo, 0, len(v.specs))
	for volumeID, spec := range v.specs {
		if spec == nil {
			return nil, fmt.Errorf("volume spec missing for %s", volumeID)
		}
		volume := v.cache[volumeID]
		if volume == nil {
			return nil, fmt.Errorf("volume %s missing", volumeID)
		}
		if !volume.GetPersistent() {
			continue
		}
		parsedID, err := uuidutil.ParseUUID(volumeID, "volume.id")
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, PersistentVolumeInfo{ID: parsedID, AgentInstanceID: v.ownerID, Volume: volume, Spec: spec})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].ID.String() < volumes[j].ID.String() })
	return volumes, nil
}

type mcpAssignment struct {
	mcp  *agentsv1.Mcp
	id   string
	name string
	port int
}

func (a *Assembler) resolveSidecarResources(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, envReq *agentsv1.ListEnvsRequest, mcp *agentsv1.Mcp) ([]*runnerv1.EnvVar, []*runnerv1.VolumeMount, error) {
	vars, err := a.listEnvs(ctx, envReq)
	if err != nil {
		return nil, nil, fmt.Errorf("list sidecar envs: %w", err)
	}
	envVars, err := resolver.ResolveEnvVars(ctx, vars)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sidecar envs: %w", err)
	}
	mounts, err := volumeResolver.mountsForMcp(ctx, mcp)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sidecar mounts: %w", err)
	}
	return envVars, mounts, nil
}

func (v *volumeResolver) ensureSpec(volumeID uuid.UUID, volume *agentsv1.Volume) *runnerv1.VolumeSpec {
	key := volumeID.String()
	if spec, ok := v.specs[key]; ok {
		return spec
	}
	shortVolume := key[:8]
	spec := &runnerv1.VolumeSpec{
		Name: fmt.Sprintf("vol-%s", shortVolume),
		Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
	}
	if volume.GetPersistent() {
		spec.Kind = runnerv1.VolumeKind_VOLUME_KIND_NAMED
		instancePrefix := v.ownerID.String()[:12]
		volumePrefix := key[:12]
		spec.PersistentName = fmt.Sprintf("pv-%s-%s", instancePrefix, volumePrefix)
		spec.Size = volume.GetSize()
		spec.StorageClass = volume.GetStorageClass()
	}
	v.specs[key] = spec
	return spec
}
