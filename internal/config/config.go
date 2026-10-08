package config

import (
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"k8s.io/client-go/tools/leaderelection"
)

const (
	// WorkloadNetworkModeTProxy is the upstream default: a NET_ADMIN
	// tunneler sidecar intercepts DNS and TCP for the whole Pod, which
	// therefore cannot run under the restricted Pod Security Standard.
	WorkloadNetworkModeTProxy = "tproxy"
	// WorkloadNetworkModeExplicitProxy runs the unprivileged workload-proxy
	// sidecar: loopback forwards for the platform endpoints and an explicit
	// HTTP(S) proxy for everything else, with no capability, no resolver or
	// hosts file writes and no DNS override. It requires ZITI_ENABLED and a
	// runner building restricted Pods.
	WorkloadNetworkModeExplicitProxy = "explicit-proxy"

	defaultWorkloadProxyEntrypoint = "/app/workload-proxy"
)

type Config struct {
	ThreadsAddress       string
	NotificationsAddress string
	AgentsAddress        string
	SecretsAddress       string
	LLMAddress           string
	RunnerAddress        string
	RunnersAddress       string
	// RunnersTokenFile is the projected ServiceAccount token (audience
	// agyn-runners) attached to Runners calls only. Empty sends none.
	RunnersTokenFile                    string
	MeteringServiceAddress              string
	MeteringSampleInterval              time.Duration
	ZitiEnabled                         bool
	ZitiManagementAddress               string
	GroupsAddress                       string
	GroupSyncEnabled                    bool
	NATSURL                             string
	ZitiLeaseRenewalInterval            time.Duration
	ZitiEnrollmentTimeout               time.Duration
	ZitiSidecarImage                    string
	WorkloadDNSUpstream                 string
	ZitiEnrollmentDNSUpstream           string
	ZitiEnrollmentControllerResolveHost string
	ZitiEnrollmentControllerPort        string
	ZitiRuntimeControllerResolveHost    string
	ZitiRuntimeControllerPort           string
	AgentGatewayAddress                 string
	AgentTracingAddress                 string
	AgentLLMBaseURL                     string
	AgyndAgentsDirectAddress            string
	AgyndRunnersDirectAddress           string
	// The two platform init images, injected into every workload. They are not
	// a configuration surface: an agent's behaviour is configured through the
	// agent, and the platform's own binaries are not a choice anyone makes.
	AgyndCLIInitImage string
	AgynCLIInitImage  string
	// Where catalog image references are rewritten to. Empty leaves references
	// as the catalog resolves them, which is the pre-proxy behaviour.
	ImageProxyHost string
	// Optional service addresses for the catalog path. Empty leaves the
	// pre-catalog behaviour in place.
	ImagesAddress             string
	OrganizationsAddress      string
	ImageProxyAddress         string
	SandboxWorkspaceSizeGB    string
	PollInterval              time.Duration
	WorkloadReconcileInterval time.Duration
	IdleTimeout               time.Duration
	StopTimeoutSec            uint32
	StopInactiveInstances     bool
	LeaseName                 string
	LeaseNamespace            string
	// LeaderElection holds the timings of the agents-orchestrator Lease; see
	// LeaderElectionTimings.
	LeaderElection    LeaderElectionTimings
	EgressCANamespace string
	// PlatformIdentityID is the identity this process acts as when it calls a
	// service that authorizes its caller. Identity registers it from the same
	// value and grants it admin on the cluster; nothing here may act as anyone
	// else.
	PlatformIdentityID uuid.UUID
	// WorkloadNetworkMode selects how a task Pod reaches the overlay; see
	// WorkloadNetworkModeTProxy and WorkloadNetworkModeExplicitProxy.
	WorkloadNetworkMode string
	// WorkloadProxyImage and WorkloadProxyEntrypoint run k8s-runner's
	// workload-proxy (enroll, serve, wait) in explicit-proxy mode.
	WorkloadProxyImage      string
	WorkloadProxyEntrypoint string
	// WorkloadProxyDirectEgress lets explicit-proxy workloads reach public
	// internet addresses no egress rule intercepts (serve --direct-egress);
	// WorkloadProxyDirectDeny adds addresses or CIDRs it must never reach.
	WorkloadProxyDirectEgress bool
	WorkloadProxyDirectDeny   []string

	// FailedWorkloadRetention keeps a failed agent workload's Pod this long
	// for investigation (FAILED_WORKLOAD_RETENTION, default 0: removed at
	// once). A retained Pod keeps its capacity slot and its agent instance is
	// not restarted until it is removed.
	FailedWorkloadRetention time.Duration
	// FailedWorkloadRetentionMax bounds how many failed Pods are retained at
	// once (FAILED_WORKLOAD_RETENTION_MAX, default 1).
	FailedWorkloadRetentionMax int
	// FailedWorkloadEvidenceLogBytes is the newest output kept per container
	// in a failed workload's record (FAILED_WORKLOAD_EVIDENCE_LOG_BYTES,
	// default 65536, at most 262144; 0 records container statuses only).
	FailedWorkloadEvidenceLogBytes int
}

func FromEnv() (Config, error) {
	cfg := Config{}
	cfg.ThreadsAddress = os.Getenv("THREADS_ADDRESS")
	if cfg.ThreadsAddress == "" {
		cfg.ThreadsAddress = "threads:50051"
	}
	cfg.NotificationsAddress = os.Getenv("NOTIFICATIONS_ADDRESS")
	if cfg.NotificationsAddress == "" {
		cfg.NotificationsAddress = "notifications:50051"
	}
	cfg.AgentsAddress = os.Getenv("AGENTS_ADDRESS")
	if cfg.AgentsAddress == "" {
		cfg.AgentsAddress = "agents:50051"
	}
	cfg.LLMAddress = os.Getenv("LLM_SERVICE_ADDRESS")
	if cfg.LLMAddress == "" {
		cfg.LLMAddress = "llm:50051"
	}
	cfg.SecretsAddress = os.Getenv("SECRETS_ADDRESS")
	if cfg.SecretsAddress == "" {
		cfg.SecretsAddress = "secrets:50051"
	}
	cfg.RunnerAddress = os.Getenv("RUNNER_ADDRESS")
	if cfg.RunnerAddress == "" {
		cfg.RunnerAddress = "k8s-runner:50051"
	}
	cfg.RunnersAddress = os.Getenv("RUNNERS_ADDRESS")
	if cfg.RunnersAddress == "" {
		cfg.RunnersAddress = "runners:50051"
	}
	cfg.RunnersTokenFile = strings.TrimSpace(os.Getenv("RUNNERS_TOKEN_FILE"))
	cfg.MeteringServiceAddress = os.Getenv("METERING_SERVICE_ADDRESS")
	if cfg.MeteringServiceAddress == "" {
		cfg.MeteringServiceAddress = "metering:50051"
	}
	meteringSampleInterval := os.Getenv("METERING_SAMPLE_INTERVAL")
	if meteringSampleInterval == "" {
		cfg.MeteringSampleInterval = time.Minute
	} else {
		parsed, err := time.ParseDuration(meteringSampleInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse METERING_SAMPLE_INTERVAL: %w", err)
		}
		cfg.MeteringSampleInterval = parsed
	}
	if cfg.MeteringSampleInterval <= 0 {
		return Config{}, fmt.Errorf("METERING_SAMPLE_INTERVAL must be greater than 0")
	}
	zitiEnabled := os.Getenv("ZITI_ENABLED")
	if zitiEnabled != "" {
		parsed, err := strconv.ParseBool(zitiEnabled)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENABLED: %w", err)
		}
		cfg.ZitiEnabled = parsed
	}
	cfg.AgentGatewayAddress = os.Getenv("AGENT_GATEWAY_ADDRESS")
	if cfg.AgentGatewayAddress == "" {
		if cfg.ZitiEnabled {
			cfg.AgentGatewayAddress = "gateway.agyn:443"
		} else {
			cfg.AgentGatewayAddress = "gateway:8080"
		}
	}
	cfg.AgentTracingAddress = os.Getenv("AGENT_TRACING_ADDRESS")
	if cfg.AgentTracingAddress == "" {
		if cfg.ZitiEnabled {
			cfg.AgentTracingAddress = "tracing.agyn:443"
		} else {
			cfg.AgentTracingAddress = "tracing:50051"
		}
	}
	cfg.AgentLLMBaseURL = os.Getenv("AGENT_LLM_BASE_URL")
	if cfg.AgentLLMBaseURL == "" {
		if cfg.ZitiEnabled {
			cfg.AgentLLMBaseURL = "http://llm-proxy.agyn/v1"
		} else {
			cfg.AgentLLMBaseURL = "http://llm-proxy-llm-proxy.platform.svc.cluster.local:8080/v1"
		}
	}
	cfg.AgyndAgentsDirectAddress = os.Getenv("AGYND_AGENTS_DIRECT_ADDRESS")
	cfg.AgyndRunnersDirectAddress = os.Getenv("AGYND_RUNNERS_DIRECT_ADDRESS")
	cfg.AgyndCLIInitImage = os.Getenv("AGYND_CLI_INIT_IMAGE")
	cfg.AgynCLIInitImage = os.Getenv("AGYN_CLI_INIT_IMAGE")
	cfg.ImageProxyHost = os.Getenv("IMAGE_PROXY_HOST")
	cfg.ImagesAddress = os.Getenv("IMAGES_ADDRESS")
	cfg.OrganizationsAddress = os.Getenv("ORGANIZATIONS_ADDRESS")
	cfg.ImageProxyAddress = os.Getenv("IMAGE_PROXY_ADDRESS")
	cfg.SandboxWorkspaceSizeGB = os.Getenv("SANDBOX_WORKSPACE_SIZE_GB")
	if cfg.SandboxWorkspaceSizeGB == "" {
		cfg.SandboxWorkspaceSizeGB = "10"
	}
	if _, err := strconv.ParseFloat(cfg.SandboxWorkspaceSizeGB, 64); err != nil {
		return Config{}, fmt.Errorf("parse SANDBOX_WORKSPACE_SIZE_GB: %w", err)
	}
	var err error
	if err != nil {
		return Config{}, err
	}
	cfg.ZitiManagementAddress = os.Getenv("ZITI_MANAGEMENT_ADDRESS")
	if cfg.ZitiManagementAddress == "" {
		cfg.ZitiManagementAddress = "ziti-management:50051"
	}
	groupSyncEnabled := os.Getenv("GROUP_SYNC_ENABLED")
	if groupSyncEnabled != "" {
		parsed, err := strconv.ParseBool(groupSyncEnabled)
		if err != nil {
			return Config{}, fmt.Errorf("parse GROUP_SYNC_ENABLED: %w", err)
		}
		cfg.GroupSyncEnabled = parsed
	}
	cfg.GroupsAddress = os.Getenv("GROUPS_ADDRESS")
	if cfg.GroupsAddress == "" {
		cfg.GroupsAddress = "groups:50051"
	}
	cfg.NATSURL = os.Getenv("NATS_URL")
	zitiLeaseRenewalInterval := os.Getenv("ZITI_LEASE_RENEWAL_INTERVAL")
	if zitiLeaseRenewalInterval == "" {
		cfg.ZitiLeaseRenewalInterval = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(zitiLeaseRenewalInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_LEASE_RENEWAL_INTERVAL: %w", err)
		}
		cfg.ZitiLeaseRenewalInterval = parsed
	}
	if cfg.ZitiLeaseRenewalInterval <= 0 {
		return Config{}, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be greater than 0")
	}
	zitiEnrollmentTimeout := os.Getenv("ZITI_ENROLLMENT_TIMEOUT")
	if zitiEnrollmentTimeout == "" {
		cfg.ZitiEnrollmentTimeout = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(zitiEnrollmentTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENROLLMENT_TIMEOUT: %w", err)
		}
		cfg.ZitiEnrollmentTimeout = parsed
	}
	if cfg.ZitiEnrollmentTimeout <= 0 {
		return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be greater than 0")
	}
	cfg.ZitiSidecarImage = os.Getenv("ZITI_SIDECAR_IMAGE")
	if cfg.ZitiSidecarImage == "" {
		cfg.ZitiSidecarImage = "openziti/ziti-tunnel:2.0.0-pre10"
	}
	cfg.WorkloadNetworkMode = strings.ToLower(strings.TrimSpace(os.Getenv("WORKLOAD_NETWORK_MODE")))
	if cfg.WorkloadNetworkMode == "" {
		cfg.WorkloadNetworkMode = WorkloadNetworkModeTProxy
	}
	cfg.WorkloadProxyImage = strings.TrimSpace(os.Getenv("WORKLOAD_PROXY_IMAGE"))
	cfg.WorkloadProxyEntrypoint = strings.TrimSpace(os.Getenv("WORKLOAD_PROXY_ENTRYPOINT"))
	if cfg.WorkloadProxyEntrypoint == "" {
		cfg.WorkloadProxyEntrypoint = defaultWorkloadProxyEntrypoint
	}
	if raw := strings.TrimSpace(os.Getenv("WORKLOAD_PROXY_DIRECT_EGRESS")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("WORKLOAD_PROXY_DIRECT_EGRESS must be a boolean")
		}
		cfg.WorkloadProxyDirectEgress = enabled
	}
	for _, value := range strings.Split(os.Getenv("WORKLOAD_PROXY_DIRECT_DENY"), ",") {
		if value = strings.TrimSpace(value); value == "" {
			continue
		}
		if _, err := netip.ParsePrefix(value); err != nil {
			if _, err := netip.ParseAddr(value); err != nil {
				return Config{}, fmt.Errorf("WORKLOAD_PROXY_DIRECT_DENY entry %q is not an address or CIDR", value)
			}
		}
		cfg.WorkloadProxyDirectDeny = append(cfg.WorkloadProxyDirectDeny, value)
	}
	if len(cfg.WorkloadProxyDirectDeny) > 0 && !cfg.WorkloadProxyDirectEgress {
		return Config{}, fmt.Errorf("WORKLOAD_PROXY_DIRECT_DENY requires WORKLOAD_PROXY_DIRECT_EGRESS=true")
	}
	switch cfg.WorkloadNetworkMode {
	case WorkloadNetworkModeTProxy:
		if cfg.WorkloadProxyDirectEgress {
			return Config{}, fmt.Errorf("WORKLOAD_PROXY_DIRECT_EGRESS requires WORKLOAD_NETWORK_MODE=explicit-proxy")
		}
	case WorkloadNetworkModeExplicitProxy:
		// Fail here rather than assemble a Pod with no overlay at all, or one
		// that silently falls back to the privileged tunneler.
		if !cfg.ZitiEnabled {
			return Config{}, fmt.Errorf("WORKLOAD_NETWORK_MODE=explicit-proxy requires ZITI_ENABLED=true")
		}
		if cfg.WorkloadProxyImage == "" {
			return Config{}, fmt.Errorf("WORKLOAD_PROXY_IMAGE is required when WORKLOAD_NETWORK_MODE=explicit-proxy")
		}
		if strings.ContainsAny(cfg.WorkloadProxyEntrypoint, " \t\r\n") || !strings.HasPrefix(cfg.WorkloadProxyEntrypoint, "/") {
			return Config{}, fmt.Errorf("WORKLOAD_PROXY_ENTRYPOINT must be one absolute path")
		}
		// The LLM forward is byte-transparent TCP to the overlay service, so
		// the workload must speak the base URL's own plaintext protocol to it.
		if parsed, err := url.Parse(cfg.AgentLLMBaseURL); err != nil || parsed.Scheme != "http" || parsed.Hostname() == "" {
			return Config{}, fmt.Errorf("WORKLOAD_NETWORK_MODE=explicit-proxy requires an http:// AGENT_LLM_BASE_URL")
		}
	default:
		return Config{}, fmt.Errorf("WORKLOAD_NETWORK_MODE must be %s or %s", WorkloadNetworkModeTProxy, WorkloadNetworkModeExplicitProxy)
	}
	clusterDNS := os.Getenv("CLUSTER_DNS")
	cfg.WorkloadDNSUpstream = os.Getenv("WORKLOAD_DNS_UPSTREAM")
	if cfg.WorkloadDNSUpstream == "" {
		cfg.WorkloadDNSUpstream = clusterDNS
	}
	if cfg.WorkloadDNSUpstream == "" {
		cfg.WorkloadDNSUpstream = "10.43.0.10"
	}
	cfg.ZitiEnrollmentDNSUpstream = os.Getenv("ZITI_ENROLLMENT_DNS_UPSTREAM")
	if cfg.ZitiEnrollmentDNSUpstream == "" {
		cfg.ZitiEnrollmentDNSUpstream = clusterDNS
	}
	if cfg.ZitiEnrollmentDNSUpstream == "" {
		cfg.ZitiEnrollmentDNSUpstream = "10.43.0.10"
	}
	cfg.ZitiEnrollmentControllerResolveHost = os.Getenv("ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST")
	if cfg.ZitiEnrollmentControllerResolveHost == "" {
		cfg.ZitiEnrollmentControllerResolveHost = "ziti-controller-client.ziti.svc.cluster.local"
	}
	// Deliberately not defaulted: empty takes the port the JWT advertises, which
	// is the one the controller's Service listens on -- the Ziti chart derives
	// both from clientApi.advertisedPort. A default is a second source of truth
	// for a number the controller already states.
	cfg.ZitiEnrollmentControllerPort = os.Getenv("ZITI_ENROLLMENT_CONTROLLER_PORT")
	if cfg.ZitiEnrollmentControllerPort != "" {
		parsed, err := strconv.ParseUint(cfg.ZitiEnrollmentControllerPort, 10, 16)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENROLLMENT_CONTROLLER_PORT: %w", err)
		}
		if parsed == 0 {
			return Config{}, fmt.Errorf("ZITI_ENROLLMENT_CONTROLLER_PORT must be greater than 0")
		}
	}
	cfg.ZitiRuntimeControllerResolveHost = os.Getenv("ZITI_RUNTIME_CONTROLLER_RESOLVE_HOST")
	if cfg.ZitiRuntimeControllerResolveHost == "" {
		cfg.ZitiRuntimeControllerResolveHost = "ziti-controller-client.ziti.svc.cluster.local"
	}
	// Not defaulted, for the same reason as the enrollment port above.
	cfg.ZitiRuntimeControllerPort = os.Getenv("ZITI_RUNTIME_CONTROLLER_PORT")
	if cfg.ZitiRuntimeControllerPort != "" {
		parsed, err := strconv.ParseUint(cfg.ZitiRuntimeControllerPort, 10, 16)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_RUNTIME_CONTROLLER_PORT: %w", err)
		}
		if parsed == 0 {
			return Config{}, fmt.Errorf("ZITI_RUNTIME_CONTROLLER_PORT must be greater than 0")
		}
	}
	// Required, and deliberately not defaulted: this process subscribes and
	// calls as this identity, and a wrong or absent one should stop it here
	// rather than surface later as a permission denied nobody can place.
	platformIdentityID := strings.TrimSpace(os.Getenv("PLATFORM_IDENTITY_ID"))
	if platformIdentityID == "" {
		return Config{}, fmt.Errorf("PLATFORM_IDENTITY_ID is required")
	}
	parsedPlatformIdentityID, err := uuid.Parse(platformIdentityID)
	if err != nil {
		return Config{}, fmt.Errorf("parse PLATFORM_IDENTITY_ID: %w", err)
	}
	cfg.PlatformIdentityID = parsedPlatformIdentityID

	pollInterval := os.Getenv("POLL_INTERVAL")
	if pollInterval == "" {
		cfg.PollInterval = 30 * time.Second
	} else {
		parsed, err := time.ParseDuration(pollInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse POLL_INTERVAL: %w", err)
		}
		cfg.PollInterval = parsed
	}

	workloadReconcileInterval := os.Getenv("WORKLOAD_RECONCILE_INTERVAL")
	if workloadReconcileInterval == "" {
		cfg.WorkloadReconcileInterval = time.Minute
	} else {
		parsed, err := time.ParseDuration(workloadReconcileInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse WORKLOAD_RECONCILE_INTERVAL: %w", err)
		}
		cfg.WorkloadReconcileInterval = parsed
	}
	if cfg.WorkloadReconcileInterval <= 0 {
		return Config{}, fmt.Errorf("WORKLOAD_RECONCILE_INTERVAL must be greater than 0")
	}

	idleTimeout := os.Getenv("IDLE_TIMEOUT")
	if idleTimeout == "" {
		cfg.IdleTimeout = 5 * time.Minute
	} else {
		parsed, err := time.ParseDuration(idleTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse IDLE_TIMEOUT: %w", err)
		}
		cfg.IdleTimeout = parsed
	}

	stopTimeout := os.Getenv("STOP_TIMEOUT_SEC")
	if stopTimeout == "" {
		cfg.StopTimeoutSec = 30
	} else {
		parsed, err := strconv.ParseUint(stopTimeout, 10, 32)
		if err != nil {
			return Config{}, fmt.Errorf("parse STOP_TIMEOUT_SEC: %w", err)
		}
		cfg.StopTimeoutSec = uint32(parsed)
	}
	if raw := os.Getenv("STOP_INACTIVE_INSTANCES"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse STOP_INACTIVE_INSTANCES: %w", err)
		}
		cfg.StopInactiveInstances = parsed
	}

	if err := parseFailedWorkloadConfig(&cfg); err != nil {
		return Config{}, err
	}

	cfg.LeaseName = os.Getenv("LEASE_NAME")
	if cfg.LeaseName == "" {
		cfg.LeaseName = "agents-orchestrator"
	}
	cfg.LeaseNamespace = os.Getenv("LEASE_NAMESPACE")
	if err := parseLeaderElection(&cfg); err != nil {
		return Config{}, err
	}
	cfg.EgressCANamespace = os.Getenv("EGRESS_CA_NAMESPACE")
	return cfg, nil
}

// LeaderElectionTimings are the client-go leader election timings of the
// orchestrator's Lease (Go durations):
//
//   - LEADER_ELECTION_LEASE_DURATION (default 15s): how long a standby waits
//     after the last renewal before it takes over;
//   - LEADER_ELECTION_RENEW_DEADLINE (default 10s): how long the leader keeps
//     retrying a failed renewal before it gives up leadership;
//   - LEADER_ELECTION_RETRY_PERIOD (default 2s): the interval between attempts.
//
// The defaults are the values this service always used. An API server whose
// storage stalls for longer than the renew deadline makes the leader give up
// the Lease, and the process then exits (cmd/orchestrator); installations on
// slow storage can lengthen all three. Every value is bounded to
// maxLeaderElectionDuration so a unit typo cannot leave a crashed leader's
// Lease blocking its replacement for hours.
type LeaderElectionTimings struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

const (
	defaultLeaderElectionLeaseDuration = 15 * time.Second
	defaultLeaderElectionRenewDeadline = 10 * time.Second
	defaultLeaderElectionRetryPeriod   = 2 * time.Second
	maxLeaderElectionDuration          = 10 * time.Minute
)

func parseLeaderElection(cfg *Config) error {
	timings := LeaderElectionTimings{
		LeaseDuration: defaultLeaderElectionLeaseDuration,
		RenewDeadline: defaultLeaderElectionRenewDeadline,
		RetryPeriod:   defaultLeaderElectionRetryPeriod,
	}
	for _, setting := range []struct {
		name  string
		value *time.Duration
	}{
		{"LEADER_ELECTION_LEASE_DURATION", &timings.LeaseDuration},
		{"LEADER_ELECTION_RENEW_DEADLINE", &timings.RenewDeadline},
		{"LEADER_ELECTION_RETRY_PERIOD", &timings.RetryPeriod},
	} {
		raw := strings.TrimSpace(os.Getenv(setting.name))
		if raw == "" {
			continue
		}
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("parse %s: %w", setting.name, err)
		}
		if parsed <= 0 || parsed > maxLeaderElectionDuration {
			return fmt.Errorf("%s must be greater than 0 and at most %s", setting.name, maxLeaderElectionDuration)
		}
		*setting.value = parsed
	}
	// The same ordering client-go's NewLeaderElector enforces, reported here
	// with the variable names instead of at election start.
	if timings.LeaseDuration <= timings.RenewDeadline {
		return fmt.Errorf("LEADER_ELECTION_LEASE_DURATION (%s) must be greater than LEADER_ELECTION_RENEW_DEADLINE (%s)", timings.LeaseDuration, timings.RenewDeadline)
	}
	if timings.RenewDeadline <= time.Duration(leaderelection.JitterFactor*float64(timings.RetryPeriod)) {
		return fmt.Errorf("LEADER_ELECTION_RENEW_DEADLINE (%s) must be greater than %.1f x LEADER_ELECTION_RETRY_PERIOD (%s)", timings.RenewDeadline, leaderelection.JitterFactor, timings.RetryPeriod)
	}
	cfg.LeaderElection = timings
	return nil
}

// Failed-workload retention bounds. The evidence bound is Runners' own
// per-container limit on Container.output_tail.
const (
	defaultFailedWorkloadRetentionMax     = 1
	defaultFailedWorkloadEvidenceLogBytes = 64 * 1024
	maxFailedWorkloadEvidenceLogBytes     = 256 * 1024
	maxFailedWorkloadRetention            = 24 * time.Hour
	maxFailedWorkloadRetentionMax         = 16
)

func parseFailedWorkloadConfig(cfg *Config) error {
	if raw := strings.TrimSpace(os.Getenv("FAILED_WORKLOAD_RETENTION")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("parse FAILED_WORKLOAD_RETENTION: %w", err)
		}
		if parsed < 0 || parsed > maxFailedWorkloadRetention {
			return fmt.Errorf("FAILED_WORKLOAD_RETENTION must be between 0 and %s", maxFailedWorkloadRetention)
		}
		cfg.FailedWorkloadRetention = parsed
	}
	cfg.FailedWorkloadRetentionMax = defaultFailedWorkloadRetentionMax
	if raw := strings.TrimSpace(os.Getenv("FAILED_WORKLOAD_RETENTION_MAX")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("parse FAILED_WORKLOAD_RETENTION_MAX: %w", err)
		}
		if parsed < 0 || parsed > maxFailedWorkloadRetentionMax {
			return fmt.Errorf("FAILED_WORKLOAD_RETENTION_MAX must be between 0 and %d", maxFailedWorkloadRetentionMax)
		}
		cfg.FailedWorkloadRetentionMax = parsed
	}
	cfg.FailedWorkloadEvidenceLogBytes = defaultFailedWorkloadEvidenceLogBytes
	if raw := strings.TrimSpace(os.Getenv("FAILED_WORKLOAD_EVIDENCE_LOG_BYTES")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("parse FAILED_WORKLOAD_EVIDENCE_LOG_BYTES: %w", err)
		}
		if parsed < 0 || parsed > maxFailedWorkloadEvidenceLogBytes {
			return fmt.Errorf("FAILED_WORKLOAD_EVIDENCE_LOG_BYTES must be between 0 and %d", maxFailedWorkloadEvidenceLogBytes)
		}
		cfg.FailedWorkloadEvidenceLogBytes = parsed
	}
	return nil
}

func parseUUIDList(raw string, name string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		parsed, err := uuid.Parse(value)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		values = append(values, parsed.String())
	}
	return values, nil
}
