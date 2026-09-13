package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	ThreadsAddress                      string
	NotificationsAddress                string
	AgentsAddress                       string
	SecretsAddress                      string
	LLMAddress                          string
	RunnerAddress                       string
	RunnersAddress                      string
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
	EgressCANamespace         string
	// PlatformIdentityID is the identity this process acts as when it calls a
	// service that authorizes its caller. Identity registers it from the same
	// value and grants it admin on the cluster; nothing here may act as anyone
	// else.
	PlatformIdentityID uuid.UUID
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

	cfg.LeaseName = os.Getenv("LEASE_NAME")
	if cfg.LeaseName == "" {
		cfg.LeaseName = "agents-orchestrator"
	}
	cfg.LeaseNamespace = os.Getenv("LEASE_NAMESPACE")
	cfg.EgressCANamespace = os.Getenv("EGRESS_CA_NAMESPACE")
	return cfg, nil
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
