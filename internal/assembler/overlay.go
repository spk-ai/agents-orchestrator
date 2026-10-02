package assembler

import (
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
)

// The explicit-proxy contract shared with k8s-runner's workload-proxy
// (cmd/workload-proxy). Every listener is a literal loopback address inside the
// Pod; nothing here is reachable from another Pod.
//
// @see k8s-runner::cmd/workload-proxy/main
const (
	workloadProxyListen  = "127.0.0.1:18080"
	gatewayForwardListen = "127.0.0.1:18443"
	llmForwardListen     = "127.0.0.1:18081"
	tracingForwardListen = "127.0.0.1:18444"
	workloadProxyURL     = "http://" + workloadProxyListen
	workloadIdentityFile = zitiIdentityMountPath + "/" + ZitiIdentityBasename + ".json"
	// Loopback is never proxied: the platform endpoints are fixed forwards,
	// and Go and most clients bypass a proxy for these anyway. .agyn is
	// deliberately absent -- overlay names resolve nowhere but the proxy.
	workloadNoProxyBase = "127.0.0.1,localhost,::1"
	// The JVM reads none of the variables above; composed into
	// JAVA_TOOL_OPTIONS so Gradle, Maven and sdkmanager's JVM take the proxy.
	workloadJavaProxyOptions = "-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=18080 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=18080 -Dhttp.nonProxyHosts=localhost|127.0.0.1|[::1]"
	// A restricted Pod runs as a UID with no passwd entry, whose HOME would
	// be "/". The main container gets a private emptyDir; an MCP sidecar,
	// which has no volume of its own, gets its writable /tmp. Either is only
	// a default: an environment or agent env named HOME wins.
	agynHomeVolumeName        = "agyn-home"
	agynHomeMountPath         = "/home/agyn"
	mcpDefaultHome            = "/tmp"
	readOnlyRootFilesystemKey = "read_only_root_filesystem"
	// AgynNetworkModeEnvVar tells agynd which network contract it runs under.
	AgynNetworkModeEnvVar = "AGYN_NETWORK_MODE"
)

// workloadProxyEnvNames are platform-owned in explicit-proxy mode: a user value
// is replaced, never merged, so a workload cannot route around the proxy by
// configuration. NO_PROXY and JAVA_TOOL_OPTIONS are composed instead.
var workloadProxyEnvNames = []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"}

// overlayPlan is how a task Pod reaches the overlay, built once for agents and
// sandboxes alike. Containers keep the names the reconciler (JWT attachment)
// and diagnostics look for.
type overlayPlan struct {
	enroll  *runnerv1.ContainerSpec
	sidecar *runnerv1.ContainerSpec
	wait    *runnerv1.ContainerSpec
	volumes []*runnerv1.VolumeSpec
	// mainMounts are added to the main container only.
	mainMounts []*runnerv1.VolumeMount
	dnsConfig  *runnerv1.DnsConfig
	explicit   bool
}

// withInit orders the overlay around the platform init containers: enrol and
// start the sidecar first, unload binaries while the overlay comes up, and
// wait last, when there is usually nothing left to wait for.
func (p *overlayPlan) withInit(platform []*runnerv1.ContainerSpec) []*runnerv1.ContainerSpec {
	if p == nil {
		return platform
	}
	return append(append([]*runnerv1.ContainerSpec{p.enroll, p.sidecar}, platform...), p.wait)
}

func (a *Assembler) explicitProxy() bool {
	return a.cfg.ZitiEnabled && a.cfg.WorkloadNetworkMode == config.WorkloadNetworkModeExplicitProxy
}

// overlay returns nil when Ziti is disabled.
func (a *Assembler) overlay() (*overlayPlan, error) {
	if !a.cfg.ZitiEnabled {
		return nil, nil
	}
	if a.explicitProxy() {
		return a.explicitProxyOverlay()
	}
	return a.tproxyOverlay()
}

// tproxyOverlay is the upstream overlay: a NET_ADMIN tunneler that rewrites
// the Pod's resolver and diverts TCP with iptables. Kept unchanged for
// installations that have not moved to explicit-proxy.
func (a *Assembler) tproxyOverlay() (*overlayPlan, error) {
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
	return &overlayPlan{
		enroll:  zitiEnroll,
		sidecar: zitiSidecar,
		wait:    zitiWait,
		volumes: []*runnerv1.VolumeSpec{{Name: zitiIdentityVolumeName, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL}},
		// Parallel resolvers (including musl) can bypass interception if an
		// ordinary nameserver is listed here. The tunnel forwards other names.
		dnsConfig: &runnerv1.DnsConfig{
			Nameservers: []string{zitiDNSNameserver},
			Searches:    []string{zitiDNSSearchService, zitiDNSSearchCluster},
		},
	}, nil
}

// explicitProxyOverlay runs k8s-runner's workload-proxy in all three overlay
// roles. No container requests a capability, writes /etc/resolv.conf or
// /etc/hosts, or overrides the Pod's DNS: enrollment reaches the controller by
// ordinary cluster DNS, and workload traffic reaches the overlay only through
// the loopback proxy and forwards. Overlay containers carry no proxy variables
// and no egress CA, so controller traffic never loops into the proxy.
func (a *Assembler) explicitProxyOverlay() (*overlayPlan, error) {
	gateway, err := overlayAuthority(a.cfg.AgentGatewayAddress, "AGENT_GATEWAY_ADDRESS")
	if err != nil {
		return nil, err
	}
	llm, err := zitiServiceWaitTarget(a.cfg.AgentLLMBaseURL)
	if err != nil {
		return nil, err
	}
	llmTarget, err := overlayAuthority(net.JoinHostPort(llm.host, llm.port), "AGENT_LLM_BASE_URL")
	if err != nil {
		return nil, err
	}
	serve := []string{"serve", "--identity", workloadIdentityFile, "--listen", workloadProxyListen,
		"--forward", gatewayForwardListen + "=" + gateway,
		"--forward", llmForwardListen + "=" + llmTarget}
	if a.cfg.AgentTracingAddress != "" {
		tracing, err := overlayAuthority(a.cfg.AgentTracingAddress, "AGENT_TRACING_ADDRESS")
		if err != nil {
			return nil, err
		}
		serve = append(serve, "--forward", tracingForwardListen+"="+tracing)
	}
	identityMount := []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}}
	readOnly := func(extra map[string]string) map[string]string {
		properties := map[string]string{readOnlyRootFilesystemKey: "true"}
		for key, value := range extra {
			properties[key] = value
		}
		return properties
	}
	return &overlayPlan{
		enroll: &runnerv1.ContainerSpec{
			Image:                a.cfg.WorkloadProxyImage,
			Name:                 ZitiEnrollContainerName,
			Entrypoint:           a.cfg.WorkloadProxyEntrypoint,
			Cmd:                  []string{"enroll", "--identity", workloadIdentityFile},
			Mounts:               identityMount,
			AdditionalProperties: readOnly(nil),
		},
		sidecar: &runnerv1.ContainerSpec{
			Image:                a.cfg.WorkloadProxyImage,
			Name:                 ZitiSidecarContainerName,
			Entrypoint:           a.cfg.WorkloadProxyEntrypoint,
			Cmd:                  serve,
			Mounts:               identityMount,
			AdditionalProperties: readOnly(map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways}),
		},
		// HTTP answers through both fixed forwards and through a CONNECT
		// tunnel, and the proxy's refusal of unrouted tripwires. Tracing is
		// optional for a turn, so it is not waited for.
		wait: &runnerv1.ContainerSpec{
			Image:      a.cfg.WorkloadProxyImage,
			Name:       zitiWaitContainerName,
			Entrypoint: a.cfg.WorkloadProxyEntrypoint,
			Cmd: []string{"wait", "--proxy", workloadProxyListen, "--http", gatewayForwardListen, "--http", llmForwardListen,
				"--connect", gateway, "--timeout", strconv.Itoa(zitiWaitTimeoutSeconds) + "s", "--interval", "250ms"},
			AdditionalProperties: readOnly(nil),
		},
		volumes: []*runnerv1.VolumeSpec{
			{Name: zitiIdentityVolumeName, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL},
			{Name: agynHomeVolumeName, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL},
		},
		mainMounts: []*runnerv1.VolumeMount{{Volume: agynHomeVolumeName, MountPath: agynHomeMountPath}},
		explicit:   true,
	}, nil
}

func overlayAuthority(address, name string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || host == "" || strings.ContainsAny(host, "/@?#% ") {
		return "", fmt.Errorf("%s %q must be an overlay host:port", name, address)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("%s %q has an invalid port", name, address)
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), nil
}

// workloadEndpoints are the platform addresses a workload container is given.
// In explicit-proxy mode they are the sidecar's loopback forwards; the overlay
// addresses remain the forwards' targets.
type workloadEndpoints struct {
	gateway    string
	gatewayURL string
	llmBaseURL string
	tracing    string
}

func (a *Assembler) workloadEndpoints() workloadEndpoints {
	if !a.explicitProxy() {
		return workloadEndpoints{
			gateway:    a.cfg.AgentGatewayAddress,
			gatewayURL: buildGatewayURL(a.cfg.AgentGatewayAddress),
			llmBaseURL: a.cfg.AgentLLMBaseURL,
			tracing:    a.cfg.AgentTracingAddress,
		}
	}
	endpoints := workloadEndpoints{
		gateway:    gatewayForwardListen,
		gatewayURL: "http://" + gatewayForwardListen,
		llmBaseURL: loopbackBaseURL(a.cfg.AgentLLMBaseURL, llmForwardListen),
	}
	if a.cfg.AgentTracingAddress != "" {
		endpoints.tracing = tracingForwardListen
	}
	return endpoints
}

// loopbackBaseURL keeps the configured scheme and path and swaps only the
// authority, so "http://llm-proxy.agyn/v1" becomes "http://127.0.0.1:18081/v1".
// Config validation guarantees an http URL in explicit-proxy mode.
func loopbackBaseURL(raw, listen string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	parsed.Host = listen
	return parsed.String()
}

// applyExplicitProxyEnv is layered last onto a main or MCP container. The
// proxy variables and the mode are platform-owned; NO_PROXY keeps any user
// entries after loopback; JAVA_TOOL_OPTIONS is composed; HOME is a default.
func applyExplicitProxyEnv(envs []*runnerv1.EnvVar, home, owner string) []*runnerv1.EnvVar {
	for _, name := range workloadProxyEnvNames {
		for _, env := range envs {
			if env.GetName() == name && env.GetValue() != workloadProxyURL {
				log.Printf("assembler: warn: replacing %s for %s in explicit-proxy mode", name, owner)
			}
		}
		envs = appendPlatformEnvVar(envs, &runnerv1.EnvVar{Name: name, Value: workloadProxyURL})
	}
	envs = composeNoProxy(envs)
	envs = appendPlatformEnvVar(envs, &runnerv1.EnvVar{Name: "NODE_USE_ENV_PROXY", Value: "1"})
	envs = appendPlatformEnvVar(envs, &runnerv1.EnvVar{Name: AgynNetworkModeEnvVar, Value: config.WorkloadNetworkModeExplicitProxy})
	envs = appendComposedEnvVar(envs, "JAVA_TOOL_OPTIONS", workloadJavaProxyOptions)
	return appendDefaultEnvVar(envs, "HOME", home)
}

// composeNoProxy writes NO_PROXY and no_proxy with the same comma-separated
// value: loopback first, then every user entry from either spelling, once.
func composeNoProxy(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	entries := strings.Split(workloadNoProxyBase, ",")
	seen := map[string]struct{}{}
	for _, entry := range entries {
		seen[strings.ToLower(entry)] = struct{}{}
	}
	for _, name := range []string{"NO_PROXY", "no_proxy"} {
		for _, env := range envs {
			if env.GetName() != name {
				continue
			}
			for _, entry := range strings.Split(env.GetValue(), ",") {
				entry = strings.TrimSpace(entry)
				if entry == "" {
					continue
				}
				if _, ok := seen[strings.ToLower(entry)]; ok {
					continue
				}
				seen[strings.ToLower(entry)] = struct{}{}
				entries = append(entries, entry)
			}
		}
	}
	value := strings.Join(entries, ",")
	envs = appendPlatformEnvVar(envs, &runnerv1.EnvVar{Name: "NO_PROXY", Value: value})
	return appendPlatformEnvVar(envs, &runnerv1.EnvVar{Name: "no_proxy", Value: value})
}
