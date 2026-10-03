package assembler

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
)

var (
	fixedAgentID    = uuid.MustParse("aaaaaaaa-0000-4000-8000-000000000001")
	fixedInstanceID = uuid.MustParse("bbbbbbbb-0000-4000-8000-000000000002")
	fixedMcpID      = uuid.MustParse("cccccccc-0000-4000-8000-000000000003")
)

func overlayTestConfig(mode string) *config.Config {
	return &config.Config{
		AgentGatewayAddress:     "gateway.agyn:443",
		AgentTracingAddress:     "tracing.agyn:443",
		AgentLLMBaseURL:         "http://llm-proxy.agyn/v1",
		ZitiEnabled:             true,
		ZitiSidecarImage:        "ziti-image",
		WorkloadDNSUpstream:     "10.43.0.10",
		WorkloadNetworkMode:     mode,
		WorkloadProxyImage:      "ghcr.io/spk-ai/k8s-runner@sha256:0000000000000000000000000000000000000000000000000000000000000000",
		WorkloadProxyEntrypoint: "/app/workload-proxy",
		AgyndCLIInitImage:       "agynd-cli-init:fixture",
		AgynCLIInitImage:        "agyn-cli-init:fixture",
	}
}

func userEnv(name, value string) *agentsv1.Env {
	return &agentsv1.Env{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: name, Source: &agentsv1.Env_Value{Value: value}}
}

// assembleOverlayAgent assembles an agent with one MCP server and user envs
// that try to steer the network: a proxy override, a NO_PROXY entry, JVM
// options and a gateway override.
func assembleOverlayAgent(t *testing.T, cfg *config.Config, agentEnvs ...*agentsv1.Env) *AssembleResult {
	t.Helper()
	return assembleOverlayAgentWithVolumes(t, cfg, nil, agentEnvs...)
}

// assembleOverlayAgentWithVolumes is assembleOverlayAgent for an environment
// that declares volumes of its own.
func assembleOverlayAgentWithVolumes(t *testing.T, cfg *config.Config, environmentVolumes []*agentsv1.Volume, agentEnvs ...*agentsv1.Env) *AssembleResult {
	t.Helper()
	agent := &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: fixedAgentID.String()}, OrganizationId: "org-1", Image: "agent-image"}
	agents := &testutil.FakeAgentsClient{
		GetAgentFunc: func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{Meta: &agentsv1.EntityMeta{Id: fixedMcpID.String()}, Name: "memory", Image: "mcp-image", Command: "npx -y server"}}}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			switch {
			case req.GetAgentId() != "":
				return &agentsv1.ListEnvsResponse{Envs: agentEnvs}, nil
			case req.GetMcpId() != "":
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{userEnv("HTTPS_PROXY", "http://mcp-own-proxy:3128"), userEnv("no_proxy", "internal.example")}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumesFunc: func(_ context.Context, req *agentsv1.ListVolumesRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			if req.GetEnvironmentId() != "" {
				return &agentsv1.ListVolumesResponse{Volumes: environmentVolumes}, nil
			}
			return &agentsv1.ListVolumesResponse{}, nil
		},
	}
	withRuntimeEnvironment(agent, agents, cfg)
	assembler := withCatalog(NewWithRunnersAndEgressCA(agents, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, cfg, []byte("test-ca")), "org-1")
	result, err := assembler.Assemble(context.Background(), fixedAgentID, fixedInstanceID, fixedInstanceID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	return result
}

func allContainers(request *runnerv1.StartWorkloadRequest) []*runnerv1.ContainerSpec {
	return append(append([]*runnerv1.ContainerSpec{request.GetMain()}, request.GetSidecars()...), request.GetInitContainers()...)
}

func containerNames(containers []*runnerv1.ContainerSpec) []string {
	names := make([]string, 0, len(containers))
	for _, container := range containers {
		names = append(names, container.GetName())
	}
	return names
}

func TestExplicitProxyAgentNeedsNoPrivilegeOrResolverWrites(t *testing.T) {
	request := assembleOverlayAgent(t, overlayTestConfig(config.WorkloadNetworkModeExplicitProxy)).Request
	if request.GetDnsConfig() != nil {
		t.Fatal("explicit-proxy mode must keep the Pod's cluster DNS")
	}
	for _, container := range allContainers(request) {
		if len(container.GetRequiredCapabilities()) != 0 {
			t.Fatalf("%s requests capabilities %v", container.GetName(), container.GetRequiredCapabilities())
		}
		text := container.GetEntrypoint() + " " + strings.Join(container.GetCmd(), " ")
		for _, env := range container.GetEnv() {
			text += " " + env.GetName() + "=" + env.GetValue()
		}
		for _, forbidden := range []string{"resolv.conf", "/etc/hosts", "iptables", "NET_ADMIN", "tproxy"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s mentions %s", container.GetName(), forbidden)
			}
		}
	}
	want := []string{ZitiEnrollContainerName, ZitiSidecarContainerName, agyndCLIInitName, agynCLIInitName, agentRuntimeInit, zitiWaitContainerName}
	if got := containerNames(request.GetInitContainers()); !slices.Equal(got, want) {
		t.Fatalf("init order %v, want %v", got, want)
	}
}

func TestExplicitProxyOverlayContainers(t *testing.T) {
	cfg := overlayTestConfig(config.WorkloadNetworkModeExplicitProxy)
	request := assembleOverlayAgent(t, cfg).Request
	enroll := testutil.FindInitContainer(request.InitContainers, ZitiEnrollContainerName)
	sidecar := testutil.FindInitContainer(request.InitContainers, ZitiSidecarContainerName)
	wait := testutil.FindInitContainer(request.InitContainers, zitiWaitContainerName)
	for _, container := range []*runnerv1.ContainerSpec{enroll, sidecar, wait} {
		if container.GetImage() != cfg.WorkloadProxyImage || container.GetEntrypoint() != "/app/workload-proxy" {
			t.Fatalf("%s runs %s %s", container.GetName(), container.GetImage(), container.GetEntrypoint())
		}
		if container.GetAdditionalProperties()[readOnlyRootFilesystemKey] != "true" {
			t.Fatalf("%s root filesystem is writable", container.GetName())
		}
		// Controller and enrollment traffic must never loop into the proxy,
		// and none of them needs the egress CA.
		if len(container.GetEnv()) != 0 || len(container.GetInlineFileMounts()) != 0 {
			t.Fatalf("%s carries env %v or inline files", container.GetName(), container.GetEnv())
		}
	}
	if !slices.Equal(enroll.GetCmd(), []string{"enroll", "--identity", "/netfoundry/agent.json"}) {
		t.Fatalf("enroll command %v", enroll.GetCmd())
	}
	wantServe := []string{"serve", "--identity", "/netfoundry/agent.json", "--listen", "127.0.0.1:18080",
		"--forward", "127.0.0.1:18443=gateway.agyn:443", "--forward", "127.0.0.1:18081=llm-proxy.agyn:80", "--forward", "127.0.0.1:18444=tracing.agyn:443"}
	if !slices.Equal(sidecar.GetCmd(), wantServe) {
		t.Fatalf("serve command %v", sidecar.GetCmd())
	}
	if sidecar.GetAdditionalProperties()[zitiRestartPolicyKey] != zitiRestartPolicyAlways {
		t.Fatal("the overlay sidecar must stay up as a restartable init container")
	}
	wantWait := []string{"wait", "--proxy", "127.0.0.1:18080", "--http", "127.0.0.1:18443", "--http", "127.0.0.1:18081",
		"--connect", "gateway.agyn:443", "--timeout", "180s", "--interval", "250ms"}
	if !slices.Equal(wait.GetCmd(), wantWait) {
		t.Fatalf("wait command %v", wait.GetCmd())
	}
	// Only the containers that need the identity can read it.
	for _, container := range allContainers(request) {
		mounted := findVolumeMount(container, zitiIdentityVolumeName) != nil
		if mounted != (container == enroll || container == sidecar) {
			t.Fatalf("%s identity mount = %v", container.GetName(), mounted)
		}
	}
	if findVolumeSpec(request.Volumes, zitiIdentityVolumeName) == nil || findVolumeSpec(request.Volumes, agynHomeVolumeName) == nil {
		t.Fatal("overlay volumes missing")
	}
	if findVolumeMount(request.Main, agynHomeVolumeName) == nil || findVolumeMount(request.Sidecars[0], agynHomeVolumeName) != nil {
		t.Fatal("the private home volume belongs to the main container only")
	}
}

func TestExplicitProxyWorkloadEnv(t *testing.T) {
	request := assembleOverlayAgent(t, overlayTestConfig(config.WorkloadNetworkModeExplicitProxy),
		userEnv("HTTP_PROXY", "http://attacker.example:3128"),
		userEnv("NO_PROXY", "foo.com, 127.0.0.1"),
		userEnv("JAVA_TOOL_OPTIONS", "-Xmx512m"),
		userEnv("GATEWAY_ADDRESS", "evil:443"),
	).Request
	main := envMap(request.Main.Env)
	for name, want := range map[string]string{
		"GATEWAY_ADDRESS":             "127.0.0.1:18443",
		"AGYN_GATEWAY_URL":            "http://127.0.0.1:18443",
		"LLM_BASE_URL":                "http://127.0.0.1:18081/v1",
		"TRACING_ADDRESS":             "127.0.0.1:18444",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:18444",
		"HTTP_PROXY":                  "http://127.0.0.1:18080",
		"http_proxy":                  "http://127.0.0.1:18080",
		"HTTPS_PROXY":                 "http://127.0.0.1:18080",
		"https_proxy":                 "http://127.0.0.1:18080",
		"NO_PROXY":                    "127.0.0.1,localhost,::1,foo.com",
		"no_proxy":                    "127.0.0.1,localhost,::1,foo.com",
		"NODE_USE_ENV_PROXY":          "1",
		"AGYN_NETWORK_MODE":           "explicit-proxy",
		"JAVA_TOOL_OPTIONS":           "-Xmx512m " + workloadJavaProxyOptions,
		"HOME":                        "/home/agyn",
		"SSL_CERT_FILE":               egressCACertPath,
	} {
		assertEnv(t, main, name, want)
	}
	if strings.Contains(main["NO_PROXY"], ".agyn") {
		t.Fatal("overlay names must go through the proxy")
	}
	mcp := envMap(request.Sidecars[0].Env)
	for name, want := range map[string]string{
		"GATEWAY_ADDRESS":  "127.0.0.1:18443",
		"AGYN_GATEWAY_URL": "http://127.0.0.1:18443",
		"HTTPS_PROXY":      "http://127.0.0.1:18080",
		"NO_PROXY":         "127.0.0.1,localhost,::1,internal.example",
		"no_proxy":         "127.0.0.1,localhost,::1,internal.example",
		"HOME":             "/tmp",
	} {
		assertEnv(t, mcp, name, want)
	}
}

func TestExplicitProxyKeepsAUserHome(t *testing.T) {
	request := assembleOverlayAgent(t, overlayTestConfig(config.WorkloadNetworkModeExplicitProxy), userEnv("HOME", "/workspace/home")).Request
	assertEnv(t, envMap(request.Main.Env), "HOME", "/workspace/home")
}

// An environment that mounts its own volume at the default home keeps it as
// the home. A second mount at the same path is refused by Kubernetes ("must
// be unique"), and spelled with a trailing slash it passes validation and the
// platform's emptyDir silently shadows the environment's volume.
func TestExplicitProxyLeavesAnEnvironmentHomeVolumeInPlace(t *testing.T) {
	for _, mountPath := range []string{"/home/agyn", "/home/agyn/"} {
		t.Run(mountPath, func(t *testing.T) {
			home := &agentsv1.Volume{Meta: &agentsv1.EntityMeta{Id: "ffffffff-0000-4000-8000-000000000006"}, Name: "home", MountPath: mountPath, Persistent: true, Size: "1Gi"}
			request := assembleOverlayAgentWithVolumes(t, overlayTestConfig(config.WorkloadNetworkModeExplicitProxy), []*agentsv1.Volume{home}).Request
			assertSingleHomeMount(t, request, "vol-ffffffff")
			assertEnv(t, envMap(request.Main.Env), "HOME", "/home/agyn")

			fixture := newSandboxFixture()
			*fixture.cfg = *overlayTestConfig(config.WorkloadNetworkModeExplicitProxy)
			fixture.cfg.SandboxWorkspaceSizeGB = testSandboxSizeGB
			listVolumes := fixture.agents.ListVolumesFunc
			fixture.agents.ListVolumesFunc = func(ctx context.Context, req *agentsv1.ListVolumesRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
				resp, err := listVolumes(ctx, req, opts...)
				if err == nil && req.GetEnvironmentId() != "" {
					resp.Volumes = append(resp.Volumes, home)
				}
				return resp, err
			}
			assertSingleHomeMount(t, fixture.assemble(t).Request, "vol-ffffffff")
		})
	}
}

func assertSingleHomeMount(t *testing.T, request *runnerv1.StartWorkloadRequest, volume string) {
	t.Helper()
	var at []string
	for _, mount := range request.GetMain().GetMounts() {
		if strings.TrimRight(mount.GetMountPath(), "/") == agynHomeMountPath {
			at = append(at, mount.GetVolume())
		}
	}
	if !slices.Equal(at, []string{volume}) {
		t.Fatalf("main mounts %v at %s, want only the environment's %s", at, agynHomeMountPath, volume)
	}
	if findVolumeSpec(request.GetVolumes(), agynHomeVolumeName) != nil {
		t.Fatal("an unused platform home volume was still declared")
	}
}

func TestExplicitProxySandbox(t *testing.T) {
	fixture := newSandboxFixture()
	*fixture.cfg = *overlayTestConfig(config.WorkloadNetworkModeExplicitProxy)
	fixture.cfg.SandboxWorkspaceSizeGB = testSandboxSizeGB
	request := fixture.assemble(t).Request
	if request.GetDnsConfig() != nil {
		t.Fatal("sandbox DNS overridden")
	}
	for _, container := range allContainers(request) {
		if len(container.GetRequiredCapabilities()) != 0 {
			t.Fatalf("%s requests capabilities", container.GetName())
		}
	}
	want := []string{ZitiEnrollContainerName, ZitiSidecarContainerName, agyndCLIInitName, agynCLIInitName, agentRuntimeInit, zitiWaitContainerName}
	if got := containerNames(request.GetInitContainers()); !slices.Equal(got, want) {
		t.Fatalf("init order %v, want %v", got, want)
	}
	main := envMap(request.Main.Env)
	assertEnv(t, main, "GATEWAY_ADDRESS", "127.0.0.1:18443")
	assertEnv(t, main, "LLM_BASE_URL", "http://127.0.0.1:18081/v1")
	assertEnv(t, main, "HTTPS_PROXY", "http://127.0.0.1:18080")
	assertEnv(t, main, "HOME", "/home/agyn")
	if findVolumeMount(request.Main, agynHomeVolumeName) == nil {
		t.Fatal("sandbox main has no writable home")
	}
}

// Without the mode the upstream tunneler is unchanged, NET_ADMIN included.
func TestTProxyModeIsUnchanged(t *testing.T) {
	request := assembleOverlayAgent(t, overlayTestConfig(config.WorkloadNetworkModeTProxy)).Request
	sidecar := testutil.FindInitContainer(request.InitContainers, ZitiSidecarContainerName)
	if !slices.Equal(sidecar.GetRequiredCapabilities(), []string{"NET_ADMIN"}) || request.GetDnsConfig() == nil {
		t.Fatal("legacy tproxy assembly changed")
	}
	main := envMap(request.Main.Env)
	assertEnv(t, main, "GATEWAY_ADDRESS", "gateway.agyn:443")
	assertEnv(t, main, "LLM_BASE_URL", "http://llm-proxy.agyn/v1")
	if _, ok := main["HTTPS_PROXY"]; ok {
		t.Fatal("tproxy mode must not set proxy variables")
	}
	if findVolumeSpec(request.Volumes, agynHomeVolumeName) != nil {
		t.Fatal("tproxy mode gained a home volume")
	}
}

func TestComposeNoProxyMergesBothSpellings(t *testing.T) {
	envs := composeNoProxy([]*runnerv1.EnvVar{{Name: "no_proxy", Value: "b.example,LOCALHOST"}, {Name: "NO_PROXY", Value: " a.example ,b.example"}})
	values := envMap(envs)
	if values["NO_PROXY"] != values["no_proxy"] || values["NO_PROXY"] != "127.0.0.1,localhost,::1,a.example,b.example" {
		t.Fatalf("composed NO_PROXY %q / %q", values["NO_PROXY"], values["no_proxy"])
	}
}

// Pod Security fixtures: the assembled requests rendered the way k8s-runner
// renders them, written to testdata and evaluated by hack/podcheck with the
// pinned Kubernetes v1.35 "restricted" evaluator. Regenerate with
// UPDATE_POD_SECURITY_FIXTURES=1 after an intended assembly change.
//
// The rendering mirrors k8s-runner's restricted profile (pod_security.go) for
// explicit-proxy requests and its "none" profile for the legacy tproxy one;
// the runner's own fixtures prove its real Pod builder.
//
// @see k8s-runner::internal/server/pod_security
func TestAssembledPodsAgainstRestrictedFixtures(t *testing.T) {
	explicit := assembleOverlayAgent(t, overlayTestConfig(config.WorkloadNetworkModeExplicitProxy)).Request
	checkAssemblerPodFixture(t, "allowed-explicit-proxy-agent", renderRunnerPod(explicit, true))

	fixture := newSandboxFixture()
	fixture.sandbox.Meta.Id = "dddddddd-0000-4000-8000-000000000004"
	fixture.workspaceVolumeID = "eeeeeeee-0000-4000-8000-000000000005"
	*fixture.cfg = *overlayTestConfig(config.WorkloadNetworkModeExplicitProxy)
	fixture.cfg.SandboxWorkspaceSizeGB = testSandboxSizeGB
	checkAssemblerPodFixture(t, "allowed-explicit-proxy-sandbox", renderRunnerPod(fixture.assemble(t).Request, true))

	legacy := assembleOverlayAgent(t, overlayTestConfig(config.WorkloadNetworkModeTProxy)).Request
	checkAssemblerPodFixture(t, "denied-tproxy-agent", renderRunnerPod(legacy, false))
}

func renderRunnerPod(request *runnerv1.StartWorkloadRequest, restricted bool) *corev1.Pod {
	pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "fixture", Namespace: "agyn-workloads"}}
	for _, volume := range request.GetVolumes() {
		source := corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
		if volume.GetKind() == runnerv1.VolumeKind_VOLUME_KIND_NAMED {
			source = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "fixture-claim"}}
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: volume.GetName(), VolumeSource: source})
	}
	if len(request.GetInlineFiles()) > 0 {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "inline-files", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "fixture-inline-files"}}})
	}
	render := func(spec *runnerv1.ContainerSpec) corev1.Container {
		container := corev1.Container{Name: spec.GetName(), Image: spec.GetImage()}
		for _, mount := range spec.GetMounts() {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: mount.GetVolume(), MountPath: mount.GetMountPath()})
		}
		for _, mount := range spec.GetInlineFileMounts() {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "inline-files", MountPath: mount.GetPath(), SubPath: "inline-file-0", ReadOnly: true})
		}
		if spec.GetAdditionalProperties()[zitiRestartPolicyKey] == zitiRestartPolicyAlways {
			always := corev1.ContainerRestartPolicyAlways
			container.RestartPolicy = &always
		}
		var add []corev1.Capability
		for _, capability := range spec.GetRequiredCapabilities() {
			add = append(add, corev1.Capability(capability))
		}
		if restricted {
			no, yes := false, true
			container.SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: &no, RunAsNonRoot: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: add}}
			if spec.GetAdditionalProperties()[readOnlyRootFilesystemKey] == "true" {
				container.SecurityContext.ReadOnlyRootFilesystem = &yes
			}
		} else if len(add) > 0 {
			container.SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: add}}
		}
		return container
	}
	pod.Spec.Containers = append(pod.Spec.Containers, render(request.GetMain()))
	for _, sidecar := range request.GetSidecars() {
		pod.Spec.Containers = append(pod.Spec.Containers, render(sidecar))
	}
	for _, init := range request.GetInitContainers() {
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, render(init))
	}
	if dns := request.GetDnsConfig(); dns != nil && len(dns.GetNameservers()) > 0 {
		pod.Spec.DNSPolicy = corev1.DNSNone
		pod.Spec.DNSConfig = &corev1.PodDNSConfig{Nameservers: dns.GetNameservers(), Searches: dns.GetSearches()}
	}
	if restricted {
		yes, id := true, int64(10001)
		onRootMismatch := corev1.FSGroupChangeOnRootMismatch
		pod.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &id, RunAsGroup: &id, FSGroup: &id,
			FSGroupChangePolicy: &onRootMismatch, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	}
	return pod
}

func checkAssemblerPodFixture(t *testing.T, name string, pod *corev1.Pod) {
	t.Helper()
	pod.Name = name
	encoded, err := json.MarshalIndent(pod, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join("testdata", "pod-security", name+".json")
	if os.Getenv("UPDATE_POD_SECURITY_FIXTURES") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing fixture %s (UPDATE_POD_SECURITY_FIXTURES=1 regenerates it): %v", path, err)
	}
	if !bytes.Equal(committed, encoded) {
		t.Fatalf("fixture %s is stale; review the assembly change and regenerate with UPDATE_POD_SECURITY_FIXTURES=1", path)
	}
}
