package assembler

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
)

func TestAssemblerMainContainer(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Name:           "assistant",
		Role:           "ops",
		Model:          "gpt-test",
		Image:          "agent-image",
		InitImage:      "agent-init-image",
		Description:    "test agent",
		Configuration:  "{\"mode\":\"test\"}",
		Capabilities:   []string{"privileged", "dind"},
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "CUSTOM_ENV", Source: &agentsv1.Env_Value{Value: "custom"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "AGENT_NAME", Source: &agentsv1.Env_Value{Value: "override"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "WORKSPACE_DIR", Source: &agentsv1.Env_Value{Value: "/override"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "HOME", Source: &agentsv1.Env_Value{Value: "/override-home"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress:       "gateway:50051",
		AgentTracingAddress:       "tracing:50051",
		AgentLLMBaseURL:           "http://llm:8080/v1",
		AgyndAgentsDirectAddress:  "10.42.0.10:50051",
		AgyndRunnersDirectAddress: "10.42.0.11:50051",
	}

	withRuntimeEnvironment(agent, agentsClient, &cfg)
	assembler := withCatalog(NewWithRunners(agentsClient, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, &cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.OrganizationID != agent.GetOrganizationId() {
		t.Fatalf("expected organization id %q, got %q", agent.GetOrganizationId(), result.OrganizationID)
	}
	if len(result.RunnerLabels) != 0 {
		t.Fatalf("expected no runner labels, got %v", result.RunnerLabels)
	}
	request := result.Request
	if request.DnsConfig != nil {
		t.Fatal("Ziti-disabled agent DNS behavior changed")
	}
	if request.Main == nil {
		t.Fatal("expected main container")
	}
	if request.Main.Image != agent.GetImage() {
		t.Fatalf("expected agent image %q, got %q", agent.GetImage(), request.Main.Image)
	}
	expectedName := "agent-" + agentID.String()[:8] + "-" + threadID.String()[:8]
	if request.Main.Name != expectedName {
		t.Fatalf("expected main name %q, got %q", expectedName, request.Main.Name)
	}
	expectedCmd := []string{agynBinBinaryPath}
	if !equalStringSlice(request.Main.Cmd, expectedCmd) {
		t.Fatalf("unexpected main cmd: %+v", request.Main.Cmd)
	}
	if len(request.Main.Mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(request.Main.Mounts))
	}
	agynBinMount := findVolumeMount(request.Main, agynBinVolumeName)
	if agynBinMount == nil {
		t.Fatalf("expected agyn-bin mount")
	}
	if agynBinMount.MountPath != agynBinMountPath {
		t.Fatalf("expected agyn-bin mount path %q, got %q", agynBinMountPath, agynBinMount.MountPath)
	}
	if len(request.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(request.Volumes))
	}
	agynBinVolume := findVolumeSpec(request.Volumes, agynBinVolumeName)
	if agynBinVolume == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if agynBinVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", agynBinVolume.Kind)
	}
	if request.ImagePullCredentials != nil {
		t.Fatalf("expected no image pull credentials, got %+v", request.ImagePullCredentials)
	}
	if len(request.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(request.InitContainers))
	}
	initContainer := testutil.FindInitContainer(request.InitContainers, agentRuntimeInit)
	if initContainer == nil {
		t.Fatalf("expected the %s init container", agentRuntimeInit)
	}
	if initContainer.Image != testResolvedRuntimeRef {
		t.Fatalf("expected init container image %q, got %q", testResolvedRuntimeRef, initContainer.Image)
	}
	if len(initContainer.Mounts) != 1 {
		t.Fatalf("expected 1 init container mount, got %d", len(initContainer.Mounts))
	}
	if initContainer.Mounts[0].Volume != agynBinVolumeName {
		t.Fatalf("expected init container agyn-bin volume, got %q", initContainer.Mounts[0].Volume)
	}
	if initContainer.Mounts[0].MountPath != agynBinMountPath {
		t.Fatalf("expected init container agyn-bin mount path %q, got %q", agynBinMountPath, initContainer.Mounts[0].MountPath)
	}
	labels := request.AdditionalProperties
	if len(labels) == 0 {
		t.Fatal("expected labels in request additional properties")
	}
	expectedLabels := map[string]string{
		LabelKeyPrefix + LabelManagedBy:  ManagedByValue,
		LabelKeyPrefix + LabelAgentID:    agentID.String(),
		LabelKeyPrefix + LabelInstanceID: threadID.String(),
		LabelKeyPrefix + LabelThreadID:   threadID.String(),
	}
	if !equalStringMap(labels, expectedLabels) {
		t.Fatalf("expected labels %+v, got %+v", expectedLabels, labels)
	}
	if !equalStringSlice(request.Capabilities, agent.GetCapabilities()) {
		t.Fatalf("expected capabilities %+v, got %+v", agent.GetCapabilities(), request.Capabilities)
	}
	envs := envMap(request.Main.Env)
	assertEnv(t, envs, "AGENT_ID", agentID.String())
	assertEnv(t, envs, "AGENT_NAME", agent.GetName())
	assertEnv(t, envs, "AGENT_ROLE", agent.GetRole())
	assertEnv(t, envs, "AGENT_MODEL", agent.GetModel())
	assertEnv(t, envs, "AGENT_CONFIG", agent.GetConfiguration())
	assertEnv(t, envs, "AGYN_ORGANIZATION_ID", agent.GetOrganizationId())
	assertEnv(t, envs, "AGENT_INSTANCE_ID", threadID.String())
	assertEnv(t, envs, "AGYN_IDENTITY_ID", threadID.String())
	// An instance serves every thread in its inbox, so naming one at startup
	// would scope it to whichever happened to be first unacked.
	if _, ok := envs["THREAD_ID"]; ok {
		t.Fatalf("expected no THREAD_ID, got %q", envs["THREAD_ID"])
	}
	assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
	assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://"+cfg.AgentGatewayAddress)
	assertEnv(t, envs, "LLM_BASE_URL", cfg.AgentLLMBaseURL)
	assertEnv(t, envs, "AGYND_AGENTS_DIRECT_ADDRESS", cfg.AgyndAgentsDirectAddress)
	assertEnv(t, envs, "AGYND_RUNNERS_DIRECT_ADDRESS", cfg.AgyndRunnersDirectAddress)
	assertEnv(t, envs, "TRACING_ADDRESS", cfg.AgentTracingAddress)
	assertEnv(t, envs, "OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+cfg.AgentTracingAddress)
	assertEnv(t, envs, "WORKSPACE_DIR", "/override")
	assertEnv(t, envs, "HOME", "/override-home")
	assertEnv(t, envs, "CUSTOM_ENV", "custom")
	if _, ok := envs["INIT_SCRIPT"]; ok {
		t.Fatal("expected INIT_SCRIPT to be absent")
	}
	if _, ok := envs["AGENT_SKILLS"]; ok {
		t.Fatal("expected AGENT_SKILLS to be absent")
	}
}

func TestAssemblerAddsZitiSidecar(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Image:          "agent-image",
		InitImage:      "agent-init-image",
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress:                 "gateway.agyn:443",
		AgentTracingAddress:                 "tracing.agyn:443",
		AgentLLMBaseURL:                     "http://llm-proxy.agyn/v1",
		ZitiEnabled:                         true,
		ZitiSidecarImage:                    "ziti-image",
		WorkloadDNSUpstream:                 "10.43.0.10",
		ZitiEnrollmentDNSUpstream:           "10.43.0.20",
		ZitiEnrollmentControllerResolveHost: "ziti-controller-client.ziti.svc.cluster.local",
		ZitiEnrollmentControllerPort:        "2496",
		ZitiRuntimeControllerResolveHost:    "istio-ingressgateway.istio-gateway.svc.cluster.local",
		ZitiRuntimeControllerPort:           "443",
	}

	withRuntimeEnvironment(agent, agentsClient, &cfg)
	assembler := withCatalog(NewWithRunners(agentsClient, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, &cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.OrganizationID != agent.GetOrganizationId() {
		t.Fatalf("expected organization id %q, got %q", agent.GetOrganizationId(), result.OrganizationID)
	}
	if len(result.RunnerLabels) != 0 {
		t.Fatalf("expected no runner labels, got %v", result.RunnerLabels)
	}
	request := result.Request
	if request.DnsConfig == nil {
		t.Fatal("expected dns config")
	}
	expectedNameservers := []string{zitiDNSNameserver}
	if !equalStringSlice(request.DnsConfig.Nameservers, expectedNameservers) {
		t.Fatalf("expected dns nameservers %+v, got %+v", expectedNameservers, request.DnsConfig.Nameservers)
	}
	expectedSearches := []string{zitiDNSSearchService, zitiDNSSearchCluster}
	if !equalStringSlice(request.DnsConfig.Searches, expectedSearches) {
		t.Fatalf("expected dns searches %+v, got %+v", expectedSearches, request.DnsConfig.Searches)
	}
	if len(request.InitContainers) != 4 {
		t.Fatalf("expected 4 init containers, got %d", len(request.InitContainers))
	}
	// The binaries land while the overlay comes up, so the wait is last and
	// usually has nothing left to wait for by the time it runs.
	for i, expected := range []string{ZitiEnrollContainerName, ZitiSidecarContainerName, agentRuntimeInit, zitiWaitContainerName} {
		if request.InitContainers[i].GetName() != expected {
			t.Fatalf("expected init container %d to be %s, got %s", i, expected, request.InitContainers[i].GetName())
		}
	}
	initContainer := testutil.FindInitContainer(request.InitContainers, agentRuntimeInit)
	if initContainer == nil {
		t.Fatalf("expected the %s container", agentRuntimeInit)
	}
	if len(request.Sidecars) != 0 {
		t.Fatalf("expected 0 sidecars, got %d", len(request.Sidecars))
	}
	if testutil.FindContainer(request.Sidecars, ZitiSidecarContainerName) != nil {
		t.Fatalf("expected %s to use runner restartable init contract, not sidecars", ZitiSidecarContainerName)
	}
	zitiEnroll := testutil.FindInitContainer(request.InitContainers, ZitiEnrollContainerName)
	if zitiEnroll == nil {
		t.Fatal("expected ziti-enroll init container")
	}
	if zitiEnroll.Image != cfg.ZitiSidecarImage {
		t.Fatalf("expected ziti enroll image %q, got %q", cfg.ZitiSidecarImage, zitiEnroll.Image)
	}
	if zitiEnroll.Entrypoint != zitiEnrollEntrypoint {
		t.Fatalf("expected ziti enroll entrypoint %q, got %q", zitiEnrollEntrypoint, zitiEnroll.Entrypoint)
	}
	expectedEnrollCmd := buildZitiEnrollCommand(cfg.ZitiEnrollmentDNSUpstream, cfg.ZitiEnrollmentControllerResolveHost, cfg.ZitiEnrollmentControllerPort, cfg.ZitiRuntimeControllerResolveHost, cfg.ZitiRuntimeControllerPort)
	if !equalStringSlice(zitiEnroll.Cmd, expectedEnrollCmd) {
		t.Fatalf("expected ziti enroll cmd %+v, got %+v", expectedEnrollCmd, zitiEnroll.Cmd)
	}
	if !strings.Contains(zitiEnroll.Cmd[1], "nameserver %s\\nsearch svc.cluster.local cluster.local\\noptions ndots:5\\n") {
		t.Fatalf("expected ziti enroll script to write workload DNS upstream resolver, got %q", zitiEnroll.Cmd[1])
	}
	if !strings.Contains(zitiEnroll.Cmd[1], `ziti edge enroll --jwt "${jwt_file}" --ca "${ziti_tls_ca_cert}" --out "${identity_file}"`) {
		t.Fatalf("expected ziti enroll script to use canonical ziti edge enrollment, got %q", zitiEnroll.Cmd[1])
	}
	if strings.Contains(zitiEnroll.Cmd[1], "curl --fail-with-body") || strings.Contains(zitiEnroll.Cmd[1], "/edge/client/v1/enroll?method=") {
		t.Fatalf("expected ziti enroll script not to hand-post CSR enrollment requests, got %q", zitiEnroll.Cmd[1])
	}
	if strings.Contains(zitiEnroll.Cmd[1], `id: {cert: $cert, key: $key, ca: $ca}`) || strings.Contains(zitiEnroll.Cmd[1], `--arg cert`) || strings.Contains(zitiEnroll.Cmd[1], `--arg key`) {
		t.Fatalf("expected ziti enroll script not to hand-construct identity cert/key/ca JSON, got %q", zitiEnroll.Cmd[1])
	}
	if strings.Contains(zitiEnroll.Cmd[1], "ziti.agyn.dev") || strings.Contains(zitiEnroll.Cmd[1], "istio-ingressgateway") || strings.Contains(zitiEnroll.Cmd[1], "ziti-controller-client") {
		t.Fatalf("expected ziti enroll script not to hard-code controller endpoints, got %q", zitiEnroll.Cmd[1])
	}
	if !strings.Contains(zitiEnroll.Cmd[1], "getent ahostsv4") {
		t.Fatalf("expected ziti enroll script to resolve controller addresses, got %q", zitiEnroll.Cmd[1])
	}
	if !strings.Contains(zitiEnroll.Cmd[1], `--ca "${ziti_tls_ca_cert}"`) {
		t.Fatalf("expected ziti enroll script to pass the controller CA bundle to canonical enrollment, got %q", zitiEnroll.Cmd[1])
	}
	if !strings.Contains(zitiEnroll.Cmd[1], ".iss") || !strings.Contains(zitiEnroll.Cmd[1], ".em") || !strings.Contains(zitiEnroll.Cmd[1], ".jti") || !strings.Contains(zitiEnroll.Cmd[1], ".sub") {
		t.Fatalf("expected ziti enroll script to derive enrollment request from JWT claims, got %q", zitiEnroll.Cmd[1])
	}
	if zitiEnroll.Cmd[3] != cfg.ZitiEnrollmentDNSUpstream {
		t.Fatalf("expected ziti enroll upstream arg %q, got %q", cfg.ZitiEnrollmentDNSUpstream, zitiEnroll.Cmd[3])
	}
	if zitiEnroll.Cmd[4] != zitiDNSNameserver {
		t.Fatalf("expected ziti enroll workload DNS nameserver arg %q, got %q", zitiDNSNameserver, zitiEnroll.Cmd[4])
	}
	if zitiEnroll.Cmd[5] != cfg.ZitiEnrollmentControllerResolveHost {
		t.Fatalf("expected ziti enroll enrollment controller resolve host arg %q, got %q", cfg.ZitiEnrollmentControllerResolveHost, zitiEnroll.Cmd[5])
	}
	if zitiEnroll.Cmd[6] != cfg.ZitiEnrollmentControllerPort {
		t.Fatalf("expected ziti enroll enrollment controller port arg %q, got %q", cfg.ZitiEnrollmentControllerPort, zitiEnroll.Cmd[6])
	}
	if zitiEnroll.Cmd[7] != cfg.ZitiRuntimeControllerResolveHost {
		t.Fatalf("expected ziti enroll runtime controller resolve host arg %q, got %q", cfg.ZitiRuntimeControllerResolveHost, zitiEnroll.Cmd[7])
	}
	if zitiEnroll.Cmd[8] != cfg.ZitiRuntimeControllerPort {
		t.Fatalf("expected ziti enroll runtime controller port arg %q, got %q", cfg.ZitiRuntimeControllerPort, zitiEnroll.Cmd[8])
	}
	zitiEnrollEnv := envMap(zitiEnroll.Env)
	assertEnv(t, zitiEnrollEnv, ZitiIdentityBasenameEnvVar, ZitiIdentityBasename)
	assertEnv(t, zitiEnrollEnv, ZitiIdentityDirEnvVar, zitiIdentityMountPath)
	assertEnv(t, zitiEnrollEnv, ZitiEnrollmentControllerResolveHostEnvVar, cfg.ZitiEnrollmentControllerResolveHost)
	assertEnv(t, zitiEnrollEnv, ZitiEnrollmentControllerPortEnvVar, cfg.ZitiEnrollmentControllerPort)
	assertSameZitiIdentityMount(t, zitiEnroll)
	zitiSidecar := testutil.FindInitContainer(request.InitContainers, ZitiSidecarContainerName)
	if zitiSidecar == nil {
		t.Fatal("expected ziti-sidecar init container")
	}
	if zitiSidecar.Image != cfg.ZitiSidecarImage {
		t.Fatalf("expected ziti sidecar image %q, got %q", cfg.ZitiSidecarImage, zitiSidecar.Image)
	}
	if zitiSidecar.Entrypoint != zitiSidecarEntrypoint {
		t.Fatalf("expected ziti sidecar entrypoint %q, got %q", zitiSidecarEntrypoint, zitiSidecar.Entrypoint)
	}
	expectedCmd := buildZitiSidecarCommand(cfg.WorkloadDNSUpstream)
	if !equalStringSlice(zitiSidecar.Cmd, expectedCmd) {
		t.Fatalf("expected ziti sidecar cmd %+v, got %+v", expectedCmd, zitiSidecar.Cmd)
	}
	if zitiSidecar.Cmd[3] != cfg.WorkloadDNSUpstream {
		t.Fatalf("expected ziti sidecar runtime DNS upstream arg %q, got %q", cfg.WorkloadDNSUpstream, zitiSidecar.Cmd[3])
	}
	if len(zitiSidecar.Cmd) != 4 {
		t.Fatalf("expected ziti sidecar command to receive only runtime DNS upstream after identity enrollment, got %+v", zitiSidecar.Cmd)
	}
	if zitiSidecar.Cmd[3] == cfg.ZitiEnrollmentDNSUpstream {
		t.Fatalf("expected ziti sidecar not to use enrollment DNS upstream %q", cfg.ZitiEnrollmentDNSUpstream)
	}
	if !equalStringSlice(zitiSidecar.RequiredCapabilities, []string{zitiRequiredCapabilityNetAdmin}) {
		t.Fatalf("expected ziti sidecar capabilities %+v, got %+v", []string{zitiRequiredCapabilityNetAdmin}, zitiSidecar.RequiredCapabilities)
	}
	zitiEnv := envMap(zitiSidecar.Env)
	assertEnv(t, zitiEnv, ZitiIdentityBasenameEnvVar, ZitiIdentityBasename)
	assertEnv(t, zitiEnv, ZitiIdentityDirEnvVar, zitiIdentityMountPath)
	assertEnv(t, zitiEnv, "WORKLOAD_DNS_UPSTREAM", cfg.WorkloadDNSUpstream)
	if _, ok := zitiEnv["ZITI_DNS_UPSTREAM"]; ok {
		t.Fatalf("expected ziti sidecar not to receive enrollment DNS upstream")
	}
	if _, ok := zitiEnv["ZITI_CTRL_ADVERTISED_ADDRESS"]; ok {
		t.Fatalf("expected ziti sidecar not to pin runtime controller through host aliases")
	}
	assertEnv(t, zitiEnv, "ZITI_SIDECAR_SERVICE_POLL_RATE", zitiSidecarServicePollRate)
	if _, ok := zitiEnv[ZitiEnrollmentTokenEnvVar]; ok {
		t.Fatalf("expected ziti sidecar not to receive %s", ZitiEnrollmentTokenEnvVar)
	}
	expectedProperties := map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways}
	if !equalStringMap(zitiSidecar.AdditionalProperties, expectedProperties) {
		t.Fatalf("expected ziti sidecar properties %+v, got %+v", expectedProperties, zitiSidecar.AdditionalProperties)
	}
	if request.InitContainers[2].GetName() != agentRuntimeInit || request.InitContainers[3].GetName() != zitiWaitContainerName {
		t.Fatalf("expected the restartable ziti sidecar to be followed by %s then %s, got %s then %s", agentRuntimeInit, zitiWaitContainerName, request.InitContainers[2].GetName(), request.InitContainers[3].GetName())
	}
	assertSameZitiIdentityMount(t, zitiSidecar)
	zitiWait := testutil.FindInitContainer(request.InitContainers, zitiWaitContainerName)
	if zitiWait == nil {
		t.Fatal("expected ziti-wait init container")
	}
	if zitiWait.Image != cfg.ZitiSidecarImage {
		t.Fatalf("expected ziti wait image %q, got %q", cfg.ZitiSidecarImage, zitiWait.Image)
	}
	if zitiWait.Entrypoint != zitiSidecarEntrypoint {
		t.Fatalf("expected ziti wait entrypoint %q, got %q", zitiSidecarEntrypoint, zitiWait.Entrypoint)
	}
	llmTarget, err := zitiServiceWaitTarget(cfg.AgentLLMBaseURL)
	if err != nil {
		t.Fatalf("llm proxy wait target: %v", err)
	}
	expectedWaitCmd := buildZitiWaitCommand(cfg.AgentGatewayAddress, llmTarget)
	if !equalStringSlice(zitiWait.Cmd, expectedWaitCmd) {
		t.Fatalf("expected ziti wait cmd %+v, got %+v", expectedWaitCmd, zitiWait.Cmd)
	}
	// One container waits for both, so both targets appear in the one script.
	if !strings.Contains(zitiWait.Cmd[1], "gateway.agyn:443") {
		t.Fatalf("expected ziti wait to name gateway.agyn:443, got %+v", zitiWait.Cmd)
	}
	if !strings.Contains(zitiWait.Cmd[1], llmTarget.host+":"+llmTarget.port) {
		t.Fatalf("expected ziti wait to name the llm proxy target, got %+v", zitiWait.Cmd)
	}
	if !strings.Contains(zitiWait.Cmd[1], `getent ahostsv4 "${h}"`) {
		t.Fatalf("expected ziti wait to resolve each target through the pod resolver, got %+v", zitiWait.Cmd)
	}
	if !strings.Contains(zitiWait.Cmd[1], `/dev/tcp/${h}/${p}`) {
		t.Fatalf("expected ziti wait to connect to each target through the tunnel, got %+v", zitiWait.Cmd)
	}
	resolverConfig := "nameserver 127.0.0.1\nsearch svc.cluster.local cluster.local\noptions ndots:5 timeout:1 attempts:1\n"
	if !strings.Contains(zitiWait.Cmd[1], strconv.Quote(resolverConfig)) {
		t.Fatalf("expected ziti wait to use only tunnel DNS in resolv.conf, got %+v", zitiWait.Cmd)
	}
	if !strings.Contains(zitiWait.Cmd[1], "dns lookup failed for") || !strings.Contains(zitiWait.Cmd[1], "tcp connect failed for") {
		t.Fatalf("expected ziti wait diagnostics to distinguish DNS and TCP failures, got %+v", zitiWait.Cmd)
	}
	// A miss overshoots by a quarter second, not a full one.
	if !strings.Contains(zitiWait.Cmd[1], "sleep 0.25") {
		t.Fatalf("expected ziti wait to poll sub-second, got %+v", zitiWait.Cmd)
	}
	if len(request.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(request.Volumes))
	}
	agynBinVolume := findVolumeSpec(request.Volumes, agynBinVolumeName)
	if agynBinVolume == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if agynBinVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", agynBinVolume.Kind)
	}
	zitiIdentityVolume := findVolumeSpec(request.Volumes, zitiIdentityVolumeName)
	if zitiIdentityVolume == nil {
		t.Fatalf("expected %s volume", zitiIdentityVolumeName)
	}
	if zitiIdentityVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected ziti identity volume kind ephemeral, got %v", zitiIdentityVolume.Kind)
	}
}

func TestAssemblerZitiDefaultsFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("PLATFORM_IDENTITY_ID", "a3c1e9d2-7f4b-5e1a-9c3d-2b8f6a4e7d10")
	t.Setenv("THREADS_ADDRESS", "")
	t.Setenv("NOTIFICATIONS_ADDRESS", "")
	t.Setenv("AGENTS_ADDRESS", "")
	t.Setenv("SECRETS_ADDRESS", "")
	t.Setenv("RUNNER_ADDRESS", "")
	t.Setenv("ZITI_ENABLED", "true")
	t.Setenv("ZITI_MANAGEMENT_ADDRESS", "")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "")
	t.Setenv("ZITI_SIDECAR_IMAGE", "")
	t.Setenv("WORKLOAD_DNS_UPSTREAM", "")
	t.Setenv("CLUSTER_DNS", "")
	t.Setenv("AGENT_GATEWAY_ADDRESS", "")
	t.Setenv("AGENT_LLM_BASE_URL", "")
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("IDLE_TIMEOUT", "")
	t.Setenv("STOP_TIMEOUT_SEC", "")
	t.Setenv("LEASE_NAME", "")
	t.Setenv("LEASE_NAMESPACE", "")
	t.Setenv("EGRESS_CA_NAMESPACE", "")

	cfg, err := config.FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}

	agentID := uuid.New()
	threadID := uuid.New()
	agent := &agentsv1.Agent{
		Name:          "assistant",
		Role:          "ops",
		Model:         "gpt-test",
		Configuration: "{}",
	}

	assembler := New(&testutil.FakeAgentsClient{}, &testutil.FakeSecretsClient{}, &cfg)
	envs := envMap(assembler.baseAgentEnvVars(agent, agentID, threadID))
	assertEnv(t, envs, "GATEWAY_ADDRESS", "gateway.agyn:443")
	assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://gateway.agyn:443")
	assertEnv(t, envs, "LLM_BASE_URL", "http://llm-proxy.agyn/v1")
	assertEnv(t, envs, "TRACING_ADDRESS", "tracing.agyn:443")
	assertEnv(t, envs, "OTEL_EXPORTER_OTLP_ENDPOINT", "http://tracing.agyn:443")
}

func TestAssemblerResolvesSecretEnv(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	resolveCalls := 0
	secretsClient := &testutil.FakeSecretsClient{
		ResolveSecretFunc: func(_ context.Context, req *secretsv1.ResolveSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
			resolveCalls++
			if req.GetId() != "secret-1" {
				return nil, errors.New("unexpected secret id")
			}
			return &secretsv1.ResolveSecretResponse{Value: "resolved"}, nil
		},
	}

	agent := &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV", Source: &agentsv1.Env_SecretId{SecretId: "secret-1"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV_TWO", Source: &agentsv1.Env_SecretId{SecretId: "secret-1"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	withRuntimeEnvironment(agent, agentsClient, cfg)
	assembler := withCatalog(NewWithRunners(agentsClient, runnersWithDefaultFlavor(), secretsClient, cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	envs := envMap(request.Main.Env)
	assertEnv(t, envs, "SECRET_ENV", "resolved")
	assertEnv(t, envs, "SECRET_ENV_TWO", "resolved")
	if resolveCalls != 1 {
		t.Fatalf("expected resolve to be cached, got %d calls", resolveCalls)
	}
}

func TestAssemblerBuildsMcpSidecarAndVolumes(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()
	volumeID := uuid.New()
	volumeStorageClass := "fast"

	agent := &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{
				{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "test-mcp", Image: "mcp-image", Command: "run-mcp"},
			}}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "MCP_ENV", Source: &agentsv1.Env_Value{Value: "enabled"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "MCP_PORT", Source: &agentsv1.Env_Value{Value: "9090"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "GATEWAY_ADDRESS", Source: &agentsv1.Env_Value{Value: "user-gateway"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "AGYN_GATEWAY_URL", Source: &agentsv1.Env_Value{Value: "http://user-gateway"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "NODE_OPTIONS", Source: &agentsv1.Env_Value{Value: "--max-old-space-size=128"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "RES_OPTIONS", Source: &agentsv1.Env_Value{Value: "ndots:2"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumesFunc: func(_ context.Context, req *agentsv1.ListVolumesRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListVolumesResponse{Volumes: []*agentsv1.Volume{
					{Meta: &agentsv1.EntityMeta{Id: volumeID.String()}, Name: "state", MountPath: "/data", Persistent: true, Size: "1Gi", StorageClass: &volumeStorageClass},
				}}, nil
			}
			return &agentsv1.ListVolumesResponse{}, nil
		},
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID.String() {
				return nil, errors.New("unexpected volume id")
			}
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{
				Meta:       &agentsv1.EntityMeta{Id: volumeID.String()},
				Persistent: true,
				MountPath:  "/data",
			}}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	withRuntimeEnvironment(agent, agentsClient, cfg)
	assembler := withCatalog(NewWithRunners(agentsClient, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	if len(request.Sidecars) != 1 {
		t.Fatalf("expected 1 sidecar, got %d", len(request.Sidecars))
	}
	sidecar := request.Sidecars[0]
	if sidecar.Image != "mcp-image" {
		t.Fatalf("expected sidecar image mcp-image, got %q", sidecar.Image)
	}
	if sidecar.Name != "mcp-"+mcpID.String()[:8] {
		t.Fatalf("unexpected sidecar name: %q", sidecar.Name)
	}
	expectedCmd := []string{"/bin/sh", "-c", "run-mcp"}
	if !equalStringSlice(sidecar.Cmd, expectedCmd) {
		t.Fatalf("unexpected sidecar cmd: %+v", sidecar.Cmd)
	}
	if len(sidecar.Mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(sidecar.Mounts))
	}
	if len(request.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(request.Volumes))
	}
	expectedName := "vol-" + volumeID.String()[:8]
	volumeSpec := findVolumeSpec(request.Volumes, expectedName)
	if volumeSpec == nil {
		t.Fatalf("expected volume %q", expectedName)
	}
	if volumeSpec.Kind != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
		t.Fatalf("expected named volume, got %v", volumeSpec.Kind)
	}
	expectedPersistent := "pv-" + threadID.String()[:12] + "-" + volumeID.String()[:12]
	if volumeSpec.PersistentName != expectedPersistent {
		t.Fatalf("expected persistent name %q, got %q", expectedPersistent, volumeSpec.PersistentName)
	}
	if volumeSpec.Size != "1Gi" {
		t.Fatalf("expected volume size 1Gi, got %q", volumeSpec.Size)
	}
	if volumeSpec.StorageClass != "fast" {
		t.Fatalf("expected storage class fast, got %q", volumeSpec.StorageClass)
	}
	agynBinVolume := findVolumeSpec(request.Volumes, agynBinVolumeName)
	if agynBinVolume == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if agynBinVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", agynBinVolume.Kind)
	}
	mount := sidecar.Mounts[0]
	if mount.Volume != expectedName {
		t.Fatalf("expected mount volume %q, got %q", expectedName, mount.Volume)
	}
	if mount.MountPath != "/data" {
		t.Fatalf("expected mount path /data, got %q", mount.MountPath)
	}
	envs := envMap(sidecar.Env)
	assertEnv(t, envs, "MCP_ENV", "enabled")
	assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
	assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://"+cfg.AgentGatewayAddress)
	assertEnv(t, envs, "NODE_OPTIONS", "--max-old-space-size=128 "+mcpNodeOptions)
	assertEnv(t, envs, "RES_OPTIONS", "ndots:2")
}

// Sharing within one workload is what shared_volumes is for: the agent writes a
// file and an MCP server reads it, from the same pod volume at the same path.
func TestSharedVolumesMountTheEnvironmentVolumeIntoTheSidecar(t *testing.T) {
	environmentID := uuid.New()
	mcpID := uuid.New()
	volumeID := uuid.New()

	agents := &testutil.FakeAgentsClient{
		ListVolumesFunc: func(_ context.Context, req *agentsv1.ListVolumesRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			if req.GetEnvironmentId() == environmentID.String() {
				return &agentsv1.ListVolumesResponse{Volumes: []*agentsv1.Volume{
					{Meta: &agentsv1.EntityMeta{Id: volumeID.String()}, Name: "workspace", MountPath: "/workspace", Persistent: true, Size: "1Gi"},
				}}, nil
			}
			return &agentsv1.ListVolumesResponse{}, nil
		},
	}
	resolver := newVolumeResolver(agents, uuid.New())
	mainMounts, err := resolver.loadEnvironmentVolumes(context.Background(), environmentID.String())
	if err != nil {
		t.Fatalf("load environment volumes: %v", err)
	}
	if len(mainMounts) != 1 || mainMounts[0].MountPath != "/workspace" {
		t.Fatalf("unexpected main mounts: %+v", mainMounts)
	}

	mcp := &agentsv1.Mcp{
		Meta:          &agentsv1.EntityMeta{Id: mcpID.String()},
		Name:          "files",
		SharedVolumes: []string{"workspace"},
	}
	sidecarMounts, err := resolver.mountsForMcp(context.Background(), mcp)
	if err != nil {
		t.Fatalf("mounts for mcp: %v", err)
	}
	if len(sidecarMounts) != 1 {
		t.Fatalf("expected 1 sidecar mount, got %d", len(sidecarMounts))
	}
	// Same pod volume, same path: a divergent path would be a debugging trap.
	if sidecarMounts[0].Volume != mainMounts[0].Volume {
		t.Fatalf("expected the sidecar to mount %q, got %q", mainMounts[0].Volume, sidecarMounts[0].Volume)
	}
	if sidecarMounts[0].MountPath != "/workspace" {
		t.Fatalf("expected /workspace, got %q", sidecarMounts[0].MountPath)
	}
}

// The same claim on a fully assembled pod rather than the resolver alone: one
// volume declared once, reaching both containers at one path.
func TestSharedVolumeReachesBothContainersOfTheAssembledPod(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	environmentID := uuid.New()
	mcpID := uuid.New()
	volumeID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Image:          "agent-image",
		EnvironmentId:  environmentID.String(),
	}
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		GetEnvironmentFunc: func(_ context.Context, req *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
			if req.GetId() != environmentID.String() {
				return nil, errors.New("unexpected environment id")
			}
			return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{
				Meta:           &agentsv1.EntityMeta{Id: environmentID.String()},
				OrganizationId: "org-1",
				Name:           "shared-runtime",
				Image:          "environment-image",
				RunnerId:       testAgentEnvironmentRunnerID,
				Flavor:         testAgentEnvironmentFlavor,
				// Assembly refuses a workload whose environment names no runtime.
				AgentRuntimeImageId:  testRuntimeImageID,
				AgentRuntimeImageTag: testRuntimeImageTag,
			}}, nil
		},
		ListSkillsFunc: func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{
				Meta:          &agentsv1.EntityMeta{Id: mcpID.String()},
				Name:          "files",
				Image:         "mcp-image",
				Command:       "run-mcp",
				SharedVolumes: []string{"workspace"},
			}}}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumesFunc: func(_ context.Context, req *agentsv1.ListVolumesRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			if req.GetEnvironmentId() == environmentID.String() {
				return &agentsv1.ListVolumesResponse{Volumes: []*agentsv1.Volume{{
					Meta:       &agentsv1.EntityMeta{Id: volumeID.String()},
					Name:       "workspace",
					MountPath:  "/workspace",
					Persistent: true,
					Size:       "1Gi",
				}}}, nil
			}
			return &agentsv1.ListVolumesResponse{}, nil
		},
	}

	runnersClient := &fakeRunnersClient{
		listFlavors: func(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{{
				RunnerId:  testAgentEnvironmentRunnerID,
				Name:      testAgentEnvironmentFlavor,
				Default:   true,
				Resources: &runnersv1.ComputeResources{RequestsCpu: "500m", RequestsMemory: "1Gi"},
			}}}, nil
		},
	}
	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	// This test wires its own environment, runner and flavor; it only needs the
	// catalog that resolves the runtime image the environment names.
	cfg.ImageProxyHost = testCatalogProxyHost
	assembler := withCatalog(NewWithRunners(agentsClient, runnersClient, &testutil.FakeSecretsClient{}, cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}

	expectedName := "vol-" + volumeID.String()[:8]
	mainMount := findVolumeMount(result.Request.GetMain(), expectedName)
	if mainMount == nil {
		t.Fatalf("expected the main container to mount %q, got %+v", expectedName, result.Request.GetMain().GetMounts())
	}
	if len(result.Request.GetSidecars()) != 1 {
		t.Fatalf("expected 1 sidecar, got %d", len(result.Request.GetSidecars()))
	}
	sidecarMount := findVolumeMount(result.Request.GetSidecars()[0], expectedName)
	if sidecarMount == nil {
		t.Fatalf("expected the sidecar to mount %q, got %+v", expectedName, result.Request.GetSidecars()[0].GetMounts())
	}
	if mainMount.MountPath != sidecarMount.MountPath {
		t.Fatalf("expected one path in both containers, got %q and %q", mainMount.MountPath, sidecarMount.MountPath)
	}

	// Declared once: a second spec would be a second PVC holding different bytes.
	matching := 0
	for _, spec := range result.Request.GetVolumes() {
		if spec.GetName() == expectedName {
			matching++
		}
	}
	if matching != 1 {
		t.Fatalf("expected the shared volume declared once, got %d specs", matching)
	}
}

// A share the environment does not declare fails scheduling rather than leaving
// a sidecar silently missing the files it was configured to read.
func TestSharedVolumesRejectAnUnresolvableName(t *testing.T) {
	resolver := newVolumeResolver(&testutil.FakeAgentsClient{}, uuid.New())
	if _, err := resolver.loadEnvironmentVolumes(context.Background(), uuid.NewString()); err != nil {
		t.Fatalf("load environment volumes: %v", err)
	}
	_, err := resolver.mountsForMcp(context.Background(), &agentsv1.Mcp{
		Meta:          &agentsv1.EntityMeta{Id: uuid.NewString()},
		Name:          "files",
		SharedVolumes: []string{"nope"},
	})
	if err == nil {
		t.Fatal("expected an unresolvable shared volume to fail")
	}
}

// A shared volume landing on a path the sidecar already uses is refused: the
// same path cannot hold two different volumes.
func TestSharedVolumesRejectAMountPathCollision(t *testing.T) {
	environmentID := uuid.New()
	mcpID := uuid.New()

	agents := &testutil.FakeAgentsClient{
		ListVolumesFunc: func(_ context.Context, req *agentsv1.ListVolumesRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			if req.GetEnvironmentId() == environmentID.String() {
				return &agentsv1.ListVolumesResponse{Volumes: []*agentsv1.Volume{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "shared", MountPath: "/data"},
				}}, nil
			}
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListVolumesResponse{Volumes: []*agentsv1.Volume{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "own", MountPath: "/data"},
				}}, nil
			}
			return &agentsv1.ListVolumesResponse{}, nil
		},
	}
	resolver := newVolumeResolver(agents, uuid.New())
	if _, err := resolver.loadEnvironmentVolumes(context.Background(), environmentID.String()); err != nil {
		t.Fatalf("load environment volumes: %v", err)
	}
	_, err := resolver.mountsForMcp(context.Background(), &agentsv1.Mcp{
		Meta:          &agentsv1.EntityMeta{Id: mcpID.String()},
		Name:          "files",
		SharedVolumes: []string{"shared"},
	})
	if err == nil {
		t.Fatal("expected a mount path collision to fail")
	}
}

func TestAssemblerMcpPortAllocation(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	lowID := "11111111-1111-1111-1111-111111111111"
	highID := "22222222-2222-2222-2222-222222222222"

	agent := &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{
				{Meta: &agentsv1.EntityMeta{Id: highID}, Name: "filesystem", Image: "fs-image", Command: "run-fs"},
				{Meta: &agentsv1.EntityMeta{Id: lowID}, Name: "memory", Image: "mem-image", Command: "run-mem"},
			}}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	withRuntimeEnvironment(agent, agentsClient, cfg)
	assembler := withCatalog(NewWithRunners(agentsClient, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	if len(request.Sidecars) != 2 {
		t.Fatalf("expected 2 sidecars, got %d", len(request.Sidecars))
	}
	mainEnvs := envMap(request.Main.Env)
	expectedServers := fmt.Sprintf("%s:%d,%s:%d", "memory", mcpBasePort, "filesystem", mcpBasePort+1)
	assertEnv(t, mainEnvs, "AGENT_MCP_SERVERS", expectedServers)

	ports := map[string]string{}
	for _, sidecar := range request.Sidecars {
		envs := envMap(sidecar.Env)
		port, ok := envs["MCP_PORT"]
		if !ok {
			t.Fatalf("missing MCP_PORT for sidecar %s", sidecar.Name)
		}
		assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
		assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://"+cfg.AgentGatewayAddress)
		assertEnv(t, envs, "RES_OPTIONS", mcpResolverOptions)
		assertEnv(t, envs, "NODE_OPTIONS", mcpNodeOptions)
		ports[sidecar.Name] = port
	}
	expectedMemoryName := "mcp-" + lowID[:8]
	expectedFilesystemName := "mcp-" + highID[:8]
	if ports[expectedMemoryName] != fmt.Sprintf("%d", mcpBasePort) {
		t.Fatalf("expected %s MCP_PORT %d, got %q", expectedMemoryName, mcpBasePort, ports[expectedMemoryName])
	}
	if ports[expectedFilesystemName] != fmt.Sprintf("%d", mcpBasePort+1) {
		t.Fatalf("expected %s MCP_PORT %d, got %q", expectedFilesystemName, mcpBasePort+1, ports[expectedFilesystemName])
	}
}

func TestAssemblerNoMcpsNoAgentMcpServersEnv(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	withRuntimeEnvironment(agent, agentsClient, cfg)
	assembler := withCatalog(NewWithRunners(agentsClient, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, cfg), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	envs := envMap(result.Request.Main.Env)
	if _, ok := envs["AGENT_MCP_SERVERS"]; ok {
		t.Fatal("expected AGENT_MCP_SERVERS to be absent")
	}
}

func envMap(envs []*runnerv1.EnvVar) map[string]string {
	result := make(map[string]string, len(envs))
	for _, env := range envs {
		if env == nil {
			continue
		}
		result[env.Name] = env.Value
	}
	return result
}

func findVolumeMount(container *runnerv1.ContainerSpec, volumeName string) *runnerv1.VolumeMount {
	if container == nil {
		return nil
	}
	for _, mount := range container.Mounts {
		if mount != nil && mount.GetVolume() == volumeName {
			return mount
		}
	}
	return nil
}

func findMountByPath(mounts []*runnerv1.VolumeMount, path string) *runnerv1.VolumeMount {
	for _, mount := range mounts {
		if mount != nil && mount.GetMountPath() == path {
			return mount
		}
	}
	return nil
}

func countMountsByPath(mounts []*runnerv1.VolumeMount, path string) int {
	count := 0
	for _, mount := range mounts {
		if mount != nil && mount.GetMountPath() == path {
			count++
		}
	}
	return count
}

func findVolumeSpec(volumes []*runnerv1.VolumeSpec, name string) *runnerv1.VolumeSpec {
	for _, volume := range volumes {
		if volume.GetName() == name {
			return volume
		}
	}
	return nil
}

func assertEnv(t *testing.T, envs map[string]string, name, expected string) {
	t.Helper()
	value, ok := envs[name]
	if !ok {
		t.Fatalf("missing env %s", name)
	}
	if value != expected {
		t.Fatalf("expected env %s=%q, got %q", name, expected, value)
	}
}

func assertSameZitiIdentityMount(t *testing.T, container *runnerv1.ContainerSpec) {
	t.Helper()
	if len(container.GetMounts()) != 1 {
		t.Fatalf("expected 1 ziti identity mount on %s, got %d", container.GetName(), len(container.GetMounts()))
	}
	mount := container.GetMounts()[0]
	if mount.GetVolume() != zitiIdentityVolumeName {
		t.Fatalf("expected ziti mount volume %q on %s, got %q", zitiIdentityVolumeName, container.GetName(), mount.GetVolume())
	}
	if mount.GetMountPath() != zitiIdentityMountPath {
		t.Fatalf("expected ziti mount path %q on %s, got %q", zitiIdentityMountPath, container.GetName(), mount.GetMountPath())
	}
}

func equalStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func equalStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i, value := range left {
		if right[i] != value {
			return false
		}
	}
	return true
}

func TestAssemblerDistributesEgressCA(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()
	cert := []byte("test-ca")

	agent := &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "mcp", Image: "mcp-image", Command: "run-mcp"}}}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress:              "gateway:50051",
		AgentLLMBaseURL:                  "http://llm:8080/v1",
		ZitiEnabled:                      true,
		ZitiSidecarImage:                 "ziti-image",
		WorkloadDNSUpstream:              "10.43.0.10",
		ZitiEnrollmentDNSUpstream:        "10.43.0.10",
		ZitiRuntimeControllerResolveHost: "istio-ingressgateway.istio-gateway.svc.cluster.local",
		ZitiRuntimeControllerPort:        "443",
	}
	withRuntimeEnvironment(agent, agentsClient, cfg)
	assembler := withCatalog(NewWithRunnersAndEgressCA(agentsClient, runnersWithDefaultFlavor(), &testutil.FakeSecretsClient{}, cfg, cert), agent.GetOrganizationId())
	result, err := assembler.Assemble(ctx, agentID, threadID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	// Contains, not equals: the file is a bundle -- the public roots with the
	// egress CA appended -- because installing the CA as the whole trust store
	// broke every connection egress does not intercept.
	if !strings.Contains(string(request.GetInlineFiles()[egressCACertPath]), string(cert)) {
		t.Fatalf("expected the egress CA in the inline bundle")
	}
	containers := []*runnerv1.ContainerSpec{request.Main}
	containers = append(containers, request.GetSidecars()...)
	containers = append(containers, request.GetInitContainers()...)
	for _, container := range containers {
		assertEgressCAEnv(t, container)
		assertInlineFileMount(t, container, egressCACertPath)
	}
	zitiEnroll := testutil.FindInitContainer(request.GetInitContainers(), ZitiEnrollContainerName)
	if zitiEnroll == nil {
		t.Fatal("expected ziti-enroll init container")
	}
}

func assertEgressCAEnv(t *testing.T, container *runnerv1.ContainerSpec) {
	t.Helper()
	envs := envMap(container.GetEnv())
	assertEnv(t, envs, "SSL_CERT_FILE", egressCACertPath)
	assertEnv(t, envs, "REQUESTS_CA_BUNDLE", egressCACertPath)
	assertEnv(t, envs, "NODE_EXTRA_CA_CERTS", egressCACertPath)
}

func assertInlineFileMount(t *testing.T, container *runnerv1.ContainerSpec, expectedPath string) {
	t.Helper()
	for _, mount := range container.GetInlineFileMounts() {
		if mount.GetPath() == expectedPath {
			return
		}
	}
	t.Fatalf("container %s missing inline file mount %s", container.GetName(), expectedPath)
}

type staticSecretGetter struct {
	secret *corev1.Secret
}

func (g staticSecretGetter) Get(context.Context, string, string) (*corev1.Secret, error) {
	return g.secret, nil
}

func TestLoadEgressCACertificate(t *testing.T) {
	cert, err := LoadEgressCACertificate(context.Background(), staticSecretGetter{secret: &corev1.Secret{
		Data: map[string][]byte{egressCASecretKey: []byte("cert")},
	}}, "platform")
	if err != nil {
		t.Fatalf("load egress CA: %v", err)
	}
	if string(cert) != "cert" {
		t.Fatalf("expected cert bytes, got %q", string(cert))
	}
}

// hostBash resolves bash on whatever runs the test. zitiEnrollEntrypoint is the
// path inside the sidecar image, where bash is at /usr/bin/bash; macOS keeps it
// at /bin/bash, so running the script through the constant fails there.
func hostBash(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Skipf("bash not available: %v", err)
	}
	return path
}

func TestZitiEnrollScriptRemovesOnlyJwtControllerLoopbackAlias(t *testing.T) {
	workDir := t.TempDir()
	identityDir := filepath.Join(workDir, "netfoundry")
	resolvPath := filepath.Join(workDir, "resolv.conf")
	hostsPath := filepath.Join(workDir, "hosts")
	logPath := filepath.Join(workDir, "enroll.log")
	controllerHost := "controller.example.test"
	otherHost := "other.example.test"
	jwt := testJWTWithIssuer(t, "https://"+controllerHost+":2496")

	if err := os.WriteFile(hostsPath, []byte(strings.Join([]string{
		"127.0.0.1\tlocalhost",
		"127.0.0.1\t" + controllerHost,
		"127.0.0.1\t" + otherHost,
		"10.43.58.17\t" + controllerHost,
		"10.42.1.121\tworkload-c84f0a23-ec5b-4410-97bb-2392195055a3",
		"",
	}, "\n")), 0o600); err != nil {
		t.Fatalf("write hosts: %v", err)
	}
	_ = writeExecutable(t, workDir, "openssl", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'openssl_resolv=%%s\n' "$(cat %s)" >> %s
case "$1" in
  s_client)
    cat <<'CERT'
-----BEGIN CERTIFICATE-----
controller-ca
-----END CERTIFICATE-----
CERT
    ;;
  ecparam)
    printf 'key\n' > "${@: -1}"
    ;;
  req)
    printf 'csr\n' > "${@: -1}"
    ;;
  *)
    echo "unexpected openssl args" >&2
    exit 1
    ;;
esac
`, resolvPath, logPath))
	_ = writeExecutable(t, workDir, "ziti", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'ziti_args=%%s\n' "$*" >> %s
printf 'ziti_hosts=%%s\n' "$(cat %s)" >> %s
if [[ "$*" != "edge enroll --jwt "* ]]; then
  echo "unexpected ziti args: $*" >&2
  exit 1
fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) shift; out="$1" ;;
  esac
  shift || true
done
printf '{"ztAPI":"https://controller.example.test:2496/edge/client/v1","id":{"cert":"agent-cert","key":"agent-key","ca":"controller-ca"}}' > "${out}"
`, logPath, hostsPath, logPath))
	_ = writeExecutable(t, workDir, "getent", `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" != "ahostsv4" ]]; then
  echo "unexpected getent args: $*" >&2
  exit 1
fi
case "${2:-}" in
  controller.example.test) printf '10.43.58.17 STREAM controller.example.test\n' ;;
  ziti-controller-client.ziti.svc.cluster.local) printf '10.43.253.228 STREAM ziti-controller-client.ziti.svc.cluster.local\n' ;;
  *) exit 2 ;;
esac
`)
	_ = writeExecutable(t, workDir, "jq", `#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "-r" ]]; then
  shift
fi
if [[ "$1" == "--arg" ]]; then
  shift; name="$1"; shift; value="$1"; shift
  if [[ "${1:-}" == '.ztAPI = $ztAPI | del(.ztAPIs)' ]]; then
    sed -E "s#\"ztAPI\":\"[^\"]+\"(,\"ztAPIs\":\[[^]]*\])?#\"ztAPI\":\"${value}\"#" "${2:-}"
    exit 0
  fi
fi
filter="${1:-}"
file="${2:-}"
if [[ "$filter" == "has(\"ztAPIs\")" ]]; then
  if grep -q '"ztAPIs"' "$file"; then exit 0; fi
  exit 1
fi
case "$filter" in
  ".iss // empty") sed -nE 's/.*"iss":"([^"]+)".*/\1/p' "$file" ;;
  ".em // empty") sed -nE 's/.*"em":"([^"]+)".*/\1/p' "$file" ;;
  ".jti // empty") sed -nE 's/.*"jti":"([^"]+)".*/\1/p' "$file" ;;
  ".sub // empty") sed -nE 's/.*"sub":"([^"]+)".*/\1/p' "$file" ;;
  ".ztAPI // empty") sed -nE 's/.*"ztAPI":"([^"]+)".*/\1/p' "$file" ;;
  *) echo "unexpected jq filter: $filter" >&2; exit 1 ;;
esac
`)
	// The stub shadows cat on PATH, so it has to exec the real one by absolute
	// path -- which is /bin/cat on macOS and /usr/bin/cat on Linux.
	realCat, err := exec.LookPath("cat")
	if err != nil {
		t.Skipf("cat not available: %v", err)
	}
	_ = writeExecutable(t, workDir, "cat", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
real_cat=%q
if [[ "$#" -ge 1 && "$1" == %q ]]; then
  printf 'nameserver 127.0.0.1\n' > %q
fi
exec "${real_cat}" "$@"
`, realCat, hostsPath+".tmp", resolvPath))

	cmd := exec.Command(hostBash(t), buildZitiEnrollCommand("10.43.0.10", "", "", "", "")...)
	cmd.Env = append(os.Environ(),
		"PATH="+workDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		ZitiEnrollmentTokenEnvVar+"="+jwt,
		ZitiIdentityBasenameEnvVar+"="+ZitiIdentityBasename,
		ZitiIdentityDirEnvVar+"="+identityDir,
		"ZITI_RESOLV_CONF="+resolvPath,
		"ZITI_HOSTS_FILE="+hostsPath,
		ZitiEnrollmentControllerResolveHostEnvVar+"=ziti-controller-client.ziti.svc.cluster.local",
		ZitiEnrollmentControllerPortEnvVar+"=2496",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run ziti enroll script: %v\n%s", err, string(output))
	}

	assertFileEquals(t, resolvPath, "nameserver 127.0.0.1\nsearch svc.cluster.local cluster.local\noptions ndots:5\n")
	hostsBytes, err := os.ReadFile(hostsPath)
	if err != nil {
		t.Fatalf("read hosts: %v", err)
	}
	hosts := string(hostsBytes)
	if strings.Contains(hosts, "127.0.0.1\t"+controllerHost) {
		t.Fatalf("expected controller loopback alias removed, got hosts:\n%s", hosts)
	}
	if !strings.Contains(hosts, "127.0.0.1\t"+otherHost) {
		t.Fatalf("expected unrelated loopback alias preserved, got hosts:\n%s", hosts)
	}
	if !strings.Contains(hosts, "10.42.1.121\tworkload-c84f0a23-ec5b-4410-97bb-2392195055a3") {
		t.Fatalf("expected workload host alias preserved, got hosts:\n%s", hosts)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read enroll log: %v", err)
	}
	log := string(logBytes) + string(output)
	if !strings.Contains(log, "openssl_resolv=nameserver 10.43.0.10") || !strings.Contains(log, "ziti_args=edge enroll") {
		t.Fatalf("expected enrollment to use upstream resolver, got:\n%s", log)
	}
	if !strings.Contains(log, " --ca "+filepath.Join(identityDir, "controller-tls-ca.pem")) {
		t.Fatalf("expected canonical enrollment to trust combined controller CA bundle, got:\n%s", log)
	}
	if !strings.Contains(log, "ziti_args=edge enroll --jwt "+filepath.Join(identityDir, "agent.jwt")+" --ca "+filepath.Join(identityDir, "controller-tls-ca.pem")+" --out "+filepath.Join(identityDir, "agent.json")) {
		t.Fatalf("expected canonical ziti enrollment with controller CA bundle, got:\n%s", log)
	}
	if !strings.Contains(log, "10.43.253.228\t"+controllerHost) {
		t.Fatalf("expected canonical ziti enrollment to resolve advertised host through enrollment controller service, got:\n%s", log)
	}
	identityBytes, err := os.ReadFile(filepath.Join(identityDir, "agent.json"))
	if err != nil {
		t.Fatalf("read identity file: %v", err)
	}
	identity := string(identityBytes)
	if !strings.Contains(identity, "https://controller.example.test:2496/edge/client/v1") || !strings.Contains(identity, "agent-cert") || !strings.Contains(identity, "controller-ca") {
		t.Fatalf("expected enrolled identity json with advertised controller endpoint, got:\n%s", identity)
	}
	if !strings.Contains(identity, "agent-key") {
		t.Fatalf("expected canonical enrolled identity to preserve private key material, got:\n%s", identity)
	}
	if strings.Contains(identity, "ziti-controller-client.ziti.svc.cluster.local") {
		t.Fatalf("expected identity runtime API to avoid cluster-local controller DNS, got:\n%s", identity)
	}
	if strings.Contains(identity, "ztAPIs") {
		t.Fatalf("expected single-controller identity to avoid ztAPIs so stock tunnel stays on cert auth, got:\n%s", identity)
	}
	if !strings.Contains(log, "ziti_identity_ztAPI=https://controller.example.test:2496/edge/client/v1") {
		t.Fatalf("expected enrollment diagnostics to print patched ztAPI, got:\n%s", log)
	}
	if !strings.Contains(log, "ziti_runtime_host_alias=10.43.58.17\t"+controllerHost) {
		t.Fatalf("expected enrollment diagnostics to print runtime controller host alias, got:\n%s", log)
	}
	runtimeHostsBytes, err := os.ReadFile(filepath.Join(identityDir, "agent.runtime.hosts"))
	if err != nil {
		t.Fatalf("read runtime hosts file: %v", err)
	}
	if string(runtimeHostsBytes) != "10.43.58.17\t"+controllerHost+"\n" {
		t.Fatalf("expected runtime hosts file for controller auth, got:\n%s", string(runtimeHostsBytes))
	}
	if strings.Contains(identity, "https://10.43.58.17:2496") || strings.Contains(identity, "https://10.43.253.228:2496") {
		t.Fatalf("expected identity runtime API to avoid direct controller IP, got:\n%s", identity)
	}
}

func TestZitiEnrollScriptSplitsEnrollmentAndRuntimeControllers(t *testing.T) {
	workDir := t.TempDir()
	identityDir := filepath.Join(workDir, "netfoundry")
	resolvPath := filepath.Join(workDir, "resolv.conf")
	hostsPath := filepath.Join(workDir, "hosts")
	logPath := filepath.Join(workDir, "enroll.log")
	controllerHost := "controller.example.test"
	enrollmentResolveHost := "ziti-controller-client.ziti.svc.cluster.local"
	runtimeResolveHost := "istio-ingressgateway.istio-gateway.svc.cluster.local"
	jwt := testJWTWithIssuer(t, "https://"+controllerHost+":2496")

	if err := os.WriteFile(hostsPath, []byte("127.0.0.1\tlocalhost\n"), 0o600); err != nil {
		t.Fatalf("write hosts: %v", err)
	}
	_ = writeExecutable(t, workDir, "openssl", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'openssl_args=%%s\n' "$*" >> %s
case "$1" in
  s_client)
    cat <<'CERT'
-----BEGIN CERTIFICATE-----
controller-ca
-----END CERTIFICATE-----
CERT
    ;;
  ecparam)
    printf 'key\n' > "${@: -1}"
    ;;
  req)
    printf 'csr\n' > "${@: -1}"
    ;;
  *)
    echo "unexpected openssl args" >&2
    exit 1
    ;;
esac
`, logPath))
	_ = writeExecutable(t, workDir, "ziti", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'ziti_args=%%s\n' "$*" >> %s
printf 'ziti_hosts=%%s\n' "$(cat %s)" >> %s
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) shift; out="$1" ;;
  esac
  shift || true
done
printf '{"ztAPI":"https://controller.example.test:2496/edge/client/v1","id":{"cert":"agent-cert","key":"agent-key","ca":"controller-ca"}}' > "${out}"
`, logPath, hostsPath, logPath))
	_ = writeExecutable(t, workDir, "getent", `#!/usr/bin/env bash
set -euo pipefail
case "${2:-}" in
  controller.example.test) exit 2 ;;
  ziti-controller-client.ziti.svc.cluster.local) printf '10.43.253.228 STREAM ziti-controller-client.ziti.svc.cluster.local\n' ;;
  istio-ingressgateway.istio-gateway.svc.cluster.local) printf '10.43.0.99 STREAM istio-ingressgateway.istio-gateway.svc.cluster.local\n' ;;
  controller.example.test) printf '10.43.253.228 STREAM controller.example.test\n' ;;
  *) exit 2 ;;
esac
`)
	_ = writeExecutable(t, workDir, "jq", `#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "-r" ]]; then
  shift
fi
if [[ "$1" == "--arg" ]]; then
  shift; name="$1"; shift; value="$1"; shift
  if [[ "${1:-}" == '.ztAPI = $ztAPI | del(.ztAPIs)' ]]; then
    sed -E "s#\"ztAPI\":\"[^\"]+\"(,\"ztAPIs\":\[[^]]*\])?#\"ztAPI\":\"${value}\"#" "${2:-}"
    exit 0
  fi
fi
filter="${1:-}"
file="${2:-}"
if [[ "$filter" == "has(\"ztAPIs\")" ]]; then
  if grep -q '"ztAPIs"' "$file"; then exit 0; fi
  exit 1
fi
case "$filter" in
  ".iss // empty") sed -nE 's/.*"iss":"([^"]+)".*/\1/p' "$file" ;;
  ".em // empty") sed -nE 's/.*"em":"([^"]+)".*/\1/p' "$file" ;;
  ".jti // empty") sed -nE 's/.*"jti":"([^"]+)".*/\1/p' "$file" ;;
  ".sub // empty") sed -nE 's/.*"sub":"([^"]+)".*/\1/p' "$file" ;;
  ".ztAPI // empty") sed -nE 's/.*"ztAPI":"([^"]+)".*/\1/p' "$file" ;;
  *) echo "unexpected jq filter: $filter" >&2; exit 1 ;;
esac
`)

	cmd := exec.Command(hostBash(t), buildZitiEnrollCommand("10.43.0.10", enrollmentResolveHost, "2496", runtimeResolveHost, "443")...)
	cmd.Env = append(os.Environ(),
		"PATH="+workDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		ZitiEnrollmentTokenEnvVar+"="+jwt,
		ZitiIdentityBasenameEnvVar+"="+ZitiIdentityBasename,
		ZitiIdentityDirEnvVar+"="+identityDir,
		"ZITI_RESOLV_CONF="+resolvPath,
		"ZITI_HOSTS_FILE="+hostsPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run ziti enroll script: %v\n%s", err, string(output))
	}

	hostsBytes, err := os.ReadFile(hostsPath)
	if err != nil {
		t.Fatalf("read hosts: %v", err)
	}
	hosts := string(hostsBytes)
	if strings.Contains(hosts, controllerHost) {
		t.Fatalf("expected runtime controller host alias not to be pinned in hosts, got hosts:\n%s", hosts)
	}
	if strings.Contains(hosts, "10.43.253.228\t"+controllerHost) {
		t.Fatalf("expected enrollment controller host alias not to be persisted for runtime, got hosts:\n%s", hosts)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read enroll log: %v", err)
	}
	log := string(logBytes) + string(output)
	if !strings.Contains(log, "-connect 10.43.253.228:2496") || !strings.Contains(log, "10.43.253.228\t"+controllerHost) {
		t.Fatalf("expected enrollment to use enrollment-resolved controller underlay, got:\n%s", log)
	}
	if strings.Contains(log, "--resolve "+controllerHost+":443:10.43.253.228") || strings.Contains(log, "-connect 10.43.0.99:2496") {
		t.Fatalf("expected enrollment and runtime controller underlays to stay split, got:\n%s", log)
	}
	identityBytes, err := os.ReadFile(filepath.Join(identityDir, "agent.json"))
	if err != nil {
		t.Fatalf("read identity file: %v", err)
	}
	identity := string(identityBytes)
	if !strings.Contains(identity, "https://"+controllerHost+":443/edge/client/v1") {
		t.Fatalf("expected identity runtime API to preserve advertised host with configured runtime port, got:\n%s", identity)
	}
	if strings.Contains(identity, "ztAPIs") {
		t.Fatalf("expected runtime API patch not to add ztAPIs because stock tunnel treats HA configs as OIDC-only, got:\n%s", identity)
	}
	if !strings.Contains(log, "ziti_identity_ztAPI=https://"+controllerHost+":443/edge/client/v1") {
		t.Fatalf("expected enrollment diagnostics to print runtime ztAPI, got:\n%s", log)
	}
	if !strings.Contains(log, "ziti_runtime_host_alias=10.43.0.99\t"+controllerHost) {
		t.Fatalf("expected enrollment script to persist runtime controller host alias for sidecar auth, got:\n%s", log)
	}
	runtimeHostsBytes, err := os.ReadFile(filepath.Join(identityDir, "agent.runtime.hosts"))
	if err != nil {
		t.Fatalf("read runtime hosts file: %v", err)
	}
	if string(runtimeHostsBytes) != "10.43.0.99\t"+controllerHost+"\n" {
		t.Fatalf("expected runtime hosts file to use runtime controller underlay, got:\n%s", string(runtimeHostsBytes))
	}
}

// Without an override the port comes from the JWT the controller issued, which
// is the port its Service listens on. A default here silently sent every
// workload on a VM that had moved off it to a port nothing answered.
func TestZitiEnrollScriptTakesControllerPortFromTheJWT(t *testing.T) {
	workDir := t.TempDir()
	identityDir := filepath.Join(workDir, "netfoundry")
	resolvPath := filepath.Join(workDir, "resolv.conf")
	hostsPath := filepath.Join(workDir, "hosts")
	logPath := filepath.Join(workDir, "enroll.log")
	controllerHost := "controller.example.test"
	resolveHost := "ziti-controller-client.ziti.svc.cluster.local"
	// Deliberately not the port any default ever used.
	jwt := testJWTWithIssuer(t, "https://"+controllerHost+":2497")

	if err := os.WriteFile(hostsPath, []byte("127.0.0.1\tlocalhost\n"), 0o600); err != nil {
		t.Fatalf("write hosts: %v", err)
	}
	_ = writeExecutable(t, workDir, "openssl", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'openssl_args=%%s\n' "$*" >> %s
case "$1" in
  s_client)
    cat <<'CERT'
-----BEGIN CERTIFICATE-----
controller-ca
-----END CERTIFICATE-----
CERT
    ;;
  ecparam)
    printf 'key\n' > "${@: -1}"
    ;;
  req)
    printf 'csr\n' > "${@: -1}"
    ;;
  *)
    echo "unexpected openssl args" >&2
    exit 1
    ;;
esac
`, logPath))
	_ = writeExecutable(t, workDir, "ziti", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
printf 'ziti_args=%%s\n' "$*" >> %s
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) shift; out="$1" ;;
  esac
  shift || true
done
printf '{"ztAPI":"https://controller.example.test:2497/edge/client/v1","id":{"cert":"agent-cert","key":"agent-key","ca":"controller-ca"}}' > "${out}"
`, logPath))
	_ = writeExecutable(t, workDir, "getent", `#!/usr/bin/env bash
set -euo pipefail
case "${2:-}" in
  ziti-controller-client.ziti.svc.cluster.local) printf '10.43.253.228 STREAM ziti-controller-client.ziti.svc.cluster.local\n' ;;
  *) exit 2 ;;
esac
`)
	_ = writeExecutable(t, workDir, "jq", `#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "-r" ]]; then
  shift
fi
if [[ "$1" == "--arg" ]]; then
  shift; name="$1"; shift; value="$1"; shift
  if [[ "${1:-}" == '.ztAPI = $ztAPI | del(.ztAPIs)' ]]; then
    sed -E "s#\"ztAPI\":\"[^\"]+\"(,\"ztAPIs\":\[[^]]*\])?#\"ztAPI\":\"${value}\"#" "${2:-}"
    exit 0
  fi
fi
filter="${1:-}"
file="${2:-}"
if [[ "$filter" == "has(\"ztAPIs\")" ]]; then
  if grep -q '"ztAPIs"' "$file"; then exit 0; fi
  exit 1
fi
case "$filter" in
  ".iss // empty") sed -nE 's/.*"iss":"([^"]+)".*/\1/p' "$file" ;;
  ".em // empty") sed -nE 's/.*"em":"([^"]+)".*/\1/p' "$file" ;;
  ".jti // empty") sed -nE 's/.*"jti":"([^"]+)".*/\1/p' "$file" ;;
  ".sub // empty") sed -nE 's/.*"sub":"([^"]+)".*/\1/p' "$file" ;;
  ".ztAPI // empty") sed -nE 's/.*"ztAPI":"([^"]+)".*/\1/p' "$file" ;;
  *) echo "unexpected jq filter: $filter" >&2; exit 1 ;;
esac
`)

	cmd := exec.Command(hostBash(t), buildZitiEnrollCommand("10.43.0.10", resolveHost, "", resolveHost, "")...)
	cmd.Env = append(os.Environ(),
		"PATH="+workDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		ZitiEnrollmentTokenEnvVar+"="+jwt,
		ZitiIdentityBasenameEnvVar+"="+ZitiIdentityBasename,
		ZitiIdentityDirEnvVar+"="+identityDir,
		"ZITI_RESOLV_CONF="+resolvPath,
		"ZITI_HOSTS_FILE="+hostsPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run ziti enroll script: %v\n%s", err, string(output))
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read enroll log: %v", err)
	}
	log := string(logBytes) + string(output)
	if !strings.Contains(log, "-connect 10.43.253.228:2497") {
		t.Fatalf("expected enrollment to dial the port the JWT advertises, got:\n%s", log)
	}
	if strings.Contains(log, ":2496") {
		t.Fatalf("expected no port other than the one the JWT advertises, got:\n%s", log)
	}
}

func TestZitiEnrollmentScriptPatchesOnlyRuntimeAPI(t *testing.T) {
	if !strings.Contains(zitiEnrollScript, `ziti edge enroll --jwt "${jwt_file}" --ca "${ziti_tls_ca_cert}" --out "${identity_file}"`) {
		t.Fatalf("expected canonical ziti edge enrollment, got %q", zitiEnrollScript)
	}
	if !strings.Contains(zitiEnrollScript, `jq --arg ztAPI "https://${ziti_runtime_controller_host}:${ziti_runtime_controller_port}/edge/client/v1" '.ztAPI = $ztAPI | del(.ztAPIs)' "${identity_file}"`) {
		t.Fatalf("expected runtime patch to update only controller API endpoints, got %q", zitiEnrollScript)
	}
	if !strings.Contains(zitiEnrollScript, `printf '%s\t%s\n' "${ziti_runtime_controller_ip}" "${ziti_runtime_controller_host}" > "${runtime_hosts_file}"`) {
		t.Fatalf("expected enrollment script to persist runtime host alias for sidecar auth, got %q", zitiEnrollScript)
	}
	if !strings.Contains(zitiEnrollScript, `ziti_runtime_resolve_host="${ziti_runtime_controller_host}"`) {
		t.Fatalf("expected enrollment script to resolve runtime controller host explicitly, got %q", zitiEnrollScript)
	}
	if !strings.Contains(zitiEnrollScript, `if jq -e 'has("ztAPIs")' "${identity_file}" >/dev/null; then`) {
		t.Fatalf("expected enrollment script to fail if ztAPIs remains in identity, got %q", zitiEnrollScript)
	}
	for _, forbidden := range []string{`openssl ecparam`, `openssl req`, `/edge/client/v1/enroll`, `id:{`, `cert:`, `key:`, `ca:`} {
		if strings.Contains(zitiEnrollScript, forbidden) {
			t.Fatalf("expected enrollment script not to hand-build identity field %q, got %q", forbidden, zitiEnrollScript)
		}
	}
}

func testJWTWithIssuer(t *testing.T, issuer string) string {
	t.Helper()
	encode := base64.RawURLEncoding.EncodeToString
	return strings.Join([]string{
		encode([]byte(`{"alg":"none"}`)),
		encode([]byte(fmt.Sprintf(`{"iss":%q,"em":"ott","jti":"token-id","sub":"agent-subject"}`, issuer))),
		"signature",
	}, ".")
}

func writeExecutable(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write executable %s: %v", name, err)
	}
	return path
}

func assertFileEquals(t *testing.T, path, expected string) {
	t.Helper()
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(actual) != expected {
		t.Fatalf("expected %s to contain %q, got %q", path, expected, string(actual))
	}
}

func TestZitiServiceWaitTargetsLLMProxyTCP(t *testing.T) {
	target, err := zitiServiceWaitTarget("http://llm-proxy.agyn/v1")
	if err != nil {
		t.Fatalf("zitiServiceWaitTarget: %v", err)
	}
	if target.host != "llm-proxy.agyn" || target.port != "80" {
		t.Fatalf("expected llm-proxy.agyn:80, got %s:%s", target.host, target.port)
	}
	cmd := buildZitiWaitCommand("gateway.agyn:443", target)
	if !strings.Contains(cmd[1], "llm-proxy.agyn:80") {
		t.Fatalf("expected ziti wait to name llm-proxy.agyn:80, got %+v", cmd)
	}
	if strings.Contains(cmd[1], "/v1/models") || strings.Contains(cmd[1], "curl") {
		t.Fatalf("expected ziti service wait not to use HTTP/model-list readiness, got %+v", cmd)
	}
}

func TestZitiServiceWaitTargetPreservesExplicitPort(t *testing.T) {
	target, err := zitiServiceWaitTarget("https://llm-proxy.agyn:8443/v1")
	if err != nil {
		t.Fatalf("zitiServiceWaitTarget: %v", err)
	}
	if target.host != "llm-proxy.agyn" || target.port != "8443" {
		t.Fatalf("expected llm-proxy.agyn:8443, got %s:%s", target.host, target.port)
	}
}

func TestZitiSidecarUsesWorkloadDNSForRuntimeAuth(t *testing.T) {
	cmd := buildZitiSidecarCommand("10.43.0.30")
	expected := []string{
		"-ec",
		zitiSidecarScript,
		ZitiSidecarContainerName,
		"10.43.0.30",
	}
	if zitiSidecarEntrypoint != "/usr/bin/bash" {
		t.Fatalf("expected sidecar entrypoint to bypass image entrypoint with shell wrapper, got %q", zitiSidecarEntrypoint)
	}
	if !equalStringSlice(cmd, expected) {
		t.Fatalf("expected ziti sidecar cmd %+v, got %+v", expected, cmd)
	}
	if !strings.Contains(zitiSidecarScript, `exec "/usr/local/bin/ziti" "tunnel" "tproxy"`) {
		t.Fatalf("expected sidecar script to exec ziti tunnel directly, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `GODEBUG="netdns=go+1"`) {
		t.Fatalf("expected sidecar script to force Go DNS resolution so /etc/hosts aliases are honored, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `GODEBUG="${GODEBUG:+${GODEBUG},}`) {
		t.Fatalf("expected sidecar script not to preserve inherited DNS debug mode, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `netdns=cgo`) {
		t.Fatalf("expected sidecar script not to force cgo DNS resolution because it can ignore /etc/hosts aliases, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `runtime_controller_dns_upstream="${ZITI_DNS_UPSTREAM:-${workload_dns_upstream}}"`) {
		t.Fatalf("expected sidecar script not to use enrollment DNS for runtime controller resolution, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `runtime_controller_dns_upstream=`) {
		t.Fatalf("expected sidecar script not to use enrollment DNS for runtime controller resolution, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `getent hosts`) {
		t.Fatalf("expected sidecar script not to pre-resolve runtime controller hosts, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `getent ahostsv4 "${runtime_controller_host}"`) {
		t.Fatalf("expected sidecar script not to pre-resolve and host-pin runtime controller, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `awk -v host="${runtime_controller_host}"`) {
		t.Fatalf("expected sidecar script to remove stale runtime host aliases before auth, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `cat "${runtime_hosts_file}" "${hosts_file}.tmp" > "${hosts_file}"`) {
		t.Fatalf("expected sidecar script to prepend runtime controller host alias before auth, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `cat "${runtime_hosts_file}" >> "${hosts_file}"`) {
		t.Fatalf("expected sidecar script not to append runtime controller host alias after stale host entries, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `ziti_sidecar_runtime_host_alias=`) {
		t.Fatalf("expected sidecar script to print runtime controller host alias, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `openssl s_client`) {
		t.Fatalf("expected sidecar startup not to fail before tunnel retry handling, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `--dnsUpstream "udp://${workload_dns_upstream}:53"`) {
		t.Fatalf("expected sidecar script to forward non-intercepted names upstream rather than refuse them, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `--svcPollRate "${ZITI_SIDECAR_SERVICE_POLL_RATE}" --resolver "udp://127.0.0.1:53"`) {
		t.Fatalf("expected sidecar script to enable service polling and supported DNS resolver, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `--diverter "${ziti_diverter}"`) {
		t.Fatalf("expected sidecar script to install a pod-local OUTPUT diverter for local workload traffic, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `iptables -t nat -C OUTPUT`) || !strings.Contains(zitiSidecarScript, `iptables -t nat -I OUTPUT`) || !strings.Contains(zitiSidecarScript, `-j REDIRECT --to-ports "${target_port}"`) {
		t.Fatalf("expected sidecar diverter to redirect pod-local service traffic to stock tproxy listener, got %q", zitiSidecarScript)
	}
	for _, expected := range []string{`-o) shift ;;`, `-n) shift ;;`, `-N) shift ;;`} {
		if !strings.Contains(zitiSidecarScript, expected) {
			t.Fatalf("expected sidecar diverter to accept stock tproxy source/interface argument %q, got %q", expected, zitiSidecarScript)
		}
	}
	if strings.Contains(zitiSidecarScript, `DNAT`) {
		t.Fatalf("expected sidecar diverter not to DNAT pod-local service traffic away from the original local destination, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `route_localnet`) {
		t.Fatalf("expected sidecar script not to mutate route_localnet for pod-local REDIRECT, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `iptables -t nat -D OUTPUT`) {
		t.Fatalf("expected sidecar diverter to remove pod-local service redirects, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `--lanIf "lo"`) {
		t.Fatalf("expected sidecar script not to rely on INPUT-only lanIf rules for pod-local service intercepts, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `--dnsUpstreamMode`) {
		t.Fatalf("expected sidecar script not to use unsupported dns upstream mode flag, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `ZITI_RUNTIME_HOSTS_FILE`) {
		t.Fatalf("expected sidecar script not to use environment-selected runtime controller host files, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, "nameserver ${workload_dns_upstream}\nsearch svc.cluster.local cluster.local\noptions ndots:5") {
		t.Fatalf("expected sidecar script to use workload DNS while authenticating with the controller, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, "nameserver 127.0.0.1\nnameserver ${workload_dns_upstream}") {
		t.Fatalf("expected sidecar script not to place empty tunnel DNS before workload DNS during controller auth, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `expected workload DNS first in ${resolv_file}`) {
		t.Fatalf("expected sidecar script to fail before stock tunnel startup if workload DNS is not first, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, `printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"`) {
		t.Fatalf("expected sidecar script not to point its startup resolver only at workload DNS, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `if jq -e 'has("ztAPIs")' "${identity_file}" >/dev/null; then`) {
		t.Fatalf("expected sidecar script to reject HA/OIDC identity fields, got %q", zitiSidecarScript)
	}
	if !strings.Contains(zitiSidecarScript, `ziti_sidecar_identity_ztAPI=`) {
		t.Fatalf("expected sidecar script to print actual mounted identity ztAPI, got %q", zitiSidecarScript)
	}
	if strings.Contains(zitiSidecarScript, ZitiEnrollmentTokenEnvVar) {
		t.Fatalf("expected sidecar script not to consume %s", ZitiEnrollmentTokenEnvVar)
	}
}

const (
	testAgentInlineImage         = "agent-image"
	testAgentEnvironmentImage    = "environment-image"
	testAgentEnvironmentFlavor   = "ram-2gb"
	testAgentEnvironmentRunnerID = "runner-environment"
)

type agentEnvironmentFixture struct {
	agentID       uuid.UUID
	threadID      uuid.UUID
	environmentID uuid.UUID
	agent         *agentsv1.Agent
	agents        *testutil.FakeAgentsClient
	runners       *fakeRunnersClient
	secrets       *testutil.FakeSecretsClient
	cfg           *config.Config
}

func newAgentEnvironmentFixture() *agentEnvironmentFixture {
	agentID := uuid.New()
	environmentID := uuid.New()
	fixture := &agentEnvironmentFixture{
		agentID:       agentID,
		threadID:      uuid.New(),
		environmentID: environmentID,
		secrets:       &testutil.FakeSecretsClient{},
		cfg: &config.Config{
			AgentGatewayAddress: "gateway:50051",
			AgentLLMBaseURL:     "http://llm:8080/v1",
		},
	}
	fixture.agent = &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Name:           "assistant",
		Image:          testAgentInlineImage,
		InitImage:      "agent-init-image",
		EnvironmentId:  environmentID.String(),
	}
	fixture.agents = &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: fixture.agent}, nil
		},
		GetEnvironmentFunc: func(_ context.Context, req *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
			if req.GetId() != environmentID.String() {
				return nil, errors.New("unexpected environment id")
			}
			return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{
				Meta:           &agentsv1.EntityMeta{Id: environmentID.String()},
				OrganizationId: "org-1",
				Name:           "shared-runtime",
				Image:          testAgentEnvironmentImage,
				RunnerId:       testAgentEnvironmentRunnerID,
				Flavor:         testAgentEnvironmentFlavor,
				// Assembly refuses a workload whose environment names no runtime.
				AgentRuntimeImageId:  testRuntimeImageID,
				AgentRuntimeImageTag: testRuntimeImageTag,
			}}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}
	fixture.runners = &fakeRunnersClient{
		listFlavors: func(_ context.Context, req *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			if req.GetRunnerId() != testAgentEnvironmentRunnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{
				{RunnerId: testAgentEnvironmentRunnerID, Name: "other", Default: true},
				{RunnerId: testAgentEnvironmentRunnerID, Name: testAgentEnvironmentFlavor},
			}}, nil
		},
	}
	return fixture
}

func (f *agentEnvironmentFixture) assemble(t *testing.T) *AssembleResult {
	t.Helper()
	f.cfg.ImageProxyHost = testCatalogProxyHost
	assembler := withCatalog(NewWithRunners(f.agents, f.runners, f.secrets, f.cfg), "org-1")
	result, err := assembler.Assemble(context.Background(), f.agentID, f.threadID, f.threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	return result
}

func TestAssemblerUsesEnvironmentImageAndRunner(t *testing.T) {
	fixture := newAgentEnvironmentFixture()
	result := fixture.assemble(t)

	main := result.Request.GetMain()
	if main == nil {
		t.Fatal("expected main container")
	}
	if main.GetImage() != testAgentEnvironmentImage {
		t.Fatalf("expected environment image %q, got %q", testAgentEnvironmentImage, main.GetImage())
	}
	if result.RunnerID != testAgentEnvironmentRunnerID {
		t.Fatalf("expected environment runner %q, got %q", testAgentEnvironmentRunnerID, result.RunnerID)
	}
	// The resolved flavor name is carried out of assembly, not discarded:
	// it is written to the workload record and is what compute bills by.
	if result.Flavor != testAgentEnvironmentFlavor {
		t.Fatalf("expected environment flavor %q, got %q", testAgentEnvironmentFlavor, result.Flavor)
	}
	// The runtime the environment names is what seeds the workload: there is no
	// longer an agent-supplied init image to fall back to.
	initContainer := testutil.FindInitContainer(result.Request.GetInitContainers(), agentRuntimeInit)
	if initContainer == nil {
		t.Fatalf("expected the %s init container", agentRuntimeInit)
	}
	// Carried out of assembly so the reconciler can record it on the workload's
	// OpenZiti identity.
	if result.EnvironmentID != fixture.agent.GetEnvironmentId() {
		t.Fatalf("expected environment id %q, got %q", fixture.agent.GetEnvironmentId(), result.EnvironmentID)
	}
}
