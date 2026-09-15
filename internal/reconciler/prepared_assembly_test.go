package reconciler

import (
	"context"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func (f *preparedControllerFixture) placementDialer() *fakeRunnerDialer {
	return &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id != f.v.RunnerId {
			f.t.Fatalf("dialed %s instead of pinned runner %s", id, f.v.RunnerId)
		}
		return f.native, nil
	}}
}

func preparedAgentAssemblyFixture(t *testing.T, ziti bool) (*preparedControllerFixture, AgentInstanceTarget) {
	t.Helper()
	f := newPreparedControllerFixture(t, false)
	f.infos, f.created, f.request.Volumes = nil, nil, nil
	target := AgentInstanceTarget{AgentID: uuid.MustParse(f.v.AgentId), AgentInstanceID: uuid.MustParse(f.v.OwnerId), ThreadID: uuid.New()}
	f.registry.listRunners = func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
		return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(f.v.RunnerId)}}, nil
	}
	f.r = newTestReconciler(Config{Runners: f.registry, RunnerDialer: f.placementDialer(), Assembler: newTestAssembler(target.AgentID, ziti)})
	return f, target
}

func preparedSandboxAssemblyFixture(t *testing.T, ziti, volume bool) (*preparedControllerFixture, *sandboxWorkloadPlan) {
	t.Helper()
	f := newPreparedControllerFixture(t, true)
	f.humanOwner = uuid.NewString()
	environmentID := uuid.NewString()
	sandbox := &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: f.v.OwnerId}, OrganizationId: f.v.OrganizationId,
		EnvironmentId: environmentID, OwnerId: f.humanOwner, Name: "sandbox", Status: agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING}
	agents := &testutil.FakeAgentsClient{
		GetSandboxFunc: func(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			return &agentsv1.GetSandboxResponse{Sandbox: sandbox}, nil
		},
		GetEnvironmentFunc: func(_ context.Context, req *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
			if req.GetId() != environmentID {
				t.Fatal("wrong environment")
			}
			return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{Meta: &agentsv1.EntityMeta{Id: environmentID}, OrganizationId: f.v.OrganizationId, RunnerId: f.v.RunnerId, Flavor: "ram-2gb", Image: "sandbox-image"}}, nil
		},
		ListVolumesFunc: func(_ context.Context, req *agentsv1.ListVolumesRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			result := &agentsv1.ListVolumesResponse{}
			if volume && req.GetEnvironmentId() == environmentID {
				result.Volumes = []*agentsv1.Volume{{Meta: &agentsv1.EntityMeta{Id: f.v.VolumeId}, Name: "workspace", MountPath: "/workspace", Persistent: true, Size: "10Gi"}}
			}
			return result, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			f.runtimeUpdates = append(f.runtimeUpdates, proto.Clone(req).(*agentsv1.UpdateSandboxRuntimeStateRequest))
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		},
	}
	f.registry.listFlavors = func(_ context.Context, req *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
		if req.GetRunnerId() != f.v.RunnerId {
			t.Fatal("wrong flavor runner")
		}
		return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{{RunnerId: f.v.RunnerId, Name: "ram-2gb", Default: true, Resources: &runnersv1.ComputeResources{RequestsCpu: "500m", RequestsMemory: "1Gi"}}}}, nil
	}
	f.registry.getRunner = func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
		if req.Id != f.v.RunnerId {
			t.Fatal("wrong sandbox runner")
		}
		return &runnersv1.GetRunnerResponse{Runner: buildRunner(f.v.RunnerId)}, nil
	}
	f.registry.createVolumeChecked = func(_ context.Context, req *runnersv1.CreateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
		f.lastVolume = proto.Clone(req.Volume).(*runnersv1.CreateVolumeRequest)
		response := checkedTestCreate(req.Volume)
		f.v = proto.Clone(response.Volume).(*runnersv1.Volume)
		return response, nil
	}
	f.registry.listWorkloads = func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
		response := &runnersv1.ListWorkloadsResponse{}
		if f.w != nil {
			response.Workloads = []*runnersv1.Workload{proto.Clone(f.w).(*runnersv1.Workload)}
		}
		return response, nil
	}
	f.registry.listVolumes = func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
		return &runnersv1.ListVolumesResponse{}, nil
	}
	cfg := &config.Config{AgentGatewayAddress: "gateway:50051", AgentLLMBaseURL: "http://llm:8080/v1", SandboxWorkspaceSizeGB: "10",
		ZitiEnabled: ziti, ZitiSidecarImage: "ziti-sidecar-image", WorkloadDNSUpstream: "10.43.0.10", ZitiEnrollmentDNSUpstream: "10.43.0.10",
		ZitiEnrollmentControllerResolveHost: "ziti-controller-client.ziti.svc.cluster.local", ZitiEnrollmentControllerPort: "2496",
		ZitiRuntimeControllerResolveHost: "istio-ingressgateway.istio-gateway.svc.cluster.local", ZitiRuntimeControllerPort: "443"}
	f.r = newTestReconciler(Config{Runners: f.registry, Agents: agents, RunnerDialer: f.placementDialer(), Assembler: assembler.NewWithRunners(agents, f.registry, &testutil.FakeSecretsClient{}, cfg)})
	assembled, err := f.r.assembler.AssembleSandbox(context.Background(), sandbox)
	if err != nil {
		t.Fatal(err)
	}
	f.infos = assembled.PersistentVolumes
	return f, &sandboxWorkloadPlan{sandboxID: uuid.MustParse(f.v.OwnerId), sandbox: sandbox}
}
