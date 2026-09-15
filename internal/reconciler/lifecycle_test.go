package reconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	groupsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/groups/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	imagesv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/images/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testOrganizationID         = "11111111-1111-1111-1111-111111111111"
	testAgentID                = "22222222-2222-2222-2222-222222222222"
	testAgentIDAlt             = "33333333-3333-3333-3333-333333333333"
	testAllocatedCPUMillicores = int32(500)
	testAllocatedRAMBytes      = int64(1 << 30)
	testEnvironmentImage       = "environment-image"
)

var errNotImplemented = errors.New("not implemented")

func TestStartWorkloadCreatesIdentityAndStores(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, true)
	groups := &fakeGroupsClient{groupsByOrg: map[string][]*groupsv1.Group{testOrganizationID: {{Meta: &groupsv1.EntityMeta{Id: "group-b"}}, {Meta: &groupsv1.EntityMeta{Id: "group-a"}}, {Meta: &groupsv1.EntityMeta{Id: "group-a"}}}}}
	f.r.groups = groups
	var identityReq *zitimgmtv1.CreateAgentIdentityRequest
	f.r.zitiMgmt = &fakeZitiMgmtClient{createAgentIdentity: func(_ context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
		identityReq = req
		assertStringSet(t, req.GetAdditionalRoleAttributes(), []string{groupRoleAttribute("group-a"), groupRoleAttribute("group-b")})
		if req.GetAgentId() != target.AgentInstanceID.String() || req.GetAgentClassId() != target.AgentID.String() {
			t.Fatal("wrong identity ownership")
		}
		return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: "ziti-identity", EnrollmentJwt: "enrollment-jwt"}, nil
	}}
	f.r.startWorkload(context.Background(), target)
	if f.activations != 1 || f.lastRequest == nil || identityReq == nil {
		t.Fatal("prepared startup did not complete")
	}
	m, request := f.lastMetadata, f.lastRequest.Workload
	if target.ThreadID == target.AgentInstanceID || f.w.Preparation.Resources.Workload.IdentityLabels[assembler.LabelThreadID] != target.ThreadID.String() {
		t.Fatal("native inbox thread was confused with the registry instance alias")
	}
	if m.Id != identityReq.WorkloadId || request.WorkloadId != m.Id || m.GetZitiIdentityId() != "ziti-identity" ||
		m.RunnerId != f.v.RunnerId || m.AgentId != target.AgentID.String() || m.ThreadId != target.AgentInstanceID.String() ||
		m.OrganizationId != testOrganizationID || m.OwnerId != target.AgentInstanceID.String() || m.OwnerKind != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE ||
		m.GetAgentClassId() != target.AgentID.String() || m.GetAgentInstanceId() != target.AgentInstanceID.String() ||
		m.AllocatedCpuMillicores != testAllocatedCPUMillicores || m.AllocatedRamBytes != testAllocatedRAMBytes {
		t.Fatal("prepared reservation lost identity or resource metadata")
	}
	if request.AdditionalProperties[assembler.LabelKeyPrefix+assembler.LabelWorkloadKey] != m.Id || envMap(request.Main.Env)["WORKLOAD_ID"] != m.Id {
		t.Fatal("workload identity not passed to agent")
	}
	enroll := testutil.FindInitContainer(request.InitContainers, assembler.ZitiEnrollContainerName)
	if enroll == nil {
		t.Fatal("missing enrollment container")
	}
	env := envMap(enroll.Env)
	if env[assembler.ZitiEnrollmentTokenEnvVar] != "enrollment-jwt" || env[assembler.ZitiIdentityBasenameEnvVar] != assembler.ZitiIdentityBasename ||
		env[assembler.ZitiEnrollmentControllerResolveHostEnvVar] != "ziti-controller-client.ziti.svc.cluster.local" || env[assembler.ZitiEnrollmentControllerPortEnvVar] != "2496" {
		t.Fatal("invalid enrollment setup")
	}
	sidecar := testutil.FindInitContainer(request.InitContainers, assembler.ZitiSidecarContainerName)
	if sidecar == nil {
		t.Fatal("missing ziti sidecar")
	}
	if _, ok := envMap(sidecar.Env)[assembler.ZitiEnrollmentTokenEnvVar]; ok {
		t.Fatal("enrollment token exposed to runtime sidecar")
	}
	if len(groups.requests) != 1 || groups.requests[0].GetMemberId() != target.AgentID.String() {
		t.Fatal("wrong group lookup")
	}
	assertStringSet(t, groups.identityIDs, []string{testPlatformIdentityID.String()})
	if f.w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || f.w.GetInstanceId() != m.Id || f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE {
		t.Fatal("activation did not preserve bound, not-yet-ready state")
	}
}

func TestStartWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, false)
	f.r.startWorkload(context.Background(), target)
	if f.activations != 1 || f.lastMetadata.ZitiIdentityId != "" {
		t.Fatal("unexpected identity or missing activation")
	}
	request := f.lastRequest.Workload
	if request.Main == nil || request.WorkloadId != f.w.Meta.Id || envMap(request.Main.Env)["WORKLOAD_ID"] != f.w.Meta.Id ||
		request.AdditionalProperties[assembler.LabelKeyPrefix+assembler.LabelWorkloadKey] != f.w.Meta.Id {
		t.Fatal("missing native workload identity")
	}
	if enroll := testutil.FindInitContainer(request.InitContainers, assembler.ZitiEnrollContainerName); enroll != nil {
		if _, ok := envMap(enroll.Env)[assembler.ZitiEnrollmentTokenEnvVar]; ok {
			t.Fatal("unexpected enrollment token")
		}
	}
}

func TestStartWorkloadPinsRunnerFromVolumes(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, false)
	f.v.RunnerId = "runner-pinned"
	reads := 0
	f.registry.listVolumesByAgentInstance = func(_ context.Context, req *runnersv1.ListVolumesByAgentInstanceRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error) {
		reads++
		if req.AgentInstanceId != target.AgentInstanceID.String() {
			t.Fatal("wrong owner pin query")
		}
		return &runnersv1.ListVolumesByAgentInstanceResponse{Volumes: []*runnersv1.Volume{{Meta: &runnersv1.EntityMeta{Id: uuid.NewString()}, RunnerId: f.v.RunnerId, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, AgentInstanceId: stringPtr(target.AgentInstanceID.String())}}}, nil
	}
	f.registry.getRunner = func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
		if req.Id != f.v.RunnerId {
			t.Fatal("wrong pinned runner")
		}
		return &runnersv1.GetRunnerResponse{Runner: buildRunner(f.v.RunnerId)}, nil
	}
	f.registry.listRunners = func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
		t.Fatal("pin ignored")
		return nil, errNotImplemented
	}
	f.r.startWorkload(context.Background(), target)
	if reads != 1 || f.activations != 1 || f.lastMetadata.RunnerId != f.v.RunnerId {
		t.Fatal("pinned prepared startup failed")
	}
}

func TestStartWorkloadDegradesWhenPinnedRunnerNotEnrolled(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	testAssembler := newTestAssembler(agentID, false)

	var calls []string
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			calls = append(calls, "degrade")
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonRunnerDeprovisioned {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		listVolumesByThread: func(_ context.Context, req *runnersv1.ListVolumesByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
			calls = append(calls, "list-volumes")
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			return &runnersv1.ListVolumesByThreadResponse{Volumes: []*runnersv1.Volume{
				{
					Meta:            &runnersv1.EntityMeta{Id: volumeKey},
					RunnerId:        runnerID,
					Status:          runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
					AgentInstanceId: stringPtr(threadID.String()),
					VolumeId:        "volume-id",
				},
			}}, nil
		},
		getRunner: func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
			calls = append(calls, "get-runner")
			if req.GetId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.GetRunnerResponse{Runner: &runnersv1.Runner{
				Meta:   &runnersv1.EntityMeta{Id: runnerID},
				Status: runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE,
			}}, nil
		},
		createWorkload: func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
			return nil, errors.New("unexpected create workload")
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
			return nil, errors.New("unexpected dial")
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Assembler:    testAssembler,
	})
	reconciler.startWorkload(ctx, AgentInstanceTarget{AgentID: agentID, AgentInstanceID: threadID})

	if !reflect.DeepEqual(calls, []string{"list-volumes", "get-runner"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStartWorkloadPlacesEnvironmentAgentOnEnvironmentRunner(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, false)
	f.v.RunnerId = "runner-environment"
	f.r.assembler = newTestEnvironmentAssembler(target.AgentID, uuid.New(), f.v.RunnerId)
	reads := 0
	f.registry.listVolumesByAgentInstance = func(_ context.Context, req *runnersv1.ListVolumesByAgentInstanceRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error) {
		reads++
		if req.AgentInstanceId != target.AgentInstanceID.String() {
			t.Fatal("wrong owner pin query")
		}
		return &runnersv1.ListVolumesByAgentInstanceResponse{}, nil
	}
	f.registry.getRunner = func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
		if req.Id != f.v.RunnerId {
			t.Fatal("wrong environment runner")
		}
		return &runnersv1.GetRunnerResponse{Runner: buildRunner(f.v.RunnerId)}, nil
	}
	f.registry.listRunners = func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
		t.Fatal("environment placement ignored")
		return nil, errNotImplemented
	}
	target.ThreadID = target.AgentInstanceID
	f.r.startWorkload(context.Background(), target)
	if reads != 1 || f.activations != 1 || f.lastMetadata.RunnerId != f.v.RunnerId || f.lastRequest.Workload.Main.Image != testEnvironmentImage {
		t.Fatal("environment prepared startup failed")
	}
}

func TestStartWorkloadKeepsPinnedRunnerWhenEnvironmentNamesAnother(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, false)
	f.v.RunnerId = "runner-pinned"
	f.r.assembler = newTestEnvironmentAssembler(target.AgentID, uuid.New(), "runner-environment")
	f.registry.listVolumesByAgentInstance = func(_ context.Context, req *runnersv1.ListVolumesByAgentInstanceRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error) {
		if req.AgentInstanceId != target.AgentInstanceID.String() {
			t.Fatal("wrong owner pin query")
		}
		return &runnersv1.ListVolumesByAgentInstanceResponse{Volumes: []*runnersv1.Volume{{Meta: &runnersv1.EntityMeta{Id: uuid.NewString()}, RunnerId: f.v.RunnerId, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: target.AgentInstanceID.String()}}}, nil
	}
	f.registry.getRunner = func(_ context.Context, req *runnersv1.GetRunnerRequest, _ ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
		if req.Id != f.v.RunnerId {
			t.Fatal("environment overrode workspace pin")
		}
		return &runnersv1.GetRunnerResponse{Runner: buildRunner(f.v.RunnerId)}, nil
	}
	f.registry.listRunners = func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
		t.Fatal("pin ignored")
		return nil, errNotImplemented
	}
	f.r.agents = &testutil.FakeAgentsClient{PauseInstanceFunc: func(context.Context, *agentsv1.PauseInstanceRequest, ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error) {
		t.Fatal("valid pinned placement paused")
		return nil, errNotImplemented
	}}
	target.ThreadID = target.AgentInstanceID
	f.r.startWorkload(context.Background(), target)
	if f.activations != 1 || f.lastMetadata.RunnerId != f.v.RunnerId {
		t.Fatal("pinned prepared startup failed")
	}
}

func TestStartWorkloadRetainsIdentityOnUnknownRunnerOutcome(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, true)
	f.r.zitiMgmt = &fakeZitiMgmtClient{
		createAgentIdentity: func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: "ziti-identity", EnrollmentJwt: "jwt"}, nil
		},
		deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			t.Fatal("uncertain execution lost identity")
			return nil, errNotImplemented
		},
	}
	calls := 0
	f.native.prepareWorkload = func(context.Context, *runnerv1.PrepareWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
		calls++
		return nil, errors.New("runner error")
	}
	f.r.startWorkload(context.Background(), target)
	if calls != 1 || f.w == nil || f.w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || f.w.RemovalConfirmedAt != nil ||
		f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || f.activations != 0 || f.removals != 0 {
		t.Fatal("unknown prepare was replayed or released")
	}
}

func TestStartWorkloadDoesNotStopUnverifiedWorkloadID(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, true)
	f.r.zitiMgmt = &fakeZitiMgmtClient{
		createAgentIdentity: func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: "ziti-identity", EnrollmentJwt: "jwt"}, nil
		},
		deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			t.Fatal("unverified binding lost identity")
			return nil, errNotImplemented
		},
	}
	prepare := f.native.prepareWorkload
	f.native.prepareWorkload = func(ctx context.Context, req *runnerv1.PrepareWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
		response, err := prepare(ctx, req, opts...)
		response.Binding.WorkloadId, response.Workload.Id = uuid.NewString(), uuid.NewString()
		return response, err
	}
	f.r.startWorkload(context.Background(), target)
	if f.prepares != 1 || f.activations != 0 || f.removals != 0 || f.w.RemovalConfirmedAt != nil || f.w.GetInstanceId() != "" || f.w.Preparation.Binding != nil {
		t.Fatal("unverified native identity was adopted or stopped")
	}
}

func TestStartWorkloadRetainsResourcesOnUncertainReservation(t *testing.T) {
	f, target := preparedAgentAssemblyFixture(t, true)
	f.r.zitiMgmt = &fakeZitiMgmtClient{
		createAgentIdentity: func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			return &zitimgmtv1.CreateAgentIdentityResponse{ZitiIdentityId: "ziti-identity", EnrollmentJwt: "jwt"}, nil
		},
		deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			t.Fatal("uncertain reservation compensated")
			return nil, errNotImplemented
		},
	}
	create := f.registry.createPreparedWorkload
	calls := 0
	f.registry.createPreparedWorkload = func(ctx context.Context, req *runnersv1.CreatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error) {
		calls++
		if _, err := create(ctx, req, opts...); err != nil {
			t.Fatal(err)
		}
		return nil, errors.New("lost registry reply")
	}
	f.r.startWorkload(context.Background(), target)
	if calls != 1 || f.prepares != 0 || f.activations != 0 || f.w.RemovalConfirmedAt != nil || f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED {
		t.Fatal("uncertain reservation authorized execution or cleanup")
	}
}

func TestStopWorkloadDeletesIdentityAfterStop(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	zitiID := "ziti-identity"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var calls []string
	var updateStatuses []runnersv1.WorkloadStatus
	runner := &fakeRunnerClient{
		inspectWorkload: absentRunnerWorkload,
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			calls = append(calls, "delete")
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			updateStatuses = append(updateStatuses, req.GetStatus())
			if req.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED && req.GetRemovalConfirmedAt() == nil {
				return nil, errors.New("missing removal_confirmed_at")
			}
			return acknowledgedWorkloadUpdate(req), nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), AgentClassId: stringPtr(agentID.String()), AgentInstanceId: stringPtr(agentID.String()), ZitiIdentityId: zitiID, InstanceId: stringPtr(instanceID)})

	if !reflect.DeepEqual(calls, []string{"dial", "update-workload", "stop", "update-workload", "delete"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if !reflect.DeepEqual(updateStatuses, []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED}) {
		t.Fatalf("unexpected update statuses: %v", updateStatuses)
	}
}

func TestStopWorkloadRetainsWorkloadOnNoTerminators(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	instanceID := "workload-" + uuid.New().String()

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return nil, errors.New("service runner-1 has no terminators")
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{
		Meta:            &runnersv1.EntityMeta{Id: "workload-1"},
		RunnerId:        runnerID,
		AgentId:         agentID.String(),
		AgentInstanceId: stringPtr(agentID.String()),
		InstanceId:      stringPtr(instanceID),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
	})

	if updateReq != nil {
		t.Fatal("an unreachable runner is not a missing workload")
	}
}

func TestStopWorkloadRetainsStoppingOnNoTerminatorsStopError(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var updateStatuses []runnersv1.WorkloadStatus
	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateStatuses = append(updateStatuses, req.GetStatus())
			if req.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED && req.GetRemovalConfirmedAt() == nil {
				return nil, errors.New("missing removal_confirmed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	stopCalled := false
	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
			return nil, errors.New("service runner-1 has no terminators")
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{
		Meta:            &runnersv1.EntityMeta{Id: "workload-1"},
		RunnerId:        runnerID,
		AgentId:         agentID.String(),
		AgentInstanceId: stringPtr(agentID.String()),
		InstanceId:      stringPtr(instanceID),
		Status:          runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
	})

	if !stopCalled {
		t.Fatal("expected stop workload")
	}
	if !reflect.DeepEqual(updateStatuses, []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING}) {
		t.Fatalf("unexpected update statuses: %v", updateStatuses)
	}
}

func TestStopWorkloadRequestsRunnerWhenStartAcknowledgementMissing(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"

	updateCalled := false
	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateCalled = true
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	dialCalled := false
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, _ string) (runnerv1.RunnerServiceClient, error) {
			dialCalled = true
			return nil, errors.New("unexpected dial")
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), AgentInstanceId: stringPtr(agentID.String())})

	if !dialCalled || updateCalled {
		t.Fatal("must contact the runner, not assume an unacknowledged start never happened")
	}
}

func TestStopWorkloadSkipsIdentityWhenNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"
	instanceID := "runner-workload-1"

	deleteCalled := false
	runner := &fakeRunnerClient{
		inspectWorkload: absentRunnerWorkload,
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, _ *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleteCalled = true
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, _ *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), AgentClassId: stringPtr(agentID.String()), AgentInstanceId: stringPtr(agentID.String()), InstanceId: stringPtr(instanceID)})

	if deleteCalled {
		t.Fatal("expected no delete identity call")
	}
}

func TestStopWorkloadSkipsIdentityWhenZitiMgmtNil(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)
	zitiID := "ziti-identity"
	runnerID := "runner-1"
	instanceID := "runner-workload-1"

	var calls []string
	runner := &fakeRunnerClient{
		inspectWorkload: absentRunnerWorkload,
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		updateWorkload: func(_ context.Context, _ *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			calls = append(calls, "update-workload")
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	reconciler.stopWorkload(ctx, &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, AgentId: agentID.String(), AgentClassId: stringPtr(agentID.String()), AgentInstanceId: stringPtr(agentID.String()), ZitiIdentityId: zitiID, InstanceId: stringPtr(instanceID)})

	if !reflect.DeepEqual(calls, []string{"dial", "update-workload", "stop", "update-workload"}) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestStopRunnerWorkloadIgnoresNotFoundForUUID(t *testing.T) {
	ctx := context.Background()
	instanceID := uuid.New().String()
	calls := []string{}

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, req.GetWorkloadId())
			if req.GetTimeoutSec() != 30 {
				return nil, errors.New("unexpected timeout")
			}
			return nil, status.Error(codes.NotFound, "not found")
		},
	}

	reconciler := newTestReconciler(Config{})
	if err := reconciler.stopRunnerWorkload(ctx, runner, instanceID); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	expected := []string{instanceID, "workload-" + instanceID}
	if !reflect.DeepEqual(calls, expected) {
		t.Fatalf("expected calls %v, got %v", expected, calls)
	}
}

func TestStopRunnerWorkloadRetriesWithPrefixedID(t *testing.T) {
	ctx := context.Background()
	instanceID := uuid.New().String()
	calls := []string{}

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, req.GetWorkloadId())
			if req.GetTimeoutSec() != 30 {
				return nil, errors.New("unexpected timeout")
			}
			switch req.GetWorkloadId() {
			case instanceID:
				return nil, status.Error(codes.NotFound, "not found")
			case "workload-" + instanceID:
				return &runnerv1.StopWorkloadResponse{}, nil
			default:
				return nil, errors.New("unexpected workload id")
			}
		},
	}

	reconciler := newTestReconciler(Config{})
	if err := reconciler.stopRunnerWorkload(ctx, runner, instanceID); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	expected := []string{instanceID, "workload-" + instanceID}
	if !reflect.DeepEqual(calls, expected) {
		t.Fatalf("expected calls %v, got %v", expected, calls)
	}
}

func TestStopRunnerWorkloadReturnsNotFoundForInvalidID(t *testing.T) {
	ctx := context.Background()
	instanceID := "workload-" + uuid.New().String()
	called := false

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			called = true
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, status.Error(codes.NotFound, "not found")
		},
	}

	reconciler := newTestReconciler(Config{})
	err := reconciler.stopRunnerWorkload(ctx, runner, instanceID)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found error, got %v", err)
	}
	if !called {
		t.Fatal("expected stop workload call")
	}
}

func TestStopRunnerWorkloadReturnsErrorOnFailure(t *testing.T) {
	ctx := context.Background()
	instanceID := "runner-workload-1"

	runner := &fakeRunnerClient{
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != instanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, status.Error(codes.Internal, "stop failed")
		},
	}

	reconciler := newTestReconciler(Config{})
	err := reconciler.stopRunnerWorkload(ctx, runner, instanceID)
	if err == nil {
		t.Fatal("expected error")
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected internal error, got %v", err)
	}
}

func TestReconcileOrphanIdentitiesDeletesOrphans(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	activeID := "active-id"
	orphanID := "orphan-id"

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if len(req.GetFilter().GetStatusIn()) == 0 {
				return nil, errors.New("missing statuses")
			}
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, OrganizationId: testOrganizationID, ZitiIdentityId: activeID},
			}}, nil
		},
	}

	deleteCalls := []string{}
	zitiMgmt := &fakeZitiMgmtClient{
		listManagedIdentities: func(_ context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, _ ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
			// The sweep covers agent and sandbox identities in the same pass;
			// this case exercises the agent half and has no sandbox identities.
			switch req.GetIdentityType() {
			case identityv1.IdentityType_IDENTITY_TYPE_AGENT:
				return &zitimgmtv1.ListManagedIdentitiesResponse{
					Identities: []*zitimgmtv1.ManagedIdentity{
						{ZitiIdentityId: activeID},
						{ZitiIdentityId: orphanID},
					},
				}, nil
			case identityv1.IdentityType_IDENTITY_TYPE_SANDBOX:
				return &zitimgmtv1.ListManagedIdentitiesResponse{}, nil
			default:
				return nil, errors.New("unexpected identity type")
			}
		},
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleteCalls = append(deleteCalls, req.GetZitiIdentityId())
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    testAssembler,
	})
	if err := reconciler.reconcileOrphanIdentities(ctx); err != nil {
		t.Fatalf("reconcile orphan identities: %v", err)
	}

	if !reflect.DeepEqual(deleteCalls, []string{orphanID}) {
		t.Fatalf("unexpected delete calls: %v", deleteCalls)
	}
}

func TestFetchActualReturnsTrackedWorkloads(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, true)
	runnerID := "runner-1"

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, RunnerId: runnerID, OrganizationId: testOrganizationID},
			}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runners:   runners,
		Assembler: testAssembler,
	})
	actual, err := reconciler.fetchActual(ctx)
	if err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if len(actual) != 1 {
		t.Fatalf("expected workload, got %d", len(actual))
	}
}

func TestFetchActualSkipsMissingRunnerID(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	testAssembler := newTestAssembler(agentID, false)

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, OrganizationId: testOrganizationID},
			}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		Runners:   runners,
		Assembler: testAssembler,
	})
	actual, err := reconciler.fetchActual(ctx)
	if err != nil {
		t.Fatalf("fetch actual: %v", err)
	}
	if len(actual) != 0 {
		t.Fatalf("expected no workloads, got %d", len(actual))
	}
}

func TestGroupMembershipEventPatchesLiveWorkloads(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	groupID := "group-a"
	zitiID := "ziti-workload-1"
	var listRequest *runnersv1.ListWorkloadsRequest
	var patchRequest *zitimgmtv1.PatchIdentityRoleAttributesRequest

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			listRequest = req
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{{
				Meta:           &runnersv1.EntityMeta{Id: "workload-1"},
				AgentId:        agentID.String(),
				OrganizationId: testOrganizationID,
				ZitiIdentityId: zitiID,
			}}}, nil
		},
	}
	zitiMgmt := &fakeZitiMgmtClient{
		patchIdentityRoleAttributes: func(_ context.Context, req *zitimgmtv1.PatchIdentityRoleAttributesRequest, _ ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error) {
			patchRequest = req
			return &zitimgmtv1.PatchIdentityRoleAttributesResponse{}, nil
		},
	}
	fakeGroups := &fakeGroupsClient{groupsByOrg: map[string][]*groupsv1.Group{testOrganizationID: {{Meta: &groupsv1.EntityMeta{Id: groupID}}}}}
	reconciler := newTestReconciler(Config{Runners: runners, ZitiMgmt: zitiMgmt, Groups: fakeGroups})
	payload := mustMarshal(t, &groupsv1.GroupMembershipAddedEvent{
		GroupId:    groupID,
		MemberType: groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT,
		MemberId:   agentID.String(),
	})

	if err := reconciler.HandleGroupMembershipEvent(ctx, groupMembershipAddedSubject, payload); err != nil {
		t.Fatalf("HandleGroupMembershipEvent: %v", err)
	}
	if listRequest == nil {
		t.Fatal("expected live workload lookup")
	}
	assertStringSet(t, listRequest.GetFilter().GetAgentIdIn(), []string{agentID.String()})
	if patchRequest == nil {
		t.Fatal("expected ziti patch")
	}
	if patchRequest.GetZitiIdentityId() != zitiID {
		t.Fatalf("expected ziti identity %s, got %s", zitiID, patchRequest.GetZitiIdentityId())
	}
	assertStringSet(t, patchRequest.GetAdd(), []string{groupRoleAttribute(groupID)})
	if len(patchRequest.GetRemove()) != 0 {
		t.Fatalf("expected no removals, got %v", patchRequest.GetRemove())
	}
	// Groups is called as the platform, never as the agent being read.
	assertStringSet(t, fakeGroups.identityIDs, []string{testPlatformIdentityID.String()})
}

func TestGroupMembershipEventsAreDuplicateAndOutOfOrderSafe(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	groupID := "group-a"
	zitiID := "ziti-workload-1"
	patchRequests := []*zitimgmtv1.PatchIdentityRoleAttributesRequest{}
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{{
				Meta:           &runnersv1.EntityMeta{Id: "workload-1"},
				AgentId:        agentID.String(),
				OrganizationId: testOrganizationID,
				ZitiIdentityId: zitiID,
			}}}, nil
		},
	}
	zitiMgmt := &fakeZitiMgmtClient{
		patchIdentityRoleAttributes: func(_ context.Context, req *zitimgmtv1.PatchIdentityRoleAttributesRequest, _ ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error) {
			patchRequests = append(patchRequests, req)
			return &zitimgmtv1.PatchIdentityRoleAttributesResponse{}, nil
		},
	}
	fakeGroups := &fakeGroupsClient{groupsByOrg: map[string][]*groupsv1.Group{testOrganizationID: {}}}
	reconciler := newTestReconciler(Config{Runners: runners, ZitiMgmt: zitiMgmt, Groups: fakeGroups})
	removed := mustMarshal(t, &groupsv1.GroupMembershipRemovedEvent{
		GroupId:    groupID,
		MemberType: groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT,
		MemberId:   agentID.String(),
	})
	added := mustMarshal(t, &groupsv1.GroupMembershipAddedEvent{
		GroupId:    groupID,
		MemberType: groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT,
		MemberId:   agentID.String(),
	})

	if err := reconciler.HandleGroupMembershipEvent(ctx, groupMembershipRemovedSubject, removed); err != nil {
		t.Fatalf("remove event: %v", err)
	}
	if err := reconciler.HandleGroupMembershipEvent(ctx, groupMembershipRemovedSubject, removed); err != nil {
		t.Fatalf("duplicate remove event: %v", err)
	}
	if err := reconciler.HandleGroupMembershipEvent(ctx, groupMembershipAddedSubject, added); err != nil {
		t.Fatalf("out-of-order add event: %v", err)
	}
	if len(patchRequests) != 3 {
		t.Fatalf("expected three patch requests, got %d", len(patchRequests))
	}
	for _, request := range patchRequests {
		if len(request.GetAdd()) != 0 {
			t.Fatalf("expected no adds while source-of-truth is empty, got %v", request.GetAdd())
		}
		assertStringSet(t, request.GetRemove(), []string{groupRoleAttribute(groupID)})
	}

	fakeGroups.groupsByOrg[testOrganizationID] = []*groupsv1.Group{{Meta: &groupsv1.EntityMeta{Id: groupID}}}
	if err := reconciler.HandleGroupMembershipEvent(ctx, groupMembershipAddedSubject, added); err != nil {
		t.Fatalf("add event: %v", err)
	}
	last := patchRequests[len(patchRequests)-1]
	assertStringSet(t, last.GetAdd(), []string{groupRoleAttribute(groupID)})
	if len(last.GetRemove()) != 0 {
		t.Fatalf("expected no removal for desired group, got %v", last.GetRemove())
	}
}

func TestReconcileAllAgentGroupRolesPatchesMissingDesiredAttrs(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	groupID := "group-a"
	zitiID := "ziti-workload-1"
	var patchRequest *zitimgmtv1.PatchIdentityRoleAttributesRequest
	agents := &testutil.FakeAgentsClient{ListAgentsFunc: func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
		return &agentsv1.ListAgentsResponse{Agents: []*agentsv1.Agent{{
			Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
			OrganizationId: testOrganizationID,
		}}}, nil
	}}
	runners := &fakeRunnersClient{listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
		return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{{
			Meta:           &runnersv1.EntityMeta{Id: "workload-1"},
			RunnerId:       "runner-1",
			AgentId:        agentID.String(),
			OrganizationId: testOrganizationID,
			ZitiIdentityId: zitiID,
		}}}, nil
	}}
	zitiMgmt := &fakeZitiMgmtClient{patchIdentityRoleAttributes: func(_ context.Context, req *zitimgmtv1.PatchIdentityRoleAttributesRequest, _ ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error) {
		patchRequest = req
		return &zitimgmtv1.PatchIdentityRoleAttributesResponse{}, nil
	}}
	fakeGroups := &fakeGroupsClient{groupsByOrg: map[string][]*groupsv1.Group{testOrganizationID: {{Meta: &groupsv1.EntityMeta{Id: groupID}}}}}
	reconciler := newTestReconciler(Config{Agents: agents, Runners: runners, ZitiMgmt: zitiMgmt, Groups: fakeGroups})

	if err := reconciler.ReconcileAllAgentGroupRoles(ctx); err != nil {
		t.Fatalf("ReconcileAllAgentGroupRoles: %v", err)
	}
	if patchRequest == nil {
		t.Fatal("expected ziti patch")
	}
	assertStringSet(t, patchRequest.GetAdd(), []string{groupRoleAttribute(groupID)})
	if len(patchRequest.GetRemove()) != 0 {
		t.Fatalf("expected no removals, got %v", patchRequest.GetRemove())
	}
	// Groups is called as the platform, never as the agent being read.
	assertStringSet(t, fakeGroups.identityIDs, []string{testPlatformIdentityID.String()})
}

func TestGroupMembershipConsumerLoopRetriesWithoutBlocking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subscription := &fakeGroupMembershipSubscription{unsubscribed: make(chan struct{})}
	subscribed := make(chan struct{})
	attempts := 0
	reconciler := newTestReconciler(Config{})
	reconciler.StartGroupMembershipConsumerLoopWithSubscriber(ctx, func(context.Context) (groupMembershipSubscription, error) {
		attempts++
		if attempts < 2 {
			return nil, errors.New("nats unavailable")
		}
		close(subscribed)
		return subscription, nil
	})

	select {
	case <-subscribed:
	case <-ctx.Done():
		t.Fatal("expected retry without blocking")
	}
	select {
	case <-subscription.unsubscribed:
		t.Fatal("did not expect unsubscribe before cancellation")
	default:
	}
	cancel()
	select {
	case <-subscription.unsubscribed:
	case <-time.After(time.Second):
		t.Fatal("expected unsubscribe after cancellation")
	}
}

// testPlatformIdentityID stands for the identity this process runs as. Set on
// every test reconciler because the calls that name a caller name this one.
var testPlatformIdentityID = uuid.MustParse("a3c1e9d2-7f4b-5e1a-9c3d-2b8f6a4e7d10")

func newTestReconciler(cfg Config) *Reconciler {
	if cfg.PlatformIdentityID == uuid.Nil {
		cfg.PlatformIdentityID = testPlatformIdentityID
	}
	if cfg.Poll == 0 {
		cfg.Poll = time.Second
	}
	if cfg.Idle == 0 {
		cfg.Idle = time.Minute
	}
	if cfg.StopSec == 0 {
		cfg.StopSec = 30
	}
	if cfg.MeteringSampleInterval == 0 {
		cfg.MeteringSampleInterval = time.Minute
	}
	if cfg.RunnerDialer == nil {
		cfg.RunnerDialer = &fakeRunnerDialer{}
	}
	if cfg.Metering == nil {
		cfg.Metering = &fakeMeteringClient{}
	}
	if cfg.Agents == nil {
		cfg.Agents = defaultAgentsClient()
	} else if agentsClient, ok := cfg.Agents.(*testutil.FakeAgentsClient); ok && agentsClient.ListAgentsFunc == nil {
		agentsClient.ListAgentsFunc = defaultListAgentsFunc()
	}
	return New(cfg)
}

func defaultAgentsClient() *testutil.FakeAgentsClient {
	return &testutil.FakeAgentsClient{ListAgentsFunc: defaultListAgentsFunc()}
}

func defaultListAgentsFunc() func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	return func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
		return &agentsv1.ListAgentsResponse{Agents: []*agentsv1.Agent{
			{
				Meta:           &agentsv1.EntityMeta{Id: testAgentID},
				OrganizationId: testOrganizationID,
			},
		}}, nil
	}
}

func newTestAssembler(agentID uuid.UUID, zitiEnabled bool) *assembler.Assembler {
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{
				Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
				OrganizationId: testOrganizationID,
				Image:          "agent-image",
				EnvironmentId:  testAssemblerEnvironmentID,
				Resources: &agentsv1.ComputeResources{
					RequestsCpu:    "500m",
					RequestsMemory: "1Gi",
				},
			}}, nil
		},
		ListSkillsFunc: func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress:                 "gateway:50051",
		AgentLLMBaseURL:                     "http://llm:8080/v1",
		ZitiEnabled:                         zitiEnabled,
		ZitiSidecarImage:                    "ziti-sidecar-image",
		WorkloadDNSUpstream:                 "10.43.0.10",
		ZitiEnrollmentDNSUpstream:           "10.43.0.10",
		ZitiEnrollmentControllerResolveHost: "ziti-controller-client.ziti.svc.cluster.local",
		ZitiEnrollmentControllerPort:        "2496",
		ZitiRuntimeControllerResolveHost:    "istio-ingressgateway.istio-gateway.svc.cluster.local",
		ZitiRuntimeControllerPort:           "443",
	}
	cfg.ImageProxyHost = testAssemblerProxyHost
	agentsClient.GetEnvironmentFunc = func(_ context.Context, _ *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
		return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{
			Meta:                 &agentsv1.EntityMeta{Id: testAssemblerEnvironmentID},
			OrganizationId:       testOrganizationID,
			Image:                "agent-image",
			RunnerId:             testAssemblerRunnerID,
			AgentRuntimeImageId:  testAssemblerRuntimeImageID,
			AgentRuntimeImageTag: "1.0.0",
		}}, nil
	}
	runners := &fakeRunnersClient{
		listFlavors: func(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{{
				Name:    "default",
				Default: true,
				Resources: &runnersv1.ComputeResources{
					RequestsCpu:    "500m",
					RequestsMemory: "1Gi",
				},
			}}}, nil
		},
	}
	return assembler.NewWithRunners(agentsClient, runners, &testutil.FakeSecretsClient{}, cfg).
		WithCatalog(&fakeImagesClient{}, &fakeOrganizationsClient{}, nil)
}

const (
	testAssemblerEnvironmentID  = "33333333-3333-3333-3333-333333333333"
	testAssemblerRunnerID       = "44444444-4444-4444-4444-444444444444"
	testAssemblerRuntimeImageID = "55555555-5555-5555-5555-555555555555"
	testAssemblerProxyHost      = "registry.agyn.test"
)

// The catalog the environment's runtime image resolves through.
type fakeImagesClient struct{}

func (f *fakeImagesClient) GetImage(_ context.Context, req *imagesv1.GetImageRequest, _ ...grpc.CallOption) (*imagesv1.GetImageResponse, error) {
	return &imagesv1.GetImageResponse{Image: &imagesv1.Image{
		Meta:           &imagesv1.EntityMeta{Id: req.GetId()},
		Name:           "agent-runtime",
		OrganizationId: testOrganizationID,
	}}, nil
}

type fakeOrganizationsClient struct{}

func (f *fakeOrganizationsClient) GetOrganization(_ context.Context, req *organizationsv1.GetOrganizationRequest, _ ...grpc.CallOption) (*organizationsv1.GetOrganizationResponse, error) {
	return &organizationsv1.GetOrganizationResponse{Organization: &organizationsv1.Organization{
		Id:   req.GetId(),
		Slug: "org-one",
	}}, nil
}

// newTestEnvironmentAssembler builds an assembler for an agent that runs in an
// environment: its image and runner come from the environment rather than from
// the agent's own deprecated inline image and label-based placement.
func newTestEnvironmentAssembler(agentID, environmentID uuid.UUID, runnerID string) *assembler.Assembler {
	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{
				Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
				OrganizationId: testOrganizationID,
				Image:          "agent-image",
				InitImage:      "agent-init-image",
				EnvironmentId:  environmentID.String(),
				Resources: &agentsv1.ComputeResources{
					RequestsCpu:    "500m",
					RequestsMemory: "1Gi",
				},
			}}, nil
		},
		GetEnvironmentFunc: func(_ context.Context, req *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
			if req.GetId() != environmentID.String() {
				return nil, errors.New("unexpected environment id")
			}
			return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{
				Meta:                 &agentsv1.EntityMeta{Id: environmentID.String()},
				AgentRuntimeImageId:  testAssemblerRuntimeImageID,
				AgentRuntimeImageTag: "1.0.0",
				OrganizationId:       testOrganizationID,
				Name:                 "shared-runtime",
				Image:                testEnvironmentImage,
				RunnerId:             runnerID,
			}}, nil
		},
		ListSkillsFunc: func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumesFunc: func(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
			return &agentsv1.ListVolumesResponse{}, nil
		},
		ListMcpsFunc: func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
	}

	// The environment names no flavor, so it takes the runner's default.
	runnersClient := &fakeRunnersClient{
		listFlavors: func(_ context.Context, req *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			if req.GetRunnerId() != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{
				{RunnerId: runnerID, Name: "ram-1gb", Default: true},
			}}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	cfg.ImageProxyHost = testAssemblerProxyHost
	return assembler.NewWithRunners(agentsClient, runnersClient, &testutil.FakeSecretsClient{}, cfg).
		WithCatalog(&fakeImagesClient{}, &fakeOrganizationsClient{}, nil)
}

func envMap(envs []*runnerv1.EnvVar) map[string]string {
	result := make(map[string]string, len(envs))
	for _, env := range envs {
		result[env.GetName()] = env.GetValue()
	}
	return result
}

func buildRunner(id string) *runnersv1.Runner {
	orgID := testOrganizationID
	return &runnersv1.Runner{
		Meta:           &runnersv1.EntityMeta{Id: id},
		OrganizationId: &orgID,
		Status:         runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED,
	}
}

type fakeRunnerDialer struct {
	dial func(context.Context, string) (runnerv1.RunnerServiceClient, error)
}

func (f *fakeRunnerDialer) Dial(ctx context.Context, runnerID string) (runnerv1.RunnerServiceClient, error) {
	if f.dial != nil {
		return f.dial(ctx, runnerID)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerDialer) Close() {}

type fakeRunnersClient struct {
	createAnchoredWorkload       func(context.Context, *runnersv1.CreateAnchoredWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateAnchoredWorkloadResponse, error)
	bindWorkloadResourceAnchors  func(context.Context, *runnersv1.BindWorkloadResourceAnchorsRequest, ...grpc.CallOption) (*runnersv1.BindWorkloadResourceAnchorsResponse, error)
	updateAnchoredWorkload       func(context.Context, *runnersv1.UpdateAnchoredWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error)
	createPreparedWorkload       func(context.Context, *runnersv1.CreatePreparedWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error)
	updatePreparedWorkload       func(context.Context, *runnersv1.UpdatePreparedWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error)
	getWorkload                  func(context.Context, *runnersv1.GetWorkloadRequest, ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error)
	createVolumeChecked          func(context.Context, *runnersv1.CreateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error)
	updateVolumeChecked          func(context.Context, *runnersv1.UpdateVolumeCheckedRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error)
	createWorkload               func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error)
	createVolume                 func(context.Context, *runnersv1.CreateVolumeRequest, ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error)
	listFlavors                  func(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error)
	deleteWorkload               func(context.Context, *runnersv1.DeleteWorkloadRequest, ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error)
	getRunner                    func(context.Context, *runnersv1.GetRunnerRequest, ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error)
	listWorkloads                func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error)
	listWorkloadsByThread        func(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error)
	batchUpdateWorkload          func(context.Context, *runnersv1.BatchUpdateWorkloadSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error)
	listVolumes                  func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error)
	listVolumesByThread          func(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error)
	listRunners                  func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error)
	updateWorkload               func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error)
	updateWorkloadStatus         func(context.Context, *runnersv1.UpdateWorkloadStatusRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error)
	updateVolume                 func(context.Context, *runnersv1.UpdateVolumeRequest, ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error)
	batchUpdateVolume            func(context.Context, *runnersv1.BatchUpdateVolumeSampledAtRequest, ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error)
	getVolume                    func(context.Context, *runnersv1.GetVolumeRequest, ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error)
	streamWorkloadLogs           func(context.Context, *runnerv1.StreamWorkloadLogsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error)
	listWorkloadsByAgentInstance func(context.Context, *runnersv1.ListWorkloadsByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error)
	listVolumesByAgentInstance   func(context.Context, *runnersv1.ListVolumesByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error)
}

func (f *fakeRunnersClient) CreateVolumeChecked(ctx context.Context, req *runnersv1.CreateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.CreateVolumeCheckedResponse, error) {
	if f.createVolumeChecked != nil {
		return f.createVolumeChecked(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateVolumeChecked(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
	if f.updateVolumeChecked != nil {
		return f.updateVolumeChecked(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) RegisterRunner(context.Context, *runnersv1.RegisterRunnerRequest, ...grpc.CallOption) (*runnersv1.RegisterRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) GetRunner(ctx context.Context, req *runnersv1.GetRunnerRequest, opts ...grpc.CallOption) (*runnersv1.GetRunnerResponse, error) {
	if f.getRunner != nil {
		return f.getRunner(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListFlavors(ctx context.Context, req *runnersv1.ListFlavorsRequest, opts ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
	if f.listFlavors != nil {
		return f.listFlavors(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListRunners(ctx context.Context, req *runnersv1.ListRunnersRequest, opts ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
	if f.listRunners != nil {
		return f.listRunners(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateRunner(context.Context, *runnersv1.UpdateRunnerRequest, ...grpc.CallOption) (*runnersv1.UpdateRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) EnrollRunner(ctx context.Context, in *runnersv1.EnrollRunnerRequest, opts ...grpc.CallOption) (*runnersv1.EnrollRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f *fakeRunnersClient) DeleteRunner(context.Context, *runnersv1.DeleteRunnerRequest, ...grpc.CallOption) (*runnersv1.DeleteRunnerResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ValidateServiceToken(context.Context, *runnersv1.ValidateServiceTokenRequest, ...grpc.CallOption) (*runnersv1.ValidateServiceTokenResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) CreateFlavor(context.Context, *runnersv1.CreateFlavorRequest, ...grpc.CallOption) (*runnersv1.CreateFlavorResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateFlavor(context.Context, *runnersv1.UpdateFlavorRequest, ...grpc.CallOption) (*runnersv1.UpdateFlavorResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) DeleteFlavor(context.Context, *runnersv1.DeleteFlavorRequest, ...grpc.CallOption) (*runnersv1.DeleteFlavorResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) CreateWorkload(ctx context.Context, req *runnersv1.CreateWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
	if f.createWorkload != nil {
		return f.createWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) CreateVolume(ctx context.Context, req *runnersv1.CreateVolumeRequest, opts ...grpc.CallOption) (*runnersv1.CreateVolumeResponse, error) {
	if f.createVolume != nil {
		return f.createVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateWorkload(ctx context.Context, req *runnersv1.UpdateWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
	if f.updateWorkload != nil {
		return f.updateWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) UpdateWorkloadStatus(ctx context.Context, req *runnersv1.UpdateWorkloadStatusRequest, opts ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error) {
	if f.updateWorkloadStatus != nil {
		return f.updateWorkloadStatus(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) DeleteWorkload(ctx context.Context, req *runnersv1.DeleteWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
	if f.deleteWorkload != nil {
		return f.deleteWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) GetWorkload(ctx context.Context, req *runnersv1.GetWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error) {
	if f.getWorkload != nil {
		return f.getWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) CreatePreparedWorkload(ctx context.Context, req *runnersv1.CreatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error) {
	if f.createPreparedWorkload != nil {
		return f.createPreparedWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "prepared registry unavailable")
}

func (f *fakeRunnersClient) UpdatePreparedWorkload(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
	if f.updatePreparedWorkload != nil {
		return f.updatePreparedWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "prepared registry unavailable")
}

func (f *fakeRunnersClient) GetVolume(ctx context.Context, req *runnersv1.GetVolumeRequest, opts ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
	if f.getVolume != nil {
		return f.getVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	if f.listWorkloadsByThread != nil {
		return f.listWorkloadsByThread(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListWorkloadsByAgentInstance(ctx context.Context, req *runnersv1.ListWorkloadsByAgentInstanceRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
	if f.listWorkloadsByAgentInstance != nil {
		return f.listWorkloadsByAgentInstance(ctx, req, opts...)
	}
	if f.listWorkloadsByThread != nil {
		resp, err := f.listWorkloadsByThread(ctx, &runnersv1.ListWorkloadsByThreadRequest{ThreadId: req.GetAgentInstanceId(), Statuses: req.GetStatuses(), PageSize: req.GetPageSize(), PageToken: req.GetPageToken()}, opts...)
		if err != nil {
			return nil, err
		}
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: resp.GetWorkloads(), NextPageToken: resp.GetNextPageToken()}, nil
	}
	return &runnersv1.ListWorkloadsByAgentInstanceResponse{}, nil
}

func (f *fakeRunnersClient) ListWorkloads(ctx context.Context, req *runnersv1.ListWorkloadsRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
	if f.listWorkloads != nil {
		return f.listWorkloads(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) BatchUpdateWorkloadSampledAt(ctx context.Context, req *runnersv1.BatchUpdateWorkloadSampledAtRequest, opts ...grpc.CallOption) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
	if f.batchUpdateWorkload != nil {
		return f.batchUpdateWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListVolumes(ctx context.Context, req *runnersv1.ListVolumesRequest, opts ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
	if f.listVolumes != nil {
		return f.listVolumes(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) ListVolumesByThread(ctx context.Context, req *runnersv1.ListVolumesByThreadRequest, opts ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
	if f.listVolumesByThread != nil {
		return f.listVolumesByThread(ctx, req, opts...)
	}
	return &runnersv1.ListVolumesByThreadResponse{}, nil
}

func (f *fakeRunnersClient) ListVolumesByAgentInstance(ctx context.Context, req *runnersv1.ListVolumesByAgentInstanceRequest, opts ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error) {
	if f.listVolumesByAgentInstance != nil {
		return f.listVolumesByAgentInstance(ctx, req, opts...)
	}
	if f.listVolumesByThread != nil {
		resp, err := f.listVolumesByThread(ctx, &runnersv1.ListVolumesByThreadRequest{ThreadId: req.GetAgentInstanceId(), PageSize: req.GetPageSize(), PageToken: req.GetPageToken()}, opts...)
		if err != nil {
			return nil, err
		}
		return &runnersv1.ListVolumesByAgentInstanceResponse{Volumes: resp.GetVolumes(), NextPageToken: resp.GetNextPageToken()}, nil
	}
	return &runnersv1.ListVolumesByAgentInstanceResponse{}, nil
}

func (f *fakeRunnersClient) BatchUpdateVolumeSampledAt(ctx context.Context, req *runnersv1.BatchUpdateVolumeSampledAtRequest, opts ...grpc.CallOption) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error) {
	if f.batchUpdateVolume != nil {
		return f.batchUpdateVolume(ctx, req, opts...)
	}
	return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
}

func (f *fakeRunnersClient) UpdateVolume(ctx context.Context, req *runnersv1.UpdateVolumeRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeResponse, error) {
	if f.updateVolume != nil {
		return f.updateVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) TouchWorkload(context.Context, *runnersv1.TouchWorkloadRequest, ...grpc.CallOption) (*runnersv1.TouchWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnersClient) StreamWorkloadLogs(ctx context.Context, req *runnerv1.StreamWorkloadLogsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error) {
	if f.streamWorkloadLogs != nil {
		return f.streamWorkloadLogs(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeRunnerClient struct {
	revokeWorkloadPreparation    func(context.Context, *runnerv1.RevokeWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.RevokeWorkloadPreparationResponse, error)
	observePreparationRevocation func(context.Context, *runnerv1.ObservePreparationRevocationRequest, ...grpc.CallOption) (*runnerv1.ObservePreparationRevocationResponse, error)
	reserveResourceAnchor        func(context.Context, *runnerv1.ReserveResourceAnchorRequest, ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error)
	prepareAnchoredWorkload      func(context.Context, *runnerv1.PrepareAnchoredWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareAnchoredWorkloadResponse, error)
	removeWorkloadAnchor         func(context.Context, *runnerv1.RemoveWorkloadAnchorRequest, ...grpc.CallOption) (*runnerv1.RemoveWorkloadAnchorResponse, error)
	observeWorkloadPreparation   func(context.Context, *runnerv1.ObserveWorkloadPreparationRequest, ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error)
	prepareWorkload              func(context.Context, *runnerv1.PrepareWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error)
	activateWorkload             func(context.Context, *runnerv1.ActivateWorkloadRequest, ...grpc.CallOption) (*runnerv1.ActivateWorkloadResponse, error)
	inspectPreparedWorkload      func(context.Context, *runnerv1.InspectPreparedWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectPreparedWorkloadResponse, error)
	removePreparedWorkload       func(context.Context, *runnerv1.RemovePreparedWorkloadRequest, ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error)
	removeVolumeBound            func(context.Context, *runnerv1.RemoveVolumeBoundRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error)
	removeVolumeAnchored         func(context.Context, *runnerv1.RemoveVolumeAnchoredRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeAnchoredResponse, error)
	startWorkload                func(context.Context, *runnerv1.StartWorkloadRequest, ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error)
	stopWorkload                 func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error)
	listWorkloads                func(context.Context, *runnerv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error)
	listVolumes                  func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error)
	removeVolume                 func(context.Context, *runnerv1.RemoveVolumeRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error)
	inspectWorkload              func(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error)
	findWorkloadsByLabels        func(context.Context, *runnerv1.FindWorkloadsByLabelsRequest, ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error)
}

func (f *fakeRunnerClient) ObserveWorkloadPreparation(ctx context.Context, req *runnerv1.ObserveWorkloadPreparationRequest, opts ...grpc.CallOption) (*runnerv1.ObserveWorkloadPreparationResponse, error) {
	if f.observeWorkloadPreparation != nil {
		return f.observeWorkloadPreparation(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "preparation observation unavailable")
}

func (f *fakeRunnerClient) PrepareWorkload(ctx context.Context, req *runnerv1.PrepareWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
	if f.prepareWorkload != nil {
		return f.prepareWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "prepared runner unavailable")
}

func (f *fakeRunnerClient) ActivateWorkload(ctx context.Context, req *runnerv1.ActivateWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.ActivateWorkloadResponse, error) {
	if f.activateWorkload != nil {
		return f.activateWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "prepared runner unavailable")
}

func (f *fakeRunnerClient) InspectPreparedWorkload(ctx context.Context, req *runnerv1.InspectPreparedWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.InspectPreparedWorkloadResponse, error) {
	if f.inspectPreparedWorkload != nil {
		return f.inspectPreparedWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "prepared runner unavailable")
}

func (f *fakeRunnerClient) RemovePreparedWorkload(ctx context.Context, req *runnerv1.RemovePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error) {
	if f.removePreparedWorkload != nil {
		return f.removePreparedWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "prepared runner unavailable")
}

func (f *fakeRunnerClient) RemoveVolumeChecked(context.Context, *runnerv1.RemoveVolumeCheckedRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveVolumeBound(ctx context.Context, req *runnerv1.RemoveVolumeBoundRequest, opts ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
	if f.removeVolumeBound != nil {
		return f.removeVolumeBound(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveVolumeAnchored(ctx context.Context, req *runnerv1.RemoveVolumeAnchoredRequest, opts ...grpc.CallOption) (*runnerv1.RemoveVolumeAnchoredResponse, error) {
	if f.removeVolumeAnchored != nil {
		return f.removeVolumeAnchored(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "anchored retirement unavailable")
}

func (f *fakeRunnerClient) Ready(context.Context, *runnerv1.ReadyRequest, ...grpc.CallOption) (*runnerv1.ReadyResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StartWorkload(ctx context.Context, req *runnerv1.StartWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
	if f.startWorkload != nil {
		return f.startWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StopWorkload(ctx context.Context, req *runnerv1.StopWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
	if f.stopWorkload != nil {
		return f.stopWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveWorkload(context.Context, *runnerv1.RemoveWorkloadRequest, ...grpc.CallOption) (*runnerv1.RemoveWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) InspectWorkload(ctx context.Context, req *runnerv1.InspectWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
	if f.inspectWorkload != nil {
		return f.inspectWorkload(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) GetWorkloadLabels(context.Context, *runnerv1.GetWorkloadLabelsRequest, ...grpc.CallOption) (*runnerv1.GetWorkloadLabelsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) FindWorkloadsByLabels(ctx context.Context, req *runnerv1.FindWorkloadsByLabelsRequest, opts ...grpc.CallOption) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
	if f.findWorkloadsByLabels != nil {
		return f.findWorkloadsByLabels(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) ListWorkloads(ctx context.Context, req *runnerv1.ListWorkloadsRequest, opts ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
	if f.listWorkloads != nil {
		return f.listWorkloads(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) ListVolumes(ctx context.Context, req *runnerv1.ListVolumesRequest, opts ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
	if f.listVolumes != nil {
		return f.listVolumes(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) ListWorkloadsByVolume(context.Context, *runnerv1.ListWorkloadsByVolumeRequest, ...grpc.CallOption) (*runnerv1.ListWorkloadsByVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) RemoveVolume(ctx context.Context, req *runnerv1.RemoveVolumeRequest, opts ...grpc.CallOption) (*runnerv1.RemoveVolumeResponse, error) {
	if f.removeVolume != nil {
		return f.removeVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) TouchWorkload(context.Context, *runnerv1.TouchWorkloadRequest, ...grpc.CallOption) (*runnerv1.TouchWorkloadResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) PutArchive(context.Context, *runnerv1.PutArchiveRequest, ...grpc.CallOption) (*runnerv1.PutArchiveResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StreamWorkloadLogs(context.Context, *runnerv1.StreamWorkloadLogsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamWorkloadLogsResponse], error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) StreamEvents(context.Context, *runnerv1.StreamEventsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[runnerv1.StreamEventsResponse], error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) Exec(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[runnerv1.ExecRequest, runnerv1.ExecResponse], error) {
	return nil, errNotImplemented
}

func (f *fakeRunnerClient) CancelExecution(context.Context, *runnerv1.CancelExecutionRequest, ...grpc.CallOption) (*runnerv1.CancelExecutionResponse, error) {
	return nil, errNotImplemented
}

type fakeZitiMgmtClient struct {
	createAgentIdentity         func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error)
	createSandboxIdentity       func(context.Context, *zitimgmtv1.CreateSandboxIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateSandboxIdentityResponse, error)
	patchIdentityRoleAttributes func(context.Context, *zitimgmtv1.PatchIdentityRoleAttributesRequest, ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error)
	createAppIdentity           func(context.Context, *zitimgmtv1.CreateAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error)
	createService               func(context.Context, *zitimgmtv1.CreateServiceRequest, ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error)
	getService                  func(context.Context, *zitimgmtv1.GetServiceRequest, ...grpc.CallOption) (*zitimgmtv1.GetServiceResponse, error)
	listServices                func(context.Context, *zitimgmtv1.ListServicesRequest, ...grpc.CallOption) (*zitimgmtv1.ListServicesResponse, error)
	createRunnerIdentity        func(context.Context, *zitimgmtv1.CreateRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error)
	deleteAppIdentity           func(context.Context, *zitimgmtv1.DeleteAppIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error)
	deleteIdentity              func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error)
	deleteRunnerIdentity        func(context.Context, *zitimgmtv1.DeleteRunnerIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error)
	listManagedIdentities       func(context.Context, *zitimgmtv1.ListManagedIdentitiesRequest, ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error)
	requestServiceIdentity      func(context.Context, *zitimgmtv1.RequestServiceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error)
	extendIdentityLease         func(context.Context, *zitimgmtv1.ExtendIdentityLeaseRequest, ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error)
	createServicePolicy         func(context.Context, *zitimgmtv1.CreateServicePolicyRequest, ...grpc.CallOption) (*zitimgmtv1.CreateServicePolicyResponse, error)
	getServicePolicy            func(context.Context, *zitimgmtv1.GetServicePolicyRequest, ...grpc.CallOption) (*zitimgmtv1.GetServicePolicyResponse, error)
	listServicePolicies         func(context.Context, *zitimgmtv1.ListServicePoliciesRequest, ...grpc.CallOption) (*zitimgmtv1.ListServicePoliciesResponse, error)
	deleteServicePolicy         func(context.Context, *zitimgmtv1.DeleteServicePolicyRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteServicePolicyResponse, error)
	deleteService               func(context.Context, *zitimgmtv1.DeleteServiceRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteServiceResponse, error)
	createDeviceIdentity        func(context.Context, *zitimgmtv1.CreateDeviceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateDeviceIdentityResponse, error)
	deleteDeviceIdentity        func(context.Context, *zitimgmtv1.DeleteDeviceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteDeviceIdentityResponse, error)
	createTunnelIdentity        func(context.Context, *zitimgmtv1.CreateTunnelIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateTunnelIdentityResponse, error)
	deleteTunnelIdentity        func(context.Context, *zitimgmtv1.DeleteTunnelIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteTunnelIdentityResponse, error)
	listServicesByTag           func(context.Context, *zitimgmtv1.ListServicesByTagRequest, ...grpc.CallOption) (*zitimgmtv1.ListServicesByTagResponse, error)
	listIdentitiesByTag         func(context.Context, *zitimgmtv1.ListIdentitiesByTagRequest, ...grpc.CallOption) (*zitimgmtv1.ListIdentitiesByTagResponse, error)
	listServicePoliciesByTag    func(context.Context, *zitimgmtv1.ListServicePoliciesByTagRequest, ...grpc.CallOption) (*zitimgmtv1.ListServicePoliciesByTagResponse, error)
	updateService               func(context.Context, *zitimgmtv1.UpdateServiceRequest, ...grpc.CallOption) (*zitimgmtv1.UpdateServiceResponse, error)
	getIdentityLiveness         func(context.Context, *zitimgmtv1.GetIdentityLivenessRequest, ...grpc.CallOption) (*zitimgmtv1.GetIdentityLivenessResponse, error)
}

func (f *fakeZitiMgmtClient) CreateAgentIdentity(ctx context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
	if f.createAgentIdentity != nil {
		return f.createAgentIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateSandboxIdentity(ctx context.Context, req *zitimgmtv1.CreateSandboxIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateSandboxIdentityResponse, error) {
	if f.createSandboxIdentity != nil {
		return f.createSandboxIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) PatchIdentityRoleAttributes(ctx context.Context, req *zitimgmtv1.PatchIdentityRoleAttributesRequest, opts ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error) {
	if f.patchIdentityRoleAttributes != nil {
		return f.patchIdentityRoleAttributes(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateAppIdentity(ctx context.Context, req *zitimgmtv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error) {
	if f.createAppIdentity != nil {
		return f.createAppIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateService(ctx context.Context, req *zitimgmtv1.CreateServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error) {
	if f.createService != nil {
		return f.createService(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) GetService(ctx context.Context, req *zitimgmtv1.GetServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.GetServiceResponse, error) {
	if f.getService != nil {
		return f.getService(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListServices(ctx context.Context, req *zitimgmtv1.ListServicesRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListServicesResponse, error) {
	if f.listServices != nil {
		return f.listServices(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateRunnerIdentity(ctx context.Context, req *zitimgmtv1.CreateRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error) {
	if f.createRunnerIdentity != nil {
		return f.createRunnerIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteAppIdentity(ctx context.Context, req *zitimgmtv1.DeleteAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error) {
	if f.deleteAppIdentity != nil {
		return f.deleteAppIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteIdentity(ctx context.Context, req *zitimgmtv1.DeleteIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
	if f.deleteIdentity != nil {
		return f.deleteIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteRunnerIdentity(ctx context.Context, req *zitimgmtv1.DeleteRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error) {
	if f.deleteRunnerIdentity != nil {
		return f.deleteRunnerIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListManagedIdentities(ctx context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
	if f.listManagedIdentities != nil {
		return f.listManagedIdentities(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ResolveIdentity(context.Context, *zitimgmtv1.ResolveIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.ResolveIdentityResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) RequestServiceIdentity(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
	if f.requestServiceIdentity != nil {
		return f.requestServiceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ExtendIdentityLease(ctx context.Context, req *zitimgmtv1.ExtendIdentityLeaseRequest, opts ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
	if f.extendIdentityLease != nil {
		return f.extendIdentityLease(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateServicePolicy(ctx context.Context, req *zitimgmtv1.CreateServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServicePolicyResponse, error) {
	if f.createServicePolicy != nil {
		return f.createServicePolicy(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) GetServicePolicy(ctx context.Context, req *zitimgmtv1.GetServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.GetServicePolicyResponse, error) {
	if f.getServicePolicy != nil {
		return f.getServicePolicy(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListServicePolicies(ctx context.Context, req *zitimgmtv1.ListServicePoliciesRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListServicePoliciesResponse, error) {
	if f.listServicePolicies != nil {
		return f.listServicePolicies(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteServicePolicy(ctx context.Context, req *zitimgmtv1.DeleteServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteServicePolicyResponse, error) {
	if f.deleteServicePolicy != nil {
		return f.deleteServicePolicy(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteService(ctx context.Context, req *zitimgmtv1.DeleteServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteServiceResponse, error) {
	if f.deleteService != nil {
		return f.deleteService(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) CreateDeviceIdentity(ctx context.Context, req *zitimgmtv1.CreateDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateDeviceIdentityResponse, error) {
	if f.createDeviceIdentity != nil {
		return f.createDeviceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteDeviceIdentity(ctx context.Context, req *zitimgmtv1.DeleteDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteDeviceIdentityResponse, error) {
	if f.deleteDeviceIdentity != nil {
		return f.deleteDeviceIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeGroupsClient struct {
	groupsByOrg map[string][]*groupsv1.Group
	requests    []*groupsv1.ListMemberGroupsRequest
	identityIDs []string
}

func (f *fakeGroupsClient) CreateGroup(context.Context, *groupsv1.CreateGroupRequest, ...grpc.CallOption) (*groupsv1.CreateGroupResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) GetGroup(context.Context, *groupsv1.GetGroupRequest, ...grpc.CallOption) (*groupsv1.GetGroupResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) ListGroups(context.Context, *groupsv1.ListGroupsRequest, ...grpc.CallOption) (*groupsv1.ListGroupsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) UpdateGroup(context.Context, *groupsv1.UpdateGroupRequest, ...grpc.CallOption) (*groupsv1.UpdateGroupResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) DeleteGroup(context.Context, *groupsv1.DeleteGroupRequest, ...grpc.CallOption) (*groupsv1.DeleteGroupResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) AddMember(context.Context, *groupsv1.AddMemberRequest, ...grpc.CallOption) (*groupsv1.AddMemberResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) RemoveMember(context.Context, *groupsv1.RemoveMemberRequest, ...grpc.CallOption) (*groupsv1.RemoveMemberResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) ListMembers(context.Context, *groupsv1.ListMembersRequest, ...grpc.CallOption) (*groupsv1.ListMembersResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeGroupsClient) ListMemberGroups(ctx context.Context, request *groupsv1.ListMemberGroupsRequest, _ ...grpc.CallOption) (*groupsv1.ListMemberGroupsResponse, error) {
	metadataValues, _ := metadata.FromOutgoingContext(ctx)
	f.identityIDs = append(f.identityIDs, metadataValues.Get(identityMetadataKey)...)
	f.requests = append(f.requests, proto.Clone(request).(*groupsv1.ListMemberGroupsRequest))
	return &groupsv1.ListMemberGroupsResponse{Groups: append([]*groupsv1.Group{}, f.groupsByOrg[request.GetOrganizationId()]...)}, nil
}

func (f *fakeGroupsClient) ListMemberGroupsBatch(context.Context, *groupsv1.ListMemberGroupsBatchRequest, ...grpc.CallOption) (*groupsv1.ListMemberGroupsBatchResponse, error) {
	return nil, errNotImplemented
}

type fakeGroupMembershipSubscription struct {
	unsubscribed chan struct{}
}

func (s *fakeGroupMembershipSubscription) Unsubscribe() error {
	close(s.unsubscribed)
	return nil
}

func mustMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	return data
}

func assertStringSet(t *testing.T, actual []string, expected []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	counts := map[string]int{}
	for _, value := range actual {
		counts[value]++
	}
	for _, value := range expected {
		counts[value]--
	}
	for value, count := range counts {
		if count != 0 {
			t.Fatalf("expected %v, got %v; mismatch on %s", expected, actual, value)
		}
	}
}

func (f *fakeZitiMgmtClient) CreateTunnelIdentity(ctx context.Context, req *zitimgmtv1.CreateTunnelIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateTunnelIdentityResponse, error) {
	if f.createTunnelIdentity != nil {
		return f.createTunnelIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) DeleteTunnelIdentity(ctx context.Context, req *zitimgmtv1.DeleteTunnelIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteTunnelIdentityResponse, error) {
	if f.deleteTunnelIdentity != nil {
		return f.deleteTunnelIdentity(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListServicesByTag(ctx context.Context, req *zitimgmtv1.ListServicesByTagRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListServicesByTagResponse, error) {
	if f.listServicesByTag != nil {
		return f.listServicesByTag(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListIdentitiesByTag(ctx context.Context, req *zitimgmtv1.ListIdentitiesByTagRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListIdentitiesByTagResponse, error) {
	if f.listIdentitiesByTag != nil {
		return f.listIdentitiesByTag(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) UpdateService(ctx context.Context, req *zitimgmtv1.UpdateServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.UpdateServiceResponse, error) {
	if f.updateService != nil {
		return f.updateService(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) GetIdentityLiveness(ctx context.Context, req *zitimgmtv1.GetIdentityLivenessRequest, opts ...grpc.CallOption) (*zitimgmtv1.GetIdentityLivenessResponse, error) {
	if f.getIdentityLiveness != nil {
		return f.getIdentityLiveness(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeZitiMgmtClient) ListServicePoliciesByTag(ctx context.Context, req *zitimgmtv1.ListServicePoliciesByTagRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListServicePoliciesByTagResponse, error) {
	if f.listServicePoliciesByTag != nil {
		return f.listServicePoliciesByTag(ctx, req, opts...)
	}
	return nil, errNotImplemented
}
