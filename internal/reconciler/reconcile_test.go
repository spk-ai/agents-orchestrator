package reconciler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Sandbox workloads were skipped outright, so a sandbox whose pod was gone sat
// in running forever: the Console kept listing it as active compute and the
// flavor-seconds sampler kept billing it.
func TestReconcileWorkloadsFailsMissingSandbox(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-sandbox-1"
	sandboxID := uuid.New().String()

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{
					Meta:           &runnersv1.EntityMeta{Id: workloadKey},
					RunnerId:       runnerID,
					OrganizationId: testOrganizationID,
					OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
					OwnerId:        sandboxID,
					Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	// The runner no longer reports it.
	runner := &fakeRunnerClient{
		inspectWorkload: absentRunnerWorkload,
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, _ string) (runnerv1.RunnerServiceClient, error) { return runner, nil },
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       &testutil.FakeAgentsClient{},
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected the sandbox workload to be updated")
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("expected failed, got %s", updateReq.GetStatus())
	}
	if updateReq.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST {
		t.Fatalf("expected runtime_lost, got %s", updateReq.GetFailureReason())
	}
}

func TestReconcileWorkloadsTransitionsStartingToRunning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))
	startTime := timestamppb.New(time.Date(2024, time.January, 1, 2, 3, 4, 0, time.UTC))
	finishTime := timestamppb.New(time.Date(2024, time.January, 1, 3, 4, 5, 0, time.UTC))
	reason := "Completed"
	message := "done"
	exitCode := int32(0)

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{Containers: []*runnerv1.WorkloadContainer{
				{
					ContainerId:  "init-id",
					Name:         "init",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_INIT,
					Image:        "init-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED,
					Reason:       &reason,
					Message:      &message,
					ExitCode:     &exitCode,
					RestartCount: 2,
					StartedAt:    startTime,
					FinishedAt:   finishTime,
				},
				{
					ContainerId:  "main-id",
					Name:         "main",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
					Image:        "main-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
					RestartCount: 1,
					StartedAt:    startTime,
				},
				{
					ContainerId: "sidecar-id",
					Name:        "sidecar",
					Role:        runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
					Image:       "sidecar-image",
					Status:      runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING,
				},
			}}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
	if len(updateReq.GetContainers()) != 3 {
		t.Fatalf("expected 3 containers, got %d", len(updateReq.GetContainers()))
	}
	initContainer := updateReq.GetContainers()[0]
	if initContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_INIT {
		t.Fatalf("unexpected init role: %v", initContainer.GetRole())
	}
	if initContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
		t.Fatalf("unexpected init status: %v", initContainer.GetStatus())
	}
	if initContainer.GetReason() != reason || initContainer.Reason == nil {
		t.Fatalf("unexpected init reason: %v", initContainer.GetReason())
	}
	if initContainer.GetMessage() != message || initContainer.Message == nil {
		t.Fatalf("unexpected init message: %v", initContainer.GetMessage())
	}
	if initContainer.GetExitCode() != exitCode || initContainer.ExitCode == nil {
		t.Fatalf("unexpected init exit code: %v", initContainer.GetExitCode())
	}
	if initContainer.GetStartedAt().AsTime() != startTime.AsTime() {
		t.Fatalf("unexpected init started_at")
	}
	if initContainer.GetFinishedAt().AsTime() != finishTime.AsTime() {
		t.Fatalf("unexpected init finished_at")
	}
	mainContainer := updateReq.GetContainers()[1]
	if mainContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
		t.Fatalf("unexpected main role: %v", mainContainer.GetRole())
	}
	if mainContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
		t.Fatalf("unexpected main status: %v", mainContainer.GetStatus())
	}
	sidecarContainer := updateReq.GetContainers()[2]
	if sidecarContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
		t.Fatalf("unexpected sidecar role: %v", sidecarContainer.GetRole())
	}
}

func TestReconcileWorkloadsTransitionsStartingToRunningWithoutContainers(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{StateRunning: true}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
	if len(updateReq.GetContainers()) != 0 {
		t.Fatalf("expected no containers, got %d", len(updateReq.GetContainers()))
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
}

func TestReconcileWorkloadsDoesNotPromoteStartingWhenNotRunning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))

	updateCalled := false
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, InstanceId: stringPtr(rawInstanceID)},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, _ *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateCalled = true
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{StateRunning: false}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateCalled {
		t.Fatal("expected no update workload")
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
}

func TestReconcileWorkloadsRefreshesContainersOnRunning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	startTime := timestamppb.New(time.Date(2024, time.February, 1, 2, 3, 4, 0, time.UTC))

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, InstanceId: stringPtr(rawInstanceID)},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{Containers: []*runnerv1.WorkloadContainer{
				{
					ContainerId:  "main-id",
					Name:         "main",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
					Image:        "main-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
					RestartCount: 3,
					StartedAt:    startTime,
				},
			}}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.InstanceId != nil {
		t.Fatalf("unexpected instance id update: %v", updateReq.GetInstanceId())
	}
	if updateReq.Status != nil {
		t.Fatalf("unexpected status update: %v", updateReq.GetStatus())
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
	if len(updateReq.GetContainers()) != 1 {
		t.Fatalf("expected 1 container, got %d", len(updateReq.GetContainers()))
	}
	mainContainer := updateReq.GetContainers()[0]
	if mainContainer.GetContainerId() != "main-id" {
		t.Fatalf("unexpected main container id: %s", mainContainer.GetContainerId())
	}
	if mainContainer.GetRestartCount() != 3 {
		t.Fatalf("unexpected main restart count: %v", mainContainer.GetRestartCount())
	}
	if mainContainer.GetStartedAt().AsTime() != startTime.AsTime() {
		t.Fatalf("unexpected main started_at")
	}
}

func TestReconcileWorkloadsFailsCrashloop(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	zitiID := "ziti-identity"
	message := "crashloop"

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, InstanceId: stringPtr(rawInstanceID), ZitiIdentityId: zitiID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	stopCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{Containers: []*runnerv1.WorkloadContainer{
				{
					ContainerId:  "main-id",
					Name:         "main",
					Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
					Image:        "main-image",
					Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING,
					Reason:       stringPtr(crashLoopBackoffFlag),
					Message:      stringPtr(message),
					RestartCount: crashloopThreshold,
				},
			}}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	deleteCalled := false
	zitiMgmt := &fakeZitiMgmtClient{
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			if req.GetZitiIdentityId() != zitiID {
				return nil, errors.New("unexpected ziti identity id")
			}
			deleteCalled = true
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
		ZitiMgmt:     zitiMgmt,
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP {
		t.Fatalf("unexpected failure reason: %v", updateReq.GetFailureReason())
	}
	if updateReq.GetFailureMessage() != message {
		t.Fatalf("unexpected failure message: %s", updateReq.GetFailureMessage())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %s", updateReq.GetInstanceId())
	}
	if !stopCalled {
		t.Fatal("expected stop workload")
	}
	if deleteCalled {
		t.Fatal("must not delete identity while inspect still reports the workload")
	}
}

func TestReconcileWorkloadsDoesNotPromoteStartingOnInspectError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID
	createdAt := timestamppb.New(time.Date(2024, time.January, 1, 1, 2, 3, 0, time.UTC))

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey, CreatedAt: createdAt}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	inspectCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, errors.New("inspect failed")
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.Status != nil {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
	if len(updateReq.GetContainers()) != 0 {
		t.Fatalf("expected no containers, got %d", len(updateReq.GetContainers()))
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
}

func TestReconcileWorkloadsStopsStoppingOnInspectError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadKey := "workload-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadKey}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	inspectCalled := false
	stopCalled := false
	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: workloadKey, InstanceId: instanceID},
			}}, nil
		},
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			inspectCalled = true
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			return nil, errors.New("inspect failed")
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetInstanceId() != rawInstanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetInstanceId())
	}
	if updateReq.Status != nil {
		t.Fatalf("unexpected status update: %v", updateReq.GetStatus())
	}
	if !inspectCalled {
		t.Fatal("expected inspect workload")
	}
	if !stopCalled {
		t.Fatal("expected stop workload")
	}
}

func TestReconcileWorkloadsStopsOrphan(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	rawInstanceID := uuid.New().String()
	instanceID := "workload-" + rawInstanceID

	stopCalled := false
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}

	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{
				{WorkloadKey: "orphan", InstanceId: instanceID},
			}}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			if req.GetWorkloadId() != rawInstanceID {
				return nil, errors.New("unexpected workload id")
			}
			stopCalled = true
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if !stopCalled {
		t.Fatal("expected stop workload")
	}
}

func TestReconcileWorkloadsRetainsWorkloadOnNoTerminators(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq != nil {
		t.Fatal("runner dial failure does not confirm removal")
	}
}

func TestReconcileWorkloadsRetainsWorkloadOnNoTerminatorsListError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq != nil {
		t.Fatal("runner list failure does not confirm removal")
	}
}

func TestReconcileWorkloadsMarksMissingRunnerOnMissingWorkload(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"
	instanceID := uuid.New().String()

	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(testAgentID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, InstanceId: stringPtr(instanceID)},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		inspectWorkload: absentRunnerWorkload,
		listWorkloads: func(_ context.Context, _ *runnerv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnerv1.ListWorkloadsResponse, error) {
			return &runnerv1.ListWorkloadsResponse{}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update workload")
	}
	if updateReq.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST {
		t.Fatalf("unexpected failure reason: %v", updateReq.GetFailureReason())
	}
	if updateReq.GetFailureMessage() != "workload missing on runner" {
		t.Fatalf("unexpected failure message: %s", updateReq.GetFailureMessage())
	}
}

func TestReconcileWorkloadsDegradesUnenrolledRunner(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	threadID := uuid.New().String()
	workloadID := "workload-1"
	secondWorkloadID := "workload-2"

	updateCount := 0
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, ThreadId: threadID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(threadID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
				{Meta: &runnersv1.EntityMeta{Id: secondWorkloadID}, RunnerId: runnerID, ThreadId: threadID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(threadID), OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			orgID := testOrganizationID
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{
				{Meta: &runnersv1.EntityMeta{Id: runnerID}, OrganizationId: &orgID, Status: runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE},
			}}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateCount++
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
				return nil, errors.New("unexpected workload status")
			}
			if req.GetRemovalConfirmedAt() == nil {
				return nil, errors.New("missing removal_confirmed_at")
			}
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}

	degradeCalls := 0
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalls++
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonRunnerDeprovisioned {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
			return nil, errors.New("unexpected dial")
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileWorkloads(ctx); err != nil {
		t.Fatalf("reconcile workloads: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("unenrollment is not removal evidence; got %d workload updates", updateCount)
	}
	if degradeCalls != 0 {
		t.Fatalf("expected 0 degrade calls, got %d", degradeCalls)
	}
}

func TestReconcileVolumesActivatesProvisioning(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	instanceID := "volume-instance-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	volume := checkedTestVolume(volumeKey, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	volume.ThreadId, volume.VolumeId = threadID, volumeID
	var updateReq *runnersv1.UpdateVolumeCheckedRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				volume,
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateReq = req
			return checkedTestUpdate(t, volume, req), nil
		},
	}

	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{
				checkedTestInstance(volume, instanceID, "original-uid"),
			}}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected update volume")
	}
	if volume.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || updateReq.GetBind() == nil {
		t.Fatalf("unexpected binding/status: %v", updateReq)
	}
	if updateReq.GetBind().GetInstance().GetInstanceId() != instanceID {
		t.Fatalf("unexpected instance id: %v", updateReq.GetBind().GetInstance())
	}
}

func TestReconcileVolumesLeavesPersistentVolumeOnNoTerminatorsListError(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	var updateCount int
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(threadID), OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, _ *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateCount++
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected no volume updates, got %d", updateCount)
	}
}

func TestReconcileVolumesRetainsRecordOnMissingPVC(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	var updateReqs []*runnersv1.UpdateVolumeCheckedRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(threadID), OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateReqs = append(updateReqs, req)
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}

	degradeCalls := 0
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalls++
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonVolumeLost {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend}, nil
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
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if len(updateReqs) != 0 {
		t.Fatal("inventory absence must not finalize an active record")
	}
	if degradeCalls != 0 {
		t.Fatalf("expected 0 degrade calls, got %d", degradeCalls)
	}
}

func TestReconcileVolumesDegradesUnenrolledRunner(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	var updateReqs []*runnersv1.UpdateVolumeCheckedRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: volumeKey}, RunnerId: runnerID, AgentId: testAgentID, AgentClassId: stringPtr(testAgentID), AgentInstanceId: stringPtr(threadID), OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, ThreadId: threadID, VolumeId: volumeID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			orgID := testOrganizationID
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{
				{Meta: &runnersv1.EntityMeta{Id: runnerID}, OrganizationId: &orgID, Status: runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE},
			}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateReqs = append(updateReqs, req)
			return &runnersv1.UpdateVolumeCheckedResponse{}, nil
		},
	}

	degradeCalls := 0
	threads := &fakeThreadsClient{
		degradeThread: func(_ context.Context, req *threadsv1.DegradeThreadRequest, _ ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalls++
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			if req.GetReason() != degradeReasonRunnerDeprovisioned {
				return nil, errors.New("unexpected degrade reason")
			}
			return &threadsv1.DegradeThreadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) {
			return nil, errors.New("unexpected dial")
		},
	}
	agents := &testutil.FakeAgentsClient{}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Threads:      threads,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if len(updateReqs) != 0 {
		t.Fatal("unenrolled runner is not proof of physical absence")
	}
	if degradeCalls != 0 {
		t.Fatalf("expected 0 degrade calls, got %d", degradeCalls)
	}
}

func TestLoadSandboxWorkloadPlanAcceptsMultipleVolumes(t *testing.T) {
	ctx := context.Background()
	sandboxID := uuid.New()
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{}, nil
		},
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: "volume-1"}, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID.String(), OrganizationId: testOrganizationID},
				{Meta: &runnersv1.EntityMeta{Id: "volume-2"}, Status: runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID.String(), OrganizationId: testOrganizationID},
				{Meta: &runnersv1.EntityMeta{Id: "volume-3"}, Status: runnersv1.VolumeStatus_VOLUME_STATUS_DELETED, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID.String(), OrganizationId: testOrganizationID},
				{Meta: &runnersv1.EntityMeta{Id: "volume-4"}, Status: runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID.String(), OrganizationId: testOrganizationID},
			}}, nil
		},
	}
	reconciler := newTestReconciler(Config{Runners: runners})

	plan, err := reconciler.loadSandboxWorkloadPlan(ctx, &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: sandboxID.String()}, OrganizationId: testOrganizationID})
	if err != nil {
		t.Fatalf("load sandbox workload plan: %v", err)
	}
	if len(plan.workspaceVolumes) != 4 {
		t.Fatalf("expected all 4 records pending checked cleanup, got %d", len(plan.workspaceVolumes))
	}
}

func TestReconcileVolumesTTLExpires(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	volumeKey := "volume-1"
	instanceID := "volume-instance-1"
	threadID := uuid.New().String()
	volumeID := uuid.New().String()

	volume := checkedTestVolume(volumeKey, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	volume.ThreadId, volume.OwnerId, volume.VolumeId = threadID, threadID, volumeID
	volume.BoundInstance = checkedTestInstance(volume, instanceID, "ttl-uid")
	volume.InstanceId = stringPtr(instanceID)
	updateStatuses := []runnersv1.VolumeStatus{}
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				volume,
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			resp := checkedTestUpdate(t, volume, req)
			updateStatuses = append(updateStatuses, resp.Volume.Status)
			return resp, nil
		},
		listWorkloadsByThread: func(_ context.Context, req *runnersv1.ListWorkloadsByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
			if req.GetThreadId() != threadID {
				return nil, errors.New("unexpected thread id")
			}
			removedAt := timestamppb.New(time.Now().Add(-2 * time.Hour))
			return &runnersv1.ListWorkloadsByThreadResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, RemovedAt: removedAt, RemovalConfirmedAt: removedAt},
			}}, nil
		},
	}

	removeCalled := false
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{
				volume.BoundInstance,
			}}, nil
		},
		removeVolumeBound: func(_ context.Context, req *runnerv1.RemoveVolumeBoundRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
			if req.GetExpected().GetInstanceId() != instanceID || req.GetExpected().GetInstanceUid() != "ttl-uid" {
				return nil, errors.New("unexpected volume id")
			}
			removeCalled = true
			return &runnerv1.RemoveVolumeBoundResponse{BackendId: checkedTestBackend, State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING}, nil
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

	agents := &testutil.FakeAgentsClient{
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID {
				return nil, errors.New("unexpected volume id")
			}
			ttl := "1h"
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Meta: &agentsv1.EntityMeta{Id: volumeID}, Persistent: true, Ttl: &ttl}}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       agents,
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if len(updateStatuses) == 0 {
		t.Fatal("expected update volume")
	}
	if updateStatuses[len(updateStatuses)-1] != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
		t.Fatalf("unexpected update status: %v", updateStatuses)
	}
	if !removeCalled {
		t.Fatal("expected remove volume")
	}
}

func TestAgentInstanceActivityDoesNotTreatFailureTimeAsRemoval(t *testing.T) {
	ctx := context.Background()
	instanceID := uuid.New().String()
	endedAt := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	runners := &fakeRunnersClient{
		listWorkloadsByThread: func(_ context.Context, _ *runnersv1.ListWorkloadsByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
			return &runnersv1.ListWorkloadsByThreadResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1", UpdatedAt: timestamppb.New(endedAt)}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED},
			}}, nil
		},
	}
	reconciler := newTestReconciler(Config{Runners: runners})

	activity, err := reconciler.agentInstanceActivity(ctx, instanceID, map[string]instanceActivity{})
	if err != nil {
		t.Fatalf("agent instance activity: %v", err)
	}
	if !activity.hasActive {
		t.Fatal("unconfirmed workload removal must retain its volume")
	}
	if activity.latestRemovedAt != nil {
		t.Fatal("updated_at is not physical removal evidence")
	}
}

func TestReconcileVolumesKeepsReusedPersistentVolume(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	agentInstanceID := uuid.New().String()
	volumeID := uuid.New().String()
	volumeKey := uuid.NewSHA1(uuid.NameSpaceOID, []byte(agentInstanceID+":"+volumeID)).String()
	instanceID := "pv-" + agentInstanceID[:12] + "-" + volumeID[:12]

	volume := checkedTestVolume(volumeKey, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	volume.ThreadId, volume.OwnerId, volume.VolumeId = agentInstanceID, agentInstanceID, volumeID
	var updateReq *runnersv1.UpdateVolumeCheckedRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				volume,
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateReq = req
			return checkedTestUpdate(t, volume, req), nil
		},
	}
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{
				checkedTestInstance(volume, instanceID, "retained-uid"),
			}}, nil
		},
		removeVolumeBound: func(context.Context, *runnerv1.RemoveVolumeBoundRequest, ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
			return nil, errors.New("reused persistent volume must not be removed")
		},
	}
	runnerDialer := &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id != runnerID {
			return nil, errors.New("unexpected runner id")
		}
		return runner, nil
	}}
	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       &testutil.FakeAgentsClient{},
		Assembler:    newTestAssembler(uuid.New(), false),
	})

	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected volume update")
	}
	if updateReq.GetId() != volumeKey {
		t.Fatalf("unexpected update id: %q", updateReq.GetId())
	}
	if updateReq.GetBind() == nil || volume.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("unexpected bind/status: %v", updateReq)
	}
	if updateReq.GetBind().GetInstance().GetInstanceId() != instanceID {
		t.Fatalf("unexpected instance: %v", updateReq.GetBind().GetInstance())
	}
}

func TestRunnerInScopeAcceptsClusterRunnerWithTrackedWorkloads(t *testing.T) {
	workloads := map[string]*runnersv1.Workload{
		"workload-1": {AgentId: testAgentID, AgentInstanceId: stringPtr(testAgentID)},
	}
	inScope, err := runnerInScope("runner-1", "", map[string]struct{}{testOrganizationID: {}}, workloads)
	if err != nil {
		t.Fatalf("runner in scope: %v", err)
	}
	if !inScope {
		t.Fatal("expected a cluster runner carrying tracked workloads to be in scope")
	}
}

func TestRunnerInScopeIgnoresUntrackedClusterRunner(t *testing.T) {
	inScope, err := runnerInScope("runner-1", "", map[string]struct{}{testOrganizationID: {}}, nil)
	if err == nil {
		t.Fatal("expected missing organization error")
	}
	if inScope {
		t.Fatal("expected a runner with no organization and no workloads to be out of scope")
	}
}

// A runner whose organization has no agents is simply not this reconciler's,
// and skipping it is not an error -- it used to be one, because there was no
// identity to borrow for that organization.
func TestRunnerInScopeSkipsUnknownOrganization(t *testing.T) {
	inScope, err := runnerInScope("runner-1", uuid.New().String(), map[string]struct{}{testOrganizationID: {}}, nil)
	if err != nil {
		t.Fatalf("runner in scope: %v", err)
	}
	if inScope {
		t.Fatal("expected a runner in an organization with no agents to be out of scope")
	}
}

// Two owners on one runner used to be rejected outright: there was no single
// identity to impersonate for calls about it. Nothing is impersonated now, so
// the runner is reconciled like any other.
func TestRunnerInScopeAcceptsMixedOwnersOnClusterRunner(t *testing.T) {
	otherAgentID := uuid.New().String()
	workloads := map[string]*runnersv1.Workload{
		"workload-1": {AgentId: testAgentID, AgentInstanceId: stringPtr(testAgentID)},
		"workload-2": {AgentId: otherAgentID, AgentInstanceId: stringPtr(otherAgentID)},
	}
	inScope, err := runnerInScope("runner-1", "", map[string]struct{}{testOrganizationID: {}}, workloads)
	if err != nil {
		t.Fatalf("runner in scope: %v", err)
	}
	if !inScope {
		t.Fatal("expected a runner carrying two owners to be in scope")
	}
}

func TestReconcileVolumesSkipsSandboxOwnedVolumes(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	agentVolumeKey := "volume-agent"
	sandboxVolumeKey := "volume-sandbox"
	instanceID := "volume-instance-1"
	threadID := uuid.New().String()
	sandboxID := uuid.New().String()
	agentVolume := checkedTestVolume(agentVolumeKey, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	agentVolume.ThreadId = threadID
	sandboxVolume := checkedTestSandboxVolume(sandboxVolumeKey, sandboxID, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)

	// Keyed by volume: reconcileVolumes walks a map, so the order two volumes
	// are updated in is not fixed and a single captured request would make the
	// assertions depend on it.
	updates := map[string][]*runnersv1.UpdateVolumeCheckedRequest{}
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				sandboxVolume, agentVolume,
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updates[req.GetId()] = append(updates[req.GetId()], req)
			if req.GetId() != agentVolumeKey {
				t.Fatalf("unexpected sandbox volume mutation: %v", req)
			}
			return checkedTestUpdate(t, agentVolume, req), nil
		},
	}
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{
				checkedTestInstance(agentVolume, instanceID, "agent-volume-uid"),
				sandboxVolume.BoundInstance,
			}}, nil
		},
		removeVolumeBound: func(_ context.Context, req *runnerv1.RemoveVolumeBoundRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
			t.Fatalf("unexpected volume removal: %v", req)
			return nil, errNotImplemented
		},
	}
	runnerDialer := &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id != runnerID {
			return nil, errors.New("unexpected runner id")
		}
		return runner, nil
	}}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents:       &testutil.FakeAgentsClient{},
		Assembler:    newTestAssembler(uuid.New(), false),
	})
	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	agentUpdates := updates[agentVolumeKey]
	if len(agentUpdates) != 1 {
		t.Fatalf("expected one agent volume update, got %d", len(agentUpdates))
	}
	if agentUpdates[0].GetBind() == nil || agentVolume.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("unexpected binding: %v", agentUpdates[0])
	}
	if len(updates[sandboxVolumeKey]) != 0 {
		t.Fatal("an active sandbox volume must retain its binding and status")
	}
}

// A sandbox is reconciled wherever it lives. Deriving the organizations to look
// in - from configuration, or from which ones have agents - left a sandbox in
// any other organization with no pod and no error to say why.
func TestListTrackedSandboxesCoversEveryOrganization(t *testing.T) {
	ctx := context.Background()
	sandboxID := uuid.NewString()
	unconfiguredOrgID := uuid.New().String()
	var requests []*agentsv1.ListSandboxesRequest
	agents := &testutil.FakeAgentsClient{
		ListSandboxesFunc: func(_ context.Context, req *agentsv1.ListSandboxesRequest, _ ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error) {
			requests = append(requests, req)
			if req.GetOrganizationId() != "" {
				return &agentsv1.ListSandboxesResponse{}, nil
			}
			return &agentsv1.ListSandboxesResponse{Sandboxes: []*agentsv1.Sandbox{
				{Meta: &agentsv1.EntityMeta{Id: sandboxID}, OrganizationId: unconfiguredOrgID, Status: agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING},
			}}, nil
		},
	}
	reconciler := newTestReconciler(Config{Agents: agents})

	sandboxes, err := reconciler.listTrackedSandboxes(ctx)
	if err != nil {
		t.Fatalf("list tracked sandboxes: %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("expected one list request, got %d", len(requests))
	}
	if requests[0].GetOrganizationId() != "" {
		t.Fatalf("expected a request naming no organization, got %q", requests[0].GetOrganizationId())
	}
	if !requests[0].GetIncludeTerminated() {
		t.Fatal("expected terminated sandboxes included")
	}
	if len(sandboxes) != 1 || sandboxes[0].GetMeta().GetId() != sandboxID {
		t.Fatalf("unexpected sandboxes: %v", sandboxes)
	}
}

func TestStartSandboxWorkloadPersistsBindingBeforeReadiness(t *testing.T) {
	f, plan := preparedSandboxAssemblyFixture(t, true, true)
	var identity *zitimgmtv1.CreateSandboxIdentityRequest
	f.r.zitiMgmt = &fakeZitiMgmtClient{
		createSandboxIdentity: func(_ context.Context, req *zitimgmtv1.CreateSandboxIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.CreateSandboxIdentityResponse, error) {
			identity = req
			return &zitimgmtv1.CreateSandboxIdentityResponse{ZitiIdentityId: "sandbox-ziti-id", EnrollmentJwt: "sandbox-jwt"}, nil
		},
		createAgentIdentity: func(context.Context, *zitimgmtv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
			t.Fatal("sandbox used agent identity")
			return nil, errNotImplemented
		},
		createDeviceIdentity: func(context.Context, *zitimgmtv1.CreateDeviceIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.CreateDeviceIdentityResponse, error) {
			t.Fatal("sandbox used device identity")
			return nil, errNotImplemented
		},
	}
	if err := f.r.startSandboxWorkload(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if f.activations != 1 || f.lastVolume == nil || identity == nil || f.lastMetadata == nil {
		t.Fatal("sandbox startup incomplete")
	}
	if f.lastVolume.AgentId != "" || f.lastVolume.OwnerKind != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX || f.lastVolume.OwnerId != plan.sandboxID.String() ||
		f.lastVolume.SizeGb != "10" || f.lastMetadata.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || f.lastMetadata.ZitiIdentityId != "sandbox-ziti-id" ||
		f.lastMetadata.OwnerKind != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX || f.lastMetadata.OwnerId != plan.sandboxID.String() {
		t.Fatal("sandbox registry identity or workspace allocation lost")
	}
	if identity.SandboxId != plan.sandboxID.String() || identity.OwnerId != plan.sandbox.OwnerId || identity.EnvironmentId != plan.sandbox.EnvironmentId ||
		identity.OrganizationId != testOrganizationID || identity.WorkloadId != f.lastRequest.Workload.WorkloadId || len(identity.AdditionalRoleAttributes) != 0 ||
		identity.Tags["agyn.sandbox.id"] != plan.sandboxID.String() || identity.Tags["agyn.workload.id"] != f.w.Meta.Id {
		t.Fatal("sandbox ziti identity mismatch")
	}
	enroll := testutil.FindInitContainer(f.lastRequest.Workload.InitContainers, assembler.ZitiEnrollContainerName)
	if enroll == nil || envMap(enroll.Env)[assembler.ZitiEnrollmentTokenEnvVar] != "sandbox-jwt" {
		t.Fatal("missing sandbox enrollment setup")
	}
	if len(f.runtimeUpdates) != 1 || f.runtimeUpdates[0].GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING ||
		f.runtimeUpdates[0].GetWorkloadId() != f.w.Meta.Id || f.w.GetInstanceId() != f.w.Meta.Id {
		t.Fatal("activation incorrectly reported readiness")
	}
	plan.activeWorkload = proto.Clone(f.w).(*runnersv1.Workload)
	if err := f.r.reconcileActiveSandboxWorkload(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if f.w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING || len(f.runtimeUpdates) != 2 || f.runtimeUpdates[1].GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING ||
		f.runtimeUpdates[1].GetWorkloadId() != f.w.Meta.Id || f.activations != 1 {
		t.Fatal("exact-binding readiness did not promote sandbox")
	}
}

func TestReconcileSandboxPromotesStartingWorkload(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	sandboxID := uuid.NewString()
	ownerID := uuid.NewString()
	workloadID := uuid.NewString()
	createdAt := timestamppb.New(time.Now().Add(-time.Minute))
	var updateReq *runnersv1.UpdateWorkloadRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if len(req.GetFilter().GetOwnerKindIn()) != 1 || req.GetFilter().GetOwnerKindIn()[0] != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
				return nil, errors.New("expected sandbox owner filter")
			}
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID, CreatedAt: createdAt}, RunnerId: runnerID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, InstanceId: stringPtr(workloadID), OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID},
			}}, nil
		},
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			updateReq = req
			return &runnersv1.UpdateWorkloadResponse{}, nil
		},
	}
	runner := &fakeRunnerClient{
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{
				StateRunning: true,
				Containers: []*runnerv1.WorkloadContainer{
					{Name: "sandbox", Role: runnerv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING},
				},
			}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id != runnerID {
			return nil, errors.New("unexpected runner id")
		}
		return runner, nil
	}}
	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
		Agents: &testutil.FakeAgentsClient{UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			if req.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING || req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected runtime update")
			}
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		}},
		Assembler: newTestAssembler(uuid.New(), false),
	})
	sandbox := &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: sandboxID, CreatedAt: createdAt}, OrganizationId: testOrganizationID, OwnerId: ownerID, Status: agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING}

	if err := reconciler.reconcileSandbox(ctx, sandbox, time.Now().UTC()); err != nil {
		t.Fatalf("reconcile sandbox: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected workload update")
	}
	if updateReq.GetId() != workloadID {
		t.Fatalf("unexpected workload id: %s", updateReq.GetId())
	}
	if updateReq.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("unexpected status: %v", updateReq.GetStatus())
	}
	if len(updateReq.GetContainers()) != 1 || updateReq.GetContainers()[0].GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
		t.Fatalf("expected running container update")
	}
}

func TestStartSandboxWorkloadWritesRuntimeStartingUntilObservedReady(t *testing.T) {
	f, plan := preparedSandboxAssemblyFixture(t, false, true)
	if err := f.r.startSandboxWorkload(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if len(f.runtimeUpdates) != 1 || f.runtimeUpdates[0].Id != plan.sandboxID.String() ||
		f.runtimeUpdates[0].GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING || f.runtimeUpdates[0].GetWorkloadId() == "" || f.activations != 1 {
		t.Fatal("prepared sandbox startup lost runtime state")
	}
}

func TestReconcileSandboxIdleStopClearsRuntimeWorkload(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	sandboxID := uuid.NewString()
	ownerID := uuid.NewString()
	workloadID := uuid.NewString()
	activeAt := timestamppb.New(time.Now().Add(-2 * time.Hour))
	runtimeWorkloadID := workloadID
	var runtimeReq *agentsv1.UpdateSandboxRuntimeStateRequest
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, OrganizationId: testOrganizationID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, InstanceId: stringPtr(workloadID), LastActivityAt: activeAt, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID}}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			return acknowledgedWorkloadUpdate(req), nil
		},
	}
	runner := &fakeRunnerClient{inspectWorkload: absentRunnerWorkload, stopWorkload: func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
		return &runnerv1.StopWorkloadResponse{}, nil
	}}
	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Runners:      runners,
		Agents: &testutil.FakeAgentsClient{UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			runtimeReq = req
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		}},
		Assembler: newTestAssembler(uuid.New(), false),
	})
	sandbox := &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: sandboxID}, OrganizationId: testOrganizationID, OwnerId: ownerID, Status: agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING, IdleTimeout: "1h", WorkloadId: &runtimeWorkloadID}

	if err := reconciler.reconcileSandbox(ctx, sandbox, time.Now().UTC()); err != nil {
		t.Fatalf("reconcile sandbox: %v", err)
	}
	if runtimeReq == nil || runtimeReq.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED || !runtimeReq.GetClearWorkloadId() {
		t.Fatalf("unexpected runtime update: %v", runtimeReq)
	}
}

func TestStartSandboxWorkloadFailureWritesRuntimeFailed(t *testing.T) {
	f, plan := preparedSandboxAssemblyFixture(t, false, false)
	previousID := uuid.NewString()
	plan.sandbox.WorkloadId = &previousID
	startErr := errors.New("runner prepare failed")
	calls := 0
	f.native.prepareWorkload = func(context.Context, *runnerv1.PrepareWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
		calls++
		return nil, startErr
	}
	if err := f.r.startSandboxWorkloadAttempt(context.Background(), plan); !errors.Is(err, startErr) {
		t.Fatalf("lost native failure: %v", err)
	}
	if calls != 1 || len(f.runtimeUpdates) != 1 || f.runtimeUpdates[0].GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED ||
		!f.runtimeUpdates[0].GetClearWorkloadId() || f.activations != 0 || f.removals != 0 || f.w.RemovalConfirmedAt != nil {
		t.Fatal("sandbox failure released uncertain compute or lost runtime failure")
	}
}

func TestReconcileSandboxStartsFromStartingRuntimeState(t *testing.T) {
	f, plan := preparedSandboxAssemblyFixture(t, false, false)
	if err := f.r.reconcileSandbox(context.Background(), plan.sandbox, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if f.prepares != 1 || f.activations != 1 || len(f.runtimeUpdates) != 1 || f.runtimeUpdates[0].GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING ||
		f.runtimeUpdates[0].GetWorkloadId() == "" {
		t.Fatal("sandbox did not start through prepared lifecycle")
	}
	if err := f.r.reconcileSandbox(context.Background(), plan.sandbox, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if f.prepares != 1 || f.activations != 1 || f.runtimeUpdates[len(f.runtimeUpdates)-1].GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING {
		t.Fatal("sandbox reconciliation restarted work or missed readiness")
	}
}

func TestReconcileSandboxesContinuesAfterRuntimeUpdateFailure(t *testing.T) {
	ctx := context.Background()
	firstSandboxID := uuid.NewString()
	secondSandboxID := uuid.NewString()
	staleWorkloadID := uuid.NewString()
	calls := 0
	agents := &testutil.FakeAgentsClient{
		ListSandboxesFunc: func(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error) {
			return &agentsv1.ListSandboxesResponse{Sandboxes: []*agentsv1.Sandbox{
				{Meta: &agentsv1.EntityMeta{Id: firstSandboxID}, OrganizationId: testOrganizationID, Status: agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, WorkloadId: &staleWorkloadID},
				{Meta: &agentsv1.EntityMeta{Id: secondSandboxID}, OrganizationId: testOrganizationID, Status: agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, WorkloadId: &staleWorkloadID},
			}}, nil
		},
		UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			calls++
			if req.GetId() == firstSandboxID {
				return nil, errors.New("agents runtime update failed")
			}
			if req.GetId() != secondSandboxID || req.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED || !req.GetClearWorkloadId() {
				return nil, errors.New("unexpected runtime update")
			}
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		},
	}
	reconciler := newTestReconciler(Config{
		Agents: agents,
		Runners: &fakeRunnersClient{
			listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
				return &runnersv1.ListWorkloadsResponse{}, nil
			},
			listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
				return &runnersv1.ListVolumesResponse{}, nil
			},
		},
	})

	if err := reconciler.reconcileSandboxes(ctx); err != nil {
		t.Fatalf("reconcile sandboxes: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected both runtime updates, got %d", calls)
	}
}

func TestReconcileSandboxTTLDeletesAfterClearingRuntimeWorkload(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	sandboxID := uuid.NewString()
	workloadID := uuid.NewString()
	var runtimeReq *agentsv1.UpdateSandboxRuntimeStateRequest
	deleteCalled := false
	agents := &testutil.FakeAgentsClient{
		UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			runtimeReq = req
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		},
		DeleteSandboxFunc: func(_ context.Context, req *agentsv1.DeleteSandboxRequest, _ ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
			deleteCalled = true
			if runtimeReq == nil {
				return nil, errors.New("runtime state must clear before delete")
			}
			if req.GetId() != sandboxID {
				return nil, errors.New("unexpected sandbox delete id")
			}
			return &agentsv1.DeleteSandboxResponse{}, nil
		},
	}
	reconciler := newTestReconciler(Config{
		Agents: agents,
		Runners: &fakeRunnersClient{
			listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
				return &runnersv1.ListWorkloadsResponse{}, nil
			},
			listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
				return &runnersv1.ListVolumesResponse{}, nil
			},
		},
	})
	sandbox := &agentsv1.Sandbox{
		Meta:           &agentsv1.EntityMeta{Id: sandboxID, CreatedAt: timestamppb.New(now.Add(-2 * time.Hour))},
		OrganizationId: testOrganizationID,
		Status:         agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING,
		Ttl:            "1h",
		WorkloadId:     &workloadID,
	}

	if err := reconciler.reconcileSandbox(ctx, sandbox, now); err != nil {
		t.Fatalf("reconcile sandbox: %v", err)
	}
	if runtimeReq == nil {
		t.Fatal("expected runtime state clear")
	}
	if runtimeReq.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED || !runtimeReq.GetClearWorkloadId() {
		t.Fatalf("unexpected runtime update: %v", runtimeReq)
	}
	if !deleteCalled {
		t.Fatal("expected sandbox delete")
	}
}

func TestReconcileSandboxTTLDoesNotDeleteWhenRuntimeClearFails(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	sandboxID := uuid.NewString()
	workloadID := uuid.NewString()
	deleteCalled := false
	agents := &testutil.FakeAgentsClient{
		UpdateSandboxRuntimeStateFunc: func(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			return nil, errors.New("runtime clear failed")
		},
		DeleteSandboxFunc: func(context.Context, *agentsv1.DeleteSandboxRequest, ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
			deleteCalled = true
			return &agentsv1.DeleteSandboxResponse{}, nil
		},
	}
	reconciler := newTestReconciler(Config{
		Agents: agents,
		Runners: &fakeRunnersClient{
			listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
				return &runnersv1.ListWorkloadsResponse{}, nil
			},
			listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
				return &runnersv1.ListVolumesResponse{}, nil
			},
		},
	})
	sandbox := &agentsv1.Sandbox{
		Meta:           &agentsv1.EntityMeta{Id: sandboxID, CreatedAt: timestamppb.New(now.Add(-2 * time.Hour))},
		OrganizationId: testOrganizationID,
		Status:         agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING,
		Ttl:            "1h",
		WorkloadId:     &workloadID,
	}

	if err := reconciler.reconcileSandbox(ctx, sandbox, now); err == nil {
		t.Fatal("expected runtime clear failure")
	}
	if deleteCalled {
		t.Fatal("delete must not run after runtime clear failure")
	}
}

func TestReconcileSandboxStoppedStopsActiveWorkloadWhenNotIdle(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	sandboxID := uuid.NewString()
	ownerID := uuid.NewString()
	workloadID := uuid.NewString()
	runtimeWorkloadID := workloadID
	// An attached session keeps the workload far from its idle timeout.
	activeAt := timestamppb.New(time.Now().Add(-time.Second))
	var runtimeReq *agentsv1.UpdateSandboxRuntimeStateRequest
	var workloadStatuses []runnersv1.WorkloadStatus
	stoppedInstanceID := ""
	runners := &fakeRunnersClient{
		listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{{
				Meta:           &runnersv1.EntityMeta{Id: workloadID},
				RunnerId:       runnerID,
				OrganizationId: testOrganizationID,
				Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				InstanceId:     stringPtr(workloadID),
				LastActivityAt: activeAt,
				OwnerKind:      runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
				OwnerId:        sandboxID,
			}}}, nil
		},
		listVolumes: func(context.Context, *runnersv1.ListVolumesRequest, ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{}, nil
		},
		updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
			if req.Status != nil {
				workloadStatuses = append(workloadStatuses, req.GetStatus())
			}
			return acknowledgedWorkloadUpdate(req), nil
		},
	}
	runner := &fakeRunnerClient{inspectWorkload: absentRunnerWorkload, stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
		stoppedInstanceID = req.GetWorkloadId()
		return &runnerv1.StopWorkloadResponse{}, nil
	}}
	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Runners:      runners,
		Agents: &testutil.FakeAgentsClient{UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			runtimeReq = req
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		}},
		Assembler: newTestAssembler(uuid.New(), false),
	})
	sandbox := &agentsv1.Sandbox{
		Meta:           &agentsv1.EntityMeta{Id: sandboxID},
		OrganizationId: testOrganizationID,
		OwnerId:        ownerID,
		Status:         agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED,
		IdleTimeout:    "1h",
		WorkloadId:     &runtimeWorkloadID,
	}

	if err := reconciler.reconcileSandbox(ctx, sandbox, time.Now().UTC()); err != nil {
		t.Fatalf("reconcile sandbox: %v", err)
	}
	if stoppedInstanceID != workloadID {
		t.Fatalf("expected sandbox workload stop on runner, got %q", stoppedInstanceID)
	}
	expectedStatuses := []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
	}
	if !reflect.DeepEqual(workloadStatuses, expectedStatuses) {
		t.Fatalf("unexpected workload status transitions: %v", workloadStatuses)
	}
	if runtimeReq == nil || runtimeReq.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED || !runtimeReq.GetClearWorkloadId() {
		t.Fatalf("unexpected runtime update: %v", runtimeReq)
	}
}

func TestReconcileVolumesActivatesSandboxWorkspaceVolume(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	sandboxVolumeKey := uuid.NewString()
	sandboxID := uuid.NewString()
	instanceID := "sandbox-volume-instance"
	volume := checkedTestSandboxVolume(sandboxVolumeKey, sandboxID, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)

	var updateReq *runnersv1.UpdateVolumeCheckedRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				volume,
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			updateReq = req
			return checkedTestUpdate(t, volume, req), nil
		},
	}
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{
				checkedTestInstance(volume, instanceID, "sandbox-workspace-uid"),
			}}, nil
		},
		removeVolumeBound: func(_ context.Context, _ *runnerv1.RemoveVolumeBoundRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
			t.Fatal("sandbox workspace volume must not be removed")
			return nil, errors.New("sandbox workspace volume must not be removed")
		},
	}
	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Runners:      runners,
		Agents: &testutil.FakeAgentsClient{GetSandboxFunc: func(_ context.Context, req *agentsv1.GetSandboxRequest, _ ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
			if req.GetId() != sandboxID {
				t.Fatalf("unexpected sandbox lookup: %s", req.GetId())
			}
			return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{
				Meta: &agentsv1.EntityMeta{Id: sandboxID}, OrganizationId: testOrganizationID, OwnerId: "fixture-sandbox-user",
			}}, nil
		}},
		Assembler: newTestAssembler(uuid.New(), false),
	})

	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if updateReq == nil {
		t.Fatal("expected sandbox workspace volume update")
	}
	if updateReq.GetId() != sandboxVolumeKey {
		t.Fatalf("unexpected volume update id: %q", updateReq.GetId())
	}
	if updateReq.GetBind() == nil || volume.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("unexpected binding: %v", updateReq)
	}
	if updateReq.GetBind().GetInstance().GetInstanceId() != instanceID {
		t.Fatalf("unexpected instance id: %q", updateReq.GetBind().GetInstance().GetInstanceId())
	}
}

func TestReconcileVolumesKeepsActiveSandboxWorkspaceVolume(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	sandboxVolumeKey := uuid.NewString()
	sandboxID := uuid.NewString()
	instanceID := "sandbox-volume-instance"
	volume := checkedTestSandboxVolume(sandboxVolumeKey, sandboxID, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	volume.BoundInstance.InstanceId, volume.InstanceId = instanceID, stringPtr(instanceID)

	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				volume,
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			t.Fatalf("unexpected volume update: %v", req)
			return nil, fmt.Errorf("unexpected volume update: %v", req)
		},
	}
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{
				volume.BoundInstance,
			}}, nil
		},
		removeVolumeBound: func(_ context.Context, _ *runnerv1.RemoveVolumeBoundRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeBoundResponse, error) {
			t.Fatal("sandbox workspace volume must survive idle stops")
			return nil, errors.New("sandbox workspace volume must survive idle stops")
		},
	}
	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Runners:      runners,
		Agents: &testutil.FakeAgentsClient{UpdateSandboxRuntimeStateFunc: func(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			t.Fatal("sandbox must not be failed while its workspace exists")
			return nil, errors.New("sandbox must not be failed while its workspace exists")
		}},
		Assembler: newTestAssembler(uuid.New(), false),
	})

	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
}

func TestReconcileVolumesFailsSandboxWhenWorkspaceVolumeLost(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	sandboxVolumeKey := uuid.NewString()
	sandboxID := uuid.NewString()

	var runtimeReq *agentsv1.UpdateSandboxRuntimeStateRequest
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, _ *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{
				{Meta: &runnersv1.EntityMeta{Id: sandboxVolumeKey}, RunnerId: runnerID, OrganizationId: testOrganizationID, Status: runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: sandboxID},
			}}, nil
		},
		listRunners: func(_ context.Context, _ *runnersv1.ListRunnersRequest, _ ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner(runnerID)}}, nil
		},
	}
	runner := &fakeRunnerClient{
		listVolumes: func(_ context.Context, _ *runnerv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend}, nil
		},
	}
	degradeCalled := false
	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
		Runners:      runners,
		Threads: &fakeThreadsClient{degradeThread: func(context.Context, *threadsv1.DegradeThreadRequest, ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
			degradeCalled = true
			return &threadsv1.DegradeThreadResponse{}, nil
		}},
		Agents: &testutil.FakeAgentsClient{UpdateSandboxRuntimeStateFunc: func(_ context.Context, req *agentsv1.UpdateSandboxRuntimeStateRequest, _ ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			runtimeReq = req
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		}},
		Assembler: newTestAssembler(uuid.New(), false),
	})

	if err := reconciler.reconcileVolumes(ctx); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
	if runtimeReq == nil {
		t.Fatal("expected sandbox runtime state update")
	}
	if runtimeReq.GetId() != sandboxID || runtimeReq.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED || !runtimeReq.GetClearWorkloadId() {
		t.Fatalf("unexpected runtime update: %v", runtimeReq)
	}
	if degradeCalled {
		t.Fatal("sandbox volumes must not degrade a thread")
	}
}

func TestReconcileOrphanIdentitiesSweepsSandboxIdentities(t *testing.T) {
	ctx := context.Background()
	activeAgentID := "active-agent-identity"
	activeSandboxID := "active-sandbox-identity"
	orphanSandboxID := "orphan-sandbox-identity"
	startingSandboxID := "starting-sandbox-identity"

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			if len(req.GetFilter().GetOwnerKindIn()) == 1 && req.GetFilter().GetOwnerKindIn()[0] == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
				return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
					{Meta: &runnersv1.EntityMeta{Id: "sandbox-workload-1"}, OrganizationId: testOrganizationID, ZitiIdentityId: activeSandboxID, OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, OwnerId: uuid.NewString()},
				}}, nil
			}
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: "workload-1"}, OrganizationId: testOrganizationID, ZitiIdentityId: activeAgentID},
			}}, nil
		},
	}

	deleteCalls := []string{}
	zitiMgmt := &fakeZitiMgmtClient{
		listManagedIdentities: func(_ context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, _ ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
			switch req.GetIdentityType() {
			case identityv1.IdentityType_IDENTITY_TYPE_AGENT:
				return &zitimgmtv1.ListManagedIdentitiesResponse{Identities: []*zitimgmtv1.ManagedIdentity{
					{ZitiIdentityId: activeAgentID},
				}}, nil
			case identityv1.IdentityType_IDENTITY_TYPE_SANDBOX:
				return &zitimgmtv1.ListManagedIdentitiesResponse{Identities: []*zitimgmtv1.ManagedIdentity{
					{ZitiIdentityId: activeSandboxID},
					{ZitiIdentityId: orphanSandboxID, CreatedAt: timestamppb.New(time.Now().Add(-time.Hour))},
					// Minted moments ago for a workload whose record is not written yet.
					{ZitiIdentityId: startingSandboxID, CreatedAt: timestamppb.New(time.Now())},
				}}, nil
			default:
				return nil, errors.New("unexpected identity type")
			}
		},
		deleteIdentity: func(_ context.Context, req *zitimgmtv1.DeleteIdentityRequest, _ ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
			deleteCalls = append(deleteCalls, req.GetZitiIdentityId())
			return &zitimgmtv1.DeleteIdentityResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: &fakeRunnerDialer{},
		ZitiMgmt:     zitiMgmt,
		Runners:      runners,
		Assembler:    newTestAssembler(uuid.New(), true),
	})
	if err := reconciler.reconcileOrphanIdentities(ctx); err != nil {
		t.Fatalf("reconcile orphan identities: %v", err)
	}
	if !reflect.DeepEqual(deleteCalls, []string{orphanSandboxID}) {
		t.Fatalf("unexpected delete calls: %v", deleteCalls)
	}
}

func TestSandboxReconcileLoopRunsOnWake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cycles := make(chan struct{}, 4)
	agents := &testutil.FakeAgentsClient{
		ListSandboxesFunc: func(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error) {
			cycles <- struct{}{}
			return &agentsv1.ListSandboxesResponse{}, nil
		},
	}
	wake := make(chan struct{}, 1)
	reconciler := newTestReconciler(Config{
		Agents:      agents,
		SandboxWake: wake,
		// Long enough that only the wake can trigger the second cycle.
		WorkloadReconcileInterval: time.Hour,
	})
	go reconciler.runSandboxReconcileLoop(ctx)

	waitForSandboxCycle(t, cycles)
	wake <- struct{}{}
	waitForSandboxCycle(t, cycles)
}

func waitForSandboxCycle(t *testing.T, cycles <-chan struct{}) {
	t.Helper()
	select {
	case <-cycles:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sandbox reconcile cycle")
	}
}
