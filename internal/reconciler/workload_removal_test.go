package reconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func absentRunnerWorkload(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
	return nil, status.Error(codes.NotFound, "removed")
}

func TestStopWorkloadRequiresConfirmedRemoval(t *testing.T) {
	for _, initial := range []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED} {
		t.Run(initial.String(), func(t *testing.T) {
			workloadID := uuid.NewString()
			workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: "runner", ZitiIdentityId: "identity", Status: initial}
			phase := "present"
			removals, deletes, stops := 0, 0, 0
			runner := &fakeRunnerClient{
				stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
					stops++
					if req.GetWorkloadId() != workloadID || req.GetTimeoutSec() != 5 {
						t.Fatalf("must use persisted ID after a lost start reply, with configured grace: %v", req)
					}
					return &runnerv1.StopWorkloadResponse{}, nil
				},
				inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
					switch phase {
					case "present":
						// A stopped main container is insufficient: the pod and its
						// sidecars can still exist, even with state_running false.
						return &runnerv1.InspectWorkloadResponse{Id: req.GetWorkloadId(), StateRunning: false}, nil
					case "unavailable":
						return nil, status.Error(codes.Unavailable, "runner disconnected")
					case "alias present":
						if req.GetWorkloadId() == runnerWorkloadPrefix+workloadID {
							return &runnerv1.InspectWorkloadResponse{Id: req.GetWorkloadId()}, nil
						}
					}
					return absentRunnerWorkload(context.Background(), req)
				},
			}
			r := newTestReconciler(Config{StopSec: 5,
				RunnerDialer: &fakeRunnerDialer{dial: func(context.Context, string) (runnerv1.RunnerServiceClient, error) { return runner, nil }},
				Runners: &fakeRunnersClient{updateWorkload: func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
					if req.GetRemovedAt() != nil {
						if phase != "gone" && phase != "store unavailable" {
							t.Fatal("removal written before physical confirmation")
						}
						if phase == "store unavailable" {
							return nil, errors.New("store unavailable")
						}
						removals++
						want := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
						if initial == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
							want = initial
						}
						if req.GetStatus() != want {
							t.Fatalf("terminal status %v, want %v", req.GetStatus(), want)
						}
					}
					return &runnersv1.UpdateWorkloadResponse{}, nil
				}},
				ZitiMgmt: &fakeZitiMgmtClient{deleteIdentity: func(context.Context, *zitimgmtv1.DeleteIdentityRequest, ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
					if phase != "gone" || removals == 0 {
						t.Fatal("identity cleaned up before confirmed, persisted removal")
					}
					deletes++
					return &zitimgmtv1.DeleteIdentityResponse{}, nil
				}},
			})
			for _, next := range []string{"present", "unavailable", "alias present", "store unavailable"} {
				phase = next
				if err := r.stopWorkloadWithContext(context.Background(), workload); err == nil {
					t.Fatalf("%s: expected pending/error", phase)
				}
				if workload.GetRemovedAt() != nil || removals != 0 || deletes != 0 {
					t.Fatalf("%s: workload incorrectly settled", phase)
				}
			}
			phase = "gone"
			if err := r.stopWorkloadWithContext(context.Background(), workload); err != nil {
				t.Fatal(err)
			}
			if removals != 1 || deletes != 1 || stops != 5 || workload.GetRemovedAt() == nil {
				t.Fatalf("unexpected settlement: removals=%d deletes=%d stops=%d", removals, deletes, stops)
			}
		})
	}
}

func TestMissingWorkloadMustBeAbsentUnderBothIDs(t *testing.T) {
	requestedID, returnedID := uuid.NewString(), uuid.NewString()
	workload := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: requestedID}, InstanceId: &returnedID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING}
	r := newTestReconciler(Config{Runners: &fakeRunnersClient{updateWorkload: func(context.Context, *runnersv1.UpdateWorkloadRequest, ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		t.Fatal("listed absence must not override a successful inspect")
		return nil, nil
	}}})
	runner := &fakeRunnerClient{inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
		if req.GetWorkloadId() == requestedID {
			return &runnerv1.InspectWorkloadResponse{Id: requestedID}, nil
		}
		return absentRunnerWorkload(context.Background(), req)
	}}
	if err := r.handleMissingRunnerWorkload(context.Background(), runner, workload); err == nil {
		t.Fatal("requested workload still exists")
	}
}

func TestFailedUnremovedWorkloadsRemainTracked(t *testing.T) {
	now := time.Now()
	agentID, instanceID := uuid.MustParse(testAgentID), uuid.New()
	pending := makeWorkload(agentID, instanceID, now, &now)
	pending.OrganizationId = testOrganizationID
	pending.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	pending.RunnerId = "runner"
	removed := makeWorkload(agentID, instanceID, now, &now)
	removed.OrganizationId = testOrganizationID
	removed.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	removed.RemovedAt = timestamppb.Now()
	r := newTestReconciler(Config{Runners: &fakeRunnersClient{listWorkloads: func(_ context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
		found := false
		for _, state := range req.GetFilter().GetStatusIn() {
			found = found || state == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		}
		if !found {
			t.Fatal("failed workloads omitted from reconciliation query")
		}
		return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{pending, removed}}, nil
	}}})
	actual, err := r.fetchActual(context.Background())
	if err != nil || len(actual) != 1 || actual[0] != pending {
		t.Fatalf("unexpected actual workloads: %v, %v", actual, err)
	}
	actions, err := ComputeActions([]AgentInstanceTarget{{AgentID: agentID, AgentInstanceID: instanceID}}, actual, nil, time.Hour, now)
	if err != nil || len(actions.ToStart) != 0 || len(actions.ToStop) != 1 || actions.ToStop[0] != pending {
		t.Fatalf("failed workload must stop before replacement: %v, %v", actions, err)
	}
}

func TestVolumeTTLWaitsForEveryWorkloadRemoval(t *testing.T) {
	for _, state := range []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED} {
		t.Run(state.String(), func(t *testing.T) {
			instanceID := uuid.NewString()
			old := timestamppb.New(time.Now().Add(-2 * time.Hour))
			pending := &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: "pending", UpdatedAt: old}, Status: state}
			r := newTestReconciler(Config{Runners: &fakeRunnersClient{listWorkloadsByAgentInstance: func(context.Context, *runnersv1.ListWorkloadsByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
				return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: []*runnersv1.Workload{
					{Meta: &runnersv1.EntityMeta{Id: "old"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, RemovedAt: old}, pending,
				}}, nil
			}}})
			ttl := time.Hour
			volume := &runnersv1.Volume{VolumeId: "definition", AgentInstanceId: &instanceID}
			info := map[string]volumeTTLInfo{"definition": {persistent: true, ttl: &ttl}}
			for _, stage := range []string{"unconfirmed", "just removed", "retention elapsed"} {
				if stage == "just removed" {
					pending.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
					pending.RemovedAt = timestamppb.Now()
				} else if stage == "retention elapsed" {
					pending.RemovedAt = old
				}
				expired, err := r.volumeTTLExpired(context.Background(), volume, info, map[string]instanceActivity{})
				if err != nil || expired != (stage == "retention elapsed") {
					t.Fatalf("%s: expired=%t err=%v", stage, expired, err)
				}
			}
		})
	}
}
