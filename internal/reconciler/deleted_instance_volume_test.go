package reconciler

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type deletedInstanceFixture struct {
	*volumeRetentionFixture
	volume        *runnersv1.Volume
	instance      *agentsv1.AgentInstance
	instanceErr   error
	workloads     []*runnersv1.Workload
	instanceReads []string
}

func newDeletedInstanceFixture(t *testing.T) *deletedInstanceFixture {
	t.Helper()
	f := &deletedInstanceFixture{volumeRetentionFixture: newVolumeRetentionFixture(t)}
	f.volume = f.volumeRetentionFixture.volume("workspace", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	f.records = []*runnersv1.Volume{f.volume}
	f.inventory = &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend, Volumes: []*runnerv1.VolumeListItem{f.volume.BoundInstance}}
	f.instance = &agentsv1.AgentInstance{
		Meta: &agentsv1.EntityMeta{Id: f.volume.GetOwnerId()}, AgentId: f.volume.GetAgentId(), OrganizationId: f.volume.GetOrganizationId(),
		State: agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_TERMINATED,
	}
	removed := timestamppb.New(time.Now().Add(-time.Minute))
	f.workloads = []*runnersv1.Workload{{Meta: &runnersv1.EntityMeta{Id: "done"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, RemovedAt: removed, RemovalConfirmedAt: removed}}
	f.reconciler.agents.(*testutil.FakeAgentsClient).GetInstanceFunc = func(_ context.Context, req *agentsv1.GetInstanceRequest, _ ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
		f.instanceReads = append(f.instanceReads, req.GetId())
		if f.instanceErr != nil {
			return nil, f.instanceErr
		}
		return &agentsv1.GetInstanceResponse{Instance: f.instance}, nil
	}
	f.reconciler.runners.(*fakeRunnersClient).listWorkloadsByAgentInstance = func(_ context.Context, req *runnersv1.ListWorkloadsByAgentInstanceRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
		if req.GetAgentInstanceId() != f.volume.GetOwnerId() {
			t.Fatalf("listed workloads of %q, want the volume owner", req.GetAgentInstanceId())
		}
		return &runnersv1.ListWorkloadsByAgentInstanceResponse{Workloads: f.workloads}, nil
	}
	return f
}

func (f *deletedInstanceFixture) assertRetained(t *testing.T) {
	t.Helper()
	if len(f.removed) != 0 || len(f.updated) != 0 || f.volume.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("volume must be retained: removed %v, updates %v, status %v", f.removed, f.updated, f.volume.GetStatus())
	}
}

func TestReconcileVolumesRemovesDeletedInstanceWorkspace(t *testing.T) {
	f := newDeletedInstanceFixture(t)
	f.reconcile(t)
	if !slices.Equal(f.instanceReads, []string{f.volume.GetOwnerId()}) {
		t.Fatalf("expected one owner read, got %v", f.instanceReads)
	}
	if !slices.Equal(f.removed, []string{"pvc-workspace"}) {
		t.Fatalf("deleted instance's workspace must be removed, removed %v", f.removed)
	}
	if len(f.updated) != 1 || f.updated[0].GetBeginRemoval() == nil || f.volume.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
		t.Fatalf("removal must go through the checked intent, updates %v", f.updated)
	}
}

func TestReconcileVolumesRetainsLiveInstanceWorkspace(t *testing.T) {
	for _, state := range []agentsv1.AgentInstanceState{
		agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE,
		agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED,
		agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_UNSPECIFIED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			f := newDeletedInstanceFixture(t)
			f.instance.State = state
			f.reconcile(t)
			f.assertRetained(t)
		})
	}
}

func TestReconcileVolumesRetainsDeletedInstanceWorkspaceWhileHeld(t *testing.T) {
	removed := timestamppb.Now()
	for name, workload := range map[string]*runnersv1.Workload{
		"unconfirmed_removal": {Meta: &runnersv1.EntityMeta{Id: "held"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, RemovedAt: removed},
		"running":             {Meta: &runnersv1.EntityMeta{Id: "held"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
		"stopping_confirmed":  {Meta: &runnersv1.EntityMeta{Id: "held"}, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, RemovalConfirmedAt: removed},
	} {
		t.Run(name, func(t *testing.T) {
			f := newDeletedInstanceFixture(t)
			f.workloads = append(f.workloads, workload)
			f.reconcile(t)
			f.assertRetained(t)
		})
	}
}

func TestReconcileVolumesRetainsWorkspaceWithoutOwnerEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*deletedInstanceFixture)
	}{
		{"not_found", func(f *deletedInstanceFixture) {
			f.instanceErr = status.Error(codes.NotFound, "agent instance not found")
		}},
		{"read_failure", func(f *deletedInstanceFixture) { f.instanceErr = errors.New("unavailable") }},
		{"other_instance", func(f *deletedInstanceFixture) { f.instance.Meta.Id = uuid.NewString() }},
		{"other_organization", func(f *deletedInstanceFixture) { f.instance.OrganizationId = uuid.NewString() }},
		{"other_agent_class", func(f *deletedInstanceFixture) { f.instance.AgentId = uuid.NewString() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeletedInstanceFixture(t)
			tc.mutate(f)
			f.reconcile(t)
			f.assertRetained(t)
		})
	}
}

func TestReconcileVolumesNeverReadsInstanceForUnownedVolume(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runnersv1.Volume)
	}{
		{"sandbox", func(v *runnersv1.Volume) {
			v.OwnerKind, v.OwnerId, v.AgentId = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, uuid.NewString(), ""
		}},
		{"class_only", func(v *runnersv1.Volume) {
			v.OwnerKind, v.OwnerId, v.AgentInstanceId = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_UNSPECIFIED, "", nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := checkedTestVolume("workspace", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
			tc.mutate(v)
			r := newTestReconciler(Config{Agents: &testutil.FakeAgentsClient{GetInstanceFunc: func(context.Context, *agentsv1.GetInstanceRequest, ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
				t.Fatal("an unowned volume must not be matched to an instance")
				return nil, nil
			}}})
			released, err := r.deletedInstanceReleasesVolume(context.Background(), v, map[string]instanceActivity{})
			if err != nil || released {
				t.Fatalf("released=%t err=%v", released, err)
			}
		})
	}
}

func TestVolumeOwnerInstanceIDPrefersInstanceOwner(t *testing.T) {
	owner, legacy := uuid.NewString(), uuid.NewString()
	v := &runnersv1.Volume{OwnerKind: runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE, OwnerId: owner, AgentInstanceId: &legacy, AgentId: uuid.NewString()}
	if got := volumeOwnerInstanceID(v); got != owner {
		t.Fatalf("owner_id must win, got %q", got)
	}
	v.OwnerId = ""
	if got := volumeOwnerInstanceID(v); got != legacy {
		t.Fatalf("agent_instance_id must cover pre-owner rows, got %q", got)
	}
	v.AgentInstanceId = nil
	if got := volumeOwnerInstanceID(v); got != "" {
		t.Fatalf("an agent class is never an instance owner, got %q", got)
	}
}
