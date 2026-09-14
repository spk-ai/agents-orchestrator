package reconciler

import (
	"context"
	"slices"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestReconcileVolumesRetainsUntrackedInventory(t *testing.T) {
	for _, name := range []string{
		"foreign_agent_organization",
		"sandbox_only_organization",
		"record_created_between_scans",
		"deleted_record_excluded_by_filter",
		"failed_record_excluded_by_filter",
		"no_record",
	} {
		t.Run(name, func(t *testing.T) {
			f := newVolumeRetentionFixture(t)
			anchor := f.volume("anchor", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
			candidate := f.volume("candidate", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
			f.records = []*runnersv1.Volume{anchor}
			f.inventory = &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{
				f.item("anchor"), f.item("candidate"),
			}}
			switch name {
			case "foreign_agent_organization", "sandbox_only_organization":
				candidate.OrganizationId = uuid.NewString()
				candidate.Status = runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE
				if name == "sandbox_only_organization" {
					candidate.OwnerKind = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX
					candidate.AgentId = ""
					candidate.AgentInstanceId = nil
				}
				f.records = append(f.records, candidate)
			case "record_created_between_scans":
				f.beforeRunnerList = func() {
					if f.registryLists != 1 {
						t.Fatalf("creation must follow the initial registry snapshot, got %d reads", f.registryLists)
					}
					f.records = append(f.records, candidate)
					f.beforeRunnerList = nil
				}
			case "deleted_record_excluded_by_filter":
				candidate.Status = runnersv1.VolumeStatus_VOLUME_STATUS_DELETED
				f.records = append(f.records, candidate)
			case "failed_record_excluded_by_filter":
				candidate.Status = runnersv1.VolumeStatus_VOLUME_STATUS_FAILED
				f.records = append(f.records, candidate)
			}
			before := proto.Clone(candidate)
			f.reconcile(t)
			if len(f.removed) != 0 {
				t.Errorf("untracked disks must be retained, removed %v", f.removed)
			}
			if len(f.updated) != 1 || f.updated[0].GetId() != "anchor" || anchor.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
				t.Fatalf("tracked provisioning must still progress, updates %v", f.updated)
			}
			if !proto.Equal(candidate, before) {
				t.Fatal("untracked volume record was changed")
			}
			if name == "record_created_between_scans" {
				f.reconcile(t)
				if len(f.removed) != 0 || candidate.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || candidate.GetInstanceId() != "pvc-candidate" {
					t.Fatalf("next scan must reconcile the retained disk: status %v, instance %q, removed %v", candidate.GetStatus(), candidate.GetInstanceId(), f.removed)
				}
			}
		})
	}
}

func TestReconcileVolumesRejectsInvalidInventory(t *testing.T) {
	valid := checkedTestInstance(checkedTestVolume("tracked", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING), "pvc-tracked", "uid-tracked")
	for _, tc := range []struct {
		name      string
		inventory *runnerv1.ListVolumesResponse
	}{
		{"nil_response", nil},
		{"nil_item", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{nil}}},
		{"missing_key", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{{InstanceId: "pvc-tracked"}}}},
		{"blank_key", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{{VolumeKey: " \t", InstanceId: "pvc-tracked"}}}},
		{"padded_key", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{{VolumeKey: "tracked ", InstanceId: "pvc-tracked"}}}},
		{"missing_instance", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{{VolumeKey: "tracked"}}}},
		{"blank_instance", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{{VolumeKey: "tracked", InstanceId: " \t"}}}},
		{"padded_instance", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{{VolumeKey: "tracked", InstanceId: "pvc-tracked "}}}},
		{"duplicate_key", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{valid, {VolumeKey: "tracked", InstanceId: "pvc-other"}}}},
		{"duplicate_instance", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{valid, {VolumeKey: "other", InstanceId: "pvc-tracked"}}}},
		{"repeated_item", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{valid, valid}}},
		{"invalid_after_valid", &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{valid, nil}}},
	} {
		for _, state := range []runnersv1.VolumeStatus{
			runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
			runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
			runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING,
		} {
			t.Run(tc.name+"/"+state.String(), func(t *testing.T) {
				f := newVolumeRetentionFixture(t)
				tracked := f.volume("tracked", state)
				f.records = []*runnersv1.Volume{tracked}
				f.inventory = tc.inventory
				f.reconcile(t)
				if len(f.removed) != 0 || len(f.updated) != 0 || f.ownerMutations != 0 {
					t.Fatalf("invalid inventory must not mutate any state: removed %v, updated %v, owner mutations %d", f.removed, f.updated, f.ownerMutations)
				}
				if f.runnerLists != 1 {
					t.Fatalf("expected one inspected inventory, got %d", f.runnerLists)
				}
			})
		}
	}
}

func TestReconcileVolumesRemovesOnlyTrackedDeprovisioningDisk(t *testing.T) {
	f := newVolumeRetentionFixture(t)
	f.records = []*runnersv1.Volume{f.volume("tracked", runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING)}
	f.inventory = &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{
		f.item("tracked"), f.item("untracked"),
	}}
	f.reconcile(t)
	if !slices.Equal(f.removed, []string{"pvc-tracked"}) {
		t.Fatalf("only the tracked deletion may proceed, removed %v", f.removed)
	}
	if len(f.updated) != 1 || f.updated[0].GetBeginRemoval() == nil || f.records[0].GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING {
		t.Fatalf("remove acknowledgement must retain the committed pending intent, updates %v", f.updated)
	}
}

func TestReconcileVolumesRetainsForeignVolumeOnLaterRegistryPage(t *testing.T) {
	f := newVolumeRetentionFixture(t)
	anchor := f.volume("anchor", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	foreign := f.volume("foreign", runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE)
	foreign.OrganizationId = uuid.NewString()
	f.records = []*runnersv1.Volume{anchor, foreign}
	f.inventory = &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{
		f.item("anchor"), f.item("foreign"),
	}}
	f.reconciler.runners.(*fakeRunnersClient).listVolumes = func(_ context.Context, req *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
		f.registryLists++
		switch req.GetPageToken() {
		case "":
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{anchor}, NextPageToken: "second"}, nil
		case "second":
			return &runnersv1.ListVolumesResponse{Volumes: []*runnersv1.Volume{foreign}}, nil
		default:
			t.Fatalf("unexpected page token %q", req.GetPageToken())
			return nil, errNotImplemented
		}
	}
	f.reconcile(t)
	if f.registryLists != 2 || len(f.removed) != 0 || len(f.updated) != 1 || f.updated[0].GetId() != "anchor" {
		t.Fatalf("paginated registry must retain foreign disks: lists %d, removed %v, updates %v", f.registryLists, f.removed, f.updated)
	}
}

func TestReconcileVolumesInvalidInventoryDoesNotBlockOtherRunner(t *testing.T) {
	f := newVolumeRetentionFixture(t)
	healthy := f.volume("healthy", runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	healthy.RunnerId = "runner-2"
	f.records = []*runnersv1.Volume{f.volume("invalid", runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING), healthy}
	f.inventory = &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{nil}}
	f.reconciler.runners.(*fakeRunnersClient).listRunners = func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
		return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{buildRunner("runner-1"), buildRunner("runner-2")}}, nil
	}
	f.reconciler.runnerDialer = &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
		if id == "runner-1" {
			return f.runner, nil
		}
		return &fakeRunnerClient{listVolumes: func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			return &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{f.item("healthy")}}, nil
		}}, nil
	}}
	f.reconcile(t)
	if len(f.removed) != 0 || len(f.updated) != 1 || f.updated[0].GetId() != "healthy" || healthy.GetStatus() != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
		t.Fatalf("healthy runner must progress without mutating the invalid runner: removed %v, updates %v", f.removed, f.updated)
	}
}

type volumeRetentionFixture struct {
	reconciler       *Reconciler
	runner           *fakeRunnerClient
	records          []*runnersv1.Volume
	inventory        *runnerv1.ListVolumesResponse
	beforeRunnerList func()
	registryLists    int
	runnerLists      int
	updated          []*runnersv1.UpdateVolumeCheckedRequest
	removed          []string
	ownerMutations   int
}

func newVolumeRetentionFixture(t *testing.T) *volumeRetentionFixture {
	t.Helper()
	f := &volumeRetentionFixture{}
	f.runner = &fakeRunnerClient{
		listVolumes: func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
			f.runnerLists++
			if f.beforeRunnerList != nil {
				f.beforeRunnerList()
			}
			return f.inventory, nil
		},
		removeVolumeChecked: func(_ context.Context, req *runnerv1.RemoveVolumeCheckedRequest, _ ...grpc.CallOption) (*runnerv1.RemoveVolumeCheckedResponse, error) {
			f.removed = append(f.removed, req.GetExpected().GetInstanceId())
			return &runnerv1.RemoveVolumeCheckedResponse{State: runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING}, nil
		},
	}
	runners := &fakeRunnersClient{
		listVolumes: func(_ context.Context, req *runnersv1.ListVolumesRequest, _ ...grpc.CallOption) (*runnersv1.ListVolumesResponse, error) {
			f.registryLists++
			resp := &runnersv1.ListVolumesResponse{}
			for _, record := range f.records {
				if slices.Contains(req.GetFilter().GetStatusIn(), record.GetStatus()) {
					resp.Volumes = append(resp.Volumes, proto.Clone(record).(*runnersv1.Volume))
				}
			}
			return resp, nil
		},
		listRunners: func(context.Context, *runnersv1.ListRunnersRequest, ...grpc.CallOption) (*runnersv1.ListRunnersResponse, error) {
			shared := buildRunner("runner-1")
			shared.OrganizationId = nil
			return &runnersv1.ListRunnersResponse{Runners: []*runnersv1.Runner{shared}}, nil
		},
		updateVolumeChecked: func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
			f.updated = append(f.updated, proto.Clone(req).(*runnersv1.UpdateVolumeCheckedRequest))
			for _, record := range f.records {
				if record.GetMeta().GetId() == req.GetId() {
					return checkedTestUpdate(t, record, req), nil
				}
			}
			t.Fatalf("update for unknown volume %q", req.GetId())
			return nil, errNotImplemented
		},
	}
	agents := &testutil.FakeAgentsClient{
		GetVolumeFunc: func(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{Persistent: true}}, nil
		},
		PauseInstanceFunc: func(context.Context, *agentsv1.PauseInstanceRequest, ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error) {
			f.ownerMutations++
			return &agentsv1.PauseInstanceResponse{}, nil
		},
		UpdateSandboxRuntimeStateFunc: func(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
			f.ownerMutations++
			return &agentsv1.UpdateSandboxRuntimeStateResponse{}, nil
		},
	}
	f.reconciler = newTestReconciler(Config{
		Agents: agents, Runners: runners,
		RunnerDialer: &fakeRunnerDialer{dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != "runner-1" {
				t.Fatalf("unexpected runner %q", id)
			}
			return f.runner, nil
		}},
	})
	return f
}

func (f *volumeRetentionFixture) volume(key string, state runnersv1.VolumeStatus) *runnersv1.Volume {
	return checkedTestVolume(key, state)
}

func (f *volumeRetentionFixture) item(key string) *runnerv1.VolumeListItem {
	return checkedTestInstance(f.volume(key, runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING), "pvc-"+key, "uid-"+key)
}

func (f *volumeRetentionFixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.reconciler.reconcileVolumes(context.Background()); err != nil {
		t.Fatalf("reconcile volumes: %v", err)
	}
}
