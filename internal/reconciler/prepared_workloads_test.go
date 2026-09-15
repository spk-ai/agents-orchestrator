package reconciler

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The registry and native runner are explicit independent in-memory fixtures.
// They assert ordering but do not establish database or Kubernetes acceptance.
type preparedControllerFixture struct {
	humanOwner                                   string
	lastRequest                                  *runnerv1.PrepareWorkloadRequest
	lastMetadata                                 *runnersv1.CreateWorkloadRequest
	lastVolume                                   *runnersv1.CreateVolumeRequest
	runtimeUpdates                               []*agentsv1.UpdateSandboxRuntimeStateRequest
	t                                            *testing.T
	r                                            *Reconciler
	registry                                     *fakeRunnersClient
	native                                       *fakeRunnerClient
	metadata                                     *runnersv1.CreateWorkloadRequest
	request                                      *runnerv1.StartWorkloadRequest
	infos                                        []assembler.PersistentVolumeInfo
	created                                      []volumeRecord
	w                                            *runnersv1.Workload
	v                                            *runnersv1.Volume
	binding                                      *runnerv1.WorkloadBinding
	active, pending                              bool
	prepares, activations, removals, inspections int
}

func newPreparedControllerFixture(t *testing.T, sandbox bool) *preparedControllerFixture {
	t.Helper()
	owner, definition := uuid.New(), uuid.New()
	info := assembler.PersistentVolumeInfo{ID: definition, AgentInstanceID: owner,
		Volume: &agentsv1.Volume{Size: "1Gi"}, Spec: &runnerv1.VolumeSpec{Name: "workspace", Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, PersistentName: "workspace-" + owner.String(), Size: "1Gi"}}
	v := checkedTestVolume(info.Key(), runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING)
	v.OwnerId, v.ThreadId, v.AgentId, v.VolumeId = owner.String(), owner.String(), uuid.NewString(), definition.String()
	if sandbox {
		v.OwnerKind, v.AgentId, v.ThreadId = runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX, "", ""
	}
	f := &preparedControllerFixture{t: t, v: v, infos: []assembler.PersistentVolumeInfo{info}, humanOwner: uuid.NewString()}
	f.metadata = &runnersv1.CreateWorkloadRequest{Id: uuid.NewString(), RunnerId: v.RunnerId, OrganizationId: v.OrganizationId,
		OwnerKind: v.OwnerKind, OwnerId: v.OwnerId, AgentId: v.AgentId, ThreadId: v.ThreadId, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}
	f.request = &runnerv1.StartWorkloadRequest{WorkloadId: f.metadata.Id, Main: &runnerv1.ContainerSpec{Name: "main", Image: "fixture"}, Volumes: []*runnerv1.VolumeSpec{info.Spec}}
	records, err := buildVolumeRecords(f.infos)
	if err != nil {
		t.Fatal(err)
	}
	records[0].checked = proto.Clone(v).(*runnersv1.Volume)
	f.created = records
	f.registry = &fakeRunnersClient{}
	f.registry.createWorkload = func(context.Context, *runnersv1.CreateWorkloadRequest, ...grpc.CallOption) (*runnersv1.CreateWorkloadResponse, error) {
		t.Fatal("legacy registry start")
		return nil, errNotImplemented
	}
	f.registry.getVolume = func(_ context.Context, req *runnersv1.GetVolumeRequest, _ ...grpc.CallOption) (*runnersv1.GetVolumeResponse, error) {
		if req.Id != f.v.Meta.Id {
			t.Fatal("unexpected volume query")
		}
		return &runnersv1.GetVolumeResponse{Volume: proto.Clone(f.v).(*runnersv1.Volume)}, nil
	}
	f.registry.updateVolumeChecked = func(_ context.Context, req *runnersv1.UpdateVolumeCheckedRequest, _ ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
		if req.GetFailProvisioning() != nil && f.w != nil && f.w.RemovalConfirmedAt == nil {
			t.Fatal("failed a reserved workspace")
		}
		return checkedTestUpdate(t, f.v, req), nil
	}
	f.registry.createPreparedWorkload = func(_ context.Context, req *runnersv1.CreatePreparedWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.CreatePreparedWorkloadResponse, error) {
		if f.w != nil && f.w.RemovalConfirmedAt == nil {
			return nil, status.Error(codes.FailedPrecondition, "owner busy")
		}
		m := req.Workload
		if len(req.VolumeIds) != len(f.infos) || len(f.infos) == 1 && req.VolumeIds[0] != f.v.Meta.Id || req.BackendId != checkedTestBackend {
			t.Fatal("incorrect complete volume intent")
		}
		f.lastMetadata = proto.Clone(m).(*runnersv1.CreateWorkloadRequest)
		f.w = &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: m.Id, CreatedAt: timestamppb.Now()}, RunnerId: m.RunnerId, OrganizationId: m.OrganizationId,
			OwnerKind: m.OwnerKind, OwnerId: m.OwnerId, ThreadId: m.ThreadId, AgentId: m.AgentId, Status: m.Status,
			ZitiIdentityId: m.ZitiIdentityId, AllocatedCpuMillicores: m.AllocatedCpuMillicores, AllocatedRamBytes: m.AllocatedRamBytes,
			Flavor: m.Flavor, PersistentShells: m.PersistentShells,
			Preparation: &runnersv1.PreparedWorkloadLifecycle{Revision: 1, Phase: runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED, BackendId: req.BackendId, VolumeIds: slices.Clone(req.VolumeIds)}}
		return &runnersv1.CreatePreparedWorkloadResponse{Workload: proto.Clone(f.w).(*runnersv1.Workload)}, nil
	}
	f.registry.getWorkload = func(_ context.Context, req *runnersv1.GetWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.GetWorkloadResponse, error) {
		if f.w == nil || req.Id != f.w.Meta.Id {
			return nil, status.Error(codes.NotFound, "no record")
		}
		return &runnersv1.GetWorkloadResponse{Workload: proto.Clone(f.w).(*runnersv1.Workload)}, nil
	}
	f.registry.updatePreparedWorkload = f.transition
	f.registry.updateWorkload = func(_ context.Context, req *runnersv1.UpdateWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
		if f.w == nil || req.Id != f.w.Meta.Id || req.InstanceId != nil || req.RemovalConfirmedAt != nil {
			t.Fatal("legacy identity or confirmation mutation")
		}
		if req.Status != nil {
			f.w.Status = req.GetStatus()
		}
		f.w.Containers = req.Containers
		return &runnersv1.UpdateWorkloadResponse{Workload: proto.Clone(f.w).(*runnersv1.Workload)}, nil
	}
	f.native = &fakeRunnerClient{}
	f.native.startWorkload = func(context.Context, *runnerv1.StartWorkloadRequest, ...grpc.CallOption) (*runnerv1.StartWorkloadResponse, error) {
		t.Fatal("legacy native start")
		return nil, errNotImplemented
	}
	f.native.stopWorkload = func(context.Context, *runnerv1.StopWorkloadRequest, ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
		t.Fatal("legacy native stop")
		return nil, errNotImplemented
	}
	f.native.inspectWorkload = func(context.Context, *runnerv1.InspectWorkloadRequest, ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
		t.Fatal("name-only inspection")
		return nil, errNotImplemented
	}
	f.native.listVolumes = func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
		result := &runnerv1.ListVolumesResponse{BackendId: checkedTestBackend}
		if f.v.BoundInstance != nil {
			result.Volumes = []*runnerv1.VolumeListItem{proto.Clone(f.v.BoundInstance).(*runnerv1.VolumeListItem)}
		}
		return result, nil
	}
	f.native.prepareWorkload = func(_ context.Context, req *runnerv1.PrepareWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
		f.lastRequest = proto.Clone(req).(*runnerv1.PrepareWorkloadRequest)
		f.prepares++
		if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING || req.Workload.WorkloadId != f.w.Meta.Id || req.BackendId != f.w.Preparation.BackendId {
			t.Fatal("prepare before durable authorization")
		}
		var volumes []*runnerv1.VolumeListItem
		if len(f.infos) > 0 {
			item := f.v.BoundInstance
			if item == nil {
				if len(req.ExpectedVolumes) != 0 {
					t.Fatal("first provision supplied an invented binding")
				}
				item = checkedTestInstance(f.v, f.infos[0].Spec.PersistentName, uuid.NewString())
				if isSandboxVolume(f.v) {
					item.IdentityLabels["sandbox-owner-id"] = f.humanOwner
				}
			} else if len(req.ExpectedVolumes) != 1 || !proto.Equal(item, req.ExpectedVolumes[0]) {
				t.Fatal("resume omitted or changed the workspace binding")
			}
			volumes = []*runnerv1.VolumeListItem{proto.Clone(item).(*runnerv1.VolumeListItem)}
		} else if len(req.ExpectedVolumes) != 0 {
			t.Fatal("unexpected volumes in zero-volume fixture")
		}
		f.binding = &runnerv1.WorkloadBinding{WorkloadId: f.w.Meta.Id, InstanceUid: uuid.NewString(), BackendId: req.BackendId, Volumes: volumes}
		f.active = false
		return &runnerv1.PrepareWorkloadResponse{Workload: &runnerv1.StartWorkloadResponse{Id: f.w.Meta.Id, Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING}, Binding: proto.Clone(f.binding).(*runnerv1.WorkloadBinding)}, nil
	}
	f.native.activateWorkload = func(_ context.Context, req *runnerv1.ActivateWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.ActivateWorkloadResponse, error) {
		f.activations++
		if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING || !samePreparedBinding(f.w.Preparation.Binding, req.Expected) || !samePreparedBinding(f.binding, req.Expected) || len(f.infos) > 0 && f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
			t.Fatal("execution before durable exact-binding authorization")
		}
		f.active = true
		return &runnerv1.ActivateWorkloadResponse{Binding: proto.Clone(f.binding).(*runnerv1.WorkloadBinding)}, nil
	}
	f.native.inspectPreparedWorkload = func(_ context.Context, req *runnerv1.InspectPreparedWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectPreparedWorkloadResponse, error) {
		f.inspections++
		if f.binding == nil {
			return nil, status.Error(codes.NotFound, "pod absent")
		}
		if !samePreparedBinding(f.binding, req.Expected) {
			return nil, status.Error(codes.FailedPrecondition, "replacement pod")
		}
		state := runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING
		if f.active {
			state = runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING
		}
		return &runnerv1.InspectPreparedWorkloadResponse{Binding: proto.Clone(f.binding).(*runnerv1.WorkloadBinding), Activated: f.active, RemovalPending: f.pending, ResourceVersion: "123",
			Workload: &runnerv1.InspectWorkloadResponse{Id: f.binding.WorkloadId, Containers: []*runnerv1.WorkloadContainer{{Name: "main", Role: runnerv1.ContainerRole_CONTAINER_ROLE_MAIN, Status: state}}}}, nil
	}
	f.native.removePreparedWorkload = func(_ context.Context, req *runnerv1.RemovePreparedWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error) {
		f.removals++
		if f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || !samePreparedBinding(f.w.Preparation.Binding, req.Expected) {
			t.Fatal("removal before durable exact intent")
		}
		state := runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT
		if f.pending {
			state = runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING
		} else {
			f.binding, f.active = nil, false
		}
		return &runnerv1.RemovePreparedWorkloadResponse{State: state, Binding: proto.Clone(req.Expected).(*runnerv1.WorkloadBinding)}, nil
	}
	f.r = &Reconciler{runners: f.registry, agents: &testutil.FakeAgentsClient{GetSandboxFunc: func(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
		return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Meta: &agentsv1.EntityMeta{Id: f.v.OwnerId}, OrganizationId: f.v.OrganizationId, OwnerId: f.humanOwner}}, nil
	}}}
	f.installAnchoredProtocol()
	return f
}

func (f *preparedControllerFixture) transition(_ context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
	if f.w == nil || req.Id != f.w.Meta.Id || req.ExpectedRevision != f.w.Preparation.Revision {
		return nil, status.Error(codes.Aborted, "revision conflict")
	}
	p := f.w.Preparation
	requirePhase := func(phase runnersv1.PreparedWorkloadPhase) {
		if p.Phase != phase {
			f.t.Fatalf("invalid fixture transition %T from %s", req.Operation, p.Phase)
		}
	}
	switch op := req.Operation.(type) {
	case *runnersv1.UpdatePreparedWorkloadRequest_BeginPreparation:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED)
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING
	case *runnersv1.UpdatePreparedWorkloadRequest_Bind:
		if p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING && p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING {
			f.t.Fatal("binding outside preparation/removal")
		}
		if p.Binding != nil || len(op.Bind.Binding.Volumes) != len(f.infos) || len(f.infos) > 0 && (!proto.Equal(op.Bind.Binding.Volumes[0], f.v.BoundInstance) || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE) {
			f.t.Fatal("binding before checked volume persistence")
		}
		p.Binding = proto.Clone(op.Bind.Binding).(*runnerv1.WorkloadBinding)
		f.w.InstanceId = stringPtr(f.w.Meta.Id)
		if p.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING {
			p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND
		}
	case *runnersv1.UpdatePreparedWorkloadRequest_BeginActivation:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND)
		if f.w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
			return nil, status.Error(codes.FailedPrecondition, "not starting")
		}
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING
	case *runnersv1.UpdatePreparedWorkloadRequest_ConfirmActivation:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING)
		if !samePreparedBinding(p.Binding, op.ConfirmActivation.Binding) {
			f.t.Fatal("changed activation binding")
		}
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE
	case *runnersv1.UpdatePreparedWorkloadRequest_BeginRemoval:
		if p.Phase <= runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED || p.Phase >= runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING {
			f.t.Fatal("invalid begin removal")
		}
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING
	case *runnersv1.UpdatePreparedWorkloadRequest_ConfirmRemoval:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
		if op.ConfirmRemoval.Observation.State != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT || !samePreparedBinding(p.Binding, op.ConfirmRemoval.Observation.Binding) {
			f.t.Fatal("confirmation without exact absence")
		}
		p.RemovalObservation = proto.Clone(op.ConfirmRemoval.Observation).(*runnerv1.RemovePreparedWorkloadResponse)
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED
	case *runnersv1.UpdatePreparedWorkloadRequest_RecordRevocation:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
		if p.Binding != nil || p.Resources.GetWorkload() == nil || p.Resources.PreparationRevocation != nil || p.Resources.RevocationObservation != nil {
			f.t.Fatal("revocation without an unbound anchored removal intent")
		}
		p.Resources.PreparationRevocation = proto.Clone(op.RecordRevocation.Revocation).(*runnerv1.PreparationRevocation)
	case *runnersv1.UpdatePreparedWorkloadRequest_ConfirmRevocation:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
		observation := op.ConfirmRevocation.Observation
		if p.Binding != nil || p.Resources.PreparationRevocation == nil || !proto.Equal(p.Resources.PreparationRevocation, observation.Revocation) ||
			observation.State != runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT {
			f.t.Fatal("revocation confirmation before persisted proof or without absence")
		}
		for _, v := range observation.Volumes {
			if f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || !proto.Equal(f.v.BoundInstance, v) {
				f.t.Fatal("revocation confirmation before checked PVC binding")
			}
		}
		p.Resources.RevocationObservation = proto.Clone(observation).(*runnerv1.ObservePreparationRevocationResponse)
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED
	case *runnersv1.UpdatePreparedWorkloadRequest_AbortReservation:
		requirePhase(runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED)
		p.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED
	default:
		f.t.Fatalf("unknown fixture operation %T", op)
	}
	p.Revision++
	if p.Resources != nil {
		p.Resources.Revision++
	}
	if p.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		f.w.RemovalConfirmedAt = timestamppb.Now()
		if f.w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
			f.w.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
		}
	}
	return &runnersv1.UpdatePreparedWorkloadResponse{Workload: proto.Clone(f.w).(*runnersv1.Workload)}, nil
}

func (f *preparedControllerFixture) start() (*runnersv1.Workload, error) {
	return f.r.startPreparedWorkload(context.Background(), f.native, f.metadata, f.request, f.infos, f.created)
}

func TestPreparedControllerFirstProvisionAndFollowup(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("sandbox=%t", sandbox), func(t *testing.T) {
			f := newPreparedControllerFixture(t, sandbox)
			w, err := f.start()
			if err != nil {
				t.Fatal(err)
			}
			if f.activations != 1 || w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE || w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
				t.Fatal("activation confused with readiness")
			}
			binding := proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)
			if err := f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w); err != nil {
				t.Fatal(err)
			}
			if w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING || f.activations != 1 {
				t.Fatal("readiness failed or reactivated workload")
			}
			f.pending = true
			if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err == nil || w.RemovalConfirmedAt != nil {
				t.Fatal("pending removal released admission")
			}
			f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
			f.pending = false
			if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil {
				t.Fatal(err)
			}
			if w.RemovalConfirmedAt == nil || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
				t.Fatal("removal lost confirmation or durable workspace")
			}
			f.metadata.Id = uuid.NewString()
			f.request.WorkloadId = f.metadata.Id
			f.created = nil
			next, err := f.start()
			if err != nil {
				t.Fatal(err)
			}
			if next.Preparation.Binding.InstanceUid == binding.InstanceUid || !proto.Equal(next.Preparation.Binding.Volumes[0], binding.Volumes[0]) || f.prepares != 2 {
				t.Fatal("followup did not isolate compute and preserve workspace")
			}
		})
	}
}

func TestPreparedControllerUnknownPrepareRetainsAdmission(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("sandbox=%t", sandbox), func(t *testing.T) {
			f := newPreparedControllerFixture(t, sandbox)
			prepare := f.native.prepareWorkload
			f.native.prepareWorkload = func(ctx context.Context, req *runnerv1.PrepareWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
				if _, err := prepare(ctx, req, opts...); err != nil {
					t.Fatal(err)
				}
				return nil, status.Error(codes.Unavailable, "lost prepare reply")
			}
			if _, err := f.start(); err == nil {
				t.Fatal("unknown preparation accepted")
			}
			w := proto.Clone(f.w).(*runnersv1.Workload)
			f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
			if err := f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w); err == nil {
				t.Fatal("unknown preparation silently recovered")
			}
			if f.prepares != 1 || f.activations != 0 || f.removals != 0 || f.active || f.binding == nil || f.w.Preparation.Binding != nil || f.w.RemovalConfirmedAt != nil || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
				t.Fatal("uncertain work replayed, released, or compensated")
			}
		})
	}
}

func TestPreparedControllerCancellationDuringPrepare(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, window := range []string{"prepare-reply", "binding-cas"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, window), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				cancel := func() {
					f.w.Preparation.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING
					f.w.Preparation.Revision++
					f.w.Preparation.Resources.Revision++
				}
				if window == "prepare-reply" {
					prepare := f.native.prepareWorkload
					f.native.prepareWorkload = func(ctx context.Context, req *runnerv1.PrepareWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
						response, err := prepare(ctx, req, opts...)
						cancel()
						return response, err
					}
				} else {
					first := true
					f.registry.updatePreparedWorkload = func(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
						if req.GetBind() != nil && first {
							first = false
							cancel()
						}
						return f.transition(ctx, req, opts...)
					}
				}
				if _, err := f.start(); err == nil {
					t.Fatal("canceled preparation succeeded")
				}
				if f.activations != 0 || f.prepares != 1 || f.removals != 1 || f.w.RemovalConfirmedAt == nil || f.w.Preparation.Binding == nil || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
					t.Fatal("late binding did not authorize cleanup only")
				}
			})
		}
	}
}

func TestPreparedControllerHealthRecoversActivationAcknowledgement(t *testing.T) {
	f := newPreparedControllerFixture(t, false)
	if _, err := f.start(); err != nil {
		t.Fatal(err)
	}
	f.w.Preparation.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING
	f.w.Preparation.Revision--
	f.w.Preparation.Resources.Revision--
	w := proto.Clone(f.w).(*runnersv1.Workload)
	if err := f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w); err != nil {
		t.Fatal(err)
	}
	if f.activations != 1 || w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE || w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatal("lost ACK recovery reexecuted work or lost readiness")
	}
}

func TestPreparedControllerReadRejectsChangedIdentityAndEvidence(t *testing.T) {
	for name, change := range map[string]func(*runnersv1.Workload){
		"owner":    func(w *runnersv1.Workload) { w.OwnerId = uuid.NewString() },
		"runner":   func(w *runnersv1.Workload) { w.RunnerId = "other" },
		"backend":  func(w *runnersv1.Workload) { w.Preparation.BackendId = "other" },
		"pod-uid":  func(w *runnersv1.Workload) { w.Preparation.Binding.InstanceUid = uuid.NewString() },
		"revision": func(w *runnersv1.Workload) { w.Preparation.Revision-- },
		"phase-without-revision": func(w *runnersv1.Workload) {
			w.Preparation.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING
		},
		"billing-confirmation": func(w *runnersv1.Workload) { w.RemovalConfirmedAt = timestamppb.Now() },
		"allocation":           func(w *runnersv1.Workload) { w.AllocatedRamBytes++ },
		"missing-binding":      func(w *runnersv1.Workload) { w.Preparation.Binding = nil },
		"instance-alias":       func(w *runnersv1.Workload) { w.AgentInstanceId = stringPtr("other") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPreparedControllerFixture(t, false)
			w, err := f.start()
			if err != nil {
				t.Fatal(err)
			}
			change(f.w)
			before := f.inspections
			if err := f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w); err == nil {
				t.Fatal("invalid registry state accepted")
			}
			if f.inspections != before || f.removals != 0 || f.activations != 1 {
				t.Fatal("invalid state caused native action")
			}
		})
	}
}

func TestPreparedControllerStalledReservationDoesNotDispatch(t *testing.T) {
	for _, phase := range []runnersv1.PreparedWorkloadPhase{runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND} {
		t.Run(phase.String(), func(t *testing.T) {
			f := newPreparedControllerFixture(t, false)
			ctx := context.Background()
			plan, err := f.r.planPreparedStart(ctx, f.native, f.metadata, f.request, f.infos, f.created)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.registry.CreatePreparedWorkload(ctx, &runnersv1.CreatePreparedWorkloadRequest{Workload: f.metadata, BackendId: plan.request.BackendId, VolumeIds: plan.ids}); err != nil {
				t.Fatal(err)
			}
			if phase >= runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING {
				if _, err := f.transition(ctx, &runnersv1.UpdatePreparedWorkloadRequest{Id: f.w.Meta.Id, ExpectedRevision: f.w.Preparation.Revision, Operation: &runnersv1.UpdatePreparedWorkloadRequest_BeginPreparation{BeginPreparation: &runnersv1.BeginWorkloadPreparation{}}}); err != nil {
					t.Fatal(err)
				}
			}
			if phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND {
				response, err := f.native.PrepareWorkload(ctx, plan.request)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.r.persistPreparedVolumes(ctx, plan, response.Binding); err != nil {
					t.Fatal(err)
				}
				if _, err := f.r.persistPreparedBinding(ctx, proto.Clone(f.w).(*runnersv1.Workload), response.Binding); err != nil {
					t.Fatal(err)
				}
			}
			f.w.Meta.CreatedAt = timestamppb.New(time.Now().Add(-2 * startGracePeriod))
			w := proto.Clone(f.w).(*runnersv1.Workload)
			prepared := f.prepares
			err = f.r.handlePreparedRunnerWorkload(ctx, f.native, w)
			if phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING {
				if err == nil || w.RemovalConfirmedAt != nil {
					t.Fatal("unknown preparation settled")
				}
			} else if err != nil || w.RemovalConfirmedAt == nil {
				t.Fatalf("stalled workload not retired: %v", err)
			}
			if f.prepares != prepared || f.activations != 0 || f.active || f.binding != nil {
				t.Fatal("recovery dispatched work or left bound compute")
			}
		})
	}
}

func TestPreparedControllerLostTransitionReplies(t *testing.T) {
	operations := map[string]func(*runnersv1.UpdatePreparedWorkloadRequest) bool{
		"begin-preparation":  func(r *runnersv1.UpdatePreparedWorkloadRequest) bool { return r.GetBeginPreparation() != nil },
		"bind":               func(r *runnersv1.UpdatePreparedWorkloadRequest) bool { return r.GetBind() != nil },
		"begin-activation":   func(r *runnersv1.UpdatePreparedWorkloadRequest) bool { return r.GetBeginActivation() != nil },
		"confirm-activation": func(r *runnersv1.UpdatePreparedWorkloadRequest) bool { return r.GetConfirmActivation() != nil },
	}
	for _, sandbox := range []bool{false, true} {
		for name, matches := range operations {
			for _, committed := range []bool{false, true} {
				t.Run(fmt.Sprintf("sandbox=%t/%s/committed=%t", sandbox, name, committed), func(t *testing.T) {
					f := newPreparedControllerFixture(t, sandbox)
					injected := false
					f.registry.updatePreparedWorkload = func(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
						if !injected && matches(req) {
							injected = true
							if committed {
								if _, err := f.transition(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, status.Error(codes.Unavailable, "transition reply lost")
						}
						return f.transition(ctx, req, opts...)
					}
					if _, err := f.start(); err == nil || !injected {
						t.Fatal("transition failure did not interrupt start")
					}
					expectedActivations := 0
					if name == "confirm-activation" {
						expectedActivations = 1
					}
					if f.activations != expectedActivations || f.active {
						t.Fatal("lost commit reply allowed execution/replay")
					}
					unknown := name == "begin-preparation" && committed || name == "bind" && !committed
					if unknown {
						if f.w.RemovalConfirmedAt != nil || f.w.Preparation.Binding != nil || f.removals != 0 {
							t.Fatal("unknown native identity was released")
						}
					} else if f.w.RemovalConfirmedAt == nil {
						t.Fatal("known reservation/binding was not retired")
					}
				})
			}
		}
	}
}

func TestPreparedControllerRemovalRestartWindows(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, window := range []string{"before-begin", "after-begin", "native-reply", "before-confirm", "after-confirm"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, window), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				w, err := f.start()
				if err != nil {
					t.Fatal(err)
				}
				binding := proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)
				injected := false
				if window == "native-reply" {
					remove := f.native.removePreparedWorkload
					f.native.removePreparedWorkload = func(ctx context.Context, req *runnerv1.RemovePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error) {
						response, err := remove(ctx, req, opts...)
						if !injected {
							injected = true
							return nil, status.Error(codes.Unavailable, "lost native removal reply")
						}
						return response, err
					}
				} else {
					f.registry.updatePreparedWorkload = func(ctx context.Context, req *runnersv1.UpdatePreparedWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdatePreparedWorkloadResponse, error) {
						matches := (window == "before-begin" || window == "after-begin") && req.GetBeginRemoval() != nil || (window == "before-confirm" || window == "after-confirm") && req.GetConfirmRemoval() != nil
						if !injected && matches {
							injected = true
							if window == "after-begin" || window == "after-confirm" {
								if _, err := f.transition(ctx, req, opts...); err != nil {
									t.Fatal(err)
								}
							}
							return nil, status.Error(codes.Unavailable, "lost registry removal reply")
						}
						return f.transition(ctx, req, opts...)
					}
				}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err == nil || !injected {
					t.Fatal("removal failure not surfaced")
				}
				f.r = &Reconciler{runners: f.registry, agents: f.r.agents}
				if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err != nil {
					t.Fatal(err)
				}
				if w.RemovalConfirmedAt == nil || f.active || !samePreparedBinding(binding, w.Preparation.Binding) || f.prepares != 1 || f.activations != 1 || f.v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE {
					t.Fatal("restart lost binding, workspace, or replay protection")
				}
			})
		}
	}
}

func TestPreparedControllerUnsafeWorkspacePlan(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		for _, scenario := range []string{"unproven-first-create", "reopened-unbound", "legacy", "other-owner", "other-runner", "other-org", "other-definition", "wrong-size", "pending-removal", "untracked-volume", "duplicate-volume", "other-backend", "missing-bound-pvc", "replaced-bound-pvc", "unknown-inventory", "incomplete-inventory"} {
			t.Run(fmt.Sprintf("sandbox=%t/%s", sandbox, scenario), func(t *testing.T) {
				f := newPreparedControllerFixture(t, sandbox)
				switch scenario {
				case "unproven-first-create":
					f.created = nil
				case "reopened-unbound":
					f.v.LifecycleRevision = 3
					f.created[0].checked.LifecycleRevision = 3
				case "legacy":
					f.v.CheckedLifecycle = false
				case "other-owner":
					f.v.OwnerId = uuid.NewString()
				case "other-runner":
					f.v.RunnerId = "another-runner"
				case "other-org":
					f.v.OrganizationId = uuid.NewString()
				case "other-definition":
					f.v.VolumeId = uuid.NewString()
				case "wrong-size":
					f.v.SizeGb = "20"
				case "pending-removal":
					f.v.Status = runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING
				case "untracked-volume":
					f.request.Volumes = append(f.request.Volumes, &runnerv1.VolumeSpec{Kind: runnerv1.VolumeKind_VOLUME_KIND_NAMED, PersistentName: "extra", Labels: map[string]string{assembler.LabelVolumeKey: uuid.NewString()}})
				case "duplicate-volume":
					f.request.Volumes = append(f.request.Volumes, proto.Clone(f.request.Volumes[0]).(*runnerv1.VolumeSpec))
				case "other-backend", "missing-bound-pvc", "replaced-bound-pvc":
					f.v.BoundInstance = checkedTestInstance(f.v, f.infos[0].Spec.PersistentName, uuid.NewString())
					f.v.InstanceId, f.v.Status, f.v.LifecycleRevision = stringPtr(f.v.BoundInstance.InstanceId), runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, 2
					f.seedAnchoredWorkspace()
					list := f.native.listVolumes
					f.native.listVolumes = func(ctx context.Context, req *runnerv1.ListVolumesRequest, opts ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
						response, err := list(ctx, req, opts...)
						switch scenario {
						case "other-backend":
							response.BackendId = "other"
							response.Volumes[0].BackendId = "other"
						case "missing-bound-pvc":
							response.Volumes = nil
						case "replaced-bound-pvc":
							response.Volumes[0].InstanceUid = uuid.NewString()
						}
						return response, err
					}
				case "unknown-inventory":
					f.native.listVolumes = func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
						return nil, status.Error(codes.Unavailable, "inventory unavailable")
					}
				case "incomplete-inventory":
					f.native.listVolumes = func(context.Context, *runnerv1.ListVolumesRequest, ...grpc.CallOption) (*runnerv1.ListVolumesResponse, error) {
						return &runnerv1.ListVolumesResponse{}, nil
					}
				}
				if _, err := f.r.planPreparedStart(context.Background(), f.native, f.metadata, f.request, f.infos, f.created); err == nil {
					t.Fatal("unsafe plan accepted")
				}
				if f.w != nil || f.prepares != 0 || f.activations != 0 || f.removals != 0 {
					t.Fatal("preflight mutated workloads")
				}
			})
		}
	}
}

func TestPreparedControllerRejectsUnverifiedRemoval(t *testing.T) {
	for _, scenario := range []string{"nil", "pending", "wrong-uid", "wrong-backend", "wrong-volume", "unimplemented"} {
		t.Run(scenario, func(t *testing.T) {
			f := newPreparedControllerFixture(t, false)
			w, err := f.start()
			if err != nil {
				t.Fatal(err)
			}
			f.native.removePreparedWorkload = func(_ context.Context, req *runnerv1.RemovePreparedWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.RemovePreparedWorkloadResponse, error) {
				response := &runnerv1.RemovePreparedWorkloadResponse{Binding: proto.Clone(req.Expected).(*runnerv1.WorkloadBinding), State: runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT}
				switch scenario {
				case "nil":
					return nil, nil
				case "unimplemented":
					return nil, status.Error(codes.Unimplemented, "old runner")
				case "pending":
					response.State = runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_PENDING
				case "wrong-uid":
					response.Binding.InstanceUid = uuid.NewString()
				case "wrong-backend":
					response.Binding.BackendId = "other"
				case "wrong-volume":
					response.Binding.Volumes[0].InstanceUid = uuid.NewString()
				}
				return response, nil
			}
			if err := f.r.stopPreparedWorkload(context.Background(), f.native, w); err == nil || w.RemovalConfirmedAt != nil || f.w.RemovalConfirmedAt != nil {
				t.Fatal("unverified absence released admission")
			}
		})
	}
}

func TestPreparedControllerExitedMainIsRetired(t *testing.T) {
	for _, sandbox := range []bool{false, true} {
		t.Run(fmt.Sprintf("sandbox=%t", sandbox), func(t *testing.T) {
			f := newPreparedControllerFixture(t, sandbox)
			w, err := f.start()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w); err != nil {
				t.Fatal(err)
			}
			inspect := f.native.inspectPreparedWorkload
			f.native.inspectPreparedWorkload = func(ctx context.Context, req *runnerv1.InspectPreparedWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.InspectPreparedWorkloadResponse, error) {
				response, err := inspect(ctx, req, opts...)
				response.Workload.Containers[0].Status = runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED
				return response, err
			}
			if err := f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w); err != nil {
				t.Fatal(err)
			}
			if f.active || w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || w.RemovalConfirmedAt == nil || f.activations != 1 || f.prepares != 1 {
				t.Fatal("exited main retained compute or triggered replay")
			}
		})
	}
}

func TestPreparedControllerHealthAcknowledgementMustBeDurable(t *testing.T) {
	for _, scenario := range []string{"nil", "wrong-binding", "wrong-status", "missing-containers", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			f := newPreparedControllerFixture(t, false)
			w, err := f.start()
			if err != nil {
				t.Fatal(err)
			}
			update := f.registry.updateWorkload
			f.registry.updateWorkload = func(ctx context.Context, req *runnersv1.UpdateWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateWorkloadResponse, error) {
				response, err := update(ctx, req, opts...)
				switch scenario {
				case "nil":
					return nil, nil
				case "wrong-binding":
					response.Workload.Preparation.Binding.InstanceUid = uuid.NewString()
				case "wrong-status":
					response.Workload.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
				case "missing-containers":
					response.Workload.Containers = nil
				case "cancellation":
					f.w.Preparation.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING
					f.w.Preparation.Revision++
					f.w.Preparation.Resources.Revision++
					response.Workload = proto.Clone(f.w).(*runnersv1.Workload)
				}
				return response, err
			}
			err = f.r.handlePreparedRunnerWorkload(context.Background(), f.native, w)
			if scenario == "cancellation" {
				if err != nil || w.RemovalConfirmedAt == nil || f.active {
					t.Fatalf("cancellation lost during readiness: %v", err)
				}
			} else if err == nil || w.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
				t.Fatal("unverified readiness acknowledged")
			}
			if f.activations != 1 || f.prepares != 1 {
				t.Fatal("health acknowledgement replayed native execution")
			}
		})
	}
}

func TestPreparedControllerCannotConfirmUnknownPreparation(t *testing.T) {
	f := newPreparedControllerFixture(t, false)
	f.native.prepareWorkload = func(context.Context, *runnerv1.PrepareWorkloadRequest, ...grpc.CallOption) (*runnerv1.PrepareWorkloadResponse, error) {
		return nil, status.Error(codes.Unavailable, "unknown prepare outcome")
	}
	if _, err := f.start(); err == nil {
		t.Fatal("unknown preparation succeeded")
	}
	previous := proto.Clone(f.w).(*runnersv1.Workload)
	f.w.Preparation.Phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED
	f.w.Preparation.Revision++
	f.w.Preparation.Resources.Revision++
	f.w.RemovalConfirmedAt = timestamppb.Now()
	if err := f.r.stopPreparedWorkload(context.Background(), f.native, previous); err == nil {
		t.Fatal("authorized preparation reclassified as unused")
	}
	if previous.RemovalConfirmedAt != nil || f.removals != 0 || f.activations != 0 {
		t.Fatal("unknown native work released or replayed")
	}
}
