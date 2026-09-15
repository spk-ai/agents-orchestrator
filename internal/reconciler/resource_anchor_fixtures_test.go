package reconciler

import (
	"context"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (f *fakeRunnersClient) CreateAnchoredWorkload(ctx context.Context, req *runnersv1.CreateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreateAnchoredWorkloadResponse, error) {
	if f.createAnchoredWorkload != nil {
		return f.createAnchoredWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "anchored registry unavailable")
}

func (f *fakeRunnersClient) BindWorkloadResourceAnchors(ctx context.Context, req *runnersv1.BindWorkloadResourceAnchorsRequest, opts ...grpc.CallOption) (*runnersv1.BindWorkloadResourceAnchorsResponse, error) {
	if f.bindWorkloadResourceAnchors != nil {
		return f.bindWorkloadResourceAnchors(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "anchored registry unavailable")
}

func (f *fakeRunnersClient) UpdateAnchoredWorkload(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
	if f.updateAnchoredWorkload != nil {
		return f.updateAnchoredWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "anchored registry unavailable")
}

func (f *fakeRunnerClient) ReserveResourceAnchor(ctx context.Context, req *runnerv1.ReserveResourceAnchorRequest, opts ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error) {
	if f.reserveResourceAnchor != nil {
		return f.reserveResourceAnchor(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "resource anchor reservation unavailable")
}

func (f *fakeRunnerClient) PrepareAnchoredWorkload(ctx context.Context, req *runnerv1.PrepareAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareAnchoredWorkloadResponse, error) {
	if f.prepareAnchoredWorkload != nil {
		return f.prepareAnchoredWorkload(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "anchored runner unavailable")
}

func (f *fakeRunnerClient) RemoveWorkloadAnchor(ctx context.Context, req *runnerv1.RemoveWorkloadAnchorRequest, opts ...grpc.CallOption) (*runnerv1.RemoveWorkloadAnchorResponse, error) {
	if f.removeWorkloadAnchor != nil {
		return f.removeWorkloadAnchor(ctx, req, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "resource anchor revocation unavailable")
}

// The old callbacks remain transition engines for legacy-record tests and fault
// injection. Actual controller RPC dispatch is through these distinct methods.
func (f *preparedControllerFixture) installAnchoredProtocol() {
	f.request.Labels = resourceAnchorLabels(&runnersv1.Workload{OwnerKind: f.metadata.OwnerKind, OwnerId: f.metadata.OwnerId, AgentId: f.metadata.AgentId, ThreadId: f.metadata.ThreadId},
		runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, f.metadata.Id, f.humanOwner)
	delete(f.request.Labels, "app.kubernetes.io/managed-by")
	delete(f.request.Labels, "agyn.dev/managed-by")
	if f.metadata.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
		f.request.Labels["thread-id"] = uuid.NewString()
	}
	anchors := map[string]*runnerv1.ResourceAnchor{}
	f.registry.createAnchoredWorkload = func(ctx context.Context, req *runnersv1.CreateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.CreateAnchoredWorkloadResponse, error) {
		response, err := f.registry.createPreparedWorkload(ctx, req.Preparation, opts...)
		if f.w != nil && f.w.Meta.Id == req.Preparation.Workload.Id {
			f.w.Preparation.Resources = &runnersv1.WorkloadResourceAnchors{Revision: 1}
		}
		if err != nil {
			return nil, err
		}
		if response.GetWorkload().GetPreparation() != nil {
			response.Workload.Preparation.Resources = &runnersv1.WorkloadResourceAnchors{Revision: 1}
		}
		return &runnersv1.CreateAnchoredWorkloadResponse{Workload: response.GetWorkload()}, nil
	}
	f.registry.updateAnchoredWorkload = func(ctx context.Context, req *runnersv1.UpdateAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.UpdateAnchoredWorkloadResponse, error) {
		if f.w == nil || f.w.Preparation.Resources == nil || f.w.Preparation.Resources.Revision != req.ExpectedAnchorRevision {
			return nil, status.Error(codes.Aborted, "anchor revision changed")
		}
		response, err := f.registry.updatePreparedWorkload(ctx, req.Operation, opts...)
		if err != nil {
			return nil, err
		}
		return &runnersv1.UpdateAnchoredWorkloadResponse{Workload: response.GetWorkload()}, nil
	}
	f.registry.bindWorkloadResourceAnchors = func(_ context.Context, req *runnersv1.BindWorkloadResourceAnchorsRequest, _ ...grpc.CallOption) (*runnersv1.BindWorkloadResourceAnchorsResponse, error) {
		p := f.w.Preparation
		if req.Id != f.w.Meta.Id || req.ExpectedPreparationRevision != p.Revision || req.ExpectedAnchorRevision != p.Resources.Revision {
			return nil, status.Error(codes.Aborted, "anchor revision changed")
		}
		if p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED || f.w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || p.Resources.Workload != nil {
			return nil, status.Error(codes.FailedPrecondition, "reservation no longer unused")
		}
		if len(req.VolumeAnchors) != len(f.infos) || len(f.infos) > 0 && !proto.Equal(req.VolumeAnchors[0], f.v.ResourceAnchor) {
			f.t.Fatal("workload owner before volume owner persistence")
		}
		p.Resources = proto.Clone(&runnersv1.WorkloadResourceAnchors{Revision: p.Resources.Revision + 1, Workload: req.WorkloadAnchor, Volumes: req.VolumeAnchors}).(*runnersv1.WorkloadResourceAnchors)
		return &runnersv1.BindWorkloadResourceAnchorsResponse{Workload: proto.Clone(f.w).(*runnersv1.Workload)}, nil
	}
	volumeUpdate := f.registry.updateVolumeChecked
	f.registry.updateVolumeChecked = func(ctx context.Context, req *runnersv1.UpdateVolumeCheckedRequest, opts ...grpc.CallOption) (*runnersv1.UpdateVolumeCheckedResponse, error) {
		if op := req.GetBindAnchor(); op != nil {
			if req.Id != f.v.Meta.Id || req.ExpectedRevision != f.v.LifecycleRevision || f.v.ResourceAnchor != nil || f.v.BoundInstance != nil ||
				f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED || op.WorkloadId != f.w.Meta.Id ||
				op.ExpectedPreparationRevision != f.w.Preparation.Revision || op.ExpectedAnchorRevision != f.w.Preparation.Resources.Revision {
				return nil, status.Error(codes.Aborted, "volume reservation changed")
			}
			f.v.ResourceAnchor = proto.Clone(op.Anchor).(*runnerv1.ResourceAnchor)
			f.v.AnchorReservation = &runnersv1.VolumeAnchorReservation{WorkloadId: op.WorkloadId, PreparationRevision: op.ExpectedPreparationRevision, ResourceRevision: op.ExpectedAnchorRevision}
			f.v.LifecycleRevision++
			return &runnersv1.UpdateVolumeCheckedResponse{Volume: proto.Clone(f.v).(*runnersv1.Volume)}, nil
		}
		return volumeUpdate(ctx, req, opts...)
	}
	f.native.reserveResourceAnchor = func(_ context.Context, req *runnerv1.ReserveResourceAnchorRequest, _ ...grpc.CallOption) (*runnerv1.ReserveResourceAnchorResponse, error) {
		if f.w == nil || f.w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED || req.Intent.InstanceUid != "" {
			f.t.Fatal("native metadata before unused registry reservation")
		}
		a := anchors[req.Intent.ResourceId]
		if a == nil {
			a = proto.Clone(req.Intent).(*runnerv1.ResourceAnchor)
			a.InstanceUid = uuid.NewString()
			anchors[a.ResourceId] = a
		}
		return &runnerv1.ReserveResourceAnchorResponse{Anchor: proto.Clone(a).(*runnerv1.ResourceAnchor)}, nil
	}
	f.native.prepareAnchoredWorkload = func(ctx context.Context, req *runnerv1.PrepareAnchoredWorkloadRequest, opts ...grpc.CallOption) (*runnerv1.PrepareAnchoredWorkloadResponse, error) {
		if !proto.Equal(req.WorkloadAnchor, f.w.Preparation.Resources.Workload) || !proto.Equal(req.WorkloadAnchor, anchors[f.w.Meta.Id]) ||
			len(req.VolumeAnchors) != len(f.infos) || len(f.infos) > 0 && !proto.Equal(req.VolumeAnchors[0], f.v.ResourceAnchor) {
			f.t.Fatal("native preparation before exact resource persistence")
		}
		response, err := f.native.prepareWorkload(ctx, req.Preparation, opts...)
		attach := func(b *runnerv1.WorkloadBinding) {
			if b == nil {
				return
			}
			b.Anchor = proto.Clone(req.WorkloadAnchor).(*runnerv1.ResourceAnchor)
			for _, v := range b.Volumes {
				if f.v.ResourceAnchor != nil {
					v.Anchor = proto.Clone(f.v.ResourceAnchor).(*runnerv1.ResourceAnchor)
				}
			}
		}
		attach(f.binding)
		if err != nil {
			return nil, err
		}
		attach(response.GetBinding())
		return &runnerv1.PrepareAnchoredWorkloadResponse{Binding: response.GetBinding(), Workload: response.GetWorkload()}, nil
	}
	f.native.removeWorkloadAnchor = func(_ context.Context, req *runnerv1.RemoveWorkloadAnchorRequest, _ ...grpc.CallOption) (*runnerv1.RemoveWorkloadAnchorResponse, error) {
		if req.Expected.Kind != runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			f.t.Fatal("workload cleanup tried to remove volume owner")
		}
		if a := anchors[req.Expected.ResourceId]; a != nil && !proto.Equal(a, req.Expected) {
			return nil, status.Error(codes.FailedPrecondition, "anchor replaced")
		}
		if f.active {
			return nil, status.Error(codes.FailedPrecondition, "active Pod must be removed first")
		}
		delete(anchors, req.Expected.ResourceId)
		return &runnerv1.RemoveWorkloadAnchorResponse{Anchor: proto.Clone(req.Expected).(*runnerv1.ResourceAnchor), State: runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_ABSENT}, nil
	}
}

func (f *preparedControllerFixture) seedAnchoredWorkspace() {
	w := &runnersv1.Workload{OwnerKind: f.metadata.OwnerKind, OwnerId: f.metadata.OwnerId, AgentId: f.metadata.AgentId, ThreadId: f.metadata.ThreadId}
	f.v.ResourceAnchor = &runnerv1.ResourceAnchor{Kind: runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME,
		ResourceId: f.v.Meta.Id, BackendId: checkedTestBackend, InstanceUid: uuid.NewString(),
		IdentityLabels: resourceAnchorLabels(w, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, f.v.Meta.Id, f.humanOwner)}
	f.v.AnchorReservation = &runnersv1.VolumeAnchorReservation{WorkloadId: uuid.NewString(), PreparationRevision: 1, ResourceRevision: 1}
	if f.v.BoundInstance != nil {
		f.v.BoundInstance.Anchor = proto.Clone(f.v.ResourceAnchor).(*runnerv1.ResourceAnchor)
		if isSandboxVolume(f.v) {
			f.v.BoundInstance.IdentityLabels["sandbox-owner-id"] = f.humanOwner
		}
		f.v.LifecycleRevision = 3
	} else {
		f.v.LifecycleRevision = 2
	}
}
