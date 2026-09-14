package reconciler

import (
	"context"
	"errors"
	"fmt"
	"slices"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type preparedStartPlan struct {
	request *runnerv1.PrepareWorkloadRequest
	volumes map[string]*runnersv1.Volume
	names   map[string]string
	ids     []string
}

func (r *Reconciler) planPreparedStart(ctx context.Context, runner runnerv1.RunnerServiceClient, metadata *runnersv1.CreateWorkloadRequest, request *runnerv1.StartWorkloadRequest, infos []assembler.PersistentVolumeInfo, created []volumeRecord) (*preparedStartPlan, error) {
	if metadata == nil || request.GetMain() == nil || !preparedUUID(metadata.Id) || request.WorkloadId != metadata.Id {
		return nil, fmt.Errorf("prepared start requires matching workload metadata")
	}
	records, err := buildVolumeRecords(infos)
	if err != nil {
		return nil, err
	}
	if len(request.Volumes) > 64 {
		return nil, fmt.Errorf("prepared volume limit exceeded")
	}
	inventory, err := runner.ListVolumes(ctx, &runnerv1.ListVolumesRequest{})
	if err != nil {
		return nil, err
	}
	indexed, err := indexRunnerVolumes(inventory)
	if err != nil {
		return nil, err
	}
	plan := &preparedStartPlan{request: &runnerv1.PrepareWorkloadRequest{Workload: proto.Clone(request).(*runnerv1.StartWorkloadRequest), BackendId: inventory.BackendId}, volumes: map[string]*runnersv1.Volume{}, names: map[string]string{}}
	specs := map[string]*runnerv1.VolumeSpec{}
	names := map[string]bool{}
	for _, spec := range request.Volumes {
		if spec.GetKind() != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
			continue
		}
		id := spec.Labels[assembler.LabelVolumeKey]
		if !preparedUUID(id) || specs[id] != nil || !validVolumeValue(spec.PersistentName) || names[spec.PersistentName] {
			return nil, fmt.Errorf("named volumes require a unique persistent registry identity")
		}
		specs[id], names[spec.PersistentName] = spec, true
	}
	if len(specs) != len(records) {
		return nil, fmt.Errorf("untracked or omitted persistent volume")
	}
	for _, record := range records {
		spec := specs[record.id]
		if spec == nil || plan.volumes[record.id] != nil {
			return nil, fmt.Errorf("prepared volume definitions differ from request")
		}
		response, err := r.runners.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: record.id})
		if err != nil {
			return nil, err
		}
		v := response.GetVolume()
		if err := validateCheckedVolume(v); err != nil {
			return nil, err
		}
		if v.Meta.Id != record.id || v.RunnerId != metadata.RunnerId || v.OrganizationId != metadata.OrganizationId ||
			v.OwnerKind != metadata.OwnerKind || v.OwnerId != metadata.OwnerId || v.ThreadId != metadata.ThreadId || v.AgentId != metadata.AgentId ||
			v.VolumeId != record.volumeID || !sameVolumeSize(v.SizeGb, record.sizeGB) || v.RemovalIntent != nil ||
			v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE && v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING {
			return nil, checkedVolumeError(v, "prepared workload owner or generation mismatch")
		}
		if v.BoundInstance != nil {
			if v.BoundInstance.BackendId != inventory.BackendId || v.BoundInstance.InstanceId != spec.PersistentName || !proto.Equal(v.BoundInstance, indexed[v.Meta.Id]) {
				return nil, checkedVolumeError(v, "bound workspace missing or replaced on selected backend")
			}
			plan.request.ExpectedVolumes = append(plan.request.ExpectedVolumes, proto.Clone(v.BoundInstance).(*runnerv1.VolumeListItem))
		} else {
			// Only this attempt's successful first-create receipt permits an
			// omitted native binding. An old unbound row is not proof of absence.
			fresh := false
			for _, receipt := range created {
				if receipt.id == record.id && receipt.checked != nil && receipt.checked.LifecycleRevision == 1 &&
					v.LifecycleRevision == 1 && sameVolumeIdentity(receipt.checked, v) && receipt.checked.BoundInstance == nil {
					fresh = true
				}
			}
			if !fresh || indexed[v.Meta.Id] != nil {
				return nil, checkedVolumeError(v, "unbound workspace requires explicit reconciliation")
			}
			for _, item := range inventory.Volumes {
				if item.InstanceId == spec.PersistentName {
					return nil, checkedVolumeError(v, "first-provision name already exists")
				}
			}
		}
		plan.volumes[record.id], plan.names[record.id] = proto.Clone(v).(*runnersv1.Volume), spec.PersistentName
		plan.ids = append(plan.ids, record.id)
	}
	slices.Sort(plan.ids)
	return plan, nil
}

func preparedMetadata(metadata *runnersv1.CreateWorkloadRequest, plan *preparedStartPlan) *runnersv1.Workload {
	return &runnersv1.Workload{Meta: &runnersv1.EntityMeta{Id: metadata.Id}, RunnerId: metadata.RunnerId,
		OrganizationId: metadata.OrganizationId, OwnerKind: metadata.OwnerKind, OwnerId: metadata.OwnerId,
		ThreadId: metadata.ThreadId, AgentId: metadata.AgentId, AgentClassId: metadata.AgentClassId, AgentInstanceId: metadata.AgentInstanceId,
		ZitiIdentityId: metadata.ZitiIdentityId, Status: metadata.Status, AllocatedCpuMillicores: metadata.AllocatedCpuMillicores,
		AllocatedRamBytes: metadata.AllocatedRamBytes, Flavor: metadata.Flavor, PersistentShells: metadata.PersistentShells,
		Preparation: &runnersv1.PreparedWorkloadLifecycle{Phase: runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED,
			Revision: 1, BackendId: plan.request.BackendId, VolumeIds: slices.Clone(plan.ids)}}
}

func (r *Reconciler) persistPreparedVolumes(ctx context.Context, plan *preparedStartPlan, binding *runnerv1.WorkloadBinding) error {
	for _, item := range binding.Volumes {
		expected := plan.volumes[item.VolumeKey]
		response, err := r.runners.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: item.VolumeKey})
		if err != nil {
			return err
		}
		v := response.GetVolume()
		if err := validateCheckedVolume(v); err != nil {
			return err
		}
		if !sameVolumeIdentity(expected, v) || !sameVolumeSize(expected.SizeGb, v.SizeGb) || v.LifecycleRevision < expected.LifecycleRevision ||
			v.RemovalIntent != nil || v.BoundInstance != nil && !proto.Equal(v.BoundInstance, item) {
			return checkedVolumeError(v, "workspace changed during preparation")
		}
		if v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE && proto.Equal(v.BoundInstance, item) {
			continue
		}
		if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING || v.LifecycleRevision != expected.LifecycleRevision {
			return checkedVolumeError(v, "provisioning generation changed during preparation")
		}
		if _, err := r.bindCheckedVolume(ctx, v, item); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) persistPreparedBinding(ctx context.Context, previous *runnersv1.Workload, binding *runnerv1.WorkloadBinding) (*runnersv1.Workload, error) {
	// Cancellation may win while prepare is in flight. Persist the same receipt
	// into REMOVING for cleanup, never turn that race into activation permission.
	for attempt := 0; attempt < 3; attempt++ {
		w, err := r.currentPreparedWorkload(ctx, previous)
		if err != nil {
			return nil, err
		}
		if w.Preparation.Binding != nil {
			if !samePreparedBinding(w.Preparation.Binding, binding) {
				return nil, fmt.Errorf("prepared receipt conflicts with registry binding")
			}
			return w, nil
		}
		phase := w.Preparation.Phase
		if phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING {
			phase = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND
		} else if phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING {
			return nil, fmt.Errorf("preparation no longer accepts a binding")
		}
		next, err := r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_Bind{Bind: &runnersv1.BindPreparedWorkload{Binding: binding}}}, phase)
		if err == nil {
			return next, nil
		}
		if status.Code(err) != codes.Aborted {
			return nil, err
		}
	}
	return nil, fmt.Errorf("prepared binding conflicted repeatedly; reconciliation required")
}

func (r *Reconciler) startPreparedWorkload(ctx context.Context, runner runnerv1.RunnerServiceClient, metadata *runnersv1.CreateWorkloadRequest, request *runnerv1.StartWorkloadRequest, infos []assembler.PersistentVolumeInfo, created []volumeRecord) (result *runnersv1.Workload, resultErr error) {
	var owned *runnersv1.Workload
	registryAttempted := false
	defer func() {
		if resultErr == nil {
			return
		}
		if !registryAttempted {
			r.markVolumeRecordsFailed(ctx, created)
			r.compensateIdentity(ctx, stringPtr(metadata.GetZitiIdentityId()), "prepared start preflight failure")
			r.revokePullCredential(ctx, metadata.GetId())
		} else if owned != nil {
			r.markWorkloadFailed(ctx, owned.Meta.Id, nil, runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, "prepared start did not complete; reconciliation required", nil)
			if err := r.stopPreparedWorkload(ctx, runner, owned); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
	}()
	plan, err := r.planPreparedStart(ctx, runner, metadata, request, infos, created)
	if err != nil {
		return nil, err
	}
	expected := preparedMetadata(metadata, plan)
	if err := validatePreparedWorkload(expected); err != nil {
		return nil, err
	}
	registryAttempted = true
	response, err := r.runners.CreatePreparedWorkload(ctx, &runnersv1.CreatePreparedWorkloadRequest{Workload: metadata, BackendId: plan.request.BackendId, VolumeIds: plan.ids})
	if err != nil {
		return nil, err
	}
	w := response.GetWorkload()
	if err := validatePreparedWorkload(w); err != nil {
		return nil, err
	}
	if !samePreparedIdentity(expected, w) || !proto.Equal(expected.Preparation, w.Preparation) || w.Status != expected.Status {
		return nil, fmt.Errorf("registry did not reserve the requested prepared workload")
	}
	owned = proto.Clone(w).(*runnersv1.Workload)
	w, err = r.updatePreparedWorkload(ctx, owned, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_BeginPreparation{BeginPreparation: &runnersv1.BeginWorkloadPreparation{}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING)
	if err != nil {
		return nil, err
	}
	owned = w
	if w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
		return nil, fmt.Errorf("preparation canceled before native dispatch")
	}
	prepared, err := runner.PrepareWorkload(ctx, plan.request)
	if err != nil {
		return nil, err
	}
	projected := proto.Clone(w).(*runnersv1.Workload)
	projected.Preparation.Phase, projected.Preparation.Binding = runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND, prepared.GetBinding()
	projected.InstanceId = stringPtr(w.Meta.Id)
	if err := validatePreparedWorkload(projected); err != nil {
		return nil, err
	}
	for _, item := range projected.Preparation.Binding.Volumes {
		v := plan.volumes[item.VolumeKey]
		if item.InstanceId != plan.names[item.VolumeKey] || v.BoundInstance != nil && !proto.Equal(v.BoundInstance, item) {
			return nil, fmt.Errorf("prepared receipt changed a requested workspace")
		}
	}
	if err := r.persistPreparedVolumes(ctx, plan, projected.Preparation.Binding); err != nil {
		return nil, err
	}
	w, err = r.persistPreparedBinding(ctx, owned, projected.Preparation.Binding)
	if err != nil {
		return nil, err
	}
	owned = w
	if w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND || w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
		return nil, fmt.Errorf("preparation canceled before activation")
	}
	if prepared.GetWorkload().GetId() != w.Meta.Id || prepared.GetWorkload().GetStatus() != runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
		return nil, fmt.Errorf("runner did not confirm an unactivated workload")
	}
	observation, err := runner.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)})
	if err != nil {
		return nil, err
	}
	if !samePreparedBinding(w.Preparation.Binding, observation.GetBinding()) || observation.GetWorkload().GetId() != w.Meta.Id ||
		!validVolumeValue(observation.GetResourceVersion()) || observation.Activated || observation.RemovalPending {
		return nil, fmt.Errorf("prepared workload is not waiting for activation")
	}
	w, err = r.updatePreparedWorkload(ctx, owned, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_BeginActivation{BeginActivation: &runnersv1.BeginWorkloadActivation{}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING)
	if err != nil {
		return nil, err
	}
	owned = w
	if w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
		return nil, fmt.Errorf("activation canceled before native dispatch")
	}
	activated, err := runner.ActivateWorkload(ctx, &runnerv1.ActivateWorkloadRequest{Expected: proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)})
	if err != nil {
		return nil, err
	}
	if !samePreparedBinding(w.Preparation.Binding, activated.GetBinding()) {
		return nil, fmt.Errorf("activation returned a different binding")
	}
	w, err = r.updatePreparedWorkload(ctx, owned, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmActivation{ConfirmActivation: &runnersv1.ConfirmWorkloadActivation{Binding: activated.Binding}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE)
	if err != nil {
		return nil, err
	}
	return w, nil
}
