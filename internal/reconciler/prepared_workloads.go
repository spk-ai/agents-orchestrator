package reconciler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func preparedUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func validatePreparedWorkload(w *runnersv1.Workload) error {
	p := w.GetPreparation()
	if p == nil || !preparedUUID(w.GetMeta().GetId()) || !validVolumeValue(w.GetRunnerId()) || !validVolumeValue(w.GetOwnerId()) ||
		!validVolumeValue(w.GetOrganizationId()) || !validVolumeValue(p.BackendId) || len(p.BackendId) > 512 ||
		p.Revision == 0 || p.Revision > math.MaxInt64 || len(p.VolumeIds) > 64 ||
		p.Phase < runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED || p.Phase > runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		return fmt.Errorf("invalid prepared workload identity or lifecycle")
	}
	if w.OwnerKind != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE && w.OwnerKind != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		return fmt.Errorf("prepared workload owner kind required")
	}
	if w.Status < runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || w.Status > runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED ||
		w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE && (!validVolumeValue(w.AgentId) || !validVolumeValue(w.ThreadId)) ||
		w.AgentClassId != nil && w.GetAgentClassId() != w.AgentId ||
		w.AgentInstanceId != nil && (w.OwnerKind != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE || w.GetAgentInstanceId() != w.OwnerId) {
		return fmt.Errorf("prepared workload status or owner aliases invalid")
	}
	if w.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING && p.Phase < runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING {
		return fmt.Errorf("running workload lacks activation authorization")
	}
	ids := make(map[string]bool, len(p.VolumeIds))
	for _, id := range p.VolumeIds {
		if !preparedUUID(id) || ids[id] {
			return fmt.Errorf("invalid prepared volume set")
		}
		ids[id] = true
	}
	if err := validateWorkloadAnchors(w); err != nil {
		return err
	}
	if b := p.Binding; b != nil {
		if b.WorkloadId != w.Meta.Id || b.BackendId != p.BackendId || !preparedUUID(b.InstanceUid) ||
			w.GetInstanceId() != w.Meta.Id || len(b.Volumes) != len(ids) {
			return fmt.Errorf("prepared workload binding mismatch")
		}
		names := map[string]bool{}
		for _, v := range b.Volumes {
			if v == nil || !ids[v.VolumeKey] || names[v.InstanceId] || v.BackendId != p.BackendId {
				return fmt.Errorf("prepared volume binding set mismatch")
			}
			if err := validateVolumeInstance(&runnersv1.Volume{Meta: &runnersv1.EntityMeta{Id: v.VolumeKey}, OwnerKind: w.OwnerKind, OwnerId: w.OwnerId, AgentId: w.AgentId, ResourceAnchor: workloadVolumeAnchor(w, v.VolumeKey)}, v); err != nil {
				return err
			}
			delete(ids, v.VolumeKey)
			names[v.InstanceId] = true
		}
	} else if w.GetInstanceId() != "" {
		return fmt.Errorf("prepared instance lacks binding")
	}
	switch p.Phase {
	case runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_PREPARING:
		if p.Binding != nil {
			return fmt.Errorf("unprepared phase has binding")
		}
	case runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE:
		if p.Binding == nil {
			return fmt.Errorf("prepared binding required")
		}
	}
	if p.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		if w.RemovalConfirmedAt == nil || w.RemovalConfirmedAt.CheckValid() != nil ||
			w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED && w.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED {
			return fmt.Errorf("prepared removal confirmation required")
		}
		if p.Binding == nil {
			if p.RemovalObservation != nil {
				return fmt.Errorf("unused reservation has native observation")
			}
		} else if p.RemovalObservation.GetState() != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT || !samePreparedBinding(p.Binding, p.RemovalObservation.GetBinding()) {
			return fmt.Errorf("prepared exact absence observation required")
		}
	} else if w.RemovalConfirmedAt != nil || p.RemovalObservation != nil {
		return fmt.Errorf("unremoved preparation has removal evidence")
	}
	return nil
}

func samePreparedBinding(a, b *runnerv1.WorkloadBinding) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	a, b = proto.Clone(a).(*runnerv1.WorkloadBinding), proto.Clone(b).(*runnerv1.WorkloadBinding)
	less := func(a, b *runnerv1.VolumeListItem) int { return strings.Compare(a.GetInstanceId(), b.GetInstanceId()) }
	slices.SortFunc(a.Volumes, less)
	slices.SortFunc(b.Volumes, less)
	return proto.Equal(a, b)
}

func samePreparedIdentity(a, b *runnersv1.Workload) bool {
	av, bv := slices.Clone(a.GetPreparation().GetVolumeIds()), slices.Clone(b.GetPreparation().GetVolumeIds())
	slices.Sort(av)
	slices.Sort(bv)
	return a.GetMeta().GetId() == b.GetMeta().GetId() && a.RunnerId == b.RunnerId && a.OrganizationId == b.OrganizationId &&
		a.OwnerKind == b.OwnerKind && a.OwnerId == b.OwnerId && a.ThreadId == b.ThreadId && a.AgentId == b.AgentId &&
		a.ZitiIdentityId == b.ZitiIdentityId && a.AllocatedCpuMillicores == b.AllocatedCpuMillicores &&
		a.AllocatedRamBytes == b.AllocatedRamBytes && a.Flavor == b.Flavor && a.PersistentShells == b.PersistentShells &&
		a.GetPreparation().GetBackendId() == b.GetPreparation().GetBackendId() && slices.Equal(av, bv)
}

func (r *Reconciler) currentPreparedWorkload(ctx context.Context, previous *runnersv1.Workload) (*runnersv1.Workload, error) {
	if err := validatePreparedWorkload(previous); err != nil {
		return nil, err
	}
	response, err := r.runners.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: previous.Meta.Id})
	if err != nil {
		return nil, err
	}
	w := response.GetWorkload()
	if err := validatePreparedSuccessor(previous, w); err != nil {
		return nil, err
	}
	return proto.Clone(w).(*runnersv1.Workload), nil
}

func validatePreparedSuccessor(previous, w *runnersv1.Workload) error {
	if err := validatePreparedWorkload(previous); err != nil {
		return err
	}
	if err := validatePreparedWorkload(w); err != nil {
		return err
	}
	if !samePreparedIdentity(previous, w) || w.Preparation.Revision < previous.Preparation.Revision || w.Preparation.Phase < previous.Preparation.Phase ||
		previous.Preparation.Binding != nil && !samePreparedBinding(previous.Preparation.Binding, w.Preparation.Binding) ||
		previous.RemovalConfirmedAt != nil && !proto.Equal(previous.RemovalConfirmedAt, w.RemovalConfirmedAt) {
		return fmt.Errorf("prepared workload identity or lifecycle regressed")
	}
	if err := validateAnchorSuccessor(previous, w); err != nil {
		return err
	}
	if w.Preparation.Revision == previous.Preparation.Revision {
		a, b := proto.Clone(previous.Preparation).(*runnersv1.PreparedWorkloadLifecycle), proto.Clone(w.Preparation).(*runnersv1.PreparedWorkloadLifecycle)
		a.Resources, b.Resources = nil, nil
		if !proto.Equal(a, b) {
			return fmt.Errorf("preparation changed without its revision")
		}
	}
	if w.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED && w.Preparation.Binding == nil &&
		previous.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED && previous.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		return fmt.Errorf("authorized preparation cannot become an unused reservation")
	}
	return nil
}

func (r *Reconciler) updatePreparedWorkload(ctx context.Context, w *runnersv1.Workload, request *runnersv1.UpdatePreparedWorkloadRequest, phase runnersv1.PreparedWorkloadPhase) (*runnersv1.Workload, error) {
	if err := validatePreparedWorkload(w); err != nil {
		return nil, err
	}
	if w.Preparation.Revision == math.MaxInt64 || w.Preparation.Resources.GetRevision() == math.MaxInt64 {
		return nil, fmt.Errorf("preparation revision exhausted")
	}
	if request == nil || request.Operation == nil {
		return nil, fmt.Errorf("prepared operation required")
	}
	w = proto.Clone(w).(*runnersv1.Workload)
	request = proto.Clone(request).(*runnersv1.UpdatePreparedWorkloadRequest)
	expectedBinding := w.Preparation.Binding
	if request.GetBind() != nil {
		expectedBinding = request.GetBind().GetBinding()
	}
	request.Id, request.ExpectedRevision = w.Meta.Id, w.Preparation.Revision
	var next *runnersv1.Workload
	var err error
	if resources := w.Preparation.Resources; resources != nil {
		var response *runnersv1.UpdateAnchoredWorkloadResponse
		response, err = r.runners.UpdateAnchoredWorkload(ctx, &runnersv1.UpdateAnchoredWorkloadRequest{Operation: request, ExpectedAnchorRevision: resources.Revision})
		next = response.GetWorkload()
	} else {
		var response *runnersv1.UpdatePreparedWorkloadResponse
		response, err = r.runners.UpdatePreparedWorkload(ctx, request)
		next = response.GetWorkload()
	}
	if err != nil {
		return nil, err
	}
	if err := validatePreparedSuccessor(w, next); err != nil {
		return nil, err
	}
	if !samePreparedIdentity(w, next) || next.Preparation.Revision != w.Preparation.Revision+1 || next.Preparation.Phase != phase ||
		!samePreparedBinding(expectedBinding, next.Preparation.Binding) {
		return nil, fmt.Errorf("prepared transition was not persisted as requested")
	}
	return proto.Clone(next).(*runnersv1.Workload), nil
}

func reflectPreparedWorkload(target, current *runnersv1.Workload) {
	target.Status, target.InstanceId, target.Preparation = current.Status, current.InstanceId, current.Preparation
	target.RemovalConfirmedAt, target.RemovedAt = current.RemovalConfirmedAt, current.RemovedAt
	target.Containers = current.Containers
}

func (r *Reconciler) stopPreparedWorkload(ctx context.Context, runner runnerv1.RunnerServiceClient, previous *runnersv1.Workload) error {
	w, err := r.currentPreparedWorkload(ctx, previous)
	if err != nil {
		return err
	}
	phase := w.Preparation.Phase
	if phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED {
		if err := revokePreparedAnchor(ctx, runner, w); err != nil {
			return err
		}
		w, err = r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_AbortReservation{AbortReservation: &runnersv1.AbortWorkloadReservation{}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
	} else if phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING && phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		w, err = r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginPreparedWorkloadRemoval{}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
	}
	if err != nil {
		return err
	}
	reflectPreparedWorkload(previous, w)
	if w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED && w.Preparation.Binding == nil {
		recovered, recoverErr := r.recoverPreparedRemovalBinding(ctx, runner, w)
		if recoverErr != nil {
			// Revocation excludes execution by late creates, but cannot itself
			// prove child cleanup or release this unknown preparation's admission.
			return errors.Join(recoverErr, revokePreparedAnchor(ctx, runner, w))
		}
		w = recovered
		reflectPreparedWorkload(previous, w)
	}
	if w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		response, err := runner.RemovePreparedWorkload(ctx, &runnerv1.RemovePreparedWorkloadRequest{Expected: proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)})
		if err != nil {
			return err
		}
		if !samePreparedBinding(w.Preparation.Binding, response.GetBinding()) {
			return fmt.Errorf("prepared removal returned a different binding")
		}
		if response.GetState() != runnerv1.PreparedWorkloadRemovalState_PREPARED_WORKLOAD_REMOVAL_STATE_ABSENT {
			return fmt.Errorf("prepared workload removal pending or unconfirmed")
		}
		if err := revokePreparedAnchor(ctx, runner, w); err != nil {
			return err
		}
		w, err = r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmPreparedWorkloadRemoval{Observation: response}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
		if err != nil {
			return err
		}
		reflectPreparedWorkload(previous, w)
	}
	r.revokePullCredential(ctx, w.Meta.Id)
	if r.zitiMgmt != nil && w.ZitiIdentityId != "" {
		return r.deleteIdentity(ctx, w.ZitiIdentityId)
	}
	return nil
}

func (r *Reconciler) handlePreparedRunnerWorkload(ctx context.Context, runner runnerv1.RunnerServiceClient, previous *runnersv1.Workload) error {
	w, err := r.currentPreparedWorkload(ctx, previous)
	if err != nil {
		return err
	}
	reflectPreparedWorkload(previous, w)
	if w.Preparation.Phase >= runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING ||
		w.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || w.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED || w.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING {
		return r.stopPreparedWorkload(ctx, runner, previous)
	}
	if w.Preparation.Phase <= runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_BOUND {
		created, err := workloadCreatedAt(w)
		if err != nil {
			return err
		}
		if time.Since(created) > startGracePeriod {
			return r.stopPreparedWorkload(ctx, runner, previous)
		}
		return nil
	}
	response, err := runner.InspectPreparedWorkload(ctx, &runnerv1.InspectPreparedWorkloadRequest{Expected: proto.Clone(w.Preparation.Binding).(*runnerv1.WorkloadBinding)})
	if status.Code(err) == codes.NotFound {
		r.markWorkloadFailed(ctx, w.Meta.Id, nil, runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST, "prepared workload absent on runner", nil)
		return r.stopPreparedWorkload(ctx, runner, previous)
	}
	if err != nil {
		return err
	}
	if !samePreparedBinding(w.Preparation.Binding, response.GetBinding()) || response.GetWorkload().GetId() != w.Meta.Id || !validVolumeValue(response.GetResourceVersion()) {
		return fmt.Errorf("prepared inspection returned an unverified incarnation")
	}
	if response.RemovalPending {
		return r.stopPreparedWorkload(ctx, runner, previous)
	}
	if !response.Activated {
		if w.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE {
			return fmt.Errorf("active workload lost its native activation state")
		}
		created, err := workloadCreatedAt(w)
		if err != nil {
			return err
		}
		if time.Since(created) > startGracePeriod {
			return r.stopPreparedWorkload(ctx, runner, previous)
		}
		return nil
	}
	if w.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVATING {
		// Observation can recover a lost activation ACK. Never reissue activation
		// or an agent message from a health/recovery path.
		w, err = r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmActivation{ConfirmActivation: &runnersv1.ConfirmWorkloadActivation{Binding: response.Binding}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_ACTIVE)
		if err != nil {
			return err
		}
		reflectPreparedWorkload(previous, w)
	}
	containers, err := mapRunnerContainers(response.Workload.Containers)
	if err != nil {
		return err
	}
	var failure *workloadFailure
	ready := false
	if w.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING {
		ready, failure, err = classifyStartingContainers(containers, w, time.Now().UTC())
	} else {
		failure, err = classifyRunningContainers(containers)
		for _, container := range containers {
			if container.GetRole() == runnersv1.ContainerRole_CONTAINER_ROLE_MAIN && container.GetStatus() == runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
				failure = &workloadFailure{reason: runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST, message: "prepared main container exited"}
				break
			}
		}
	}
	if err != nil {
		return err
	}
	if failure != nil {
		r.markWorkloadFailed(ctx, w.Meta.Id, nil, failure.reason, failure.message, containers)
		return r.stopPreparedWorkload(ctx, runner, previous)
	}
	update := &runnersv1.UpdateWorkloadRequest{Id: w.Meta.Id, Containers: containers}
	if ready {
		update.Status = workloadStatusPtr(runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING)
	}
	if len(containers) > 0 || ready {
		response, err := r.runners.UpdateWorkload(ctx, update)
		if err != nil {
			return err
		}
		next := response.GetWorkload()
		if err := validatePreparedSuccessor(w, next); err != nil {
			return err
		}
		if !slices.EqualFunc(next.Containers, containers, func(a, b *runnersv1.Container) bool { return proto.Equal(a, b) }) {
			return fmt.Errorf("prepared health observation was not persisted")
		}
		reflectPreparedWorkload(previous, proto.Clone(next).(*runnersv1.Workload))
		if next.Preparation.Phase >= runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING ||
			next.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || next.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED || next.Status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING {
			return r.stopPreparedWorkload(ctx, runner, previous)
		}
		if ready && next.Status != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
			return fmt.Errorf("prepared readiness was not persisted")
		}
	}
	return nil
}
