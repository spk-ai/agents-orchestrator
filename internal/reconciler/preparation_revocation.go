package reconciler

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation"
)

func canonicalPreparationRevocation(w *runnersv1.Workload, value *runnerv1.PreparationRevocation) (*runnerv1.PreparationRevocation, error) {
	resources := w.GetPreparation().GetResources()
	if resources.GetWorkload() == nil || value == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
		!preparedUUID(value.InstanceUid) || value.SelectedPodUid != "" && !preparedUUID(value.SelectedPodUid) ||
		!proto.Equal(value.WorkloadAnchor, resources.Workload) || len(value.VolumeAnchors) != len(resources.Volumes) || len(value.VolumeAnchors) > 64 {
		return nil, fmt.Errorf("complete matching preparation revocation required")
	}
	seen := map[string]bool{}
	for _, a := range value.VolumeAnchors {
		if a == nil || seen[a.ResourceId] || !slices.ContainsFunc(resources.Volumes, func(expected *runnerv1.ResourceAnchor) bool { return proto.Equal(a, expected) }) {
			return nil, fmt.Errorf("preparation revocation changed the native owner set")
		}
		seen[a.ResourceId] = true
	}
	copy := proto.Clone(value).(*runnerv1.PreparationRevocation)
	slices.SortFunc(copy.VolumeAnchors, func(a, b *runnerv1.ResourceAnchor) int { return strings.Compare(a.ResourceId, b.ResourceId) })
	if data, err := protojson.Marshal(copy); err != nil || len(data) > 128*1024 {
		return nil, fmt.Errorf("preparation revocation exceeds its bound")
	}
	return copy, nil
}

func canonicalRevocationObservation(w *runnersv1.Workload, value *runnerv1.ObservePreparationRevocationResponse) (*runnerv1.ObservePreparationRevocationResponse, error) {
	expected := w.GetPreparation().GetResources().GetPreparationRevocation()
	if expected == nil || value == nil || len(value.ProtoReflect().GetUnknown()) != 0 ||
		value.State != runnerv1.RevokedPreparationState_REVOKED_PREPARATION_STATE_POD_ABSENT ||
		len(value.Volumes)+len(value.AbsentVolumeIds) != len(expected.VolumeAnchors) {
		return nil, fmt.Errorf("complete revoked preparation absence observation required")
	}
	proof, err := canonicalPreparationRevocation(w, value.Revocation)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(proof, expected) {
		return nil, fmt.Errorf("revocation observation changed the persisted proof")
	}
	anchors := map[string]*runnerv1.ResourceAnchor{}
	for _, a := range expected.VolumeAnchors {
		anchors[a.ResourceId] = a
	}
	seen, names, uids := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, v := range value.Volumes {
		if v == nil || len(v.ProtoReflect().GetUnknown()) != 0 || !preparedUUID(v.InstanceUid) ||
			v.InstanceId == "" || len(validation.IsDNS1123Subdomain(v.InstanceId)) != 0 ||
			seen[v.VolumeKey] || names[v.InstanceId] || uids[v.InstanceUid] || anchors[v.VolumeKey] == nil ||
			v.BackendId != expected.WorkloadAnchor.BackendId || !proto.Equal(v.Anchor, anchors[v.VolumeKey]) || !maps.Equal(v.IdentityLabels, v.Anchor.IdentityLabels) {
			return nil, fmt.Errorf("revocation observation contains an invalid workspace identity")
		}
		seen[v.VolumeKey], names[v.InstanceId], uids[v.InstanceUid] = true, true, true
	}
	for _, id := range value.AbsentVolumeIds {
		if anchors[id] == nil || seen[id] {
			return nil, fmt.Errorf("revocation workspace partition is incomplete or ambiguous")
		}
		seen[id] = true
	}
	copy := proto.Clone(value).(*runnerv1.ObservePreparationRevocationResponse)
	copy.Revocation = proof
	slices.SortFunc(copy.Volumes, func(a, b *runnerv1.VolumeListItem) int { return strings.Compare(a.VolumeKey, b.VolumeKey) })
	slices.Sort(copy.AbsentVolumeIds)
	if data, err := protojson.Marshal(copy); err != nil || len(data) > 256*1024 {
		return nil, fmt.Errorf("revocation observation exceeds its bound")
	}
	return copy, nil
}

func validatePreparationRevocation(w *runnersv1.Workload) error {
	p := w.Preparation
	proof, observation := p.Resources.GetPreparationRevocation(), p.Resources.GetRevocationObservation()
	if proof == nil && observation == nil {
		return nil
	}
	if proof == nil || p.Binding != nil || p.RemovalObservation != nil ||
		p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING && p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED ||
		(p.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED) != (observation != nil) {
		return fmt.Errorf("invalid persisted preparation revocation lifecycle")
	}
	canonical, err := canonicalPreparationRevocation(w, proof)
	if err != nil {
		return err
	}
	if !proto.Equal(proof, canonical) {
		return fmt.Errorf("persisted preparation revocation is not canonical")
	}
	if observation != nil {
		canonical, err := canonicalRevocationObservation(w, observation)
		if err != nil {
			return err
		}
		if !proto.Equal(observation, canonical) {
			return fmt.Errorf("persisted revocation observation is not canonical")
		}
	}
	return nil
}

func validateRevokedWorkspace(w *runnersv1.Workload, anchor *runnerv1.ResourceAnchor, item *runnerv1.VolumeListItem, v *runnersv1.Volume) error {
	if err := validateCheckedVolume(v); err != nil {
		return err
	}
	if v.Meta.Id != anchor.ResourceId || v.OwnerKind != w.OwnerKind || v.OwnerId != w.OwnerId || v.OrganizationId != w.OrganizationId ||
		v.RunnerId != w.RunnerId || v.AgentId != w.AgentId || v.ThreadId != w.ThreadId || v.RemovalIntent != nil || !proto.Equal(v.ResourceAnchor, anchor) {
		return checkedVolumeError(v, "revocation changed the recorded workspace owner")
	}
	if item != nil {
		if err := validateVolumeInstance(v, item); err != nil {
			return err
		}
		if v.BoundInstance != nil {
			if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE || !proto.Equal(v.BoundInstance, item) {
				return checkedVolumeError(v, "revocation cannot replace a known workspace")
			}
			return nil
		}
	}
	reservation := v.AnchorReservation
	if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING || v.LifecycleRevision != 2 || v.BoundInstance != nil || v.InstanceId != nil ||
		reservation == nil || reservation.PreparationRevision != 1 || reservation.ResourceRevision != 1 {
		return checkedVolumeError(v, "revocation requires the original unbound workspace generation")
	}
	return nil
}

// recoverRevokedPreparation persists exact proof before observing it. Validate the
// complete found/absent partition, bind discovered original PVCs, then confirm via
// a separate preparation/resource CAS. The registry rechecks under its owner lock.
// Stored proof resumes observation, not Pod discovery; lost replies never reset
// allocation provenance, replace a known UID or authorize execution.
// @see runners::internal/server/preparation_revocation
// @see k8s-runner::internal/server/preparation_revocation
func (r *Reconciler) recoverRevokedPreparation(ctx context.Context, runner runnerv1.RunnerServiceClient, previous *runnersv1.Workload) (*runnersv1.Workload, error) {
	w, err := r.currentPreparedWorkload(ctx, previous)
	if err != nil {
		return nil, err
	}
	if w.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED || w.Preparation.Binding != nil {
		return w, nil
	}
	if w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || w.Preparation.Resources.GetWorkload() == nil {
		return nil, fmt.Errorf("revocation requires an unbound anchored removal intent")
	}
	if w.Preparation.Resources.PreparationRevocation == nil {
		response, err := runner.RevokeWorkloadPreparation(ctx, &runnerv1.RevokeWorkloadPreparationRequest{
			WorkloadAnchor: proto.Clone(w.Preparation.Resources.Workload).(*runnerv1.ResourceAnchor), VolumeAnchors: w.Preparation.Resources.Volumes})
		if err != nil {
			return nil, fmt.Errorf("preparation revocation unconfirmed; admission retained: %w", err)
		}
		proof, err := canonicalPreparationRevocation(w, response.GetRevocation())
		if err != nil {
			return nil, err
		}
		w, err = r.currentPreparedWorkload(ctx, w)
		if err != nil {
			return nil, err
		}
		if w.Preparation.Binding != nil {
			return w, nil
		}
		if stored := w.Preparation.Resources.PreparationRevocation; stored != nil {
			if !proto.Equal(stored, proof) {
				return nil, fmt.Errorf("competing recovery persisted a different revocation")
			}
		} else {
			w, err = r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_RecordRevocation{
				RecordRevocation: &runnersv1.RecordPreparationRevocation{Revocation: proof}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING)
			if err != nil {
				return nil, err
			}
		}
	}
	if w.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		return w, nil
	}
	response, err := runner.ObservePreparationRevocation(ctx, &runnerv1.ObservePreparationRevocationRequest{
		Expected: proto.Clone(w.Preparation.Resources.PreparationRevocation).(*runnerv1.PreparationRevocation)})
	if err != nil {
		return nil, err
	}
	observation, err := canonicalRevocationObservation(w, response)
	if err != nil {
		return nil, err
	}
	found := map[string]*runnerv1.VolumeListItem{}
	for _, item := range observation.Volumes {
		found[item.VolumeKey] = item
	}
	// Validate the entire partition before binding newly discovered PVCs. An
	// absent known UID is an error, never permission to reset its reservation.
	plan := &preparedStartPlan{volumes: map[string]*runnersv1.Volume{}}
	for _, anchor := range w.Preparation.Resources.Volumes {
		response, err := r.runners.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: anchor.ResourceId})
		if err != nil {
			return nil, err
		}
		v := response.GetVolume()
		if err := validateRevokedWorkspace(w, anchor, found[anchor.ResourceId], v); err != nil {
			return nil, err
		}
		plan.volumes[anchor.ResourceId] = proto.Clone(v).(*runnersv1.Volume)
	}
	if err := r.persistPreparedVolumes(ctx, plan, &runnerv1.WorkloadBinding{Volumes: observation.Volumes}); err != nil {
		return nil, err
	}
	w, err = r.currentPreparedWorkload(ctx, w)
	if err != nil {
		return nil, err
	}
	if w.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
		return w, nil
	}
	// The registry rechecks checked-volume identities under the same owner lock
	// as admission. A late bind cannot turn this observation into stale absence.
	return r.updatePreparedWorkload(ctx, w, &runnersv1.UpdatePreparedWorkloadRequest{Operation: &runnersv1.UpdatePreparedWorkloadRequest_ConfirmRevocation{
		ConfirmRevocation: &runnersv1.ConfirmPreparationRevocation{Observation: observation}}}, runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED)
}
