package reconciler

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"google.golang.org/protobuf/proto"
)

func resourceAnchorLabels(w *runnersv1.Workload, kind runnerv1.ResourceAnchorKind, id, human string) map[string]string {
	labels := map[string]string{"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator", "managed-by": "agents-orchestrator"}
	if w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		labels["sandbox-id"], labels["sandbox-owner-id"] = w.OwnerId, human
	} else {
		labels["agent-instance-id"], labels["agent-id"] = w.OwnerId, w.AgentId
		if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			labels["thread-id"] = w.ThreadId
		}
	}
	if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME {
		labels["volume_key"] = id
	}
	return labels
}

func validateResourceAnchor(w *runnersv1.Workload, a *runnerv1.ResourceAnchor, kind runnerv1.ResourceAnchorKind, id, human string) error {
	if a == nil || a.Kind != kind || a.ResourceId != id || !preparedUUID(id) || !preparedUUID(a.InstanceUid) ||
		a.BackendId != w.GetPreparation().GetBackendId() || !validVolumeValue(a.BackendId) || len(a.BackendId) > 512 || len(a.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("complete matching native resource anchor required")
	}
	if !preparedUUID(w.OwnerId) || w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE &&
		(!preparedUUID(w.AgentId) || kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD && !preparedUUID(a.IdentityLabels["thread-id"])) ||
		w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX && !preparedUUID(human) {
		return fmt.Errorf("canonical resource anchor owner required")
	}
	expected := resourceAnchorLabels(w, kind, id, human)
	if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD && w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
		// Native inbox threads are not the registry's legacy instance alias.
		expected["thread-id"] = a.IdentityLabels["thread-id"]
	}
	if !maps.Equal(a.IdentityLabels, expected) {
		return fmt.Errorf("resource anchor owner mismatch")
	}
	return nil
}

func workloadVolumeAnchor(w *runnersv1.Workload, id string) *runnerv1.ResourceAnchor {
	for _, a := range w.GetPreparation().GetResources().GetVolumes() {
		if a.GetResourceId() == id {
			return a
		}
	}
	return nil
}

func validateWorkloadAnchors(w *runnersv1.Workload) error {
	p := w.Preparation
	a := p.Resources
	if a == nil {
		if p.Binding.GetAnchor() != nil {
			return fmt.Errorf("legacy workload contains an anchored native binding")
		}
		for _, v := range p.Binding.GetVolumes() {
			if v.GetAnchor() != nil {
				return fmt.Errorf("legacy workload contains an anchored workspace")
			}
		}
		return nil
	}
	if a.Revision == 0 || a.Revision > math.MaxInt64 || len(a.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("valid resource revision required")
	}
	if a.Workload == nil {
		if len(a.Volumes) != 0 || p.Binding != nil || a.PreparationRevocation != nil || a.RevocationObservation != nil || p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED && p.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED {
			return fmt.Errorf("resource anchors must precede preparation authority")
		}
		return nil
	}
	human := a.Workload.IdentityLabels["sandbox-owner-id"]
	if err := validateResourceAnchor(w, a.Workload, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, w.Meta.Id, human); err != nil {
		return err
	}
	if len(a.Volumes) != len(p.VolumeIds) {
		return fmt.Errorf("complete persistent volume anchor set required")
	}
	seen := map[string]bool{}
	for _, v := range a.Volumes {
		id := v.GetResourceId()
		if seen[id] || !slices.Contains(p.VolumeIds, id) {
			return fmt.Errorf("unique matching volume anchor set required")
		}
		if err := validateResourceAnchor(w, v, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, id, human); err != nil {
			return err
		}
		seen[id] = true
	}
	if b := p.Binding; b != nil {
		if !proto.Equal(b.Anchor, a.Workload) {
			return fmt.Errorf("native binding changed the workload owner UID")
		}
		for _, v := range b.Volumes {
			expected := workloadVolumeAnchor(w, v.GetVolumeKey())
			if expected == nil || !proto.Equal(v.GetAnchor(), expected) || !maps.Equal(v.IdentityLabels, expected.IdentityLabels) {
				return fmt.Errorf("native binding changed a persistent volume owner")
			}
		}
	}
	return validatePreparationRevocation(w)
}

func sameResourceOwners(a, b *runnersv1.WorkloadResourceAnchors) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	a, b = proto.Clone(a).(*runnersv1.WorkloadResourceAnchors), proto.Clone(b).(*runnersv1.WorkloadResourceAnchors)
	a.Revision, b.Revision = 0, 0
	a.PreparationRevocation, b.PreparationRevocation = nil, nil
	a.RevocationObservation, b.RevocationObservation = nil, nil
	less := func(a, b *runnerv1.ResourceAnchor) int {
		if a.ResourceId < b.ResourceId {
			return -1
		}
		if a.ResourceId > b.ResourceId {
			return 1
		}
		return 0
	}
	slices.SortFunc(a.Volumes, less)
	slices.SortFunc(b.Volumes, less)
	return proto.Equal(a, b)
}

func validateAnchorSuccessor(previous, next *runnersv1.Workload) error {
	a, b := previous.Preparation.Resources, next.Preparation.Resources
	if a == nil || b == nil {
		if a != nil || b != nil {
			return fmt.Errorf("workload resource capability changed")
		}
		return nil
	}
	if b.Revision < a.Revision || a.Workload != nil && !sameResourceOwners(a, b) {
		return fmt.Errorf("immutable resource identity or revision changed")
	}
	if a.PreparationRevocation != nil && !proto.Equal(a.PreparationRevocation, b.PreparationRevocation) ||
		a.RevocationObservation != nil && !proto.Equal(a.RevocationObservation, b.RevocationObservation) ||
		(b.Revision == a.Revision || previous.Preparation.Phase == runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVED) &&
			(!proto.Equal(a.PreparationRevocation, b.PreparationRevocation) || !proto.Equal(a.RevocationObservation, b.RevocationObservation)) {
		return fmt.Errorf("immutable preparation revocation changed or lacks a revision")
	}
	revocationWrites := uint64(0)
	if a.PreparationRevocation == nil && b.PreparationRevocation != nil {
		revocationWrites++
	}
	if a.RevocationObservation == nil && b.RevocationObservation != nil {
		revocationWrites++
	}
	if next.Preparation.Revision-previous.Preparation.Revision < revocationWrites {
		return fmt.Errorf("revocation proof and observation require separate durable writes")
	}
	metadataBindings := uint64(0)
	if a.Workload == nil && b.Workload != nil {
		metadataBindings = 1
	}
	if b.Revision-a.Revision != next.Preparation.Revision-previous.Preparation.Revision+metadataBindings {
		return fmt.Errorf("preparation and resource revisions diverged")
	}
	return nil
}

func preparedRequestOwnerLabels(req *runnerv1.StartWorkloadRequest) (map[string]string, error) {
	labels := map[string]string{"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator"}
	for _, key := range []string{"app.kubernetes.io/managed-by", "agyn.dev/managed-by", "agyn.io/workload-id"} {
		_, explicit := req.GetLabels()[key]
		_, additional := req.GetAdditionalProperties()[assembler.LabelKeyPrefix+key]
		if explicit || additional {
			return nil, fmt.Errorf("prepared request overrides a reserved native label")
		}
	}
	// Native buildLabels applies explicit labels after label.* properties. Only
	// the ownership projection is persisted; runtime configuration stays private.
	for _, key := range []string{assembler.LabelManagedBy, assembler.LabelInstanceID, assembler.LabelAgentID,
		assembler.LabelThreadID, assembler.LabelSandboxID, assembler.LabelSandboxOwnerID} {
		if value, ok := req.GetAdditionalProperties()[assembler.LabelKeyPrefix+key]; ok {
			labels[key] = value
		}
		if value, ok := req.GetLabels()[key]; ok {
			labels[key] = value
		}
	}
	return labels, nil
}

func (r *Reconciler) reservePreparedAnchors(ctx context.Context, runner runnerv1.RunnerServiceClient, w *runnersv1.Workload, plan *preparedStartPlan) (*runnersv1.Workload, error) {
	if w.Preparation.Resources.GetRevision() != 1 || w.Preparation.Resources.Workload != nil || w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_RESERVED {
		return nil, fmt.Errorf("unused anchored reservation required")
	}
	// Build the owner projection from the assembled request, then verify sandbox
	// ownership against Agents before reserving any native metadata.
	workLabels, err := preparedRequestOwnerLabels(plan.request.Workload)
	if err != nil {
		return nil, err
	}
	human := workLabels[assembler.LabelSandboxOwnerID]
	if err := r.validateObservedPreparedOwner(ctx, w, workLabels); err != nil {
		return nil, err
	}
	reserve := func(kind runnerv1.ResourceAnchorKind, id string) (*runnerv1.ResourceAnchor, error) {
		intent := &runnerv1.ResourceAnchor{Kind: kind, ResourceId: id, BackendId: w.Preparation.BackendId, IdentityLabels: resourceAnchorLabels(w, kind, id, human)}
		if kind == runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD {
			intent.IdentityLabels = maps.Clone(workLabels)
		}
		response, err := runner.ReserveResourceAnchor(ctx, &runnerv1.ReserveResourceAnchorRequest{Intent: intent})
		if err != nil {
			return nil, err
		}
		if err := validateResourceAnchor(w, response.GetAnchor(), kind, id, human); err != nil {
			return nil, err
		}
		if !maps.Equal(intent.IdentityLabels, response.Anchor.IdentityLabels) {
			return nil, fmt.Errorf("native resource reservation changed its requested owner or thread")
		}
		return proto.Clone(response.Anchor).(*runnerv1.ResourceAnchor), nil
	}
	workload, err := reserve(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_WORKLOAD, w.Meta.Id)
	if err != nil {
		return nil, err
	}
	anchors := make([]*runnerv1.ResourceAnchor, 0, len(plan.ids))
	for _, id := range plan.ids {
		v := plan.volumes[id]
		a := v.ResourceAnchor
		if a == nil {
			if v.BoundInstance != nil {
				return nil, checkedVolumeError(v, "legacy workspace needs explicit anchor adoption")
			}
			a, err = reserve(runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, id)
			if err != nil {
				return nil, err
			}
			v, err = r.updateCheckedVolume(ctx, v, &runnersv1.UpdateVolumeCheckedRequest{Operation: &runnersv1.UpdateVolumeCheckedRequest_BindAnchor{
				BindAnchor: &runnersv1.BindVolumeResourceAnchor{Anchor: a, WorkloadId: w.Meta.Id, ExpectedPreparationRevision: w.Preparation.Revision, ExpectedAnchorRevision: w.Preparation.Resources.Revision}}})
			if err != nil {
				return nil, err
			}
			plan.volumes[id] = v
		}
		if err := validateResourceAnchor(w, a, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, id, human); err != nil {
			return nil, err
		}
		anchors = append(anchors, proto.Clone(a).(*runnerv1.ResourceAnchor))
	}
	response, err := r.runners.BindWorkloadResourceAnchors(ctx, &runnersv1.BindWorkloadResourceAnchorsRequest{Id: w.Meta.Id,
		ExpectedPreparationRevision: w.Preparation.Revision, ExpectedAnchorRevision: w.Preparation.Resources.Revision, WorkloadAnchor: workload, VolumeAnchors: anchors})
	if err != nil {
		return nil, err
	}
	next := response.GetWorkload()
	if err := validatePreparedSuccessor(w, next); err != nil {
		return nil, err
	}
	expected := &runnersv1.WorkloadResourceAnchors{Workload: workload, Volumes: anchors}
	if next.Preparation.Revision != w.Preparation.Revision || next.Preparation.Resources.Revision != w.Preparation.Resources.Revision+1 || !sameResourceOwners(next.Preparation.Resources, expected) {
		return nil, fmt.Errorf("registry did not persist the reserved native owners")
	}
	return proto.Clone(next).(*runnersv1.Workload), nil
}

func revokePreparedAnchor(ctx context.Context, runner runnerv1.RunnerServiceClient, w *runnersv1.Workload) error {
	a := w.GetPreparation().GetResources().GetWorkload()
	if a == nil {
		return nil
	}
	response, err := runner.RemoveWorkloadAnchor(ctx, &runnerv1.RemoveWorkloadAnchorRequest{Expected: proto.Clone(a).(*runnerv1.ResourceAnchor)})
	if err != nil {
		return err
	}
	if !proto.Equal(response.GetAnchor(), a) || response.GetState() != runnerv1.ResourceAnchorRemovalState_RESOURCE_ANCHOR_REMOVAL_STATE_ABSENT {
		return fmt.Errorf("workload resource anchor revocation pending or unverified")
	}
	return nil
}
