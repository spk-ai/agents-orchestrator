package reconciler

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/big"
	"strings"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func validVolumeValue(value string) bool {
	return value != "" && strings.TrimSpace(value) == value
}

func checkedVolumeError(v *runnersv1.Volume, reason string) error {
	return fmt.Errorf("volume %s: %s: %w", v.GetMeta().GetId(), reason, ErrInvalidVolumeRecord)
}

func validateCheckedVolume(v *runnersv1.Volume) error {
	if !v.GetCheckedLifecycle() || v.GetLifecycleRevision() == 0 || v.GetLifecycleRevision() > math.MaxInt64 {
		return checkedVolumeError(v, "checked lifecycle and valid revision required")
	}
	for _, value := range []string{v.GetMeta().GetId(), v.GetRunnerId(), v.GetOrganizationId(), v.GetOwnerId(), v.GetVolumeId()} {
		if !validVolumeValue(value) {
			return checkedVolumeError(v, "persistent identity missing or malformed")
		}
	}
	if size, ok := new(big.Rat).SetString(v.GetSizeGb()); !ok || size.Sign() <= 0 {
		return checkedVolumeError(v, "positive size required")
	}
	switch v.GetOwnerKind() {
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE:
		if !validVolumeValue(v.GetAgentId()) || !validVolumeValue(v.GetThreadId()) ||
			v.AgentInstanceId != nil && v.GetAgentInstanceId() != v.GetOwnerId() {
			return checkedVolumeError(v, "agent owner identity invalid")
		}
	case runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX:
	default:
		return checkedVolumeError(v, "owner kind required")
	}
	if v.AgentClassId != nil && v.GetAgentClassId() != v.GetAgentId() ||
		v.VolumeDefinitionId != nil && v.GetVolumeDefinitionId() != v.GetVolumeId() {
		return checkedVolumeError(v, "identity aliases conflict")
	}
	if (v.ResourceAnchor == nil) != (v.AnchorReservation == nil) {
		return checkedVolumeError(v, "anchor reservation receipt missing or unexpected")
	}
	if a := v.ResourceAnchor; a != nil {
		if v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING && (v.BoundInstance != nil || v.LifecycleRevision != 2) {
			return checkedVolumeError(v, "anchored first provision requires its original unbound generation")
		}
		receipt := v.AnchorReservation
		if !preparedUUID(receipt.WorkloadId) || receipt.PreparationRevision == 0 || receipt.PreparationRevision > math.MaxInt64 ||
			receipt.ResourceRevision == 0 || receipt.ResourceRevision > math.MaxInt64 || len(receipt.ProtoReflect().GetUnknown()) != 0 ||
			v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
			return checkedVolumeError(v, "invalid persistent anchor reservation")
		}
		w := &runnersv1.Workload{OwnerKind: v.OwnerKind, OwnerId: v.OwnerId, AgentId: v.AgentId, Preparation: &runnersv1.PreparedWorkloadLifecycle{BackendId: a.BackendId}}
		if err := validateResourceAnchor(w, a, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, v.Meta.Id, a.IdentityLabels["sandbox-owner-id"]); err != nil {
			return err
		}
	}
	if v.GetBoundInstance() != nil {
		if err := validateVolumeInstance(v, v.BoundInstance); err != nil {
			return err
		}
		if v.GetInstanceId() != v.BoundInstance.InstanceId {
			return checkedVolumeError(v, "binding differs from stored instance")
		}
	} else if v.GetInstanceId() != "" {
		return checkedVolumeError(v, "instance has no checked binding")
	}
	switch v.GetStatus() {
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		if v.BoundInstance == nil {
			return checkedVolumeError(v, "active volume is unbound")
		}
		fallthrough
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_FAILED:
		if v.RemovalIntent != nil {
			return checkedVolumeError(v, "open or failed volume has removal intent")
		}
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_DELETED:
		intent := v.GetRemovalIntent()
		if intent == nil || !validVolumeValue(intent.Id) || intent.RequestedAt == nil || intent.RequestedAt.CheckValid() != nil ||
			v.BoundInstance == nil || !proto.Equal(intent.Expected, v.BoundInstance) {
			return checkedVolumeError(v, "valid immutable removal intent required")
		}
		if v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
			if intent.ConfirmedAt == nil || intent.ConfirmedAt.CheckValid() != nil {
				return checkedVolumeError(v, "deleted volume lacks confirmation")
			}
		} else if intent.ConfirmedAt != nil {
			return checkedVolumeError(v, "pending removal already confirmed")
		}
	default:
		return checkedVolumeError(v, "unsupported lifecycle state")
	}
	return validateAnchoredVolumeRetirement(v)
}

func validateVolumeInstance(v *runnersv1.Volume, item *runnerv1.VolumeListItem) error {
	if item == nil || !validVolumeValue(item.InstanceId) || !validVolumeValue(item.InstanceUid) || item.VolumeKey != v.GetMeta().GetId() ||
		!validVolumeValue(item.BackendId) || len(item.BackendId) > 512 {
		return checkedVolumeError(v, "complete physical identity required")
	}
	if !proto.Equal(v.ResourceAnchor, item.Anchor) {
		return checkedVolumeError(v, "native volume changed its recorded anchor")
	}
	if a := v.ResourceAnchor; a != nil {
		if !preparedUUID(item.InstanceUid) || len(item.ProtoReflect().GetUnknown()) != 0 {
			return checkedVolumeError(v, "anchored volume requires its exact native UID")
		}
		w := &runnersv1.Workload{OwnerKind: v.OwnerKind, OwnerId: v.OwnerId, AgentId: v.AgentId, Preparation: &runnersv1.PreparedWorkloadLifecycle{BackendId: item.BackendId}}
		if err := validateResourceAnchor(w, a, runnerv1.ResourceAnchorKind_RESOURCE_ANCHOR_KIND_VOLUME, v.GetMeta().GetId(), a.IdentityLabels["sandbox-owner-id"]); err != nil {
			return err
		}
		if !maps.Equal(item.IdentityLabels, a.IdentityLabels) {
			return checkedVolumeError(v, "native volume anchor owner mismatch")
		}
	}
	labels := item.IdentityLabels
	if labels["managed-by"] != "agents-orchestrator" || labels["app.kubernetes.io/managed-by"] != "k8s-runner" ||
		labels["volume_key"] != item.VolumeKey {
		return checkedVolumeError(v, "physical manager or volume key mismatch")
	}
	for key, value := range labels {
		if !validVolumeValue(value) {
			return checkedVolumeError(v, "invalid physical identity label")
		}
		switch key {
		case "managed-by", "app.kubernetes.io/managed-by", "volume_key", "agent-instance-id", "agent-id", "sandbox-id", "sandbox-owner-id":
		case "agyn.dev/managed-by":
			if value != "agents-orchestrator" {
				return checkedVolumeError(v, "physical workload manager mismatch")
			}
		default:
			return checkedVolumeError(v, "unexpected physical identity label")
		}
	}
	if isSandboxVolume(v) {
		if labels["sandbox-id"] != v.OwnerId || labels["sandbox-owner-id"] == "" || labels["agent-id"] != "" || labels["agent-instance-id"] != "" {
			return checkedVolumeError(v, "physical sandbox owner mismatch")
		}
	} else if labels["agent-instance-id"] != v.GetOwnerId() || labels["agent-id"] != v.GetAgentId() || labels["sandbox-id"] != "" || labels["sandbox-owner-id"] != "" {
		return checkedVolumeError(v, "physical agent owner mismatch")
	}
	return nil
}

func sameVolumeIdentity(a, b *runnersv1.Volume) bool {
	return a.GetMeta().GetId() == b.GetMeta().GetId() && a.GetRunnerId() == b.GetRunnerId() &&
		a.GetOrganizationId() == b.GetOrganizationId() && a.GetOwnerKind() == b.GetOwnerKind() &&
		a.GetOwnerId() == b.GetOwnerId() && a.GetThreadId() == b.GetThreadId() &&
		a.GetAgentId() == b.GetAgentId() && a.GetVolumeId() == b.GetVolumeId()
}

func volumeMatchesRequest(v *runnersv1.Volume, req *runnersv1.CreateVolumeRequest) bool {
	definition, class := req.GetVolumeId(), req.GetAgentId()
	if req.VolumeDefinitionId != nil {
		definition = req.GetVolumeDefinitionId()
	}
	if req.AgentClassId != nil {
		class = req.GetAgentClassId()
	}
	return sameVolumeIdentity(v, &runnersv1.Volume{
		Meta: &runnersv1.EntityMeta{Id: req.GetId()}, RunnerId: req.GetRunnerId(), OrganizationId: req.GetOrganizationId(),
		OwnerKind: req.GetOwnerKind(), OwnerId: req.GetOwnerId(), ThreadId: req.GetThreadId(), AgentId: class, VolumeId: definition,
	})
}

func sameVolumeSize(a, b string) bool {
	left, ok := new(big.Rat).SetString(a)
	right, otherOK := new(big.Rat).SetString(b)
	return ok && otherOK && left.Cmp(right) == 0
}

func (r *Reconciler) updateCheckedVolume(ctx context.Context, v *runnersv1.Volume, req *runnersv1.UpdateVolumeCheckedRequest) (*runnersv1.Volume, error) {
	if err := validateCheckedVolume(v); err != nil {
		return nil, err
	}
	if req.GetOperation() == nil || v.LifecycleRevision == math.MaxInt64 {
		return nil, checkedVolumeError(v, "operation and advanceable revision required")
	}
	if v.ResourceAnchor != nil && req.GetBind() == nil && req.GetBeginAnchoredRemoval() == nil && req.GetConfirmAnchoredRemoval() == nil {
		return nil, checkedVolumeError(v, "anchored volume requires a separate retirement contract")
	}
	if v.ResourceAnchor == nil && (req.GetBeginAnchoredRemoval() != nil || req.GetConfirmAnchoredRemoval() != nil) {
		return nil, checkedVolumeError(v, "anchored retirement requires persistent ownership")
	}
	v = proto.Clone(v).(*runnersv1.Volume)
	req = proto.Clone(req).(*runnersv1.UpdateVolumeCheckedRequest)
	req.Id, req.ExpectedRevision = v.Meta.Id, v.LifecycleRevision
	resp, err := r.runners.UpdateVolumeChecked(ctx, req)
	if err != nil {
		return nil, err
	}
	next := resp.GetVolume()
	if err := validateCheckedVolume(next); err != nil {
		return nil, err
	}
	if !sameVolumeIdentity(v, next) || next.LifecycleRevision != v.LifecycleRevision+1 {
		return nil, checkedVolumeError(v, "checked update changed identity or returned the wrong revision")
	}
	if req.GetBindAnchor() == nil && (!proto.Equal(v.ResourceAnchor, next.ResourceAnchor) || !proto.Equal(v.AnchorReservation, next.AnchorReservation)) {
		return nil, checkedVolumeError(v, "checked update changed persistent native ownership")
	}
	if req.GetReopen() == nil && !sameVolumeSize(v.SizeGb, next.SizeGb) {
		return nil, checkedVolumeError(v, "checked update changed size outside an explicit reopen")
	}
	if req.GetConfirmAnchoredRemoval() == nil && !proto.Equal(v.AnchoredRemovalObservation, next.AnchoredRemovalObservation) {
		return nil, checkedVolumeError(v, "checked update changed native retirement evidence")
	}
	valid := false
	switch op := req.Operation.(type) {
	case *runnersv1.UpdateVolumeCheckedRequest_BindAnchor:
		valid = v.ResourceAnchor == nil && v.BoundInstance == nil && next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING &&
			next.BoundInstance == nil && next.RemovalIntent == nil && proto.Equal(next.ResourceAnchor, op.BindAnchor.GetAnchor()) &&
			proto.Equal(next.AnchorReservation, &runnersv1.VolumeAnchorReservation{WorkloadId: op.BindAnchor.GetWorkloadId(),
				PreparationRevision: op.BindAnchor.GetExpectedPreparationRevision(), ResourceRevision: op.BindAnchor.GetExpectedAnchorRevision()})
	case *runnersv1.UpdateVolumeCheckedRequest_Bind:
		valid = next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE && proto.Equal(next.BoundInstance, op.Bind.GetInstance())
	case *runnersv1.UpdateVolumeCheckedRequest_BeginRemoval:
		valid = next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING && proto.Equal(next.BoundInstance, v.BoundInstance) &&
			(v.RemovalIntent == nil || proto.Equal(v.RemovalIntent, next.RemovalIntent))
	case *runnersv1.UpdateVolumeCheckedRequest_BeginAnchoredRemoval:
		valid = next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING && next.RemovalIntent.GetAnchored() &&
			proto.Equal(next.BoundInstance, v.BoundInstance) && (v.RemovalIntent == nil || proto.Equal(v.RemovalIntent, next.RemovalIntent))
	case *runnersv1.UpdateVolumeCheckedRequest_ConfirmAnchoredRemoval:
		if v.RemovalIntent != nil && op.ConfirmAnchoredRemoval.GetIntentId() == v.RemovalIntent.Id && next.RemovalIntent != nil {
			expected := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			if expected.ConfirmedAt == nil {
				expected.ConfirmedAt = next.RemovalIntent.ConfirmedAt
			}
			valid = next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED && proto.Equal(expected, next.RemovalIntent) &&
				proto.Equal(v.BoundInstance, next.BoundInstance) && proto.Equal(next.AnchoredRemovalObservation, op.ConfirmAnchoredRemoval.Observation)
		}
	case *runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval:
		if v.RemovalIntent != nil && op.ConfirmRemoval.GetIntentId() == v.RemovalIntent.Id && next.RemovalIntent != nil {
			expected := proto.Clone(v.RemovalIntent).(*runnersv1.VolumeRemovalIntent)
			expected.ConfirmedAt = next.RemovalIntent.ConfirmedAt
			valid = next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED && proto.Equal(expected, next.RemovalIntent)
		}
	case *runnersv1.UpdateVolumeCheckedRequest_FailProvisioning:
		valid = next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED && proto.Equal(next.BoundInstance, v.BoundInstance)
	case *runnersv1.UpdateVolumeCheckedRequest_Reopen:
		valid = volumeMatchesRequest(next, op.Reopen.GetVolume()) && next.Status == runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING &&
			sameVolumeSize(next.SizeGb, op.Reopen.GetVolume().GetSizeGb()) && next.RemovedAt == nil
		if v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
			valid = valid && next.BoundInstance == nil
		} else {
			valid = valid && proto.Equal(v.BoundInstance, next.BoundInstance)
		}
	}
	if !valid {
		return nil, checkedVolumeError(v, "checked update did not confirm the requested transition")
	}
	return proto.Clone(next).(*runnersv1.Volume), nil
}

func (r *Reconciler) createOrReuseCheckedVolume(ctx context.Context, req *runnersv1.CreateVolumeRequest) (*runnersv1.Volume, bool, error) {
	resp, err := r.runners.CreateVolumeChecked(ctx, &runnersv1.CreateVolumeCheckedRequest{Volume: req})
	if err == nil {
		v := resp.GetVolume()
		if err := validateCheckedVolume(v); err != nil {
			return nil, false, err
		}
		if !volumeMatchesRequest(v, req) || v.LifecycleRevision != 1 || v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING ||
			v.BoundInstance != nil || v.ResourceAnchor != nil || v.AnchorReservation != nil || v.RemovedAt != nil || !sameVolumeSize(v.SizeGb, req.GetSizeGb()) {
			return nil, false, checkedVolumeError(v, "create did not return the requested new generation")
		}
		return proto.Clone(v).(*runnersv1.Volume), true, nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return nil, false, err
	}
	v, err := r.prepareExistingVolumeRecord(ctx, req)
	if err != nil {
		return nil, false, err
	}
	switch v.Status {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		return proto.Clone(v).(*runnersv1.Volume), false, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, runnersv1.VolumeStatus_VOLUME_STATUS_DELETED:
		// The registry's owner admission guard also requires all predecessor
		// workloads to have explicit removal confirmation before reopening.
		next, err := r.updateCheckedVolume(ctx, v, &runnersv1.UpdateVolumeCheckedRequest{
			Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: req}},
		})
		return next, err == nil, err
	default:
		return nil, false, checkedVolumeError(v, "volume removal is still pending")
	}
}

func (r *Reconciler) bindCheckedVolume(ctx context.Context, v *runnersv1.Volume, item *runnerv1.VolumeListItem) (*runnersv1.Volume, error) {
	if err := validateCheckedVolume(v); err != nil {
		return nil, err
	}
	if err := validateVolumeInstance(v, item); err != nil {
		return nil, err
	}
	if v.BoundInstance != nil && !proto.Equal(v.BoundInstance, item) {
		return nil, checkedVolumeError(v, "cannot rebind a physical incarnation")
	}
	if isSandboxVolume(v) {
		if r.agents == nil {
			return nil, checkedVolumeError(v, "sandbox ownership lookup unavailable")
		}
		sandbox, err := r.agents.GetSandbox(ctx, &agentsv1.GetSandboxRequest{Ref: &agentsv1.GetSandboxRequest_Id{Id: v.OwnerId}})
		if err != nil {
			return nil, err
		}
		owner := sandbox.GetSandbox()
		if owner.GetMeta().GetId() != v.OwnerId || owner.GetOrganizationId() != v.OrganizationId ||
			owner.GetOwnerId() != item.IdentityLabels["sandbox-owner-id"] {
			return nil, checkedVolumeError(v, "physical sandbox user ownership mismatch")
		}
	}
	return r.updateCheckedVolume(ctx, v, &runnersv1.UpdateVolumeCheckedRequest{
		Operation: &runnersv1.UpdateVolumeCheckedRequest_Bind{Bind: &runnersv1.BindVolumeInstance{Instance: item}},
	})
}

func (r *Reconciler) advanceVolumeRemoval(ctx context.Context, runner runnerv1.RunnerServiceClient, v *runnersv1.Volume) (bool, error) {
	if err := validateCheckedVolume(v); err != nil {
		return false, err
	}
	if v.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
		return true, nil
	}
	if v.BoundInstance == nil {
		return false, checkedVolumeError(v, "unbound volume requires provisioning reconciliation before removal")
	}
	if v.ResourceAnchor != nil {
		return r.advanceAnchoredVolumeRemoval(ctx, runner, v)
	}
	next, err := r.updateCheckedVolume(ctx, v, &runnersv1.UpdateVolumeCheckedRequest{
		Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginRemoval{BeginRemoval: &runnersv1.BeginVolumeRemoval{}},
	})
	if err != nil {
		return false, err
	}
	resp, err := runner.RemoveVolumeBound(ctx, &runnerv1.RemoveVolumeBoundRequest{
		Expected: proto.Clone(next.RemovalIntent.Expected).(*runnerv1.VolumeListItem),
	})
	if err != nil {
		return false, err
	}
	if resp.GetBackendId() != next.RemovalIntent.Expected.BackendId {
		return false, checkedVolumeError(next, "runner did not confirm the stored backend identity")
	}
	switch resp.GetState() {
	case runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING:
		return false, nil
	case runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT:
		_, err := r.updateCheckedVolume(ctx, next, &runnersv1.UpdateVolumeCheckedRequest{
			Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmRemoval{ConfirmRemoval: &runnersv1.ConfirmVolumeRemoval{IntentId: next.RemovalIntent.Id, BackendId: resp.BackendId}},
		})
		return err == nil, err
	default:
		return false, checkedVolumeError(next, "runner did not report a recognized removal state")
	}
}
