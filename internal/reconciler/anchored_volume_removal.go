package reconciler

import (
	"context"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/proto"
)

func validateAnchoredVolumeObservation(v *runnersv1.Volume, observation *runnerv1.RemoveVolumeAnchoredResponse) error {
	if v.ResourceAnchor == nil || v.BoundInstance == nil || observation == nil || len(observation.ProtoReflect().GetUnknown()) != 0 ||
		observation.BackendId != v.BoundInstance.BackendId || !proto.Equal(observation.Anchor, v.ResourceAnchor) ||
		observation.State != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING && observation.State != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
		return checkedVolumeError(v, "runner did not confirm the exact anchored retirement target")
	}
	return nil
}

func validateAnchoredVolumeRetirement(v *runnersv1.Volume) error {
	intent, observation := v.RemovalIntent, v.AnchoredRemovalObservation
	if v.ResourceAnchor == nil {
		if intent.GetAnchored() || observation != nil {
			return checkedVolumeError(v, "unanchored volume has anchored retirement evidence")
		}
		return nil
	}
	if intent != nil && (!intent.Anchored || !preparedUUID(intent.Id) || len(intent.ProtoReflect().GetUnknown()) != 0) {
		return checkedVolumeError(v, "explicit anchored retirement intent required")
	}
	if v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
		if observation != nil {
			return checkedVolumeError(v, "unretired volume has an absence receipt")
		}
		return nil
	}
	if err := validateAnchoredVolumeObservation(v, observation); err != nil {
		return err
	}
	if observation.State != runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_ABSENT {
		return checkedVolumeError(v, "retired volume requires PVC and owner absence")
	}
	return nil
}

// advanceAnchoredVolumeRemoval persists intent before native PVC/owner retirement
// and its exact ABSENT receipt before reporting completion. Checked-volume helpers
// preserve immutable provenance; corrupt/unsupported replies never authorize an
// unanchored deletion fallback.
// @see runners::internal/server/anchored_volume_removal
// @see k8s-runner::internal/server/anchored_volume_removal
func (r *Reconciler) advanceAnchoredVolumeRemoval(ctx context.Context, runner runnerv1.RunnerServiceClient, v *runnersv1.Volume) (bool, error) {
	next, err := r.updateCheckedVolume(ctx, v, &runnersv1.UpdateVolumeCheckedRequest{
		Operation: &runnersv1.UpdateVolumeCheckedRequest_BeginAnchoredRemoval{BeginAnchoredRemoval: &runnersv1.BeginAnchoredVolumeRemoval{}},
	})
	if err != nil {
		return false, err
	}
	resp, err := runner.RemoveVolumeAnchored(ctx, &runnerv1.RemoveVolumeAnchoredRequest{Expected: proto.Clone(next.RemovalIntent.Expected).(*runnerv1.VolumeListItem)})
	if err != nil {
		return false, err
	}
	if err := validateAnchoredVolumeObservation(next, resp); err != nil {
		return false, err
	}
	if resp.State == runnerv1.VolumeRemovalState_VOLUME_REMOVAL_STATE_PENDING {
		return false, nil
	}
	_, err = r.updateCheckedVolume(ctx, next, &runnersv1.UpdateVolumeCheckedRequest{
		Operation: &runnersv1.UpdateVolumeCheckedRequest_ConfirmAnchoredRemoval{ConfirmAnchoredRemoval: &runnersv1.ConfirmAnchoredVolumeRemoval{
			IntentId: next.RemovalIntent.Id, Observation: proto.Clone(resp).(*runnerv1.RemoveVolumeAnchoredResponse),
		}},
	})
	return err == nil, err
}
