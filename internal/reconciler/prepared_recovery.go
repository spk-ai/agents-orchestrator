package reconciler

import (
	"context"
	"errors"
	"fmt"
	"maps"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/proto"
)

func (r *Reconciler) validateObservedPreparedOwner(ctx context.Context, w *runnersv1.Workload, labels map[string]string) error {
	expected := map[string]string{"app.kubernetes.io/managed-by": "k8s-runner", "agyn.dev/managed-by": "agents-orchestrator", "managed-by": "agents-orchestrator"}
	if w.OwnerKind == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_AGENT_INSTANCE {
		expected["agent-instance-id"], expected["agent-id"], expected["thread-id"] = w.OwnerId, w.AgentId, w.ThreadId
		if resources := w.GetPreparation().GetResources(); resources != nil {
			thread := labels["thread-id"]
			if resources.Workload != nil {
				thread = resources.Workload.IdentityLabels["thread-id"]
			}
			if !preparedUUID(thread) {
				return fmt.Errorf("canonical native inbox thread required")
			}
			expected["thread-id"] = thread
		}
	} else {
		if r.agents == nil {
			return fmt.Errorf("prepared sandbox ownership lookup unavailable")
		}
		response, err := r.agents.GetSandbox(ctx, &agentsv1.GetSandboxRequest{Ref: &agentsv1.GetSandboxRequest_Id{Id: w.OwnerId}})
		if err != nil {
			return err
		}
		sandbox := response.GetSandbox()
		if sandbox.GetMeta().GetId() != w.OwnerId || sandbox.GetOrganizationId() != w.OrganizationId || !validVolumeValue(sandbox.GetOwnerId()) {
			return fmt.Errorf("prepared sandbox ownership mismatch")
		}
		expected["sandbox-id"], expected["sandbox-owner-id"] = w.OwnerId, sandbox.OwnerId
	}
	if !maps.Equal(expected, labels) {
		return fmt.Errorf("observed preparation owner or manager mismatch")
	}
	return nil
}

func (r *Reconciler) recoverPreparedRemovalBinding(ctx context.Context, runner runnerv1.RunnerServiceClient, previous *runnersv1.Workload) (*runnersv1.Workload, error) {
	if err := validatePreparedWorkload(previous); err != nil {
		return nil, err
	}
	if previous.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING || previous.Preparation.Binding != nil || previous.Preparation.Resources.GetPreparationRevocation() != nil {
		return nil, fmt.Errorf("preparation discovery requires an unbound durable removal intent")
	}
	observation, err := runner.ObserveWorkloadPreparation(ctx, &runnerv1.ObserveWorkloadPreparationRequest{WorkloadId: previous.Meta.Id, BackendId: previous.Preparation.BackendId})
	if err != nil {
		if previous.Preparation.Resources.GetWorkload() != nil {
			recovered, revokeErr := r.recoverRevokedPreparation(ctx, runner, previous)
			if revokeErr == nil {
				return recovered, nil
			}
			err = errors.Join(revokeErr, err)
		}
		// Neither NotFound nor an unsupported capability releases admission.
		return nil, fmt.Errorf("workload %s preparation outcome unknown; admission retained: %w", previous.Meta.Id, err)
	}
	if observation.GetBinding() == nil || !validVolumeValue(observation.GetResourceVersion()) {
		return nil, fmt.Errorf("complete preparation observation required")
	}
	projected := proto.Clone(previous).(*runnersv1.Workload)
	projected.Preparation.Binding = proto.Clone(observation.Binding).(*runnerv1.WorkloadBinding)
	projected.InstanceId = stringPtr(previous.Meta.Id)
	if err := validatePreparedWorkload(projected); err != nil {
		return nil, err
	}
	if err := r.validateObservedPreparedOwner(ctx, previous, observation.IdentityLabels); err != nil {
		return nil, err
	}
	w, err := r.currentPreparedWorkload(ctx, previous)
	if err != nil {
		return nil, err
	}
	if w.Preparation.Binding != nil {
		if !samePreparedBinding(w.Preparation.Binding, observation.Binding) {
			return nil, fmt.Errorf("preparation observation conflicts with the recorded binding")
		}
		return w, nil
	}
	if w.Preparation.Phase != runnersv1.PreparedWorkloadPhase_PREPARED_WORKLOAD_PHASE_REMOVING {
		return nil, fmt.Errorf("preparation recovery lost removal authority")
	}
	// Validate the complete set before persisting any previously unknown claim.
	// An unbound first-provision row must still be its original generation; known
	// bindings may only be retained, never rediscovered as a different workspace.
	plan := &preparedStartPlan{volumes: map[string]*runnersv1.Volume{}}
	for _, item := range observation.Binding.Volumes {
		response, err := r.runners.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: item.VolumeKey})
		if err != nil {
			return nil, err
		}
		v := response.GetVolume()
		if err := validateCheckedVolume(v); err != nil {
			return nil, err
		}
		if v.Meta.Id != item.VolumeKey || v.OwnerKind != w.OwnerKind || v.OwnerId != w.OwnerId || v.OrganizationId != w.OrganizationId ||
			v.RunnerId != w.RunnerId || v.AgentId != w.AgentId || v.ThreadId != w.ThreadId || v.RemovalIntent != nil ||
			v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE && v.Status != runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING ||
			v.BoundInstance != nil && !proto.Equal(v.BoundInstance, item) || v.BoundInstance == nil && v.ResourceAnchor == nil && v.LifecycleRevision != 1 ||
			!proto.Equal(v.ResourceAnchor, workloadVolumeAnchor(w, item.VolumeKey)) {
			return nil, checkedVolumeError(v, "observed preparation changed the workspace owner, binding or generation")
		}
		if err := validateVolumeInstance(v, item); err != nil {
			return nil, err
		}
		plan.volumes[item.VolumeKey] = proto.Clone(v).(*runnersv1.Volume)
	}
	if err := r.persistPreparedVolumes(ctx, plan, observation.Binding); err != nil {
		return nil, err
	}
	// Existing checked-volume and workload CAS guards serialize competing
	// recovery/cleanup. Binding into REMOVING never grants activation authority.
	return r.persistPreparedBinding(ctx, w, observation.Binding)
}
