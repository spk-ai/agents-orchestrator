package reconciler

import (
	"context"
	"log"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

// Absence from desired can mean idle, not paused. Only an explicit lifecycle
// state authorizes bypassing the idle timer while a daemon is still touching it.
func (r *Reconciler) inactiveInstanceStopRequests(ctx context.Context, desired []AgentInstanceTarget, actual []*runnersv1.Workload) (map[uuid.UUID]struct{}, error) {
	if !r.stopInactiveInstances {
		return nil, nil
	}
	checked := make(map[uuid.UUID]struct{}, len(desired)+len(actual))
	for _, target := range desired {
		checked[target.AgentInstanceID] = struct{}{}
	}
	requests := make(map[uuid.UUID]struct{})
	for _, workload := range actual {
		id, err := uuidutil.ParseUUID(workloadAgentInstanceID(workload), "workload.agent_instance_id")
		if err != nil {
			return nil, err
		}
		if _, ok := checked[id]; ok {
			continue
		}
		checked[id] = struct{}{}
		response, err := r.agents.GetInstance(r.platformContext(ctx), &agentsv1.GetInstanceRequest{Id: id.String()})
		if err != nil {
			// A read outage or a filtered NotFound is not an instruction to kill.
			// Retry next cycle without blocking stops for other known instances.
			log.Printf("reconciler: read lifecycle for instance %s before immediate stop: %v", id, err)
			continue
		}
		instance := response.GetInstance()
		if instance.GetMeta().GetId() != id.String() || instance.GetAgentId() != workloadAgentClassID(workload) || instance.GetOrganizationId() != workload.GetOrganizationId() {
			log.Printf("reconciler: instance %s lifecycle response has mismatched identity", id)
			continue
		}
		switch instance.GetState() {
		case agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED, agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_TERMINATED:
			requests[id] = struct{}{}
		case agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE:
		default:
			log.Printf("reconciler: instance %s lifecycle state is unspecified", id)
		}
	}
	return requests, nil
}
