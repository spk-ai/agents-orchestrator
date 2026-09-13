package reconciler

import (
	"fmt"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

type Actions struct {
	ToStart []AgentInstanceTarget
	ToStop  []*runnersv1.Workload
}

type workloadEntry struct {
	workload   *runnersv1.Workload
	activityAt time.Time
}

func ComputeActions(desired []AgentInstanceTarget, actual []*runnersv1.Workload, idleTimeouts map[uuid.UUID]time.Duration, fallbackIdleTimeout time.Duration, now time.Time) (Actions, error) {
	desiredSet := make(map[uuid.UUID]AgentInstanceTarget, len(desired))
	for _, item := range desired {
		desiredSet[item.AgentInstanceID] = item
	}
	actualSet := make(map[uuid.UUID]workloadEntry, len(actual))
	var duplicates []*runnersv1.Workload
	for _, workload := range actual {
		agentInstanceID, err := uuidutil.ParseUUID(workloadAgentInstanceID(workload), "workload.agent_instance_id")
		if err != nil {
			return Actions{}, err
		}
		activityAt, err := workloadActivityAt(workload)
		if err != nil {
			return Actions{}, err
		}
		entry := workloadEntry{workload: workload, activityAt: activityAt}
		if existing, ok := actualSet[agentInstanceID]; ok {
			if entry.activityAt.Before(existing.activityAt) {
				duplicates = append(duplicates, existing.workload)
				actualSet[agentInstanceID] = entry
			} else {
				duplicates = append(duplicates, entry.workload)
			}
			continue
		}
		actualSet[agentInstanceID] = entry
	}
	result := Actions{ToStop: duplicates}
	for _, item := range desired {
		if _, ok := actualSet[item.AgentInstanceID]; !ok {
			result.ToStart = append(result.ToStart, item)
		}
	}
	for agentInstanceID, entry := range actualSet {
		if entry.workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING || entry.workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED || entry.workload.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
			result.ToStop = append(result.ToStop, entry.workload)
			continue
		}
		if _, ok := desiredSet[agentInstanceID]; ok {
			continue
		}
		agentID, err := uuidutil.ParseUUID(workloadAgentClassID(entry.workload), "workload.agent_class_id")
		if err != nil {
			return Actions{}, err
		}
		idleTimeout, ok := idleTimeouts[agentID]
		if !ok {
			idleTimeout = fallbackIdleTimeout
		}
		if now.Sub(entry.activityAt) > idleTimeout {
			result.ToStop = append(result.ToStop, entry.workload)
		}
	}
	return result, nil
}

func workloadActivityAt(workload *runnersv1.Workload) (time.Time, error) {
	meta := workload.GetMeta()
	if meta == nil {
		return time.Time{}, fmt.Errorf("workload meta missing")
	}
	if lastActivity := workload.GetLastActivityAt(); lastActivity != nil {
		return lastActivity.AsTime(), nil
	}
	createdAt := meta.GetCreatedAt()
	if createdAt == nil {
		return time.Time{}, fmt.Errorf("workload meta created_at missing")
	}
	return createdAt.AsTime(), nil
}
