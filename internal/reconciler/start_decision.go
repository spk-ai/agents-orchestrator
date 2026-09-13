package reconciler

import (
	"context"
	"fmt"
	"log"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
)

const maxStartAttempts = 10

var startBackoffSchedule = []time.Duration{
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
}

func (r *Reconciler) shouldStartWorkload(ctx context.Context, target AgentInstanceTarget, now time.Time, agentUpdatedAt map[uuid.UUID]time.Time) (bool, error) {
	active, err := r.listWorkloadsByAgentInstance(ctx, target.AgentInstanceID.String(), []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}, 0)
	if err != nil {
		return false, err
	}
	for _, workload := range active {
		if workload.GetRemovedAt() == nil {
			return false, nil
		}
	}
	latest, err := r.latestWorkloadByAgentInstance(ctx, target.AgentInstanceID.String(), []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	})
	if err != nil {
		return false, err
	}
	if latest == nil || latest.GetStatus() == runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
		return true, nil
	}
	updatedAt, ok := agentUpdatedAt[target.AgentID]
	if !ok {
		return false, fmt.Errorf("agent %s updated_at missing", target.AgentID.String())
	}
	latestRemovedAt, err := workloadEndedAt(latest)
	if err != nil {
		return false, err
	}
	if updatedAt.After(latestRemovedAt) {
		return true, nil
	}
	lastStopped, err := r.latestWorkloadByAgentInstance(ctx, target.AgentInstanceID.String(), []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
	})
	if err != nil {
		return false, err
	}
	resetFloor := updatedAt
	if lastStopped != nil {
		stoppedAt, err := workloadCreatedAt(lastStopped)
		if err != nil {
			return false, err
		}
		if stoppedAt.After(resetFloor) {
			resetFloor = stoppedAt
		}
	}
	recentFailures, err := r.listWorkloadsByAgentInstance(ctx, target.AgentInstanceID.String(), []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}, maxStartAttempts+1)
	if err != nil {
		return false, err
	}
	consecutiveFailures := 0
	for _, failure := range recentFailures {
		failureAt, err := workloadCreatedAt(failure)
		if err != nil {
			return false, err
		}
		if !failureAt.After(resetFloor) {
			break
		}
		consecutiveFailures++
	}
	if consecutiveFailures >= maxStartAttempts {
		r.pauseInstance(ctx, target.AgentInstanceID.String(), pauseReasonStartFailuresExhausted)
		return false, nil
	}
	if consecutiveFailures == 0 {
		return true, nil
	}
	backoffIndex := consecutiveFailures - 1
	if backoffIndex >= len(startBackoffSchedule) {
		backoffIndex = len(startBackoffSchedule) - 1
	}
	backoff := startBackoffSchedule[backoffIndex]
	if now.Sub(latestRemovedAt) < backoff {
		return false, nil
	}
	return true, nil
}

func (r *Reconciler) latestWorkloadByAgentInstance(ctx context.Context, agentInstanceID string, statuses []runnersv1.WorkloadStatus) (*runnersv1.Workload, error) {
	workloads, err := r.listWorkloadsByAgentInstance(ctx, agentInstanceID, statuses, 1)
	if err != nil {
		return nil, err
	}
	if len(workloads) == 0 {
		return nil, nil
	}
	return workloads[0], nil
}

func (r *Reconciler) listWorkloadsByAgentInstance(ctx context.Context, agentInstanceID string, statuses []runnersv1.WorkloadStatus, limit int) ([]*runnersv1.Workload, error) {
	if agentInstanceID == "" {
		return nil, fmt.Errorf("agent instance id missing")
	}
	workloads := []*runnersv1.Workload{}
	pageToken := ""
	for {
		pageSize := workloadHistoryPageSize
		if limit > 0 {
			remaining := limit - len(workloads)
			if remaining <= 0 {
				break
			}
			if remaining < int(pageSize) {
				pageSize = int32(remaining)
			}
		}
		resp, err := r.runners.ListWorkloadsByAgentInstance(ctx, &runnersv1.ListWorkloadsByAgentInstanceRequest{
			AgentInstanceId: agentInstanceID,
			Statuses:        statuses,
			PageSize:        pageSize,
			PageToken:       pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list workloads for agent instance %s: %w", agentInstanceID, err)
		}
		for _, workload := range resp.GetWorkloads() {
			if workload == nil {
				return nil, fmt.Errorf("workload is nil")
			}
			meta := workload.GetMeta()
			if meta == nil {
				return nil, fmt.Errorf("workload meta missing")
			}
			if meta.GetId() == "" {
				return nil, fmt.Errorf("workload meta id missing")
			}
			workloads = append(workloads, workload)
			if limit > 0 && len(workloads) >= limit {
				break
			}
		}
		if limit > 0 && len(workloads) >= limit {
			break
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return workloads, nil
}

// pauseInstance records that the platform can no longer run an instance. The
// callers hold a runner context carrying the instance's own identity, which is
// right for the runner reads around them and wrong here: pausing is not the
// instance acting, and an instance holds no rights over its own class, so
// presenting it got every one of these refused. Agents serves the reconciler as
// an internal caller, so the identity is dropped.
func (r *Reconciler) pauseInstance(ctx context.Context, agentInstanceID, reason string) {
	if agentInstanceID == "" {
		return
	}
	ctx = ctx
	if _, err := r.agents.PauseInstance(ctx, &agentsv1.PauseInstanceRequest{Id: agentInstanceID, PauseReason: reason}); err != nil {
		log.Printf("reconciler: pause agent instance %s: %v", agentInstanceID, err)
	}
}

func workloadCreatedAt(workload *runnersv1.Workload) (time.Time, error) {
	meta := workload.GetMeta()
	if meta == nil {
		return time.Time{}, fmt.Errorf("workload meta missing")
	}
	createdAt := meta.GetCreatedAt()
	if createdAt == nil {
		return time.Time{}, fmt.Errorf("workload created_at missing")
	}
	return createdAt.AsTime().UTC(), nil
}

// workloadEndedAt is when a terminal workload ended. Runner-reported failures
// carry no removed_at; the row's updated_at is when the report landed.
func workloadEndedAt(workload *runnersv1.Workload) (time.Time, error) {
	if removedAt := workload.GetRemovedAt(); removedAt != nil {
		return removedAt.AsTime().UTC(), nil
	}
	updatedAt := workload.GetMeta().GetUpdatedAt()
	if updatedAt == nil {
		return time.Time{}, fmt.Errorf("workload removed_at and updated_at missing")
	}
	return updatedAt.AsTime().UTC(), nil
}
