package reconciler

import (
	"context"
	"fmt"
	"log"
	"strings"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
)

const activeWorkloadPageSize int32 = 100

func (r *Reconciler) fetchActual(ctx context.Context) ([]*runnersv1.Workload, error) {
	organizations, err := r.agentOrganizations(ctx)
	if err != nil {
		return nil, err
	}
	tracked, err := r.listActiveWorkloads(ctx, organizations)
	if err != nil {
		return nil, err
	}
	actual := make([]*runnersv1.Workload, 0, len(tracked))
	for _, workload := range tracked {
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: workload %s missing runner id", workload.GetMeta().GetId())
			continue
		}
		actual = append(actual, workload)
	}
	return actual, nil
}

func (r *Reconciler) listActiveWorkloads(ctx context.Context, organizations map[string]struct{}) ([]*runnersv1.Workload, error) {
	active := []*runnersv1.Workload{}
	if len(organizations) == 0 {
		return active, nil
	}
	pageToken := ""
	statuses := []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}
	for {
		resp, err := r.runners.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{
			PageSize:  activeWorkloadPageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListWorkloadsFilter{
				StatusIn: statuses,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list workloads: %w", err)
		}
		for _, workload := range resp.GetWorkloads() {
			// Agent workloads only. The callers that diff this against desired
			// agent instances would read a sandbox as an orphan and stop it.
			if workload.GetOwnerKind() == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
				continue
			}
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
			if workload.GetRemovedAt() != nil {
				continue
			}
			orgID := strings.TrimSpace(workload.GetOrganizationId())
			if orgID == "" {
				return nil, fmt.Errorf("workload %s organization id missing", meta.GetId())
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "workload.organization_id")
			if err != nil {
				return nil, err
			}
			if _, ok := organizations[parsedOrgID.String()]; !ok {
				continue
			}
			active = append(active, workload)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return active, nil
}
