package reconciler

import (
	"context"
	"fmt"
	"log"
	"time"

	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
)

// managedIdentityPageSize bounds each list call to keep pagination reasonable.
const managedIdentityPageSize int32 = 100

// identityCreateGracePeriod protects identities that were just minted for a
// workload whose record does not exist yet: the identity is created before the
// workload row, so a sweep in that window would reclaim a live identity.
const identityCreateGracePeriod = time.Minute

func (r *Reconciler) reconcileOrphanIdentities(ctx context.Context) error {
	organizations, err := r.agentOrganizations(ctx)
	if err != nil {
		return err
	}
	tracked, err := r.listActiveWorkloads(ctx, organizations)
	if err != nil {
		return err
	}
	active := activeZitiIdentities(tracked)

	now := time.Now().UTC()
	if err := r.sweepOrphanIdentities(ctx, identityv1.IdentityType_IDENTITY_TYPE_AGENT, active, now); err != nil {
		return err
	}
	sandboxWorkloads, err := r.listActiveSandboxWorkloads(ctx)
	if err != nil {
		return err
	}
	return r.sweepOrphanIdentities(ctx, identityv1.IdentityType_IDENTITY_TYPE_SANDBOX, activeZitiIdentities(sandboxWorkloads), now)
}

func activeZitiIdentities(workloads []*runnersv1.Workload) map[string]struct{} {
	active := make(map[string]struct{}, len(workloads))
	for _, workload := range workloads {
		zitiIdentityID := workload.GetZitiIdentityId()
		if zitiIdentityID == "" {
			continue
		}
		active[zitiIdentityID] = struct{}{}
	}
	return active
}

// listActiveSandboxWorkloads lists every non-terminal sandbox workload. Unlike
// the agent listing it is not scoped by agent organizations: a sandbox may run
// in an organization that has no agent at all.
func (r *Reconciler) listActiveSandboxWorkloads(ctx context.Context) ([]*runnersv1.Workload, error) {
	statuses := []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
	}
	pageToken := ""
	var workloads []*runnersv1.Workload
	for {
		resp, err := r.runners.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{
			PageSize:  activeWorkloadPageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListWorkloadsFilter{
				StatusIn:    statuses,
				OwnerKindIn: []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list sandbox workloads: %w", err)
		}
		for _, workload := range resp.GetWorkloads() {
			if workload == nil {
				return nil, fmt.Errorf("workload is nil")
			}
			if workload.GetRemovedAt() != nil {
				continue
			}
			workloads = append(workloads, workload)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return workloads, nil
		}
	}
}

func (r *Reconciler) sweepOrphanIdentities(ctx context.Context, identityType identityv1.IdentityType, active map[string]struct{}, now time.Time) error {
	pageToken := ""
	var deleteErr error
	for {
		resp, err := r.zitiMgmt.ListManagedIdentities(ctx, &zitimgmtv1.ListManagedIdentitiesRequest{
			IdentityType: identityType,
			PageSize:     managedIdentityPageSize,
			PageToken:    pageToken,
		})
		if err != nil {
			return fmt.Errorf("list managed identities: %w", err)
		}
		for _, identity := range resp.GetIdentities() {
			if identity == nil {
				return fmt.Errorf("managed identity is nil")
			}
			identityID := identity.GetZitiIdentityId()
			if identityID == "" {
				return fmt.Errorf("managed identity missing ziti_identity_id")
			}
			if _, ok := active[identityID]; ok {
				continue
			}
			if identityWithinCreateGrace(identity, now) {
				continue
			}
			if err := r.deleteIdentity(ctx, identityID); err != nil {
				log.Printf("reconciler: delete orphan ziti identity %s: %v", identityID, err)
				if deleteErr == nil {
					deleteErr = fmt.Errorf("delete orphan ziti identity %s: %w", identityID, err)
				}
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return deleteErr
}

func identityWithinCreateGrace(identity *zitimgmtv1.ManagedIdentity, now time.Time) bool {
	createdAt := identity.GetCreatedAt()
	if createdAt == nil {
		return false
	}
	return now.Sub(createdAt.AsTime().UTC()) < identityCreateGracePeriod
}
