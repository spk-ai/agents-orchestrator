package reconciler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const sandboxPageSize int32 = 100

type sandboxWorkloadPlan struct {
	sandbox          *agentsv1.Sandbox
	sandboxID        uuid.UUID
	workspaceVolumes []*runnersv1.Volume
	activeWorkload   *runnersv1.Workload
}

func (r *Reconciler) reconcileSandboxes(ctx context.Context) error {
	sandboxes, err := r.listTrackedSandboxes(ctx)
	if err != nil {
		return err
	}
	for _, sandbox := range sandboxes {
		if err := r.reconcileSandbox(ctx, sandbox, time.Now().UTC()); err != nil {
			log.Printf("reconciler: sandbox %s failed: %v", sandbox.GetMeta().GetId(), err)
		}
	}
	return nil
}

// listTrackedSandboxes reads every sandbox on the platform.
//
// Naming no organization asks the Agents service for all of them. Deriving the
// set instead - from configuration, or from which organizations have agents -
// leaves a sandbox in an organization the Orchestrator has not heard of never
// reconciled, with no pod and no error to say why.
func (r *Reconciler) listTrackedSandboxes(ctx context.Context) ([]*agentsv1.Sandbox, error) {
	var (
		sandboxes []*agentsv1.Sandbox
		pageToken string
	)
	for {
		resp, err := r.agents.ListSandboxes(ctx, &agentsv1.ListSandboxesRequest{
			IncludeTerminated: true,
			PageSize:          sandboxPageSize,
			PageToken:         pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list sandboxes: %w", err)
		}
		sandboxes = append(sandboxes, resp.GetSandboxes()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return sandboxes, nil
}

func (r *Reconciler) reconcileSandbox(ctx context.Context, sandbox *agentsv1.Sandbox, now time.Time) error {
	plan, err := r.loadSandboxWorkloadPlan(ctx, sandbox)
	if err != nil {
		return err
	}
	if ttlExpired(sandbox, now) && sandbox.GetStatus() != agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED {
		return r.terminateSandbox(ctx, plan)
	}
	switch sandbox.GetStatus() {
	case agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING:
		if plan.activeWorkload == nil {
			return r.startSandboxWorkloadAttempt(ctx, plan)
		}
		if err := r.reconcileActiveSandboxWorkload(ctx, plan); err != nil {
			return err
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING:
		if plan.activeWorkload == nil {
			if plan.sandbox.WorkloadId != nil {
				return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, "", true)
			}
			return r.startSandboxWorkloadAttempt(ctx, plan)
		}
		if err := r.reconcileActiveSandboxWorkload(ctx, plan); err != nil {
			return err
		}
		if sandboxIdle(sandbox, plan.activeWorkload, now) {
			if err := r.stopSandboxWorkload(ctx, plan.activeWorkload); err != nil {
				return err
			}
			return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, "", true)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED:
		// A stopped sandbox must always converge to no running workload, whether it
		// was stopped explicitly or by the idle timeout. Idleness is irrelevant here:
		// an attached session does not keep an explicitly stopped sandbox alive.
		if plan.activeWorkload != nil {
			if err := r.stopSandboxWorkload(ctx, plan.activeWorkload); err != nil {
				return err
			}
			return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, "", true)
		}
		if plan.sandbox.WorkloadId != nil {
			return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_STOPPED, "", true)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED:
		if plan.activeWorkload != nil {
			if err := r.stopSandboxWorkload(ctx, plan.activeWorkload); err != nil {
				return err
			}
			return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED, "", true)
		}
		if plan.sandbox.WorkloadId != nil {
			return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED, "", true)
		}
		return nil
	case agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED:
		return r.terminateSandbox(ctx, plan)
	case agentsv1.SandboxStatus_SANDBOX_STATUS_UNSPECIFIED:
		return fmt.Errorf("sandbox %s status unspecified", plan.sandboxID.String())
	default:
		return fmt.Errorf("sandbox %s status %s unsupported", plan.sandboxID.String(), sandbox.GetStatus().String())
	}
}

func (r *Reconciler) loadSandboxWorkloadPlan(ctx context.Context, sandbox *agentsv1.Sandbox) (*sandboxWorkloadPlan, error) {
	if sandbox == nil || sandbox.GetMeta() == nil {
		return nil, fmt.Errorf("sandbox meta missing")
	}
	sandboxID, err := uuidutil.ParseUUID(sandbox.GetMeta().GetId(), "sandbox.meta.id")
	if err != nil {
		return nil, err
	}
	workloads, err := r.listSandboxWorkloads(ctx, sandboxID.String())
	if err != nil {
		return nil, err
	}
	volumes, err := r.listSandboxVolumes(ctx, sandboxID.String())
	if err != nil {
		return nil, err
	}
	plan := &sandboxWorkloadPlan{sandbox: sandbox, sandboxID: sandboxID}
	// Validate the complete owner-scoped snapshot before stopping duplicates or
	// removing disks; a malformed later page must not authorize partial cleanup.
	for _, workload := range workloads {
		if workload == nil || workload.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX ||
			workload.GetOwnerId() != sandboxID.String() || workload.GetOrganizationId() != sandbox.GetOrganizationId() {
			return nil, fmt.Errorf("sandbox %s workload ownership mismatch", sandboxID)
		}
	}
	// One row per persistent volume the environment declares, plus whatever a
	// replaced definition left behind until its disk is confirmed gone.
	for _, volume := range volumes {
		if volume == nil || !isSandboxVolume(volume) || volume.GetOwnerId() != sandboxID.String() ||
			volume.GetOrganizationId() != sandbox.GetOrganizationId() {
			return nil, fmt.Errorf("sandbox %s volume ownership mismatch", sandboxID)
		}
		// Failed or deleting rows can still name retained disks. They must not
		// disappear from termination merely because billing or provisioning ended.
		plan.workspaceVolumes = append(plan.workspaceVolumes, volume)
	}
	for _, workload := range workloads {
		if workload.GetRemovalConfirmedAt() == nil {
			if plan.activeWorkload != nil {
				if err := r.stopSandboxWorkload(ctx, workload); err != nil {
					return nil, err
				}
				continue
			}
			plan.activeWorkload = workload
		}
	}
	return plan, nil
}

func (r *Reconciler) listSandboxWorkloads(ctx context.Context, sandboxID string) ([]*runnersv1.Workload, error) {
	pageToken := ""
	seenIDs, seenTokens := map[string]struct{}{}, map[string]struct{}{}
	var workloads []*runnersv1.Workload
	for {
		resp, err := r.runners.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{
			PageSize:  activeWorkloadPageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListWorkloadsFilter{
				OwnerKindIn: []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX},
				OwnerIdIn:   []string{sandboxID},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list sandbox workloads %s: %w", sandboxID, err)
		}
		if resp == nil {
			return nil, fmt.Errorf("list sandbox workloads %s: nil response", sandboxID)
		}
		for _, workload := range resp.GetWorkloads() {
			id := workload.GetMeta().GetId()
			if !validVolumeValue(id) {
				return nil, fmt.Errorf("sandbox %s workload identity missing or malformed", sandboxID)
			}
			if _, exists := seenIDs[id]; exists {
				return nil, fmt.Errorf("sandbox %s workload id %q is duplicated", sandboxID, id)
			}
			seenIDs[id] = struct{}{}
		}
		workloads = append(workloads, resp.GetWorkloads()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return workloads, nil
		}
		if _, exists := seenTokens[pageToken]; exists {
			return nil, fmt.Errorf("sandbox %s workload pagination cycle", sandboxID)
		}
		seenTokens[pageToken] = struct{}{}
	}
}

func (r *Reconciler) listSandboxVolumes(ctx context.Context, sandboxID string) ([]*runnersv1.Volume, error) {
	pageToken := ""
	seenIDs, seenTokens := map[string]struct{}{}, map[string]struct{}{}
	var volumes []*runnersv1.Volume
	for {
		resp, err := r.runners.ListVolumes(ctx, &runnersv1.ListVolumesRequest{
			PageSize:  activeVolumePageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListVolumesFilter{
				OwnerKindIn: []runnersv1.RuntimeOwnerKind{runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX},
				OwnerIdIn:   []string{sandboxID},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list sandbox volumes %s: %w", sandboxID, err)
		}
		if resp == nil {
			return nil, fmt.Errorf("list sandbox volumes %s: nil response", sandboxID)
		}
		for _, volume := range resp.GetVolumes() {
			id := volume.GetMeta().GetId()
			if !validVolumeValue(id) {
				return nil, fmt.Errorf("sandbox %s volume identity missing or malformed", sandboxID)
			}
			if _, exists := seenIDs[id]; exists {
				return nil, fmt.Errorf("sandbox %s volume id %q is duplicated", sandboxID, id)
			}
			seenIDs[id] = struct{}{}
		}
		volumes = append(volumes, resp.GetVolumes()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return volumes, nil
		}
		if _, exists := seenTokens[pageToken]; exists {
			return nil, fmt.Errorf("sandbox %s volume pagination cycle", sandboxID)
		}
		seenTokens[pageToken] = struct{}{}
	}
}

func (r *Reconciler) reconcileActiveSandboxWorkload(ctx context.Context, plan *sandboxWorkloadPlan) error {
	workload := plan.activeWorkload
	if workload == nil {
		return nil
	}
	runnerID := strings.TrimSpace(workload.GetRunnerId())
	if runnerID == "" {
		return fmt.Errorf("sandbox workload %s runner id missing", workload.GetMeta().GetId())
	}
	ownerID := strings.TrimSpace(workload.GetOwnerId())
	if ownerID == "" {
		return fmt.Errorf("sandbox workload %s owner id missing", workload.GetMeta().GetId())
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return fmt.Errorf("dial runner %s for sandbox workload %s: %w", runnerID, workload.GetMeta().GetId(), err)
	}
	instanceID := normalizeRunnerWorkloadID(workload.GetInstanceId())
	if instanceID == "" && workload.GetPreparation() == nil {
		return nil
	}
	if err := r.handlePresentRunnerWorkload(ctx, runnerClient, workload, &runnerv1.WorkloadListItem{
		InstanceId:  instanceID,
		WorkloadKey: workload.GetMeta().GetId(),
	}); err != nil {
		return err
	}
	switch workload.GetStatus() {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_RUNNING, workload.GetMeta().GetId(), false)
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED, "", true)
	default:
		return nil
	}
}

func (r *Reconciler) startSandboxWorkloadAttempt(ctx context.Context, plan *sandboxWorkloadPlan) error {
	if err := r.startSandboxWorkload(ctx, plan); err != nil {
		if updateErr := r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED, "", true); updateErr != nil {
			return fmt.Errorf("start sandbox workload: %w; update sandbox runtime failed: %v", err, updateErr)
		}
		return err
	}
	return nil
}

func (r *Reconciler) startSandboxWorkload(ctx context.Context, plan *sandboxWorkloadPlan) error {
	assembled, err := r.assembler.AssembleSandbox(ctx, plan.sandbox)
	if err != nil {
		return err
	}
	runnerID := strings.TrimSpace(assembled.RunnerID)
	if runnerID == "" {
		return fmt.Errorf("sandbox %s runner id missing", plan.sandboxID.String())
	}
	runner, enrolled, err := r.getRunnerIfEnrolled(ctx, runnerID)
	if err != nil {
		return err
	}
	if !enrolled {
		return fmt.Errorf("sandbox %s runner %s is not enrolled", plan.sandboxID.String(), runnerID)
	}
	if runner.GetMeta().GetId() == "" {
		return fmt.Errorf("sandbox %s runner meta missing", plan.sandboxID.String())
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return fmt.Errorf("dial runner %s: %w", runnerID, err)
	}
	workloadID := uuid.NewString()
	request := assembled.Request
	request.WorkloadId = workloadID
	request.Main.Env = append(request.Main.Env, &runnerv1.EnvVar{Name: "WORKLOAD_ID", Value: workloadID})
	if request.AdditionalProperties == nil {
		request.AdditionalProperties = map[string]string{}
	}
	request.AdditionalProperties[assembler.LabelKeyPrefix+assembler.LabelWorkloadKey] = workloadID
	// A sandbox pulls the same catalog images an agent does, so it needs the
	// same per-workload credential.
	if credentials, err := r.mintSandboxPullCredential(ctx, workloadID, assembled); err != nil {
		log.Printf("reconciler: %v", err)
		return err
	} else if len(credentials) > 0 {
		request.ImagePullCredentials = credentials
	}
	identity, err := r.createSandboxIdentity(ctx, plan.sandboxID, assembled.EnvironmentID, assembled.OwnerID, uuid.MustParse(workloadID), assembled.OrganizationID, assembled.LLMRoleAttributes)
	if err != nil {
		return err
	}
	zitiIdentityID := identity.idPtr()
	if identity != nil {
		if err := attachZitiEnrollmentToken(request, identity.enrollmentJWT); err != nil {
			r.compensateIdentity(ctx, zitiIdentityID, "missing ziti enroll container")
			return err
		}
	}
	createdVolumes, err := r.createSandboxVolumeRecords(ctx, assembled, runnerID)
	if err != nil {
		r.markVolumeRecordsFailed(ctx, createdVolumes)
		r.compensateIdentity(ctx, zitiIdentityID, "sandbox workspace record failure")
		return err
	}
	metadata := sandboxWorkloadMetadata(workloadID, runnerID, assembled, zitiIdentityID)
	if _, err := r.startPreparedWorkload(ctx, runnerClient, metadata, request, assembled.PersistentVolumes, createdVolumes); err != nil {
		return err
	}
	// Activation is not readiness. The shared exact-binding health check will
	// move the workload and sandbox to RUNNING after container observation.
	return r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING, workloadID, false)
}

func (r *Reconciler) updateSandboxRuntimeState(ctx context.Context, sandbox *agentsv1.Sandbox, status agentsv1.SandboxStatus, workloadID string, clearWorkloadID bool) error {
	sandboxID, err := sandboxIDString(sandbox)
	if err != nil {
		return err
	}
	statusValue := status
	req := &agentsv1.UpdateSandboxRuntimeStateRequest{Id: sandboxID, Status: &statusValue}
	if clearWorkloadID {
		req.WorkloadIdUpdate = &agentsv1.UpdateSandboxRuntimeStateRequest_ClearWorkloadId{ClearWorkloadId: true}
	} else if strings.TrimSpace(workloadID) != "" {
		req.WorkloadIdUpdate = &agentsv1.UpdateSandboxRuntimeStateRequest_WorkloadId{WorkloadId: strings.TrimSpace(workloadID)}
	}
	if runtimeStateAlreadyCurrent(sandbox, req) {
		return nil
	}
	if _, err := r.agents.UpdateSandboxRuntimeState(ctx, req); err != nil {
		return fmt.Errorf("update sandbox %s runtime state: %w", sandboxID, err)
	}
	return nil
}

// markSandboxFailed transitions a sandbox to failed when only its id is known.
// Sandboxes have no agent instance to pause, so a lost runtime resource fails
// the sandbox itself and clears the workload reference.
func (r *Reconciler) markSandboxFailed(ctx context.Context, sandboxID string) error {
	parsedSandboxID, err := uuidutil.ParseUUID(strings.TrimSpace(sandboxID), "sandbox.id")
	if err != nil {
		return err
	}
	status := agentsv1.SandboxStatus_SANDBOX_STATUS_FAILED
	if _, err := r.agents.UpdateSandboxRuntimeState(ctx, &agentsv1.UpdateSandboxRuntimeStateRequest{
		Id:               parsedSandboxID.String(),
		Status:           &status,
		WorkloadIdUpdate: &agentsv1.UpdateSandboxRuntimeStateRequest_ClearWorkloadId{ClearWorkloadId: true},
	}); err != nil {
		return fmt.Errorf("update sandbox %s runtime state: %w", parsedSandboxID.String(), err)
	}
	return nil
}

func sandboxIDString(sandbox *agentsv1.Sandbox) (string, error) {
	if sandbox == nil || sandbox.GetMeta() == nil {
		return "", fmt.Errorf("sandbox meta missing")
	}
	sandboxID, err := uuidutil.ParseUUID(sandbox.GetMeta().GetId(), "sandbox.meta.id")
	if err != nil {
		return "", err
	}
	return sandboxID.String(), nil
}

func runtimeStateAlreadyCurrent(sandbox *agentsv1.Sandbox, req *agentsv1.UpdateSandboxRuntimeStateRequest) bool {
	if sandbox.GetStatus() != req.GetStatus() {
		return false
	}
	switch workloadUpdate := req.GetWorkloadIdUpdate().(type) {
	case *agentsv1.UpdateSandboxRuntimeStateRequest_WorkloadId:
		return sandbox.GetWorkloadId() == workloadUpdate.WorkloadId
	case *agentsv1.UpdateSandboxRuntimeStateRequest_ClearWorkloadId:
		return workloadUpdate.ClearWorkloadId && sandbox.WorkloadId == nil
	case nil:
		return true
	default:
		return false
	}
}

func sandboxWorkloadMetadata(workloadID, runnerID string, assembled *assembler.SandboxAssembleResult, zitiIdentityID *string) *runnersv1.CreateWorkloadRequest {
	status := runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	zitiIdentityValue := ""
	if zitiIdentityID != nil {
		zitiIdentityValue = *zitiIdentityID
	}
	return &runnersv1.CreateWorkloadRequest{
		Id:                     workloadID,
		RunnerId:               runnerID,
		OrganizationId:         assembled.OrganizationID,
		Status:                 status,
		ZitiIdentityId:         zitiIdentityValue,
		AllocatedCpuMillicores: assembled.AllocatedCPUMillicores,
		AllocatedRamBytes:      assembled.AllocatedRAMBytes,
		Flavor:                 assembled.Flavor,
		PersistentShells:       assembled.PersistentShells,
		OwnerKind:              runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
		OwnerId:                assembled.Request.GetAdditionalProperties()[assembler.LabelKeyPrefix+assembler.LabelSandboxID],
	}
}

func (r *Reconciler) createSandboxVolumeRecords(ctx context.Context, assembled *assembler.SandboxAssembleResult, runnerID string) ([]volumeRecord, error) {
	records, err := buildVolumeRecords(assembled.PersistentVolumes)
	if err != nil {
		return nil, err
	}
	var created []volumeRecord
	for _, record := range records {
		checked, owned, err := r.createOrReuseCheckedVolume(ctx, &runnersv1.CreateVolumeRequest{
			Id:                 record.id,
			RunnerId:           runnerID,
			OrganizationId:     assembled.OrganizationID,
			SizeGb:             record.sizeGB,
			Status:             runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
			OwnerKind:          runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX,
			OwnerId:            assembled.Request.GetAdditionalProperties()[assembler.LabelKeyPrefix+assembler.LabelSandboxID],
			VolumeDefinitionId: &record.volumeID,
		})
		if err != nil {
			return created, err
		}
		if owned {
			record.checked = checked
			created = append(created, record)
		}
	}
	return created, nil
}

func (r *Reconciler) createSandboxIdentity(ctx context.Context, sandboxID, environmentID, ownerID, workloadID uuid.UUID, organizationID string, llmRoleAttributes []string) (*identityInfo, error) {
	if r.zitiMgmt == nil {
		return nil, nil
	}
	resp, err := r.zitiMgmt.CreateSandboxIdentity(ctx, &zitimgmtv1.CreateSandboxIdentityRequest{
		SandboxId:                sandboxID.String(),
		OwnerId:                  ownerID.String(),
		EnvironmentId:            environmentID.String(),
		OrganizationId:           organizationID,
		WorkloadId:               workloadID.String(),
		AdditionalRoleAttributes: llmRoleAttributes,
		Tags: map[string]string{
			"agyn.sandbox.id":       sandboxID.String(),
			"agyn.sandbox.owner_id": ownerID.String(),
			"agyn.environment.id":   environmentID.String(),
			"agyn.workload.id":      workloadID.String(),
			"agyn.organization.id":  organizationID,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create ziti identity for sandbox %s workload %s: %w", sandboxID.String(), workloadID.String(), err)
	}
	identityID := resp.GetZitiIdentityId()
	enrollmentJWT := resp.GetEnrollmentJwt()
	if identityID == "" || enrollmentJWT == "" {
		var identityPtr *string
		if identityID != "" {
			identityPtr = &identityID
		}
		r.compensateIdentity(ctx, identityPtr, "missing sandbox identity fields")
		return nil, fmt.Errorf("ziti identity response missing fields for sandbox %s workload %s", sandboxID.String(), workloadID.String())
	}
	return &identityInfo{id: identityID, enrollmentJWT: enrollmentJWT}, nil
}

func (r *Reconciler) stopSandboxWorkload(ctx context.Context, workload *runnersv1.Workload) error {
	if workload == nil || workload.GetMeta() == nil || workload.GetMeta().GetId() == "" {
		return fmt.Errorf("sandbox workload meta missing")
	}
	ownerID := strings.TrimSpace(workload.GetOwnerId())
	if ownerID == "" {
		ownerID = strings.TrimSpace(workload.GetAgentId())
	}
	// Before the stop, not after: the container is the only thing that knows
	// where its shells are, and it is about to be gone.
	r.snapshotShellDirectories(ctx, workload)

	if err := r.stopWorkloadWithContext(ctx, workload); err != nil {
		return err
	}
	if r.zitiMgmt == nil || workload.GetZitiIdentityId() == "" {
		return nil
	}
	return r.deleteIdentity(ctx, workload.GetZitiIdentityId())
}

func (r *Reconciler) terminateSandbox(ctx context.Context, plan *sandboxWorkloadPlan) error {
	if plan.activeWorkload != nil {
		if err := r.stopSandboxWorkload(ctx, plan.activeWorkload); err != nil {
			return err
		}
	}
	if err := r.updateSandboxRuntimeState(ctx, plan.sandbox, agentsv1.SandboxStatus_SANDBOX_STATUS_TERMINATED, "", true); err != nil {
		return err
	}
	if err := r.deleteSandboxWorkspace(ctx, plan); err != nil {
		return err
	}
	_, err := r.agents.DeleteSandbox(ctx, &agentsv1.DeleteSandboxRequest{Id: plan.sandboxID.String()})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

func (r *Reconciler) deleteSandboxWorkspace(ctx context.Context, plan *sandboxWorkloadPlan) error {
	for _, volume := range plan.workspaceVolumes {
		if err := validateCheckedVolume(volume); err != nil {
			return err
		}
		if volume.Status == runnersv1.VolumeStatus_VOLUME_STATUS_DELETED {
			continue
		}
		runnerClient, err := r.runnerDialer.Dial(ctx, volume.RunnerId)
		if err != nil {
			return err
		}
		if volume.Status == runnersv1.VolumeStatus_VOLUME_STATUS_FAILED {
			// Termination may recover a failed provisioning record for cleanup,
			// but it never starts a workload or invents a missing physical binding.
			volume, err = r.updateCheckedVolume(ctx, volume, &runnersv1.UpdateVolumeCheckedRequest{
				Operation: &runnersv1.UpdateVolumeCheckedRequest_Reopen{Reopen: &runnersv1.ReopenVolume{Volume: &runnersv1.CreateVolumeRequest{
					Id: volume.Meta.Id, RunnerId: volume.RunnerId, OrganizationId: volume.OrganizationId,
					OwnerKind: volume.OwnerKind, OwnerId: volume.OwnerId, ThreadId: volume.ThreadId,
					AgentId: volume.AgentId, VolumeId: volume.VolumeId, SizeGb: volume.SizeGb,
					Status: runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
				}}},
			})
			if err != nil {
				return err
			}
		}
		if volume.BoundInstance == nil {
			inventory, err := runnerClient.ListVolumes(ctx, &runnerv1.ListVolumesRequest{})
			if err != nil {
				return err
			}
			items, err := indexRunnerVolumes(inventory)
			if err != nil {
				return err
			}
			volume, err = r.bindCheckedVolume(ctx, volume, items[volume.Meta.Id])
			if err != nil {
				return err
			}
		}
		done, err := r.advanceVolumeRemoval(ctx, runnerClient, volume)
		if err != nil {
			return err
		}
		if !done {
			return fmt.Errorf("sandbox %s volume %s removal is pending", plan.sandboxID, volume.Meta.Id)
		}
	}
	return nil
}

func ttlExpired(sandbox *agentsv1.Sandbox, now time.Time) bool {
	meta := sandbox.GetMeta()
	if meta == nil || meta.GetCreatedAt() == nil {
		return false
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(sandbox.GetTtl()))
	if err != nil || ttl <= 0 {
		return false
	}
	return !now.Before(meta.GetCreatedAt().AsTime().UTC().Add(ttl))
}

func sandboxIdle(sandbox *agentsv1.Sandbox, workload *runnersv1.Workload, now time.Time) bool {
	idleTimeout, err := time.ParseDuration(strings.TrimSpace(sandbox.GetIdleTimeout()))
	if err != nil || idleTimeout <= 0 {
		return false
	}
	activityAt, err := workloadActivityAt(workload)
	if err != nil {
		return false
	}
	return now.Sub(activityAt) > idleTimeout
}

func isActiveWorkloadStatus(status runnersv1.WorkloadStatus) bool {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return true
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return false
	default:
		return false
	}
}
